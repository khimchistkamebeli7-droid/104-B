import type { Candle, Tick, Timeframe, ConnectionStatus, SourceId } from '@/types/domain';
import type { DataSource, ConnectResult } from '../source';
import { normalizeServerTime } from '../source';
import { serverClock } from '../server-clock';
import { mapSymbolForDeriv } from '../symbols';
import { PROVIDERS_CONFIG, buildDerivWsUrls } from '../providers.config';
import { captureError } from '@/lib/sentry';

type StatusListener = (status: ConnectionStatus) => void;
type TickListener = (tick: Tick) => void;
type CandleListener = (candle: Candle, isClosed: boolean) => void;

interface PendingReq {
  resolve: (data: Record<string, unknown>) => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

function safeNum(v: unknown): number {
  if (typeof v === 'number') return v;
  if (typeof v === 'string') return parseFloat(v);
  return NaN;
}

function hostOf(url: string): string {
  try { return new URL(url).host; } catch { return url; }
}

export class DerivSource implements DataSource {
  readonly id: SourceId = 'deriv';

  private ws: WebSocket | null = null;
  private activeSymbol: string | null = null;
  private activeTimeframe: Timeframe | null = null;
  private statusListeners = new Set<StatusListener>();
  private tickListeners = new Set<TickListener>();
  private candleListeners = new Set<CandleListener>();
  private pending = new Map<string, PendingReq>();
  private reqSeq = 0;
  private status: ConnectionStatus = 'idle';
  private reconnectCount = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private lastCandleTime = 0;
  // Здоровье WS-стрима: polling должен работать как fallback/watchdog, а не
  // как постоянный параллельный поток. Для публичного app_id=1089 (без
  // поддержки подписок) стрим никогда не станет healthy, и поведение не
  // отличается от прежнего — poll работает как раньше на каждом тике таймера.
  // Но когда стрим реально жив (продакшен app_id, форекс — где Deriv
  // является ПЕРВИЧНЫМ источником по ROUTING_CHAIN), poll() становится no-op,
  // пока WS присылает сообщения регулярно — это убирает постоянную гонку
  // WS-vs-REST за одну и ту же формирующуюся свечу.
  private streamHealthy = false;
  private lastStreamMessageAt = 0;
  // Подключение к сокету: единый in-flight промис (чтобы параллельные
  // ensureSocket() не открывали два сокета), функция отмены текущей попытки
  // и «поколение» — disconnect() увеличивает его, и устаревший цикл перебора
  // эндпоинтов, начатый до disconnect(), сам понимает, что ему пора выйти.
  private socketPromise: Promise<void> | null = null;
  private cancelConnect: (() => void) | null = null;
  private connectGen = 0;

  async connect(symbolId: string, timeframe: Timeframe): Promise<ConnectResult> {
    const cfg = PROVIDERS_CONFIG.deriv;
    const granularity = cfg.granularityMap[timeframe];
    if (!granularity) throw new Error(`Deriv: unsupported timeframe ${timeframe}`);

    this.disconnect();
    this.activeSymbol = symbolId;
    this.activeTimeframe = timeframe;
    this.reconnectCount = 0;
    this.setStatus('connecting');
    try {
      await this.ensureSocket();
      const derivSymbol = mapSymbolForDeriv(symbolId);
      const candles = await this.fetchHistory(symbolId, timeframe, cfg.defaultHistory);
      // Streaming subscriptions are best-effort: the public Deriv app_id (1089)
      // does not support live subscriptions, so we fall back to polling below.
      try {
        await this.subscribeStreams(derivSymbol);
      } catch (streamErr) {
        captureError(new Error(`Deriv stream subscription failed, using polling: ${streamErr instanceof Error ? streamErr.message : 'unknown'}`), { level: 'info' });
      }
      this.startPing();
      this.startFallbackPolling(symbolId, timeframe);
      this.setStatus('live');
      return { candles, source: this.id };
    } catch (err) {
      // Неудачный connect() не должен оставлять «зомби»: ConnectionManager
      // создаёт НОВЫЙ экземпляр на каждую попытку и старый не отключает.
      // Без этой очистки onclose провалившегося сокета запускал бы
      // scheduleReconnect() в фоне, и брошенный экземпляр продолжал бы
      // переподключаться и опрашивать Deriv параллельно с рабочим.
      this.disconnect();
      throw err;
    }
  }

  disconnect(): void {
    this.connectGen++;
    const cancel = this.cancelConnect;
    this.cancelConnect = null;
    this.socketPromise = null;
    cancel?.();
    this.activeSymbol = null;
    this.activeTimeframe = null;
    this.reconnectCount = 0;
    this.lastCandleTime = 0;
    this.streamHealthy = false;
    this.lastStreamMessageAt = 0;
    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }
    if (this.ws) {
      this.ws.onopen = null;
      this.ws.onmessage = null;
      this.ws.onerror = null;
      this.ws.onclose = null;
      if (this.ws.readyState === WebSocket.OPEN) this.ws.close();
      this.ws = null;
    }
    this.pending.forEach((p) => { clearTimeout(p.timer); p.reject(new Error('Disconnected')); });
    this.pending.clear();
    this.setStatus('idle');
  }

  async fetchHistory(symbolId: string, timeframe: Timeframe, count: number): Promise<Candle[]> {
    const cfg = PROVIDERS_CONFIG.deriv;
    const granularity = cfg.granularityMap[timeframe];
    if (!granularity) throw new Error(`Deriv: unsupported timeframe ${timeframe}`);
    const derivSymbol = mapSymbolForDeriv(symbolId);
    const resp = await this.send({
      ticks_history: derivSymbol, end: 'latest', style: 'candles', granularity, count,
    });
    const candlesRaw = resp.candles as Array<Record<string, unknown>> | undefined;
    if (!Array.isArray(candlesRaw)) throw new Error('Deriv: unexpected history shape');
    return candlesRaw.map((c) => ({
      time: safeNum(c.epoch),
      open: safeNum(c.open),
      high: safeNum(c.high),
      low: safeNum(c.low),
      close: safeNum(c.close),
      volume: 0,
    }));
  }

  async fetchServerTime(): Promise<number> {
    const resp = await this.send({ time: 1 });
    return normalizeServerTime(safeNum(resp.time));
  }

  onTick(cb: TickListener): () => void {
    this.tickListeners.add(cb);
    return () => this.tickListeners.delete(cb);
  }

  onCandle(cb: CandleListener): () => void {
    this.candleListeners.add(cb);
    return () => this.candleListeners.delete(cb);
  }

  onStatus(cb: StatusListener): () => void {
    this.statusListeners.add(cb);
    cb(this.status);
    return () => this.statusListeners.delete(cb);
  }

  private ensureSocket(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return Promise.resolve();
    if (this.socketPromise) return this.socketPromise;
    const p: Promise<void> = this.openWithFallback().finally(() => {
      if (this.socketPromise === p) this.socketPromise = null;
    });
    this.socketPromise = p;
    return p;
  }

  /**
   * Открывает сокет, перебирая эндпоинты из PROVIDERS_CONFIG.deriv.wsEndpoints
   * СТРОГО С НАЧАЛА списка (сначала ws.binaryws.com, затем ws.derivws.com).
   * Вызывается и при первичном подключении, и из scheduleReconnect(), поэтому
   * каждое переподключение снова начинает с приоритетного хоста, а не
   * «залипает» на запасном.
   */
  private async openWithFallback(): Promise<void> {
    const gen = this.connectGen;
    const urls = buildDerivWsUrls();
    const failures: string[] = [];
    for (let i = 0; i < urls.length; i++) {
      const url = urls[i];
      try {
        const ws = await this.openSocketOnce(url);
        if (gen !== this.connectGen) {
          // disconnect() случился, пока сокет открывался — не подключаемся.
          ws.onmessage = null;
          try { ws.close(); } catch { /* ignore */ }
          throw new Error('Deriv: disconnected');
        }
        this.attachSocket(ws);
        if (i > 0) {
          captureError(
            new Error(`Deriv: ${hostOf(urls[0])} unavailable (${failures.join('; ')}), connected via ${hostOf(url)}`),
            { level: 'info' },
          );
        }
        return;
      } catch (err) {
        if (gen !== this.connectGen) throw new Error('Deriv: disconnected');
        failures.push(`${hostOf(url)}: ${err instanceof Error ? err.message : 'unknown error'}`);
      }
    }
    throw new Error(`Deriv WS connection failed — all endpoints unreachable (${failures.join('; ')})`);
  }

  /** Одна попытка открыть сокет на конкретном URL, с таймаутом. */
  private openSocketOnce(url: string): Promise<WebSocket> {
    const timeoutMs = PROVIDERS_CONFIG.deriv.connectTimeoutMs;
    return new Promise<WebSocket>((resolve, reject) => {
      const ws = new WebSocket(url);
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | null = null;
      const finish = (err: Error | null): void => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        this.cancelConnect = null;
        if (err) {
          ws.onopen = null;
          ws.onerror = null;
          ws.onclose = null;
          ws.onmessage = null;
          try { ws.close(); } catch { /* ignore */ }
          reject(err);
        } else {
          resolve(ws);
        }
      };
      timer = setTimeout(() => finish(new Error(`connect timeout ${timeoutMs}ms`)), timeoutMs);
      this.cancelConnect = () => finish(new Error('Deriv: disconnected'));
      ws.onopen = () => finish(null);
      ws.onerror = () => finish(new Error('connection error'));
      ws.onclose = () => finish(new Error('closed before open'));
    });
  }

  /** Навешивает рабочие обработчики на уже открытый сокет. */
  private attachSocket(ws: WebSocket): void {
    this.ws = ws;
    // Ошибку после открытия обрабатывает onclose, который браузер шлёт следом.
    ws.onerror = null;
    ws.onclose = () => {
      // Закрытие устаревшего сокета (уже заменённого новым) не должно
      // запускать переподключение поверх живого соединения.
      if (this.ws !== ws) return;
      this.pending.forEach((p) => { clearTimeout(p.timer); p.reject(new Error('Deriv WS closed')); });
      this.pending.clear();
      if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
      // Стрим больше не жив — polling должен снова взять на себя роль
      // основного источника, пока (и если) reconnect не восстановит WS.
      this.streamHealthy = false;
      if (this.activeSymbol !== null) this.scheduleReconnect();
    };
    ws.onmessage = (e) => {
      if (typeof e.data === 'string') this.handleMessage(e.data);
    };
  }

  private send(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return Promise.reject(new Error('Deriv: socket not open'));
    }
    const reqId = ++this.reqSeq;
    const key = String(reqId);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.has(key)) {
          this.pending.delete(key);
          reject(new Error('Deriv: request timeout'));
        }
      }, PROVIDERS_CONFIG.deriv.requestTimeoutMs);
      this.pending.set(key, { resolve, reject, timer });
      this.ws!.send(JSON.stringify({ ...payload, req_id: reqId }));
    });
  }

  private async subscribeStreams(derivSymbol: string): Promise<void> {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    await this.send({ ticks: derivSymbol, subscribe: 1 });
    const granularity = PROVIDERS_CONFIG.deriv.granularityMap[this.activeTimeframe ?? '1m'];
    if (granularity) {
      await this.send({ ohlc: derivSymbol, subscribe: 1, granularity });
    }
    // Подписки прошли успешно — стрим считается живым с этого момента.
    // Дальше он подтверждается фактическими сообщениями в handleMessage().
    this.streamHealthy = true;
    this.lastStreamMessageAt = Date.now();
  }

  private startPing(): void {
    if (this.pingTimer) clearInterval(this.pingTimer);
    const cfg = PROVIDERS_CONFIG.deriv;
    this.pingTimer = setInterval(() => {
      if (this.ws && this.ws.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ ping: 1 }));
      }
    }, cfg.pingIntervalMs);
  }

  private startFallbackPolling(symbolId: string, timeframe: Timeframe): void {
    if (this.pollTimer) clearInterval(this.pollTimer);
    const intervalMs = this.getFallbackPollInterval(timeframe);
    this.pollTimer = setInterval(() => {
      if (!this.activeSymbol || !this.activeTimeframe) return;
      // Poll — это fallback/watchdog, а не постоянный параллельный поток.
      // Если WS реально жив и недавно что-то прислал, REST-запрос — no-op:
      // иначе для форекса (где Deriv — первичный источник, см.
      // providers.config.ts ROUTING_CHAIN) WS и polling гонялись бы за одну
      // и ту же формирующуюся свечу постоянно, а не только при сбое стрима.
      // Порог 2×intervalMs даёт стриму разумный запас — WS шлёт ohlc на
      // каждый тик, значительно чаще, чем раз в intervalMs при нормальной
      // работе.
      const streamStale = !this.streamHealthy || (Date.now() - this.lastStreamMessageAt) > intervalMs * 2;
      if (!streamStale) return;
      void this.poll(symbolId, timeframe).catch(() => {
        this.setStatus('degraded');
      });
    }, intervalMs);
  }

  private getFallbackPollInterval(timeframe: Timeframe): number {
    switch (timeframe) {
      case '1m': return 3_000;
      case '5m': return 5_000;
      case '15m': return 10_000;
      case '30m': return 15_000;
      case '1h': return 30_000;
      case '4h': return 60_000;
      case '1d': return 120_000;
      default: return 10_000;
    }
  }

  private async poll(symbolId: string, timeframe: Timeframe): Promise<void> {
    const tfSec = PROVIDERS_CONFIG.deriv.granularityMap[timeframe] ?? 60;
    const candles = await this.fetchHistory(symbolId, timeframe, 2);
    if (candles.length === 0) return;
    const now = Math.floor(serverClock.now() / 1000);
    const latest = candles[candles.length - 1];
    if (latest.time > this.lastCandleTime) {
      this.lastCandleTime = latest.time;
      const isClosed = now >= latest.time + tfSec;
      this.emit(this.candleListeners, latest, isClosed);
    }
    if (latest) {
      this.emit(this.tickListeners, { price: latest.close, time: latest.time });
    }
    this.setStatus('live');
  }

  private scheduleReconnect(): void {
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    const cfg = PROVIDERS_CONFIG.deriv;
    const maxReconnect = cfg.reconnectBackoffMs.length;
    if (this.reconnectCount >= maxReconnect) {
      this.setStatus('failed');
      return;
    }
    const idx = Math.min(this.reconnectCount, cfg.reconnectBackoffMs.length - 1);
    const delay = cfg.reconnectBackoffMs[idx];
    this.reconnectCount++;
    this.setStatus('reconnecting');
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      if (!this.activeSymbol || !this.activeTimeframe) return;
      void this.ensureSocket().then(() => {
        const derivSymbol = mapSymbolForDeriv(this.activeSymbol!);
        return this.subscribeStreams(derivSymbol).catch((e) => {
          captureError(new Error(`Deriv reconnect stream failed, using polling: ${e instanceof Error ? e.message : 'unknown'}`), { level: 'info' });
        });
      }).then(() => {
        this.startPing();
        this.startFallbackPolling(this.activeSymbol!, this.activeTimeframe!);
        this.reconnectCount = 0;
        this.setStatus('live');
      }).catch(() => {
        this.scheduleReconnect();
      });
    }, delay);
  }

  private handleMessage(raw: string): void {
    let data: unknown;
    try { data = JSON.parse(raw); } catch { return; }
    if (!data || typeof data !== 'object') return;
    const msg = data as Record<string, unknown>;

    const reqId = typeof msg.req_id === 'number' ? String(msg.req_id)
      : typeof msg.req_id === 'string' ? msg.req_id : undefined;
    if (reqId && this.pending.has(reqId)) {
      const pending = this.pending.get(reqId)!;
      this.pending.delete(reqId);
      clearTimeout(pending.timer);
      if (msg.error) {
        const errObj = msg.error as Record<string, unknown>;
        const errMsg = errObj.message;
        pending.reject(new Error(typeof errMsg === 'string' ? errMsg : 'Deriv error'));
      } else {
        pending.resolve(msg);
      }
      return;
    }

    if (msg.error && !reqId) {
      const errObj = msg.error as Record<string, unknown>;
      const errMsg = typeof errObj.message === 'string' ? errObj.message : 'Deriv subscription error';
      captureError(new Error(`Deriv stream error: ${errMsg}`), { level: 'warning' });
      return;
    }

    if (msg.tick) {
      this.lastStreamMessageAt = Date.now();
      const tick = msg.tick as Record<string, unknown>;
      const t: Tick = { price: safeNum(tick.quote), time: safeNum(tick.epoch) };
      this.emit(this.tickListeners, t);
    }

    if (msg.ohlc) {
      this.lastStreamMessageAt = Date.now();
      const ohlc = msg.ohlc as Record<string, unknown>;
      const tfSec = PROVIDERS_CONFIG.deriv.granularityMap[this.activeTimeframe ?? '1m'] ?? 60;
      const openTime = safeNum(ohlc.open_time);
      const candle: Candle = {
        time: openTime,
        open: safeNum(ohlc.open),
        high: safeNum(ohlc.high),
        low: safeNum(ohlc.low),
        close: safeNum(ohlc.close),
        volume: 0,
      };
      const now = Math.floor(serverClock.now() / 1000);
      const isClosed = now >= openTime + tfSec;
      this.emit(this.candleListeners, candle, isClosed);
    }
  }

  private setStatus(s: ConnectionStatus): void {
    this.status = s;
    this.emit(this.statusListeners, s);
  }

  private emit<T extends (...args: never[]) => void>(listeners: Set<T>, ...args: Parameters<T>): void {
    listeners.forEach((l) => {
      try { l(...args); } catch { /* isolate listener errors */ }
    });
  }
}
