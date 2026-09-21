import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('../server-clock', () => ({
  serverClock: { now: () => Date.now() },
}));

const PRIMARY_URL = 'wss://primary.mock/websockets/v3?app_id=test';
const FALLBACK_URL = 'wss://fallback.mock/websockets/v3?app_id=test';

vi.mock('../providers.config', () => ({
  buildDerivWsUrls: () => [
    'wss://primary.mock/websockets/v3?app_id=test',
    'wss://fallback.mock/websockets/v3?app_id=test',
  ],
  PROVIDERS_CONFIG: {
    deriv: {
      connectTimeoutMs: 6000,
      appId: 'test',
      granularityMap: { '1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400 },
      reconnectBackoffMs: [3000, 6000, 12000, 30000, 60000],
      pingIntervalMs: 15000,
      requestTimeoutMs: 10000,
      defaultHistory: 1000,
    },
  },
}));

vi.mock('../symbols', () => ({
  mapSymbolForDeriv: (s: string) => s,
}));

vi.mock('@/lib/sentry', () => ({
  captureError: vi.fn(),
}));

class MockWebSocket {
  static instances: MockWebSocket[] = [];
  static last(): MockWebSocket { return MockWebSocket.instances[MockWebSocket.instances.length - 1]; }

  // Реальный global.WebSocket.OPEN/.CONNECTING и т.д. — эти статики нужны,
  // потому что производственный код сравнивает readyState именно с
  // `WebSocket.OPEN` (глобальным), а не с числом-литералом. Без них любое
  // сравнение с undefined всегда ложно, и send()/subscribeStreams() тихо
  // не отправляют ничего даже при открытом соединении — раньше это было
  // незаметно, потому что fetchHistory в тестах мокался целиком и не
  // проходил через реальный send().
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSING = 2;
  readonly CLOSED = 3;

  readyState = 0;
  onopen: (() => void) | null = null;
  onmessage: ((e: { data: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  onclose: (() => void) | null = null;
  sent: string[] = [];

  constructor(public url: string) {
    MockWebSocket.instances.push(this);
  }

  addEventListener(event: string, cb: (...args: never[]) => void) {
    if (event === 'open') this.onopen = cb;
    if (event === 'error') this.onerror = cb;
    if (event === 'close') this.onclose = cb;
  }

  removeEventListener() {}

  send(data: string) { this.sent.push(data); }

  close() { this.readyState = 3; this.onclose?.(); }

  fireOpen() { this.readyState = 1; this.onopen?.(); }

  fireError() { this.onerror?.(); }

  fireMessage(obj: Record<string, unknown>) {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }
}

global.WebSocket = MockWebSocket as unknown as typeof WebSocket;

import { DerivSource } from './deriv';

describe('DerivSource fallback polling', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    MockWebSocket.instances = [];
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function flushMicrotasks(times = 10): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve();
  }

  it('invokes fetchHistory via fallback polling once the stream goes stale', async () => {
    const source = new DerivSource();
    const fetchSpy = vi.spyOn(source, 'fetchHistory').mockResolvedValue([
      { time: 100, open: 1, high: 2, low: 0.5, close: 1.5, volume: 0 },
    ]);

    const connectPromise = source.connect('R_100', '1m');
    const ws = MockWebSocket.last();
    ws.fireOpen();

    // Резолвим subscribeStreams (ticks-subscribe req_id 1, ohlc-subscribe
    // req_id 2) — с реальными WebSocket.OPEN-константами в моке send()
    // теперь действительно уходит в сокет и ждёт ответа, поэтому оба
    // запроса нужно закрыть, иначе connect() зависнет на requestTimeoutMs.
    await flushMicrotasks();
    ws.fireMessage({ req_id: 1 });
    await flushMicrotasks();
    ws.fireMessage({ req_id: 2 });
    await flushMicrotasks();

    await connectPromise;

    const initialCalls = fetchSpy.mock.calls.length;

    // Стрим не присылает ничего дальше — становится stale, watchdog-polling
    // должен взять на себя роль основного источника.
    vi.advanceTimersByTime(10_000);

    expect(fetchSpy.mock.calls.length).toBeGreaterThan(initialCalls);

    source.disconnect();
  });

  it('does not poll while the WS stream is healthy (recent tick/ohlc message received)', async () => {
    // Регрессионный тест на фикс "polling как watchdog, а не постоянный
    // параллельный поток": если стрим реально жив (успешная подписка +
    // недавнее сообщение), poll() не должен вызываться на каждом тике
    // таймера — иначе WS и REST гоняются за одной и той же формирующейся
    // свечой, и REST может перезаписать более свежие/широкие high/low.
    const source = new DerivSource();
    const fetchSpy = vi.spyOn(source, 'fetchHistory').mockResolvedValue([
      { time: 100, open: 1, high: 2, low: 0.5, close: 1.5, volume: 0 },
    ]);

    const connectPromise = source.connect('R_100', '1m');
    const ws = MockWebSocket.last();
    ws.fireOpen();

    // Резолвим все pending req_id по очереди: fetchHistory замокан
    // напрямую (не идёт через send()), поэтому единственные реальные
    // send()-запросы — это subscribeStreams (ticks subscribe, ohlc
    // subscribe). Отвечаем на оба, чтобы подписка считалась успешной.
    // flushMicrotasks нужен, потому что между fireOpen и фактическим
    // ws.send() внутри subscribeStreams лежит несколько промежуточных
    // await (ensureSocket, мокнутый fetchHistory) — одного tick очереди
    // микрозадач недостаточно, чтобы до них добраться.
    await flushMicrotasks();
    ws.fireMessage({ req_id: 1 });
    await flushMicrotasks();
    ws.fireMessage({ req_id: 2 });
    await flushMicrotasks();

    await connectPromise;

    const initialCalls = fetchSpy.mock.calls.length;

    // Стрим шлёт ohlc-апдейты регулярно, чаще, чем таймер fallback-poll
    // (3с для '1m') — держим его "свежим" на каждом тике таймера, как
    // вело бы себя реальное живое соединение.
    for (let i = 0; i < 8; i++) {
      vi.advanceTimersByTime(1_000);
      ws.fireMessage({ ohlc: { open_time: 100, open: 1, high: 2, low: 0.5, close: 1.6 } });
    }

    // За 8 секунд поллинг-таймер (интервал 3с) успел бы тикнуть дважды-трижды,
    // но т.к. стрим все это время оставался "свежим" (heartbeat каждую
    // секунду < intervalMs*2 = 6с), poll ни разу реально не вызвался.
    expect(fetchSpy.mock.calls.length).toBe(initialCalls); // poll не вызвался — стрим здоров

    source.disconnect();
  });

  it('resumes polling once the stream goes stale (no messages for a while)', async () => {
    const source = new DerivSource();
    const fetchSpy = vi.spyOn(source, 'fetchHistory').mockResolvedValue([
      { time: 100, open: 1, high: 2, low: 0.5, close: 1.5, volume: 0 },
    ]);

    const connectPromise = source.connect('R_100', '1m');
    const ws = MockWebSocket.last();
    ws.fireOpen();
    await flushMicrotasks();
    ws.fireMessage({ req_id: 1 });
    await flushMicrotasks();
    ws.fireMessage({ req_id: 2 });
    await flushMicrotasks();
    await connectPromise;

    const initialCalls = fetchSpy.mock.calls.length;

    // Стрим "замолкает" дольше 2×intervalMs (2×3000мс для '1m') без новых
    // сообщений — polling должен снова взять на себя роль основного
    // источника.
    vi.advanceTimersByTime(9_000);

    expect(fetchSpy.mock.calls.length).toBeGreaterThan(initialCalls);

    source.disconnect();
  });
});


describe('DerivSource endpoint fallback', () => {
  const HISTORY = [{ time: 100, open: 1, high: 2, low: 0.5, close: 1.5, volume: 0 }];

  beforeEach(() => {
    vi.useFakeTimers();
    MockWebSocket.instances = [];
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function flushMicrotasks(times = 20): Promise<void> {
    for (let i = 0; i < times; i++) await Promise.resolve();
  }

  // Отвечает на все ещё не отвеченные запросы с req_id (подписки ticks/ohlc),
  // чтобы connect() дошёл до конца. fetchHistory в этих тестах замокан.
  async function answerRequests(ws: MockWebSocket, rounds = 6): Promise<void> {
    const answered = new Set<number>();
    for (let i = 0; i < rounds; i++) {
      await flushMicrotasks();
      for (const raw of ws.sent) {
        const { req_id } = JSON.parse(raw) as { req_id?: number };
        if (req_id !== undefined && !answered.has(req_id)) {
          answered.add(req_id);
          ws.fireMessage({ req_id });
        }
      }
    }
  }

  const urls = () => MockWebSocket.instances.map((w) => w.url);

  function makeSource(): DerivSource {
    const source = new DerivSource();
    vi.spyOn(source, 'fetchHistory').mockResolvedValue(HISTORY);
    return source;
  }

  it('tries the primary endpoint (ws.binaryws.com) first and does not touch the fallback when it works', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');

    expect(urls()).toEqual([PRIMARY_URL]);
    const ws = MockWebSocket.last();
    ws.fireOpen();
    await answerRequests(ws);
    const result = await connectPromise;

    expect(result.source).toBe('deriv');
    expect(urls()).toEqual([PRIMARY_URL]);
    source.disconnect();
  });

  it('switches to the fallback endpoint (ws.derivws.com) when the primary errors out', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');

    MockWebSocket.instances[0].fireError();
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL, FALLBACK_URL]);

    const ws = MockWebSocket.last();
    ws.fireOpen();
    await answerRequests(ws);
    await expect(connectPromise).resolves.toMatchObject({ source: 'deriv' });
    source.disconnect();
  });

  it('switches to the fallback when the primary closes before opening', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');

    MockWebSocket.instances[0].close();
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL, FALLBACK_URL]);

    const ws = MockWebSocket.last();
    ws.fireOpen();
    await answerRequests(ws);
    await expect(connectPromise).resolves.toMatchObject({ source: 'deriv' });
    source.disconnect();
  });

  it('switches to the fallback when the primary hangs past connectTimeoutMs', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');

    // Первый хост молчит: ни open, ни error.
    vi.advanceTimersByTime(5_999);
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL]);

    vi.advanceTimersByTime(1);
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL, FALLBACK_URL]);
    // «Зависший» сокет не остаётся висеть в фоне.
    expect(MockWebSocket.instances[0].readyState).toBe(3);

    const ws = MockWebSocket.last();
    ws.fireOpen();
    await answerRequests(ws);
    await expect(connectPromise).resolves.toMatchObject({ source: 'deriv' });
    source.disconnect();
  });

  it('rejects with a message naming both hosts when every endpoint is down, and leaves no background reconnect', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');
    const assertion = expect(connectPromise).rejects.toThrow(/all endpoints unreachable.*primary\.mock.*fallback\.mock/);

    MockWebSocket.instances[0].fireError();
    await flushMicrotasks();
    MockWebSocket.instances[1].fireError();
    await flushMicrotasks();
    await assertion;

    // Регрессия «зомби»: провалившийся connect() не должен запускать фоновый
    // scheduleReconnect(). За две минуты — ни одного нового сокета.
    const created = MockWebSocket.instances.length;
    vi.advanceTimersByTime(120_000);
    await flushMicrotasks();
    expect(MockWebSocket.instances.length).toBe(created);
  });

  it('cleans up (closes the socket, no reconnect) when connect() fails after the socket opened', async () => {
    const source = new DerivSource();
    vi.spyOn(source, 'fetchHistory').mockRejectedValue(new Error('history down'));
    const connectPromise = source.connect('EURUSD', '1m');
    const assertion = expect(connectPromise).rejects.toThrow('history down');

    const ws = MockWebSocket.last();
    ws.fireOpen();
    await flushMicrotasks();
    await assertion;

    expect(ws.readyState).toBe(3);
    vi.advanceTimersByTime(120_000);
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL]);
  });

  it('restarts from the primary endpoint on every reconnect (not stuck on the fallback)', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');

    // Первичное подключение: primary недоступен, поднимаемся на fallback.
    MockWebSocket.instances[0].fireError();
    await flushMicrotasks();
    const wsFallback = MockWebSocket.last();
    wsFallback.fireOpen();
    await answerRequests(wsFallback);
    await connectPromise;
    expect(urls()).toEqual([PRIMARY_URL, FALLBACK_URL]);

    // Обрыв соединения → reconnect через 3 с должен снова начать с primary.
    wsFallback.close();
    vi.advanceTimersByTime(3_000);
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL, FALLBACK_URL, PRIMARY_URL]);

    // Primary всё ещё лежит → снова fallback (список перебирается заново).
    MockWebSocket.instances[2].fireError();
    await flushMicrotasks();
    expect(urls()).toEqual([PRIMARY_URL, FALLBACK_URL, PRIMARY_URL, FALLBACK_URL]);

    source.disconnect();
  });

  it('a stale socket closing after being replaced does not trigger a spurious reconnect', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');
    const ws1 = MockWebSocket.last();
    ws1.fireOpen();
    await answerRequests(ws1);
    await connectPromise;

    // Обрыв → штатный reconnect на новый сокет ws2, доводим его до 'live'.
    ws1.close();
    vi.advanceTimersByTime(3_000);
    await flushMicrotasks();
    const ws2 = MockWebSocket.last();
    expect(ws2).not.toBe(ws1);
    ws2.fireOpen();
    await answerRequests(ws2);

    const statuses: string[] = [];
    const unsubscribe = source.onStatus((st) => statuses.push(st));
    statuses.length = 0; // onStatus сразу отдаёт текущий статус — отбрасываем его

    // Запоздалое событие close от уже замещённого сокета не должно ни
    // переводить источник в 'reconnecting', ни рвать запросы на живом ws2.
    ws1.onclose?.();
    await flushMicrotasks();
    expect(statuses).toEqual([]);

    unsubscribe();
    source.disconnect();
  });

  it('disconnect() while connecting cancels the attempt and does not fall through to the next endpoint', async () => {
    const source = makeSource();
    const connectPromise = source.connect('EURUSD', '1m');
    const assertion = expect(connectPromise).rejects.toThrow('disconnected');

    source.disconnect();
    await flushMicrotasks();
    await assertion;

    expect(urls()).toEqual([PRIMARY_URL]);
    expect(MockWebSocket.instances[0].readyState).toBe(3);
  });
});
