import { describe, it, expect } from 'vitest';
import { PROVIDERS_CONFIG, buildDerivWsUrls, resolveDerivAppId } from './providers.config';

describe('Deriv WS endpoints', () => {
  it('lists ws.binaryws.com first and ws.derivws.com as the fallback', () => {
    expect(PROVIDERS_CONFIG.deriv.wsEndpoints).toEqual([
      'wss://ws.binaryws.com/websockets/v3',
      'wss://ws.derivws.com/websockets/v3',
    ]);
  });

  it('buildDerivWsUrls keeps the priority order and appends the current app_id to every endpoint', () => {
    const appId = encodeURIComponent(resolveDerivAppId());
    expect(buildDerivWsUrls()).toEqual([
      `wss://ws.binaryws.com/websockets/v3?app_id=${appId}`,
      `wss://ws.derivws.com/websockets/v3?app_id=${appId}`,
    ]);
  });

  it('has a positive per-endpoint connect timeout so a hanging host cannot block the fallback', () => {
    expect(PROVIDERS_CONFIG.deriv.connectTimeoutMs).toBeGreaterThan(0);
  });
});
