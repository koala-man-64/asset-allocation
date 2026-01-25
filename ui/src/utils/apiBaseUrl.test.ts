import { describe, expect, it } from 'vitest';

import { normalizeApiBaseUrl, toWebSocketBaseUrl } from './apiBaseUrl';

describe('normalizeApiBaseUrl', () => {
  it('defaults to /api', () => {
    expect(normalizeApiBaseUrl(undefined)).toBe('/api');
    expect(normalizeApiBaseUrl('')).toBe('/api');
    expect(normalizeApiBaseUrl('   ')).toBe('/api');
  });

  it('adds /api to bare origin URLs', () => {
    expect(normalizeApiBaseUrl('http://localhost:8000')).toBe('http://localhost:8000/api');
    expect(normalizeApiBaseUrl('https://example.com')).toBe('https://example.com/api');
  });

  it('keeps explicit paths', () => {
    expect(normalizeApiBaseUrl('/api')).toBe('/api');
    expect(normalizeApiBaseUrl('/api/')).toBe('/api');
    expect(normalizeApiBaseUrl('http://localhost:8000/api')).toBe('http://localhost:8000/api');
  });
});

describe('toWebSocketBaseUrl', () => {
  it('converts http(s) to ws(s)', () => {
    expect(toWebSocketBaseUrl('http://localhost:8000/api')).toBe('ws://localhost:8000/api');
    expect(toWebSocketBaseUrl('https://example.com/api')).toBe('wss://example.com/api');
  });

  it('leaves relative URLs unchanged', () => {
    expect(toWebSocketBaseUrl('/api')).toBe('/api');
  });
});

