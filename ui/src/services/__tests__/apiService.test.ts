import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('@/services/authTransport', () => ({
  appendAuthHeaders: vi.fn(async (headersInput?: HeadersInit) => new Headers(headersInput))
}));

type ApiServiceModule = typeof import('@/services/apiService');

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { 'Content-Type': 'application/json' }
  });
}

describe('apiService cold start handling', () => {
  const fetchMock = vi.fn();
  const windowWithConfig = window as typeof window & {
    __API_UI_CONFIG__?: { apiBaseUrl?: string };
  };

  beforeEach(() => {
    vi.resetModules();
    fetchMock.mockReset();
    windowWithConfig.__API_UI_CONFIG__ = { apiBaseUrl: '/api' };
    vi.stubGlobal('fetch', fetchMock);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    delete windowWithConfig.__API_UI_CONFIG__;
  });

  async function importApiService(): Promise<ApiServiceModule> {
    return import('@/services/apiService');
  }

  it('warms up once and does not repeat warm-up calls on later requests', async () => {
    fetchMock
      .mockResolvedValueOnce(new Response('warming', { status: 503, statusText: 'Service Unavailable' }))
      .mockResolvedValueOnce(jsonResponse({ status: 'ok' }))
      .mockResolvedValueOnce(jsonResponse({ data: 1 }))
      .mockResolvedValueOnce(jsonResponse({ data: 2 }));

    const { request } = await importApiService();

    const first = await request<{ data: number }>('/system/health');
    const second = await request<{ data: number }>('/system/health');

    expect(first.data).toBe(1);
    expect(second.data).toBe(2);
    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/healthz');
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/healthz');
    expect(fetchMock.mock.calls[2]?.[0]).toContain('/api/system/health');
    expect(fetchMock.mock.calls[3]?.[0]).toContain('/api/system/health');
  });

  it('retries transient response failures for primary requests', async () => {
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ status: 'ok' }))
      .mockResolvedValueOnce(new Response('temporary failure', { status: 503, statusText: 'Service Unavailable' }))
      .mockResolvedValueOnce(jsonResponse({ data: 7 }));

    const { request } = await importApiService();

    const response = await request<{ data: number }>('/system/health');

    expect(response.data).toBe(7);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/healthz');
    expect(fetchMock.mock.calls[1]?.[0]).toContain('/api/system/health');
    expect(fetchMock.mock.calls[2]?.[0]).toContain('/api/system/health');
  });

  it('falls back from prefixed api base to /api on 404 and reuses fallback', async () => {
    windowWithConfig.__API_UI_CONFIG__ = { apiBaseUrl: '/asset-allocation/api' };
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ status: 'ok' }))
      .mockResolvedValueOnce(new Response('not found', { status: 404, statusText: 'Not Found' }))
      .mockResolvedValueOnce(jsonResponse({ status: 'ok' }))
      .mockResolvedValueOnce(jsonResponse({ data: 11 }))
      .mockResolvedValueOnce(jsonResponse({ data: 12 }));

    const { request } = await importApiService();

    const first = await request<{ data: number }>('/system/health');
    const second = await request<{ data: number }>('/system/health');

    expect(first.data).toBe(11);
    expect(second.data).toBe(12);
    expect(fetchMock).toHaveBeenCalledTimes(5);
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/asset-allocation/healthz');
    expect(fetchMock.mock.calls[1]?.[0]).toContain('/asset-allocation/api/system/health');
    expect(fetchMock.mock.calls[2]?.[0]).toBe('/healthz');
    expect(fetchMock.mock.calls[3]?.[0]).toContain('/api/system/health');
    expect(fetchMock.mock.calls[4]?.[0]).toContain('/api/system/health');
  });
});
