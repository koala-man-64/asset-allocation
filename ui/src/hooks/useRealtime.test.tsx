import React from 'react';
import { render, waitFor, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const { mockToastError } = vi.hoisted(() => ({
  mockToastError: vi.fn()
}));

vi.mock('sonner', () => ({
  toast: {
    error: mockToastError
  }
}));

import { useRealtime } from './useRealtime';
import { setAccessTokenProvider } from '@/services/authTransport';
import { queryKeys } from '@/hooks/useDataQueries';
import { REALTIME_SUBSCRIBE_EVENT, addConsoleLogStreamListener } from '@/services/realtimeBus';

class MockWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: MockWebSocket[] = [];

  readonly url: string;
  readyState = MockWebSocket.CONNECTING;
  sent: string[] = [];
  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onclose: ((event: CloseEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;

  constructor(url: string | URL) {
    this.url = String(url);
    MockWebSocket.instances.push(this);
  }

  send(data: string): void {
    this.sent.push(String(data));
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.(new Event('close') as CloseEvent);
  }

  open(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.(new Event('open'));
  }

  emitJson(payload: unknown): void {
    this.onmessage?.({ data: JSON.stringify(payload) } as MessageEvent);
  }
}

function Harness() {
  useRealtime();
  return null;
}

function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
        staleTime: 0
      }
    }
  });
}

describe('useRealtime', () => {
  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal('WebSocket', MockWebSocket as unknown as typeof WebSocket);
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ ticket: 'default-ticket', expiresAt: '2026-03-15T12:00:00Z' })
      }) as unknown as typeof fetch
    );
    setAccessTokenProvider(null);
    (window as Window & { __API_UI_CONFIG__?: Record<string, unknown> }).__API_UI_CONFIG__ = {
      apiBaseUrl: '/api'
    };
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
    mockToastError.mockReset();
    setAccessTokenProvider(null);
    delete (window as Window & { __API_UI_CONFIG__?: Record<string, unknown> }).__API_UI_CONFIG__;
  });

  it('subscribes to dynamic log topics and emits console log stream events', async () => {
    const listener = vi.fn();
    const unsubscribe = addConsoleLogStreamListener(listener);
    const queryClient = createQueryClient();

    const view = render(
      <QueryClientProvider client={queryClient}>
        <Harness />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(MockWebSocket.instances).toHaveLength(1);
    });
    const ws = MockWebSocket.instances[0];

    act(() => {
      ws.open();
    });

    await waitFor(() => {
      expect(ws.sent).toHaveLength(1);
    });
    expect(JSON.parse(ws.sent[0])).toEqual({
      action: 'subscribe',
      topics: [
        'backtests',
        'system-health',
        'jobs',
        'container-apps',
        'runtime-config',
        'debug-symbols'
      ]
    });

    act(() => {
      window.dispatchEvent(
        new CustomEvent(REALTIME_SUBSCRIBE_EVENT, {
          detail: { topics: ['job-logs:bronze-market-job'] }
        })
      );
    });

    await waitFor(() => {
      expect(
        ws.sent.some((message) => {
          const parsed = JSON.parse(message);
          return (
            parsed.action === 'subscribe' &&
            Array.isArray(parsed.topics) &&
            parsed.topics.includes('job-logs:bronze-market-job')
          );
        })
      ).toBe(true);
    });

    act(() => {
      ws.emitJson({
        topic: 'job-logs:bronze-market-job',
        data: {
          type: 'CONSOLE_LOG_STREAM',
          payload: {
            resourceType: 'job',
            resourceName: 'bronze-market-job',
            lines: [
              {
                id: 'line-1',
                message: 'streamed line',
                timestamp: '2026-03-11T12:00:00Z',
                stream_s: 'stderr',
                executionName: 'bronze-market-job-exec-001'
              }
            ],
            polledAt: '2026-03-11T12:00:05Z'
          }
        }
      });
    });

    await waitFor(() => {
      expect(listener).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'job-logs:bronze-market-job',
          resourceType: 'job',
          resourceName: 'bronze-market-job',
          lines: [
            expect.objectContaining({
              id: 'line-1',
              message: 'streamed line',
              stream_s: 'stderr',
              executionName: 'bronze-market-job-exec-001'
            })
          ]
        })
      );
    });

    unsubscribe();
    view.unmount();
  });

  it('fetches a websocket ticket before connecting', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ ticket: 'ticket-123', expiresAt: '2026-03-15T12:00:00Z' })
    });
    vi.stubGlobal('fetch', fetchMock as unknown as typeof fetch);
    setAccessTokenProvider(async () => 'access-token');
    (window as Window & { __API_UI_CONFIG__?: Record<string, unknown> }).__API_UI_CONFIG__ = {
      apiBaseUrl: '/api'
    };

    const queryClient = createQueryClient();
    render(
      <QueryClientProvider client={queryClient}>
        <Harness />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(MockWebSocket.instances).toHaveLength(1);
    });

    const request = fetchMock.mock.calls[0];
    expect(request[0]).toBe('/api/realtime/ticket');
    expect(request[1]?.method).toBe('POST');
    expect(request[1]?.headers).toBeInstanceOf(Headers);
    expect((request[1]?.headers as Headers).get('Authorization')).toBe('Bearer access-token');
    expect(MockWebSocket.instances[0]?.url).toBe(
      'ws://localhost:3000/api/ws/updates?ticket=ticket-123'
    );
  });

  it('invalidates the unified status view and metadata snapshot on metadata change events', async () => {
    const queryClient = createQueryClient();
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries');

    render(
      <QueryClientProvider client={queryClient}>
        <Harness />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(MockWebSocket.instances).toHaveLength(1);
    });
    const ws = MockWebSocket.instances[0];

    act(() => {
      ws.open();
    });

    await waitFor(() => {
      expect(ws.sent).toHaveLength(1);
    });

    act(() => {
      ws.emitJson({
        topic: 'system-health',
        data: {
          type: 'DOMAIN_METADATA_SNAPSHOT_CHANGED',
          payload: {
            reason: 'refresh',
            targets: [{ layer: 'bronze', domain: 'market' }],
            updatedAt: '2026-03-17T12:00:00Z'
          }
        }
      });
    });

    await waitFor(() => {
      const invalidatedKeys = invalidateSpy.mock.calls.map(([options]) =>
        JSON.stringify(options?.queryKey)
      );
      expect(invalidatedKeys).toEqual(
        expect.arrayContaining([
          JSON.stringify(queryKeys.systemStatusView()),
          JSON.stringify(queryKeys.systemHealth()),
          JSON.stringify(queryKeys.domainMetadataSnapshot('all', 'all'))
        ])
      );
    });
  });

  it('requests a fresh ticket before reconnecting', async () => {
    vi.useFakeTimers();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ ticket: 'ticket-1', expiresAt: '2026-03-15T12:00:00Z' })
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ ticket: 'ticket-2', expiresAt: '2026-03-15T12:00:05Z' })
      });
    vi.stubGlobal('fetch', fetchMock as unknown as typeof fetch);

    const queryClient = createQueryClient();
    render(
      <QueryClientProvider client={queryClient}>
        <Harness />
      </QueryClientProvider>
    );

    await act(async () => {
      await Promise.resolve();
    });
    expect(MockWebSocket.instances).toHaveLength(1);

    const first = MockWebSocket.instances[0];
    act(() => {
      first.open();
      first.close();
    });

    await act(async () => {
      vi.advanceTimersByTime(5000);
      await Promise.resolve();
    });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(MockWebSocket.instances).toHaveLength(2);

    expect(MockWebSocket.instances[1]?.url).toBe(
      'ws://localhost:3000/api/ws/updates?ticket=ticket-2'
    );
  });

  it('surfaces realtime as unavailable when ticket retrieval fails', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      text: async () => 'Unauthorized'
    });
    vi.stubGlobal('fetch', fetchMock as unknown as typeof fetch);

    const queryClient = createQueryClient();
    render(
      <QueryClientProvider client={queryClient}>
        <Harness />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(1);
      expect(mockToastError).toHaveBeenCalledWith('Realtime updates unavailable: Unauthorized');
    });
    expect(MockWebSocket.instances).toHaveLength(0);
  });
});
