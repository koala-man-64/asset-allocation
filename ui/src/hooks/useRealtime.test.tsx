import React from 'react';
import { render, waitFor, act } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useRealtime } from './useRealtime';
import {
  REALTIME_SUBSCRIBE_EVENT,
  addConsoleLogStreamListener
} from '@/services/realtimeBus';

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
  });

  afterEach(() => {
    vi.unstubAllGlobals();
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

    expect(MockWebSocket.instances).toHaveLength(1);
    const ws = MockWebSocket.instances[0];

    act(() => {
      ws.open();
    });

    await waitFor(() => {
      expect(ws.sent).toHaveLength(1);
    });
    expect(JSON.parse(ws.sent[0])).toEqual({
      action: 'subscribe',
      topics: ['backtests', 'system-health', 'jobs', 'container-apps', 'runtime-config', 'debug-symbols']
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
              executionName: 'bronze-market-job-exec-001'
            })
          ]
        })
      );
    });

    unsubscribe();
    view.unmount();
  });
});
