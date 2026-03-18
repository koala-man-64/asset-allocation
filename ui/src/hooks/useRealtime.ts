import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';

import { config } from '@/config';
import { queryKeys } from '@/hooks/useDataQueries';
import { appendAuthHeaders } from '@/services/authTransport';
import { backtestKeys } from '@/services/backtestHooks';
import {
  CONSOLE_LOG_STREAM_EVENT_TYPE,
  REALTIME_SUBSCRIBE_EVENT,
  REALTIME_UNSUBSCRIBE_EVENT,
  emitConsoleLogStream
} from '@/services/realtimeBus';

const SUBSCRIPTION_TOPICS = [
  'backtests',
  'system-health',
  'jobs',
  'container-apps',
  'runtime-config',
  'debug-symbols'
] as const;

const CONTAINER_APPS_QUERY_KEY = ['system', 'container-apps'] as const;

type RealtimeEvent = {
  type?: unknown;
  payload?: unknown;
};

type RealtimeEnvelope = {
  topic?: unknown;
  data?: unknown;
  type?: unknown;
  payload?: unknown;
};

type TopicSubscriptionDetail = {
  topics?: unknown;
};

type RealtimeTicketResponse = {
  ticket?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object';
}

export function useRealtime() {
  const queryClient = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);
  const keepAliveRef = useRef<number | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);
  const dynamicTopicCountsRef = useRef<Map<string, number>>(new Map());
  const connectInFlightRef = useRef(false);
  const realtimeUnavailableRef = useRef(false);

  useEffect(() => {
    const httpBase = config.apiBaseUrl.replace(/\/+$/, '');
    const wsPath = `${httpBase}/ws/updates`;
    const wsUrl = new URL(wsPath, window.location.origin);
    wsUrl.protocol = wsUrl.protocol === 'https:' ? 'wss:' : 'ws:';

    function normalizeTopics(value: unknown): string[] {
      if (!Array.isArray(value)) return [];
      return value.map((topic) => String(topic || '').trim()).filter((topic) => topic.length > 0);
    }

    function getDynamicTopics(): string[] {
      return Array.from(dynamicTopicCountsRef.current.entries())
        .filter(([, count]) => count > 0)
        .map(([topic]) => topic);
    }

    function sendSubscription(action: 'subscribe' | 'unsubscribe', topics: string[]): void {
      const filtered = normalizeTopics(topics);
      if (filtered.length === 0 || wsRef.current?.readyState !== WebSocket.OPEN) {
        return;
      }
      wsRef.current.send(JSON.stringify({ action, topics: filtered }));
    }

    function updateDynamicSubscriptions(
      action: 'subscribe' | 'unsubscribe',
      topics: string[]
    ): void {
      const filtered = normalizeTopics(topics);
      if (filtered.length === 0) return;

      const changed: string[] = [];
      filtered.forEach((topic) => {
        const currentCount = dynamicTopicCountsRef.current.get(topic) ?? 0;
        if (action === 'subscribe') {
          dynamicTopicCountsRef.current.set(topic, currentCount + 1);
          if (currentCount === 0) {
            changed.push(topic);
          }
          return;
        }

        if (currentCount <= 1) {
          dynamicTopicCountsRef.current.delete(topic);
          if (currentCount > 0) {
            changed.push(topic);
          }
          return;
        }

        dynamicTopicCountsRef.current.set(topic, currentCount - 1);
      });

      sendSubscription(action, changed);
    }

    function handleTopicSubscriptionRequest(event: Event): void {
      const customEvent = event as CustomEvent<TopicSubscriptionDetail>;
      updateDynamicSubscriptions('subscribe', normalizeTopics(customEvent.detail?.topics));
    }

    function handleTopicUnsubscriptionRequest(event: Event): void {
      const customEvent = event as CustomEvent<TopicSubscriptionDetail>;
      updateDynamicSubscriptions('unsubscribe', normalizeTopics(customEvent.detail?.topics));
    }

    function markRealtimeUnavailable(message: string): void {
      if (realtimeUnavailableRef.current) {
        return;
      }
      realtimeUnavailableRef.current = true;
      toast.error(message);
    }

    function scheduleReconnect(): void {
      if (reconnectTimeoutRef.current) {
        window.clearTimeout(reconnectTimeoutRef.current);
      }
      reconnectTimeoutRef.current = window.setTimeout(() => {
        void connect();
      }, 5000);
    }

    async function fetchRealtimeTicket(): Promise<string | null> {
      const headers = await appendAuthHeaders();
      const response = await fetch(`${httpBase}/realtime/ticket`, {
        method: 'POST',
        headers,
        cache: 'no-store'
      });

      if (!response.ok) {
        const message = await response.text();
        throw new Error(message || `Realtime ticket request failed (${response.status})`);
      }

      const payload = (await response.json()) as RealtimeTicketResponse;
      const ticket = typeof payload.ticket === 'string' ? payload.ticket.trim() : '';
      if (!ticket) {
        throw new Error('Realtime ticket response was missing a ticket.');
      }
      return ticket;
    }

    async function connect(): Promise<void> {
      if (connectInFlightRef.current) return;
      if (
        wsRef.current?.readyState === WebSocket.OPEN ||
        wsRef.current?.readyState === WebSocket.CONNECTING
      ) {
        return;
      }

      connectInFlightRef.current = true;
      try {
        let wsHref = wsUrl.toString();
        const ticket = await fetchRealtimeTicket();
        const authedUrl = new URL(wsHref);
        authedUrl.searchParams.set('ticket', String(ticket));
        wsHref = authedUrl.toString();

        const ws = new WebSocket(wsHref);
        wsRef.current = ws;

        ws.onopen = () => {
          connectInFlightRef.current = false;
          realtimeUnavailableRef.current = false;

          const topics = [...SUBSCRIPTION_TOPICS, ...getDynamicTopics()];
          sendSubscription('subscribe', topics);

          if (keepAliveRef.current) {
            window.clearInterval(keepAliveRef.current);
          }
          keepAliveRef.current = window.setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send('ping');
            }
          }, 30_000);
        };

        ws.onmessage = (event) => {
          if (event.data === 'pong') return;
          try {
            const message: unknown = JSON.parse(event.data);
            handleMessage(message);
          } catch (err) {
            console.error('[Realtime] Failed to parse message:', err);
          }
        };

        ws.onclose = (event) => {
          connectInFlightRef.current = false;
          if (keepAliveRef.current) {
            window.clearInterval(keepAliveRef.current);
            keepAliveRef.current = null;
          }
          wsRef.current = null;

          if (event.code === 4401) {
            markRealtimeUnavailable(
              'Realtime updates unavailable: websocket authentication was rejected.'
            );
          }

          scheduleReconnect();
        };

        ws.onerror = (err) => {
          console.error('[Realtime] Error:', err);
          ws.close();
        };
      } catch (err) {
        connectInFlightRef.current = false;
        wsRef.current = null;
        console.error('[Realtime] Ticket request failed:', err);
        const message = err instanceof Error ? err.message : 'Realtime authentication failed.';
        markRealtimeUnavailable(`Realtime updates unavailable: ${message}`);
        scheduleReconnect();
      }
    }

    function handleMessage(message: unknown): void {
      if (!isRecord(message)) return;

      let topic: string | null = null;
      let eventType: string | null = null;
      let payload: unknown = null;

      const envelope = message as RealtimeEnvelope;
      if (typeof envelope.topic === 'string') {
        topic = envelope.topic;
      }

      if (isRecord(envelope.data)) {
        const event = envelope.data as RealtimeEvent;
        if (typeof event.type === 'string') {
          eventType = event.type;
          payload = event.payload;
        } else {
          payload = envelope.data;
        }
      } else if (typeof envelope.type === 'string') {
        eventType = envelope.type;
        payload = envelope.payload;
      } else {
        payload = envelope.data;
      }

      if (!eventType && typeof envelope.type === 'string') {
        eventType = envelope.type;
      }

      if (eventType === CONSOLE_LOG_STREAM_EVENT_TYPE && topic && isRecord(payload)) {
        const resourceType =
          payload.resourceType === 'job' || payload.resourceType === 'container-app'
            ? payload.resourceType
            : null;
        const resourceName = typeof payload.resourceName === 'string' ? payload.resourceName : null;
        const lines = Array.isArray(payload.lines)
          ? payload.lines.filter((line): line is Record<string, unknown> => isRecord(line))
          : [];

        if (resourceType && resourceName) {
          emitConsoleLogStream({
            topic,
            resourceType,
            resourceName,
            lines: lines.map((line) => ({
              id: typeof line.id === 'string' ? line.id : '',
              timestamp: typeof line.timestamp === 'string' ? line.timestamp : undefined,
              stream_s: typeof line.stream_s === 'string' ? line.stream_s : null,
              message: typeof line.message === 'string' ? line.message : '',
              executionName: typeof line.executionName === 'string' ? line.executionName : null
            })),
            polledAt: typeof payload.polledAt === 'string' ? payload.polledAt : undefined
          });
        }
        return;
      }

      if (eventType === 'RUN_UPDATE') {
        if (isRecord(payload)) {
          const runId = payload.run_id;
          if (typeof runId === 'string' && runId) {
            void queryClient.invalidateQueries({ queryKey: backtestKeys.run(runId) });
          }
        }

        void queryClient.invalidateQueries({ queryKey: backtestKeys.runs() });
        return;
      }

      const shouldRefreshSystem =
        topic === 'system-health' ||
        topic === 'jobs' ||
        topic === 'container-apps' ||
        eventType === 'SYSTEM_HEALTH_UPDATE' ||
        eventType === 'JOB_STATE_CHANGED' ||
        eventType === 'CONTAINER_APP_STATE_CHANGED' ||
        eventType === 'DOMAIN_METADATA_SNAPSHOT_CHANGED';

      if (shouldRefreshSystem) {
        void queryClient.invalidateQueries({ queryKey: queryKeys.systemStatusView() });
        void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
        void queryClient.invalidateQueries({ queryKey: CONTAINER_APPS_QUERY_KEY });
      }

      if (topic === 'system-health' || eventType === 'DOMAIN_METADATA_SNAPSHOT_CHANGED') {
        void queryClient.invalidateQueries({
          queryKey: queryKeys.domainMetadataSnapshot('all', 'all')
        });
      }

      if (topic === 'runtime-config' || eventType === 'RUNTIME_CONFIG_CHANGED') {
        void queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfigCatalog() });
        void queryClient.invalidateQueries({ queryKey: ['runtimeConfig'] });
      }

      if (topic === 'debug-symbols' || eventType === 'DEBUG_SYMBOLS_CHANGED') {
        void queryClient.invalidateQueries({ queryKey: queryKeys.debugSymbols() });
      }
    }

    window.addEventListener(REALTIME_SUBSCRIBE_EVENT, handleTopicSubscriptionRequest);
    window.addEventListener(REALTIME_UNSUBSCRIBE_EVENT, handleTopicUnsubscriptionRequest);
    void connect();

    return () => {
      window.removeEventListener(REALTIME_SUBSCRIBE_EVENT, handleTopicSubscriptionRequest);
      window.removeEventListener(REALTIME_UNSUBSCRIBE_EVENT, handleTopicUnsubscriptionRequest);
      if (keepAliveRef.current) {
        window.clearInterval(keepAliveRef.current);
        keepAliveRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        window.clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      connectInFlightRef.current = false;
      if (wsRef.current) {
        wsRef.current.onclose = null;
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [queryClient]);
}
