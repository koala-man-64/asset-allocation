import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { config } from '@/config';
import { backtestKeys } from '@/services/backtestHooks';
import { queryKeys } from '@/hooks/useDataQueries';

const SUBSCRIPTION_TOPICS = [
  'backtests',
  'system-health',
  'jobs',
  'container-apps',
  'alerts',
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object';
}

export function useRealtime() {
  const queryClient = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);
  const keepAliveRef = useRef<number | null>(null);
  const reconnectTimeoutRef = useRef<number | null>(null);

  useEffect(() => {
    // `config.apiBaseUrl` is the API base (expected to include `/api`).
    // The websocket endpoint is mounted at `/api/ws/updates`, so append `/ws/updates`.
    const httpBase = config.apiBaseUrl.replace(/\/+$/, '');
    const wsPath = `${httpBase}/ws/updates`;
    const wsUrl = new URL(wsPath, window.location.origin);
    wsUrl.protocol = wsUrl.protocol === 'https:' ? 'wss:' : 'ws:';

    function connect() {
      if (wsRef.current?.readyState === WebSocket.OPEN) return;

      const wsHref = wsUrl.toString();
      const ws = new WebSocket(wsHref);
      wsRef.current = ws;

      ws.onopen = () => {
        // Subscribe to server topics so publish events are delivered.
        ws.send(JSON.stringify({ action: 'subscribe', topics: [...SUBSCRIPTION_TOPICS] }));

        // Keep the connection active behind ingress/load-balancers.
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

      ws.onclose = () => {
        if (keepAliveRef.current) {
          window.clearInterval(keepAliveRef.current);
          keepAliveRef.current = null;
        }
        wsRef.current = null;
        if (reconnectTimeoutRef.current) {
          window.clearTimeout(reconnectTimeoutRef.current);
        }
        reconnectTimeoutRef.current = window.setTimeout(connect, 5000);
      };

      ws.onerror = (err) => {
        console.error('[Realtime] Error:', err);
        ws.close();
      };
    }

    function handleMessage(message: unknown) {
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

      // Backward-compat with old event format expected by the UI.
      if (!eventType && typeof envelope.type === 'string') {
        eventType = envelope.type;
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
        topic === 'alerts' ||
        eventType === 'SYSTEM_HEALTH_UPDATE' ||
        eventType === 'JOB_STATE_CHANGED' ||
        eventType === 'CONTAINER_APP_STATE_CHANGED' ||
        eventType === 'ALERT_STATE_CHANGED';

      if (shouldRefreshSystem) {
        void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
        void queryClient.invalidateQueries({ queryKey: CONTAINER_APPS_QUERY_KEY });
      }

      if (topic === 'runtime-config' || eventType === 'RUNTIME_CONFIG_CHANGED') {
        void queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfigCatalog() });
        void queryClient.invalidateQueries({ queryKey: ['runtimeConfig'] });
      }

      if (topic === 'debug-symbols' || eventType === 'DEBUG_SYMBOLS_CHANGED') {
        void queryClient.invalidateQueries({ queryKey: queryKeys.debugSymbols() });
      }
    }

    connect();

    return () => {
      if (keepAliveRef.current) {
        window.clearInterval(keepAliveRef.current);
        keepAliveRef.current = null;
      }
      if (reconnectTimeoutRef.current) {
        window.clearTimeout(reconnectTimeoutRef.current);
        reconnectTimeoutRef.current = null;
      }
      if (wsRef.current) {
        // Prevent reconnect on unmount
        wsRef.current.onclose = null;
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [queryClient]);
}
