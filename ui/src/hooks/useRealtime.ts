import { useEffect, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { config } from '@/config';
import { backtestKeys } from '@/services/backtestHooks';

export function useRealtime() {
  const queryClient = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);

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
      console.log('[Realtime] Connecting to', wsHref);
      const ws = new WebSocket(wsHref);
      wsRef.current = ws;

      ws.onopen = () => {
        console.log('[Realtime] Connected');
      };

      ws.onmessage = (event) => {
        try {
          const message: unknown = JSON.parse(event.data);
          handleMessage(message);
        } catch (err) {
          console.error('[Realtime] Failed to parse message:', err);
        }
      };

      ws.onclose = () => {
        console.log('[Realtime] Disconnected. Reconnecting in 5s...');
        wsRef.current = null;
        setTimeout(connect, 5000);
      };

      ws.onerror = (err) => {
        console.error('[Realtime] Error:', err);
        ws.close();
      };
    }

    function handleMessage(message: unknown) {
      if (!message || typeof message !== 'object') return;
      const type = (message as { type?: unknown }).type;
      if (type !== 'RUN_UPDATE') return;

      const payload = (message as { payload?: unknown }).payload;
      console.log('[Realtime] Run update:', payload);

      if (payload && typeof payload === 'object') {
        const runId = (payload as { run_id?: unknown }).run_id;
        if (typeof runId === 'string' && runId) {
          void queryClient.invalidateQueries({ queryKey: backtestKeys.run(runId) });
        }
      }

      void queryClient.invalidateQueries({ queryKey: backtestKeys.runs() });
    }

    connect();

    return () => {
      if (wsRef.current) {
        // Prevent reconnect on unmount
        wsRef.current.onclose = null;
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [queryClient]);
}
