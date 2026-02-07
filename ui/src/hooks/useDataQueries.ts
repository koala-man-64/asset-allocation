import { useQuery, type UseQueryResult } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import type { DomainMetadata, SystemHealth } from '@/types/strategy';
import type { RequestMeta } from '@/services/apiService';

function isApiNotFoundError(error: unknown): boolean {
  return error instanceof Error && error.message.includes('API Error: 404');
}

const ENABLE_QUERY_LOGS =
  import.meta.env.DEV ||
  ['1', 'true', 'yes', 'y', 'on'].includes(
    String(import.meta.env.VITE_DEBUG_API ?? '')
      .trim()
      .toLowerCase()
  );

function queryInfo(message: string, meta: Record<string, unknown> = {}): void {
  if (!ENABLE_QUERY_LOGS) return;
  if (Object.keys(meta).length > 0) {
    console.info(message, meta);
    return;
  }
  console.info(message);
}

let lastSystemHealthMeta: RequestMeta | null = null;

export function getLastSystemHealthMeta(): RequestMeta | null {
  return lastSystemHealthMeta;
}

function systemHealthRefetchInterval(query: {
  state: { error: unknown; data: unknown };
}): false | number {
  if (isApiNotFoundError(query.state.error)) {
    return false;
  }
  const payload = query.state.data as SystemHealth | undefined;
  const baseMs =
    payload?.overall === 'critical' ? 10_000 : payload?.overall === 'degraded' ? 15_000 : 30_000;
  const jitter = Math.round(baseMs * 0.1 * Math.random());
  return baseMs + jitter;
}

// Key Factory for consistent query keys
// Key Factory for consistent query keys
export const queryKeys = {
  // System & Data Health
  systemHealth: () => ['systemHealth'] as const,
  lineage: () => ['lineage'] as const,
  debugSymbols: () => ['debugSymbols'] as const,
  runtimeConfigCatalog: () => ['runtimeConfigCatalog'] as const,
  runtimeConfig: (scope: string) => ['runtimeConfig', scope] as const,
  domainMetadata: (layer: string, domain: string) => ['domainMetadata', layer, domain] as const
};

/**
 * System & Health Queries
 */

export function useSystemHealthQuery(): UseQueryResult<SystemHealth> {
  return useQuery<SystemHealth>({
    queryKey: queryKeys.systemHealth(),
    queryFn: async () => {
      queryInfo('[useSystemHealthQuery] fetch start');
      try {
        const response = await DataService.getSystemHealthWithMeta();
        lastSystemHealthMeta = response.meta;
        queryInfo('[useSystemHealthQuery] fetch success', {
          overall: response.data?.overall,
          layers: response.data?.dataLayers?.length ?? 0,
          alerts: response.data?.alerts?.length ?? 0,
          durationMs: response.meta.durationMs,
          cacheHint: response.meta.cacheHint,
          stale: response.meta.stale,
          requestId: response.meta.requestId
        });
        return response.data;
      } catch (error) {
        lastSystemHealthMeta = null;
        console.error('[useSystemHealthQuery] fetch error', error);
        throw error;
      }
    },
    retry: (failureCount, error) => (isApiNotFoundError(error) ? false : failureCount < 3),
    refetchInterval: systemHealthRefetchInterval,
    onSuccess: (data) => {
      queryInfo('[Query] systemHealth success', {
        overall: data.overall,
        alerts: data.alerts?.length ?? 0,
        requestId: lastSystemHealthMeta?.requestId,
        cacheHint: lastSystemHealthMeta?.cacheHint,
        stale: lastSystemHealthMeta?.stale
      });
    },
    onError: (err) => {
      console.error('[Query] systemHealth error', {
        error: err instanceof Error ? err.message : String(err)
      });
    }
  });
}

export function useLineageQuery() {
  return useQuery({
    queryKey: queryKeys.lineage(),
    queryFn: async () => {
      queryInfo('[Query] lineage fetch');
      return DataService.getLineage();
    },
    staleTime: 5 * 60 * 1000,
    refetchInterval: 60 * 1000,
    onSuccess: (data) => {
      const impacts = (data as { impactsByDomain?: unknown })?.impactsByDomain;
      queryInfo('[Query] lineage success', {
        domains: impacts && typeof impacts === 'object' ? Object.keys(impacts as object).length : 0
      });
    },
    onError: (err) => {
      console.error('[Query] lineage error', {
        error: err instanceof Error ? err.message : String(err)
      });
    }
  });
}

export function useDebugSymbolsQuery() {
  return useQuery({
    queryKey: queryKeys.debugSymbols(),
    queryFn: async () => {
      return DataService.getDebugSymbols();
    },
    staleTime: 30 * 1000,
    refetchInterval: 60 * 1000
  });
}

export function useRuntimeConfigCatalogQuery() {
  return useQuery({
    queryKey: queryKeys.runtimeConfigCatalog(),
    queryFn: async () => {
      return DataService.getRuntimeConfigCatalog();
    },
    staleTime: 5 * 60 * 1000,
    refetchInterval: false
  });
}

export function useRuntimeConfigQuery(scope: string = 'global') {
  return useQuery({
    queryKey: queryKeys.runtimeConfig(scope),
    queryFn: async () => {
      return DataService.getRuntimeConfig(scope);
    },
    staleTime: 30 * 1000,
    refetchInterval: 60 * 1000
  });
}

export function useDomainMetadataQuery(
  layer: 'bronze' | 'silver' | 'gold' | 'platinum' | undefined,
  domain: string | undefined,
  options: { enabled?: boolean } = {}
) {
  return useQuery<DomainMetadata>({
    queryKey: queryKeys.domainMetadata(String(layer || ''), String(domain || '')),
    queryFn: async () => {
      if (!layer || !domain) {
        throw new Error('Layer and domain are required.');
      }
      return DataService.getDomainMetadata(layer, domain);
    },
    enabled: Boolean(layer && domain) && options.enabled !== false,
    staleTime: 5 * 60 * 1000,
    refetchInterval: false
  });
}
