import { useEffect } from 'react';
import { useQuery, type UseQueryResult } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import type { DomainMetadata, SystemHealth } from '@/types/strategy';
import type { RequestMeta } from '@/services/apiService';

function isApiNotFoundError(error: unknown): boolean {
  return error instanceof Error && error.message.includes('API Error: 404');
}

let lastSystemHealthMeta: RequestMeta | null = null;

export function getLastSystemHealthMeta(): RequestMeta | null {
  return lastSystemHealthMeta;
}

export interface UseSystemHealthQueryOptions {
  autoRefresh?: boolean;
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
export const queryKeys = {
  // System & Data Health
  systemHealth: () => ['systemHealth'] as const,
  lineage: () => ['lineage'] as const,
  debugSymbols: () => ['debugSymbols'] as const,
  runtimeConfigCatalog: () => ['runtimeConfigCatalog'] as const,
  runtimeConfig: (scope: string) => ['runtimeConfig', scope] as const,
  domainMetadata: (layer: string, domain: string) => ['domainMetadata', layer, domain] as const,
  domainMetadataSnapshot: (layers: string = 'all', domains: string = 'all') =>
    ['domainMetadataSnapshot', layers, domains] as const
};

/**
 * System & Health Queries
 */

export function useSystemHealthQuery(
  options: UseSystemHealthQueryOptions = {}
): UseQueryResult<SystemHealth> {
  const autoRefresh = options.autoRefresh ?? false;
  const query = useQuery<SystemHealth>({
    queryKey: queryKeys.systemHealth(),
    queryFn: async () => {
      try {
        const response = await DataService.getSystemHealthWithMeta();
        lastSystemHealthMeta = response.meta;
        return response.data;
      } catch (error) {
        lastSystemHealthMeta = null;
        console.error('[useSystemHealthQuery] fetch error', error);
        throw error;
      }
    },
    retry: (failureCount, error) => (isApiNotFoundError(error) ? false : failureCount < 3),
    refetchInterval: autoRefresh ? systemHealthRefetchInterval : false
  });

  useEffect(() => {
    if (query.error) {
      console.error('[Query] systemHealth error', {
        error: query.error instanceof Error ? query.error.message : String(query.error)
      });
    }
  }, [query.error]);

  return query;
}

export function useLineageQuery() {
  const query = useQuery({
    queryKey: queryKeys.lineage(),
    queryFn: async () => {
      return DataService.getLineage();
    },
    staleTime: 5 * 60 * 1000,
    refetchInterval: false
  });

  useEffect(() => {
    if (query.error) {
      console.error('[Query] lineage error', {
        error: query.error instanceof Error ? query.error.message : String(query.error)
      });
    }
  }, [query.error]);

  return query;
}

export function useDebugSymbolsQuery() {
  return useQuery({
    queryKey: queryKeys.debugSymbols(),
    queryFn: async () => {
      return DataService.getDebugSymbols();
    },
    staleTime: 30 * 1000,
    refetchInterval: false
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
    refetchInterval: false
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
