import { useQuery } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import type { DomainMetadata, SystemHealth } from '@/types/strategy';

function isApiNotFoundError(error: unknown): boolean {
  return error instanceof Error && error.message.includes('API Error: 404');
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

export function useSystemHealthQuery() {
  return useQuery<SystemHealth>({
    queryKey: queryKeys.systemHealth(),
    queryFn: async () => {
      console.info('[useSystemHealthQuery] fetch start');
      try {
        const data = await DataService.getSystemHealth();
        console.info('[useSystemHealthQuery] fetch success', {
          overall: data?.overall,
          layers: data?.dataLayers?.length ?? 0,
          alerts: data?.alerts?.length ?? 0
        });
        return data;
      } catch (error) {
        console.error('[useSystemHealthQuery] fetch error', error);
        throw error;
      }
    },
    retry: (failureCount, error) => (isApiNotFoundError(error) ? false : failureCount < 3),
    refetchInterval: (query) => (isApiNotFoundError(query.state.error) ? false : 10000),
    onSuccess: (data) => {
      console.info('[Query] systemHealth success', {
        overall: data.overall,
        alerts: data.alerts?.length ?? 0
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
      console.info('[Query] lineage fetch');
      return DataService.getLineage();
    },
    staleTime: 5 * 60 * 1000,
    refetchInterval: 60 * 1000,
    onSuccess: (data) => {
      const impacts = (data as { impactsByDomain?: unknown })?.impactsByDomain;
      console.info('[Query] lineage success', {
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
