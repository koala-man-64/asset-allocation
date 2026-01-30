import { useQuery } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import type { SystemHealth } from '@/types/strategy';

// Key Factory for consistent query keys
// Key Factory for consistent query keys
export const queryKeys = {
    // System & Data Health
    systemHealth: () => ['systemHealth'] as const,
    lineage: () => ['lineage'] as const,
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
                    alerts: data?.alerts?.length ?? 0,
                });
                return data;
            } catch (error) {
                console.error('[useSystemHealthQuery] fetch error', error);
                throw error;
            }
        },
        refetchInterval: 10000,
        onSuccess: (data) => {
            console.info('[Query] systemHealth success', {
                overall: data.overall,
                alerts: data.alerts?.length ?? 0,
            });
        },
        onError: (err) => {
            console.error('[Query] systemHealth error', {
                error: err instanceof Error ? err.message : String(err),
            });
        },
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
                domains: impacts && typeof impacts === 'object' ? Object.keys(impacts as object).length : 0,
            });
        },
        onError: (err) => {
            console.error('[Query] lineage error', {
                error: err instanceof Error ? err.message : String(err),
            });
        },
    });
}
