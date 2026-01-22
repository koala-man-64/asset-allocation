import { useQuery } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import { useUIStore } from '@/stores/useUIStore';
import { useEffect } from 'react';

// Key Factory for consistent query keys
export const queryKeys = {
    strategies: () => ['strategies'] as const,
    systemHealth: () => ['systemHealth'] as const,
    liveSystemHealth: () => ['liveSystemHealth'] as const,
    signals: () => ['signals'] as const,
    stressEvents: () => ['stressEvents'] as const,
    positions: (strategyId?: string) => ['positions', strategyId] as const,
    orders: (strategyId?: string) => ['orders', strategyId] as const,
    alerts: () => ['alerts'] as const,
    riskMetrics: (strategyId: string) => ['riskMetrics', strategyId] as const,
    executionMetrics: (strategyId: string) => ['executionMetrics', strategyId] as const,
};

/**
 * Hook to sync DataService mode with Zustand dataSource state
 */
export function useDataSync() {
    const dataSource = useUIStore((s) => s.dataSource);

    useEffect(() => {
        DataService.setMode(dataSource);
    }, [dataSource]);
}

/**
 * Standard Query Hooks
 */

export function useStrategiesQuery() {
    const dataSource = useUIStore((s) => s.dataSource);
    return useQuery({
        queryKey: [...queryKeys.strategies(), dataSource],
        queryFn: () => DataService.getStrategies(),
    });
}

export function useSystemHealthQuery() {
    const dataSource = useUIStore((s) => s.dataSource);
    return useQuery({
        queryKey: [...queryKeys.systemHealth(), dataSource],
        queryFn: () => DataService.getSystemHealth(),
        refetchInterval: 30000, // Auto refresh health every 30s
    });
}

export function useLiveSystemHealthQuery() {
    // Always fetches from live API, bypassing global dataSource
    return useQuery({
        queryKey: queryKeys.liveSystemHealth(),
        queryFn: () => DataService.getLiveSystemHealth(),
        refetchInterval: 10000, // Faster refresh for live monitor
    });
}

export function useSignalsQuery() {
    const dataSource = useUIStore((s) => s.dataSource);
    return useQuery({
        queryKey: [...queryKeys.signals(), dataSource],
        queryFn: () => DataService.getSignals(),
    });
}

export function usePositionsQuery(strategyId?: string) {
    const dataSource = useUIStore((s) => s.dataSource);
    return useQuery({
        queryKey: [...queryKeys.positions(strategyId), dataSource],
        queryFn: () => DataService.getPositions(strategyId),
    });
}

export function useRiskMetricsQuery(strategyId: string) {
    const dataSource = useUIStore((s) => s.dataSource);
    return useQuery({
        queryKey: [...queryKeys.riskMetrics(strategyId), dataSource],
        queryFn: () => DataService.getRiskMetrics(strategyId),
    });
}

export function useExecutionMetricsQuery(strategyId: string) {
    const dataSource = useUIStore((s) => s.dataSource);
    return useQuery({
        queryKey: [...queryKeys.executionMetrics(strategyId), dataSource],
        queryFn: () => DataService.getExecutionMetrics(strategyId),
    });
}
