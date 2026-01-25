import { useQuery } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';

// Key Factory for consistent query keys
// Key Factory for consistent query keys
export const queryKeys = {
    // System & Data Health
    systemHealth: () => ['systemHealth'] as const,
    lineage: () => ['lineage'] as const,

    // High-level Strategy & Signals
    strategies: () => ['strategies'] as const,
    signals: () => ['signals'] as const,

    // Strategy Specific Details
    positions: (strategyId?: string) => ['positions', strategyId] as const,
    orders: (strategyId?: string) => ['orders', strategyId] as const,
    alerts: () => ['alerts'] as const,
    riskMetrics: (strategyId: string) => ['riskMetrics', strategyId] as const,
    executionMetrics: (strategyId: string) => ['executionMetrics', strategyId] as const,
    stressEvents: () => ['stressEvents'] as const,
};

/**
 * System & Health Queries
 */

export function useSystemHealthQuery() {
    return useQuery({
        queryKey: queryKeys.systemHealth(),
        queryFn: () => DataService.getSystemHealth(),
        refetchInterval: 10000,
    });
}

export function useLineageQuery() {
    return useQuery({
        queryKey: queryKeys.lineage(),
        queryFn: () => DataService.getLineage(),
        staleTime: 5 * 60 * 1000,
        refetchInterval: 60 * 1000,
    });
}

/**
 * Strategy & Signal Queries
 */

export function useStrategiesQuery() {
    return useQuery({
        queryKey: queryKeys.strategies(),
        queryFn: () => DataService.getStrategies(),
    });
}

export function useSignalsQuery() {
    return useQuery({
        queryKey: queryKeys.signals(),
        queryFn: () => DataService.getSignals(),
        refetchInterval: 10000,
    });
}

/**
 * Strategy Detail Queries
 */

export function usePositionsQuery(strategyId?: string) {
    return useQuery({
        queryKey: queryKeys.positions(strategyId),
        queryFn: () => DataService.getPositions(strategyId),
    });
}

export function useRiskMetricsQuery(strategyId: string) {
    return useQuery({
        queryKey: queryKeys.riskMetrics(strategyId),
        queryFn: () => DataService.getRiskMetrics(strategyId),
    });
}

export function useExecutionMetricsQuery(strategyId: string) {
    return useQuery({
        queryKey: queryKeys.executionMetrics(strategyId),
        queryFn: () => DataService.getExecutionMetrics(strategyId),
    });
}
