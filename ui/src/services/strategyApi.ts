import { apiService } from '@/services/apiService';

export interface StrategyConfig {
    universe: string;
    rebalance: string;
    longOnly?: boolean;
    topN?: number;
    lookbackWindow?: number;
    holdingPeriod?: number;
    costModel?: string;
    [key: string]: unknown;
}

export interface Strategy {
    name: string;
    type: string;
    description?: string;
    updated_at?: string;
    config?: StrategyConfig;
}

export const strategyApi = {
    async listStrategies(signal?: AbortSignal): Promise<Strategy[]> {
        return apiService.request<Strategy[]>('/strategies', { signal });
    },

    async getStrategy(name: string, signal?: AbortSignal): Promise<StrategyConfig> {
        return apiService.request<StrategyConfig>(`/strategies/${encodeURIComponent(name)}`, { signal });
    },

    async saveStrategy(strategy: Strategy, signal?: AbortSignal): Promise<{ status: string; message: string }> {
        return apiService.request<{ status: string; message: string }>('/strategies', {
            method: 'POST',
            body: JSON.stringify(strategy),
            signal
        });
    }
};
