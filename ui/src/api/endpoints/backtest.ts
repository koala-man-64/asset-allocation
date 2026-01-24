import { apiClient } from '@/api/client';
import { PaginatedResponse } from '@/api/types';
import type { StrategyRun } from '@/types/strategy';

export const backtestApi = {
    /**
     * List all backtest runs
     */
    listRuns: async (limit: number = 50, offset: number = 0): Promise<PaginatedResponse<StrategyRun>> => {
        return apiClient.get<PaginatedResponse<StrategyRun>>(`/backtests?limit=${limit}&offset=${offset}`);
    },

    /**
     * Get details for a specific run
     */
    getRun: async (runId: string): Promise<StrategyRun> => {
        return apiClient.get<StrategyRun>(`/backtests/${runId}`);
    },

    /**
     * Submit a new backtest job
     */
    submitRun: async (config: unknown): Promise<{ runId: string }> => {
        return apiClient.post<{ runId: string }>('/backtests', config);
    }
};
