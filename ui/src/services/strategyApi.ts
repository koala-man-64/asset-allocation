import { request } from '@/services/apiService';
import type { StrategyConfig, StrategyDetail, StrategySummary } from '@/types/strategy';

export type { ExitRule, ExitRuleType, IntrabarConflictPolicy, StrategyConfig, StrategyDetail, StrategySummary } from '@/types/strategy';

export const strategyApi = {
  async listStrategies(signal?: AbortSignal): Promise<StrategySummary[]> {
    return request<StrategySummary[]>('/strategies', { signal });
  },

  async getStrategy(name: string, signal?: AbortSignal): Promise<StrategyConfig> {
    return request<StrategyConfig>(`/strategies/${encodeURIComponent(name)}`, { signal });
  },

  async getStrategyDetail(name: string, signal?: AbortSignal): Promise<StrategyDetail> {
    return request<StrategyDetail>(`/strategies/${encodeURIComponent(name)}/detail`, { signal });
  },

  async saveStrategy(
    strategy: StrategyDetail,
    signal?: AbortSignal
  ): Promise<{ status: string; message: string }> {
    return request<{ status: string; message: string }>('/strategies', {
      method: 'POST',
      body: JSON.stringify(strategy),
      signal
    });
  }
};
