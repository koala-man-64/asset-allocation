import { request } from '@/services/apiService';
import type {
  StrategyConfig,
  StrategyDetail,
  StrategySummary,
  UniverseCatalogResponse,
  UniverseDefinition,
  UniversePreviewResponse
} from '@/types/strategy';

export type {
  ExitRule,
  ExitRuleType,
  IntrabarConflictPolicy,
  RegimeBlockedAction,
  RegimeCode,
  RegimePolicy,
  StrategyConfig,
  StrategyDetail,
  StrategySummary,
  TargetGrossExposureByRegime,
  UniverseCatalogResponse,
  UniverseCondition,
  UniverseConditionOperator,
  UniverseDefinition,
  UniverseGroup,
  UniverseGroupOperator,
  UniverseNode,
  UniversePreviewResponse,
  UniverseValue,
  UniverseValueKind
} from '@/types/strategy';

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

  async getUniverseCatalog(signal?: AbortSignal): Promise<UniverseCatalogResponse> {
    return request<UniverseCatalogResponse>('/universes/catalog', { signal });
  },

  async previewUniverse(
    payload: {
      universe: UniverseDefinition;
      sampleLimit?: number;
    },
    signal?: AbortSignal
  ): Promise<UniversePreviewResponse> {
    return request<UniversePreviewResponse>('/universes/preview', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
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
  },

  async deleteStrategy(
    name: string,
    signal?: AbortSignal
  ): Promise<{ status: string; message: string }> {
    return request<{ status: string; message: string }>(`/strategies/${encodeURIComponent(name)}`, {
      method: 'DELETE',
      signal
    });
  }
};
