import { request } from '@/services/apiService';
import type {
  RegimeInputRow,
  RegimeModelDetailResponse,
  RegimeModelRevision,
  RegimeModelSummary,
  RegimeSnapshot,
  RegimeTransitionRow
} from '@/types/regime';

export type {
  CurveState,
  RegimeInputRow,
  RegimeModelDetailResponse,
  RegimeModelRevision,
  RegimeModelSummary,
  RegimeSnapshot,
  RegimeStatus,
  RegimeTransitionRow,
  TrendState
} from '@/types/regime';

export const regimeApi = {
  async getCurrent(
    params: {
      modelName?: string;
      modelVersion?: number;
    } = {}
  ): Promise<RegimeSnapshot> {
    return request<RegimeSnapshot>('/regimes/current', { params });
  },

  async getHistory(
    params: {
      modelName?: string;
      modelVersion?: number;
      startDate?: string;
      endDate?: string;
      limit?: number;
    } = {}
  ): Promise<{
    modelName: string;
    modelVersion?: number | null;
    rows: RegimeSnapshot[];
    limit: number;
  }> {
    return request<{
      modelName: string;
      modelVersion?: number | null;
      rows: RegimeSnapshot[];
      limit: number;
    }>('/regimes/history', { params });
  },

  async listModels(): Promise<{ models: RegimeModelSummary[] }> {
    return request<{ models: RegimeModelSummary[] }>('/regimes/models');
  },

  async getModel(modelName: string): Promise<RegimeModelDetailResponse> {
    return request<RegimeModelDetailResponse>(`/regimes/models/${encodeURIComponent(modelName)}`);
  },

  async createModel(payload: {
    name: string;
    description?: string;
    config: Record<string, unknown>;
  }): Promise<{ model: RegimeModelSummary; activeRevision?: RegimeModelRevision | null }> {
    return request<{ model: RegimeModelSummary; activeRevision?: RegimeModelRevision | null }>(
      '/regimes/models',
      {
        method: 'POST',
        body: JSON.stringify(payload)
      }
    );
  },

  async activateModel(
    modelName: string,
    payload: { version?: number }
  ): Promise<{
    model: string;
    activatedRevision: RegimeModelRevision;
    jobTrigger?: Record<string, unknown> | null;
  }> {
    return request<{
      model: string;
      activatedRevision: RegimeModelRevision;
      jobTrigger?: Record<string, unknown> | null;
    }>(`/regimes/models/${encodeURIComponent(modelName)}/activate`, {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  },

  async getDatasetRows(
    dataset: 'inputs' | 'history' | 'latest' | 'transitions',
    params: Record<string, string | number | boolean | undefined> = {}
  ) {
    if (dataset === 'inputs') {
      return request<RegimeInputRow[]>(`/data/gold/regime/${dataset}`, { params });
    }
    if (dataset === 'transitions') {
      return request<RegimeTransitionRow[]>(`/data/gold/regime/${dataset}`, { params });
    }
    return request<RegimeSnapshot[]>(`/data/gold/regime/${dataset}`, { params });
  }
};
