import { request } from '@/services/apiService';
import type {
  RankingCatalogResponse,
  RankingMaterializationSummary,
  RankingPreviewResponse,
  RankingSchemaConfig,
  RankingSchemaDetail,
  RankingSchemaSummary
} from '@/types/strategy';

export const rankingApi = {
  async listRankingSchemas(signal?: AbortSignal): Promise<RankingSchemaSummary[]> {
    return request<RankingSchemaSummary[]>('/rankings', { signal });
  },

  async getRankingSchemaDetail(name: string, signal?: AbortSignal): Promise<RankingSchemaDetail> {
    return request<RankingSchemaDetail>(`/rankings/${encodeURIComponent(name)}/detail`, { signal });
  },

  async getRankingCatalog(signal?: AbortSignal): Promise<RankingCatalogResponse> {
    return request<RankingCatalogResponse>('/rankings/catalog', { signal });
  },

  async saveRankingSchema(
    payload: {
      name: string;
      description?: string;
      config: RankingSchemaConfig;
    },
    signal?: AbortSignal
  ): Promise<{ status: string; message: string; version: number }> {
    return request<{ status: string; message: string; version: number }>('/rankings', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
  },

  async deleteRankingSchema(
    name: string,
    signal?: AbortSignal
  ): Promise<{ status: string; message: string }> {
    return request<{ status: string; message: string }>(`/rankings/${encodeURIComponent(name)}`, {
      method: 'DELETE',
      signal
    });
  },

  async previewRanking(
    payload: {
      strategyName: string;
      asOfDate: string;
      limit?: number;
      schemaName?: string;
      schema?: RankingSchemaConfig;
    },
    signal?: AbortSignal
  ): Promise<RankingPreviewResponse> {
    return request<RankingPreviewResponse>('/rankings/preview', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
  },

  async materializeRankings(
    payload: {
      strategyName: string;
      startDate?: string;
      endDate?: string;
    },
    signal?: AbortSignal
  ): Promise<RankingMaterializationSummary> {
    return request<RankingMaterializationSummary>('/rankings/materialize', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
  }
};
