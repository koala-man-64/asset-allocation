import { request } from '@/services/apiService';
import type {
  UniverseCatalogResponse,
  UniverseConfigDetail,
  UniverseConfigSummary,
  UniverseDefinition,
  UniversePreviewResponse
} from '@/types/strategy';

export const universeApi = {
  async listUniverseConfigs(signal?: AbortSignal): Promise<UniverseConfigSummary[]> {
    return request<UniverseConfigSummary[]>('/universes', { signal });
  },

  async getUniverseConfigDetail(name: string, signal?: AbortSignal): Promise<UniverseConfigDetail> {
    return request<UniverseConfigDetail>(`/universes/${encodeURIComponent(name)}/detail`, {
      signal
    });
  },

  async saveUniverseConfig(
    payload: {
      name: string;
      description?: string;
      config: UniverseDefinition;
    },
    signal?: AbortSignal
  ): Promise<{ status: string; message: string; version: number }> {
    return request<{ status: string; message: string; version: number }>('/universes', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
  },

  async deleteUniverseConfig(
    name: string,
    signal?: AbortSignal
  ): Promise<{ status: string; message: string }> {
    return request<{ status: string; message: string }>(`/universes/${encodeURIComponent(name)}`, {
      method: 'DELETE',
      signal
    });
  },

  async getUniverseCatalog(signal?: AbortSignal): Promise<UniverseCatalogResponse> {
    return request<UniverseCatalogResponse>('/universes/catalog', { signal });
  },

  async previewUniverse(
    payload: {
      universeName?: string;
      universe?: UniverseDefinition;
      sampleLimit?: number;
    },
    signal?: AbortSignal
  ): Promise<UniversePreviewResponse> {
    return request<UniversePreviewResponse>('/universes/preview', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
  }
};
