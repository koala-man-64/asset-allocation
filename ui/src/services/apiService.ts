/* global RequestInit */

import { FinanceData, MarketData } from '@/types/data';
import { DomainMetadata, SystemHealth } from '@/types/strategy';
import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';
import { config as uiConfig } from '@/config';
import { appendAuthHeaders } from '@/services/authTransport';

interface WindowWithConfig extends Window {
  __API_UI_CONFIG__?: { apiBaseUrl?: string };
}
const runtimeConfig = (window as WindowWithConfig).__API_UI_CONFIG__ || {};
const API_BASE_URL = normalizeApiBaseUrl(runtimeConfig.apiBaseUrl || uiConfig.apiBaseUrl, '/api');

export interface RequestConfig extends RequestInit {
  params?: Record<string, string | number | boolean | undefined>;
}

export interface RequestMeta {
  requestId: string;
  status: number;
  durationMs: number;
  url: string;
  cacheHint?: string;
  stale?: boolean;
}

export interface ResponseWithMeta<T> {
  data: T;
  meta: RequestMeta;
}

function createRequestId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `req-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

async function performRequest<T>(
  endpoint: string,
  config: RequestConfig = {}
): Promise<ResponseWithMeta<T>> {
  const { params, headers, ...customConfig } = config;

  let url = `${API_BASE_URL}${endpoint}`;
  if (params) {
    const searchParams = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) {
        searchParams.append(key, String(value));
      }
    });
    const queryString = searchParams.toString();
    if (queryString) {
      url += `?${queryString}`;
    }
  }

  const requestHeaders = new Headers(headers);
  const hasBody = customConfig.body !== undefined && customConfig.body !== null;
  if (hasBody && !requestHeaders.has('Content-Type')) {
    requestHeaders.set('Content-Type', 'application/json');
  }
  if (!requestHeaders.has('X-Request-ID')) {
    requestHeaders.set('X-Request-ID', createRequestId());
  }
  const authHeaders = await appendAuthHeaders(requestHeaders);

  const startedAt = performance.now();
  const response = await fetch(url, {
    headers: authHeaders,
    ...customConfig
  });
  const durationMs = Math.max(0, Math.round(performance.now() - startedAt));
  const requestId = authHeaders.get('X-Request-ID') || '';

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(
      `API Error: ${response.status} ${response.statusText} [requestId=${requestId}] - ${errorBody}`
    );
  }

  let data: T;
  if (response.status === 204) {
    data = {} as T;
  } else {
    data = (await response.json()) as T;
  }

  return {
    data,
    meta: {
      requestId,
      status: response.status,
      durationMs,
      url: response.url || url,
      cacheHint: response.headers.get('X-System-Health-Cache') || undefined,
      stale: response.headers.get('X-System-Health-Stale') === '1'
    }
  };
}

export async function request<T>(endpoint: string, config: RequestConfig = {}): Promise<T> {
  const result = await performRequest<T>(endpoint, config);
  return result.data;
}

export async function requestWithMeta<T>(
  endpoint: string,
  config: RequestConfig = {}
): Promise<ResponseWithMeta<T>> {
  return performRequest<T>(endpoint, config);
}

export interface JobLogsResponse {
  logs: string[];
  offset: number;
  hasMore: boolean;
}

export interface StockScreenerResponse {
  items: unknown[];
  total: number;
  offset: number;
  limit: number;
}

export interface PurgeRequest {
  scope: 'layer-domain' | 'layer' | 'domain';
  layer?: string;
  domain?: string;
  confirm: boolean;
}

export interface PurgeResponse {
  scope: string;
  layer?: string | null;
  domain?: string | null;
  totalDeleted: number;
  targets: Array<{
    container: string;
    prefix?: string | null;
    layer?: string | null;
    domain?: string | null;
    deleted: number;
  }>;
}

export interface DebugSymbolsResponse {
  enabled: boolean;
  symbols: string;
  updatedAt?: string | null;
  updatedBy?: string | null;
}

export interface RuntimeConfigCatalogItem {
  key: string;
  description: string;
  example: string;
}

export interface RuntimeConfigCatalogResponse {
  items: RuntimeConfigCatalogItem[];
}

export interface RuntimeConfigItem {
  scope: string;
  key: string;
  enabled: boolean;

export interface JobLogsResponse {
  logs: string[];
  offset: number;
  hasMore: boolean;
}

export interface StockScreenerResponse {
  items: unknown[];
  total: number;
  offset: number;
  limit: number;
}

export interface PurgeRequest {
  scope: 'layer-domain' | 'layer' | 'domain';
  layer?: string;
  domain?: string;
  confirm: boolean;
}

export interface PurgeResponse {
  scope: string;
  layer?: string | null;
  domain?: string | null;
  totalDeleted: number;
  targets: Array<{
    container: string;
    prefix?: string | null;
    layer?: string | null;
    domain?: string | null;
    deleted: number;
  }>;
}

export interface DebugSymbolsResponse {
  enabled: boolean;
  symbols: string;
  updatedAt?: string | null;
  updatedBy?: string | null;
}

export interface RuntimeConfigCatalogItem {
  key: string;
  description: string;
  example: string;
}

export interface RuntimeConfigCatalogResponse {
  items: RuntimeConfigCatalogItem[];
}

export interface RuntimeConfigItem {
  scope: string;
  key: string;
  enabled: boolean;
  value: string;
  description?: string | null;
  updatedAt?: string | null;
  updatedBy?: string | null;
}

export interface RuntimeConfigListResponse {
  scope: string;
  items: RuntimeConfigItem[];
}

export interface ValidationColumnStat {
  name: string;
  type: string;
  total: number;
  unique: number;
  notNull: number;
  null: number;
}

export interface ValidationReport {
  layer: string;
  domain: string;
  status: string;
  rowCount: number;
  columns: ValidationColumnStat[];
  timestamp: string;
  error?: string;
  sampleLimit?: number;
}

export const apiService = {
  // --- Data Endpoints ---

  getMarketData(
    ticker: string,
    layer: 'silver' | 'gold' = 'silver',
    signal?: AbortSignal
  ): Promise<MarketData[]> {
    return request<MarketData[]>(`/data/${layer}/market`, { params: { ticker }, signal });
  },

  getFinanceData(
    ticker: string,
    subDomain: string,
    layer: 'silver' | 'gold' = 'silver',
    signal?: AbortSignal
  ): Promise<FinanceData[]> {
    return request<FinanceData[]>(`/data/${layer}/finance/${encodeURIComponent(subDomain)}`, {
      params: { ticker },
      signal
    });
  },

  getSystemHealth(params: { refresh?: boolean } = {}): Promise<SystemHealth> {
    return request<SystemHealth>('/system/health', { params });
  },

  getSystemHealthWithMeta(params: { refresh?: boolean } = {}): Promise<ResponseWithMeta<SystemHealth>> {
    return requestWithMeta<SystemHealth>('/system/health', { params });
  },

  getDomainMetadata(
    layer: 'bronze' | 'silver' | 'gold' | 'platinum',
    domain: string,
    params: { refresh?: boolean } = {}
  ): Promise<DomainMetadata> {
    return request<DomainMetadata>('/system/domain-metadata', {
      params: { layer, domain, ...params }
    });
  },

  getLineage(): Promise<unknown> {
    return request<unknown>('/system/lineage');
  },

  getJobLogs(
    jobName: string,
    params: { runs?: number } = {},
    signal?: AbortSignal
  ): Promise<JobLogsResponse> {
    return request<JobLogsResponse>(`/system/jobs/${jobName}/logs`, {
      params,
      signal
    });
  },

  getStockScreener(
    params: {
      q?: string;
      limit?: number;
      offset?: number;
      asOf?: string;
      sort?: string;
      direction?: 'asc' | 'desc';
    } = {},
    signal?: AbortSignal
  ): Promise<StockScreenerResponse> {
    return request<StockScreenerResponse>('/data/screener', {
      params,
      signal
    });
  },

  getGenericData(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string,
    ticker?: string,
    limit?: number,
    signal?: AbortSignal
  ): Promise<Record<string, unknown>[]> {
    const endpoint = `/data/${layer}/${domain}`;
    return request<Record<string, unknown>[]>(endpoint, {
      params: { ticker, limit },
      signal
    });
  },

  getDataQualityValidation(
    layer: string,
    domain: string,
    signal?: AbortSignal
  ): Promise<ValidationReport> {
    return request<ValidationReport>(`/data/quality/${layer}/${domain}/validation`, { signal });
  },

  purgeData(payload: PurgeRequest): Promise<PurgeResponse> {
    return request<PurgeResponse>('/system/purge', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  },

  getDebugSymbols(): Promise<DebugSymbolsResponse> {
    return request<DebugSymbolsResponse>('/system/debug-symbols');
  },

  setDebugSymbols(payload: { enabled: boolean; symbols?: string }): Promise<DebugSymbolsResponse> {
    return request<DebugSymbolsResponse>('/system/debug-symbols', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  },

  getRuntimeConfigCatalog(): Promise<RuntimeConfigCatalogResponse> {
    return request<RuntimeConfigCatalogResponse>('/system/runtime-config/catalog');
  },

  getRuntimeConfig(scope: string = 'global'): Promise<RuntimeConfigListResponse> {
    return request<RuntimeConfigListResponse>('/system/runtime-config', {
      params: { scope }
    });
  },

  setRuntimeConfig(payload: {
    key: string;
    scope?: string;
    enabled: boolean;
    value: string;
    description?: string;
  }): Promise<RuntimeConfigItem> {
    return request<RuntimeConfigItem>('/system/runtime-config', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  },

  deleteRuntimeConfig(
    key: string,
    scope: string = 'global'
  ): Promise<{ scope: string; key: string; deleted: boolean }> {
    return request<{ scope: string; key: string; deleted: boolean }>(
      `/system/runtime-config/${encodeURIComponent(key)}`,
      {
        method: 'DELETE',
        params: { scope }
      }
    );
  }
};
