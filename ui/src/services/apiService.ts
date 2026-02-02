/* global RequestInit */

import { FinanceData, MarketData } from '@/types/data';
import { DomainMetadata, SystemHealth } from '@/types/strategy';
import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';
import { config as uiConfig } from '@/config';

interface WindowWithConfig extends Window {
  __API_UI_CONFIG__?: { apiBaseUrl?: string };
}
const runtimeConfig = (window as WindowWithConfig).__API_UI_CONFIG__ || {};
const API_BASE_URL = normalizeApiBaseUrl(runtimeConfig.apiBaseUrl || uiConfig.apiBaseUrl, '/api');

export interface RequestConfig extends RequestInit {
  params?: Record<string, string | number | boolean | undefined>;
}

export async function request<T>(endpoint: string, config: RequestConfig = {}): Promise<T> {
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

  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...headers
    },
    ...customConfig
  });

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`API Error: ${response.status} ${response.statusText} - ${errorBody}`);
  }

  // Handle void response
  if (response.status === 204) {
    return {} as T;
  }

  return response.json();
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
  presetSymbols?: string;
  targets?: string[];
  updatedJobs?: string[];
  requestedJobs?: string[];
}

export const apiService = {
  // --- Data Endpoints ---

  getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
    return request<MarketData[]>(`/data/market/${ticker}`, { params: { layer } });
  },

  getFinanceData(
    ticker: string,
    subDomain: string,
    layer: 'silver' | 'gold' = 'silver'
  ): Promise<FinanceData[]> {
    return request<FinanceData[]>(`/data/finance/${ticker}/${subDomain}`, { params: { layer } });
  },

  getSystemHealth(params: { refresh?: boolean } = {}): Promise<SystemHealth> {
    return request<SystemHealth>('/system/health', { params });
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
    limit?: number
  ): Promise<Record<string, unknown>[]> {
    const endpoint = `/data/${layer}/${domain}`;
    return request<Record<string, unknown>[]>(endpoint, {
      params: { ticker, limit }
    });
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
  }
};
