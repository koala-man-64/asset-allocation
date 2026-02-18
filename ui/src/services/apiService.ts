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
  cacheDegraded?: boolean;
  // Legacy alias retained for backward compatibility.
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
      cacheDegraded:
        response.headers.get('X-System-Health-Cache-Degraded') === '1' ||
        response.headers.get('X-System-Health-Stale') === '1',
      stale:
        response.headers.get('X-System-Health-Cache-Degraded') === '1' ||
        response.headers.get('X-System-Health-Stale') === '1'
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

export interface StockScreenerRow {
  symbol: string;
  name?: string | null;
  sector?: string | null;
  industry?: string | null;
  country?: string | null;
  isOptionable?: boolean | null;
  open?: number | null;
  high?: number | null;
  low?: number | null;
  close?: number | null;
  volume?: number | null;
  return1d?: number | null;
  return5d?: number | null;
  vol20d?: number | null;
  drawdown1y?: number | null;
  atr14d?: number | null;
  gapAtr?: number | null;
  sma50d?: number | null;
  sma200d?: number | null;
  trend50_200?: number | null;
  aboveSma50?: number | null;
  bbWidth20d?: number | null;
  compressionScore?: number | null;
  volumeZ20d?: number | null;
  volumePctRank252d?: number | null;
  hasSilver?: number | null;
  hasGold?: number | null;
}

export interface StockScreenerResponse {
  asOf: string;
  total: number;
  limit: number;
  offset: number;
  rows: StockScreenerRow[];
}

export interface PurgeRequest {
  scope: 'layer-domain' | 'layer' | 'domain';
  layer?: string;
  domain?: string;
  confirm: boolean;
}

export interface PurgeCandidateRow {
  symbol: string;
  matchedValue: number;
  rowsContributing: number;
  latestAsOf: string | null;
}

export interface PurgeCandidatesCriteria {
  requestedLayer: string;
  resolvedLayer: string;
  domain: string;
  column: string;
  operator: string;
  value: number;
  asOf?: string | null;
  minRows: number;
  recentRows: number;
  aggregation: 'min' | 'max' | 'avg' | 'stddev';
}

export interface PurgeCandidatesSummary {
  totalRowsScanned: number;
  symbolsMatched: number;
  rowsContributing: number;
  estimatedDeletionTargets: number;
}

export interface PurgeCandidatesResponse {
  criteria: PurgeCandidatesCriteria;
  expression: string;
  summary: PurgeCandidatesSummary;
  symbols: PurgeCandidateRow[];
  offset: number;
  limit: number;
  total: number;
  hasMore: boolean;
  note?: string | null;
}

export interface PurgeSymbolResultItem {
  symbol: string;
  status: 'succeeded' | 'failed' | 'skipped';
  deleted?: number;
  dryRun?: boolean;
  error?: string;
}

export interface PurgeBatchOperationResult {
  scope: 'symbols';
  dryRun: boolean;
  scopeNote?: string | null;
  requestedSymbols: string[];
  requestedSymbolCount: number;
  succeeded: number;
  failed: number;
  skipped: number;
  totalDeleted: number;
  symbolResults: PurgeSymbolResultItem[];
}

export interface PurgeOperationResponse {
  operationId: string;
  status: 'running' | 'succeeded' | 'failed';
  scope: string;
  layer?: string | null;
  domain?: string | null;
  createdAt: string;
  updatedAt: string;
  startedAt: string;
  completedAt?: string | null;
  result?: PurgeResponse | PurgeBatchOperationResult;
  error?: string | null;
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
  notNull: number;
  nullPct: number;
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

export interface ProfilingBucket {
  label: string;
  count: number;
  start?: number | null;
  end?: number | null;
}

export interface ProfilingTopValue {
  value: string;
  count: number;
}

export interface DataProfilingResponse {
  layer: string;
  domain: string;
  column: string;
  kind: 'numeric' | 'date' | 'string';
  totalRows: number;
  nonNullCount: number;
  nullCount: number;
  sampleRows: number;
  bins: ProfilingBucket[];
  uniqueCount?: number;
  duplicateCount?: number;
  topValues?: ProfilingTopValue[];
}

export interface StorageFolderUsage {
  path: string;
  fileCount: number | null;
  totalBytes: number | null;
  truncated: boolean;
  error?: string | null;
}

export interface StorageContainerUsage {
  layer: string;
  layerLabel: string;
  container: string;
  totalFiles: number | null;
  totalBytes: number | null;
  truncated: boolean;
  error?: string | null;
  folders: StorageFolderUsage[];
}

export interface StorageUsageResponse {
  generatedAt: string;
  scanLimit: number;
  containers: StorageContainerUsage[];
}

export interface ContainerAppHealthCheck {
  status: 'healthy' | 'warning' | 'error' | 'unknown';
  url?: string | null;
  httpStatus?: number | null;
  checkedAt?: string | null;
  error?: string | null;
}

export interface ContainerAppStatusItem {
  name: string;
  resourceType?: string;
  status: 'healthy' | 'warning' | 'error' | 'unknown';
  details?: string;
  provisioningState?: string | null;
  runningState?: string | null;
  latestReadyRevisionName?: string | null;
  ingressFqdn?: string | null;
  azureId?: string | null;
  checkedAt?: string | null;
  error?: string | null;
  health?: ContainerAppHealthCheck | null;
}

export interface ContainerAppsStatusResponse {
  probed: boolean;
  apps: ContainerAppStatusItem[];
}

export interface ContainerAppControlResponse {
  appName: string;
  action: 'start' | 'stop';
  provisioningState?: string | null;
  runningState?: string | null;
}

export interface ContainerAppLogsResponse {
  appName: string;
  lookbackMinutes: number;
  tailLines: number;
  logs: string[];
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

  getSystemHealthWithMeta(
    params: { refresh?: boolean } = {}
  ): Promise<ResponseWithMeta<SystemHealth>> {
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

  getContainerApps(
    params: { probe?: boolean } = {},
    signal?: AbortSignal
  ): Promise<ContainerAppsStatusResponse> {
    return request<ContainerAppsStatusResponse>('/system/container-apps', {
      params: { probe: params.probe ?? true },
      signal
    });
  },

  startContainerApp(
    appName: string,
    signal?: AbortSignal
  ): Promise<ContainerAppControlResponse> {
    return request<ContainerAppControlResponse>(
      `/system/container-apps/${encodeURIComponent(appName)}/start`,
      {
        method: 'POST',
        signal
      }
    );
  },

  stopContainerApp(
    appName: string,
    signal?: AbortSignal
  ): Promise<ContainerAppControlResponse> {
    return request<ContainerAppControlResponse>(
      `/system/container-apps/${encodeURIComponent(appName)}/stop`,
      {
        method: 'POST',
        signal
      }
    );
  },

  getContainerAppLogs(
    appName: string,
    params: { minutes?: number; tail?: number } = {},
    signal?: AbortSignal
  ): Promise<ContainerAppLogsResponse> {
    return request<ContainerAppLogsResponse>(
      `/system/container-apps/${encodeURIComponent(appName)}/logs`,
      {
        params: {
          minutes: params.minutes ?? 60,
          tail: params.tail ?? 50
        },
        signal
      }
    );
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
    tickerOrSignal?: string | AbortSignal,
    signal?: AbortSignal
  ): Promise<ValidationReport> {
    const ticker = typeof tickerOrSignal === 'string' ? tickerOrSignal : undefined;
    const resolvedSignal =
      tickerOrSignal instanceof AbortSignal ? tickerOrSignal : signal;
    return request<ValidationReport>(`/data/quality/${layer}/${domain}/validation`, {
      params: { ticker },
      signal: resolvedSignal
    });
  },

  getStorageUsage(signal?: AbortSignal): Promise<StorageUsageResponse> {
    return request<StorageUsageResponse>('/data/storage-usage', {
      signal
    });
  },

  getDataProfile(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string,
    column: string,
    params: {
      ticker?: string;
      bins?: number;
      sampleRows?: number;
      topValues?: number;
    } = {},
    signal?: AbortSignal
  ): Promise<DataProfilingResponse> {
    return request<DataProfilingResponse>(`/data/${layer}/profile`, {
      params: {
        domain,
        column,
        ticker: params.ticker,
        bins: params.bins,
        sampleRows: params.sampleRows,
        topValues: params.topValues
      },
      signal
    });
  },

  purgeData(payload: PurgeRequest): Promise<PurgeOperationResponse> {
    return request<PurgeOperationResponse>('/system/purge', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  },

  getPurgeCandidates(payload: {
    layer: 'bronze' | 'silver' | 'gold';
    domain: 'market' | 'finance' | 'earnings' | 'price-target';
    column: string;
    operator: 'gt' | 'gte' | 'lt' | 'lte' | 'eq' | 'ne' | 'top_percent' | 'bottom_percent';
    aggregation?: 'min' | 'max' | 'avg' | 'stddev';
    value?: number;
    percentile?: number;
    as_of?: string;
    recent_rows?: number;
    offset?: number;
    min_rows?: number;
  }): Promise<PurgeCandidatesResponse> {
    return request<PurgeCandidatesResponse>('/system/purge-candidates', { params: payload });
  },

  getPurgeOperation(operationId: string): Promise<PurgeOperationResponse> {
    return request<PurgeOperationResponse>(`/system/purge/${encodeURIComponent(operationId)}`);
  },

  purgeSymbolsBatch(payload: {
    symbols: string[];
    confirm: boolean;
    scope_note?: string;
    dry_run?: boolean;
    audit_rule?: {
      layer: 'bronze' | 'silver' | 'gold';
      domain: 'market' | 'finance' | 'earnings' | 'price-target';
      column_name: string;
      operator: 'gt' | 'gte' | 'lt' | 'lte' | 'eq' | 'ne' | 'top_percent' | 'bottom_percent';
      threshold: number;
      aggregation?: 'min' | 'max' | 'avg' | 'stddev';
      recent_rows?: number;
      expression?: string;
      selected_symbol_count?: number;
      matched_symbol_count?: number;
    };
  }): Promise<PurgeOperationResponse> {
    return request<PurgeOperationResponse>('/system/purge-symbols', {
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
  },

  getSymbolSyncState(): Promise<SymbolSyncState> {
    return request<SymbolSyncState>('/system/symbol-sync-state');
  }
};

export interface SymbolSyncState {
  id: number;
  last_refreshed_at: string;
  last_refreshed_sources: {
    nasdaq?: { rows: number; timestamp: string };
    alpha_vantage?: { rows: number; timestamp: string };
    massive?: { rows: number; timestamp: string };
  };
  last_refresh_error?: string;
}
