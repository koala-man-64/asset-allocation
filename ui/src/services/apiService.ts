/* global RequestInit */

import { FinanceData, MarketData } from '@/types/data';
import { DomainMetadata, SystemHealth } from '@/types/strategy';
import { config as uiConfig } from '@/config';
import { appendAuthHeaders } from '@/services/authTransport';

const API_WARMUP_PATH = '/healthz';
const API_COLD_START_RETRYABLE_STATUS_CODES = new Set([408, 425, 429, 500, 502, 503, 504]);
const API_WARMUP_MAX_ATTEMPTS = 3;
const API_WARMUP_BASE_DELAY_MS = 500;
const API_WARMUP_MAX_DELAY_MS = 4000;
const API_WARMUP_TIMEOUT_MS = 5000;
const API_REQUEST_MAX_ATTEMPTS = 3;
const API_REQUEST_RETRY_BASE_DELAY_MS = 500;
const API_REQUEST_RETRY_MAX_DELAY_MS = 4000;

const apiWarmupAttempted = new Set<string>();
const apiWarmupInFlight = new Map<string, Promise<void>>();

function isRetryableStatusCode(statusCode: number): boolean {
  return API_COLD_START_RETRYABLE_STATUS_CODES.has(statusCode);
}

function isRetryableFetchError(error: unknown, externalSignal?: AbortSignal | null): boolean {
  if (externalSignal?.aborted) {
    return false;
  }

  if (error instanceof Error && error.message.startsWith('API timeout after ')) {
    return true;
  }

  if (error instanceof DOMException && error.name === 'AbortError') {
    return true;
  }

  if (error instanceof TypeError) {
    return true;
  }

  const message = error instanceof Error ? error.message.toLowerCase() : '';
  return (
    message.includes('network') ||
    message.includes('failed to fetch') ||
    message.includes('connection refused') ||
    message.includes('load failed')
  );
}

function resolveWarmupUrl(apiBaseUrl: string): string {
  const base = apiBaseUrl.replace(/\/+$/, '').replace(/\/api$/i, '');
  return `${base || ''}${API_WARMUP_PATH}`;
}

function buildRequestUrl(
  apiBaseUrl: string,
  endpoint: string,
  params?: Record<string, string | number | boolean | undefined>
): string {
  let url = `${apiBaseUrl}${endpoint}`;
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
  return url;
}

async function wait(delayMs: number): Promise<void> {
  if (delayMs <= 0) {
    return;
  }
  await new Promise<void>((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

async function fetchWithOptionalTimeout(
  url: string,
  init: RequestInit,
  timeoutMs: number | undefined,
  endpointLabel: string,
  requestId: string
): Promise<Response> {
  let timeoutController: AbortController | undefined;
  let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
  let mergedSignal: AbortSignal | null | undefined = init.signal;
  let removeExternalAbortListener: (() => void) | undefined;

  if (typeof timeoutMs === 'number' && Number.isFinite(timeoutMs) && timeoutMs > 0) {
    timeoutController = new AbortController();
    timeoutHandle = setTimeout(() => {
      timeoutController?.abort();
    }, Math.max(1, Math.floor(timeoutMs)));

    if (init.signal) {
      if (init.signal.aborted) {
        timeoutController.abort();
      } else {
        const relayAbort = () => timeoutController?.abort();
        init.signal.addEventListener('abort', relayAbort, { once: true });
        removeExternalAbortListener = () => init.signal?.removeEventListener('abort', relayAbort);
      }
    }
    mergedSignal = timeoutController.signal;
  }

  try {
    return await fetch(url, {
      ...init,
      signal: mergedSignal ?? undefined
    });
  } catch (error) {
    if (timeoutController?.signal.aborted && !(init.signal?.aborted)) {
      throw new Error(
        `API timeout after ${Math.floor(timeoutMs || 0)}ms [requestId=${requestId}] - ${endpointLabel}`
      );
    }
    throw error;
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
    if (removeExternalAbortListener) {
      removeExternalAbortListener();
    }
  }
}

async function warmUpApiOnce(apiBaseUrl: string): Promise<void> {
  if (apiWarmupAttempted.has(apiBaseUrl)) {
    return;
  }

  if (!apiWarmupInFlight.has(apiBaseUrl)) {
    const warmupPromise = (async () => {
      let delayMs = API_WARMUP_BASE_DELAY_MS;
      const warmupUrl = resolveWarmupUrl(apiBaseUrl);

      try {
        for (let attempt = 1; attempt <= API_WARMUP_MAX_ATTEMPTS; attempt += 1) {
          const shouldRetry = attempt < API_WARMUP_MAX_ATTEMPTS;
          try {
            const response = await fetchWithOptionalTimeout(
              warmupUrl,
              {
                method: 'GET',
                headers: new Headers({ 'X-Request-ID': createRequestId() }),
                cache: 'no-store'
              },
              API_WARMUP_TIMEOUT_MS,
              API_WARMUP_PATH,
              'warmup'
            );
            if (response.status < 400) {
              return;
            }
            if (!shouldRetry || !isRetryableStatusCode(response.status)) {
              return;
            }
          } catch (error) {
            if (!shouldRetry || !isRetryableFetchError(error)) {
              return;
            }
          }

          await wait(delayMs);
          delayMs = Math.min(API_WARMUP_MAX_DELAY_MS, Math.max(delayMs * 2, 100));
        }
      } finally {
        apiWarmupAttempted.add(apiBaseUrl);
        apiWarmupInFlight.delete(apiBaseUrl);
      }
    })();
    apiWarmupInFlight.set(apiBaseUrl, warmupPromise);
  }

  await apiWarmupInFlight.get(apiBaseUrl);
}

export interface RequestConfig extends RequestInit {
  params?: Record<string, string | number | boolean | undefined>;
  timeoutMs?: number;
  retryOnStatusCodes?: number[] | false;
  retryAttempts?: number;
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
  const { params, headers, timeoutMs, retryOnStatusCodes, retryAttempts, ...customConfig } = config;
  const apiBaseUrl = uiConfig.apiBaseUrl;
  const maxAttempts = Number.isFinite(retryAttempts)
    ? Math.max(1, Math.floor(Number(retryAttempts)))
    : API_REQUEST_MAX_ATTEMPTS;
  const retryableStatusCodes =
    retryOnStatusCodes === false
      ? new Set<number>()
      : Array.isArray(retryOnStatusCodes)
        ? new Set<number>(retryOnStatusCodes)
        : API_COLD_START_RETRYABLE_STATUS_CODES;

  let url = buildRequestUrl(apiBaseUrl, endpoint, params);

  const requestHeaders = new Headers(headers);
  const hasBody = customConfig.body !== undefined && customConfig.body !== null;
  if (hasBody && !requestHeaders.has('Content-Type')) {
    requestHeaders.set('Content-Type', 'application/json');
  }
  if (!requestHeaders.has('X-Request-ID')) {
    requestHeaders.set('X-Request-ID', createRequestId());
  }
  const authHeaders = await appendAuthHeaders(requestHeaders);
  const requestId = authHeaders.get('X-Request-ID') || '';
  await warmUpApiOnce(apiBaseUrl);

  let retryDelayMs = API_REQUEST_RETRY_BASE_DELAY_MS;
  let response: Response | null = null;
  const startedAt = performance.now();
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const shouldRetry = attempt < maxAttempts;
    try {
      response = await fetchWithOptionalTimeout(
        url,
        {
          headers: authHeaders,
          ...customConfig
        },
        timeoutMs,
        endpoint,
        requestId
      );
    } catch (error) {
      if (!shouldRetry || !isRetryableFetchError(error, customConfig.signal)) {
        throw error;
      }
      await wait(retryDelayMs);
      retryDelayMs = Math.min(API_REQUEST_RETRY_MAX_DELAY_MS, Math.max(retryDelayMs * 2, 100));
      continue;
    }

    if (response.ok) {
      break;
    }

    if (!shouldRetry || !retryableStatusCodes.has(response.status)) {
      break;
    }

    await wait(retryDelayMs);
    retryDelayMs = Math.min(API_REQUEST_RETRY_MAX_DELAY_MS, Math.max(retryDelayMs * 2, 100));
  }

  if (!response) {
    throw new Error(`API request failed with no response [requestId=${requestId}] - ${endpoint}`);
  }

  const durationMs = Math.max(0, Math.round(performance.now() - startedAt));

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

export interface DomainListResetRequest {
  layer: string;
  domain: string;
  confirm: boolean;
}

export interface DomainListResetResponse {
  layer: string;
  domain: string;
  container: string;
  resetCount: number;
  targets: Array<{
    listType: 'whitelist' | 'blacklist';
    path: string;
    status: 'reset';
    existed: boolean;
  }>;
  updatedAt: string;
}

export interface DomainListFilePreview {
  listType: 'whitelist' | 'blacklist';
  path: string;
  exists: boolean;
  symbolCount: number;
  symbols: string[];
  truncated: boolean;
  warning?: string | null;
}

export interface DomainListsResponse {
  layer: string;
  domain: string;
  container: string;
  limit: number;
  files: DomainListFilePreview[];
  loadedAt: string;
}

export interface DomainColumnsResponse {
  layer: 'bronze' | 'silver' | 'gold';
  domain: string;
  columns: string[];
  found: boolean;
  promptRetrieve: boolean;
  source: 'common-file';
  cachePath: string;
  updatedAt?: string | null;
}

export interface PurgeCandidateRow {
  symbol: string;
  matchedValue: number;
  rowsContributing: number;
  latestAsOf: string | null;
}

export interface PurgeCandidatesRequest {
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

export interface PurgeBlacklistSource {
  path: string;
  symbolCount: number;
  warning?: string;
}

export interface PurgeBlacklistSymbolsResponse {
  container: string;
  symbolCount: number;
  symbols: string[];
  sources: PurgeBlacklistSource[];
  loadedAt?: string;
}

export interface PurgeBatchOperationResult {
  scope: 'symbols';
  dryRun: boolean;
  scopeNote?: string | null;
  requestedSymbols: string[];
  requestedSymbolCount: number;
  completed?: number;
  pending?: number;
  inProgress?: number;
  progressPct?: number;
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
  result?: PurgeResponse | PurgeBatchOperationResult | PurgeCandidatesResponse;
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

export interface DomainMetadataSnapshotResponse {
  version: number;
  updatedAt?: string | null;
  entries: Record<string, DomainMetadata>;
  warnings?: string[];
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
    params: { refresh?: boolean; cacheOnly?: boolean } = {}
  ): Promise<DomainMetadata> {
    return request<DomainMetadata>('/system/domain-metadata', {
      params: { layer, domain, ...params }
    });
  },

  getDomainMetadataSnapshot(
    params: { layers?: string; domains?: string; cacheOnly?: boolean; refresh?: boolean } = {}
  ): Promise<DomainMetadataSnapshotResponse> {
    return request<DomainMetadataSnapshotResponse>('/system/domain-metadata/snapshot', {
      params
    });
  },

  getPersistedDomainMetadataSnapshotCache(): Promise<DomainMetadataSnapshotResponse> {
    return request<DomainMetadataSnapshotResponse>('/system/domain-metadata/snapshot/cache');
  },

  savePersistedDomainMetadataSnapshotCache(
    payload: DomainMetadataSnapshotResponse
  ): Promise<DomainMetadataSnapshotResponse> {
    return request<DomainMetadataSnapshotResponse>('/system/domain-metadata/snapshot/cache', {
      method: 'PUT',
      body: JSON.stringify(payload)
    });
  },

  getDomainColumns(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string
  ): Promise<DomainColumnsResponse> {
    return request<DomainColumnsResponse>('/system/domain-columns', {
      params: { layer, domain },
      timeoutMs: 10000
    });
  },

  refreshDomainColumns(payload: {
    layer: 'bronze' | 'silver' | 'gold';
    domain: string;
    sample_limit?: number;
  }): Promise<DomainColumnsResponse> {
    return request<DomainColumnsResponse>('/system/domain-columns/refresh', {
      method: 'POST',
      body: JSON.stringify(payload),
      timeoutMs: 30000
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

  resetDomainLists(payload: DomainListResetRequest): Promise<DomainListResetResponse> {
    return request<DomainListResetResponse>('/system/domain-lists/reset', {
      method: 'POST',
      body: JSON.stringify(payload)
    });
  },

  getDomainLists(
    layer: string,
    domain: string,
    params: { limit?: number } = {}
  ): Promise<DomainListsResponse> {
    return request<DomainListsResponse>('/system/domain-lists', {
      params: { layer, domain, ...params }
    });
  },

  getPurgeCandidates(payload: PurgeCandidatesRequest): Promise<PurgeCandidatesResponse> {
    return request<PurgeCandidatesResponse>('/system/purge-candidates', {
      params: payload,
      timeoutMs: 30000,
      retryOnStatusCodes: [408, 425, 429, 500, 502, 503]
    });
  },

  createPurgeCandidatesOperation(payload: PurgeCandidatesRequest): Promise<PurgeOperationResponse> {
    return request<PurgeOperationResponse>('/system/purge-candidates', {
      method: 'POST',
      body: JSON.stringify(payload),
      timeoutMs: 30000,
      retryOnStatusCodes: [408, 425, 429, 500, 502, 503]
    });
  },

  getPurgeOperation(operationId: string): Promise<PurgeOperationResponse> {
    return request<PurgeOperationResponse>(`/system/purge/${encodeURIComponent(operationId)}`);
  },

  getPurgeBlacklistSymbols(): Promise<PurgeBlacklistSymbolsResponse> {
    return request<PurgeBlacklistSymbolsResponse>('/system/purge-symbols/blacklist');
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
