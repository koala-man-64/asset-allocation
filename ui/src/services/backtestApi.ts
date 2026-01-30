/* global RequestInit */
import type { FinanceData, MarketData } from '@/types/data';
import type { SystemHealth } from '@/types/strategy';
import { config } from '@/config';

export type RunStatus = 'queued' | 'running' | 'completed' | 'failed';
export type DataSource = 'auto' | 'local' | 'adls';
export type DataDomain = 'market' | 'earnings' | 'price-target';

type AccessTokenProvider = () => Promise<string | null>;

let accessTokenProvider: AccessTokenProvider | null = null;

// Define config interface locally to avoid circular dependencies if needed, or just extend Window
interface WindowWithConfig extends Window {
  __BACKTEST_UI_CONFIG__?: { backtestApiBaseUrl?: string };
}
const runtimeConfig = (window as WindowWithConfig).__BACKTEST_UI_CONFIG__ || {};
const debugApi = (() => {
  const isTruthy = (value: unknown): boolean => {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;
    const text = String(value ?? '').trim().toLowerCase();
    return ['1', 'true', 'yes', 'y', 'on'].includes(text);
  };

  let localStorageFlag: string | null = null;
  try {
    localStorageFlag = window.localStorage.getItem('debugApi');
  } catch {
    localStorageFlag = null;
  }

  const queryFlag = new URLSearchParams(window.location.search).get('debugApi');
  const candidates = [
    runtimeConfig.debugApi,
    import.meta.env.VITE_DEBUG_API,
    queryFlag,
    localStorageFlag,
  ];
  const explicitFlag = candidates.find((value) => {
    if (value === undefined || value === null) return false;
    return String(value).trim() !== '';
  });
  if (explicitFlag === undefined) return true;
  return isTruthy(explicitFlag);
})();

const apiLogPrefix = '[Backtest API]';


function logApi(message: string, meta: Record<string, unknown> = {}): void {
  if (!debugApi) return;
  if (Object.keys(meta).length) {
    console.info(apiLogPrefix, message, meta);
    return;
  }
  console.info(apiLogPrefix, message);
}



if (debugApi) {
  logApi('Runtime config', {
    apiBaseUrl: config.apiBaseUrl,
    runtimeBaseUrl: runtimeConfig.backtestApiBaseUrl,
    envBaseUrl: import.meta.env.VITE_BACKTEST_API_BASE_URL || import.meta.env.VITE_API_BASE_URL,
    origin: window.location.origin,
  });
}

export function setAccessTokenProvider(provider: AccessTokenProvider | null): void {
  accessTokenProvider = provider;
}

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

export interface RunRecordResponse {
  run_id: string;
  status: RunStatus;
  submitted_at: string;
  started_at?: string | null;
  completed_at?: string | null;
  run_name?: string | null;
  start_date?: string | null;
  end_date?: string | null;
  output_dir?: string | null;
  adls_container?: string | null;
  adls_prefix?: string | null;
  error?: string | null;
}

export interface RunListResponse {
  runs: RunRecordResponse[];
  limit: number;
  offset: number;
}

export interface TimeseriesPointResponse {
  date: string;
  portfolio_value: number;
  drawdown: number;
  daily_return?: number | null;
  cumulative_return?: number | null;
  cash?: number | null;
  gross_exposure?: number | null;
  net_exposure?: number | null;
  turnover?: number | null;
  commission?: number | null;
  slippage_cost?: number | null;
}

export interface TimeseriesResponse {
  points: TimeseriesPointResponse[];
  total_points: number;
  truncated: boolean;
}

export interface RollingMetricPointResponse {
  date: string;
  window_days: number;
  rolling_return?: number | null;
  rolling_volatility?: number | null;
  rolling_sharpe?: number | null;
  rolling_max_drawdown?: number | null;
  turnover_sum?: number | null;
  commission_sum?: number | null;
  slippage_cost_sum?: number | null;
  n_trades_sum?: number | null;
  gross_exposure_avg?: number | null;
  net_exposure_avg?: number | null;
}

export interface RollingMetricsResponse {
  points: RollingMetricPointResponse[];
  total_points: number;
  truncated: boolean;
}

export interface JobTriggerResponse {
  jobName: string;
  status: string;
  executionId?: string | null;
  executionName?: string | null;
}

export interface JobControlResponse {
  jobName: string;
  action: 'suspend' | 'resume';
  runningState?: string | null;
}

export interface JobLogRunResponse {
  executionName?: string | null;
  executionId?: string | null;
  status?: string | null;
  startTime?: string | null;
  endTime?: string | null;
  tail: string[];
  error?: string | null;
}

export interface JobLogsResponse {
  jobName: string;
  runsRequested: number;
  runsReturned: number;
  tailLines: number;
  runs: JobLogRunResponse[];
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

export interface TradeResponse {
  execution_date: string;
  symbol: string;
  quantity: number;
  price: number;
  notional: number;
  commission: number;
  slippage_cost: number;
  cash_after: number;
}

export interface TradeListResponse {
  trades: TradeResponse[];
  total: number;
  limit: number;
  offset: number;
}

export type GenericDataRow = Record<string, unknown>;

export interface BacktestSummary {
  run_id?: string;
  run_name?: string;
  start_date?: string;
  end_date?: string;
  total_return?: number;
  annualized_return?: number;
  annualized_volatility?: number;
  sharpe_ratio?: number;
  max_drawdown?: number;
  trades?: number;
  initial_cash?: number;
  final_equity?: number;
  [key: string]: unknown;
}

export interface ListRunsParams {
  status?: RunStatus;
  q?: string;
  limit?: number;
  offset?: number;
}

export interface GetTimeseriesParams {
  source?: DataSource;
  maxPoints?: number;
}

export interface GetRollingParams {
  source?: DataSource;
  windowDays?: number;
  maxPoints?: number;
}

export interface GetTradesParams {
  source?: DataSource;
  limit?: number;
  offset?: number;
}


function getBaseUrl(): string {
  return config.apiBaseUrl;
}

function getApiKey(): string {
  // NOTE: Do not rely on this for production secrets. This is intended for local/dev only.
  return (import.meta.env.VITE_BACKTEST_API_KEY ?? '').trim();
}

function shouldSendApiKey(): boolean {
  const mode = String(import.meta.env.VITE_AUTH_MODE ?? '').trim().toLowerCase();
  if (mode === 'api_key') return true;

  const explicitOverride = String(import.meta.env.VITE_ALLOW_BROWSER_API_KEY ?? '').trim().toLowerCase();
  if (explicitOverride === 'true') return true;

  return Boolean(import.meta.env.DEV);
}

function buildQuery(params: Record<string, string | number | boolean | undefined | null>): string {
  const qs = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    const text = String(value).trim();
    if (!text) return;
    qs.set(key, text);
  });
  const rendered = qs.toString();
  return rendered ? `?${rendered}` : '';
}

async function request(path: string, init: RequestInit = {}): Promise<Response> {
  const baseUrl = getBaseUrl();
  const url = baseUrl ? `${baseUrl}${path}` : path;
  const method = (init.method ?? 'GET').toUpperCase();

  console.info('[backtestApi] request start', {
    method,
    baseUrl,
    path,
    url,
  });

  const headers = new Headers(init.headers);
  if (!headers.has('Accept')) {
    headers.set('Accept', 'application/json');
  }

  if (accessTokenProvider && !headers.has('Authorization')) {
    const token = await accessTokenProvider();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
  }

  const apiKey = getApiKey();
  if (apiKey && shouldSendApiKey() && !headers.has('X-API-Key')) {
    headers.set('X-API-Key', apiKey);
  }

  const resp = await fetch(url, { ...init, headers });
  console.info('[backtestApi] response', {
    method,
    url: resp.url,
    status: resp.status,
  });
  if (!resp.ok) {
    const detail = await resp.text().catch(() => '');
    console.error('[backtestApi] response error', {
      method,
      url: resp.url,
      status: resp.status,
      detail,
    });
    throw new ApiError(resp.status, detail || resp.statusText);
  }
  return resp;
}

async function requestJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const resp = await request(path, init);
  return resp.json() as Promise<T>;
}

export const backtestApi = {
  async listRuns(params: ListRunsParams = {}, signal?: AbortSignal): Promise<RunListResponse> {
    const query = buildQuery({
      status: params.status,
      q: params.q,
      limit: params.limit ?? 200,
      offset: params.offset ?? 0,
    });
    return requestJson<RunListResponse>(`/backtests${query}`, { signal });
  },

  async getSummary(runId: string, params: { source?: DataSource } = {}, signal?: AbortSignal): Promise<BacktestSummary> {
    const query = buildQuery({ source: params.source ?? 'auto' });
    return requestJson<BacktestSummary>(`/backtests/${encodeURIComponent(runId)}/summary${query}`, { signal });
  },

  async getTimeseries(
    runId: string,
    params: GetTimeseriesParams = {},
    signal?: AbortSignal,
  ): Promise<TimeseriesResponse> {
    const query = buildQuery({
      source: params.source ?? 'auto',
      max_points: params.maxPoints ?? 5000,
    });
    return requestJson<TimeseriesResponse>(`/backtests/${encodeURIComponent(runId)}/metrics/timeseries${query}`, { signal });
  },

  async getRolling(
    runId: string,
    params: GetRollingParams = {},
    signal?: AbortSignal,
  ): Promise<RollingMetricsResponse> {
    const query = buildQuery({
      source: params.source ?? 'auto',
      window_days: params.windowDays ?? 63,
      max_points: params.maxPoints ?? 5000,
    });
    return requestJson<RollingMetricsResponse>(`/backtests/${encodeURIComponent(runId)}/metrics/rolling${query}`, { signal });
  },

  async getTrades(runId: string, params: GetTradesParams = {}, signal?: AbortSignal): Promise<TradeListResponse> {
    const query = buildQuery({
      source: params.source ?? 'auto',
      limit: params.limit ?? 2000,
      offset: params.offset ?? 0,
    });
    return requestJson<TradeListResponse>(`/backtests/${encodeURIComponent(runId)}/trades${query}`, { signal });
  },

  async getSystemHealth(signal?: AbortSignal): Promise<SystemHealth> {
    console.info('[backtestApi] getSystemHealth', { baseUrl: getBaseUrl() });
    return requestJson<SystemHealth>('/system/health', { signal });
  },

  async getLineage(signal?: AbortSignal): Promise<unknown> {
    return requestJson<unknown>('/system/lineage', { signal });
  },


  async acknowledgeAlert(alertId: string, signal?: AbortSignal): Promise<unknown> {
    const encoded = encodeURIComponent(alertId);
    return requestJson<unknown>(`/system/alerts/${encoded}/ack`, { method: 'POST', signal });
  },

  async snoozeAlert(
    alertId: string,
    payload: { minutes?: number; until?: string } = {},
    signal?: AbortSignal,
  ): Promise<unknown> {
    const encoded = encodeURIComponent(alertId);
    return requestJson<unknown>(`/system/alerts/${encoded}/snooze`, {
      method: 'POST',
      signal,
      body: JSON.stringify(payload),
      headers: { 'Content-Type': 'application/json' },
    });
  },

  async resolveAlert(alertId: string, signal?: AbortSignal): Promise<unknown> {
    const encoded = encodeURIComponent(alertId);
    return requestJson<unknown>(`/system/alerts/${encoded}/resolve`, { method: 'POST', signal });
  },


  async getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver', signal?: AbortSignal): Promise<MarketData[]> {
    const query = buildQuery({ ticker });
    return requestJson<MarketData[]>(`/data/${layer}/market${query}`, { signal });
  },

  async getStockScreener(
    params: {
      q?: string;
      limit?: number;
      offset?: number;
      asOf?: string;
      sort?: string;
      direction?: 'asc' | 'desc';
    } = {},
    signal?: AbortSignal,
  ): Promise<StockScreenerResponse> {
    const query = buildQuery({
      q: params.q,
      limit: params.limit ?? 250,
      offset: params.offset ?? 0,
      as_of: params.asOf,
      sort: params.sort ?? 'volume',
      direction: params.direction ?? 'desc',
    });
    return requestJson<StockScreenerResponse>(`/data/screener${query}`, { signal });
  },

  async getDomainData(
    ticker: string,
    domain: Exclude<DataDomain, 'market'>,
    layer: 'silver' | 'gold' = 'silver',
    signal?: AbortSignal,
  ): Promise<GenericDataRow[]> {
    const query = buildQuery({ ticker });
    const encoded = encodeURIComponent(domain);
    return requestJson<GenericDataRow[]>(`/data/${layer}/${encoded}${query}`, { signal });
  },

  async getFinanceData(
    ticker: string,
    subDomain: string,
    layer: 'silver' | 'gold' = 'silver',
    signal?: AbortSignal,
  ): Promise<FinanceData[]> {
    const encodedSub = encodeURIComponent(subDomain);
    const query = buildQuery({ ticker });
    return requestJson<FinanceData[]>(`/data/${layer}/finance/${encodedSub}${query}`, { signal });
  },



  async triggerJob(jobName: string, signal?: AbortSignal): Promise<JobTriggerResponse> {
    const encoded = encodeURIComponent(jobName);
    return requestJson<JobTriggerResponse>(`/system/jobs/${encoded}/run`, { method: 'POST', signal });
  },

  async suspendJob(jobName: string, signal?: AbortSignal): Promise<JobControlResponse> {
    const encoded = encodeURIComponent(jobName);
    return requestJson<JobControlResponse>(`/system/jobs/${encoded}/suspend`, { method: 'POST', signal });
  },

  async resumeJob(jobName: string, signal?: AbortSignal): Promise<JobControlResponse> {
    const encoded = encodeURIComponent(jobName);
    return requestJson<JobControlResponse>(`/system/jobs/${encoded}/resume`, { method: 'POST', signal });
  },

  async getJobLogs(
    jobName: string,
    params: { runs?: number } = {},
    signal?: AbortSignal,
  ): Promise<JobLogsResponse> {
    const encoded = encodeURIComponent(jobName);
    const query = buildQuery({ runs: params.runs ?? 1 });
    return requestJson<JobLogsResponse>(`/system/jobs/${encoded}/logs${query}`, { signal });
  },
};
