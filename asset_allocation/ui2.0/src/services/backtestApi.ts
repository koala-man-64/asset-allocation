export type RunStatus = 'queued' | 'running' | 'completed' | 'failed';
export type DataSource = 'auto' | 'local' | 'adls';

type AccessTokenProvider = () => Promise<string | null>;

let accessTokenProvider: AccessTokenProvider | null = null;

type RuntimeConfig = {
  backtestApiBaseUrl?: string;
};

function getRuntimeConfig(): RuntimeConfig {
  return (window.__BACKTEST_UI_CONFIG__ as RuntimeConfig | undefined) ?? {};
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
  const runtime = getRuntimeConfig();
  const raw = String(runtime.backtestApiBaseUrl ?? import.meta.env.VITE_BACKTEST_API_BASE_URL ?? '').trim();
  return raw.replace(/\/+$/, '');
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
  if (!resp.ok) {
    const detail = await resp.text().catch(() => '');
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
};
