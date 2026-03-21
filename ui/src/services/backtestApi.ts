export { ApiError } from '@/services/apiService';
import { request as apiRequest } from '@/services/apiService';

export type RunStatus = 'queued' | 'running' | 'completed' | 'failed';
export type DataSource = 'auto' | 'local' | 'adls';
export type DataDomain = 'market' | 'earnings' | 'price-target';

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
  action: 'suspend' | 'resume' | 'stop';
  command?: 'suspend' | 'resume' | 'stop' | 'run';
  runningState?: string | null;
}

export interface JobConsoleLogEntry {
  timestamp?: string | null;
  stream_s?: string | null;
  executionName?: string | null;
  message: string;
}

export interface JobLogRunResponse {
  executionName?: string | null;
  executionId?: string | null;
  status?: string | null;
  startTime?: string | null;
  endTime?: string | null;
  tail: string[];
  consoleLogs?: JobConsoleLogEntry[];
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

export interface SubmitBacktestPayload {
  strategyName: string;
  strategyVersion?: number;
  startTs: string;
  endTs: string;
  barSize: string;
  runName?: string;
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

export const backtestApi = {
  async submitRun(
    payload: SubmitBacktestPayload,
    signal?: AbortSignal
  ): Promise<RunRecordResponse> {
    return apiRequest<RunRecordResponse>('/backtests', {
      method: 'POST',
      body: JSON.stringify(payload),
      signal
    });
  },

  async listRuns(params: ListRunsParams = {}, signal?: AbortSignal): Promise<RunListResponse> {
    return apiRequest<RunListResponse>('/backtests', {
      params: {
        status: params.status,
        q: params.q,
        limit: params.limit ?? 200,
        offset: params.offset ?? 0
      },
      signal
    });
  },

  async getSummary(
    runId: string,
    params: { source?: DataSource } = {},
    signal?: AbortSignal
  ): Promise<BacktestSummary> {
    return apiRequest<BacktestSummary>(`/backtests/${encodeURIComponent(runId)}/summary`, {
      params: { source: params.source ?? 'auto' },
      signal
    });
  },

  async getTimeseries(
    runId: string,
    params: GetTimeseriesParams = {},
    signal?: AbortSignal
  ): Promise<TimeseriesResponse> {
    return apiRequest<TimeseriesResponse>(
      `/backtests/${encodeURIComponent(runId)}/metrics/timeseries`,
      {
        params: {
          source: params.source ?? 'auto',
          max_points: params.maxPoints ?? 5000
        },
        signal
      }
    );
  },

  async getRolling(
    runId: string,
    params: GetRollingParams = {},
    signal?: AbortSignal
  ): Promise<RollingMetricsResponse> {
    return apiRequest<RollingMetricsResponse>(
      `/backtests/${encodeURIComponent(runId)}/metrics/rolling`,
      {
        params: {
          source: params.source ?? 'auto',
          window_days: params.windowDays ?? 63,
          max_points: params.maxPoints ?? 5000
        },
        signal
      }
    );
  },

  async getTrades(
    runId: string,
    params: GetTradesParams = {},
    signal?: AbortSignal
  ): Promise<TradeListResponse> {
    return apiRequest<TradeListResponse>(`/backtests/${encodeURIComponent(runId)}/trades`, {
      params: {
        source: params.source ?? 'auto',
        limit: params.limit ?? 2000,
        offset: params.offset ?? 0
      },
      signal
    });
  },

  async triggerJob(jobName: string, signal?: AbortSignal): Promise<JobTriggerResponse> {
    const encoded = encodeURIComponent(jobName);
    return apiRequest<JobTriggerResponse>(`/system/jobs/${encoded}/run`, {
      method: 'POST',
      signal
    });
  },

  async suspendJob(jobName: string, signal?: AbortSignal): Promise<JobControlResponse> {
    const encoded = encodeURIComponent(jobName);
    return apiRequest<JobControlResponse>(`/system/jobs/${encoded}/suspend`, {
      method: 'POST',
      signal
    });
  },

  async stopJob(jobName: string, signal?: AbortSignal): Promise<JobControlResponse> {
    const encoded = encodeURIComponent(jobName);
    return apiRequest<JobControlResponse>(`/system/jobs/${encoded}/stop`, {
      method: 'POST',
      signal
    });
  },

  async resumeJob(jobName: string, signal?: AbortSignal): Promise<JobControlResponse> {
    const encoded = encodeURIComponent(jobName);
    return apiRequest<JobControlResponse>(`/system/jobs/${encoded}/resume`, {
      method: 'POST',
      signal
    });
  },

  async getJobLogs(
    jobName: string,
    params: { runs?: number } = {},
    signal?: AbortSignal
  ): Promise<JobLogsResponse> {
    const encoded = encodeURIComponent(jobName);
    return apiRequest<JobLogsResponse>(`/system/jobs/${encoded}/logs`, {
      params: { runs: params.runs ?? 1 },
      signal
    });
  }
};
