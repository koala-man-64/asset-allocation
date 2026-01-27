/* global RequestInit */
import type { FinanceData, MarketData } from '@/types/data';
import type { StrategyRun, StressEvent, SystemHealth, TradingSignal } from '@/types/strategy';
import { config } from '@/config';

type AccessTokenProvider = () => Promise<string | null>;

let accessTokenProvider: AccessTokenProvider | null = null;

interface WindowWithConfig extends Window {
  __API_UI_CONFIG__?: { apiBaseUrl?: string; debugApi?: unknown };
}

const runtimeConfig = (window as WindowWithConfig).__API_UI_CONFIG__ || {};

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
  const candidates = [runtimeConfig.debugApi, import.meta.env.VITE_DEBUG_API, queryFlag, localStorageFlag];
  const explicitFlag = candidates.find((value) => {
    if (value === undefined || value === null) return false;
    return String(value).trim() !== '';
  });
  if (explicitFlag === undefined) return true;
  return isTruthy(explicitFlag);
})();

const apiLogPrefix = '[API]';

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
    runtimeBaseUrl: runtimeConfig.apiBaseUrl,
    envBaseUrl: import.meta.env.VITE_API_BASE_URL,
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

export type DataDomain = 'market' | 'earnings' | 'price-target' | string;

export interface JobTriggerResponse {
  jobName: string;
  status: string;
  executionId?: string | null;
  executionName?: string | null;
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

export type GenericDataRow = Record<string, unknown>;

function getBaseUrl(): string {
  return config.apiBaseUrl;
}

function getApiKey(): string {
  // NOTE: Do not rely on this for production secrets. This is intended for local/dev only.
  return (import.meta.env.VITE_API_KEY ?? '').trim();
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

  logApi('request start', {
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
  logApi('response', {
    method,
    url: resp.url,
    status: resp.status,
  });
  if (!resp.ok) {
    const detail = await resp.text().catch(() => '');
    logApi('response error', {
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

export const apiClient = {
  async getSystemHealth(signal?: AbortSignal): Promise<SystemHealth> {
    logApi('getSystemHealth', { baseUrl: getBaseUrl() });
    return requestJson<SystemHealth>('/system/health', { signal });
  },

  async getLineage(signal?: AbortSignal): Promise<unknown> {
    return requestJson<unknown>('/system/lineage', { signal });
  },

  async getSignals(
    params: { date?: string; limit?: number } = {},
    signal?: AbortSignal,
  ): Promise<TradingSignal[]> {
    const query = buildQuery({
      date: params.date,
      limit: params.limit ?? 500,
    });
    return requestJson<TradingSignal[]>(`/ranking/signals${query}`, { signal });
  },

  async getStrategies(signal?: AbortSignal): Promise<StrategyRun[]> {
    return requestJson<StrategyRun[]>('/ranking/strategies', { signal });
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

  async getStressEvents(_signal?: AbortSignal): Promise<StressEvent[]> {
    return [];
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

  async triggerJob(jobName: string, signal?: AbortSignal): Promise<JobTriggerResponse> {
    const encoded = encodeURIComponent(jobName);
    return requestJson<JobTriggerResponse>(`/system/jobs/${encoded}/run`, { method: 'POST', signal });
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
