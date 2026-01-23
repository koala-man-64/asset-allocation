import { useEffect, useMemo, useState } from 'react';

import { ApiError, BacktestSummary, DataSource, ListRunsParams, RollingMetricsResponse, RunListResponse, TimeseriesResponse, TradeListResponse, backtestApi } from '@/services/backtestApi';

class Semaphore {
  private active = 0;
  private readonly queue: Array<() => void> = [];

  constructor(private readonly limit: number) {}

  async withLock<T>(fn: () => Promise<T>): Promise<T> {
    if (this.active >= this.limit) {
      await new Promise<void>((resolve) => this.queue.push(resolve));
    }
    this.active += 1;
    try {
      return await fn();
    } finally {
      this.active -= 1;
      const next = this.queue.shift();
      if (next) next();
    }
  }
}

interface CacheEntry<T> {
  hasData: boolean;
  data?: T;
  error?: unknown;
  promise?: Promise<T>;
}

function getOrInitEntry<T>(cache: Map<string, CacheEntry<T>>, key: string): CacheEntry<T> {
  const existing = cache.get(key);
  if (existing) return existing;
  const created: CacheEntry<T> = { hasData: false };
  cache.set(key, created);
  return created;
}

async function fetchCached<T>(cache: Map<string, CacheEntry<T>>, key: string, fetcher: () => Promise<T>): Promise<T> {
  const entry = getOrInitEntry(cache, key);
  if (entry.hasData) return entry.data as T;
  if (entry.error) throw entry.error;
  if (entry.promise) return entry.promise;

  entry.promise = fetcher()
    .then((data) => {
      entry.data = data;
      entry.hasData = true;
      entry.error = undefined;
      entry.promise = undefined;
      return data;
    })
    .catch((err) => {
      entry.error = err;
      entry.promise = undefined;
      throw err;
    });

  return entry.promise;
}

function toErrorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    return `HTTP ${err.status}: ${err.message || 'Request failed'}`;
  }
  if (err instanceof Error) return err.message;
  return String(err);
}

const runListCache = new Map<string, CacheEntry<RunListResponse>>();
const summaryCache = new Map<string, CacheEntry<BacktestSummary | null>>();
const timeseriesCache = new Map<string, CacheEntry<TimeseriesResponse | null>>();
const rollingCache = new Map<string, CacheEntry<RollingMetricsResponse | null>>();
const tradesCache = new Map<string, CacheEntry<TradeListResponse | null>>();

const summarySemaphore = new Semaphore(4);

function normalizeListRunsParams(params: ListRunsParams): {
  status: NonNullable<ListRunsParams['status']> | null;
  q: NonNullable<ListRunsParams['q']> | null;
  limit: number;
  offset: number;
} {
  return {
    status: params.status ?? null,
    q: params.q ?? null,
    limit: params.limit ?? 200,
    offset: params.offset ?? 0,
  };
}

function runListKey(params: ListRunsParams): string {
  const normalized = normalizeListRunsParams(params);
  return JSON.stringify(normalized);
}

async function fetchRunList(params: ListRunsParams, signal?: AbortSignal): Promise<RunListResponse> {
  return backtestApi.listRuns(params, signal);
}

async function fetchSummary(runId: string, source: DataSource): Promise<BacktestSummary | null> {
  try {
    return await summarySemaphore.withLock(() => backtestApi.getSummary(runId, { source }));
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

async function fetchTimeseries(runId: string, source: DataSource, maxPoints: number): Promise<TimeseriesResponse | null> {
  try {
    return await backtestApi.getTimeseries(runId, { source, maxPoints });
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

async function fetchRolling(
  runId: string,
  source: DataSource,
  windowDays: number,
  maxPoints: number,
): Promise<RollingMetricsResponse | null> {
  try {
    return await backtestApi.getRolling(runId, { source, windowDays, maxPoints });
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

async function fetchTrades(runId: string, source: DataSource, limit: number, offset: number): Promise<TradeListResponse | null> {
  try {
    return await backtestApi.getTrades(runId, { source, limit, offset });
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return null;
    throw err;
  }
}

export function useRunList(params: ListRunsParams = {}, opts: { enabled?: boolean } = {}) {
  const enabled = opts.enabled ?? true;
  const [refreshNonce, setRefreshNonce] = useState(0);
  const key = useMemo(() => runListKey(params), [params.status, params.q, params.limit, params.offset]);

  const [state, setState] = useState<{
    data?: RunListResponse;
    loading: boolean;
    error?: string;
  }>(() => {
    const entry = runListCache.get(key);
    if (entry?.hasData) return { data: entry.data, loading: false };
    if (entry?.error) return { loading: false, error: toErrorMessage(entry.error) };
    return { loading: enabled };
  });

  useEffect(() => {
    if (!enabled) {
      setState((prev) => ({ ...prev, loading: false }));
      return;
    }

    const entry = runListCache.get(key);
    if (entry?.hasData) {
      setState({ data: entry.data, loading: false });
      return;
    }
    if (entry?.error) {
      setState({ loading: false, error: toErrorMessage(entry.error) });
      return;
    }

    const controller = new AbortController();
    setState((prev) => ({ ...prev, loading: true, error: undefined }));
    void fetchCached(runListCache, key, () => fetchRunList(params, controller.signal))
      .then((data) => setState({ data, loading: false }))
      .catch((err) => {
        if (controller.signal.aborted) return;
        setState({ loading: false, error: toErrorMessage(err) });
      });

    return () => controller.abort();
  }, [enabled, key, refreshNonce]);

  const refresh = () => {
    runListCache.delete(key);
    setRefreshNonce((n) => n + 1);
  };

  return {
    response: state.data,
    runs: state.data?.runs ?? [],
    loading: state.loading,
    error: state.error,
    refresh,
  };
}

export function useRunSummary(runId: string | undefined, opts: { enabled?: boolean; source?: DataSource } = {}) {
  const enabled = opts.enabled ?? Boolean(runId);
  const source = opts.source ?? 'auto';
  const key = runId ? `${runId}|${source}` : '';

  const [state, setState] = useState<{
    data?: BacktestSummary | null;
    loading: boolean;
    error?: string;
  }>(() => {
    if (!enabled || !runId) return { loading: false };
    const entry = summaryCache.get(key);
    if (entry?.hasData) return { data: entry.data, loading: false };
    if (entry?.error) return { loading: false, error: toErrorMessage(entry.error) };
    return { loading: true };
  });

  useEffect(() => {
    if (!enabled || !runId) {
      setState({ loading: false });
      return;
    }

    const entry = summaryCache.get(key);
    if (entry?.hasData) {
      setState({ data: entry.data, loading: false });
      return;
    }
    if (entry?.error) {
      setState({ loading: false, error: toErrorMessage(entry.error) });
      return;
    }

    setState((prev) => ({ ...prev, loading: true, error: undefined }));
    void fetchCached(summaryCache, key, () => fetchSummary(runId, source))
      .then((data) => setState({ data, loading: false }))
      .catch((err) => setState({ loading: false, error: toErrorMessage(err) }));
  }, [enabled, key, runId, source]);

  return state;
}

export function useRunSummaries(
  runIds: string[],
  opts: { enabled?: boolean; source?: DataSource; limit?: number } = {},
) {
  const enabled = opts.enabled ?? runIds.length > 0;
  const source = opts.source ?? 'auto';
  const limit = opts.limit;

  const normalizedIds = useMemo(() => {
    const unique = Array.from(new Set(runIds.filter(Boolean)));
    return typeof limit === 'number' ? unique.slice(0, Math.max(0, limit)) : unique;
  }, [runIds.join('|'), limit]);

  const [summaries, setSummaries] = useState<Record<string, BacktestSummary | null | undefined>>({});
  const [loadingCount, setLoadingCount] = useState(0);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (!enabled || normalizedIds.length === 0) {
      setSummaries({});
      setLoadingCount(0);
      setError(undefined);
      return;
    }

    let cancelled = false;
    setError(undefined);

    let pending = 0;
    const updatePending = (delta: number) => {
      pending += delta;
      if (!cancelled) setLoadingCount(pending);
    };

    normalizedIds.forEach((runId) => {
      const key = `${runId}|${source}`;
      const entry = summaryCache.get(key);
      if (entry?.hasData) {
        setSummaries((prev) => ({ ...prev, [runId]: entry.data }));
        return;
      }
      if (entry?.error) {
        setSummaries((prev) => ({ ...prev, [runId]: undefined }));
        setError(toErrorMessage(entry.error));
        return;
      }

      updatePending(1);
      void fetchCached(summaryCache, key, () => fetchSummary(runId, source))
        .then((data) => {
          if (cancelled) return;
          setSummaries((prev) => ({ ...prev, [runId]: data }));
        })
        .catch((err) => {
          if (cancelled) return;
          setSummaries((prev) => ({ ...prev, [runId]: undefined }));
          setError(toErrorMessage(err));
        })
        .finally(() => updatePending(-1));
    });

    return () => {
      cancelled = true;
    };
  }, [enabled, normalizedIds.join('|'), source]);

  return {
    summaries,
    loading: loadingCount > 0,
    error,
  };
}

export function useTimeseriesMulti(
  runIds: string[],
  opts: { enabled?: boolean; source?: DataSource; maxPoints?: number } = {},
) {
  const enabled = opts.enabled ?? runIds.length > 0;
  const source = opts.source ?? 'auto';
  const maxPoints = opts.maxPoints ?? 5000;

  const normalizedIds = useMemo(() => Array.from(new Set(runIds.filter(Boolean))), [runIds.join('|')]);

  const [timeseriesByRunId, setTimeseriesByRunId] = useState<Record<string, TimeseriesResponse | null | undefined>>({});
  const [loadingCount, setLoadingCount] = useState(0);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (!enabled || normalizedIds.length === 0) {
      setTimeseriesByRunId({});
      setLoadingCount(0);
      setError(undefined);
      return;
    }

    let cancelled = false;
    setError(undefined);

    let pending = 0;
    const updatePending = (delta: number) => {
      pending += delta;
      if (!cancelled) setLoadingCount(pending);
    };

    normalizedIds.forEach((runId) => {
      const key = `${runId}|${source}|${maxPoints}`;
      const entry = timeseriesCache.get(key);
      if (entry?.hasData) {
        setTimeseriesByRunId((prev) => ({ ...prev, [runId]: entry.data }));
        return;
      }
      if (entry?.error) {
        setTimeseriesByRunId((prev) => ({ ...prev, [runId]: undefined }));
        setError(toErrorMessage(entry.error));
        return;
      }

      updatePending(1);
      void fetchCached(timeseriesCache, key, () => fetchTimeseries(runId, source, maxPoints))
        .then((data) => {
          if (cancelled) return;
          setTimeseriesByRunId((prev) => ({ ...prev, [runId]: data }));
        })
        .catch((err) => {
          if (cancelled) return;
          setTimeseriesByRunId((prev) => ({ ...prev, [runId]: undefined }));
          setError(toErrorMessage(err));
        })
        .finally(() => updatePending(-1));
    });

    return () => {
      cancelled = true;
    };
  }, [enabled, normalizedIds.join('|'), source, maxPoints]);

  return {
    timeseriesByRunId,
    loading: loadingCount > 0,
    error,
  };
}

export function useRollingMulti(
  runIds: string[],
  windowDays: number,
  opts: { enabled?: boolean; source?: DataSource; maxPoints?: number } = {},
) {
  const enabled = opts.enabled ?? runIds.length > 0;
  const source = opts.source ?? 'auto';
  const maxPoints = opts.maxPoints ?? 5000;

  const normalizedIds = useMemo(() => Array.from(new Set(runIds.filter(Boolean))), [runIds.join('|')]);

  const [rollingByRunId, setRollingByRunId] = useState<Record<string, RollingMetricsResponse | null | undefined>>({});
  const [loadingCount, setLoadingCount] = useState(0);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (!enabled || normalizedIds.length === 0) {
      setRollingByRunId({});
      setLoadingCount(0);
      setError(undefined);
      return;
    }

    let cancelled = false;
    setError(undefined);

    let pending = 0;
    const updatePending = (delta: number) => {
      pending += delta;
      if (!cancelled) setLoadingCount(pending);
    };

    normalizedIds.forEach((runId) => {
      const key = `${runId}|${source}|${windowDays}|${maxPoints}`;
      const entry = rollingCache.get(key);
      if (entry?.hasData) {
        setRollingByRunId((prev) => ({ ...prev, [runId]: entry.data }));
        return;
      }
      if (entry?.error) {
        setRollingByRunId((prev) => ({ ...prev, [runId]: undefined }));
        setError(toErrorMessage(entry.error));
        return;
      }

      updatePending(1);
      void fetchCached(rollingCache, key, () => fetchRolling(runId, source, windowDays, maxPoints))
        .then((data) => {
          if (cancelled) return;
          setRollingByRunId((prev) => ({ ...prev, [runId]: data }));
        })
        .catch((err) => {
          if (cancelled) return;
          setRollingByRunId((prev) => ({ ...prev, [runId]: undefined }));
          setError(toErrorMessage(err));
        })
        .finally(() => updatePending(-1));
    });

    return () => {
      cancelled = true;
    };
  }, [enabled, normalizedIds.join('|'), source, windowDays, maxPoints]);

  return {
    rollingByRunId,
    loading: loadingCount > 0,
    error,
  };
}

export function useTrades(
  runId: string | undefined,
  opts: { enabled?: boolean; source?: DataSource; limit?: number; offset?: number } = {},
) {
  const enabled = opts.enabled ?? Boolean(runId);
  const source = opts.source ?? 'auto';
  const limit = opts.limit ?? 2000;
  const offset = opts.offset ?? 0;
  const key = runId ? `${runId}|${source}|${limit}|${offset}` : '';

  const [state, setState] = useState<{
    data?: TradeListResponse | null;
    loading: boolean;
    error?: string;
  }>(() => {
    if (!enabled || !runId) return { loading: false };
    const entry = tradesCache.get(key);
    if (entry?.hasData) return { data: entry.data, loading: false };
    if (entry?.error) return { loading: false, error: toErrorMessage(entry.error) };
    return { loading: true };
  });

  useEffect(() => {
    if (!enabled || !runId) {
      setState({ loading: false });
      return;
    }

    const entry = tradesCache.get(key);
    if (entry?.hasData) {
      setState({ data: entry.data, loading: false });
      return;
    }
    if (entry?.error) {
      setState({ loading: false, error: toErrorMessage(entry.error) });
      return;
    }

    setState((prev) => ({ ...prev, loading: true, error: undefined }));
    void fetchCached(tradesCache, key, () => fetchTrades(runId, source, limit, offset))
      .then((data) => setState({ data, loading: false }))
      .catch((err) => setState({ loading: false, error: toErrorMessage(err) }));
  }, [enabled, key, runId, source, limit, offset]);

  return state;
}
