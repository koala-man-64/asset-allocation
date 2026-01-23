import { useQuery, useQueries, keepPreviousData } from '@tanstack/react-query';
import { BacktestSummary, DataSource, ListRunsParams, backtestApi } from '@/services/backtestApi';

// Key Factory for consistent query keys
export const backtestKeys = {
  all: ['backtest'] as const,
  runs: () => [...backtestKeys.all, 'runs'] as const,
  runList: (params: ListRunsParams) => [...backtestKeys.runs(), params] as const,
  run: (runId: string) => [...backtestKeys.runs(), runId] as const,
  summary: (runId: string, source: DataSource) => [...backtestKeys.run(runId), 'summary', source] as const,
  timeseries: (runId: string, source: DataSource, maxPoints: number) => [...backtestKeys.run(runId), 'timeseries', source, maxPoints] as const,
  rolling: (runId: string, source: DataSource, windowDays: number, maxPoints: number) => [...backtestKeys.run(runId), 'rolling', source, windowDays, maxPoints] as const,
  trades: (runId: string, source: DataSource, limit: number, offset: number) => [...backtestKeys.run(runId), 'trades', source, limit, offset] as const,
};

export function useRunList(params: ListRunsParams = {}, opts: { enabled?: boolean } = {}) {
  const { data, error, isLoading, refetch } = useQuery({
    queryKey: backtestKeys.runList(params),
    queryFn: ({ signal }) => backtestApi.listRuns(params, signal),
    enabled: opts.enabled ?? true,
    placeholderData: keepPreviousData,
  });

  return {
    response: data,
    runs: data?.runs ?? [],
    loading: isLoading,
    error: error ? String(error) : undefined,
    refresh: refetch,
  };
}

export function useRunSummary(runId: string | undefined, opts: { enabled?: boolean; source?: DataSource } = {}) {
  const source = opts.source ?? 'auto';
  const enabled = (opts.enabled ?? true) && !!runId;

  const { data, error, isLoading } = useQuery({
    queryKey: backtestKeys.summary(runId!, source),
    queryFn: ({ signal }) => backtestApi.getSummary(runId!, { source }, signal),
    enabled,
    retry: (failureCount, error: any) => {
      // Don't retry 404s
      if (error?.status === 404) return false;
      return failureCount < 3;
    }
  });

  return {
    data,
    loading: isLoading,
    error: error ? String(error) : undefined,
  };
}

export function useRunSummaries(
  runIds: string[],
  opts: { enabled?: boolean; source?: DataSource; limit?: number } = {},
) {
  const source = opts.source ?? 'auto';
  const limit = opts.limit;

  // Normalize and limit IDs
  const uniqueIds = Array.from(new Set(runIds.filter(Boolean)));
  const targetIds = typeof limit === 'number' ? uniqueIds.slice(0, Math.max(0, limit)) : uniqueIds;
  const enabled = (opts.enabled ?? true) && targetIds.length > 0;

  const results = useQueries({
    queries: targetIds.map(runId => ({
      queryKey: backtestKeys.summary(runId, source),
      queryFn: ({ signal }: { signal: AbortSignal }) => backtestApi.getSummary(runId, { source }, signal),
      enabled,
      retry: (failureCount: number, error: any) => {
        if (error?.status === 404) return false;
        return failureCount < 3;
      }
    }))
  });

  const summaries: Record<string, BacktestSummary | null | undefined> = {};
  let loading = false;
  let error: string | undefined;

  results.forEach((result, index) => {
    const runId = targetIds[index];
    summaries[runId] = result.data;
    if (result.isLoading) loading = true;
    if (result.error) error = String(result.error);
  });

  return { summaries, loading, error };
}

export function useTimeseriesMulti(
  runIds: string[],
  opts: { enabled?: boolean; source?: DataSource; maxPoints?: number } = {},
) {
  const source = opts.source ?? 'auto';
  const maxPoints = opts.maxPoints ?? 5000;
  const uniqueIds = Array.from(new Set(runIds.filter(Boolean)));
  const enabled = (opts.enabled ?? true) && uniqueIds.length > 0;

  const results = useQueries({
    queries: uniqueIds.map(runId => ({
      queryKey: backtestKeys.timeseries(runId, source, maxPoints),
      queryFn: ({ signal }: { signal: AbortSignal }) => backtestApi.getTimeseries(runId, { source, maxPoints }, signal),
      enabled,
      retry: (failureCount: number, error: any) => {
        if (error?.status === 404) return false;
        return failureCount < 3;
      }
    }))
  });

  const timeseriesByRunId: Record<string, any> = {}; // using any to match loose types from before if needed, or specific type
  let loading = false;
  let error: string | undefined;

  results.forEach((result, index) => {
    const runId = uniqueIds[index];
    timeseriesByRunId[runId] = result.data;
    if (result.isLoading) loading = true;
    if (result.error) error = String(result.error);
  });

  return { timeseriesByRunId, loading, error };
}

export function useRollingMulti(
  runIds: string[],
  windowDays: number,
  opts: { enabled?: boolean; source?: DataSource; maxPoints?: number } = {},
) {
  const source = opts.source ?? 'auto';
  const maxPoints = opts.maxPoints ?? 5000;
  const uniqueIds = Array.from(new Set(runIds.filter(Boolean)));
  const enabled = (opts.enabled ?? true) && uniqueIds.length > 0;

  const results = useQueries({
    queries: uniqueIds.map(runId => ({
      queryKey: backtestKeys.rolling(runId, source, windowDays, maxPoints),
      queryFn: ({ signal }: { signal: AbortSignal }) => backtestApi.getRolling(runId, { source, windowDays, maxPoints }, signal),
      enabled,
      retry: (failureCount: number, error: any) => {
        if (error?.status === 404) return false;
        return failureCount < 3;
      }
    }))
  });

  const rollingByRunId: Record<string, any> = {};
  let loading = false;
  let error: string | undefined;

  results.forEach((result, index) => {
    const runId = uniqueIds[index];
    rollingByRunId[runId] = result.data;
    if (result.isLoading) loading = true;
    if (result.error) error = String(result.error);
  });

  return { rollingByRunId, loading, error };
}

export function useTrades(
  runId: string | undefined,
  opts: { enabled?: boolean; source?: DataSource; limit?: number; offset?: number } = {},
) {
  const source = opts.source ?? 'auto';
  const limit = opts.limit ?? 2000;
  const offset = opts.offset ?? 0;
  const enabled = (opts.enabled ?? true) && !!runId;

  const { data, error, isLoading } = useQuery({
    queryKey: backtestKeys.trades(runId!, source, limit, offset),
    queryFn: ({ signal }) => backtestApi.getTrades(runId!, { source, limit, offset }, signal),
    enabled,
  });

  return {
    data,
    loading: isLoading,
    error: error ? String(error) : undefined,
  };
}
