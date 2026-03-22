import { useEffect, useMemo } from 'react';
import { useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';

import {
  hasActiveJobRunningState,
  normalizeAzureJobName
} from '@/app/components/pages/system-status/SystemStatusHelpers';
import { queryKeys } from '@/hooks/useDataQueries';
import type { JobTriggerResponse } from '@/services/backtestApi';
import type { JobRun, ResourceHealth, SystemHealth } from '@/types/strategy';

const SYSTEM_HEALTH_JOB_OVERRIDE_TTL_MS = 2 * 60 * 1000;
const SYSTEM_HEALTH_JOB_OVERRIDE_STORAGE_KEY = 'asset-allocation.systemHealthJobOverrides';
const RUNNING_JOB_STATUS: JobRun['status'] = 'running';
const RUNNING_RESOURCE_STATE = 'Running';
const MANUAL_TRIGGER_SOURCE = 'manual';
const SERVER_CATCH_UP_STATUSES = new Set([
  'pending',
  'running',
  'success',
  'succeeded',
  'warning',
  'succeededwithwarnings',
  'failed',
  'error'
]);

export interface SystemHealthJobOverride {
  jobName: string;
  jobKey: string;
  status: JobRun['status'];
  runningState: string;
  startTime: string;
  triggeredBy: string;
  executionId?: string | null;
  executionName?: string | null;
  expiresAt: string;
}

export type SystemHealthJobOverrideMap = Record<string, SystemHealthJobOverride>;

function readStoredJobOverrides(): SystemHealthJobOverrideMap {
  if (typeof window === 'undefined') {
    return {};
  }

  try {
    const raw = window.sessionStorage.getItem(SYSTEM_HEALTH_JOB_OVERRIDE_STORAGE_KEY);
    if (!raw) {
      return {};
    }

    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      window.sessionStorage.removeItem(SYSTEM_HEALTH_JOB_OVERRIDE_STORAGE_KEY);
      return {};
    }

    return parsed as SystemHealthJobOverrideMap;
  } catch {
    try {
      window.sessionStorage.removeItem(SYSTEM_HEALTH_JOB_OVERRIDE_STORAGE_KEY);
    } catch {
      // Ignore storage cleanup failures and fall back to the in-memory query state.
    }
    return {};
  }
}

function writeStoredJobOverrides(overrides?: SystemHealthJobOverrideMap): void {
  if (typeof window === 'undefined') {
    return;
  }

  const activeOverrides = activeOverrideMap(overrides);
  try {
    if (Object.keys(activeOverrides).length === 0) {
      window.sessionStorage.removeItem(SYSTEM_HEALTH_JOB_OVERRIDE_STORAGE_KEY);
      return;
    }

    window.sessionStorage.setItem(
      SYSTEM_HEALTH_JOB_OVERRIDE_STORAGE_KEY,
      JSON.stringify(activeOverrides)
    );
  } catch {
    // Ignore storage failures and keep the query cache as the source of truth.
  }
}

function runStartEpoch(raw?: string | null): number {
  const value = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
}

function toJobKey(jobName: string): string {
  return (
    normalizeAzureJobName(jobName) ||
    String(jobName || '')
      .trim()
      .toLowerCase()
  );
}

function hasRunningState(raw?: string | null): boolean {
  return hasActiveJobRunningState(raw);
}

function activeOverrideMap(
  overrides?: SystemHealthJobOverrideMap,
  nowMs: number = Date.now()
): SystemHealthJobOverrideMap {
  const next: SystemHealthJobOverrideMap = {};
  for (const override of Object.values(overrides || {})) {
    if (!override?.jobKey) continue;
    const expiresAtMs = Date.parse(String(override.expiresAt || ''));
    if (Number.isFinite(expiresAtMs) && expiresAtMs < nowMs) continue;
    next[override.jobKey] = override;
  }
  return next;
}

function resolveActiveOverrideSnapshot(
  current?: SystemHealthJobOverrideMap
): SystemHealthJobOverrideMap {
  return activeOverrideMap({
    ...readStoredJobOverrides(),
    ...(current || {})
  });
}

function nextExpirationDelayMs(overrides?: SystemHealthJobOverrideMap): number | null {
  let nextDelayMs: number | null = null;
  for (const override of Object.values(overrides || {})) {
    const expiresAtMs = Date.parse(String(override?.expiresAt || ''));
    if (!Number.isFinite(expiresAtMs)) continue;
    const delayMs = Math.max(0, expiresAtMs - Date.now());
    if (nextDelayMs === null || delayMs < nextDelayMs) {
      nextDelayMs = delayMs;
    }
  }
  return nextDelayMs;
}

function latestRecentJob(recentJobs: JobRun[], jobKey: string): JobRun | undefined {
  let latest: JobRun | undefined;
  for (const job of recentJobs) {
    if (toJobKey(String(job?.jobName || '')) !== jobKey) continue;
    if (!latest || runStartEpoch(job.startTime) > runStartEpoch(latest.startTime)) {
      latest = job;
    }
  }
  return latest;
}

function resourceForJob(
  resources: ResourceHealth[] | undefined,
  jobKey: string
): ResourceHealth | undefined {
  return (resources || []).find((resource) => toJobKey(String(resource?.name || '')) === jobKey);
}

function jobReflectsServerState(
  job: JobRun | undefined,
  override: SystemHealthJobOverride
): boolean {
  if (!job) return false;
  const status = String(job.status || '')
    .trim()
    .toLowerCase();
  if (!SERVER_CATCH_UP_STATUSES.has(status)) return false;
  return runStartEpoch(job.startTime) >= runStartEpoch(override.startTime);
}

function resourceReflectsServerState(
  resource: ResourceHealth | undefined,
  override: SystemHealthJobOverride
): boolean {
  if (!resource) return false;
  if (hasRunningState(resource.runningState)) return true;
  return runStartEpoch(resource.lastModifiedAt) >= runStartEpoch(override.startTime);
}

function optimisticJobRun(override: SystemHealthJobOverride, recentJobs: JobRun[]): JobRun {
  const existing = latestRecentJob(recentJobs, override.jobKey);
  return {
    jobName: override.jobName,
    jobType: existing?.jobType ?? 'data-ingest',
    status: RUNNING_JOB_STATUS,
    startTime: override.startTime,
    duration: existing?.duration,
    recordsProcessed: existing?.recordsProcessed,
    gitSha: existing?.gitSha,
    triggeredBy: override.triggeredBy,
    warnings: existing?.warnings,
    metadata: existing?.metadata
  };
}

export function mergeSystemHealthWithJobOverrides(
  data: SystemHealth | undefined,
  overrides?: SystemHealthJobOverrideMap
): SystemHealth | undefined {
  if (!data) return data;

  const activeOverrides = Object.values(activeOverrideMap(overrides)).sort(
    (left, right) => runStartEpoch(right.startTime) - runStartEpoch(left.startTime)
  );
  if (activeOverrides.length === 0) return data;

  const pendingOverrides = new Map<string, SystemHealthJobOverride>();
  for (const override of activeOverrides) {
    const recentJob = latestRecentJob(data.recentJobs, override.jobKey);
    const resource = resourceForJob(data.resources, override.jobKey);
    if (
      jobReflectsServerState(recentJob, override) ||
      resourceReflectsServerState(resource, override)
    ) {
      continue;
    }
    if (!pendingOverrides.has(override.jobKey)) {
      pendingOverrides.set(override.jobKey, override);
    }
  }

  if (pendingOverrides.size === 0) return data;

  const optimisticRuns = Array.from(pendingOverrides.values()).map((override) =>
    optimisticJobRun(override, data.recentJobs)
  );
  const resources = data.resources?.map((resource) => {
    const override = pendingOverrides.get(toJobKey(String(resource?.name || '')));
    if (!override || hasRunningState(resource.runningState)) {
      return resource;
    }
    return {
      ...resource,
      runningState: override.runningState,
      lastModifiedAt: override.startTime || resource.lastModifiedAt || null
    };
  });

  return {
    ...data,
    recentJobs: [...optimisticRuns, ...data.recentJobs],
    resources
  };
}

export function upsertRunningJobOverride(
  queryClient: QueryClient,
  payload: {
    jobName: string;
    startTime?: string;
    triggeredBy?: string;
    response?: Pick<JobTriggerResponse, 'executionId' | 'executionName'>;
  }
): SystemHealthJobOverride | null {
  const jobName = String(payload.jobName || '').trim();
  const jobKey = toJobKey(jobName);
  if (!jobName || !jobKey) return null;

  const startTime = payload.startTime || new Date().toISOString();
  const override: SystemHealthJobOverride = {
    jobName,
    jobKey,
    status: RUNNING_JOB_STATUS,
    runningState: RUNNING_RESOURCE_STATE,
    startTime,
    triggeredBy: payload.triggeredBy || MANUAL_TRIGGER_SOURCE,
    executionId: payload.response?.executionId ?? null,
    executionName: payload.response?.executionName ?? null,
    expiresAt: new Date(Date.now() + SYSTEM_HEALTH_JOB_OVERRIDE_TTL_MS).toISOString()
  };

  queryClient.setQueryData<SystemHealthJobOverrideMap>(
    queryKeys.systemHealthJobOverrides(),
    (current) => {
      const next = {
        ...resolveActiveOverrideSnapshot(current),
        [jobKey]: override
      };
      writeStoredJobOverrides(next);
      return next;
    }
  );

  return override;
}

export function clearJobOverride(queryClient: QueryClient, jobName: string): void {
  const jobKey = toJobKey(jobName);
  if (!jobKey) return;

  queryClient.setQueryData<SystemHealthJobOverrideMap>(
    queryKeys.systemHealthJobOverrides(),
    (current) => {
      const next = resolveActiveOverrideSnapshot(current);
      if (!(jobKey in next)) {
        writeStoredJobOverrides(next);
        return next;
      }
      delete next[jobKey];
      writeStoredJobOverrides(next);
      return next;
    }
  );
}

export function useSystemHealthJobOverrides() {
  const queryClient = useQueryClient();
  const query = useQuery<SystemHealthJobOverrideMap>({
    queryKey: queryKeys.systemHealthJobOverrides(),
    queryFn: async () =>
      resolveActiveOverrideSnapshot(
        queryClient.getQueryData<SystemHealthJobOverrideMap>(queryKeys.systemHealthJobOverrides())
      ),
    initialData: () =>
      resolveActiveOverrideSnapshot(
        queryClient.getQueryData<SystemHealthJobOverrideMap>(queryKeys.systemHealthJobOverrides())
      ),
    staleTime: Infinity,
    gcTime: Infinity,
    refetchOnMount: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  useEffect(() => {
    const timeoutMs = nextExpirationDelayMs(query.data);
    if (timeoutMs === null) {
      return undefined;
    }

    const handle = window.setTimeout(() => {
      queryClient.setQueryData<SystemHealthJobOverrideMap>(
        queryKeys.systemHealthJobOverrides(),
        (current) => {
          const next = activeOverrideMap(current);
          writeStoredJobOverrides(next);
          return next;
        }
      );
    }, timeoutMs + 25);

    return () => {
      window.clearTimeout(handle);
    };
  }, [query.data, queryClient]);

  useEffect(() => {
    writeStoredJobOverrides(query.data);
  }, [query.data]);

  const data = useMemo(() => activeOverrideMap(query.data), [query.data]);
  return {
    ...query,
    data
  };
}
