import React, { useEffect, useMemo, useState, lazy, Suspense } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useSystemHealthQuery, queryKeys } from '@/hooks/useDataQueries';
import {
  mergeSystemHealthWithJobOverrides,
  useSystemHealthJobOverrides
} from '@/hooks/useSystemHealthJobOverrides';
import { DataService } from '@/services/DataService';
import { ErrorBoundary } from '@/app/components/common/ErrorBoundary';
import { Skeleton } from '@/app/components/ui/skeleton';
import { PageLoader } from '@/app/components/common/PageLoader';
import type { ManagedContainerJob } from './system-status/JobKillSwitchPanel';
import type { SystemHealth } from '@/types/strategy';
import type { JobLogStreamTarget } from './system-status/JobLogStreamPanel';

// Lazy load components to reduce initial bundle size of the page
const DomainLayerComparisonPanel = lazy(() =>
  import('./system-status/DomainLayerComparisonPanel').then((m) => ({
    default: m.DomainLayerComparisonPanel
  }))
);
const ContainerAppsPanel = lazy(() =>
  import('./system-status/ContainerAppsPanel').then((m) => ({ default: m.ContainerAppsPanel }))
);
const JobLogStreamPanel = lazy(() =>
  import('./system-status/JobLogStreamPanel').then((m) => ({ default: m.JobLogStreamPanel }))
);

import { buildLatestJobRunIndex, normalizeAzureJobName } from './system-status/SystemStatusHelpers';
import { normalizeDomainKey } from './system-status/SystemPurgeControls';

const JOB_STATUS_POLL_INTERVAL_MS = 10_000;

type JobPollingSnapshot = Pick<SystemHealth, 'overall' | 'recentJobs' | 'resources'>;

function pickJobPollingSnapshot(payload?: SystemHealth | null): JobPollingSnapshot | null {
  if (!payload) return null;
  return {
    overall: payload.overall,
    recentJobs: payload.recentJobs || [],
    resources: payload.resources || []
  };
}

export function SystemStatusPage() {
  const { data, isLoading, error, isFetching } = useSystemHealthQuery({
    autoRefresh: false
  });
  const jobOverrides = useSystemHealthJobOverrides();
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [jobPollingSnapshot, setJobPollingSnapshot] = useState<JobPollingSnapshot | null>(null);
  const hasSystemHealth = Boolean(data);
  const hasSystemHealthError = Boolean(error);

  useEffect(() => {
    if (!hasSystemHealth || hasSystemHealthError) {
      setJobPollingSnapshot(null);
      return;
    }

    let disposed = false;
    let pollingInFlight = false;

    const pollJobStatus = async () => {
      if (pollingInFlight || disposed) return;
      pollingInFlight = true;
      try {
        const fresh = await DataService.getSystemHealth({ refresh: true });
        if (!disposed) {
          setJobPollingSnapshot(pickJobPollingSnapshot(fresh));
        }
      } catch (err) {
        if (!disposed) {
          console.error('[SystemStatusPage] job status poll failed', err);
        }
      } finally {
        pollingInFlight = false;
      }
    };

    void pollJobStatus();

    const handle = window.setInterval(() => {
      void pollJobStatus();
    }, JOB_STATUS_POLL_INTERVAL_MS);

    return () => {
      disposed = true;
      window.clearInterval(handle);
    };
  }, [hasSystemHealth, hasSystemHealthError]);

  const systemHealthWithPolledJobs = useMemo<SystemHealth | undefined>(() => {
    if (!data) return data;
    if (!jobPollingSnapshot) return data;
    return {
      ...data,
      overall: jobPollingSnapshot.overall,
      recentJobs: jobPollingSnapshot.recentJobs,
      resources: jobPollingSnapshot.resources
    };
  }, [data, jobPollingSnapshot]);

  const systemHealth = useMemo(
    () => mergeSystemHealthWithJobOverrides(systemHealthWithPolledJobs, jobOverrides.data),
    [systemHealthWithPolledJobs, jobOverrides.data]
  );

  const displayDataLayers = useMemo(() => {
    return (systemHealth?.dataLayers || []).map((layer) => ({
      ...layer,
      domains: (layer.domains || []).filter((domain) => {
        const domainKey = normalizeDomainKey(String(domain?.name || ''));
        return domainKey !== 'platinum';
      })
    }));
  }, [systemHealth]);

  const jobStates = useMemo(() => {
    const states: Record<string, string> = {};
    for (const resource of systemHealth?.resources || []) {
      if (resource.resourceType !== 'Microsoft.App/jobs') continue;
      const jobKey = normalizeAzureJobName(resource.name);
      const runningState = String(resource.runningState || '').trim();
      if (jobKey && runningState) {
        states[jobKey] = runningState;
      }
    }
    return states;
  }, [systemHealth]);

  const managedContainerJobs = useMemo<ManagedContainerJob[]>(() => {
    const seen = new Set<string>();
    const items: ManagedContainerJob[] = [];
    for (const resource of systemHealth?.resources || []) {
      if (resource.resourceType !== 'Microsoft.App/jobs') continue;
      const rawName = String(resource.name || '').trim();
      if (!rawName) continue;
      const normalizedName = normalizeAzureJobName(rawName);
      const dedupeKey = normalizedName || rawName.toLowerCase();
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      items.push({
        name: rawName,
        runningState: resource.runningState || null,
        lastModifiedAt: resource.lastModifiedAt || null
      });
    }
    return items;
  }, [systemHealth]);

  const latestJobRuns = useMemo(
    () => buildLatestJobRunIndex(systemHealth?.recentJobs || []),
    [systemHealth?.recentJobs]
  );

  const jobLogStreamJobs = useMemo<JobLogStreamTarget[]>(() => {
    type MutableJobTarget = Omit<
      JobLogStreamTarget,
      'runningState' | 'recentStatus' | 'startTime'
    > & {
      sortLayerName: string;
    };
    const items = new Map<string, MutableJobTarget>();

    for (const layer of displayDataLayers || []) {
      for (const domain of layer.domains || []) {
        const rawJobName =
          String(domain.jobName || '').trim() || normalizeAzureJobName(domain.jobUrl) || '';
        if (!rawJobName) continue;
        const key = normalizeAzureJobName(rawJobName) || rawJobName.toLowerCase();
        if (items.has(key)) continue;
        items.set(key, {
          name: rawJobName,
          label: `${layer.name} / ${domain.name} / ${rawJobName}`,
          layerName: layer.name,
          domainName: domain.name,
          jobUrl: domain.jobUrl || null,
          sortLayerName: layer.name
        });
      }
    }

    for (const resource of systemHealth?.resources || []) {
      if (resource.resourceType !== 'Microsoft.App/jobs') continue;
      const rawJobName = String(resource.name || '').trim();
      if (!rawJobName) continue;
      const key = normalizeAzureJobName(rawJobName) || rawJobName.toLowerCase();
      if (items.has(key)) continue;
      items.set(key, {
        name: rawJobName,
        label: rawJobName,
        layerName: null,
        domainName: null,
        jobUrl: null,
        sortLayerName: ''
      });
    }

    for (const run of latestJobRuns.values()) {
      const rawJobName = String(run.jobName || '').trim();
      if (!rawJobName) continue;
      const key = normalizeAzureJobName(rawJobName) || rawJobName.toLowerCase();
      if (items.has(key)) continue;
      items.set(key, {
        name: rawJobName,
        label: rawJobName,
        layerName: null,
        domainName: null,
        jobUrl: null,
        sortLayerName: ''
      });
    }

    return Array.from(items.entries())
      .map(([key, item]) => {
        const latestRun = latestJobRuns.get(key);
        return {
          ...item,
          runningState: jobStates[key] || null,
          recentStatus: latestRun?.status || null,
          startTime: latestRun?.startTime || null
        };
      })
      .sort((left, right) => {
        const leftRunning = String(left.runningState || left.recentStatus || '')
          .trim()
          .toLowerCase()
          .includes('running')
          ? 1
          : 0;
        const rightRunning = String(right.runningState || right.recentStatus || '')
          .trim()
          .toLowerCase()
          .includes('running')
          ? 1
          : 0;
        if (leftRunning !== rightRunning) {
          return rightRunning - leftRunning;
        }

        const leftStart = left.startTime ? Date.parse(left.startTime) : Number.NEGATIVE_INFINITY;
        const rightStart = right.startTime ? Date.parse(right.startTime) : Number.NEGATIVE_INFINITY;
        if (leftStart !== rightStart) {
          return rightStart - leftStart;
        }

        if (left.sortLayerName !== right.sortLayerName) {
          return left.sortLayerName.localeCompare(right.sortLayerName);
        }

        return left.label.localeCompare(right.label);
      })
      .map(({ sortLayerName: _sortLayerName, ...item }) => item);
  }, [displayDataLayers, jobStates, latestJobRuns, systemHealth?.resources]);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      const fresh = await DataService.getSystemHealth({ refresh: true });
      queryClient.setQueryData(queryKeys.systemHealth(), fresh);
      setJobPollingSnapshot(pickJobPollingSnapshot(fresh));
    } catch (err) {
      console.error('[SystemStatusPage] refresh failed', err);
    } finally {
      setIsRefreshing(false);
    }
  };

  if (isLoading) {
    return <PageLoader text="Initializing System Link..." />;
  }

  if (error || !systemHealth) {
    return (
      <div className="p-6 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive font-mono">
        <h3 className="text-lg font-bold mb-2 uppercase">System Link Failure</h3>
        <p>{error ? (error as Error).message : 'No telemetry available'}</p>
      </div>
    );
  }

  const { overall, recentJobs } = systemHealth;

  return (
    <div className="page-shell">
      {/* Domain Layer Coverage Comparison */}
      <ErrorBoundary>
        <Suspense fallback={<Skeleton className="h-[280px] w-full rounded-xl bg-muted/20" />}>
          <DomainLayerComparisonPanel
            overall={overall}
            dataLayers={displayDataLayers}
            recentJobs={recentJobs}
            jobStates={jobStates}
            managedContainerJobs={managedContainerJobs}
            onRefresh={handleRefresh}
            isRefreshing={isRefreshing}
            isFetching={isFetching}
          />
        </Suspense>
      </ErrorBoundary>

      {/* Container App Runtime Controls */}
      <ErrorBoundary>
        <Suspense fallback={<Skeleton className="h-[220px] w-full rounded-xl bg-muted/20" />}>
          <ContainerAppsPanel />
        </Suspense>
      </ErrorBoundary>

      <ErrorBoundary>
        <Suspense fallback={<Skeleton className="h-[260px] w-full rounded-xl bg-muted/20" />}>
          <JobLogStreamPanel jobs={jobLogStreamJobs} />
        </Suspense>
      </ErrorBoundary>

      {/* Footer Status Line */}
      <div className="flex justify-end border-t border-dashed border-zinc-800 pt-2 opacity-50">
        <div className="flex items-center gap-2 font-mono text-[10px]">
          <span
            className={`h-2 w-2 rounded-full ${isFetching ? 'bg-cyan-500 animate-pulse' : 'bg-zinc-600'}`}
          />
          {isFetching ? 'RECEIVING TELEMETRY...' : 'LINK ESTABLISHED'}
        </div>
      </div>
    </div>
  );
}
