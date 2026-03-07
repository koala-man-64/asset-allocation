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

// Lazy load components to reduce initial bundle size of the page
const DomainLayerComparisonPanel = lazy(() =>
  import('./system-status/DomainLayerComparisonPanel').then((m) => ({
    default: m.DomainLayerComparisonPanel
  }))
);
const ContainerAppsPanel = lazy(() =>
  import('./system-status/ContainerAppsPanel').then((m) => ({ default: m.ContainerAppsPanel }))
);

import {
  normalizeAzureJobName,
} from './system-status/SystemStatusHelpers';
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

  useEffect(() => {
    if (!data || error) {
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

    const handle = window.setInterval(() => {
      void pollJobStatus();
    }, JOB_STATUS_POLL_INTERVAL_MS);

    return () => {
      disposed = true;
      window.clearInterval(handle);
    };
  }, [data, error]);

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
