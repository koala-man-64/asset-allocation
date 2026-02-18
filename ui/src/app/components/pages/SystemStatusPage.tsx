import React, { useEffect, useMemo, useState, lazy, Suspense } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useSystemHealthQuery, queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import { ErrorBoundary } from '@/app/components/common/ErrorBoundary';
import { Skeleton } from '@/app/components/ui/skeleton';
import { PageLoader } from '@/app/components/common/PageLoader';
import type { ManagedContainerJob } from './system-status/JobKillSwitchPanel';

// Lazy load components to reduce initial bundle size of the page
const StatusOverview = lazy(() => import('./system-status/StatusOverview').then(m => ({ default: m.StatusOverview })));
const DomainLayerComparisonPanel = lazy(() =>
  import('./system-status/DomainLayerComparisonPanel').then((m) => ({
    default: m.DomainLayerComparisonPanel
  }))
);
const ScheduledJobMonitor = lazy(() => import('./system-status/ScheduledJobMonitor').then(m => ({ default: m.ScheduledJobMonitor })));
const ContainerAppsPanel = lazy(() =>
  import('./system-status/ContainerAppsPanel').then((m) => ({ default: m.ContainerAppsPanel }))
);

import {
  getAzurePortalUrl,
  normalizeAzureJobName,
  normalizeAzurePortalUrl
} from './system-status/SystemStatusHelpers';
import { normalizeDomainKey } from './system-status/SystemPurgeControls';

export function SystemStatusPage() {
  const { data, isLoading, error, isFetching } = useSystemHealthQuery();
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [_, setTick] = useState(0);

  const displayDataLayers = useMemo(() => {
    return (data?.dataLayers || []).map((layer) => ({
      ...layer,
      domains: (layer.domains || []).filter((domain) => {
        const domainKey = normalizeDomainKey(String(domain?.name || ''));
        return domainKey !== 'platinum';
      })
    }));
  }, [data]);

  const jobLinks = useMemo(() => {
    if (!data) {
      return {};
    }

    const links: Record<string, string> = {};
    for (const layer of displayDataLayers) {
      for (const domain of layer.domains || []) {
        if (domain.jobName && domain.jobUrl) {
          const rawName = String(domain.jobName).trim();
          const normalizedName = normalizeAzureJobName(rawName);
          const url = normalizeAzurePortalUrl(domain.jobUrl);
          links[rawName] = url;
          if (normalizedName) {
            links[normalizedName] = url;
          }
        }
      }
    }
    for (const resource of data.resources || []) {
      if (resource.resourceType === 'Microsoft.App/jobs' && resource.azureId) {
        const rawName = String(resource.name || '').trim();
        const normalizedName = normalizeAzureJobName(rawName);
        const url = getAzurePortalUrl(resource.azureId);
        if (rawName) {
          links[rawName] = url;
        }
        if (normalizedName) {
          links[normalizedName] = url;
        }
      }
    }
    return links;
  }, [data, displayDataLayers]);

  const jobStates = useMemo(() => {
    const states: Record<string, string> = {};
    for (const resource of data?.resources || []) {
      if (resource.resourceType !== 'Microsoft.App/jobs') continue;
      const jobKey = normalizeAzureJobName(resource.name);
      const runningState = String(resource.runningState || '').trim();
      if (jobKey && runningState) {
        states[jobKey] = runningState;
      }
    }
    return states;
  }, [data]);

  const managedContainerJobs = useMemo<ManagedContainerJob[]>(() => {
    const seen = new Set<string>();
    const items: ManagedContainerJob[] = [];
    for (const resource of data?.resources || []) {
      if (resource.resourceType !== 'Microsoft.App/jobs') continue;
      const rawName = String(resource.name || '').trim();
      if (!rawName) continue;
      const normalizedName = normalizeAzureJobName(rawName);
      const dedupeKey = normalizedName || rawName.toLowerCase();
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      items.push({
        name: rawName,
        runningState: resource.runningState || null
      });
    }
    return items;
  }, [data]);

  useEffect(() => {
    const errorMessage = error instanceof Error ? error.message : error ? String(error) : null;
    console.info('[SystemStatusPage] system health query state', {
      isLoading,
      isFetching,
      hasData: Boolean(data),
      error: errorMessage
    });
  }, [isLoading, isFetching, data, error]);

  // Force re-render for clock
  useEffect(() => {
    const h = setInterval(() => setTick((t) => t + 1), 1000);
    return () => clearInterval(h);
  }, []);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      const fresh = await DataService.getSystemHealth({ refresh: true });
      queryClient.setQueryData(queryKeys.systemHealth(), fresh);
    } catch (err) {
      console.error('[SystemStatusPage] refresh failed', err);
    } finally {
      setIsRefreshing(false);
    }
  };

  if (isLoading) {
    return <PageLoader text="Initializing System Link..." />;
  }

  if (error || !data) {
    return (
      <div className="p-6 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive font-mono">
        <h3 className="text-lg font-bold mb-2 uppercase">System Link Failure</h3>
        <p>{error ? (error as Error).message : 'No telemetry available'}</p>
      </div>
    );
  }

  const { overall, recentJobs } = data;

  return (
    <div className="page-shell">
      {/* Status Matrix - The Hero Component */}
      <ErrorBoundary>
        <Suspense fallback={<Skeleton className="h-[300px] w-full rounded-xl bg-muted/20" />}>
          <StatusOverview
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

      {/* Domain Layer Coverage Comparison */}
      <ErrorBoundary>
        <Suspense fallback={<Skeleton className="h-[280px] w-full rounded-xl bg-muted/20" />}>
          <DomainLayerComparisonPanel dataLayers={displayDataLayers} />
        </Suspense>
      </ErrorBoundary>

      {/* Jobs */}
      <ErrorBoundary>
        <Suspense fallback={<Skeleton className="h-[400px] w-full rounded-xl bg-muted/20" />}>
          <ScheduledJobMonitor
            dataLayers={displayDataLayers}
            recentJobs={recentJobs}
            jobLinks={jobLinks}
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
            className={`w-2 h-2 rounded-full ${isFetching ? 'bg-cyan-500 animate-pulse' : 'bg-zinc-600'}`}
          />
          {isFetching ? 'RECEIVING TELEMETRY...' : 'LINK ESTABLISHED'}
        </div>
      </div>
    </div>
  );
}
