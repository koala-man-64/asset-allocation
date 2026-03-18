import React, { useCallback, useMemo, useState, lazy, Suspense } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/hooks/useDataQueries';
import { useSystemStatusViewQuery } from '@/hooks/useSystemStatusView';
import { DataService } from '@/services/DataService';
import type {
  DomainMetadataSnapshotResponse,
  SystemStatusViewResponse
} from '@/services/apiService';
import { ErrorBoundary } from '@/app/components/common/ErrorBoundary';
import { Skeleton } from '@/app/components/ui/skeleton';
import { PageLoader } from '@/app/components/common/PageLoader';
import type { ManagedContainerJob } from './system-status/JobKillSwitchPanel';
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
import { effectiveJobStatus, formatTimeAgo } from './system-status/SystemStatusHelpers';
import { normalizeDomainKey } from './system-status/SystemPurgeControls';

export function SystemStatusPage() {
  const { data, isLoading, error, isFetching } = useSystemStatusViewQuery({
    autoRefresh: true
  });
  const queryClient = useQueryClient();
  const [isRefreshing, setIsRefreshing] = useState(false);
  const systemStatusView = data;
  const systemHealth = systemStatusView?.systemHealth;

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
        const leftRunning =
          effectiveJobStatus(left.recentStatus, left.runningState) === 'running' ? 1 : 0;
        const rightRunning =
          effectiveJobStatus(right.recentStatus, right.runningState) === 'running' ? 1 : 0;
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

  const handleMetadataSnapshotChange = useCallback(
    (
      updater: (
        previous: DomainMetadataSnapshotResponse | undefined
      ) => DomainMetadataSnapshotResponse | undefined
    ) => {
      queryClient.setQueryData<SystemStatusViewResponse | undefined>(
        queryKeys.systemStatusView(),
        (current) => {
          if (!current) return current;
          const nextMetadataSnapshot = updater(current.metadataSnapshot);
          if (!nextMetadataSnapshot) return current;
          return {
            ...current,
            metadataSnapshot: nextMetadataSnapshot
          };
        }
      );
      queryClient.setQueryData(queryKeys.domainMetadataSnapshot('all', 'all'), updater);
    },
    [queryClient]
  );

  const handleRefresh = async () => {
    setIsRefreshing(true);
    try {
      const fresh = await DataService.getSystemStatusView({ refresh: true });
      queryClient.setQueryData(queryKeys.systemStatusView(), fresh);
      queryClient.setQueryData(queryKeys.systemHealth(), fresh.systemHealth);
      queryClient.setQueryData(
        queryKeys.domainMetadataSnapshot('all', 'all'),
        fresh.metadataSnapshot
      );
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
  const generatedAtLabel = systemStatusView?.generatedAt
    ? `VIEW UPDATED ${formatTimeAgo(systemStatusView.generatedAt)} AGO`
    : 'LINK ESTABLISHED';

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
            metadataSnapshot={systemStatusView?.metadataSnapshot}
            metadataUpdatedAt={systemStatusView?.metadataSnapshot.updatedAt || null}
            metadataSource={systemStatusView?.sources.metadataSnapshot}
            onMetadataSnapshotChange={handleMetadataSnapshotChange}
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
          {isFetching ? 'RECEIVING TELEMETRY...' : generatedAtLabel}
        </div>
      </div>
    </div>
  );
}
