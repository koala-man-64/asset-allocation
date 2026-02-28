import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  AlertTriangle,
  CirclePause,
  CirclePlay,
  EllipsisVertical,
  ExternalLink,
  FolderOpen,
  GitCompareArrows,
  Loader2,
  Play,
  RefreshCw,
  RotateCcw,
  ScrollText,
  Square,
  Trash2
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Button } from '@/app/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '@/app/components/ui/dropdown-menu';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import type { DomainMetadataSnapshotResponse } from '@/services/apiService';
import type { DataLayer, DomainMetadata } from '@/types/strategy';
import { StatusTypos } from './StatusTokens';
import { normalizeDomainKey, normalizeLayerKey } from './SystemPurgeControls';
import { getDomainOrderEntries } from './domainOrdering';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { formatSystemStatusText } from './systemStatusText';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle
} from '@/app/components/ui/alert-dialog';
import { toast } from 'sonner';
import { DomainListViewerSheet, type DomainListViewerTarget } from './DomainListViewerSheet';
import { JobKillSwitchInline, type ManagedContainerJob } from './JobKillSwitchPanel';
import { useJobSuspend } from '@/hooks/useJobSuspend';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import {
  formatSchedule,
  formatTimeAgo,
  getStatusConfig,
  getAzureJobExecutionsUrl,
  normalizeAzureJobName,
  normalizeAzurePortalUrl
} from './SystemStatusHelpers';
import type { DataDomain, JobRun } from '@/types/strategy';

const LAYER_ORDER = ['bronze', 'silver', 'gold', 'platinum'] as const;
type LayerKey = (typeof LAYER_ORDER)[number];
const ACTION_LAYER_PRIORITY = ['gold', 'silver', 'bronze', 'platinum'] as const;
const CHECKPOINT_RESET_LAYERS = new Set<LayerKey>(['silver', 'gold']);
const DOMAIN_METADATA_SNAPSHOT_STORAGE_KEY = 'asset-allocation.domain-metadata-snapshot.v1';
const PURGE_POLL_INTERVAL_MS = 1000;
const PURGE_POLL_TIMEOUT_MS = 5 * 60_000;
type LayerVisualConfig = {
  accent: string;
  softBg: string;
  strongBg: string;
  border: string;
  mutedText: string;
};

type LayerColumn = {
  key: LayerKey;
  label: string;
};

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const FINANCE_SUBFOLDER_ITEMS = [
  { key: 'balance_sheet', label: 'Balance Sheet' },
  { key: 'income_statement', label: 'Income Statement' },
  { key: 'cash_flow', label: 'Cash Flow' },
  { key: 'valuation', label: 'Valuation' }
] as const;
const LAYER_VISUALS: Record<LayerKey, LayerVisualConfig> = {
  bronze: {
    accent: '#9a5b2d',
    softBg: 'rgba(154, 91, 45, 0.14)',
    strongBg: 'rgba(154, 91, 45, 0.22)',
    border: 'rgba(154, 91, 45, 0.5)',
    mutedText: 'rgba(122, 72, 34, 0.88)'
  },
  silver: {
    accent: '#4b5563',
    softBg: 'rgba(75, 85, 99, 0.14)',
    strongBg: 'rgba(75, 85, 99, 0.22)',
    border: 'rgba(75, 85, 99, 0.5)',
    mutedText: 'rgba(55, 65, 81, 0.88)'
  },
  gold: {
    accent: '#9a7400',
    softBg: 'rgba(154, 116, 0, 0.14)',
    strongBg: 'rgba(154, 116, 0, 0.22)',
    border: 'rgba(154, 116, 0, 0.5)',
    mutedText: 'rgba(120, 90, 0, 0.9)'
  },
  platinum: {
    accent: '#0f766e',
    softBg: 'rgba(15, 118, 110, 0.14)',
    strongBg: 'rgba(15, 118, 110, 0.22)',
    border: 'rgba(15, 118, 110, 0.5)',
    mutedText: 'rgba(17, 94, 89, 0.9)'
  }
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const runStartEpoch = (raw?: string | null): number => {
  const value = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
};

function extractAzureJobName(jobUrl?: string | null): string | null {
  const normalized = normalizeAzurePortalUrl(jobUrl);
  if (!normalized) return null;
  const match = normalized.match(/\/jobs\/([^/?#]+)/);
  if (!match) return null;
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return match[1];
  }
}

function toLayerKey(value: string): LayerKey | null {
  const normalized = normalizeLayerKey(value);
  if (!LAYER_ORDER.includes(normalized as LayerKey)) return null;
  return normalized as LayerKey;
}

function getLayerVisual(layerKey: LayerKey): LayerVisualConfig {
  return LAYER_VISUALS[layerKey];
}

function hasFiniteNumber(value: number | null | undefined): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function makeCellKey(layerKey: LayerKey, domainKey: string): string {
  return `${layerKey}:${domainKey}`;
}

function makeSnapshotKey(layerKey: LayerKey, domainKey: string): string {
  return `${layerKey}/${domainKey}`;
}

function loadPersistedSnapshot(): DomainMetadataSnapshotResponse | undefined {
  if (typeof window === 'undefined') return undefined;
  try {
    const raw = window.localStorage.getItem(DOMAIN_METADATA_SNAPSHOT_STORAGE_KEY);
    if (!raw) return undefined;
    const parsed = JSON.parse(raw) as DomainMetadataSnapshotResponse;
    if (!parsed || typeof parsed !== 'object' || !parsed.entries) return undefined;
    return parsed;
  } catch {
    return undefined;
  }
}

function persistSnapshot(payload: DomainMetadataSnapshotResponse): void {
  if (typeof window === 'undefined') return;
  try {
    window.localStorage.setItem(DOMAIN_METADATA_SNAPSHOT_STORAGE_KEY, JSON.stringify(payload));
  } catch {
    // best-effort browser persistence only
  }
}

function mergeSnapshots(
  remote: DomainMetadataSnapshotResponse | null,
  live: DomainMetadataSnapshotResponse | null
): DomainMetadataSnapshotResponse {
  const remoteEntries = remote?.entries || {};
  const liveEntries = live?.entries || {};
  const warnings = [
    ...((remote?.warnings || []).filter(Boolean) as string[]),
    ...((live?.warnings || []).filter(Boolean) as string[])
  ];
  return {
    version: live?.version || remote?.version || 1,
    updatedAt: live?.updatedAt || remote?.updatedAt || null,
    entries: {
      ...remoteEntries,
      ...liveEntries
    },
    warnings: Array.from(new Set(warnings))
  };
}

function formatInt(value: number | null | undefined): string {
  if (!hasFiniteNumber(value)) return 'N/A';
  return numberFormatter.format(value);
}

function formatSymbolCount(value: number | null | undefined): string {
  if (!hasFiniteNumber(value)) return 'N/A';
  return `${numberFormatter.format(value)} symbols`;
}

function compareSymbols(
  current: DomainMetadata,
  previous: DomainMetadata
): {
  text: string;
  className: string;
} {
  if (!hasFiniteNumber(current.symbolCount) || !hasFiniteNumber(previous.symbolCount)) {
    return { text: 'symbols n/a', className: 'text-mcm-walnut/70' };
  }

  const delta = current.symbolCount - previous.symbolCount;
  if (delta === 0) {
    return { text: 'symbols match', className: 'text-mcm-teal' };
  }

  const prefix = delta > 0 ? '+' : '';
  return {
    text: `${prefix}${numberFormatter.format(delta)} symbols`,
    className: delta > 0 ? 'text-mcm-olive' : 'text-destructive'
  };
}

function summarizeBlacklistCount(metadata: DomainMetadata): { text: string; className: string } {
  if (!hasFiniteNumber(metadata.blacklistedSymbolCount)) {
    return { text: 'blacklist n/a', className: 'text-mcm-walnut/70' };
  }
  if (metadata.blacklistedSymbolCount === 0) {
    return { text: '0 blacklisted', className: 'text-mcm-teal' };
  }
  return {
    text: `${numberFormatter.format(metadata.blacklistedSymbolCount)} blacklisted`,
    className: 'text-mcm-walnut/85'
  };
}

function toDataStatusLabel(statusKey: string): string {
  const key = String(statusKey || '')
    .trim()
    .toLowerCase();
  if (key === 'healthy' || key === 'success') return 'OK';
  if (key === 'stale' || key === 'warning' || key === 'degraded') return 'STALE';
  if (key === 'error' || key === 'failed' || key === 'critical') return 'ERR';
  if (key === 'pending') return 'PENDING';
  return key.toUpperCase();
}

interface DomainLayerComparisonPanelProps {
  overall?: string;
  dataLayers: DataLayer[];
  recentJobs?: JobRun[];
  jobStates?: Record<string, string>;
  managedContainerJobs?: ManagedContainerJob[];
  onRefresh?: () => void;
  isRefreshing?: boolean;
  isFetching?: boolean;
}

export function DomainLayerComparisonPanel({
  overall = 'unknown',
  dataLayers,
  recentJobs = [],
  jobStates,
  managedContainerJobs = [],
  onRefresh,
  isRefreshing,
  isFetching
}: DomainLayerComparisonPanelProps) {
  const queryClient = useQueryClient();
  const { triggeringJob, triggerJob } = useJobTrigger();
  const { jobControl, setJobSuspended } = useJobSuspend();
  const [refreshingCells, setRefreshingCells] = useState<Set<string>>(new Set());
  const [isRefreshingPanelCounts, setIsRefreshingPanelCounts] = useState(false);
  const [purgeTarget, setPurgeTarget] = useState<{
    layerKey: LayerKey;
    layerLabel: string;
    domainKey: string;
    domainLabel: string;
  } | null>(null);
  const [isPurging, setIsPurging] = useState(false);
  const [activePurgeTarget, setActivePurgeTarget] = useState<{
    layerKey: LayerKey;
    domainKey: string;
  } | null>(null);
  const [listViewerTarget, setListViewerTarget] = useState<DomainListViewerTarget | null>(null);
  const [listResetTarget, setListResetTarget] = useState<{
    layerKey: LayerKey;
    layerLabel: string;
    domainKey: string;
    domainLabel: string;
  } | null>(null);
  const [checkpointResetTarget, setCheckpointResetTarget] = useState<{
    layerKey: LayerKey;
    layerLabel: string;
    domainKey: string;
    domainLabel: string;
  } | null>(null);
  const [isResetAllDialogOpen, setIsResetAllDialogOpen] = useState(false);
  const [isResettingLists, setIsResettingLists] = useState(false);
  const [isResettingAllLists, setIsResettingAllLists] = useState(false);
  const [isResettingCheckpoints, setIsResettingCheckpoints] = useState(false);
  const [resettingCellKey, setResettingCellKey] = useState<string | null>(null);
  const [resettingCheckpointCellKey, setResettingCheckpointCellKey] = useState<string | null>(null);
  const [expandedRowKey, setExpandedRowKey] = useState<string | null>(null);
  const [clockNow, setClockNow] = useState(() => new Date());
  const overallStatusConfig = getStatusConfig(overall);
  const overallAnim =
    overallStatusConfig.animation === 'spin'
      ? 'animate-spin'
      : overallStatusConfig.animation === 'pulse'
        ? 'animate-pulse'
        : '';
  const overallLabel = String(overall || '')
    .trim()
    .toUpperCase();

  useEffect(() => {
    const handle = window.setInterval(() => setClockNow(new Date()), 1000);
    return () => window.clearInterval(handle);
  }, []);

  const centralClock = (() => {
    const now = clockNow;

    const time = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Chicago',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(now);

    const tzRaw =
      new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/Chicago',
        timeZoneName: 'short'
      })
        .formatToParts(now)
        .find((part) => part.type === 'timeZoneName')?.value ?? '';

    const tz = (() => {
      const value = String(tzRaw || '').trim();
      if (!value) return 'CST';
      if (value === 'CST' || value === 'CDT') return value;
      if (/central.*daylight/i.test(value)) return 'CDT';
      if (/central.*standard/i.test(value)) return 'CST';

      const offsetMatch = value.match(/(?:GMT|UTC)([+-]\d{1,2})(?::?(\d{2}))?/i);
      if (!offsetMatch) return 'CST';

      const hours = Number.parseInt(offsetMatch[1] || '0', 10);
      const minutes = Number.parseInt(offsetMatch[2] || '0', 10);
      const total = hours * 60 + (hours < 0 ? -minutes : minutes);
      if (total === -360) return 'CST';
      if (total === -300) return 'CDT';
      return 'CST';
    })();

    return { time, tz };
  })();

  const layersByKey = useMemo(() => {
    const index = new Map<LayerKey, DataLayer>();
    for (const layer of dataLayers || []) {
      const key = toLayerKey(String(layer?.name || ''));
      if (!key || index.has(key)) continue;
      index.set(key, layer);
    }
    return index;
  }, [dataLayers]);

  const layerColumns = useMemo<LayerColumn[]>(() => {
    const columns: LayerColumn[] = [];
    for (const key of LAYER_ORDER) {
      const layer = layersByKey.get(key);
      if (!layer) continue;
      columns.push({ key, label: String(layer.name || key).trim() || key });
    }
    return columns;
  }, [layersByKey]);

  const jobIndex = useMemo(() => {
    const index = new Map<string, JobRun>();
    for (const job of recentJobs) {
      if (!job?.jobName) continue;
      const key = normalizeAzureJobName(job.jobName);
      if (!key) continue;
      const existing = index.get(key);
      if (!existing || runStartEpoch(job.startTime) > runStartEpoch(existing.startTime)) {
        index.set(key, job);
      }
    }
    return index;
  }, [recentJobs]);

  const { domainsByLayer, domainRows, domainConfigByLayer } = useMemo(() => {
    const matrix = new Map<string, Map<LayerKey, true>>();
    const domainConfig = new Map<LayerKey, Map<string, DataDomain>>();

    for (const layerColumn of layerColumns) {
      const domains = layersByKey.get(layerColumn.key)?.domains || [];
      const configForLayer = domainConfig.get(layerColumn.key) || new Map<string, DataDomain>();
      for (const domain of domains) {
        const domainName = String(domain?.name || '').trim();
        if (!domainName) continue;
        const domainKey = normalizeDomainKey(domainName);
        if (!domainKey) continue;

        const row = matrix.get(domainKey) || new Map<LayerKey, true>();
        row.set(layerColumn.key, true);
        matrix.set(domainKey, row);
        if (!configForLayer.has(domainKey)) {
          configForLayer.set(domainKey, domain);
        }
      }
      domainConfig.set(layerColumn.key, configForLayer);
    }

    const rows = getDomainOrderEntries(dataLayers).filter((entry) => {
      return matrix.has(entry.key);
    });

    return { domainsByLayer: matrix, domainRows: rows, domainConfigByLayer: domainConfig };
  }, [dataLayers, layerColumns, layersByKey]);

  const queryPairs = useMemo(() => {
    const pairs: Array<{ layerKey: LayerKey; domainKey: string }> = [];

    for (const row of domainRows) {
      const domainsForRow = domainsByLayer.get(row.key);
      if (!domainsForRow) continue;

      for (const layerColumn of layerColumns) {
        if (!domainsForRow.has(layerColumn.key)) continue;
        pairs.push({ layerKey: layerColumn.key, domainKey: row.key });
      }
    }
    return pairs;
  }, [domainRows, domainsByLayer, layerColumns]);

  const snapshotQueryKey = queryKeys.domainMetadataSnapshot('all', 'all');

  const metadataSnapshotQuery = useQuery({
    queryKey: snapshotQueryKey,
    queryFn: async () => {
      const [persistedResult, liveResult] = await Promise.allSettled([
        DataService.getPersistedDomainMetadataSnapshotCache(),
        DataService.getDomainMetadataSnapshot({ cacheOnly: true })
      ]);

      const persistedSnapshot =
        persistedResult.status === 'fulfilled' ? persistedResult.value : null;
      const liveSnapshot = liveResult.status === 'fulfilled' ? liveResult.value : null;

      if (!persistedSnapshot && !liveSnapshot) {
        const reason =
          liveResult.status === 'rejected'
            ? liveResult.reason
            : persistedResult.status === 'rejected'
              ? persistedResult.reason
              : new Error('No domain metadata snapshot sources were available.');
        throw reason instanceof Error ? reason : new Error(String(reason));
      }

      const merged = mergeSnapshots(persistedSnapshot, liveSnapshot);
      persistSnapshot(merged);
      void DataService.savePersistedDomainMetadataSnapshotCache(merged).catch(() => {
        // best-effort persistence to common container
      });
      return merged;
    },
    initialData: loadPersistedSnapshot,
    enabled: true,
    staleTime: 5 * 60 * 1000,
    refetchInterval: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchOnMount: false
  });

  const { metadataByCell, errorByCell, pendingByCell } = useMemo(() => {
    const metadata = new Map<string, DomainMetadata>();
    const errors = new Map<string, string>();
    const pending = new Set<string>();
    const snapshotEntries = metadataSnapshotQuery.data?.entries || {};
    const snapshotErrorMessage = metadataSnapshotQuery.error
      ? formatSystemStatusText(metadataSnapshotQuery.error)
      : null;
    const snapshotPending = metadataSnapshotQuery.isLoading || metadataSnapshotQuery.isFetching;

    queryPairs.forEach((pair) => {
      const key = makeCellKey(pair.layerKey, pair.domainKey);
      const cachedSingle = queryClient.getQueryData<DomainMetadata>(
        queryKeys.domainMetadata(pair.layerKey, pair.domainKey)
      );
      const cachedBatch = snapshotEntries[makeSnapshotKey(pair.layerKey, pair.domainKey)];
      const resolved = cachedSingle || cachedBatch;
      if (resolved) {
        metadata.set(key, resolved);
      }
      if (snapshotErrorMessage) {
        errors.set(key, snapshotErrorMessage);
      }
      if (snapshotPending || refreshingCells.has(key)) {
        pending.add(key);
      }
    });

    return { metadataByCell: metadata, errorByCell: errors, pendingByCell: pending };
  }, [metadataSnapshotQuery, queryClient, queryPairs, refreshingCells]);

  const isAnyRefreshInProgress =
    Boolean(isRefreshing) ||
    Boolean(isFetching) ||
    isRefreshingPanelCounts ||
    isResettingCheckpoints ||
    metadataSnapshotQuery.isLoading ||
    metadataSnapshotQuery.isFetching ||
    refreshingCells.size > 0;
  const isPanelActionBusy =
    isRefreshingPanelCounts || isResettingAllLists || isResettingLists || isResettingCheckpoints;

  const filteredDomainRows = useMemo(() => {
    return domainRows.filter((row) => Boolean(domainsByLayer.get(row.key)));
  }, [domainRows, domainsByLayer]);

  const layerAggregateStatus = useMemo(() => {
    const byLayer = new Map<
      LayerKey,
      {
        ok: number;
        warn: number;
        fail: number;
      }
    >();

    for (const layerColumn of layerColumns) {
      let ok = 0;
      let warn = 0;
      let fail = 0;

      for (const row of filteredDomainRows) {
        const domainsForRow = domainsByLayer.get(row.key);
        if (!domainsForRow?.has(layerColumn.key)) continue;

        const domainConfig = domainConfigByLayer.get(layerColumn.key)?.get(row.key);
        const dataStatusKey =
          String(domainConfig?.status || '')
            .trim()
            .toLowerCase() || 'pending';

        const jobName =
          String(domainConfig?.jobName || '').trim() ||
          extractAzureJobName(domainConfig?.jobUrl) ||
          '';
        const jobKey = normalizeAzureJobName(jobName);
        const run = jobKey ? jobIndex.get(jobKey) : null;
        const runStatusKey = String(run?.status || '')
          .trim()
          .toLowerCase();
        const jobStatusKey =
          !jobName || !run
            ? 'pending'
            : ['running', 'failed', 'success', 'succeeded', 'error', 'pending'].includes(
                  runStatusKey
                )
              ? runStatusKey
              : 'pending';

        const isCritical =
          ['error', 'failed', 'critical'].includes(dataStatusKey) ||
          ['error', 'failed'].includes(jobStatusKey);
        const isWarning =
          !isCritical &&
          (['stale', 'warning', 'degraded', 'pending'].includes(dataStatusKey) ||
            ['pending'].includes(jobStatusKey));

        if (isCritical) fail += 1;
        else if (isWarning) warn += 1;
        else ok += 1;
      }

      byLayer.set(layerColumn.key, { ok, warn, fail });
    }

    return byLayer;
  }, [domainConfigByLayer, domainsByLayer, filteredDomainRows, jobIndex, layerColumns]);

  const handleCellRefresh = useCallback(
    async (layerKey: LayerKey, domainKey: string) => {
      const cellKey = makeCellKey(layerKey, domainKey);
      if (refreshingCells.has(cellKey)) return;

      setRefreshingCells((previous) => {
        const next = new Set(previous);
        next.add(cellKey);
        return next;
      });

      try {
        const metadata = await DataService.getDomainMetadata(layerKey, domainKey, {
          refresh: true
        });
        let snapshotToPersist: DomainMetadataSnapshotResponse | null = null;
        queryClient.setQueryData(queryKeys.domainMetadata(layerKey, domainKey), metadata);
        queryClient.setQueryData<DomainMetadataSnapshotResponse | undefined>(
          snapshotQueryKey,
          (previous) => {
            const nextEntries = {
              ...(previous?.entries || {}),
              [makeSnapshotKey(layerKey, domainKey)]: metadata
            };
            const nextPayload: DomainMetadataSnapshotResponse = {
              version: previous?.version || 1,
              updatedAt: metadata.cachedAt || metadata.computedAt || previous?.updatedAt || null,
              entries: nextEntries,
              warnings: (previous?.warnings || []).filter(Boolean)
            };
            persistSnapshot(nextPayload);
            snapshotToPersist = nextPayload;
            return nextPayload;
          }
        );
        if (snapshotToPersist) {
          void DataService.savePersistedDomainMetadataSnapshotCache(snapshotToPersist).catch(() => {
            // best-effort persistence to common container
          });
        }
      } catch (error) {
        console.error('[DomainLayerComparisonPanel] cell refresh failed', {
          layerKey,
          domainKey,
          error: formatSystemStatusText(error)
        });
      } finally {
        setRefreshingCells((previous) => {
          if (!previous.has(cellKey)) return previous;
          const next = new Set(previous);
          next.delete(cellKey);
          return next;
        });
      }
    },
    [queryClient, refreshingCells, snapshotQueryKey]
  );

  const clearDomainMetadataCache = useCallback(
    async (pairs: Array<{ layerKey: LayerKey; domainKey: string }>) => {
      if (pairs.length === 0) return;

      for (const pair of pairs) {
        queryClient.removeQueries({
          queryKey: queryKeys.domainMetadata(pair.layerKey, pair.domainKey),
          exact: true
        });
      }

      let snapshotToPersist: DomainMetadataSnapshotResponse | null = null;
      queryClient.setQueryData<DomainMetadataSnapshotResponse | undefined>(
        snapshotQueryKey,
        (previous) => {
          const nextEntries = { ...(previous?.entries || {}) };
          let changed = false;
          for (const pair of pairs) {
            const key = makeSnapshotKey(pair.layerKey, pair.domainKey);
            if (key in nextEntries) {
              delete nextEntries[key];
              changed = true;
            }
          }

          if (!changed && previous) {
            return previous;
          }

          const nextPayload: DomainMetadataSnapshotResponse = {
            version: previous?.version || 1,
            updatedAt: new Date().toISOString(),
            entries: nextEntries,
            warnings: (previous?.warnings || []).filter(Boolean)
          };
          persistSnapshot(nextPayload);
          snapshotToPersist = nextPayload;
          return nextPayload;
        }
      );

      if (snapshotToPersist) {
        await DataService.savePersistedDomainMetadataSnapshotCache(snapshotToPersist).catch(() => {
          // best-effort persistence to common container
        });
      }
    },
    [queryClient, snapshotQueryKey]
  );

  const waitForPurgeResult = useCallback(async (operationId: string) => {
    const startedAt = Date.now();
    let attempt = 0;
    while (true) {
      let operation: unknown;
      try {
        operation = await DataService.getPurgeOperation(operationId);
      } catch {
        if (Date.now() - startedAt > PURGE_POLL_TIMEOUT_MS) {
          throw new Error(
            `Purge status polling failed after timeout. Check system status for progress. operationId=${operationId}`
          );
        }
        const delay = PURGE_POLL_INTERVAL_MS + Math.min(attempt * 250, 2000);
        await sleep(delay);
        attempt += 1;
        continue;
      }

      const polledOperation = operation as {
        status?: string;
        result?: {
          totalDeleted?: number;
        };
        error?: string;
      };
      if (polledOperation.status === 'succeeded') {
        if (!polledOperation.result) {
          throw new Error('Purge completed with no result payload.');
        }
        return polledOperation.result;
      }
      if (polledOperation.status === 'failed') {
        throw new Error(polledOperation.error || 'Purge failed.');
      }
      if (Date.now() - startedAt > PURGE_POLL_TIMEOUT_MS) {
        throw new Error(
          `Purge is still running. Check system status for progress. operationId=${operationId}`
        );
      }
      const delay = PURGE_POLL_INTERVAL_MS + Math.min(attempt * 250, 2000);
      await sleep(delay);
      attempt += 1;
    }
  }, []);

  const confirmPurge = useCallback(async () => {
    const target = purgeTarget;
    if (!target) return;
    setIsPurging(true);
    setActivePurgeTarget({ layerKey: target.layerKey, domainKey: target.domainKey });
    let operationId: string | null = null;
    try {
      const operation = await DataService.purgeData({
        scope: 'layer-domain',
        layer: target.layerKey,
        domain: target.domainKey,
        confirm: true
      });
      operationId = operation.operationId;
      const result =
        operation.status === 'succeeded'
          ? operation.result
          : await waitForPurgeResult(operation.operationId);
      if (!result) {
        throw new Error('Purge returned no completion result.');
      }
      toast.success(`Purged ${result.totalDeleted} blob(s).`);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      const detail = operationId ? `operation ${operationId}: ${message}` : message;
      toast.error(`Purge failed (${detail})`);
    } finally {
      setIsPurging(false);
      setActivePurgeTarget(null);
      setPurgeTarget(null);
    }
  }, [purgeTarget, queryClient, waitForPurgeResult]);

  const confirmDomainListReset = useCallback(async () => {
    const target = listResetTarget;
    if (!target) return;
    const targetCellKey = makeCellKey(target.layerKey, target.domainKey);
    setIsResettingLists(true);
    setResettingCellKey(targetCellKey);
    try {
      const result = await DataService.resetDomainLists({
        layer: target.layerKey,
        domain: target.domainKey,
        confirm: true
      });
      toast.success(
        `Reset ${result.resetCount} list file(s) for ${target.layerLabel} • ${target.domainLabel}.`
      );
      await clearDomainMetadataCache([{ layerKey: target.layerKey, domainKey: target.domainKey }]);
      await handleCellRefresh(target.layerKey, target.domainKey);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (error) {
      toast.error(`List reset failed (${formatSystemStatusText(error) || 'Unknown error'})`);
    } finally {
      setIsResettingLists(false);
      setResettingCellKey(null);
      setListResetTarget(null);
    }
  }, [clearDomainMetadataCache, handleCellRefresh, listResetTarget, queryClient]);

  const confirmDomainCheckpointReset = useCallback(async () => {
    const target = checkpointResetTarget;
    if (!target) return;
    const targetCellKey = makeCellKey(target.layerKey, target.domainKey);
    setIsResettingCheckpoints(true);
    setResettingCheckpointCellKey(targetCellKey);
    try {
      const result = await DataService.resetDomainCheckpoints({
        layer: target.layerKey,
        domain: target.domainKey,
        confirm: true
      });
      if (result.resetCount === 0) {
        toast.warning(
          result.note ||
            `No checkpoint gates are configured for ${target.layerLabel} • ${target.domainLabel}.`
        );
      } else {
        toast.success(
          `Reset ${result.deletedCount}/${result.resetCount} checkpoint gate file(s) for ${target.layerLabel} • ${target.domainLabel}.`
        );
      }
      await clearDomainMetadataCache([{ layerKey: target.layerKey, domainKey: target.domainKey }]);
      await handleCellRefresh(target.layerKey, target.domainKey);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (error) {
      toast.error(`Checkpoint reset failed (${formatSystemStatusText(error) || 'Unknown error'})`);
    } finally {
      setIsResettingCheckpoints(false);
      setResettingCheckpointCellKey(null);
      setCheckpointResetTarget(null);
    }
  }, [checkpointResetTarget, clearDomainMetadataCache, handleCellRefresh, queryClient]);

  const refreshAllPanelCounts = useCallback(async () => {
    if (
      queryPairs.length === 0 ||
      isRefreshingPanelCounts ||
      isResettingAllLists ||
      isResettingLists ||
      isResettingCheckpoints
    ) {
      return;
    }

    const panelCellKeys = queryPairs.map((pair) => makeCellKey(pair.layerKey, pair.domainKey));
    setIsRefreshingPanelCounts(true);
    setRefreshingCells((previous) => {
      const next = new Set(previous);
      panelCellKeys.forEach((key) => next.add(key));
      return next;
    });

    try {
      const snapshot = await DataService.getDomainMetadataSnapshot({
        layers: layerColumns.map((layer) => layer.key).join(','),
        domains: domainRows.map((row) => row.key).join(','),
        refresh: true
      });
      persistSnapshot(snapshot);
      queryClient.setQueryData(snapshotQueryKey, snapshot);
      for (const pair of queryPairs) {
        const entry = snapshot.entries?.[makeSnapshotKey(pair.layerKey, pair.domainKey)];
        if (entry) {
          queryClient.setQueryData(queryKeys.domainMetadata(pair.layerKey, pair.domainKey), entry);
        }
      }
      void DataService.savePersistedDomainMetadataSnapshotCache(snapshot).catch(() => {
        // best-effort persistence to common container
      });
      const refreshedCells = queryPairs.reduce((count, pair) => {
        const key = makeSnapshotKey(pair.layerKey, pair.domainKey);
        return snapshot.entries?.[key] ? count + 1 : count;
      }, 0);
      toast.success(`Refreshed counts for ${refreshedCells}/${queryPairs.length} panel cells.`);
    } catch (error) {
      toast.error(`Refresh failed (${formatSystemStatusText(error) || 'Unknown error'})`);
    } finally {
      setIsRefreshingPanelCounts(false);
      setRefreshingCells((previous) => {
        const next = new Set(previous);
        panelCellKeys.forEach((key) => next.delete(key));
        return next;
      });
    }
  }, [
    domainRows,
    isRefreshingPanelCounts,
    isResettingAllLists,
    isResettingCheckpoints,
    isResettingLists,
    layerColumns,
    queryClient,
    queryPairs,
    snapshotQueryKey
  ]);

  const confirmResetAllPanelLists = useCallback(async () => {
    if (
      queryPairs.length === 0 ||
      isRefreshingPanelCounts ||
      isResettingAllLists ||
      isResettingLists ||
      isResettingCheckpoints
    ) {
      return;
    }

    setIsResettingAllLists(true);
    try {
      const resetResults = await Promise.allSettled(
        queryPairs.map((pair) =>
          DataService.resetDomainLists({
            layer: pair.layerKey,
            domain: pair.domainKey,
            confirm: true
          })
        )
      );
      let successfulResets = 0;
      let failedResets = 0;
      let totalFilesReset = 0;
      let firstFailureMessage = '';
      const successfulPairs: Array<{ layerKey: LayerKey; domainKey: string }> = [];
      resetResults.forEach((result, index) => {
        if (result.status === 'fulfilled') {
          successfulResets += 1;
          totalFilesReset += result.value.resetCount;
          successfulPairs.push(queryPairs[index]);
          return;
        }
        failedResets += 1;
        if (!firstFailureMessage) {
          firstFailureMessage = formatSystemStatusText(result.reason) || 'Unknown error';
        }
      });

      if (successfulResets > 0) {
        await clearDomainMetadataCache(successfulPairs);
        await Promise.all(
          successfulPairs.map((pair) => handleCellRefresh(pair.layerKey, pair.domainKey))
        );
        toast.success(
          `Reset ${totalFilesReset} list file(s) across ${successfulResets}/${queryPairs.length} panel cells.`
        );
        void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
      }
      if (failedResets > 0) {
        toast.error(`Failed to reset ${failedResets} panel cells (first: ${firstFailureMessage})`);
      }
    } finally {
      setIsResettingAllLists(false);
      setIsResetAllDialogOpen(false);
    }
  }, [
    clearDomainMetadataCache,
    handleCellRefresh,
    isRefreshingPanelCounts,
    isResettingAllLists,
    isResettingCheckpoints,
    isResettingLists,
    queryClient,
    queryPairs
  ]);

  return (
    <Card className="h-full">
      <DomainListViewerSheet
        target={listViewerTarget}
        open={Boolean(listViewerTarget)}
        onOpenChange={(open) => {
          if (!open) {
            setListViewerTarget(null);
          }
        }}
      />

      <AlertDialog
        open={Boolean(purgeTarget)}
        onOpenChange={(open) => (!open ? setPurgeTarget(null) : undefined)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-destructive" />
              Confirm purge
            </AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete all blobs for{' '}
              <strong>
                {purgeTarget
                  ? `${purgeTarget.layerLabel} • ${purgeTarget.domainLabel}`
                  : 'selected scope'}
              </strong>
              . Containers remain, but the data cannot be recovered.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isPurging}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => void confirmPurge()}
              disabled={isPurging}
            >
              {isPurging ? (
                <span className="inline-flex items-center gap-2">
                  <Trash2 className="h-4 w-4 animate-spin" />
                  Purging...
                </span>
              ) : (
                'Purge'
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={Boolean(checkpointResetTarget)}
        onOpenChange={(open) => (!open ? setCheckpointResetTarget(null) : undefined)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-destructive" />
              Confirm checkpoint reset
            </AlertDialogTitle>
            <AlertDialogDescription>
              This will clear incremental checkpoint gate files for{' '}
              <strong>
                {checkpointResetTarget
                  ? `${checkpointResetTarget.layerLabel} • ${checkpointResetTarget.domainLabel}`
                  : 'selected scope'}
              </strong>
              . Data tables and list files are not deleted.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isResettingCheckpoints}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => void confirmDomainCheckpointReset()}
              disabled={isResettingCheckpoints}
            >
              {isResettingCheckpoints ? (
                <span className="inline-flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Resetting...
                </span>
              ) : (
                'Reset Checkpoints'
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={Boolean(listResetTarget)}
        onOpenChange={(open) => (!open ? setListResetTarget(null) : undefined)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-destructive" />
              Confirm list reset
            </AlertDialogTitle>
            <AlertDialogDescription>
              This will clear both <strong>whitelist.csv</strong> and <strong>blacklist.csv</strong>{' '}
              for{' '}
              <strong>
                {listResetTarget
                  ? `${listResetTarget.layerLabel} • ${listResetTarget.domainLabel}`
                  : 'selected scope'}
              </strong>
              .
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isResettingLists}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => void confirmDomainListReset()}
              disabled={isResettingLists}
            >
              {isResettingLists ? (
                <span className="inline-flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Resetting...
                </span>
              ) : (
                'Reset Lists'
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={isResetAllDialogOpen}
        onOpenChange={(open) => (!isResettingAllLists ? setIsResetAllDialogOpen(open) : undefined)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-destructive" />
              Confirm panel-wide list reset
            </AlertDialogTitle>
            <AlertDialogDescription>
              This will clear both <strong>whitelist.csv</strong> and <strong>blacklist.csv</strong>{' '}
              for all <strong>{queryPairs.length}</strong> configured layer/domain cells in this
              panel.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isResettingAllLists}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => void confirmResetAllPanelLists()}
              disabled={isResettingAllLists}
            >
              {isResettingAllLists ? (
                <span className="inline-flex items-center gap-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Resetting all...
                </span>
              ) : (
                'Reset All Lists'
              )}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <CardHeader className="gap-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="flex min-w-0 flex-col gap-2">
            <div className="flex min-w-0 items-start gap-2">
              <GitCompareArrows className="mt-0.5 h-5 w-5 shrink-0" />
              <div className="flex min-w-0 flex-col gap-1">
                <CardTitle className="leading-tight">Domain Layer Coverage</CardTitle>
              </div>
            </div>
            <div className="flex w-full max-w-full flex-nowrap items-center gap-2 overflow-x-auto sm:w-auto">
              <div
                role="status"
                aria-live="polite"
                aria-atomic="true"
                className="inline-flex items-center gap-2 rounded-full border-2 border-mcm-walnut/15 bg-mcm-cream/60 px-3 py-1 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.08)]"
              >
                <span className="text-[12px] font-semibold text-mcm-walnut/85">System status</span>
                <span
                  className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-black uppercase tracking-widest"
                  style={{
                    backgroundColor: overallStatusConfig.bg,
                    color: overallStatusConfig.text,
                    borderColor: overallStatusConfig.border
                  }}
                >
                  <overallStatusConfig.icon className={`h-3 w-3 ${overallAnim}`} />
                  {overallLabel || 'UNKNOWN'}
                </span>
              </div>

              <div className="inline-flex min-w-[220px] items-center gap-2 rounded-full border-2 border-mcm-walnut/15 bg-mcm-cream/60 px-3 py-1 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.08)]">
                <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-mcm-olive">
                  Uptime clock
                </span>
                <span className={`${StatusTypos.MONO} text-sm text-mcm-walnut/85`}>
                  {centralClock.time} {centralClock.tz}
                </span>
              </div>
            </div>
          </div>

          <div className="flex w-full flex-wrap items-center gap-2 xl:w-auto xl:justify-end">
            {managedContainerJobs.length > 0 ? (
              <JobKillSwitchInline jobs={managedContainerJobs} />
            ) : null}

            {onRefresh ? (
              <Button
                variant="outline"
                size="sm"
                className="h-9 px-3.5 text-[11px]"
                onClick={onRefresh}
                disabled={!onRefresh || isAnyRefreshInProgress}
              >
                {isAnyRefreshInProgress ? (
                  <span className="inline-flex items-center gap-2">
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    Refreshing...
                  </span>
                ) : (
                  <>
                    <RefreshCw className="h-4 w-4" />
                    Refresh health
                  </>
                )}
              </Button>
            ) : null}

            {queryPairs.length > 0 ? (
              <>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  className="h-9 px-3.5 text-[11px]"
                  onClick={() => void refreshAllPanelCounts()}
                  disabled={isPanelActionBusy}
                >
                  {isRefreshingPanelCounts ? (
                    <span className="inline-flex items-center gap-2">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      Refreshing counts...
                    </span>
                  ) : (
                    <>
                      <RefreshCw className="h-4 w-4" />
                      Refresh counts
                    </>
                  )}
                </Button>

                <Button
                  type="button"
                  variant="destructive"
                  size="sm"
                  className="h-9 px-3.5 text-[11px]"
                  onClick={() => setIsResetAllDialogOpen(true)}
                  disabled={isPanelActionBusy}
                >
                  {isResettingAllLists ? (
                    <span className="inline-flex items-center gap-2">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      Resetting lists...
                    </span>
                  ) : (
                    <>
                      <Trash2 className="h-4 w-4" />
                      Reset lists
                    </>
                  )}
                </Button>
              </>
            ) : null}
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-3">
        {layerColumns.length === 0 ? (
          <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/40 p-4 text-sm text-mcm-walnut/70">
            No medallion layers are currently available in the system health payload.
          </div>
        ) : domainRows.length === 0 ? (
          <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/40 p-4 text-sm text-mcm-walnut/70">
            No domains found to compare.
          </div>
        ) : filteredDomainRows.length === 0 ? (
          <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/40 p-4 text-sm text-mcm-walnut/70">
            No domains found to compare.
          </div>
        ) : (
          <div className="rounded-[1.2rem] border border-mcm-walnut/20 bg-mcm-cream/30">
            <div className="overflow-x-auto overflow-y-visible rounded-[1.2rem] [scrollbar-width:none] [-ms-overflow-style:none] [&::-webkit-scrollbar]:hidden">
              <Table className="min-w-[1280px] border-collapse border-spacing-y-0">
                <caption className="sr-only">
                  Compact layer-by-layer domain coverage summary with expandable details.
                </caption>
                <TableHeader>
                  <TableRow className="h-14">
                    <TableHead className="sticky left-0 top-0 z-30 w-[320px] border-b border-mcm-walnut/25 bg-mcm-cream/90">
                      Domain
                    </TableHead>
                    {layerColumns.map((layer) => {
                      const aggregate = layerAggregateStatus.get(layer.key);
                      const layerVisual = getLayerVisual(layer.key);
                      return (
                        <TableHead
                          key={`compact-head-${layer.key}`}
                          className="sticky top-0 z-20 min-w-[190px] border-b border-mcm-walnut/25"
                          style={{
                            backgroundColor: layerVisual.strongBg,
                            boxShadow: `inset 0 2px 0 ${layerVisual.border}, inset 2px 0 0 ${layerVisual.border}, inset -1px 0 0 ${layerVisual.border}`
                          }}
                        >
                          <div className="flex items-center gap-2">
                            <span
                              className="inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-black uppercase tracking-widest"
                              style={{
                                backgroundColor: layerVisual.strongBg,
                                color: layerVisual.accent,
                                borderColor: layerVisual.border
                              }}
                            >
                              {layer.label}
                            </span>
                            {aggregate ? (
                              <span
                                className={`${StatusTypos.MONO} text-[10px] font-semibold uppercase tracking-wider`}
                                style={{ color: layerVisual.mutedText }}
                              >
                                ok {aggregate.ok} • warn {aggregate.warn} • fail {aggregate.fail}
                              </span>
                            ) : null}
                          </div>
                        </TableHead>
                      );
                    })}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredDomainRows.map((row) => {
                    const domainsForRow = domainsByLayer.get(row.key);
                    if (!domainsForRow) return null;
                    const isExpanded = expandedRowKey === row.key;
                    const expandedMaxHeightClass =
                      row.key === 'finance' ? 'max-h-[320px]' : 'max-h-[240px]';

                    const layerModels = layerColumns.map((layerColumn, layerIndex) => {
                      const isConfigured = domainsForRow.has(layerColumn.key);
                      const key = makeCellKey(layerColumn.key, row.key);
                      const metadata = metadataByCell.get(key);
                      const error = errorByCell.get(key);
                      const isPending = pendingByCell.has(key);
                      const isCellRefreshing = refreshingCells.has(key);
                      const isCellBusy = isCellRefreshing || isPending;
                      const isResettingThisCell = resettingCellKey === key && isResettingLists;
                      const isResettingThisCheckpointCell =
                        resettingCheckpointCellKey === key && isResettingCheckpoints;

                      const domainConfig = isConfigured
                        ? domainConfigByLayer.get(layerColumn.key)?.get(row.key)
                        : undefined;
                      const baseFolderUrl = normalizeAzurePortalUrl(domainConfig?.portalUrl) || '';
                      const jobName =
                        String(domainConfig?.jobName || '').trim() ||
                        extractAzureJobName(domainConfig?.jobUrl) ||
                        '';
                      const jobKey = normalizeAzureJobName(jobName);
                      const run = jobKey ? jobIndex.get(jobKey) : null;
                      const lastStartDisplay = (() => {
                        if (!jobName) return 'N/A';
                        if (!run?.startTime) return 'NO RUN';
                        return formatTimeAgo(run.startTime);
                      })();
                      const scheduleRaw = String(
                        domainConfig?.cron ||
                          domainConfig?.frequency ||
                          layersByKey.get(layerColumn.key)?.refreshFrequency ||
                          ''
                      ).trim();
                      const scheduleDisplay = scheduleRaw ? formatSchedule(scheduleRaw) : '-';

                      const dataStatusKey =
                        String(domainConfig?.status || '')
                          .trim()
                          .toLowerCase() || 'pending';
                      const dataConfig = getStatusConfig(dataStatusKey);
                      const dataLabel = toDataStatusLabel(dataStatusKey);

                      const jobStatusKey = (() => {
                        const statusKey = String(run?.status || '')
                          .trim()
                          .toLowerCase();
                        if (!jobName) return 'pending';
                        if (!run) return 'pending';
                        if (
                          statusKey === 'running' ||
                          statusKey === 'failed' ||
                          statusKey === 'success' ||
                          statusKey === 'succeeded' ||
                          statusKey === 'error' ||
                          statusKey === 'pending'
                        ) {
                          return statusKey;
                        }
                        return 'pending';
                      })();
                      const jobConfig = getStatusConfig(jobStatusKey);
                      const jobLabel = (() => {
                        if (!jobName) return 'N/A';
                        if (!run) return 'NO RUN';
                        const statusKey = String(jobStatusKey || '').toLowerCase();
                        if (statusKey === 'success' || statusKey === 'succeeded') return 'OK';
                        if (statusKey === 'failed' || statusKey === 'error') return 'FAIL';
                        if (statusKey === 'running') return 'RUN';
                        if (statusKey === 'pending') return 'PENDING';
                        return statusKey.toUpperCase();
                      })();

                      const actionJobName = String(run?.jobName || jobName).trim();
                      const runningState = jobKey ? jobStates?.[jobKey] : undefined;
                      const isSuspended =
                        String(runningState || '')
                          .trim()
                          .toLowerCase() === 'suspended';
                      const isRunning =
                        String(run?.status || '')
                          .trim()
                          .toLowerCase() === 'running' ||
                        String(runningState || '')
                          .trim()
                          .toLowerCase()
                          .includes('running');
                      const isControlling =
                        Boolean(actionJobName) && jobControl?.jobName === actionJobName;
                      const isTriggeringThisJob =
                        Boolean(actionJobName) && triggeringJob === actionJobName;
                      const isJobControlBlocked =
                        Boolean(triggeringJob) ||
                        Boolean(jobControl) ||
                        isCellBusy ||
                        isResettingLists ||
                        isResettingAllLists ||
                        isResettingCheckpoints ||
                        isRefreshingPanelCounts;

                      const executionsUrl = getAzureJobExecutionsUrl(domainConfig?.jobUrl);
                      const jobPortalUrl = normalizeAzurePortalUrl(domainConfig?.jobUrl);
                      const updatedAgo = domainConfig?.lastUpdated
                        ? formatTimeAgo(domainConfig.lastUpdated)
                        : '--';
                      const updatedLabel = domainConfig?.lastUpdated
                        ? `${updatedAgo} ago`
                        : 'unknown';
                      const adlsModifiedAt = metadata?.folderLastModified || null;
                      const adlsModifiedDisplay = adlsModifiedAt
                        ? formatTimeAgo(adlsModifiedAt)
                        : 'N/A';
                      const isPurgingThisTarget =
                        isPurging &&
                        activePurgeTarget?.layerKey === layerColumn.key &&
                        activePurgeTarget?.domainKey === row.key;

                      let previousMetadata: DomainMetadata | null = null;
                      let previousLabel = '';
                      for (let index = layerIndex - 1; index >= 0; index -= 1) {
                        const previousLayer = layerColumns[index];
                        const previousCellKey = makeCellKey(previousLayer.key, row.key);
                        const candidate = metadataByCell.get(previousCellKey);
                        if (!candidate) continue;
                        previousMetadata = candidate;
                        previousLabel = previousLayer.label;
                        break;
                      }

                      const symbolComparison =
                        metadata && previousMetadata
                          ? compareSymbols(metadata, previousMetadata)
                          : null;
                      const blacklistSummary = metadata
                        ? summarizeBlacklistCount(metadata)
                        : { text: 'blacklist n/a', className: 'text-mcm-walnut/70' };
                      const financeSubfolderCounts =
                        row.key === 'finance' && metadata
                          ? FINANCE_SUBFOLDER_ITEMS.map((item) => ({
                              ...item,
                              count: metadata.financeSubfolderSymbolCounts?.[item.key]
                            }))
                          : [];
                      const showFinanceSubfolders =
                        financeSubfolderCounts.length > 0 &&
                        financeSubfolderCounts.some((item) => hasFiniteNumber(item.count));
                      const layerVisual = getLayerVisual(layerColumn.key);
                      const supportsCheckpointReset = CHECKPOINT_RESET_LAYERS.has(layerColumn.key);

                      return {
                        key,
                        layerColumn,
                        layerIndex,
                        isConfigured,
                        metadata,
                        error,
                        isPending,
                        isCellRefreshing,
                        isCellBusy,
                        isResettingThisCell,
                        isResettingThisCheckpointCell,
                        domainConfig,
                        baseFolderUrl,
                        jobName,
                        run,
                        dataStatusKey,
                        dataConfig,
                        dataLabel,
                        jobStatusKey,
                        jobConfig,
                        jobLabel,
                        lastStartDisplay,
                        scheduleRaw,
                        scheduleDisplay,
                        actionJobName,
                        isSuspended,
                        isRunning,
                        isControlling,
                        isTriggeringThisJob,
                        isJobControlBlocked,
                        executionsUrl,
                        jobPortalUrl,
                        updatedLabel,
                        adlsModifiedAt,
                        adlsModifiedDisplay,
                        isPurgingThisTarget,
                        symbolComparison,
                        previousLabel,
                        blacklistSummary,
                        financeSubfolderCounts,
                        showFinanceSubfolders,
                        layerVisual,
                        supportsCheckpointReset
                      };
                    });

                    const configuredModels = layerModels.filter((model) => model.isConfigured);
                    const preferredModel =
                      ACTION_LAYER_PRIORITY.reduce<(typeof configuredModels)[number] | null>(
                        (current, layerKey) => {
                          if (current) return current;
                          return (
                            configuredModels.find(
                              (model) =>
                                model.layerColumn.key === layerKey && Boolean(model.actionJobName)
                            ) || null
                          );
                        },
                        null
                      ) ||
                      configuredModels.find((model) => Boolean(model.actionJobName)) ||
                      configuredModels[0] ||
                      null;

                    const refreshDomainRow = async () => {
                      if (
                        isRefreshingPanelCounts ||
                        isResettingAllLists ||
                        isResettingLists ||
                        isResettingCheckpoints
                      )
                        return;
                      const refreshTargets = configuredModels.map((model) =>
                        handleCellRefresh(model.layerColumn.key, row.key)
                      );
                      await Promise.allSettled(refreshTargets);
                    };
                    const toggleRowExpanded = () =>
                      setExpandedRowKey((previous) => (previous === row.key ? null : row.key));

                    return [
                      <TableRow
                        key={`summary-${row.key}`}
                        className="h-[52px] cursor-pointer even:[&>td]:bg-mcm-cream/20"
                        onClick={(event) => {
                          const target = event.target as HTMLElement | null;
                          if (
                            target?.closest(
                              'button, a, input, select, textarea, [role="button"], [role="menuitem"], [data-no-row-toggle="true"]'
                            )
                          ) {
                            return;
                          }
                          toggleRowExpanded();
                        }}
                      >
                        <TableCell className="sticky left-0 z-10 border-b border-mcm-walnut/20 bg-mcm-paper/95 py-1.5">
                          <div className="flex items-center justify-between gap-2">
                            <div className="min-w-0 flex flex-col gap-0.5">
                              <span className="truncate font-semibold text-mcm-walnut">{row.label}</span>
                              <span className={`${StatusTypos.MONO} truncate text-[11px] text-mcm-walnut/75`}>
                                {row.key}
                              </span>
                            </div>
                            <div className="flex shrink-0 items-center justify-end gap-1">
                              <Button
                                type="button"
                                variant="outline"
                                size="sm"
                                className="h-7 px-2 text-[11px]"
                                disabled={
                                  !preferredModel?.actionJobName || preferredModel.isJobControlBlocked
                                }
                                onClick={() => {
                                  if (!preferredModel?.actionJobName) return;
                                  if (preferredModel.isRunning) {
                                    void setJobSuspended(preferredModel.actionJobName, true);
                                  } else {
                                    void triggerJob(preferredModel.actionJobName);
                                  }
                                }}
                              >
                                {preferredModel?.isControlling ||
                                preferredModel?.isTriggeringThisJob ? (
                                  <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                                ) : preferredModel?.isRunning ? (
                                  <Square className="mr-1 h-3.5 w-3.5" />
                                ) : (
                                  <Play className="mr-1 h-3.5 w-3.5" />
                                )}
                                {preferredModel?.isRunning ? 'Stop' : 'Run'}
                              </Button>
                              <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                  <Button
                                    type="button"
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7"
                                    aria-label={`More actions for ${row.label}`}
                                  >
                                    <EllipsisVertical className="h-4 w-4" />
                                  </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="w-56">
                                  <DropdownMenuLabel>{row.label}</DropdownMenuLabel>
                                  <DropdownMenuItem
                                    onSelect={(event) => {
                                      event.preventDefault();
                                      void refreshDomainRow();
                                    }}
                                  >
                                    <RefreshCw className="h-4 w-4" />
                                    Refresh domain counts
                                  </DropdownMenuItem>
                                  <DropdownMenuSeparator />
                                  {preferredModel?.jobPortalUrl ? (
                                    <DropdownMenuItem asChild>
                                      <a
                                        href={preferredModel.jobPortalUrl}
                                        target="_blank"
                                        rel="noreferrer"
                                      >
                                        <ExternalLink className="h-4 w-4" />
                                        Open job in Azure
                                      </a>
                                    </DropdownMenuItem>
                                  ) : null}
                                  {preferredModel?.executionsUrl ? (
                                    <DropdownMenuItem asChild>
                                      <a
                                        href={preferredModel.executionsUrl}
                                        target="_blank"
                                        rel="noreferrer"
                                      >
                                        <ScrollText className="h-4 w-4" />
                                        Open run history
                                      </a>
                                    </DropdownMenuItem>
                                  ) : null}
                                  {preferredModel?.baseFolderUrl ? (
                                    <DropdownMenuItem asChild>
                                      <a
                                        href={preferredModel.baseFolderUrl}
                                        target="_blank"
                                        rel="noreferrer"
                                      >
                                        <FolderOpen className="h-4 w-4" />
                                        Open ADLS folder
                                      </a>
                                    </DropdownMenuItem>
                                  ) : null}
                                </DropdownMenuContent>
                              </DropdownMenu>
                            </div>
                          </div>
                        </TableCell>

                        {layerModels.map((model) => {
                          if (!model.isConfigured) {
                            return (
                              <TableCell
                                key={`summary-${row.key}-${model.layerColumn.key}`}
                                className={`${StatusTypos.MONO} border-b border-mcm-walnut/20 py-1.5 text-center text-[12px] text-mcm-walnut/65`}
                                style={{
                                  backgroundColor: model.layerVisual.softBg,
                                  boxShadow: `inset 3px 0 0 ${model.layerVisual.border}, inset -1px 0 0 ${model.layerVisual.border}`
                                }}
                              >
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <span className="inline-flex cursor-default items-center justify-center">
                                      —
                                    </span>
                                  </TooltipTrigger>
                                  <TooltipContent side="top">
                                    {model.layerColumn.label} is not configured for {row.label}
                                  </TooltipContent>
                                </Tooltip>
                              </TableCell>
                            );
                          }

                          const DataIcon = model.dataConfig.icon;
                          const JobIcon = model.jobConfig.icon;
                          return (
                            <TableCell
                              key={`summary-${row.key}-${model.layerColumn.key}`}
                              className="border-b border-mcm-walnut/20 py-1.5"
                              style={{
                                backgroundColor: model.layerVisual.softBg,
                                boxShadow: `inset 3px 0 0 ${model.layerVisual.border}, inset -1px 0 0 ${model.layerVisual.border}`
                              }}
                            >
                              <div className="flex items-center justify-between gap-2">
                                <span
                                  className={`${StatusTypos.MONO} tabular-nums text-right text-sm font-bold text-mcm-walnut`}
                                >
                                  {formatSymbolCount(model.metadata?.symbolCount)}
                                </span>
                                <div className="flex flex-col items-end gap-1">
                                  <span
                                    tabIndex={0}
                                    className="inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-black uppercase tracking-widest focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-mcm-teal/50"
                                    style={{
                                      backgroundColor: model.dataConfig.bg,
                                      color: model.dataConfig.text,
                                      borderColor: model.dataConfig.border
                                    }}
                                  >
                                    <DataIcon className="h-3 w-3" />
                                    {model.dataLabel}
                                  </span>
                                  <span
                                    tabIndex={0}
                                    className="inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-black uppercase tracking-widest focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-mcm-teal/50"
                                    style={{
                                      backgroundColor: model.jobConfig.bg,
                                      color: model.jobConfig.text,
                                      borderColor: model.jobConfig.border
                                    }}
                                  >
                                    <JobIcon className="h-3 w-3" />
                                    {model.jobLabel}
                                  </span>
                                </div>
                              </div>
                              {model.isPending ? (
                                <div
                                  className={`${StatusTypos.MONO} mt-1 text-[11px] text-mcm-walnut/70`}
                                >
                                  refreshing...
                                </div>
                              ) : null}
                              {model.error ? (
                                <div
                                  className={`${StatusTypos.MONO} mt-1 text-[11px] text-destructive/90`}
                                >
                                  metadata warning
                                </div>
                              ) : null}
                            </TableCell>
                          );
                        })}

                      </TableRow>,

                      <TableRow
                        key={`details-${row.key}`}
                        className="border-0 hover:bg-transparent"
                      >
                        <TableCell
                          colSpan={layerColumns.length + 1}
                          className="border-0 bg-transparent p-0"
                        >
                          <div
                            className={`transition-[max-height,opacity,transform] duration-300 ${
                              isExpanded
                                ? `${expandedMaxHeightClass} opacity-100 translate-y-0 overflow-hidden`
                                : 'max-h-0 opacity-0 -translate-y-1 overflow-hidden pointer-events-none'
                            }`}
                            aria-hidden={!isExpanded}
                          >
                            <div className="border-t border-mcm-walnut/20 bg-mcm-paper/50 p-3">
                              <div className="grid gap-2 lg:grid-cols-2 xl:grid-cols-4">
                                {configuredModels.map((model) => {
                                  const DataIcon = model.dataConfig.icon;
                                  const JobIcon = model.jobConfig.icon;
                                  return (
                                    <div
                                      key={`detail-card-${row.key}-${model.layerColumn.key}`}
                                      className="flex min-h-[132px] flex-col rounded-lg border border-mcm-walnut/20 bg-mcm-cream/35 p-2.5"
                                      style={{
                                        backgroundColor: model.layerVisual.strongBg,
                                        borderColor: model.layerVisual.border,
                                        boxShadow: `inset 4px 0 0 ${model.layerVisual.border}, inset 0 2px 0 ${model.layerVisual.border}`
                                      }}
                                    >
                                      <div className="flex items-start justify-between gap-2">
                                        <div>
                                          <div
                                            className="text-[11px] font-black uppercase tracking-widest"
                                            style={{ color: model.layerVisual.accent }}
                                          >
                                            {model.layerColumn.label}
                                          </div>
                                          <div
                                            className={`${StatusTypos.MONO} tabular-nums text-lg font-black text-mcm-walnut`}
                                          >
                                            {formatSymbolCount(model.metadata?.symbolCount)}
                                          </div>
                                        </div>
                                        <div className="flex flex-col items-end gap-1">
                                          <span
                                            className="inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-black uppercase tracking-widest"
                                            style={{
                                              backgroundColor: model.dataConfig.bg,
                                              color: model.dataConfig.text,
                                              borderColor: model.dataConfig.border
                                            }}
                                          >
                                            <DataIcon className="h-3 w-3" />
                                            {model.dataLabel}
                                          </span>
                                          <span
                                            className="inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-[10px] font-black uppercase tracking-widest"
                                            style={{
                                              backgroundColor: model.jobConfig.bg,
                                              color: model.jobConfig.text,
                                              borderColor: model.jobConfig.border
                                            }}
                                          >
                                            <JobIcon className="h-3 w-3" />
                                            {model.jobLabel}
                                          </span>
                                        </div>
                                      </div>

                                      <div className="mt-2 space-y-1 text-[11px] leading-snug">
                                        {model.symbolComparison ? (
                                          <div className={`${StatusTypos.MONO}`}>
                                            <span className="text-mcm-walnut/70">
                                              vs {model.previousLabel}:{' '}
                                            </span>
                                            <span className={model.symbolComparison.className}>
                                              {model.symbolComparison.text}
                                            </span>
                                            <span className="text-mcm-walnut/60">{' | '}</span>
                                            <span className={model.blacklistSummary.className}>
                                              {model.blacklistSummary.text}
                                            </span>
                                          </div>
                                        ) : (
                                          <div className={`${StatusTypos.MONO}`}>
                                            <span className={model.blacklistSummary.className}>
                                              {model.blacklistSummary.text}
                                            </span>
                                          </div>
                                        )}
                                        <div
                                          className={`${StatusTypos.MONO} flex items-center gap-1`}
                                        >
                                          <span className="text-mcm-walnut/70">last start:</span>
                                          <span
                                            className="text-mcm-walnut/90"
                                            title={model.run?.startTime || undefined}
                                          >
                                            {model.lastStartDisplay}
                                          </span>
                                        </div>
                                        <div
                                          className={`${StatusTypos.MONO} flex items-center gap-1`}
                                        >
                                          <span className="text-mcm-walnut/70">schedule:</span>
                                          <span
                                            className="truncate text-mcm-walnut/90"
                                            title={model.scheduleRaw || undefined}
                                          >
                                            {model.scheduleDisplay}
                                          </span>
                                        </div>
                                        <div
                                          className={`${StatusTypos.MONO} flex items-center gap-1`}
                                        >
                                          <span className="text-mcm-walnut/70">adls modified:</span>
                                          <span
                                            className="text-mcm-walnut/90"
                                            title={model.adlsModifiedAt || undefined}
                                          >
                                            {model.adlsModifiedDisplay}
                                          </span>
                                        </div>
                                        {model.showFinanceSubfolders ? (
                                          <div className="grid grid-cols-2 gap-x-2 gap-y-0.5">
                                            {model.financeSubfolderCounts.map((item) => (
                                              <div
                                                key={`finance-detail-${row.key}-${model.layerColumn.key}-${item.key}`}
                                                className="flex items-center justify-between"
                                              >
                                                <span className="text-mcm-walnut/80">
                                                  {item.label}
                                                </span>
                                                <span
                                                  className={`${StatusTypos.MONO} tabular-nums text-mcm-walnut/95`}
                                                >
                                                  {formatInt(item.count)}
                                                </span>
                                              </div>
                                            ))}
                                          </div>
                                        ) : null}
                                        {model.isCellRefreshing ? (
                                          <div
                                            className={`${StatusTypos.MONO} inline-flex items-center gap-1 text-mcm-walnut/75`}
                                          >
                                            <RefreshCw className="h-3 w-3 animate-spin" />
                                            refreshing...
                                          </div>
                                        ) : null}
                                      </div>

                                      <div className="mt-auto flex items-center justify-end gap-1 pt-2">
                                        <Button
                                          type="button"
                                          size="sm"
                                          variant="outline"
                                          className="h-7 px-2 text-[11px]"
                                          disabled={
                                            !model.actionJobName || model.isJobControlBlocked
                                          }
                                          onClick={() => {
                                            if (!model.actionJobName) return;
                                            if (model.isRunning) {
                                              void setJobSuspended(model.actionJobName, true);
                                            } else {
                                              void triggerJob(model.actionJobName);
                                            }
                                          }}
                                        >
                                          {model.isControlling || model.isTriggeringThisJob ? (
                                            <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                                          ) : model.isRunning ? (
                                            <Square className="mr-1 h-3.5 w-3.5" />
                                          ) : (
                                            <Play className="mr-1 h-3.5 w-3.5" />
                                          )}
                                          {model.isRunning ? 'Stop' : 'Run'}
                                        </Button>
                                        <DropdownMenu>
                                          <DropdownMenuTrigger asChild>
                                            <Button
                                              type="button"
                                              variant="ghost"
                                              size="icon"
                                              className="h-7 w-7"
                                              aria-label={`More ${model.layerColumn.label} actions for ${row.label}`}
                                            >
                                              <EllipsisVertical className="h-4 w-4" />
                                            </Button>
                                          </DropdownMenuTrigger>
                                          <DropdownMenuContent align="end" className="w-56">
                                            <DropdownMenuLabel className="flex items-center justify-between gap-2">
                                              <span>{model.layerColumn.label} • {row.label}</span>
                                              {model.isCellRefreshing ? (
                                                <span
                                                  className={`${StatusTypos.MONO} inline-flex items-center gap-1 text-[10px] font-medium uppercase tracking-wide text-mcm-walnut/75`}
                                                >
                                                  <RefreshCw className="h-3 w-3 animate-spin" />
                                                  refreshing
                                                </span>
                                              ) : null}
                                            </DropdownMenuLabel>
                                            <DropdownMenuItem
                                              disabled={
                                                model.isCellBusy ||
                                                isResettingAllLists ||
                                                isResettingCheckpoints
                                              }
                                              onSelect={(event) => {
                                                event.preventDefault();
                                                void handleCellRefresh(
                                                  model.layerColumn.key,
                                                  row.key
                                                );
                                              }}
                                            >
                                              <RefreshCw
                                                className={`h-4 w-4 ${model.isCellRefreshing ? 'animate-spin' : ''}`}
                                              />
                                              {model.isCellRefreshing
                                                ? 'Refreshing...'
                                                : model.isPending
                                                  ? 'Loading metadata...'
                                                  : 'Refresh'}
                                            </DropdownMenuItem>
                                            {model.baseFolderUrl ? (
                                              <DropdownMenuItem asChild>
                                                <a
                                                  href={model.baseFolderUrl}
                                                  target="_blank"
                                                  rel="noreferrer"
                                                >
                                                  <FolderOpen className="h-4 w-4" />
                                                  Open ADLS folder
                                                </a>
                                              </DropdownMenuItem>
                                            ) : null}
                                            {model.jobPortalUrl ? (
                                              <DropdownMenuItem asChild>
                                                <a
                                                  href={model.jobPortalUrl}
                                                  target="_blank"
                                                  rel="noreferrer"
                                                >
                                                  <ExternalLink className="h-4 w-4" />
                                                  Open job in Azure
                                                </a>
                                              </DropdownMenuItem>
                                            ) : null}
                                            {model.executionsUrl ? (
                                              <DropdownMenuItem asChild>
                                                <a
                                                  href={model.executionsUrl}
                                                  target="_blank"
                                                  rel="noreferrer"
                                                >
                                                  <ScrollText className="h-4 w-4" />
                                                  Execution history
                                                </a>
                                              </DropdownMenuItem>
                                            ) : null}
                                            <DropdownMenuSeparator />
                                            <DropdownMenuItem
                                              disabled={
                                                !model.actionJobName || model.isJobControlBlocked
                                              }
                                              onSelect={(event) => {
                                                event.preventDefault();
                                                if (!model.actionJobName) return;
                                                void setJobSuspended(
                                                  model.actionJobName,
                                                  !model.isSuspended
                                                );
                                              }}
                                            >
                                              {model.isSuspended ? (
                                                <CirclePlay className="h-4 w-4" />
                                              ) : (
                                                <CirclePause className="h-4 w-4" />
                                              )}
                                              {model.isSuspended ? 'Resume job' : 'Suspend job'}
                                            </DropdownMenuItem>
                                            <DropdownMenuItem
                                              disabled={
                                                !model.supportsCheckpointReset ||
                                                model.isCellBusy ||
                                                isResettingCheckpoints ||
                                                isResettingLists ||
                                                isRefreshingPanelCounts ||
                                                isResettingAllLists
                                              }
                                              onSelect={() =>
                                                setCheckpointResetTarget({
                                                  layerKey: model.layerColumn.key,
                                                  layerLabel: model.layerColumn.label,
                                                  domainKey: row.key,
                                                  domainLabel: row.label
                                                })
                                              }
                                            >
                                              <RotateCcw className="h-4 w-4" />
                                              {model.isResettingThisCheckpointCell
                                                ? 'Resetting checkpoints...'
                                                : model.supportsCheckpointReset
                                                  ? 'Reset checkpoints'
                                                  : 'Checkpoint reset unavailable'}
                                            </DropdownMenuItem>
                                            <DropdownMenuItem
                                              disabled={
                                                model.isCellBusy ||
                                                isResettingCheckpoints ||
                                                isResettingLists ||
                                                isRefreshingPanelCounts ||
                                                isResettingAllLists
                                              }
                                              onSelect={() =>
                                                setListResetTarget({
                                                  layerKey: model.layerColumn.key,
                                                  layerLabel: model.layerColumn.label,
                                                  domainKey: row.key,
                                                  domainLabel: row.label
                                                })
                                              }
                                            >
                                              <Trash2 className="h-4 w-4" />
                                              {model.isResettingThisCell
                                                ? 'Resetting lists...'
                                                : 'Reset lists'}
                                            </DropdownMenuItem>
                                            <DropdownMenuItem
                                              disabled={
                                                isPurging ||
                                                model.isCellBusy ||
                                                isResettingCheckpoints ||
                                                isResettingLists ||
                                                isResettingAllLists ||
                                                isRefreshingPanelCounts
                                              }
                                              onSelect={() =>
                                                setPurgeTarget({
                                                  layerKey: model.layerColumn.key,
                                                  layerLabel: model.layerColumn.label,
                                                  domainKey: row.key,
                                                  domainLabel: row.label
                                                })
                                              }
                                            >
                                              <Trash2
                                                className={`h-4 w-4 ${
                                                  model.isPurgingThisTarget
                                                    ? 'animate-spin text-rose-600'
                                                    : ''
                                                }`}
                                              />
                                              {model.isPurgingThisTarget
                                                ? 'Purging data...'
                                                : 'Purge data'}
                                            </DropdownMenuItem>
                                          </DropdownMenuContent>
                                        </DropdownMenu>
                                      </div>
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          </div>
                        </TableCell>
                      </TableRow>
                    ];
                  })}
                </TableBody>
              </Table>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
