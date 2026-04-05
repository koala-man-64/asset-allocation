import { useCallback, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import {
  AlertTriangle,
  ChevronDown,
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
import type {
  DataDomain,
  DataLayer,
  DomainMetadata,
  JobRun,
  ResourceSignal
} from '@/types/strategy';
import { StatusTypos } from '@/app/components/pages/system-status/StatusTokens';
import {
  normalizeDomainKey,
  normalizeLayerKey
} from '@/app/components/pages/system-status/SystemPurgeControls';
import { getDomainOrderEntries } from '@/app/components/pages/system-status/domainOrdering';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { formatSystemStatusText } from '@/app/components/pages/system-status/systemStatusText';
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
import {
  DomainListViewerSheet,
  type DomainListViewerTarget
} from '@/app/components/pages/system-status/DomainListViewerSheet';
import type { ManagedContainerJob } from '@/app/components/pages/system-status/JobKillSwitchPanel';
import { useJobSuspend } from '@/hooks/useJobSuspend';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import {
  buildLatestJobRunIndex,
  effectiveJobStatus,
  formatDuration,
  formatSchedule,
  formatTimeAgo,
  getStatusConfig,
  getAzureJobExecutionsUrl,
  hasActiveJobRunningState,
  normalizeAzureJobName,
  normalizeAzurePortalUrl,
  resolveManagedJobName,
  isSuspendedJobRunningState,
  toJobStatusLabel
} from '@/app/components/pages/system-status/SystemStatusHelpers';
import { formatMetadataTimestamp } from '@/app/components/pages/system-status/systemStatusClock';

const LAYER_ORDER = ['bronze', 'silver', 'gold', 'platinum'] as const;
type LayerKey = (typeof LAYER_ORDER)[number];
const CHECKPOINT_RESET_LAYERS = new Set<LayerKey>(['silver', 'gold']);
const DOMAIN_COLUMN_WIDTH_PX = 320;
const PURGE_POLL_INTERVAL_MS = 1000;
const PURGE_POLL_TIMEOUT_MS = 5 * 60_000;
const CPU_USAGE_PERCENT_SIGNAL_NAMES = [
  'cpupercent',
  'cpupercentage',
  'cpuusagepercent',
  'cpuusage'
];
const CPU_USAGE_RAW_SIGNAL_NAMES = ['usagenanocores'];
const MEMORY_USAGE_PERCENT_SIGNAL_NAMES = ['memorypercent', 'memoryusagepercent', 'memoryusage'];
const MEMORY_USAGE_RAW_SIGNAL_NAMES = [
  'usagebytes',
  'memoryworkingsetbytes',
  'workingsetbytes',
  'memorybytes'
];
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

function getPurgeDeletedCount(result: unknown): number {
  if (typeof result !== 'object' || result === null) {
    return 0;
  }
  const totalDeleted = (result as { totalDeleted?: unknown }).totalDeleted;
  return typeof totalDeleted === 'number' && Number.isFinite(totalDeleted) ? totalDeleted : 0;
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

type JobDurationSummary = {
  averageDurationSeconds: number;
  sampleCount: number;
};

function formatInt(value: number | null | undefined): string {
  if (!hasFiniteNumber(value)) return 'N/A';
  return numberFormatter.format(value);
}

function formatSymbolCount(value: number | null | undefined): string {
  if (!hasFiniteNumber(value)) return 'N/A';
  return `${numberFormatter.format(value)} symbols`;
}

function resolveColumnCount(metadata?: DomainMetadata | null): number | null {
  if (!metadata) return null;
  if (hasFiniteNumber(metadata.columnCount)) return metadata.columnCount;
  if (Array.isArray(metadata.columns)) return metadata.columns.length;
  return null;
}

function formatColumnCount(value: number | null | undefined): string {
  if (!hasFiniteNumber(value)) return 'cols n/a';
  return `${numberFormatter.format(value)} cols`;
}

function buildJobDurationSummaryIndex(recentJobs: JobRun[] = []): Map<string, JobDurationSummary> {
  const totals = new Map<string, { totalSeconds: number; sampleCount: number }>();

  for (const job of recentJobs) {
    const jobKey = normalizeAzureJobName(job?.jobName);
    const duration = job?.duration;
    if (!jobKey || !hasFiniteNumber(duration) || duration < 0) {
      continue;
    }

    const current = totals.get(jobKey) || { totalSeconds: 0, sampleCount: 0 };
    current.totalSeconds += duration;
    current.sampleCount += 1;
    totals.set(jobKey, current);
  }

  const summary = new Map<string, JobDurationSummary>();
  for (const [jobKey, current] of totals) {
    if (current.sampleCount <= 0) {
      continue;
    }
    summary.set(jobKey, {
      averageDurationSeconds: current.totalSeconds / current.sampleCount,
      sampleCount: current.sampleCount
    });
  }

  return summary;
}

function formatStorageBytes(value: number | null | undefined): string {
  if (!hasFiniteNumber(value)) return 'size n/a';
  if (value === 0) return '0 B';
  if (value < 1024) return `${value} B`;
  const units = ['KB', 'MB', 'GB', 'TB', 'PB'] as const;
  let size = value / 1024;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function normalizeSignalName(value?: string | null): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function formatMetricPercent(value: number): string {
  return (
    new Intl.NumberFormat(undefined, {
      maximumFractionDigits: value >= 10 ? 0 : 1
    }).format(value) + '%'
  );
}

function formatMetricNumber(value: number, maximumFractionDigits = 1): string {
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits
  }).format(value);
}

function formatBinaryBytes(value: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const sign = value < 0 ? -1 : 1;
  let scaled = Math.abs(value);
  let unitIndex = 0;

  while (scaled >= 1024 && unitIndex < units.length - 1) {
    scaled /= 1024;
    unitIndex += 1;
  }

  const maximumFractionDigits = scaled >= 100 || unitIndex === 0 ? 0 : scaled >= 10 ? 1 : 2;
  return `${formatMetricNumber(sign * scaled, maximumFractionDigits)} ${units[unitIndex]}`;
}

function formatCpuCoresFromNanocores(value: number): string {
  const cores = value / 1_000_000_000;
  const maximumFractionDigits = cores >= 10 ? 1 : cores >= 1 ? 2 : 3;
  return `${formatMetricNumber(cores, maximumFractionDigits)} cores`;
}

function formatUsageSignal(
  signal: ResourceSignal | null | undefined,
  metric: 'cpu' | 'memory'
): string {
  if (!signal || !hasFiniteNumber(signal.value)) {
    return 'N/A';
  }

  const unit = normalizeSignalName(signal.unit);
  if (unit.includes('percent')) {
    return formatMetricPercent(signal.value);
  }

  if (metric === 'cpu' && unit.includes('nanocore')) {
    return formatCpuCoresFromNanocores(signal.value);
  }

  if (metric === 'memory' && unit.includes('byte')) {
    return formatBinaryBytes(signal.value);
  }

  const suffix = String(signal.unit || '').trim();
  const valueText = formatMetricNumber(signal.value);
  return suffix ? `${valueText} ${suffix}` : valueText;
}

function findPreferredSignal(
  signals: ResourceSignal[] | null | undefined,
  preferredNames: string[]
): ResourceSignal | null {
  if (!Array.isArray(signals) || signals.length === 0) {
    return null;
  }

  const normalizedSignals = signals.map((signal) => ({
    signal,
    name: normalizeSignalName(signal?.name)
  }));

  for (const preferredName of preferredNames) {
    const normalizedPreferredName = normalizeSignalName(preferredName);
    const exactMatch = normalizedSignals.find((entry) => entry.name === normalizedPreferredName);
    if (exactMatch) {
      return exactMatch.signal;
    }

    const broadMatch = normalizedSignals.find(
      (entry) =>
        entry.name &&
        (entry.name.includes(normalizedPreferredName) ||
          normalizedPreferredName.includes(entry.name))
    );
    if (broadMatch) {
      return broadMatch.signal;
    }
  }

  return null;
}

function buildRunningUsageDisplay(signals: ResourceSignal[] | null | undefined): {
  cpuDisplay: string;
  memoryDisplay: string;
  compactText: string | null;
} | null {
  if (!Array.isArray(signals) || signals.length === 0) {
    return null;
  }

  const cpuDisplay = formatUsageSignal(
    findPreferredSignal(signals, [
      ...CPU_USAGE_RAW_SIGNAL_NAMES,
      ...CPU_USAGE_PERCENT_SIGNAL_NAMES
    ]),
    'cpu'
  );
  const memoryDisplay = formatUsageSignal(
    findPreferredSignal(signals, [
      ...MEMORY_USAGE_RAW_SIGNAL_NAMES,
      ...MEMORY_USAGE_PERCENT_SIGNAL_NAMES
    ]),
    'memory'
  );
  const compactParts = [
    cpuDisplay !== 'N/A' ? `cpu ${cpuDisplay}` : null,
    memoryDisplay !== 'N/A' ? `mem ${memoryDisplay}` : null
  ].filter((value): value is string => Boolean(value));

  return {
    cpuDisplay,
    memoryDisplay,
    compactText: compactParts.length > 0 ? compactParts.join(' | ') : null
  };
}

function formatDateRangeBoundary(value: string | null | undefined): string {
  if (!value) return '—';
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toISOString().slice(0, 10);
}

function formatMetadataDateRange(metadata?: DomainMetadata | null): string | null {
  const min = metadata?.dateRange?.min;
  const max = metadata?.dateRange?.max;
  if (!min && !max) return null;

  const formattedMin = formatDateRangeBoundary(min);
  const formattedMax = formatDateRangeBoundary(max);
  if (min && max && formattedMin === formattedMax) {
    return formattedMin;
  }
  return `${formattedMin} → ${formattedMax}`;
}

function describeMetadataDateRange(metadata?: DomainMetadata | null): string | null {
  const fragments = [
    metadata?.dateRange?.column ? `column=${metadata.dateRange.column}` : null,
    metadata?.dateRange?.source ? `source=${metadata.dateRange.source}` : null
  ].filter((fragment): fragment is string => Boolean(fragment));

  return fragments.length > 0 ? fragments.join(' • ') : null;
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

function summarizeRetrySymbols(run?: JobRun | null): {
  text: string;
  title?: string;
} | null {
  const metadata = run?.metadata;
  if (!metadata) return null;

  const previewSymbols = Array.isArray(metadata.retrySymbols)
    ? metadata.retrySymbols
        .map((value) => String(value || '').trim())
        .filter((value): value is string => Boolean(value))
    : [];
  const totalCount = hasFiniteNumber(metadata.retrySymbolCount)
    ? metadata.retrySymbolCount
    : previewSymbols.length;
  if (totalCount <= 0 && previewSymbols.length === 0) {
    return null;
  }

  const preview = previewSymbols.join(', ');
  const isTruncated = Boolean(metadata.retrySymbolsTruncated);
  const hiddenCount = Math.max(totalCount - previewSymbols.length, 0);

  if (preview && hiddenCount === 0 && !isTruncated) {
    return {
      text: preview,
      title: preview
    };
  }

  const fragments = [`${numberFormatter.format(totalCount)} total`];
  if (preview) {
    fragments.push(preview);
  }
  if (hiddenCount > 0) {
    fragments.push(`+${numberFormatter.format(hiddenCount)} more`);
  }

  const titleFragments = [preview || null, isTruncated ? 'job log preview truncated' : null].filter(
    (value): value is string => Boolean(value)
  );

  return {
    text: fragments.join(' â€¢ '),
    title: titleFragments.join(' â€¢ ') || undefined
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
  metadataSnapshot?: DomainMetadataSnapshotResponse;
  metadataUpdatedAt?: string | null;
  metadataSource?: string | null;
  onMetadataSnapshotChange?: (
    updater: (
      previous: DomainMetadataSnapshotResponse | undefined
    ) => DomainMetadataSnapshotResponse | undefined
  ) => void;
  onRefresh?: () => Promise<void> | void;
  isRefreshing?: boolean;
  isFetching?: boolean;
}

export function DomainLayerComparisonPanel({
  overall: _overall = 'unknown',
  dataLayers,
  recentJobs = [],
  jobStates,
  managedContainerJobs = [],
  metadataSnapshot,
  metadataUpdatedAt,
  metadataSource,
  onMetadataSnapshotChange,
  onRefresh,
  isRefreshing,
  isFetching
}: DomainLayerComparisonPanelProps) {
  const queryClient = useQueryClient();
  const { triggeringJob, triggerJob } = useJobTrigger();
  const { jobControl, setJobSuspended, stopJob } = useJobSuspend();
  const [refreshingCells, setRefreshingCells] = useState<Set<string>>(new Set());
  const [triggeringLayerKeys, setTriggeringLayerKeys] = useState<Set<LayerKey>>(new Set());
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
      const hasDomains = (layer.domains || []).some((domain) => {
        const domainName = String(domain?.name || '').trim();
        return Boolean(normalizeDomainKey(domainName));
      });
      if (!hasDomains) continue;
      columns.push({ key, label: String(layer.name || key).trim() || key });
    }
    return columns;
  }, [layersByKey]);

  const jobIndex = useMemo(() => {
    return buildLatestJobRunIndex(recentJobs);
  }, [recentJobs]);
  const jobDurationSummaryIndex = useMemo(() => {
    return buildJobDurationSummaryIndex(recentJobs);
  }, [recentJobs]);

  const managedJobIndex = useMemo(() => {
    const index = new Map<string, ManagedContainerJob>();
    for (const job of managedContainerJobs) {
      const key = normalizeAzureJobName(String(job?.name || ''));
      if (!key || index.has(key)) continue;
      index.set(key, job);
    }
    return index;
  }, [managedContainerJobs]);

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

  const queryPairsByLayer = useMemo(() => {
    const index = new Map<LayerKey, Array<{ layerKey: LayerKey; domainKey: string }>>();
    for (const pair of queryPairs) {
      const current = index.get(pair.layerKey) || [];
      current.push(pair);
      index.set(pair.layerKey, current);
    }
    return index;
  }, [queryPairs]);

  const statusInvalidationKeys = useMemo(
    () => [queryKeys.systemStatusView(), queryKeys.systemHealth()] as const,
    []
  );

  const updateMetadataSnapshot = useCallback(
    (
      updater: (
        previous: DomainMetadataSnapshotResponse | undefined
      ) => DomainMetadataSnapshotResponse | undefined
    ) => {
      if (onMetadataSnapshotChange) {
        onMetadataSnapshotChange(updater);
        return;
      }
      queryClient.setQueryData(queryKeys.domainMetadataSnapshot('all', 'all'), updater);
    },
    [onMetadataSnapshotChange, queryClient]
  );

  const refreshStatus = useCallback(async () => {
    if (onRefresh) {
      await onRefresh();
      return;
    }
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.systemStatusView() }),
      queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() })
    ]);
  }, [onRefresh, queryClient]);

  const { metadataByCell, errorByCell, pendingByCell } = useMemo(() => {
    const metadata = new Map<string, DomainMetadata>();
    const errors = new Map<string, string>();
    const pending = new Set<string>();
    const snapshotEntries = metadataSnapshot?.entries || {};

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
      if (refreshingCells.has(key)) {
        pending.add(key);
      }
    });

    return { metadataByCell: metadata, errorByCell: errors, pendingByCell: pending };
  }, [metadataSnapshot, queryClient, queryPairs, refreshingCells]);

  const isAnyRefreshInProgress =
    Boolean(isRefreshing) ||
    Boolean(isFetching) ||
    isRefreshingPanelCounts ||
    isResettingCheckpoints ||
    refreshingCells.size > 0;
  const isPanelActionBusy =
    isAnyRefreshInProgress || isResettingAllLists || isResettingLists || isResettingCheckpoints;

  const filteredDomainRows = useMemo(() => {
    return domainRows.filter((row) => Boolean(domainsByLayer.get(row.key)));
  }, [domainRows, domainsByLayer]);

  const layerTriggerGroups = useMemo(() => {
    const groups = new Map<LayerKey, { layerLabel: string; jobNames: string[] }>();

    for (const layerColumn of layerColumns) {
      const jobNames = new Set<string>();
      const layer = layersByKey.get(layerColumn.key);

      for (const domain of layer?.domains || []) {
        const configuredJobName = resolveManagedJobName({
          jobName: domain.jobName,
          jobUrl: domain.jobUrl,
          layerName: layer?.name,
          domainName: domain?.name
        });
        const normalizedJobName = normalizeAzureJobName(configuredJobName);
        if (normalizedJobName) {
          jobNames.add(normalizedJobName);
        }
      }

      groups.set(layerColumn.key, {
        layerLabel: layerColumn.label,
        jobNames: Array.from(jobNames)
      });
    }

    return groups;
  }, [layerColumns, layersByKey]);

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

        const jobName = resolveManagedJobName({
          jobName: domainConfig?.jobName,
          jobUrl: domainConfig?.jobUrl,
          layerName: layerColumn.label,
          domainName: row.key
        });
        const jobKey = normalizeAzureJobName(jobName);
        const run = jobKey ? jobIndex.get(jobKey) : null;
        const runningState = jobKey ? jobStates?.[jobKey] : undefined;
        const hasLiveJobState =
          hasActiveJobRunningState(runningState) || isSuspendedJobRunningState(runningState);
        const jobStatusKey =
          !jobName || (!run && !hasLiveJobState)
            ? 'pending'
            : effectiveJobStatus(run?.status, runningState);

        const isCritical =
          ['error', 'failed', 'critical'].includes(dataStatusKey) ||
          ['error', 'failed'].includes(jobStatusKey);
        const isWarning =
          !isCritical &&
          (['stale', 'warning', 'degraded', 'pending'].includes(dataStatusKey) ||
            ['warning', 'pending'].includes(jobStatusKey));

        if (isCritical) fail += 1;
        else if (isWarning) warn += 1;
        else ok += 1;
      }

      byLayer.set(layerColumn.key, { ok, warn, fail });
    }

    return byLayer;
  }, [domainConfigByLayer, domainsByLayer, filteredDomainRows, jobIndex, jobStates, layerColumns]);

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
        queryClient.setQueryData(queryKeys.domainMetadata(layerKey, domainKey), metadata);
        updateMetadataSnapshot((previous) => {
          const nextEntries = {
            ...(previous?.entries || {}),
            [makeSnapshotKey(layerKey, domainKey)]: metadata
          };
          return {
            version: previous?.version || 1,
            updatedAt: metadata.cachedAt || metadata.computedAt || previous?.updatedAt || null,
            entries: nextEntries,
            warnings: (previous?.warnings || []).filter(Boolean)
          };
        });
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
    [queryClient, refreshingCells, updateMetadataSnapshot]
  );

  const refreshDomainMetadataAndStatus = useCallback(
    async (targets: Array<{ layerKey: LayerKey; domainKey: string }>) => {
      const dedupedTargets = Array.from(
        new Map(
          targets.map((target) => [makeCellKey(target.layerKey, target.domainKey), target] as const)
        ).values()
      );
      if (dedupedTargets.length === 0) return;

      const metadataRefreshPromise = Promise.allSettled(
        dedupedTargets.map((target) => handleCellRefresh(target.layerKey, target.domainKey))
      );
      const statusRefreshPromise = refreshStatus();

      const [, statusResult] = await Promise.allSettled([
        metadataRefreshPromise,
        statusRefreshPromise
      ]);
      if (statusResult.status === 'rejected') {
        console.error('[DomainLayerComparisonPanel] status refresh failed', {
          error: formatSystemStatusText(statusResult.reason)
        });
      }
    },
    [handleCellRefresh, refreshStatus]
  );

  const refreshLayerMetadataAndStatus = useCallback(
    async (layerKey: LayerKey) => {
      if (
        isRefreshingPanelCounts ||
        isResettingAllLists ||
        isResettingLists ||
        isResettingCheckpoints
      )
        return;
      const targets = queryPairsByLayer.get(layerKey) || [];
      await refreshDomainMetadataAndStatus(targets);
    },
    [
      isRefreshingPanelCounts,
      isResettingAllLists,
      isResettingCheckpoints,
      isResettingLists,
      queryPairsByLayer,
      refreshDomainMetadataAndStatus
    ]
  );

  const triggerLayerJobs = useCallback(
    async (layerKey: LayerKey) => {
      const layerGroup = layerTriggerGroups.get(layerKey);
      const layerLabel = layerGroup?.layerLabel || layerKey;
      const jobNames = layerGroup?.jobNames || [];
      if (
        jobNames.length === 0 ||
        isRefreshingPanelCounts ||
        isResettingAllLists ||
        isResettingLists ||
        isResettingCheckpoints ||
        Boolean(jobControl) ||
        Boolean(triggeringJob) ||
        triggeringLayerKeys.has(layerKey)
      ) {
        if (jobNames.length === 0) {
          toast.warning(`No jobs configured for ${layerLabel}`);
        }
        return;
      }

      setTriggeringLayerKeys((previous) => {
        const next = new Set(previous);
        next.add(layerKey);
        return next;
      });

      try {
        for (const jobName of jobNames) {
          await triggerJob(jobName, statusInvalidationKeys);
        }
      } finally {
        setTriggeringLayerKeys((previous) => {
          if (!previous.has(layerKey)) return previous;
          const next = new Set(previous);
          next.delete(layerKey);
          return next;
        });
      }
    },
    [
      isRefreshingPanelCounts,
      isResettingAllLists,
      isResettingCheckpoints,
      isResettingLists,
      jobControl,
      layerTriggerGroups,
      triggerJob,
      triggeringLayerKeys,
      triggeringJob,
      statusInvalidationKeys
    ]
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

      updateMetadataSnapshot((previous) => {
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

        return {
          version: previous?.version || 1,
          updatedAt: new Date().toISOString(),
          entries: nextEntries,
          warnings: (previous?.warnings || []).filter(Boolean)
        };
      });
    },
    [queryClient, updateMetadataSnapshot]
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
      toast.success(`Purged ${getPurgeDeletedCount(result)} blob(s).`);
      await refreshStatus();
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      const detail = operationId ? `operation ${operationId}: ${message}` : message;
      toast.error(`Purge failed (${detail})`);
    } finally {
      setIsPurging(false);
      setActivePurgeTarget(null);
      setPurgeTarget(null);
    }
  }, [purgeTarget, refreshStatus, waitForPurgeResult]);

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
      await refreshStatus();
    } catch (error) {
      toast.error(`List reset failed (${formatSystemStatusText(error) || 'Unknown error'})`);
    } finally {
      setIsResettingLists(false);
      setResettingCellKey(null);
      setListResetTarget(null);
    }
  }, [clearDomainMetadataCache, handleCellRefresh, listResetTarget, refreshStatus]);

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
      await refreshStatus();
    } catch (error) {
      toast.error(`Checkpoint reset failed (${formatSystemStatusText(error) || 'Unknown error'})`);
    } finally {
      setIsResettingCheckpoints(false);
      setResettingCheckpointCellKey(null);
      setCheckpointResetTarget(null);
    }
  }, [checkpointResetTarget, clearDomainMetadataCache, handleCellRefresh, refreshStatus]);

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
      const refreshResults = await Promise.allSettled(
        queryPairs.map((pair) =>
          DataService.getDomainMetadata(pair.layerKey, pair.domainKey, { refresh: true })
        )
      );

      let refreshedCells = 0;
      let failedCells = 0;
      let firstFailureMessage = '';
      const previousSnapshot = metadataSnapshot || null;
      const nextEntries = { ...(previousSnapshot?.entries || {}) };

      refreshResults.forEach((result, index) => {
        const pair = queryPairs[index];
        if (result.status === 'fulfilled') {
          refreshedCells += 1;
          nextEntries[makeSnapshotKey(pair.layerKey, pair.domainKey)] = result.value;
          queryClient.setQueryData(
            queryKeys.domainMetadata(pair.layerKey, pair.domainKey),
            result.value
          );
          return;
        }
        failedCells += 1;
        if (!firstFailureMessage) {
          firstFailureMessage = formatSystemStatusText(result.reason) || 'Unknown error';
        }
      });

      const snapshot: DomainMetadataSnapshotResponse = {
        version: previousSnapshot?.version || 1,
        updatedAt: new Date().toISOString(),
        entries: nextEntries,
        warnings: (previousSnapshot?.warnings || []).filter(Boolean)
      };

      updateMetadataSnapshot(() => snapshot);

      toast.success(`Refreshed counts for ${refreshedCells}/${queryPairs.length} panel cells.`);
      if (failedCells > 0) {
        toast.error(`Failed to refresh ${failedCells} panel cells (first: ${firstFailureMessage})`);
      }
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
    isRefreshingPanelCounts,
    isResettingAllLists,
    isResettingCheckpoints,
    isResettingLists,
    metadataSnapshot,
    queryClient,
    queryPairs,
    updateMetadataSnapshot
  ]);

  const refreshPanelCoverage = useCallback(async () => {
    const statusRefreshPromise = refreshStatus();

    if (queryPairs.length > 0) {
      const [, statusResult] = await Promise.allSettled([
        refreshAllPanelCounts(),
        statusRefreshPromise
      ]);
      if (statusResult.status === 'rejected') {
        console.error('[DomainLayerComparisonPanel] status refresh failed', {
          error: formatSystemStatusText(statusResult.reason)
        });
      }
      return;
    }

    await statusRefreshPromise;
  }, [queryPairs.length, refreshAllPanelCounts, refreshStatus]);

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
        await refreshStatus();
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
    queryPairs,
    refreshStatus
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
        <div className="flex min-w-0 items-center gap-2">
          <GitCompareArrows className="h-5 w-5 shrink-0" />
          <CardTitle className="leading-tight">Domain Layer Coverage</CardTitle>
        </div>
        <div className="text-xs text-mcm-walnut/70">
          {metadataSource === 'persisted-snapshot' ? 'Persisted snapshot' : 'Snapshot'}
          {metadataUpdatedAt
            ? ` updated ${formatMetadataTimestamp(metadataUpdatedAt)}`
            : ' not available'}
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
              <Table className="min-w-[1280px] table-fixed border-collapse border-spacing-y-0">
                <caption className="sr-only">
                  Compact layer-by-layer domain coverage summary with expandable details.
                </caption>
                <TableHeader>
                  <TableRow className="h-14">
                    <TableHead
                      className="sticky left-0 top-0 z-30 border-b border-mcm-walnut/25 bg-mcm-cream/90"
                      style={{ width: DOMAIN_COLUMN_WIDTH_PX, minWidth: DOMAIN_COLUMN_WIDTH_PX }}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span>Domain</span>
                        <div className="flex items-center gap-1">
                          {onRefresh || queryPairs.length > 0 ? (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-7 w-7 shrink-0 rounded-full border border-transparent text-mcm-walnut/70 hover:border-mcm-walnut/20 hover:bg-mcm-paper/55 hover:text-mcm-walnut"
                                  aria-label="Refresh domain layer coverage"
                                  onClick={() => void refreshPanelCoverage()}
                                  disabled={isPanelActionBusy}
                                >
                                  <RefreshCw
                                    className={`h-3.5 w-3.5 ${isAnyRefreshInProgress ? 'animate-spin' : ''}`}
                                  />
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent side="top">
                                {isAnyRefreshInProgress
                                  ? 'Refreshing domain layer coverage'
                                  : 'Refresh domain layer coverage'}
                              </TooltipContent>
                            </Tooltip>
                          ) : null}

                          {queryPairs.length > 0 ? (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-7 w-7 shrink-0 rounded-full border border-transparent text-destructive/80 hover:border-destructive/20 hover:bg-destructive/10 hover:text-destructive"
                                  aria-label="Reset lists for all configured domains"
                                  onClick={() => setIsResetAllDialogOpen(true)}
                                  disabled={isPanelActionBusy}
                                >
                                  {isResettingAllLists ? (
                                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                  ) : (
                                    <Trash2 className="h-3.5 w-3.5" />
                                  )}
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent side="top">
                                {isResettingAllLists
                                  ? 'Resetting all configured lists'
                                  : 'Reset lists for all configured domains'}
                              </TooltipContent>
                            </Tooltip>
                          ) : null}
                        </div>
                      </div>
                    </TableHead>
                    {layerColumns.map((layer) => {
                      const aggregate = layerAggregateStatus.get(layer.key);
                      const layerJobGroup = layerTriggerGroups.get(layer.key);
                      const layerVisual = getLayerVisual(layer.key);
                      const layerTargets = queryPairsByLayer.get(layer.key) || [];
                      const isLayerRefreshing = layerTargets.some((target) =>
                        refreshingCells.has(makeCellKey(target.layerKey, target.domainKey))
                      );
                      const isLayerTriggering = triggeringLayerKeys.has(layer.key);
                      const isLayerBusy =
                        isLayerRefreshing ||
                        isRefreshingPanelCounts ||
                        isResettingAllLists ||
                        isResettingLists ||
                        isResettingCheckpoints;
                      const isLayerTriggerDisabled =
                        !layerJobGroup?.jobNames.length ||
                        isLayerBusy ||
                        Boolean(jobControl) ||
                        Boolean(triggeringJob) ||
                        isLayerTriggering;
                      return (
                        <TableHead
                          key={`compact-head-${layer.key}`}
                          className="sticky top-0 z-20 min-w-[190px] border-b border-mcm-walnut/25"
                          style={{
                            backgroundColor: layerVisual.strongBg,
                            boxShadow: `inset 0 2px 0 ${layerVisual.border}, inset 2px 0 0 ${layerVisual.border}, inset -1px 0 0 ${layerVisual.border}`
                          }}
                        >
                          <div className="flex items-center justify-between gap-2">
                            <div className="flex min-w-0 items-center gap-2">
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
                                  className={`${StatusTypos.MONO} truncate text-[10px] font-semibold uppercase tracking-wider`}
                                  style={{ color: layerVisual.mutedText }}
                                >
                                  ok {aggregate.ok} • warn {aggregate.warn} • fail {aggregate.fail}
                                </span>
                              ) : null}
                            </div>
                            <div className="flex items-center gap-1">
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <Button
                                    type="button"
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7 shrink-0 rounded-full border border-transparent text-mcm-walnut/70 hover:border-mcm-walnut/20 hover:bg-mcm-paper/55 hover:text-mcm-walnut"
                                    aria-label={`Trigger ${layer.label} layer jobs`}
                                    disabled={isLayerTriggerDisabled}
                                    onClick={() => {
                                      void triggerLayerJobs(layer.key);
                                    }}
                                  >
                                    {isLayerTriggering ? (
                                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                    ) : (
                                      <Play className="h-3.5 w-3.5" />
                                    )}
                                  </Button>
                                </TooltipTrigger>
                                <TooltipContent side="top">
                                  {layerJobGroup?.jobNames.length
                                    ? `Trigger ${layerJobGroup.jobNames.length} ${layer.label} job${layerJobGroup.jobNames.length === 1 ? '' : 's'}`
                                    : `No configured jobs for ${layer.label}`}
                                </TooltipContent>
                              </Tooltip>
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <Button
                                    type="button"
                                    variant="ghost"
                                    size="icon"
                                    className="h-7 w-7 shrink-0 rounded-full border border-transparent text-mcm-walnut/70 hover:border-mcm-walnut/20 hover:bg-mcm-paper/55 hover:text-mcm-walnut"
                                    aria-label={`Refresh ${layer.label} layer`}
                                    disabled={layerTargets.length === 0 || isLayerBusy}
                                    onClick={() => {
                                      void refreshLayerMetadataAndStatus(layer.key);
                                    }}
                                  >
                                    <RefreshCw
                                      className={`h-3.5 w-3.5 ${isLayerRefreshing ? 'animate-spin' : ''}`}
                                    />
                                  </Button>
                                </TooltipTrigger>
                                <TooltipContent side="top">
                                  {isLayerRefreshing
                                    ? `Refreshing ${layer.label} domains`
                                    : `Refresh all ${layer.label} domains`}
                                </TooltipContent>
                              </Tooltip>
                            </div>
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
                      const jobName = resolveManagedJobName({
                        jobName: domainConfig?.jobName,
                        jobUrl: domainConfig?.jobUrl,
                        layerName: layerColumn.label,
                        domainName: row.key
                      });
                      const jobKey = normalizeAzureJobName(jobName);
                      const run = jobKey ? jobIndex.get(jobKey) : null;
                      const durationSummary = jobKey ? jobDurationSummaryIndex.get(jobKey) : null;
                      const managedJob = jobKey ? managedJobIndex.get(jobKey) : null;
                      const liveUsageDisplay = buildRunningUsageDisplay(managedJob?.signals);
                      const lastStartDisplay = (() => {
                        if (!jobName) return 'N/A';
                        if (!run?.startTime) return 'NO RUN';
                        return formatTimeAgo(run.startTime);
                      })();
                      const averageRuntimeSummary = durationSummary
                        ? `avg runtime ${formatDuration(durationSummary.averageDurationSeconds)}`
                        : null;
                      const averageRuntimeDetail = durationSummary
                        ? `${formatDuration(durationSummary.averageDurationSeconds)} (${durationSummary.sampleCount} run${durationSummary.sampleCount === 1 ? '' : 's'})`
                        : 'N/A';
                      const averageRuntimeTitle = durationSummary
                        ? `Average from ${durationSummary.sampleCount} recent execution${durationSummary.sampleCount === 1 ? '' : 's'}`
                        : undefined;
                      const jobUpdatedAt = managedJob?.lastModifiedAt || null;
                      const jobUpdatedDisplay = jobUpdatedAt ? formatTimeAgo(jobUpdatedAt) : 'N/A';
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

                      const runningState = jobKey ? jobStates?.[jobKey] : undefined;
                      const hasLiveJobState =
                        hasActiveJobRunningState(runningState) ||
                        isSuspendedJobRunningState(runningState);
                      const jobStatusKey =
                        !jobName || (!run && !hasLiveJobState)
                          ? 'pending'
                          : effectiveJobStatus(run?.status, runningState);
                      const jobConfig = getStatusConfig(jobStatusKey);
                      const jobLabel = !jobName
                        ? 'N/A'
                        : !run && !hasLiveJobState
                          ? 'NO RUN'
                          : toJobStatusLabel(jobStatusKey);
                      const jobStatusCode =
                        String(run?.statusCode || run?.status || '').trim() || null;

                      const actionJobName = String(run?.jobName || jobName).trim();
                      const isSuspended = isSuspendedJobRunningState(runningState);
                      const isRunning = effectiveJobStatus(run?.status, runningState) === 'running';
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
                      const metadataUpdatedAt = metadata?.computedAt || null;
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
                      const retrySymbolsSummary = summarizeRetrySymbols(run);
                      const columnCount = resolveColumnCount(metadata);
                      const storageBytes = metadata?.totalBytes;
                      const dateRangeDisplay = formatMetadataDateRange(metadata);
                      const dateRangeTooltip = describeMetadataDateRange(metadata);
                      const columnStorageSummary =
                        [
                          columnCount !== null ? formatColumnCount(columnCount) : null,
                          hasFiniteNumber(storageBytes) ? formatStorageBytes(storageBytes) : null
                        ]
                          .filter((value): value is string => Boolean(value))
                          .join(' • ') || null;
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
                        jobStatusCode,
                        lastStartDisplay,
                        averageRuntimeSummary,
                        averageRuntimeDetail,
                        averageRuntimeTitle,
                        jobUpdatedAt,
                        jobUpdatedDisplay,
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
                        metadataUpdatedAt,
                        adlsModifiedAt,
                        adlsModifiedDisplay,
                        isPurgingThisTarget,
                        symbolComparison,
                        columnCount,
                        columnStorageSummary,
                        dateRangeDisplay,
                        dateRangeTooltip,
                        previousLabel,
                        blacklistSummary,
                        retrySymbolsSummary,
                        liveUsageDisplay,
                        financeSubfolderCounts,
                        showFinanceSubfolders,
                        layerVisual,
                        supportsCheckpointReset
                      };
                    });

                    const configuredModels = layerModels.filter((model) => model.isConfigured);
                    const isDomainRefreshing = configuredModels.some(
                      (model) => model.isCellRefreshing
                    );
                    const detailRegionIds = new Map(
                      configuredModels.map((model) => [
                        model.key,
                        `domain-layer-details-${row.key}-${model.layerColumn.key}`
                      ])
                    );
                    const detailRegionControls = Array.from(detailRegionIds.values()).join(' ');
                    const toggleRowExpanded = () =>
                      setExpandedRowKey((previous) => (previous === row.key ? null : row.key));

                    return (
                      <TableRow
                        key={`summary-${row.key}`}
                        className="cursor-pointer even:[&>td]:bg-mcm-cream/20"
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
                        <TableCell
                          className="sticky left-0 z-10 border-b border-mcm-walnut/20 bg-mcm-paper/95 py-1.5 align-top"
                          style={{
                            width: DOMAIN_COLUMN_WIDTH_PX,
                            minWidth: DOMAIN_COLUMN_WIDTH_PX
                          }}
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0 flex flex-col gap-0.5">
                              <span className="truncate font-semibold text-mcm-walnut">
                                {row.label}
                              </span>
                              <span
                                className={`${StatusTypos.MONO} truncate text-[11px] text-mcm-walnut/75`}
                              >
                                {row.key}
                              </span>
                              {isDomainRefreshing ? (
                                <span
                                  data-testid={`domain-refresh-indicator-${row.key}`}
                                  className={`${StatusTypos.MONO} inline-flex w-fit items-center gap-1 rounded-full border border-mcm-teal/35 bg-mcm-teal/10 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-mcm-teal`}
                                >
                                  <RefreshCw className="h-3 w-3 animate-spin" />
                                  refreshing
                                </span>
                              ) : null}
                            </div>
                            <Button
                              type="button"
                              variant="ghost"
                              size="icon"
                              className="h-8 w-8 shrink-0 rounded-full border border-transparent text-mcm-walnut/70 hover:border-mcm-walnut/20 hover:bg-mcm-paper/55 hover:text-mcm-walnut"
                              aria-label={`${isExpanded ? 'Collapse' : 'Expand'} ${row.label} details`}
                              aria-expanded={isExpanded}
                              aria-controls={detailRegionControls || undefined}
                              data-no-row-toggle="true"
                              onClick={() => toggleRowExpanded()}
                            >
                              <ChevronDown
                                className={`h-4 w-4 transition-transform motion-reduce:transition-none ${isExpanded ? 'rotate-180' : ''}`}
                              />
                            </Button>
                          </div>
                        </TableCell>

                        {layerModels.map((model) => {
                          if (!model.isConfigured) {
                            return (
                              <TableCell
                                key={`summary-${row.key}-${model.layerColumn.key}`}
                                className={`${StatusTypos.MONO} border-b border-mcm-walnut/20 py-1.5 text-center text-[12px] text-mcm-walnut/65 align-top whitespace-normal`}
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
                          const detailRegionId = detailRegionIds.get(model.key);
                          const jobControlAction =
                            model.actionJobName && jobControl?.jobName === model.actionJobName
                              ? jobControl.action
                              : null;
                          return (
                            <TableCell
                              key={`summary-${row.key}-${model.layerColumn.key}`}
                              className="border-b border-mcm-walnut/20 py-1.5 align-top whitespace-normal"
                              style={{
                                backgroundColor: model.layerVisual.softBg,
                                boxShadow: `inset 3px 0 0 ${model.layerVisual.border}, inset -1px 0 0 ${model.layerVisual.border}`
                              }}
                            >
                              <div className="flex h-full flex-col">
                                <div className="flex items-start justify-between gap-2">
                                  <div className="min-w-0">
                                    <span
                                      className={`${StatusTypos.MONO} tabular-nums text-sm font-bold text-mcm-walnut`}
                                    >
                                      {formatSymbolCount(model.metadata?.symbolCount)}
                                    </span>
                                    {model.columnStorageSummary ? (
                                      <div
                                        className={`${StatusTypos.MONO} mt-0.5 text-[10px] text-mcm-walnut/70`}
                                      >
                                        {model.columnStorageSummary}
                                      </div>
                                    ) : null}
                                    {model.dateRangeDisplay ? (
                                      <div
                                        className={`${StatusTypos.MONO} mt-0.5 text-[10px] text-mcm-walnut/70`}
                                        title={model.dateRangeTooltip || undefined}
                                      >
                                        range {model.dateRangeDisplay}
                                      </div>
                                    ) : null}
                                    {model.metadataUpdatedAt ? (
                                      <div
                                        className={`${StatusTypos.MONO} mt-0.5 text-[10px] text-mcm-walnut/65`}
                                        title={model.metadataUpdatedAt}
                                      >
                                        updated {formatMetadataTimestamp(model.metadataUpdatedAt)}
                                      </div>
                                    ) : null}
                                    {model.averageRuntimeSummary ? (
                                      <div
                                        className={`${StatusTypos.MONO} mt-0.5 text-[10px] text-mcm-walnut/65`}
                                        title={model.averageRuntimeTitle}
                                      >
                                        {model.averageRuntimeSummary}
                                      </div>
                                    ) : null}
                                    {model.isRunning && model.liveUsageDisplay?.compactText ? (
                                      <div
                                        className={`${StatusTypos.MONO} mt-0.5 text-[10px] text-mcm-walnut/70`}
                                        title="Live job resource usage"
                                      >
                                        {model.liveUsageDisplay.compactText}
                                      </div>
                                    ) : null}
                                  </div>
                                  <div className="flex items-center gap-2 self-start">
                                    {model.isCellRefreshing ? (
                                      <span
                                        data-testid={`cell-refresh-icon-summary-${row.key}-${model.layerColumn.key}`}
                                        aria-label={`${model.layerColumn.label} ${row.label} refreshing`}
                                        title="Refreshing metadata and job state"
                                        className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full border border-mcm-teal/35 bg-mcm-teal/10 text-mcm-teal shadow-[0_0_0_1px_rgba(34,138,126,0.08)]"
                                      >
                                        <RefreshCw className="h-3.5 w-3.5 animate-spin" />
                                      </span>
                                    ) : null}
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
                                        title={model.jobStatusCode || undefined}
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
                                </div>
                                {model.isPending ? (
                                  <div
                                    className={`${StatusTypos.MONO} mt-1 text-[11px] text-mcm-walnut/70`}
                                  >
                                    loading metadata...
                                  </div>
                                ) : null}
                                {model.error ? (
                                  <div
                                    className={`${StatusTypos.MONO} mt-1 text-[11px] text-destructive/90`}
                                  >
                                    metadata warning
                                  </div>
                                ) : null}
                                {isExpanded ? (
                                  <div
                                    id={detailRegionId}
                                    data-no-row-toggle="true"
                                    className="mt-2 flex flex-1 flex-col gap-2 border-t border-mcm-walnut/15 pt-2"
                                  >
                                    {model.symbolComparison ? (
                                      <div
                                        className={`${StatusTypos.MONO} flex flex-wrap items-center gap-x-1.5 gap-y-0.5 text-[10.5px]`}
                                      >
                                        <span className="text-mcm-walnut/70">
                                          vs {model.previousLabel}:
                                        </span>
                                        <span className={model.symbolComparison.className}>
                                          {model.symbolComparison.text}
                                        </span>
                                        <span className="text-mcm-walnut/50">•</span>
                                        <span className={model.blacklistSummary.className}>
                                          {model.blacklistSummary.text}
                                        </span>
                                      </div>
                                    ) : (
                                      <div
                                        className={`${StatusTypos.MONO} flex flex-wrap items-center gap-x-1.5 gap-y-0.5 text-[10.5px]`}
                                      >
                                        <span className={model.blacklistSummary.className}>
                                          {model.blacklistSummary.text}
                                        </span>
                                      </div>
                                    )}
                                    <dl
                                      className={`${StatusTypos.MONO} grid grid-cols-[max-content_minmax(0,1fr)] gap-x-2 gap-y-1 text-[10.5px]`}
                                    >
                                      <dt className="text-mcm-walnut/70">last start:</dt>
                                      <dd
                                        className="min-w-0 truncate text-right text-mcm-walnut/90"
                                        title={model.run?.startTime || undefined}
                                      >
                                        {model.lastStartDisplay}
                                      </dd>
                                      <dt className="text-mcm-walnut/70">avg runtime:</dt>
                                      <dd
                                        className="min-w-0 truncate text-right text-mcm-walnut/90"
                                        title={model.averageRuntimeTitle}
                                      >
                                        {model.averageRuntimeDetail}
                                      </dd>
                                      {model.isRunning ? (
                                        <>
                                          <dt className="text-mcm-walnut/70">cpu usage:</dt>
                                          <dd className="min-w-0 truncate text-right text-mcm-walnut/90">
                                            {model.liveUsageDisplay?.cpuDisplay || 'N/A'}
                                          </dd>
                                          <dt className="text-mcm-walnut/70">memory usage:</dt>
                                          <dd className="min-w-0 truncate text-right text-mcm-walnut/90">
                                            {model.liveUsageDisplay?.memoryDisplay || 'N/A'}
                                          </dd>
                                        </>
                                      ) : null}
                                      {model.retrySymbolsSummary ? (
                                        <>
                                          <dt className="self-start text-mcm-walnut/70">
                                            symbols to retry:
                                          </dt>
                                          <dd
                                            className="min-w-0 break-words text-right text-mcm-walnut/90"
                                            title={model.retrySymbolsSummary.title}
                                          >
                                            {model.retrySymbolsSummary.text}
                                          </dd>
                                        </>
                                      ) : null}
                                      <dt className="text-mcm-walnut/70">job updated:</dt>
                                      <dd
                                        className="min-w-0 truncate text-right text-mcm-walnut/90"
                                        title={model.jobUpdatedAt || undefined}
                                      >
                                        {model.jobUpdatedDisplay}
                                      </dd>
                                      <dt className="text-mcm-walnut/70">schedule:</dt>
                                      <dd
                                        className="min-w-0 truncate text-right text-mcm-walnut/90"
                                        title={model.scheduleRaw || undefined}
                                      >
                                        {model.scheduleDisplay}
                                      </dd>
                                      <dt className="self-start text-mcm-walnut/70">date range:</dt>
                                      <dd
                                        className="min-w-0 break-words text-right text-mcm-walnut/90"
                                        title={model.dateRangeTooltip || undefined}
                                      >
                                        {model.dateRangeDisplay || 'N/A'}
                                      </dd>
                                      <dt className="text-mcm-walnut/70">adls modified:</dt>
                                      <dd
                                        className="min-w-0 truncate text-right text-mcm-walnut/90"
                                        title={model.adlsModifiedAt || undefined}
                                      >
                                        {model.adlsModifiedDisplay}
                                      </dd>
                                    </dl>
                                    {model.showFinanceSubfolders ? (
                                      <div className="grid grid-cols-2 gap-x-2 gap-y-1 rounded-md border border-mcm-walnut/12 bg-mcm-paper/25 px-2 py-1.5">
                                        {model.financeSubfolderCounts.map((item) => (
                                          <div
                                            key={`finance-detail-${row.key}-${model.layerColumn.key}-${item.key}`}
                                            className="flex items-center justify-between"
                                          >
                                            <span className="text-mcm-walnut/80">{item.label}</span>
                                            <span
                                              className={`${StatusTypos.MONO} tabular-nums text-mcm-walnut/95`}
                                            >
                                              {formatInt(item.count)}
                                            </span>
                                          </div>
                                        ))}
                                      </div>
                                    ) : null}
                                    <div className="mt-auto flex items-center justify-end gap-1 border-t border-mcm-walnut/15 pt-2">
                                      <Button
                                        type="button"
                                        size="sm"
                                        variant="outline"
                                        className="h-7 px-2 text-[11px]"
                                        disabled={
                                          !model.actionJobName ||
                                          model.isJobControlBlocked ||
                                          model.isRunning
                                        }
                                        onClick={() => {
                                          if (!model.actionJobName) return;
                                          void triggerJob(
                                            model.actionJobName,
                                            statusInvalidationKeys
                                          );
                                        }}
                                      >
                                        {model.isTriggeringThisJob ? (
                                          <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                                        ) : (
                                          <Play className="mr-1 h-3.5 w-3.5" />
                                        )}
                                        Run
                                      </Button>
                                      <Button
                                        type="button"
                                        size="sm"
                                        variant="outline"
                                        className="h-7 px-2 text-[11px] text-destructive hover:bg-destructive/10 hover:text-destructive"
                                        aria-label={`Stop all running ${model.actionJobName || model.layerColumn.label} runs`}
                                        disabled={
                                          !model.actionJobName ||
                                          model.isJobControlBlocked ||
                                          !model.isRunning
                                        }
                                        onClick={() => {
                                          if (!model.actionJobName) return;
                                          void stopJob(model.actionJobName, statusInvalidationKeys);
                                        }}
                                      >
                                        {jobControlAction === 'stop' ? (
                                          <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                                        ) : (
                                          <Square className="mr-1 h-3.5 w-3.5" />
                                        )}
                                        Stop all runs
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
                                            <span>
                                              {model.layerColumn.label} • {row.label}
                                            </span>
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
                                              void refreshDomainMetadataAndStatus([
                                                {
                                                  layerKey: model.layerColumn.key,
                                                  domainKey: row.key
                                                }
                                              ]);
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
                                                !model.isSuspended,
                                                statusInvalidationKeys
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
                                            className="text-destructive focus:text-destructive"
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
                                ) : null}
                              </div>
                            </TableCell>
                          );
                        })}
                      </TableRow>
                    );
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
