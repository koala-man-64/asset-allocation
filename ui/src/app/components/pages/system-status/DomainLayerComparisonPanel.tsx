import { useCallback, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, GitCompareArrows, Info, Loader2, RefreshCw, Trash2 } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Button } from '@/app/components/ui/button';
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

const LAYER_ORDER = ['bronze', 'silver', 'gold', 'platinum'] as const;
type LayerKey = (typeof LAYER_ORDER)[number];
const MATRIX_HEAD_CLASS =
  'min-w-[190px] border-b border-mcm-walnut/25 bg-mcm-cream/55';
const MATRIX_BODY_CELL_CLASS =
  'align-top border-0 border-b border-mcm-walnut/20 first:rounded-none last:rounded-none first:border-l-0 last:border-r-0';
const DOMAIN_METADATA_SNAPSHOT_STORAGE_KEY = 'asset-allocation.domain-metadata-snapshot.v1';

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

function toLayerKey(value: string): LayerKey | null {
  const normalized = normalizeLayerKey(value);
  if (!LAYER_ORDER.includes(normalized as LayerKey)) return null;
  return normalized as LayerKey;
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

function normalizeDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    const raw = String(value).trim();
    return raw ? raw.slice(0, 10) : null;
  }
  return parsed.toISOString().slice(0, 10);
}

function formatDateRange(metadata: DomainMetadata | undefined): string {
  if (!metadata?.dateRange) return 'N/A';
  const min = normalizeDate(metadata.dateRange.min);
  const max = normalizeDate(metadata.dateRange.max);
  if (!min && !max) return 'N/A';
  return `${min || 'N/A'} -> ${max || 'N/A'}`;
}

function dateRangeUnavailableReason(metadata: DomainMetadata | undefined): string | null {
  if (!metadata) {
    return null;
  }

  if (metadata.dateRange && (normalizeDate(metadata.dateRange.min) || normalizeDate(metadata.dateRange.max))) {
    return null;
  }

  if (metadata.type === 'blob') {
    return 'Date range is unavailable for blob-backed domains.';
  }

  if (metadata.type === 'delta') {
    if ((metadata.warnings || []).some((warning) => warning.toLowerCase().includes('date range'))) {
      return 'Date range is unavailable or could not be parsed for this delta domain.';
    }
    return 'Date range was not detected for this delta domain.';
  }

  return 'Date range is not available for this metadata source.';
}

function compareSymbols(current: DomainMetadata, previous: DomainMetadata): {
  text: string;
  className: string;
} {
  if (!hasFiniteNumber(current.symbolCount) || !hasFiniteNumber(previous.symbolCount)) {
    return { text: 'symbols n/a', className: 'text-mcm-walnut/50' };
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

function compareDateRanges(current: DomainMetadata, previous: DomainMetadata): {
  text: string;
  className: string;
} {
  const currentMin = normalizeDate(current.dateRange?.min);
  const currentMax = normalizeDate(current.dateRange?.max);
  const previousMin = normalizeDate(previous.dateRange?.min);
  const previousMax = normalizeDate(previous.dateRange?.max);

  if (!currentMin || !currentMax || !previousMin || !previousMax) {
    return { text: 'range n/a', className: 'text-mcm-walnut/50' };
  }

  const isSameRange = currentMin === previousMin && currentMax === previousMax;
  return {
    text: isSameRange ? 'range match' : 'range shifted',
    className: isSameRange ? 'text-mcm-teal' : 'text-mcm-mustard'
  };
}

export function DomainLayerComparisonPanel({ dataLayers }: { dataLayers: DataLayer[] }) {
  const queryClient = useQueryClient();
  const [refreshingCells, setRefreshingCells] = useState<Set<string>>(new Set());
  const [isRefreshingPanelCounts, setIsRefreshingPanelCounts] = useState(false);
  const [listResetTarget, setListResetTarget] = useState<{
    layerKey: LayerKey;
    layerLabel: string;
    domainKey: string;
    domainLabel: string;
  } | null>(null);
  const [isResetAllDialogOpen, setIsResetAllDialogOpen] = useState(false);
  const [isResettingLists, setIsResettingLists] = useState(false);
  const [isResettingAllLists, setIsResettingAllLists] = useState(false);
  const [resettingCellKey, setResettingCellKey] = useState<string | null>(null);

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

  const { domainsByLayer, domainRows } = useMemo(() => {
    const matrix = new Map<string, Map<LayerKey, true>>();

    for (const layerColumn of layerColumns) {
      const domains = layersByKey.get(layerColumn.key)?.domains || [];
      for (const domain of domains) {
        const domainName = String(domain?.name || '').trim();
        if (!domainName) continue;
        const domainKey = normalizeDomainKey(domainName);
        if (!domainKey) continue;

        const row = matrix.get(domainKey) || new Map<LayerKey, true>();
        row.set(layerColumn.key, true);
        matrix.set(domainKey, row);
      }
    }

    const rows = getDomainOrderEntries(dataLayers).filter((entry) => {
      return matrix.has(entry.key);
    });

    return { domainsByLayer: matrix, domainRows: rows };
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
        const metadata = await DataService.getDomainMetadata(layerKey, domainKey, { refresh: true });
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
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (error) {
      toast.error(`List reset failed (${formatSystemStatusText(error) || 'Unknown error'})`);
    } finally {
      setIsResettingLists(false);
      setResettingCellKey(null);
      setListResetTarget(null);
    }
  }, [listResetTarget, queryClient]);

  const refreshAllPanelCounts = useCallback(async () => {
    if (queryPairs.length === 0 || isRefreshingPanelCounts || isResettingAllLists || isResettingLists) {
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
    isResettingLists,
    layerColumns,
    queryClient,
    queryPairs,
    snapshotQueryKey
  ]);

  const confirmResetAllPanelLists = useCallback(async () => {
    if (queryPairs.length === 0 || isRefreshingPanelCounts || isResettingAllLists || isResettingLists) {
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
      for (const result of resetResults) {
        if (result.status === 'fulfilled') {
          successfulResets += 1;
          totalFilesReset += result.value.resetCount;
          continue;
        }
        failedResets += 1;
        if (!firstFailureMessage) {
          firstFailureMessage = formatSystemStatusText(result.reason) || 'Unknown error';
        }
      }

      if (successfulResets > 0) {
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
  }, [isRefreshingPanelCounts, isResettingAllLists, isResettingLists, queryClient, queryPairs]);

  return (
    <Card className="h-full">
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
        <div className="flex items-start justify-between gap-3">
          <div className="flex min-w-0 items-start gap-2">
            <GitCompareArrows className="mt-0.5 h-5 w-5 shrink-0" />
            <div className="flex min-w-0 flex-wrap items-baseline gap-x-3 gap-y-1">
              <CardTitle className="leading-tight">Domain Layer Coverage</CardTitle>
              <p className="text-sm leading-relaxed text-muted-foreground">
                Compare symbol counts and date windows layer-to-layer for each domain.
              </p>
            </div>
          </div>
          {queryPairs.length > 0 ? (
            <div className="inline-flex items-center gap-1">
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="h-8 w-8 shrink-0 rounded-full text-mcm-walnut/65 hover:text-mcm-walnut"
                    onClick={() => void refreshAllPanelCounts()}
                    disabled={isRefreshingPanelCounts || isResettingAllLists || isResettingLists}
                    aria-label="Refresh counts for the entire panel"
                  >
                    {isRefreshingPanelCounts ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <RefreshCw className="h-4 w-4" />
                    )}
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="top">
                  {isRefreshingPanelCounts
                    ? 'Refreshing counts for the entire panel...'
                    : 'Refresh counts for the entire panel'}
                </TooltipContent>
              </Tooltip>

              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="h-8 w-8 shrink-0 rounded-full text-rose-700/70 hover:bg-rose-500/10 hover:text-rose-800"
                    onClick={() => setIsResetAllDialogOpen(true)}
                    disabled={isRefreshingPanelCounts || isResettingAllLists || isResettingLists}
                    aria-label="Reset lists for the entire panel"
                  >
                    {isResettingAllLists ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Trash2 className="h-4 w-4" />
                    )}
                  </Button>
                </TooltipTrigger>
                <TooltipContent side="top">
                  {isResettingAllLists
                    ? 'Resetting lists for the entire panel...'
                    : 'Reset lists for the entire panel'}
                </TooltipContent>
              </Tooltip>
            </div>
          ) : null}
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
        ) : (
          <div className="rounded-[1.2rem] border border-mcm-walnut/20 bg-mcm-cream/30 overflow-hidden">
            <div className="overflow-x-auto">
              <Table className="min-w-[960px] border-collapse border-spacing-y-0">
                <caption className="sr-only">
                  Layer-by-layer domain comparison of symbol count and date ranges.
                </caption>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[180px] border-b border-mcm-walnut/25 bg-mcm-cream/55">
                      Domain
                    </TableHead>
                    {layerColumns.map((layer) => (
                      <TableHead key={layer.key} className={MATRIX_HEAD_CLASS}>
                        <div className="flex flex-col gap-0.5">
                          <span>{layer.label}</span>
                          <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/55`}>
                            symbols + date range
                          </span>
                        </div>
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {domainRows.map((row) => {
                    const domainsForRow = domainsByLayer.get(row.key);
                    return (
                      <TableRow key={row.key} className="even:[&>td]:bg-mcm-cream/20">
                        <TableCell className={MATRIX_BODY_CELL_CLASS}>
                          <div className="flex flex-col gap-0.5">
                            <span className="font-semibold text-mcm-walnut">{row.label}</span>
                            <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/55`}>
                              {row.key}
                            </span>
                          </div>
                        </TableCell>
                        {layerColumns.map((layerColumn, layerIndex) => {
                          const isConfigured = Boolean(domainsForRow?.has(layerColumn.key));
                          if (!isConfigured) {
                            return (
                              <TableCell
                                key={`${row.key}-${layerColumn.key}`}
                                className={`${StatusTypos.MONO} ${MATRIX_BODY_CELL_CLASS} text-[11px] text-mcm-walnut/45`}
                              >
                                Not configured
                              </TableCell>
                            );
                          }

                          const key = makeCellKey(layerColumn.key, row.key);
                          const metadata = metadataByCell.get(key);
                          const error = errorByCell.get(key);
                          const isPending = pendingByCell.has(key);
                          const isCellRefreshing = refreshingCells.has(key);
                          const isCellBusy = isCellRefreshing || isPending;
                          const isResettingThisCell = resettingCellKey === key && isResettingLists;
                          const refreshButton = (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-6 w-6 shrink-0 rounded-full text-mcm-walnut/60 hover:text-mcm-walnut"
                                  onClick={() => void handleCellRefresh(layerColumn.key, row.key)}
                                  disabled={
                                    isCellBusy || isRefreshingPanelCounts || isResettingAllLists
                                  }
                                  aria-label={`Refresh ${layerColumn.label} ${row.label} lineage`}
                                >
                                  {isCellBusy ? (
                                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                  ) : (
                                    <RefreshCw className="h-3.5 w-3.5" />
                                  )}
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent side="top">
                                Refresh {layerColumn.label} • {row.label}
                              </TooltipContent>
                            </Tooltip>
                          );
                          const resetButton = (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  type="button"
                                  variant="ghost"
                                  size="icon"
                                  className="h-6 w-6 shrink-0 rounded-full text-rose-700/70 hover:bg-rose-500/10 hover:text-rose-800"
                                  onClick={() =>
                                    setListResetTarget({
                                      layerKey: layerColumn.key,
                                      layerLabel: layerColumn.label,
                                      domainKey: row.key,
                                      domainLabel: row.label
                                    })
                                  }
                                  disabled={
                                    isCellBusy ||
                                    isResettingLists ||
                                    isRefreshingPanelCounts ||
                                    isResettingAllLists
                                  }
                                  aria-label={`Reset ${layerColumn.label} ${row.label} whitelist and blacklist`}
                                >
                                  {isResettingThisCell ? (
                                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                  ) : (
                                    <Trash2 className="h-3.5 w-3.5" />
                                  )}
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent side="top">
                                {isResettingThisCell
                                  ? `Resetting ${layerColumn.label} • ${row.label}`
                                  : `Reset lists ${layerColumn.label} • ${row.label}`}
                              </TooltipContent>
                            </Tooltip>
                          );
                          const cellActions = (
                            <div className="inline-flex items-center gap-1">
                              {refreshButton}
                              {resetButton}
                            </div>
                          );

                          if (!metadata && isPending) {
                            return (
                              <TableCell
                                key={`${row.key}-${layerColumn.key}`}
                                className={`${StatusTypos.MONO} ${MATRIX_BODY_CELL_CLASS} text-[11px] text-mcm-walnut/60`}
                              >
                                <div className="flex items-start justify-between gap-2">
                                  <span className="inline-flex items-center gap-1.5">
                                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                    Loading metadata
                                  </span>
                                  {cellActions}
                                </div>
                              </TableCell>
                            );
                          }

                          if (!metadata && error) {
                            return (
                              <TableCell
                                key={`${row.key}-${layerColumn.key}`}
                                className={MATRIX_BODY_CELL_CLASS}
                              >
                                <div className="space-y-1.5">
                                  <div className="flex items-start justify-between gap-2">
                                    <div className={`${StatusTypos.MONO} text-[11px] text-destructive`}>
                                      Metadata unavailable
                                    </div>
                                    {cellActions}
                                  </div>
                                  <div
                                    className={`${StatusTypos.MONO} text-[10px] text-destructive/80 break-words`}
                                  >
                                    {error}
                                  </div>
                                </div>
                              </TableCell>
                            );
                          }

                          if (!metadata) {
                            return (
                              <TableCell
                                key={`${row.key}-${layerColumn.key}`}
                                className={`${StatusTypos.MONO} ${MATRIX_BODY_CELL_CLASS} text-[11px] text-mcm-walnut/55`}
                              >
                                <div className="flex items-start justify-between gap-2">
                                  <span>Awaiting metadata</span>
                                  {cellActions}
                                </div>
                              </TableCell>
                            );
                          }

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

                          const symbolComparison = previousMetadata
                            ? compareSymbols(metadata, previousMetadata)
                            : null;
                          const rangeComparison = previousMetadata
                            ? compareDateRanges(metadata, previousMetadata)
                            : null;
                          const dateRangeReason = dateRangeUnavailableReason(metadata);
                          const financeSubfolderCounts =
                            row.key === 'finance'
                              ? FINANCE_SUBFOLDER_ITEMS.map((item) => ({
                                  ...item,
                                  count: metadata.financeSubfolderSymbolCounts?.[item.key]
                                }))
                              : [];
                          const showFinanceSubfolders =
                            financeSubfolderCounts.length > 0 &&
                            financeSubfolderCounts.some((item) => hasFiniteNumber(item.count));

                          return (
                            <TableCell
                              key={`${row.key}-${layerColumn.key}`}
                              className={MATRIX_BODY_CELL_CLASS}
                            >
                              <div className="space-y-1.5">
                                <div className="flex items-start justify-between gap-2">
                                  <div className={`${StatusTypos.MONO} text-base font-black text-mcm-walnut`}>
                                    {formatInt(metadata.symbolCount)}
                                    <span className="ml-1 text-[10px] uppercase tracking-widest text-mcm-walnut/55">
                                      symbols
                                    </span>
                                  </div>
                                  {cellActions}
                                </div>
                                {showFinanceSubfolders ? (
                                  <div className="rounded-md border border-mcm-walnut/15 bg-mcm-cream/30 p-2">
                                    <div className={`${StatusTypos.MONO} text-[9px] uppercase tracking-[0.16em] text-mcm-walnut/55`}>
                                      finance subfolders
                                    </div>
                                    <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-1">
                                      {financeSubfolderCounts.map((item) => (
                                        <div
                                          key={`${row.key}-${layerColumn.key}-${item.key}`}
                                          className="flex items-center justify-between gap-2 text-[10px]"
                                        >
                                          <span className="text-mcm-walnut/65">{item.label}</span>
                                          <span className={`${StatusTypos.MONO} text-mcm-walnut/85`}>
                                            {formatInt(item.count)}
                                          </span>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                ) : null}
                                <div className={`${StatusTypos.MONO} text-[11px] font-semibold text-mcm-walnut/80`}>
                                  {dateRangeReason ? (
                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <span className="inline-flex items-center gap-1">
                                          {formatDateRange(metadata)}
                                          <Info className="h-3 w-3 opacity-60" />
                                        </span>
                                      </TooltipTrigger>
                                      <TooltipContent side="top" className="max-w-xs">
                                        {dateRangeReason}
                                      </TooltipContent>
                                    </Tooltip>
                                  ) : (
                                    formatDateRange(metadata)
                                  )}
                                </div>
                                {dateRangeReason ? (
                                  <div className="text-[10px] text-mcm-walnut/55">{dateRangeReason}</div>
                                ) : null}
                                {metadata.dateRange?.source ? (
                                  <div className="text-[10px] text-mcm-walnut/50">
                                    date range source: <span className={StatusTypos.MONO}>{metadata.dateRange.source}</span>
                                  </div>
                                ) : null}
                                {symbolComparison && rangeComparison ? (
                                  <div className={`${StatusTypos.MONO} text-[10px]`}>
                                    <span className="text-mcm-walnut/50">vs {previousLabel}: </span>
                                    <span className={symbolComparison.className}>{symbolComparison.text}</span>
                                    <span className="text-mcm-walnut/40">{' | '}</span>
                                    <span className={rangeComparison.className}>{rangeComparison.text}</span>
                                  </div>
                                ) : (
                                  <div className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/45`}>
                                    Baseline layer
                                  </div>
                                )}
                                {isPending ? (
                                  <div className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/50`}>
                                    refreshing...
                                  </div>
                                ) : null}
                                {error ? (
                                  <div className={`${StatusTypos.MONO} text-[10px] text-destructive/80`}>
                                    metadata warning
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
