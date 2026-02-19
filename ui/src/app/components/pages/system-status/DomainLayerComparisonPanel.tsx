import { useMemo, useState } from 'react';
import { useQueries, useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, GitCompareArrows, Info, Layers, Loader2, RefreshCw } from 'lucide-react';
import { Badge } from '@/app/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
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
import type { DataLayer, DomainMetadata } from '@/types/strategy';
import { StatusTypos } from './StatusTokens';
import { normalizeDomainKey, normalizeLayerKey } from './SystemPurgeControls';
import { getDomainOrderEntries } from './domainOrdering';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { formatSystemStatusText } from './systemStatusText';

const LAYER_ORDER = ['bronze', 'silver', 'gold', 'platinum'] as const;
type LayerKey = (typeof LAYER_ORDER)[number];

type LayerColumn = {
  key: LayerKey;
  label: string;
};

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

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
  const [isManualRefreshRunning, setIsManualRefreshRunning] = useState(false);

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

  const metadataQueries = useQueries({
    queries: queryPairs.map((pair) => ({
      queryKey: queryKeys.domainMetadata(pair.layerKey, pair.domainKey),
      queryFn: () => DataService.getDomainMetadata(pair.layerKey, pair.domainKey),
      staleTime: Number.POSITIVE_INFINITY,
      refetchInterval: false,
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      refetchOnMount: false
    }))
  });

  const { metadataByCell, errorByCell, pendingByCell } = useMemo(() => {
    const metadata = new Map<string, DomainMetadata>();
    const errors = new Map<string, string>();
    const pending = new Set<string>();

    queryPairs.forEach((pair, index) => {
      const query = metadataQueries[index];
      if (!query) return;
      const key = makeCellKey(pair.layerKey, pair.domainKey);
      if (query.data) metadata.set(key, query.data);
      if (query.error) {
        const message = formatSystemStatusText(query.error);
        errors.set(key, message);
      }
      if (query.isLoading || query.isFetching) pending.add(key);
    });

    return { metadataByCell: metadata, errorByCell: errors, pendingByCell: pending };
  }, [metadataQueries, queryPairs]);

  const totalCells = queryPairs.length;
  const loadedCells = metadataByCell.size;
  const failedCells = errorByCell.size;
  const pendingCells = pendingByCell.size;
  const isRefreshDisabled = isManualRefreshRunning || pendingCells > 0 || queryPairs.length === 0;

  const handleManualRefresh = async () => {
    if (isRefreshDisabled) return;
    setIsManualRefreshRunning(true);
    try {
      const results = await Promise.allSettled(
        queryPairs.map(async (pair) => {
          const metadata = await DataService.getDomainMetadata(pair.layerKey, pair.domainKey, {
            refresh: true
          });
          queryClient.setQueryData(queryKeys.domainMetadata(pair.layerKey, pair.domainKey), metadata);
        })
      );

      const failed = results.filter((result) => result.status === 'rejected').length;
      if (failed > 0) {
        console.error('[DomainLayerComparisonPanel] manual refresh failed', {
          failed,
          total: queryPairs.length
        });
      }
    } finally {
      setIsManualRefreshRunning(false);
    }
  };

  return (
    <Card className="h-full">
      <CardHeader className="gap-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0 flex-1">
            <div className="flex min-w-0 items-start gap-2">
              <GitCompareArrows className="mt-0.5 h-5 w-5 shrink-0" />
              <div className="min-w-0">
                <CardTitle className="leading-tight">Domain Layer Coverage</CardTitle>
                <CardDescription className="mt-1">
                  Compare symbol counts and date windows layer-to-layer for each domain.
                </CardDescription>
              </div>
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-2 self-start xl:justify-end">
            <Button
              variant="outline"
              size="sm"
              className="h-8 gap-2 px-3 text-xs"
              onClick={() => void handleManualRefresh()}
              disabled={isRefreshDisabled}
            >
              <RefreshCw className={`h-3.5 w-3.5 ${isManualRefreshRunning ? 'animate-spin' : ''}`} />
              Refresh Lineage
            </Button>
            <Badge variant="outline" className="inline-flex items-center gap-1">
              <Layers className="h-3.5 w-3.5" />
              {layerColumns.length} layer{layerColumns.length === 1 ? '' : 's'}
            </Badge>
            <Badge variant="outline" className={StatusTypos.MONO}>
              {domainRows.length} domain{domainRows.length === 1 ? '' : 's'}
            </Badge>
            <Badge variant="outline" className={StatusTypos.MONO}>
              {loadedCells}/{totalCells || 0} cells
            </Badge>
            {pendingCells > 0 ? (
              <Badge variant="outline" className="inline-flex items-center gap-1">
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Updating
              </Badge>
            ) : null}
            {failedCells > 0 ? (
              <Badge
                variant="outline"
                className="inline-flex items-center gap-1 border-destructive/40 text-destructive"
              >
                <AlertTriangle className="h-3.5 w-3.5" />
                {failedCells} unavailable
              </Badge>
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
        ) : (
          <div className="rounded-[1.2rem] border-2 border-mcm-walnut/20 bg-mcm-cream/30 overflow-hidden">
            <div className="overflow-x-auto">
              <Table className="min-w-[960px]">
                <caption className="sr-only">
                  Layer-by-layer domain comparison of symbol count and date ranges.
                </caption>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[180px]">Domain</TableHead>
                    {layerColumns.map((layer) => (
                      <TableHead key={layer.key} className="min-w-[190px]">
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
                        <TableCell className="align-top">
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
                                className={`${StatusTypos.MONO} align-top text-[11px] text-mcm-walnut/45`}
                              >
                                Not configured
                              </TableCell>
                            );
                          }

                          const key = makeCellKey(layerColumn.key, row.key);
                          const metadata = metadataByCell.get(key);
                          const error = errorByCell.get(key);
                          const isPending = pendingByCell.has(key);

                          if (!metadata && isPending) {
                            return (
                              <TableCell
                                key={`${row.key}-${layerColumn.key}`}
                                className={`${StatusTypos.MONO} align-top text-[11px] text-mcm-walnut/60`}
                              >
                                <span className="inline-flex items-center gap-1.5">
                                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                  Loading metadata
                                </span>
                              </TableCell>
                            );
                          }

                          if (!metadata && error) {
                            return (
                              <TableCell key={`${row.key}-${layerColumn.key}`} className="align-top">
                                <div className="space-y-1.5">
                                  <div className={`${StatusTypos.MONO} text-[11px] text-destructive`}>
                                    Metadata unavailable
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
                                className={`${StatusTypos.MONO} align-top text-[11px] text-mcm-walnut/55`}
                              >
                                Awaiting metadata
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

                          return (
                            <TableCell key={`${row.key}-${layerColumn.key}`} className="align-top">
                              <div className="space-y-1.5">
                                <div className={`${StatusTypos.MONO} text-base font-black text-mcm-walnut`}>
                                  {formatInt(metadata.symbolCount)}
                                  <span className="ml-1 text-[10px] uppercase tracking-widest text-mcm-walnut/55">
                                    symbols
                                  </span>
                                </div>
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
