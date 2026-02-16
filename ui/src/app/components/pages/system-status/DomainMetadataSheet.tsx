import React, { useMemo } from 'react';
import { CalendarDays, Database, Files, Hash, HardDrive, Info, Loader2, Ban } from 'lucide-react';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle
} from '@/app/components/ui/sheet';
import { Alert, AlertDescription, AlertTitle } from '@/app/components/ui/alert';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { StatusTypos } from './StatusTokens';
import { useDomainMetadataQuery } from '@/hooks/useDataQueries';

type LayerKey = 'bronze' | 'silver' | 'gold' | 'platinum';

export interface DomainMetadataSheetTarget {
  layer: LayerKey;
  domain: string;
  displayLayer: string;
  displayDomain: string;
  lastUpdated?: string | null;
}

interface DomainMetadataSheetProps {
  target: DomainMetadataSheetTarget | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

function formatInt(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return numberFormatter.format(value);
}

function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  if (value < 1024) return `${value} B`;
  const units = ['KB', 'MB', 'GB', 'TB'] as const;
  let size = value / 1024;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '—';
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toISOString().slice(0, 10);
}

function dateRangeUnavailableReason(metadata: {
  type?: string | null;
  dateRange?: { min?: string | null; max?: string | null } | null;
  warnings?: Array<string> | null;
}): string | null {
  if (!metadata) {
    return null;
  }

  if (metadata.type === 'blob') {
    return 'Date range is unavailable for blob-based domains.';
  }

  if (metadata.type === 'delta' && metadata.dateRange) {
    const hasBounds = Boolean(metadata.dateRange.min) || Boolean(metadata.dateRange.max);
    if (hasBounds) {
      return null;
    }
  }

  if (metadata.type !== 'delta') {
    return null;
  }

  if ((metadata.warnings || []).some((warning) => warning.toLowerCase().includes('date range')) {
    return 'Date range stats were unavailable or not parseable for this delta domain.';
  }
  return 'Date range is unavailable for this delta domain.';
}

export function DomainMetadataSheet({ target, open, onOpenChange }: DomainMetadataSheetProps) {
  const layer = target?.layer;
  const domain = target?.domain;

  const query = useDomainMetadataQuery(layer, domain, { enabled: open });
  const metadata = query.data;

  const headerSubtitle = useMemo(() => {
    if (!target) return '';
    const pieces: string[] = [];
    if (metadata?.container) pieces.push(`container=${metadata.container}`);
    if (metadata?.type === 'delta' && metadata?.tablePath)
      pieces.push(`table=${metadata.tablePath}`);
    if (metadata?.type === 'blob' && metadata?.prefix) pieces.push(`prefix=${metadata.prefix}`);
    return pieces.join(' • ');
  }, [metadata?.container, metadata?.prefix, metadata?.tablePath, metadata?.type, target]);

  const dateRange = metadata?.dateRange
    ? `${formatDate(metadata.dateRange.min)} → ${formatDate(metadata.dateRange.max)}`
    : '—';
  const dateRangeReason = metadata ? dateRangeUnavailableReason(metadata) : null;

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="w-[420px] sm:w-[540px] bg-mcm-paper border-l-2 border-mcm-walnut shadow-[10px_10px_0px_0px_rgba(119,63,26,0.1)]"
      >
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            <Database className="h-4 w-4 text-mcm-walnut/70" />
            <span>
              {target ? `${target.displayLayer} • ${target.displayDomain}` : 'Dataset metadata'}
            </span>
          </SheetTitle>
          <SheetDescription className="text-xs text-mcm-walnut/60">
            {headerSubtitle || 'Symbols, date range, and storage rollups.'}
          </SheetDescription>
        </SheetHeader>

        <div className="px-4 pb-4">
          {query.isLoading ? (
            <div className="flex items-center gap-2 text-sm text-mcm-walnut/70">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading metadata…
            </div>
          ) : query.isError ? (
            <Alert variant="destructive">
              <AlertTitle>Metadata unavailable</AlertTitle>
              <AlertDescription className="break-words">
                {query.error instanceof Error ? query.error.message : String(query.error)}
              </AlertDescription>
            </Alert>
          ) : metadata ? (
            <div className="grid gap-4">
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/60 p-3">
                  <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-mcm-olive">
                    <Hash className="h-3.5 w-3.5 text-mcm-walnut/60" />
                    Active Symbols
                  </div>
                  <div className={`${StatusTypos.MONO} mt-1 text-lg font-black text-mcm-walnut`}>
                    {formatInt(metadata.symbolCount)}
                  </div>
                </div>
                <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/60 p-3">
                  <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-mcm-olive">
                    <Ban className="h-3.5 w-3.5 text-mcm-walnut/60" />
                    Blacklisted
                  </div>
                  <div className={`${StatusTypos.MONO} mt-1 text-lg font-black text-mcm-walnut`}>
                    {formatInt(metadata.blacklistedSymbolCount)}
                  </div>
                </div>
                <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/60 p-3">
                  <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-mcm-olive">
                    <Files className="h-3.5 w-3.5 text-mcm-walnut/60" />
                    Rows
                  </div>
                  <div className={`${StatusTypos.MONO} mt-1 text-lg font-black text-mcm-walnut`}>
                    {formatInt(metadata.totalRows)}
                  </div>
                </div>
                <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/60 p-3">
                  <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-mcm-olive">
                    <CalendarDays className="h-3.5 w-3.5 text-mcm-walnut/60" />
                    Date Range
                  </div>
                  <div className={`${StatusTypos.MONO} mt-1 text-sm font-bold text-mcm-walnut/80`}>
                    <div className="inline-flex items-center gap-1">
                      {dateRange}
                      {dateRangeReason ? (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Info className="h-3.5 w-3.5 cursor-help opacity-70" />
                          </TooltipTrigger>
                          <TooltipContent side="top" className="max-w-xs">
                            {dateRangeReason}
                          </TooltipContent>
                        </Tooltip>
                      ) : null}
                    </div>
                    {dateRangeReason ? (
                      <div className="mt-1 text-[10px] text-mcm-walnut/55">{dateRangeReason}</div>
                    ) : null}
                  </div>
                  {metadata.dateRange?.column ? (
                    <div className="mt-1 text-[10px] text-mcm-walnut/50">
                      column: <span className={StatusTypos.MONO}>{metadata.dateRange.column}</span>
                    </div>
                  ) : null}
                </div>
                <div className="rounded-xl border-2 border-mcm-walnut/15 bg-mcm-cream/60 p-3">
                  <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-widest text-mcm-olive">
                    <HardDrive className="h-3.5 w-3.5 text-mcm-walnut/60" />
                    Storage
                  </div>
                  <div className={`${StatusTypos.MONO} mt-1 text-sm font-bold text-mcm-walnut/80`}>
                    {formatBytes(metadata.totalBytes)}
                  </div>
                  <div className="mt-1 text-[10px] text-mcm-walnut/50">
                    files: <span className={StatusTypos.MONO}>{formatInt(metadata.fileCount)}</span>
                    {metadata.deltaVersion !== null && metadata.deltaVersion !== undefined ? (
                      <>
                        {' • '}v
                        <span className={StatusTypos.MONO}>{formatInt(metadata.deltaVersion)}</span>
                      </>
                    ) : null}
                  </div>
                </div>
              </div>

              {metadata.warnings && metadata.warnings.length > 0 ? (
                <div className="rounded-xl border border-mcm-walnut/15 bg-mcm-cream/40 p-3 text-xs text-mcm-walnut/70">
                  <div className={`${StatusTypos.HEADER} text-[10px]`}>NOTES</div>
                  <ul className="mt-1 list-disc pl-5">
                    {metadata.warnings.map((warning, idx) => (
                      <li key={idx}>{warning}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="text-sm text-mcm-walnut/60">Select a domain to view metadata.</div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
