import React, { useEffect, useMemo, useState } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { ArrowRight, ArrowUpDown, Database, Loader2, RefreshCcw, Search } from 'lucide-react';

import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Input } from '@/app/components/ui/input';
import { ScrollArea } from '@/app/components/ui/scroll-area';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import type { StockScreenerRow } from '@/services/backtestApi';
import { DataService } from '@/services/DataService';
import { cn } from '@/app/components/ui/utils';

type SortDirection = 'asc' | 'desc';

const PAGE_SIZE = 250;

const formatPrice = (value: number | null | undefined): string => {
  if (value === null || value === undefined) return '—';
  if (!Number.isFinite(value)) return '—';
  return `$${value.toFixed(2)}`;
};

const formatPercent = (value: number | null | undefined, digits = 2): string => {
  if (value === null || value === undefined) return '—';
  if (!Number.isFinite(value)) return '—';
  return `${(value * 100).toFixed(digits)}%`;
};

const formatMillions = (value: number | null | undefined): string => {
  if (value === null || value === undefined) return '—';
  if (!Number.isFinite(value)) return '—';
  return `${(value / 1_000_000).toFixed(1)}M`;
};

const formatNumber = (value: number | null | undefined, digits = 2): string => {
  if (value === null || value === undefined) return '—';
  if (!Number.isFinite(value)) return '—';
  return value.toFixed(digits);
};

const heatClassForPercent = (value: number | null | undefined): string => {
  if (value === null || value === undefined) return 'text-muted-foreground';
  if (!Number.isFinite(value)) return 'text-muted-foreground';
  if (value > 0) return 'text-emerald-600 dark:text-emerald-400';
  if (value < 0) return 'text-rose-600 dark:text-rose-400';
  return 'text-slate-600 dark:text-slate-400';
};

export function StockExplorerPage() {
  const navigate = useNavigate();

  const [rawQuery, setRawQuery] = useState('');
  const [query, setQuery] = useState('');
  const [asOf, setAsOf] = useState<string>('');
  const [sort, setSort] = useState('volume');
  const [direction, setDirection] = useState<SortDirection>('desc');

  useEffect(() => {
    const handle = window.setTimeout(() => setQuery(rawQuery.trim()), 250);
    return () => window.clearTimeout(handle);
  }, [rawQuery]);

  const queryKey = useMemo(
    () => ['stockScreener', query || '-', sort, direction, asOf || '-'] as const,
    [query, sort, direction, asOf]
  );

  const screenerQuery = useInfiniteQuery({
    queryKey,
    queryFn: ({ pageParam, signal }) =>
      DataService.getStockScreener(
        {
          q: query || undefined,
          limit: PAGE_SIZE,
          offset: typeof pageParam === 'number' ? pageParam : 0,
          asOf: asOf || undefined,
          sort,
          direction
        },
        signal
      ),
    initialPageParam: 0,
    getNextPageParam: (lastPage) => {
      const nextOffset = lastPage.offset + (lastPage.rows?.length ?? 0);
      if (nextOffset >= lastPage.total) return undefined;
      return nextOffset;
    },
    staleTime: 15_000,
    retry: false
  });

  const rows = useMemo(() => {
    const pages = screenerQuery.data?.pages ?? [];
    return pages.flatMap((page) => page.rows ?? []) as StockScreenerRow[];
  }, [screenerQuery.data]);

  const total = screenerQuery.data?.pages?.[0]?.total ?? 0;
  const resolvedAsOf = screenerQuery.data?.pages?.[0]?.asOf ?? null;
  const showing = rows.length;

  const onToggleSort = (nextSort: string) => {
    if (sort === nextSort) {
      setDirection((d) => (d === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSort(nextSort);
    setDirection('desc');
  };

  const sortChip = `${sort}${direction === 'asc' ? ' ↑' : ' ↓'}`;

  return (
    <div className="h-full flex flex-col gap-4 p-6">
      {/* Design Direction: Institutional terminal (dense, honest, daily-only) */}
      <div className="flex flex-col gap-3">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
          <div className="min-w-0">
            <div className="flex items-center gap-3">
              <div className="grid h-10 w-10 place-items-center rounded-lg border bg-zinc-950 text-zinc-50 shadow-sm">
                <Database className="h-5 w-5" />
              </div>
              <div className="min-w-0">
                <h1 className="text-lg font-black tracking-tight">STOCK SCREENER</h1>
                <div className="mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-1 font-mono text-[11px] uppercase tracking-widest text-muted-foreground">
                  <span>Daily</span>
                  <span>•</span>
                  <span>Universe: Postgres</span>
                  <span>•</span>
                  <span>Layers: Silver + Gold</span>
                  {resolvedAsOf && (
                    <>
                      <span>•</span>
                      <span>As-of {resolvedAsOf}</span>
                    </>
                  )}
                </div>
              </div>
            </div>
          </div>

          <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-end">
            <div className="relative w-full sm:w-[320px]">
              <Search className="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input
                value={rawQuery}
                onChange={(e) => setRawQuery(e.target.value)}
                placeholder="Search symbol or name…"
                className="h-9 pl-9 font-mono text-xs"
              />
            </div>
            <div className="flex items-center gap-2">
              <Input
                type="date"
                value={asOf}
                onChange={(e) => setAsOf(e.target.value)}
                className="h-9 w-[160px] font-mono text-xs"
                aria-label="As-of date"
              />
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-9 gap-2 font-mono text-[11px]"
                onClick={() => void screenerQuery.refetch()}
              >
                <RefreshCcw className={cn('h-4 w-4', screenerQuery.isFetching && 'animate-spin')} />
                Refresh
              </Button>
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline" className="font-mono text-[10px] uppercase tracking-widest">
            {screenerQuery.isFetching ? 'Loading' : screenerQuery.isError ? 'Unavailable' : 'Ready'}
          </Badge>
          <Badge variant="secondary" className="font-mono text-[10px] uppercase tracking-widest">
            Sort: {sortChip}
          </Badge>
          <Badge variant="outline" className="font-mono text-[10px] uppercase tracking-widest">
            Showing {showing.toLocaleString()} / {total.toLocaleString()}
          </Badge>
        </div>
      </div>

      <Card className="flex-1 overflow-hidden">
        <CardHeader className="flex flex-row items-center justify-between gap-3 border-b py-3">
          <CardTitle className="text-xs font-black uppercase tracking-widest text-muted-foreground">
            Cross-Section Snapshot
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="font-mono text-[10px] uppercase tracking-widest">
              Silver: OHLCV
            </Badge>
            <Badge variant="outline" className="font-mono text-[10px] uppercase tracking-widest">
              Gold: Features
            </Badge>
          </div>
        </CardHeader>

        <CardContent className="p-0">
          <ScrollArea className="h-[calc(100vh-290px)]">
            {screenerQuery.isError ? (
              <div className="p-6 font-mono text-xs text-rose-600">
                {(screenerQuery.error as Error)?.message || 'Failed to load screener.'}
                <div className="mt-2 text-muted-foreground">
                  Requires Postgres (`core.symbols`) + Silver/Gold by-date Delta tables.
                </div>
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/30 hover:bg-muted/30">
                    <TableHead className="w-[120px] font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('symbol')}
                        className="inline-flex items-center gap-1 hover:text-foreground"
                      >
                        Symbol <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="min-w-[240px] font-mono text-[10px] uppercase tracking-widest">
                      Name
                    </TableHead>
                    <TableHead className="min-w-[160px] font-mono text-[10px] uppercase tracking-widest">
                      Sector
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('close')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        Close <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('return_1d')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        1D% <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('return_5d')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        5D% <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('vol_20d')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        Vol20 <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('drawdown_1y')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        DD1Y <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('atr_14d')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        ATR14 <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('volume')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        Vol <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="text-right font-mono text-[10px] uppercase tracking-widest">
                      <button
                        type="button"
                        onClick={() => onToggleSort('compression_score')}
                        className="inline-flex items-center justify-end gap-1 hover:text-foreground"
                      >
                        Compress <ArrowUpDown className="h-3.5 w-3.5" />
                      </button>
                    </TableHead>
                    <TableHead className="w-[70px]" />
                  </TableRow>
                </TableHeader>

                <TableBody>
                  {rows.map((row) => {
                    const symbol = String(row.symbol || '')
                      .trim()
                      .toUpperCase();
                    const hasSilver = Boolean(row.hasSilver);
                    const hasGold = Boolean(row.hasGold);

                    return (
                      <TableRow
                        key={`${symbol}:${row.name || ''}`}
                        className="text-xs hover:bg-muted/40"
                      >
                        <TableCell className="font-mono font-black">
                          <div className="flex items-center gap-2">
                            <button
                              type="button"
                              onClick={() => navigate(`/stock/${encodeURIComponent(symbol)}`)}
                              className="text-primary hover:underline decoration-dotted underline-offset-4"
                            >
                              {symbol || '—'}
                            </button>
                            <div className="flex items-center gap-1">
                              <span
                                className={cn(
                                  'h-2 w-2 rounded-full border',
                                  hasSilver ? 'bg-sky-500 border-sky-600' : 'bg-muted border-border'
                                )}
                                title={hasSilver ? 'Silver data present' : 'Missing Silver data'}
                              />
                              <span
                                className={cn(
                                  'h-2 w-2 rounded-full border',
                                  hasGold
                                    ? 'bg-amber-500 border-amber-600'
                                    : 'bg-muted border-border'
                                )}
                                title={hasGold ? 'Gold data present' : 'Missing Gold data'}
                              />
                            </div>
                          </div>
                        </TableCell>
                        <TableCell className="min-w-[240px]">
                          <div className="truncate font-medium text-slate-900 dark:text-slate-100">
                            {row.name || '—'}
                          </div>
                          <div className="truncate font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
                            {row.industry || ''}
                          </div>
                        </TableCell>
                        <TableCell className="min-w-[160px] text-muted-foreground">
                          {row.sector || '—'}
                        </TableCell>
                        <TableCell className="text-right font-mono font-semibold">
                          {formatPrice(row.close)}
                        </TableCell>
                        <TableCell
                          className={cn(
                            'text-right font-mono font-semibold',
                            heatClassForPercent(row.return1d)
                          )}
                        >
                          {formatPercent(row.return1d)}
                        </TableCell>
                        <TableCell
                          className={cn(
                            'text-right font-mono font-semibold',
                            heatClassForPercent(row.return5d)
                          )}
                        >
                          {formatPercent(row.return5d)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          {formatPercent(row.vol20d, 1)}
                        </TableCell>
                        <TableCell
                          className={cn(
                            'text-right font-mono font-semibold',
                            heatClassForPercent(row.drawdown1y)
                          )}
                        >
                          {formatPercent(row.drawdown1y, 1)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          {formatNumber(row.atr14d, 2)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          {formatMillions(row.volume)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          {row.compressionScore === null || row.compressionScore === undefined
                            ? '—'
                            : `${Math.round((1 - row.compressionScore) * 100)}%`}
                        </TableCell>
                        <TableCell className="text-center">
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            onClick={() => navigate(`/stock/${encodeURIComponent(symbol)}`)}
                            aria-label={`Open ${symbol}`}
                          >
                            <ArrowRight className="h-4 w-4 text-muted-foreground" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    );
                  })}

                  {screenerQuery.isFetching && rows.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={12} className="py-16">
                        <div className="flex items-center justify-center gap-2 text-muted-foreground">
                          <Loader2 className="h-4 w-4 animate-spin" />
                          <span className="font-mono text-xs uppercase tracking-widest">
                            Loading snapshot…
                          </span>
                        </div>
                      </TableCell>
                    </TableRow>
                  )}

                  {!screenerQuery.isFetching && rows.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={12} className="py-16 text-center">
                        <div className="font-mono text-xs uppercase tracking-widest text-muted-foreground">
                          No rows found
                        </div>
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            )}
          </ScrollArea>

          <div className="flex items-center justify-between border-t px-4 py-3">
            <div className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">
              {resolvedAsOf ? `As-of ${resolvedAsOf}` : 'As-of —'} • {showing.toLocaleString()}{' '}
              shown • {total.toLocaleString()} total
            </div>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-8 font-mono text-[11px]"
                disabled={!screenerQuery.hasNextPage || screenerQuery.isFetchingNextPage}
                onClick={() => void screenerQuery.fetchNextPage()}
              >
                {screenerQuery.isFetchingNextPage
                  ? 'Loading…'
                  : screenerQuery.hasNextPage
                    ? 'Load More'
                    : 'End'}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
