// Overview Page - PM Scoreboard (API-backed)

import { useMemo } from 'react';
import { useApp } from '@/contexts/AppContext';
import { useRunList, useRunSummaries } from '@/services/backtestHooks';
import { formatCurrency, formatNumber, formatPercentDecimal } from '@/utils/format';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Checkbox } from '@/app/components/ui/checkbox';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/app/components/ui/tooltip';
import { AlertTriangle, TrendingDown } from 'lucide-react';
import {
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
  ZAxis,
} from 'recharts';

export function OverviewPage() {
  const { selectedRuns, addToCart, removeFromCart } = useApp();
  const { runs, loading: runsLoading, error: runsError } = useRunList({ limit: 200, offset: 0 });

  const runIds = useMemo(() => runs.map((r) => r.run_id), [runs]);
  const { summaries, loading: summariesLoading } = useRunSummaries(runIds, { limit: 100, source: 'auto' });

  const rows = useMemo(() => {
    return runs.map((run) => ({ run, summary: summaries[run.run_id] }));
  }, [runs, summaries]);

  const rowsWithSummary = rows.filter((r) => Boolean(r.summary));

  const bestSharpe = rowsWithSummary.reduce<{ name: string; value: number } | null>((best, row) => {
    const sharpe = Number(row.summary?.sharpe_ratio);
    if (!Number.isFinite(sharpe)) return best;
    const name = row.run.run_name || row.run.run_id;
    if (!best || sharpe > best.value) return { name, value: sharpe };
    return best;
  }, null);

  const worstDrawdown = rowsWithSummary.reduce<{ name: string; value: number } | null>((worst, row) => {
    const dd = Number(row.summary?.max_drawdown);
    if (!Number.isFinite(dd)) return worst;
    const name = row.run.run_name || row.run.run_id;
    if (!worst || dd < worst.value) return { name, value: dd };
    return worst;
  }, null);

  const mostTrades = rowsWithSummary.reduce<{ name: string; value: number } | null>((best, row) => {
    const trades = Number(row.summary?.trades);
    if (!Number.isFinite(trades)) return best;
    const name = row.run.run_name || row.run.run_id;
    if (!best || trades > best.value) return { name, value: trades };
    return best;
  }, null);

  const scatterData = useMemo(() => {
    return rowsWithSummary
      .map((row) => {
        const sharpe = Number(row.summary?.sharpe_ratio);
        const maxDD = Number(row.summary?.max_drawdown);
        const cagr = Number(row.summary?.annualized_return);
        if (!Number.isFinite(sharpe) || !Number.isFinite(maxDD) || !Number.isFinite(cagr)) return null;
        return {
          name: row.run.run_name || row.run.run_id,
          sharpe,
          maxDD: Math.abs(maxDD * 100),
          cagr: cagr * 100,
        };
      })
      .filter(Boolean);
  }, [rowsWithSummary]);

  const handleCheckboxChange = (runId: string, checked: boolean) => {
    if (checked) addToCart(runId);
    else removeFromCart(runId);
  };

  if (runsLoading) {
    return (
      <div className="space-y-6">
        <Card>
          <CardContent className="p-6 text-muted-foreground">Loading runs…</CardContent>
        </Card>
      </div>
    );
  }

  if (runsError) {
    return (
      <div className="space-y-6">
        <Card>
          <CardContent className="p-6">
            <div className="font-semibold">Failed to load runs</div>
            <div className="text-sm text-muted-foreground mt-1">{runsError}</div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* KPI Ribbon */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">Best Sharpe Ratio</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{bestSharpe ? formatNumber(bestSharpe.value, 2) : '—'}</div>
            <p className="text-xs text-muted-foreground mt-1">{bestSharpe?.name ?? 'No summaries loaded yet'}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <TrendingDown className="h-4 w-4 text-red-500" />
              Worst Max Drawdown
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-500">
              {worstDrawdown ? formatPercentDecimal(worstDrawdown.value, 1) : '—'}
            </div>
            <p className="text-xs text-muted-foreground mt-1">{worstDrawdown?.name ?? 'No summaries loaded yet'}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-orange-500" />
              Most Trades
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{mostTrades ? formatNumber(mostTrades.value, 0) : '—'}</div>
            <p className="text-xs text-muted-foreground mt-1">{mostTrades?.name ?? 'No summaries loaded yet'}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">Total Runs</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{runs.length}</div>
            <p className="text-xs text-muted-foreground mt-1">
              {summariesLoading ? 'Loading summary metrics…' : 'Available for analysis'}
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Run Table */}
      <Card>
        <CardHeader>
          <CardTitle>Run Universe</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-12"></TableHead>
                  <TableHead>Run</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Ann. Return</TableHead>
                  <TableHead className="text-right">Ann. Vol</TableHead>
                  <TableHead className="text-right">Sharpe</TableHead>
                  <TableHead className="text-right">Max DD</TableHead>
                  <TableHead className="text-right">Trades</TableHead>
                  <TableHead className="text-right">Final Equity</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map(({ run, summary }) => {
                  const runName = run.run_name || run.run_id;

                  return (
                    <TableRow key={run.run_id} className="hover:bg-muted/50">
                      <TableCell>
                        <Checkbox
                          checked={selectedRuns.has(run.run_id)}
                          onCheckedChange={(checked) => handleCheckboxChange(run.run_id, checked as boolean)}
                        />
                      </TableCell>

                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="font-medium cursor-help">
                              <div className="leading-tight">
                                <div>{runName}</div>
                                <div className="text-xs text-muted-foreground font-mono">{run.run_id}</div>
                              </div>
                            </TableCell>
                          </TooltipTrigger>
                          <TooltipContent side="right">
                            <div className="space-y-1">
                              <p className="font-semibold">{runName}</p>
                              <p className="text-xs">Run ID: {run.run_id}</p>
                              {run.start_date && run.end_date && (
                                <p className="text-xs">
                                  Window: {run.start_date} → {run.end_date}
                                </p>
                              )}
                              {run.error && <p className="text-xs text-red-500">Error: {run.error}</p>}
                            </div>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>

                      <TableCell>
                        <Badge variant={run.status === 'failed' ? 'destructive' : 'outline'}>{run.status}</Badge>
                      </TableCell>

                      <TableCell className="text-right font-mono">
                        {summary ? formatPercentDecimal(Number(summary.annualized_return), 1) : '—'}
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        {summary ? formatPercentDecimal(Number(summary.annualized_volatility), 1) : '—'}
                      </TableCell>
                      <TableCell className="text-right font-mono font-semibold">
                        {summary ? formatNumber(Number(summary.sharpe_ratio), 2) : '—'}
                      </TableCell>
                      <TableCell className="text-right font-mono text-red-500">
                        {summary ? formatPercentDecimal(Number(summary.max_drawdown), 1) : '—'}
                      </TableCell>
                      <TableCell className="text-right font-mono">{summary ? formatNumber(Number(summary.trades), 0) : '—'}</TableCell>
                      <TableCell className="text-right font-mono">
                        {summary ? formatCurrency(Number(summary.final_equity)) : '—'}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      {/* Distribution Sanity Panel */}
      <Card>
        <CardHeader>
          <CardTitle>Distribution Sanity Check</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 20, right: 20, bottom: 60, left: 60 }}>
                <XAxis
                  type="number"
                  dataKey="maxDD"
                  name="Max Drawdown"
                  label={{ value: 'Max Drawdown (%)', position: 'bottom', offset: 40 }}
                />
                <YAxis
                  type="number"
                  dataKey="sharpe"
                  name="Sharpe Ratio"
                  label={{ value: 'Sharpe Ratio', angle: -90, position: 'left', offset: 40 }}
                />
                <ZAxis type="number" dataKey="cagr" range={[50, 400]} />
                <RechartsTooltip
                  cursor={{ strokeDasharray: '3 3' }}
                  content={({ active, payload }) => {
                    if (active && payload && payload.length) {
                      const data: any = payload[0].payload;
                      return (
                        <div className="bg-background border rounded-lg p-3 shadow-lg">
                          <p className="font-semibold">{data.name}</p>
                          <p className="text-sm">Sharpe: {Number(data.sharpe).toFixed(2)}</p>
                          <p className="text-sm">Max DD: {Number(data.maxDD).toFixed(1)}%</p>
                          <p className="text-sm">Ann. Return: {Number(data.cagr).toFixed(1)}%</p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
                <Scatter data={scatterData as any} fill="hsl(var(--primary))" />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
          <div className="text-xs text-muted-foreground mt-3">
            Chart uses summary metrics when available (up to the first 100 runs).
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
