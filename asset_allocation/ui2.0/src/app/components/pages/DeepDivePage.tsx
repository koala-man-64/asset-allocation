// Single Run Deep Dive Page - API-backed

import { useEffect, useMemo, useState } from 'react';
import { useApp } from '@/contexts/AppContext';
import { useRollingMulti, useRunList, useRunSummary, useTimeseriesMulti, useTrades } from '@/services/backtestHooks';
import { formatCurrency, formatNumber, formatPercent, formatPercentDecimal } from '@/utils/format';
import { computeMonthlyReturns, computeTopDrawdowns, kurtosis, skewness } from '@/utils/stats';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/app/components/ui/tabs';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/app/components/ui/select';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Copy, Download, RotateCcw, TrendingDown, TrendingUp } from 'lucide-react';

const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

export function DeepDivePage() {
  const { addToCart } = useApp();
  const { runs, loading: runsLoading, error: runsError } = useRunList({ limit: 200, offset: 0 });

  const [selectedRunId, setSelectedRunId] = useState<string>('');
  useEffect(() => {
    if (!selectedRunId && runs.length) setSelectedRunId(runs[0].run_id);
  }, [runs, selectedRunId]);

  const selectedRun = useMemo(() => runs.find((r) => r.run_id === selectedRunId) ?? null, [runs, selectedRunId]);

  const { data: summary } = useRunSummary(selectedRunId, { enabled: Boolean(selectedRunId), source: 'auto' });
  const { timeseriesByRunId, loading: tsLoading, error: tsError } = useTimeseriesMulti(selectedRunId ? [selectedRunId] : [], {
    source: 'auto',
    maxPoints: 200000,
  });
  const { rollingByRunId } = useRollingMulti(selectedRunId ? [selectedRunId] : [], 63, { source: 'auto', maxPoints: 5000 });
  const { data: tradesResponse, loading: tradesLoading, error: tradesError } = useTrades(selectedRunId, {
    enabled: Boolean(selectedRunId),
    source: 'auto',
    limit: 5000,
    offset: 0,
  });

  const timeseries = selectedRunId ? timeseriesByRunId[selectedRunId] : null;
  const rolling = selectedRunId ? rollingByRunId[selectedRunId] : null;

  const equityCurve = useMemo(() => {
    const points = timeseries?.points ?? [];
    return points.map((p) => ({ date: p.date, value: Number(p.portfolio_value) }));
  }, [timeseries]);

  const drawdownCurve = useMemo(() => {
    const points = timeseries?.points ?? [];
    return points.map((p) => ({ date: p.date, value: Number(p.drawdown) * 100 }));
  }, [timeseries]);

  const dailyReturns = useMemo(() => {
    const points = timeseries?.points ?? [];
    const out: Array<{ date: string; dailyReturn: number }> = [];
    for (let i = 0; i < points.length; i++) {
      const p = points[i];
      const explicit = p.daily_return;
      let r: number | null = null;
      if (explicit !== null && explicit !== undefined && Number.isFinite(explicit)) {
        r = explicit;
      } else if (i > 0) {
        const prev = points[i - 1];
        r = prev.portfolio_value ? p.portfolio_value / prev.portfolio_value - 1 : null;
      }
      if (r === null || !Number.isFinite(r)) continue;
      out.push({ date: p.date, dailyReturn: r });
    }
    return out;
  }, [timeseries]);

  const monthlyReturns = useMemo(() => computeMonthlyReturns(dailyReturns), [dailyReturns]);
  const years = useMemo(() => Array.from(new Set(monthlyReturns.map((d) => d.year))).sort(), [monthlyReturns]);

  const histogram = useMemo(() => {
    const returnsPct = dailyReturns.map((r) => r.dailyReturn * 100);
    return Array.from({ length: 20 }, (_, i) => {
      const min = -5 + i * 0.5;
      const max = min + 0.5;
      const count = returnsPct.filter((r) => r >= min && r < max).length;
      return { range: `${min.toFixed(1)}`, count };
    });
  }, [dailyReturns]);

  const topDrawdowns = useMemo(() => computeTopDrawdowns(drawdownCurve), [drawdownCurve]);

  const skew = skewness(dailyReturns.map((r) => r.dailyReturn));
  const kurt = kurtosis(dailyReturns.map((r) => r.dailyReturn));

  const dailyWinRate = useMemo(() => {
    if (!dailyReturns.length) return null;
    const wins = dailyReturns.filter((r) => r.dailyReturn > 0).length;
    return (wins / dailyReturns.length) * 100;
  }, [dailyReturns]);

  const monthlyWinRate = useMemo(() => {
    if (!monthlyReturns.length) return null;
    const wins = monthlyReturns.filter((r) => r.return > 0).length;
    return (wins / monthlyReturns.length) * 100;
  }, [monthlyReturns]);

  const trades = tradesResponse?.trades ?? [];
  const tradeRows = useMemo(() => {
    return trades.map((t) => {
      const side = t.quantity >= 0 ? 'BUY' : 'SELL';
      const shares = Math.abs(t.quantity);
      const totalCost = t.commission + t.slippage_cost;
      return {
        date: t.execution_date,
        symbol: t.symbol,
        side,
        shares,
        price: t.price,
        notional: t.notional,
        commission: t.commission,
        slippage: t.slippage_cost,
        totalCost,
      };
    });
  }, [trades]);

  const buys = tradeRows.filter((t) => t.side === 'BUY').length;
  const sells = tradeRows.filter((t) => t.side === 'SELL').length;

  const rollingSharpeCurve = useMemo(() => {
    const points = rolling?.points ?? [];
    return points
      .map((p) => ({ date: p.date, value: p.rolling_sharpe ?? null }))
      .filter((p) => p.value !== null);
  }, [rolling]);

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

  if (!selectedRunId) {
    return (
      <div className="space-y-6">
        <Card>
          <CardContent className="p-6 text-muted-foreground">No runs available.</CardContent>
        </Card>
      </div>
    );
  }

  if (tsLoading && !timeseries) {
    return (
      <div className="space-y-6">
        <Card>
          <CardContent className="p-6 text-muted-foreground">Loading timeseries…</CardContent>
        </Card>
      </div>
    );
  }

  if (tsError) {
    return (
      <div className="space-y-6">
        <Card>
          <CardContent className="p-6">
            <div className="font-semibold">Failed to load timeseries</div>
            <div className="text-sm text-muted-foreground mt-1">{tsError}</div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Run Selector */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-3">
                <Select value={selectedRunId} onValueChange={setSelectedRunId}>
                  <SelectTrigger className="w-80">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {runs.map((r) => (
                      <SelectItem key={r.run_id} value={r.run_id}>
                        {r.run_name || r.run_id}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Badge>{selectedRunId}</Badge>
                {selectedRun?.status && <Badge variant="outline">{selectedRun.status}</Badge>}
              </CardTitle>
              <div className="text-sm text-muted-foreground mt-2">
                Submitted: <span className="font-mono">{selectedRun?.submitted_at ?? '—'}</span>
              </div>
            </div>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={() => addToCart(selectedRunId)}>
                <Copy className="h-4 w-4 mr-2" />
                Add to Cart
              </Button>
              <Button variant="outline" size="sm" disabled>
                <RotateCcw className="h-4 w-4 mr-2" />
                Rerun
              </Button>
              <Button variant="outline" size="sm" disabled>
                <Download className="h-4 w-4 mr-2" />
                Download
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-4 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Window:</span>
              <span className="ml-2 font-mono">
                {(summary?.start_date as string | undefined) ?? selectedRun?.start_date ?? '—'} →{' '}
                {(summary?.end_date as string | undefined) ?? selectedRun?.end_date ?? '—'}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">Final Equity:</span>
              <span className="ml-2 font-mono">{formatCurrency(Number(summary?.final_equity))}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Trades:</span>
              <span className="ml-2 font-mono">{formatNumber(Number(summary?.trades), 0)}</span>
            </div>
            <div>
              <span className="text-muted-foreground">ADLS Prefix:</span>
              <span className="ml-2 font-mono">{selectedRun?.adls_prefix ?? '—'}</span>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Primary Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Equity Curve</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={equityCurve}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis
                    dataKey="date"
                    tick={{ fontSize: 10 }}
                    tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { year: '2-digit' })}
                  />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Line type="monotone" dataKey="value" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Drawdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={drawdownCurve}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis
                    dataKey="date"
                    tick={{ fontSize: 10 }}
                    tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { year: '2-digit' })}
                  />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Area type="monotone" dataKey="value" stroke="#ef4444" fill="#ef4444" fillOpacity={0.3} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Rolling Metrics (Example) */}
      <Card>
        <CardHeader>
          <CardTitle>Rolling Sharpe (63d)</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={rollingSharpeCurve}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis dataKey="date" tick={{ fontSize: 10 }} tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short' })} />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip />
                <Line type="monotone" dataKey="value" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>

      {/* Monthly Returns Heatmap */}
      <Card>
        <CardHeader>
          <CardTitle>Monthly Returns Heatmap</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr>
                  <th className="p-2 text-left">Year</th>
                  {months.map((m) => (
                    <th key={m} className="p-2 text-center">
                      {m}
                    </th>
                  ))}
                  <th className="p-2 text-center font-semibold">YTD</th>
                </tr>
              </thead>
              <tbody>
                {years.map((year) => {
                  const yearReturns = monthlyReturns.filter((d) => d.year === year);
                  const ytd = yearReturns.reduce((sum, d) => sum + d.return, 0);

                  return (
                    <tr key={year} className="border-b">
                      <td className="p-2 font-semibold">{year}</td>
                      {months.map((m, idx) => {
                        const monthData = yearReturns.find((d) => d.month === idx + 1);
                        const ret = monthData?.return ?? 0;
                        const color =
                          ret > 2 ? 'bg-green-600' : ret > 0 ? 'bg-green-400' : ret > -2 ? 'bg-red-400' : 'bg-red-600';

                        return (
                          <td key={m} className="p-1">
                            <div className={`${color} text-white rounded px-2 py-1 text-center`}>
                              {monthData ? ret.toFixed(1) : '-'}
                            </div>
                          </td>
                        );
                      })}
                      <td className="p-1">
                        <div
                          className={`${ytd > 0 ? 'bg-green-600' : 'bg-red-600'} text-white rounded px-2 py-1 text-center font-semibold`}
                        >
                          {ytd.toFixed(1)}%
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* Analysis Tabs */}
      <Card>
        <CardHeader>
          <CardTitle>Detailed Analysis</CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="stats" className="w-full">
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="stats">Return Stats</TabsTrigger>
              <TabsTrigger value="drawdowns">Drawdown Stats</TabsTrigger>
              <TabsTrigger value="consistency">Consistency</TabsTrigger>
            </TabsList>

            <TabsContent value="stats" className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <h4 className="font-semibold mb-3">Performance Metrics</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Ann. Return</span>
                      <span className="font-mono">{formatPercentDecimal(Number(summary?.annualized_return), 2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Ann. Volatility</span>
                      <span className="font-mono">{formatPercentDecimal(Number(summary?.annualized_volatility), 2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Sharpe Ratio</span>
                      <span className="font-mono font-semibold">{formatNumber(Number(summary?.sharpe_ratio), 2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Max Drawdown</span>
                      <span className="font-mono">{formatPercentDecimal(Number(summary?.max_drawdown), 2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Skewness</span>
                      <span className="font-mono">{skew === null ? '—' : skew.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Kurtosis</span>
                      <span className="font-mono">{kurt === null ? '—' : kurt.toFixed(2)}</span>
                    </div>
                  </div>
                </div>

                <div>
                  <h4 className="font-semibold mb-3">Return Distribution</h4>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={histogram}>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                        <XAxis dataKey="range" tick={{ fontSize: 10 }} />
                        <YAxis tick={{ fontSize: 10 }} />
                        <Tooltip />
                        <Bar dataKey="count" fill="hsl(var(--primary))" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="drawdowns" className="space-y-4">
              <h4 className="font-semibold">Top 5 Drawdowns</h4>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left p-2">Rank</th>
                      <th className="text-left p-2">Start Date</th>
                      <th className="text-left p-2">Trough Date</th>
                      <th className="text-left p-2">End Date</th>
                      <th className="text-right p-2">Depth</th>
                      <th className="text-right p-2">Duration (days)</th>
                      <th className="text-right p-2">Recovery (days)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topDrawdowns.map((dd, idx) => (
                      <tr key={idx} className="border-b hover:bg-muted/50">
                        <td className="p-2">{idx + 1}</td>
                        <td className="p-2 font-mono">{dd.startDate}</td>
                        <td className="p-2 font-mono">{dd.troughDate}</td>
                        <td className="p-2 font-mono">{dd.endDate || 'In Progress'}</td>
                        <td className="text-right p-2 font-mono text-red-500">{formatPercent(dd.depth, 2)}</td>
                        <td className="text-right p-2 font-mono">{dd.duration}</td>
                        <td className="text-right p-2 font-mono">{dd.recovery ?? '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div>
                <h4 className="font-semibold mb-3">Underwater Plot</h4>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={drawdownCurve}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip />
                      <Area type="monotone" dataKey="value" stroke="#ef4444" fill="#ef4444" fillOpacity={0.5} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="consistency" className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <h4 className="font-semibold mb-3">Win Rates</h4>
                  <div className="space-y-2 text-sm">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Daily Win Rate</span>
                      <span className="font-mono">{dailyWinRate === null ? '—' : formatPercent(dailyWinRate, 1)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Monthly Win Rate</span>
                      <span className="font-mono">{monthlyWinRate === null ? '—' : formatPercent(monthlyWinRate, 1)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Positive Months</span>
                      <span className="font-mono">
                        {monthlyReturns.filter((r) => r.return > 0).length} / {monthlyReturns.length}
                      </span>
                    </div>
                  </div>
                </div>

                <div>
                  <h4 className="font-semibold mb-3">Notes</h4>
                  <div className="text-sm text-muted-foreground">
                    Regime/attribution panels remain placeholders until backend exposes those datasets.
                  </div>
                </div>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Trade Blotter */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Trade Blotter</CardTitle>
            <div className="flex items-center gap-4 text-sm">
              <div className="text-muted-foreground">
                Total Trades: <span className="font-mono font-semibold">{tradeRows.length}</span>
              </div>
              <div className="text-muted-foreground">
                Buys: <span className="font-mono font-semibold text-green-600">{buys}</span>
              </div>
              <div className="text-muted-foreground">
                Sells: <span className="font-mono font-semibold text-red-600">{sells}</span>
              </div>
              <Button variant="outline" size="sm" disabled>
                <Download className="h-4 w-4 mr-2" />
                Export CSV
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {tradesError ? (
            <div className="text-sm text-muted-foreground">{tradesError}</div>
          ) : tradesLoading ? (
            <div className="text-sm text-muted-foreground">Loading trades…</div>
          ) : (
            <>
              <div className="rounded-md border">
                <div className="max-h-96 overflow-y-auto">
                  <Table>
                    <TableHeader className="sticky top-0 bg-background">
                      <TableRow>
                        <TableHead className="w-24">Date</TableHead>
                        <TableHead className="w-16">Side</TableHead>
                        <TableHead className="w-20">Symbol</TableHead>
                        <TableHead className="text-right">Shares</TableHead>
                        <TableHead className="text-right">Price</TableHead>
                        <TableHead className="text-right">Notional</TableHead>
                        <TableHead className="text-right">Commission</TableHead>
                        <TableHead className="text-right">Slippage</TableHead>
                        <TableHead className="text-right">Total Cost</TableHead>
                        <TableHead className="text-right">P&L</TableHead>
                        <TableHead className="text-right">P&L %</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {tradeRows.slice(-100).reverse().map((trade, idx) => (
                        <TableRow key={idx} className="hover:bg-muted/50">
                          <TableCell className="font-mono text-xs">{trade.date}</TableCell>
                          <TableCell>
                            <Badge variant={trade.side === 'BUY' ? 'default' : 'destructive'} className="font-mono text-xs">
                              {trade.side === 'BUY' ? (
                                <TrendingUp className="h-3 w-3 mr-1" />
                              ) : (
                                <TrendingDown className="h-3 w-3 mr-1" />
                              )}
                              {trade.side}
                            </Badge>
                          </TableCell>
                          <TableCell className="font-mono font-semibold">{trade.symbol}</TableCell>
                          <TableCell className="text-right font-mono">{trade.shares.toLocaleString()}</TableCell>
                          <TableCell className="text-right font-mono">${trade.price.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono">{formatCurrency(trade.notional)}</TableCell>
                          <TableCell className="text-right font-mono text-muted-foreground">{formatCurrency(trade.commission)}</TableCell>
                          <TableCell className="text-right font-mono text-muted-foreground">{formatCurrency(trade.slippage)}</TableCell>
                          <TableCell className="text-right font-mono font-semibold text-red-500">{formatCurrency(trade.totalCost)}</TableCell>
                          <TableCell className="text-right font-mono text-muted-foreground">-</TableCell>
                          <TableCell className="text-right font-mono text-muted-foreground">-</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>
              <div className="mt-4 text-xs text-muted-foreground">
                Showing last 100 trades (most recent first). P&L is not available from the API yet.
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
