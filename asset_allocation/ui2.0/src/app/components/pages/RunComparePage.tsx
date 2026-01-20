// Run Compare Page - The Workbench for side-by-side strategy evaluation

import { useMemo, useState } from 'react';
import { useApp } from '@/contexts/AppContext';
import { useRollingMulti, useRunList, useRunSummaries, useTimeseriesMulti } from '@/services/backtestHooks';
import { formatCurrency, formatNumber, formatPercentDecimal } from '@/utils/format';
import { correlation as corr } from '@/utils/stats';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Switch } from '@/app/components/ui/switch';
import { Label } from '@/app/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid
} from 'recharts';
import { X } from 'lucide-react';
import { Button } from '@/app/components/ui/button';

const colors = ['#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#eab308'];

export function RunComparePage() {
  const { selectedRuns, removeFromCart } = useApp();
  const [normalizeToHundred, setNormalizeToHundred] = useState(true);
  const [logScale, setLogScale] = useState(false);
  const [rollingWindow, setRollingWindow] = useState('3m');
  
  const selectedRunIds = useMemo(() => Array.from(selectedRuns.values()), [selectedRuns]);
  const { runs } = useRunList({ limit: 200, offset: 0 });
  const runsById = useMemo(() => new Map(runs.map((r) => [r.run_id, r])), [runs]);

  const { summaries } = useRunSummaries(selectedRunIds, { source: 'auto' });
  const { timeseriesByRunId, loading: tsLoading, error: tsError } = useTimeseriesMulti(selectedRunIds, {
    source: 'auto',
    maxPoints: 200000,
  });

  const windowDays = rollingWindow === '1m' ? 21 : rollingWindow === '6m' ? 126 : rollingWindow === '12m' ? 252 : 63;
  const { rollingByRunId } = useRollingMulti(selectedRunIds, windowDays, {
    source: 'auto',
    maxPoints: 200000,
  });
  
  if (selectedRunIds.length === 0) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <p className="text-lg text-muted-foreground">No strategies selected for comparison</p>
          <p className="text-sm text-muted-foreground mt-2">Add strategies from the Overview page</p>
        </div>
      </div>
    );
  }
  
  const seriesMaps = useMemo(() => {
    const out = new Map<string, Map<string, any>>();
    selectedRunIds.forEach((runId) => {
      const points = timeseriesByRunId[runId]?.points ?? [];
      const map = new Map<string, any>();
      points.forEach((p) => map.set(p.date, p));
      out.set(runId, map);
    });
    return out;
  }, [selectedRunIds.join('|'), timeseriesByRunId]);

  const equityData = useMemo(() => {
    const dates = new Set<string>();
    selectedRunIds.forEach((runId) => {
      const points = timeseriesByRunId[runId]?.points ?? [];
      points.forEach((p) => dates.add(p.date));
    });
    const ordered = Array.from(dates.values()).sort();

    const baseByRunId = new Map<string, number>();
    selectedRunIds.forEach((runId) => {
      const pts = timeseriesByRunId[runId]?.points ?? [];
      const base = pts.length ? pts[0].portfolio_value : 0;
      baseByRunId.set(runId, base);
    });

    return ordered.map((date) => {
      const row: any = { date };
      selectedRunIds.forEach((runId) => {
        const point = seriesMaps.get(runId)?.get(date);
        if (!point) {
          row[runId] = null;
          return;
        }
        const value = Number(point.portfolio_value);
        const base = baseByRunId.get(runId) || 0;
        row[runId] = normalizeToHundred && base ? (value / base) * 100 : value;
      });
      return row;
    });
  }, [selectedRunIds.join('|'), timeseriesByRunId, normalizeToHundred, seriesMaps]);

  const drawdownData = useMemo(() => {
    const dates = new Set<string>();
    selectedRunIds.forEach((runId) => {
      const points = timeseriesByRunId[runId]?.points ?? [];
      points.forEach((p) => dates.add(p.date));
    });
    const ordered = Array.from(dates.values()).sort();

    return ordered.map((date) => {
      const row: any = { date };
      selectedRunIds.forEach((runId) => {
        const point = seriesMaps.get(runId)?.get(date);
        row[runId] = point ? Number(point.drawdown) * 100 : null;
      });
      return row;
    });
  }, [selectedRunIds.join('|'), timeseriesByRunId, seriesMaps]);

  const rollingMaps = useMemo(() => {
    const out = new Map<string, Map<string, any>>();
    selectedRunIds.forEach((runId) => {
      const points = rollingByRunId[runId]?.points ?? [];
      const map = new Map<string, any>();
      points.forEach((p) => map.set(p.date, p));
      out.set(runId, map);
    });
    return out;
  }, [selectedRunIds.join('|'), rollingByRunId]);

  const rollingDates = useMemo(() => {
    const dates = new Set<string>();
    selectedRunIds.forEach((runId) => {
      const points = rollingByRunId[runId]?.points ?? [];
      points.forEach((p) => dates.add(p.date));
    });
    return Array.from(dates.values()).sort();
  }, [selectedRunIds.join('|'), rollingByRunId]);

  const buildRollingData = (valueFn: (p: any) => number | null) => {
    return rollingDates.map((date) => {
      const row: any = { date };
      selectedRunIds.forEach((runId) => {
        const point = rollingMaps.get(runId)?.get(date);
        row[runId] = point ? valueFn(point) : null;
      });
      return row;
    });
  };

  const rollingSharpeData = useMemo(() => buildRollingData((p) => (p.rolling_sharpe ?? null) as number | null), [rollingDates.join('|'), rollingMaps]);
  const rollingVolData = useMemo(
    () => buildRollingData((p) => (p.rolling_volatility === null || p.rolling_volatility === undefined ? null : Number(p.rolling_volatility) * 100)),
    [rollingDates.join('|'), rollingMaps],
  );
  const rollingReturnData = useMemo(
    () => buildRollingData((p) => (p.rolling_return === null || p.rolling_return === undefined ? null : Number(p.rolling_return) * 100)),
    [rollingDates.join('|'), rollingMaps],
  );
  const rollingMaxDDData = useMemo(
    () => buildRollingData((p) => (p.rolling_max_drawdown === null || p.rolling_max_drawdown === undefined ? null : Number(p.rolling_max_drawdown) * 100)),
    [rollingDates.join('|'), rollingMaps],
  );

  const correlationMatrix = useMemo(() => {
    const returnsByRunId = new Map<string, Map<string, number>>();
    selectedRunIds.forEach((runId) => {
      const points = timeseriesByRunId[runId]?.points ?? [];
      const m = new Map<string, number>();
      points.forEach((p, idx) => {
        const explicit = p.daily_return;
        if (explicit !== null && explicit !== undefined && Number.isFinite(explicit)) {
          m.set(p.date, explicit);
          return;
        }
        if (idx === 0) return;
        const prev = points[idx - 1];
        const r = prev.portfolio_value ? p.portfolio_value / prev.portfolio_value - 1 : null;
        if (r !== null && Number.isFinite(r)) m.set(p.date, r);
      });
      returnsByRunId.set(runId, m);
    });

    return selectedRunIds.map((runA, i) =>
      selectedRunIds.map((runB, j) => {
        if (i === j) return 1.0;
        const a = returnsByRunId.get(runA);
        const b = returnsByRunId.get(runB);
        if (!a || !b) return 0;
        const dates = Array.from(a.keys()).filter((d) => b.has(d));
        const xs = dates.map((d) => a.get(d) as number);
        const ys = dates.map((d) => b.get(d) as number);
        const c = corr(xs, ys);
        return c === null ? 0 : c;
      }),
    );
  }, [selectedRunIds.join('|'), timeseriesByRunId]);

  const hasAnyTimeseries = selectedRunIds.some((id) => Boolean(timeseriesByRunId[id]));
  if (tsLoading && !hasAnyTimeseries) {
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
      {/* Controls & Legend */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Comparing {selectedRunIds.length} Runs</CardTitle>
            <div className="flex items-center gap-4">
              <div className="flex items-center space-x-2">
                <Switch
                  id="normalize"
                  checked={normalizeToHundred}
                  onCheckedChange={setNormalizeToHundred}
                />
                <Label htmlFor="normalize">Normalize to 100</Label>
              </div>
              <div className="flex items-center space-x-2">
                <Switch
                  id="logscale"
                  checked={logScale}
                  onCheckedChange={setLogScale}
                />
                <Label htmlFor="logscale">Log Scale</Label>
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {selectedRunIds.map((runId, idx) => {
              const run = runsById.get(runId);
              const name = run?.run_name || runId;
              const summary = summaries[runId] ?? null;
              const sharpe = summary ? formatNumber(Number(summary.sharpe_ratio), 2) : '—';

              return (
              <div
                key={runId}
                className="flex items-center gap-2 px-3 py-1.5 border rounded-md"
              >
                <div
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: colors[idx % colors.length] }}
                />
                <span className="font-medium">{name}</span>
                <span className="text-sm text-muted-foreground">Sharpe: {sharpe}</span>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0"
                  onClick={() => removeFromCart(runId)}
                >
                  <X className="h-3 w-3" />
                </Button>
              </div>
              );
            })}
          </div>
        </CardContent>
      </Card>
      
      {/* Equity Curves */}
      <Card>
        <CardHeader>
          <CardTitle>Equity Curves</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={equityData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="date" 
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short', year: '2-digit' })}
                />
                <YAxis scale={logScale ? 'log' : 'linear'} tick={{ fontSize: 12 }} />
                <Tooltip 
                  content={({ active, payload, label }) => {
                    if (active && payload && payload.length) {
                      return (
                        <div className="bg-background border rounded-lg p-3 shadow-lg">
                          <p className="font-semibold mb-2">{new Date(label).toLocaleDateString()}</p>
                          {payload.map((entry: any, index: number) => {
                            const run = runsById.get(entry.dataKey);
                            return (
                              <p key={index} style={{ color: entry.color }}>
                                {run?.run_name || entry.dataKey}: {Number(entry.value).toFixed(2)}
                              </p>
                            );
                          })}
                        </div>
                      );
                    }
                    return null;
                  }}
                />
                <Legend />
                {selectedRunIds.map((runId, idx) => {
                  const run = runsById.get(runId);
                  return (
                  <Line
                    key={runId}
                    type="monotone"
                    dataKey={runId}
                    name={run?.run_name || runId}
                    stroke={colors[idx % colors.length]}
                    strokeWidth={2}
                    dot={false}
                  />
                  );
                })}
              </LineChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>
      
      {/* Drawdown Overlay */}
      <Card>
        <CardHeader>
          <CardTitle>Drawdown from Peak</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={drawdownData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                <XAxis 
                  dataKey="date"
                  tick={{ fontSize: 12 }}
                  tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short', year: '2-digit' })}
                />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                {selectedRunIds.map((runId, idx) => {
                  const run = runsById.get(runId);
                  return (
                  <Area
                    key={runId}
                    type="monotone"
                    dataKey={runId}
                    name={run?.run_name || runId}
                    stroke={colors[idx % colors.length]}
                    fill={colors[idx % colors.length]}
                    fillOpacity={0.3}
                  />
                  );
                })}
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>
      
      {/* Rolling Metrics */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Rolling Metrics</CardTitle>
            <Select value={rollingWindow} onValueChange={setRollingWindow}>
              <SelectTrigger className="w-32">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1m">1 Month</SelectItem>
                <SelectItem value="3m">3 Months</SelectItem>
                <SelectItem value="6m">6 Months</SelectItem>
                <SelectItem value="12m">12 Months</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {[
              { label: 'Rolling Sharpe', data: rollingSharpeData },
              { label: 'Rolling Volatility (%)', data: rollingVolData },
              { label: 'Rolling Return (%)', data: rollingReturnData },
              { label: 'Rolling Max Drawdown (%)', data: rollingMaxDDData },
            ].map((metric) => (
              <div key={metric}>
                <h4 className="text-sm font-medium mb-2">{metric.label}</h4>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={metric.data}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis 
                        dataKey="date" 
                        tick={{ fontSize: 10 }}
                        tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short' })}
                      />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip />
                      {selectedRunIds.map((runId, idx) => (
                        <Line
                          key={runId}
                          type="monotone"
                          dataKey={runId}
                          stroke={colors[idx % colors.length]}
                          strokeWidth={1.5}
                          dot={false}
                        />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
      
      {/* Summary Stats Comparison */}
      <Card>
        <CardHeader>
          <CardTitle>Performance Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left p-2">Metric</th>
                  {selectedRunIds.map((runId) => (
                    <th key={runId} className="text-right p-2">{runsById.get(runId)?.run_name || runId}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[
                  { label: 'Ann. Return', kind: 'summary', key: 'annualized_return', format: (v: any) => formatPercentDecimal(v, 1) },
                  { label: 'Ann. Volatility', kind: 'summary', key: 'annualized_volatility', format: (v: any) => formatPercentDecimal(v, 1) },
                  { label: 'Sharpe', kind: 'summary', key: 'sharpe_ratio', format: (v: any) => formatNumber(v, 2) },
                  { label: 'Max Drawdown', kind: 'summary', key: 'max_drawdown', format: (v: any) => formatPercentDecimal(v, 1) },
                  { label: 'Total Return', kind: 'summary', key: 'total_return', format: (v: any) => formatPercentDecimal(v, 1) },
                  { label: 'Trades', kind: 'summary', key: 'trades', format: (v: any) => formatNumber(v, 0) },
                  { label: 'Final Equity', kind: 'summary', key: 'final_equity', format: (v: any) => formatCurrency(v) },
                ].map((metric) => (
                  <tr key={metric.key} className="border-b hover:bg-muted/50">
                    <td className="p-2 font-medium">{metric.label}</td>
                    {selectedRunIds.map((runId) => {
                      const summary = summaries[runId] ?? null;
                      const raw = summary ? (summary as any)[metric.key] : null;
                      const value = raw === null || raw === undefined ? null : Number(raw);
                      return (
                        <td key={runId} className="text-right p-2 font-mono">
                          {summary ? metric.format(value) : '—'}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
      
      {/* Correlation Matrix */}
      <Card>
        <CardHeader>
          <CardTitle>Correlation Matrix</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th className="p-2"></th>
                  {selectedRunIds.map((runId) => (
                    <th key={runId} className="p-2 text-center text-xs">{runsById.get(runId)?.run_name || runId}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {selectedRunIds.map((runIdA, i) => (
                  <tr key={runIdA}>
                    <td className="p-2 text-xs font-medium">{runsById.get(runIdA)?.run_name || runIdA}</td>
                    {selectedRunIds.map((runIdB, j) => {
                      const corr = correlationMatrix[i][j];
                      const intensity = Math.abs(corr);
                      const color = corr > 0.7 ? 'bg-red-500' : corr > 0.4 ? 'bg-yellow-500' : 'bg-green-500';
                      
                      return (
                        <td key={runIdB} className="p-2 text-center">
                          <div
                            className={`${color} rounded px-2 py-1 text-white text-xs font-mono`}
                            style={{ opacity: 0.3 + intensity * 0.7 }}
                          >
                            {corr.toFixed(2)}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="text-xs text-muted-foreground mt-3">
            Correlations are computed from overlapping daily returns.
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
