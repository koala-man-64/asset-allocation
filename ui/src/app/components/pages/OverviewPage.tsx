// Overview Page - PM Scoreboard

import { useState } from 'react';
import { useUIStore } from '@/stores/useUIStore';
import { useStrategiesQuery } from '@/hooks/useDataQueries';
import { StrategyRun } from '@/types/strategy';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Checkbox } from '@/app/components/ui/checkbox';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/app/components/ui/table';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/app/components/ui/tooltip';
import { AlertTriangle, DollarSign, TrendingDown } from 'lucide-react';
import { LineChart, Line, ResponsiveContainer, ScatterChart, Scatter, XAxis, YAxis, Tooltip as RechartsTooltip, ZAxis } from 'recharts';
import { MetricTooltip, InfoTooltip } from '@/app/components/ui/metric-tooltip';
import { StrategyConfigModal } from '@/app/components/modals/StrategyConfigModal';

export function OverviewPage() {
  const { selectedRuns, addToCart, removeFromCart } = useUIStore();
  const { data: strategies = [], isLoading: loading, error } = useStrategiesQuery();
  const [selectedStrategy, setSelectedStrategy] = useState<StrategyRun | null>(null);
  const [configModalOpen, setConfigModalOpen] = useState(false);


  const handleViewConfig = (strategy: StrategyRun) => {
    setSelectedStrategy(strategy);
    setConfigModalOpen(true);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-muted-foreground">Loading strategies...</div>
      </div>
    );
  }

  if (strategies.length === 0) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-center">
          <p className="text-muted-foreground">No strategies found</p>
          {error && (
            <p className="text-xs text-muted-foreground mt-2">{(error as Error).message}</p>
          )}
        </div>
      </div>
    );
  }

  // Calculate best/worst
  const bestSharpe = strategies.reduce((best, s) => s.sharpe > best.sharpe ? s : best);
  const worstDD = strategies.reduce((worst, s) => s.maxDD < worst.maxDD ? s : worst);
  const highestTurnover = strategies.reduce((highest, s) => s.turnoverAnn > highest.turnoverAnn ? s : highest);

  const scatterData = strategies.map(s => ({
    name: s.name,
    sharpe: s.sharpe,
    maxDD: Math.abs(s.maxDD),
    cagr: s.cagr
  }));

  const handleCheckboxChange = (runId: string, checked: boolean) => {
    if (checked) {
      addToCart(runId);
    } else {
      removeFromCart(runId);
    }
  };

  return (
    <div className="space-y-8">
      {/* KPI Ribbon */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">Best Sharpe Ratio</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{bestSharpe.sharpe.toFixed(2)}</div>
            <p className="text-xs text-muted-foreground mt-1">{bestSharpe.name}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">Worst Max Drawdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-500">{worstDD.maxDD.toFixed(1)}%</div>
            <p className="text-xs text-muted-foreground mt-1">{worstDD.name}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
              <AlertTriangle className="h-4 w-4 text-orange-500" />
              Highest Turnover
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{highestTurnover.turnoverAnn.toFixed(0)}%</div>
            <p className="text-xs text-muted-foreground mt-1">{highestTurnover.name}</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">Total Strategies</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{strategies.length}</div>
            <p className="text-xs text-muted-foreground mt-1">Available for analysis</p>
          </CardContent>
        </Card>
      </div>

      {/* Strategy Table */}
      <Card>
        <CardHeader>
          <CardTitle>Strategy Universe</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-12"></TableHead>
                  <TableHead>Strategy Name</TableHead>
                  <TableHead>Tags</TableHead>
                  <TableHead className="text-right">
                    <MetricTooltip metric="cagr">CAGR</MetricTooltip>
                  </TableHead>
                  <TableHead className="text-right">
                    <MetricTooltip metric="volatility">Vol</MetricTooltip>
                  </TableHead>
                  <TableHead className="text-right">
                    <MetricTooltip metric="sharpe">Sharpe</MetricTooltip>
                  </TableHead>
                  <TableHead className="text-right">
                    <MetricTooltip metric="sortino">Sortino</MetricTooltip>
                  </TableHead>
                  <TableHead className="text-right">
                    <MetricTooltip metric="maxDrawdown">Max DD</MetricTooltip>
                  </TableHead>
                  <TableHead className="text-right">
                    <MetricTooltip metric="turnover">Turnover</MetricTooltip>
                  </TableHead>
                  <TableHead>Flags</TableHead>
                  <TableHead className="w-32">
                    <InfoTooltip content="12-Month Performance Trend" /> 12M Trend
                  </TableHead>
                  <TableHead className="w-24">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {strategies.map((strategy) => {
                  const recentData = strategy.equityCurve.slice(-252);
                  const chartData = recentData.map(d => ({ value: d.value }));

                  return (
                    <TableRow key={strategy.id} className="hover:bg-muted/50">
                      <TableCell>
                        <Checkbox
                          checked={selectedRuns.includes(strategy.id)}
                          onCheckedChange={(checked) => handleCheckboxChange(strategy.id, checked as boolean)}
                        />
                      </TableCell>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="font-medium cursor-help">{strategy.name}</TableCell>
                          </TooltipTrigger>
                          <TooltipContent side="right">
                            <div className="space-y-1">
                              <p className="font-semibold">{strategy.name}</p>
                              <p className="text-xs">Run ID: {strategy.id}</p>
                              <p className="text-xs">Git SHA: {strategy.audit.gitSha}</p>
                              <p className="text-xs">Data Version: {strategy.audit.dataVersionId}</p>
                            </div>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TableCell>
                        <div className="flex gap-1 flex-wrap">
                          {strategy.tags.slice(0, 2).map(tag => (
                            <Badge key={tag} variant="outline" className="text-xs">
                              {tag}
                            </Badge>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell className="text-right font-mono">{strategy.cagr.toFixed(1)}%</TableCell>
                      <TableCell className="text-right font-mono">{strategy.annVol.toFixed(1)}%</TableCell>
                      <TableCell className="text-right font-mono font-semibold">
                        {strategy.sharpe.toFixed(2)}
                      </TableCell>
                      <TableCell className="text-right font-mono">{strategy.sortino.toFixed(2)}</TableCell>
                      <TableCell className="text-right font-mono text-red-500">
                        {strategy.maxDD.toFixed(1)}%
                      </TableCell>
                      <TableCell className="text-right font-mono">{strategy.turnoverAnn.toFixed(0)}%</TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          {strategy.regimeFragility && (
                            <MetricTooltip metric="fragility" customContent="Regime Fragility: Performance varies across regimes">
                              <AlertTriangle className="h-4 w-4 text-orange-500" />
                            </MetricTooltip>
                          )}
                          {strategy.costSensitive && (
                            <MetricTooltip metric="costs" customContent="Cost Sensitive: High impact from transaction costs">
                              <DollarSign className="h-4 w-4 text-yellow-500" />
                            </MetricTooltip>
                          )}
                          {strategy.tailRisk && (
                            <MetricTooltip metric="tail" customContent="Tail Risk: Negative skew or fat tails">
                              <TrendingDown className="h-4 w-4 text-red-500" />
                            </MetricTooltip>
                          )}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div style={{ width: 120, height: 40 }}>
                          <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={chartData}>
                              <Line
                                type="monotone"
                                dataKey="value"
                                stroke="hsl(var(--primary))"
                                strokeWidth={1.5}
                                dot={false}
                              />
                            </LineChart>
                          </ResponsiveContainer>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Button
                          size="sm"
                          onClick={() => handleViewConfig(strategy)}
                        >
                          Config
                        </Button>
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
          <div style={{ height: 320, minHeight: 320 }}>
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
                      const data = payload[0].payload;
                      return (
                        <div className="bg-background border rounded-lg p-3 shadow-lg">
                          <p className="font-semibold">{data.name}</p>
                          <p className="text-sm">Sharpe: {data.sharpe.toFixed(2)}</p>
                          <p className="text-sm">Max DD: {data.maxDD.toFixed(1)}%</p>
                          <p className="text-sm">CAGR: {data.cagr.toFixed(1)}%</p>
                        </div>
                      );
                    }
                    return null;
                  }}
                />
                <Scatter data={scatterData} fill="hsl(var(--primary))" />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>

      {/* Strategy Config Modal */}
      <StrategyConfigModal
        open={configModalOpen}
        onClose={() => setConfigModalOpen(false)}
        strategy={selectedStrategy}
      />
    </div>
  );
}
