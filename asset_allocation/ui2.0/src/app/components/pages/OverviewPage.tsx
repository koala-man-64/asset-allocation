// Overview Page - PM Scoreboard

import { mockStrategies } from '@/data/mockData';
import { useApp } from '@/contexts/AppContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
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

export function OverviewPage() {
  const { selectedRuns, addToCart, removeFromCart } = useApp();
  
  // Calculate best/worst
  const bestSharpe = mockStrategies.reduce((best, s) => s.sharpe > best.sharpe ? s : best);
  const worstDD = mockStrategies.reduce((worst, s) => s.maxDD < worst.maxDD ? s : worst);
  const highestTurnover = mockStrategies.reduce((highest, s) => s.turnoverAnn > highest.turnoverAnn ? s : highest);
  
  const scatterData = mockStrategies.map(s => ({
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
    <div className="space-y-6">
      {/* KPI Ribbon */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
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
            <div className="text-2xl font-bold">{mockStrategies.length}</div>
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
                  <TableHead className="text-right">CAGR</TableHead>
                  <TableHead className="text-right">Vol</TableHead>
                  <TableHead className="text-right">Sharpe</TableHead>
                  <TableHead className="text-right">Sortino</TableHead>
                  <TableHead className="text-right">Max DD</TableHead>
                  <TableHead className="text-right">Turnover</TableHead>
                  <TableHead>Flags</TableHead>
                  <TableHead className="w-32">12M Trend</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {mockStrategies.map((strategy) => {
                  const recentData = strategy.equityCurve.slice(-252);
                  const chartData = recentData.map(d => ({ value: d.value }));
                  const maxDDPoint = strategy.drawdownCurve.reduce(
                    (worst, point) => (point.value < worst.value ? point : worst),
                    strategy.drawdownCurve[0],
                  );
                  
                  return (
                    <TableRow key={strategy.id} className="hover:bg-muted/50">
                      <TableCell>
                        <Checkbox
                          checked={selectedRuns.has(strategy.id)}
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
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="text-right font-mono cursor-help">{strategy.cagr.toFixed(1)}%</TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Compound Annual Growth Rate</p>
                            <p className="text-xs text-muted-foreground">Annualized return over backtest period</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="text-right font-mono cursor-help">{strategy.annVol.toFixed(1)}%</TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Annualized Volatility</p>
                            <p className="text-xs text-muted-foreground">Standard deviation of returns</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="text-right font-mono font-semibold cursor-help">
                              {strategy.sharpe.toFixed(2)}
                            </TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Sharpe Ratio</p>
                            <p className="text-xs text-muted-foreground">Risk-adjusted return (excess return / volatility)</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="text-right font-mono cursor-help">{strategy.sortino.toFixed(2)}</TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Sortino Ratio</p>
                            <p className="text-xs text-muted-foreground">Downside risk-adjusted return</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="text-right font-mono text-red-500 cursor-help">
                              {strategy.maxDD.toFixed(1)}%
                            </TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Maximum Drawdown</p>
                            <p className="text-xs text-muted-foreground">Largest peak-to-trough decline</p>
                            <p className="text-xs text-muted-foreground mt-1">Date: {maxDDPoint.date}</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="text-right font-mono cursor-help">{strategy.turnoverAnn.toFixed(0)}%</TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>Annual Turnover</p>
                            <p className="text-xs text-muted-foreground">Portfolio rebalancing frequency</p>
                            <p className="text-xs text-muted-foreground mt-1">Avg Daily: {(strategy.turnoverAnn / 252).toFixed(1)}%</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                      <TableCell>
                        <TooltipProvider>
                          <div className="flex gap-1">
                            {strategy.regimeFragility && (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <AlertTriangle className="h-4 w-4 text-orange-500 cursor-help" />
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p className="font-semibold">Regime Fragility</p>
                                  <p className="text-xs text-muted-foreground">Performance varies significantly across market regimes</p>
                                </TooltipContent>
                              </Tooltip>
                            )}
                            {strategy.costSensitive && (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <DollarSign className="h-4 w-4 text-yellow-500 cursor-help" />
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p className="font-semibold">Cost Sensitive</p>
                                  <p className="text-xs text-muted-foreground">Returns significantly impacted by transaction costs</p>
                                </TooltipContent>
                              </Tooltip>
                            )}
                            {strategy.tailRisk && (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <TrendingDown className="h-4 w-4 text-red-500 cursor-help" />
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p className="font-semibold">Tail Risk</p>
                                  <p className="text-xs text-muted-foreground">Exhibits negative skew or fat-tailed distribution</p>
                                </TooltipContent>
                              </Tooltip>
                            )}
                          </div>
                        </TooltipProvider>
                      </TableCell>
                      <TooltipProvider>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <TableCell className="cursor-help">
                              <ResponsiveContainer width="100%" height={40}>
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
                            </TableCell>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p>12-Month Performance Trend</p>
                            <p className="text-xs text-muted-foreground">Recent equity curve (last 252 trading days)</p>
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
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
    </div>
  );
}
