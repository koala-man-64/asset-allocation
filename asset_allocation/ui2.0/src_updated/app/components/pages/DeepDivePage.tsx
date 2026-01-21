// Single Run Deep Dive Page - The definitive tear sheet

import { useState } from 'react';
import { mockStrategies, getTopDrawdowns } from '@/data/mockData';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/app/components/ui/tabs';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/app/components/ui/table';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Cell
} from 'recharts';
import { Copy, Download, RotateCcw, TrendingUp, TrendingDown } from 'lucide-react';

export function DeepDivePage() {
  const [selectedStrategyId, setSelectedStrategyId] = useState(mockStrategies[0].id);
  
  const strategy = mockStrategies.find(s => s.id === selectedStrategyId) || mockStrategies[0];
  const topDrawdowns = getTopDrawdowns(strategy);
  
  // Prepare monthly returns heatmap data
  const heatmapData = strategy.monthlyReturns;
  const years = Array.from(new Set(heatmapData.map(d => d.year))).sort();
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  
  // Return distribution for histogram
  const returns = strategy.equityCurve.map((p, i) => {
    if (i === 0) return 0;
    return ((p.value - strategy.equityCurve[i - 1].value) / strategy.equityCurve[i - 1].value) * 100;
  });
  
  const histogram = Array.from({ length: 20 }, (_, i) => {
    const min = -5 + i * 0.5;
    const max = min + 0.5;
    const count = returns.filter(r => r >= min && r < max).length;
    return { range: `${min.toFixed(1)}`, count };
  });
  
  return (
    <div className="space-y-6">
      {/* Strategy Selector */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="flex items-center gap-3">
                <Select value={selectedStrategyId} onValueChange={setSelectedStrategyId}>
                  <SelectTrigger className="w-80">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {mockStrategies.map(s => (
                      <SelectItem key={s.id} value={s.id}>
                        {s.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Badge>{strategy.id}</Badge>
                {strategy.tags.map(tag => (
                  <Badge key={tag} variant="outline">{tag}</Badge>
                ))}
              </CardTitle>
              <div className="text-sm text-muted-foreground mt-2">
                Config: {strategy.config.universe} | Rebal: {strategy.config.rebalance} | {strategy.config.longOnly ? 'Long Only' : 'Long/Short'} | Top {strategy.config.topN} names
              </div>
            </div>
            <div className="flex gap-2">
              <Button variant="outline" size="sm">
                <Copy className="h-4 w-4 mr-2" />
                Add to Cart
              </Button>
              <Button variant="outline" size="sm">
                <RotateCcw className="h-4 w-4 mr-2" />
                Rerun
              </Button>
              <Button variant="outline" size="sm">
                <Download className="h-4 w-4 mr-2" />
                Download
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-4 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Git SHA:</span>
              <span className="ml-2 font-mono">{strategy.audit.gitSha}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Data Version:</span>
              <span className="ml-2 font-mono">{strategy.audit.dataVersionId}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Config Hash:</span>
              <span className="ml-2 font-mono">{strategy.audit.configHash}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Created:</span>
              <span className="ml-2">{new Date(strategy.audit.createdAt).toLocaleDateString()}</span>
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
            <div style={{ height: 256, minHeight: 256 }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={strategy.equityCurve}>
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
                <AreaChart data={strategy.drawdownCurve}>
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
                  {months.map(m => (
                    <th key={m} className="p-2 text-center">{m}</th>
                  ))}
                  <th className="p-2 text-center font-semibold">YTD</th>
                </tr>
              </thead>
              <tbody>
                {years.map(year => {
                  const yearReturns = heatmapData.filter(d => d.year === year);
                  const ytd = yearReturns.reduce((sum, d) => sum + d.return, 0);
                  
                  return (
                    <tr key={year} className="border-b">
                      <td className="p-2 font-semibold">{year}</td>
                      {months.map((m, idx) => {
                        const monthData = yearReturns.find(d => d.month === idx + 1);
                        const ret = monthData?.return || 0;
                        const color = ret > 2 ? 'bg-green-600' : ret > 0 ? 'bg-green-400' : ret > -2 ? 'bg-red-400' : 'bg-red-600';
                        
                        return (
                          <td key={m} className="p-1">
                            <div className={`${color} text-white rounded px-2 py-1 text-center`}>
                              {monthData ? ret.toFixed(1) : '-'}
                            </div>
                          </td>
                        );
                      })}
                      <td className="p-1">
                        <div className={`${ytd > 0 ? 'bg-green-600' : 'bg-red-600'} text-white rounded px-2 py-1 text-center font-semibold`}>
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
                      <span className="text-muted-foreground">CAGR</span>
                      <span className="font-mono">{strategy.cagr.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Annual Volatility</span>
                      <span className="font-mono">{strategy.annVol.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Sharpe Ratio</span>
                      <span className="font-mono font-semibold">{strategy.sharpe.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Sortino Ratio</span>
                      <span className="font-mono">{strategy.sortino.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Calmar Ratio</span>
                      <span className="font-mono">{strategy.calmar.toFixed(2)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Skewness</span>
                      <span className="font-mono">-0.23</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Kurtosis</span>
                      <span className="font-mono">3.45</span>
                    </div>
                  </div>
                </div>
                
                <div>
                  <h4 className="font-semibold mb-3">Return Distribution</h4>
                  <div style={{ height: 256, minHeight: 256 }}>
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
                        <td className="text-right p-2 font-mono text-red-500">{dd.depth.toFixed(2)}%</td>
                        <td className="text-right p-2 font-mono">{dd.duration}</td>
                        <td className="text-right p-2 font-mono">{dd.recovery || '-'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              
              <div>
                <h4 className="font-semibold mb-3">Underwater Plot</h4>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={strategy.drawdownCurve}>
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
                      <span className="font-mono">54.2%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Weekly Win Rate</span>
                      <span className="font-mono">58.7%</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Monthly Win Rate</span>
                      <span className="font-mono">66.7%</span>
                    </div>
                  </div>
                </div>
                
                <div>
                  <h4 className="font-semibold mb-3">Rolling 6M Sharpe</h4>
                  <div className="h-48">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={strategy.rollingMetrics.sharpe}>
                        <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                        <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                        <YAxis tick={{ fontSize: 10 }} />
                        <Tooltip />
                        <Line type="monotone" dataKey="value" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
      
      {/* Trade History */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Trade History</CardTitle>
            <div className="flex items-center gap-4 text-sm">
              <div className="text-muted-foreground">
                Total Trades: <span className="font-mono font-semibold text-foreground">{strategy.trades.length}</span>
              </div>
              <div className="text-muted-foreground">
                Buys: <span className="font-mono font-semibold text-green-600">
                  {strategy.trades.filter(t => t.side === 'BUY').length}
                </span>
              </div>
              <div className="text-muted-foreground">
                Sells: <span className="font-mono font-semibold text-red-600">
                  {strategy.trades.filter(t => t.side === 'SELL').length}
                </span>
              </div>
              <Button variant="outline" size="sm">
                <Download className="h-4 w-4 mr-2" />
                Export CSV
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent>
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
                  {strategy.trades.slice(-100).reverse().map((trade, idx) => {
                    const notional = trade.shares * trade.price;
                    const totalCost = trade.commission + trade.slippage;
                    const hasPnL = trade.pnl !== undefined;
                    
                    return (
                      <TableRow key={idx} className="hover:bg-muted/50">
                        <TableCell className="font-mono text-xs">{trade.date}</TableCell>
                        <TableCell>
                          <Badge 
                            variant={trade.side === 'BUY' ? 'default' : 'destructive'}
                            className="font-mono text-xs"
                          >
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
                        <TableCell className="text-right font-mono">${notional.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          ${trade.commission.toFixed(2)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-muted-foreground">
                          ${trade.slippage.toFixed(2)}
                        </TableCell>
                        <TableCell className="text-right font-mono font-semibold text-red-500">
                          ${totalCost.toFixed(2)}
                        </TableCell>
                        <TableCell className={`text-right font-mono font-semibold ${
                          !hasPnL ? 'text-muted-foreground' : 
                          trade.pnl! > 0 ? 'text-green-600' : 'text-red-600'
                        }`}>
                          {hasPnL ? `$${trade.pnl!.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '-'}
                        </TableCell>
                        <TableCell className={`text-right font-mono font-semibold ${
                          !hasPnL ? 'text-muted-foreground' : 
                          trade.pnlPercent! > 0 ? 'text-green-600' : 'text-red-600'
                        }`}>
                          {hasPnL ? `${trade.pnlPercent!.toFixed(2)}%` : '-'}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          </div>
          <div className="mt-4 text-xs text-muted-foreground">
            Showing last 100 trades (most recent first). P&L calculated for closed positions (SELL trades). Download CSV for complete history.
          </div>
        </CardContent>
      </Card>
    </div>
  );
}