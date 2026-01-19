// Run Compare Page - The Workbench for side-by-side strategy evaluation

import { useState } from 'react';
import { mockStrategies } from '@/data/mockData';
import { useApp } from '@/contexts/AppContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
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
  const [grossReturns, setGrossReturns] = useState(false);
  const [rollingWindow, setRollingWindow] = useState('3m');
  
  const selectedStrategies = mockStrategies.filter(s => selectedRuns.has(s.id));
  
  if (selectedStrategies.length === 0) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-center">
          <p className="text-lg text-muted-foreground">No strategies selected for comparison</p>
          <p className="text-sm text-muted-foreground mt-2">Add strategies from the Overview page</p>
        </div>
      </div>
    );
  }
  
  // Prepare equity curve data
  const equityData = selectedStrategies[0].equityCurve.map((point, idx) => {
    const dataPoint: any = { date: point.date };
    selectedStrategies.forEach((strategy, stratIdx) => {
      const value = strategy.equityCurve[idx]?.value || 0;
      dataPoint[strategy.id] = normalizeToHundred ? (value / strategy.equityCurve[0].value) * 100 : value;
    });
    return dataPoint;
  });
  
  // Prepare drawdown data
  const drawdownData = selectedStrategies[0].drawdownCurve.map((point, idx) => {
    const dataPoint: any = { date: point.date };
    selectedStrategies.forEach((strategy, stratIdx) => {
      dataPoint[strategy.id] = strategy.drawdownCurve[idx]?.value || 0;
    });
    return dataPoint;
  });
  
  // Prepare rolling metrics data (using Sharpe as example)
  const rollingData = selectedStrategies[0].rollingMetrics.sharpe.map((point, idx) => {
    const dataPoint: any = { date: point.date };
    selectedStrategies.forEach((strategy, stratIdx) => {
      dataPoint[strategy.id] = strategy.rollingMetrics.sharpe[idx]?.value || 0;
    });
    return dataPoint;
  });
  
  // Calculate correlation matrix
  const correlationMatrix = selectedStrategies.map((s1, i) => 
    selectedStrategies.map((s2, j) => {
      if (i === j) return 1.0;
      // Simplified correlation calculation
      return 0.3 + Math.random() * 0.4;
    })
  );
  
  return (
    <div className="space-y-6">
      {/* Controls & Legend */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Comparing {selectedStrategies.length} Strategies</CardTitle>
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
              <div className="flex items-center space-x-2">
                <Switch
                  id="gross"
                  checked={grossReturns}
                  onCheckedChange={setGrossReturns}
                />
                <Label htmlFor="gross">Gross Returns</Label>
              </div>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {selectedStrategies.map((strategy, idx) => (
              <div
                key={strategy.id}
                className="flex items-center gap-2 px-3 py-1.5 border rounded-md"
              >
                <div
                  className="w-3 h-3 rounded-full"
                  style={{ backgroundColor: colors[idx % colors.length] }}
                />
                <span className="font-medium">{strategy.name}</span>
                <span className="text-sm text-muted-foreground">
                  Sharpe: {strategy.sharpe.toFixed(2)}
                </span>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-6 w-6 p-0"
                  onClick={() => removeFromCart(strategy.id)}
                >
                  <X className="h-3 w-3" />
                </Button>
              </div>
            ))}
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
                            const strategy = selectedStrategies.find(s => s.id === entry.dataKey);
                            return (
                              <p key={index} style={{ color: entry.color }}>
                                {strategy?.name}: {entry.value.toFixed(2)}
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
                {selectedStrategies.map((strategy, idx) => (
                  <Line
                    key={strategy.id}
                    type="monotone"
                    dataKey={strategy.id}
                    name={strategy.name}
                    stroke={colors[idx % colors.length]}
                    strokeWidth={2}
                    dot={false}
                  />
                ))}
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
                {selectedStrategies.map((strategy, idx) => (
                  <Area
                    key={strategy.id}
                    type="monotone"
                    dataKey={strategy.id}
                    name={strategy.name}
                    stroke={colors[idx % colors.length]}
                    fill={colors[idx % colors.length]}
                    fillOpacity={0.3}
                  />
                ))}
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
            {['Rolling Sharpe', 'Rolling Volatility', 'Rolling Beta', 'Rolling Correlation'].map((metric) => (
              <div key={metric}>
                <h4 className="text-sm font-medium mb-2">{metric}</h4>
                <div className="h-48">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={rollingData}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis 
                        dataKey="date" 
                        tick={{ fontSize: 10 }}
                        tickFormatter={(value) => new Date(value).toLocaleDateString('en-US', { month: 'short' })}
                      />
                      <YAxis tick={{ fontSize: 10 }} />
                      <Tooltip />
                      {selectedStrategies.map((strategy, idx) => (
                        <Line
                          key={strategy.id}
                          type="monotone"
                          dataKey={strategy.id}
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
                  {selectedStrategies.map(s => (
                    <th key={s.id} className="text-right p-2">{s.name}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {[
                  { label: 'CAGR', key: 'cagr', suffix: '%' },
                  { label: 'Volatility', key: 'annVol', suffix: '%' },
                  { label: 'Sharpe', key: 'sharpe', suffix: '' },
                  { label: 'Sortino', key: 'sortino', suffix: '' },
                  { label: 'Calmar', key: 'calmar', suffix: '' },
                  { label: 'Max DD', key: 'maxDD', suffix: '%' },
                  { label: 'Recovery (days)', key: 'timeToRecovery', suffix: '' },
                  { label: 'Turnover', key: 'turnoverAnn', suffix: '%' },
                ].map(metric => (
                  <tr key={metric.key} className="border-b hover:bg-muted/50">
                    <td className="p-2 font-medium">{metric.label}</td>
                    {selectedStrategies.map(s => (
                      <td key={s.id} className="text-right p-2 font-mono">
                        {(s as any)[metric.key].toFixed(2)}{metric.suffix}
                      </td>
                    ))}
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
                  {selectedStrategies.map(s => (
                    <th key={s.id} className="p-2 text-center text-xs">{s.name}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {selectedStrategies.map((s1, i) => (
                  <tr key={s1.id}>
                    <td className="p-2 text-xs font-medium">{s1.name}</td>
                    {selectedStrategies.map((s2, j) => {
                      const corr = correlationMatrix[i][j];
                      const intensity = Math.abs(corr);
                      const color = corr > 0.7 ? 'bg-red-500' : corr > 0.4 ? 'bg-yellow-500' : 'bg-green-500';
                      
                      return (
                        <td key={s2.id} className="p-2 text-center">
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
        </CardContent>
      </Card>
    </div>
  );
}
