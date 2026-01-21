// Risk & Exposures Page

import { useState, useEffect } from 'react';
import { Strategy, StressEvent } from '../../../data/strategies';
import { StrategyService } from '../../../services/StrategyService';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import { LineChart, Line, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid, BarChart, Bar, Cell } from 'recharts';

export function RiskPage() {
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const [stressEvents, setStressEvents] = useState<StressEvent[]>([]);
  const [selectedStrategyId, setSelectedStrategyId] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const [strategiesData, stressData] = await Promise.all([
          StrategyService.getStrategies(),
          StrategyService.getStressEvents()
        ]);

        setStrategies(strategiesData);
        setStressEvents(stressData);

        if (strategiesData.length > 0) {
          // Preserve selection if possible, else default to first
          setSelectedStrategyId(prev => strategiesData.find(s => s.id === prev) ? prev : strategiesData[0].id);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  if (loading) return <div className="p-6">Loading risk analysis...</div>;
  if (error) return <div className="p-6 text-red-500">Error: {error}</div>;
  if (!strategies.length) return <div className="p-6">No strategies found.</div>;

  const strategy = strategies.find(s => s.id === selectedStrategyId) || strategies[0];

  const factorLoadings = [
    { factor: 'Value', loading: 0.25 },
    { factor: 'Momentum', loading: 0.68 },
    { factor: 'Size', loading: -0.12 },
    { factor: 'Quality', loading: 0.43 },
    { factor: 'Volatility', loading: -0.31 },
  ];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Risk & Exposures Analysis</CardTitle>
            <Select value={selectedStrategyId} onValueChange={setSelectedStrategyId}>
              <SelectTrigger className="w-64">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {strategies.map(s => (
                  <SelectItem key={s.id} value={s.id}>{s.name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardHeader>
      </Card>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Rolling Beta to Benchmark</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height={256}>
                <LineChart data={strategy.rollingMetrics.beta}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} />
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
            <CardTitle>Factor Loadings</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height={256}>
                <BarChart data={factorLoadings}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="factor" tick={{ fontSize: 12 }} />
                  <YAxis tick={{ fontSize: 12 }} />
                  <Tooltip />
                  <Bar dataKey="loading">
                    {factorLoadings.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.loading > 0 ? '#10b981' : '#ef4444'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Stress Test Results</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left p-3">Event</th>
                  <th className="text-left p-3">Date</th>
                  <th className="text-right p-3">Strategy Return</th>
                  <th className="text-right p-3">Benchmark Return</th>
                  <th className="text-right p-3">Relative</th>
                </tr>
              </thead>
              <tbody>
                {stressEvents.map(event => {
                  const relative = event.strategyReturn - event.benchmarkReturn;
                  return (
                    <tr key={event.name} className="border-b hover:bg-muted/50">
                      <td className="p-3 font-medium">{event.name}</td>
                      <td className="p-3 font-mono">{event.date}</td>
                      <td className={`text-right p-3 font-mono ${event.strategyReturn > 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {event.strategyReturn.toFixed(1)}%
                      </td>
                      <td className={`text-right p-3 font-mono ${event.benchmarkReturn > 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {event.benchmarkReturn.toFixed(1)}%
                      </td>
                      <td className={`text-right p-3 font-mono font-semibold ${relative > 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {relative > 0 ? '+' : ''}{relative.toFixed(1)}%
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Risk Metrics Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div>
              <div className="text-sm text-muted-foreground">Beta</div>
              <div className="text-2xl font-bold">{strategy.betaToBenchmark.toFixed(2)}</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground">Value at Risk (95%)</div>
              <div className="text-2xl font-bold text-red-500">-2.1%</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground">Up Capture</div>
              <div className="text-2xl font-bold text-green-500">112%</div>
            </div>
            <div>
              <div className="text-sm text-muted-foreground">Down Capture</div>
              <div className="text-2xl font-bold text-green-500">78%</div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}