// Execution & Costs Page

import { useState, useEffect } from 'react';
import { DataService } from '@/services/DataService';
import { StrategyRun } from '@/types/strategy';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import { LineChart, Line, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid, PieChart, Pie, Cell } from 'recharts';

export function ExecutionPage() {
  const [strategies, setStrategies] = useState<StrategyRun[]>([]);
  const [selectedStrategyId, setSelectedStrategyId] = useState<string>('');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      setLoading(true);
      try {
        const data = await DataService.getStrategies();
        setStrategies(data);
        if (data.length > 0) {
          setSelectedStrategyId(data[0].id);
        }
      } catch (error) {
        console.error("Failed to load strategies for execution analysis:", error);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  const strategy = strategies.find(s => s.id === selectedStrategyId) || strategies[0];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-muted-foreground">Loading execution analysis...</div>
      </div>
    );
  }

  if (!strategy) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-muted-foreground">No strategy data available.</div>
      </div>
    );
  }

  const costBreakdown = [
    { name: 'Commissions', value: 35, color: '#3b82f6' },
    { name: 'Slippage', value: 45, color: '#10b981' },
    { name: 'Financing', value: 20, color: '#f59e0b' },
  ];

  const grossNetData = strategy.equityCurve.map((point, idx) => ({
    date: point.date,
    gross: point.value,
    net: point.value * 0.97 // Simplified cost drag
  }));

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Execution & Cost Analysis</CardTitle>
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

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Annualized Turnover</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">{strategy.turnoverAnn.toFixed(0)}%</div>
            <p className="text-sm text-muted-foreground mt-2">
              {strategy.turnoverAnn > 300 ? 'High frequency' : strategy.turnoverAnn > 100 ? 'Moderate' : 'Low turnover'}
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Total Cost Drag</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold text-red-500">-312 bps</div>
            <p className="text-sm text-muted-foreground mt-2">Annual impact on returns</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">Avg Holding Period</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-bold">8.5 days</div>
            <p className="text-sm text-muted-foreground mt-2">Median position duration</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Gross vs Net Returns</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={grossNetData}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Line type="monotone" dataKey="gross" name="Gross" stroke="#10b981" strokeWidth={2} dot={false} />
                  <Line type="monotone" dataKey="net" name="Net" stroke="#ef4444" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Cost Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64 flex items-center justify-center">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={costBreakdown}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {costBreakdown.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Turnover Over Time</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={strategy.rollingMetrics.turnover}>
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
    </div>
  );
}
