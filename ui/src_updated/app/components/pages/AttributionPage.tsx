// Attribution Page - Performance decomposition

import { useState } from 'react';
import { mockStrategies } from '@/data/mockData';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import { BarChart, Bar, ResponsiveContainer, XAxis, YAxis, Tooltip, CartesianGrid, Cell } from 'recharts';

export function AttributionPage() {
  const [selectedStrategyId, setSelectedStrategyId] = useState(mockStrategies[0].id);
  const [groupBy, setGroupBy] = useState('symbol');
  
  const strategy = mockStrategies.find(s => s.id === selectedStrategyId) || mockStrategies[0];
  
  const contributorsData = strategy.contributions
    .filter(c => c.type === groupBy)
    .sort((a, b) => b.contribution - a.contribution);
  
  const topContributors = contributorsData.slice(0, 10);
  const topDetractors = contributorsData.slice(-5);
  
  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Attribution Analysis</CardTitle>
            <div className="flex gap-4">
              <Select value={selectedStrategyId} onValueChange={setSelectedStrategyId}>
                <SelectTrigger className="w-64">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {mockStrategies.map(s => (
                    <SelectItem key={s.id} value={s.id}>{s.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
              
              <Select value={groupBy} onValueChange={setGroupBy}>
                <SelectTrigger className="w-40">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="symbol">By Symbol</SelectItem>
                  <SelectItem value="sector">By Sector</SelectItem>
                  <SelectItem value="factor">By Factor</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardHeader>
      </Card>
      
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle>Top 10 Contributors</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topContributors} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis type="number" tick={{ fontSize: 12 }} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 12 }} width={80} />
                  <Tooltip />
                  <Bar dataKey="contribution" fill="#10b981" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader>
            <CardTitle>Top 5 Detractors</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-80">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={topDetractors} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                  <XAxis type="number" tick={{ fontSize: 12 }} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 12 }} width={80} />
                  <Tooltip />
                  <Bar dataKey="contribution" fill="#ef4444" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>
      
      <Card>
        <CardHeader>
          <CardTitle>Contribution Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left p-3">Name</th>
                  <th className="text-left p-3">Type</th>
                  <th className="text-right p-3">Contribution ($)</th>
                  <th className="text-right p-3">% of Total P&L</th>
                </tr>
              </thead>
              <tbody>
                {contributorsData.map(c => {
                  const totalPnL = contributorsData.reduce((sum, item) => sum + Math.abs(item.contribution), 0);
                  const pct = (c.contribution / totalPnL) * 100;
                  
                  return (
                    <tr key={c.name} className="border-b hover:bg-muted/50">
                      <td className="p-3 font-medium">{c.name}</td>
                      <td className="p-3 capitalize">{c.type}</td>
                      <td className={`text-right p-3 font-mono ${c.contribution > 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {c.contribution > 0 ? '+' : ''}{c.contribution.toLocaleString()}
                      </td>
                      <td className="text-right p-3 font-mono">{pct.toFixed(1)}%</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
