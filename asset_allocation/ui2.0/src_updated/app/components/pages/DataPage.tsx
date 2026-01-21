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
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/app/components/ui/tabs';
import { CheckCircle2, AlertTriangle, Layers, ArrowRight } from 'lucide-react';

interface DataPageProps {
  onNavigate?: (page: string) => void;
}

export function DataPage({ onNavigate }: DataPageProps) {
  const [selectedStrategyId, setSelectedStrategyId] = useState(mockStrategies[0].id);
  const strategy = mockStrategies.find(s => s.id === selectedStrategyId) || mockStrategies[0];
  
  return (
    <div className="space-y-6">
      {/* Quick Link to Data Tiers */}
      <Card className="border-blue-200 bg-gradient-to-r from-blue-50 to-indigo-50">
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="p-3 rounded-lg bg-blue-100">
                <Layers className="h-6 w-6 text-blue-600" />
              </div>
              <div>
                <h3 className="font-semibold text-blue-900 mb-1">View Data Architecture</h3>
                <p className="text-sm text-blue-700">
                  Explore Bronze, Silver, Gold, and Platinum data tiers with sample data
                </p>
              </div>
            </div>
            <Button 
              className="bg-blue-600 hover:bg-blue-700"
              onClick={() => onNavigate?.('data-tiers')}
            >
              View Data Tiers
              <ArrowRight className="h-4 w-4 ml-2" />
            </Button>
          </div>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle>Data & Lineage</CardTitle>
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
          </div>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Comprehensive audit trail ensuring reproducibility and trust in backtest results.
          </p>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Audit Trail</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h4 className="font-semibold mb-3">Code Version</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Git SHA:</span>
                  <span className="font-mono">{strategy.audit.gitSha}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Repository:</span>
                  <span className="font-mono">quant-strategies/main</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Commit Date:</span>
                  <span className="font-mono">2025-01-15</span>
                </div>
              </div>
            </div>
            
            <div>
              <h4 className="font-semibold mb-3">Data Version</h4>
              <div className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Version ID:</span>
                  <span className="font-mono">{strategy.audit.dataVersionId}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Pricing DB:</span>
                  <span className="font-mono">US_Equities_v2024.12</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Corporate Actions:</span>
                  <span className="font-mono">CA_v2024.12.01</span>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Configuration</CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs defaultValue="summary">
            <TabsList>
              <TabsTrigger value="summary">Summary</TabsTrigger>
              <TabsTrigger value="json">Full JSON</TabsTrigger>
            </TabsList>
            
            <TabsContent value="summary" className="space-y-3 mt-4">
              <div className="grid grid-cols-2 gap-4 text-sm">
                <div>
                  <span className="text-muted-foreground">Universe:</span>
                  <span className="ml-2 font-medium">{strategy.config.universe}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Rebalance Frequency:</span>
                  <span className="ml-2 font-medium">{strategy.config.rebalance}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Position Type:</span>
                  <span className="ml-2 font-medium">{strategy.config.longOnly ? 'Long Only' : 'Long/Short'}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Top N Holdings:</span>
                  <span className="ml-2 font-medium">{strategy.config.topN}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Lookback Window:</span>
                  <span className="ml-2 font-medium">{strategy.config.lookbackWindow} days</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Holding Period:</span>
                  <span className="ml-2 font-medium">{strategy.config.holdingPeriod} days</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Cost Model:</span>
                  <span className="ml-2 font-medium">{strategy.config.costModel}</span>
                </div>
                <div>
                  <span className="text-muted-foreground">Config Hash:</span>
                  <span className="ml-2 font-mono text-xs">{strategy.audit.configHash}</span>
                </div>
              </div>
            </TabsContent>
            
            <TabsContent value="json" className="mt-4">
              <pre className="bg-muted p-4 rounded-lg overflow-x-auto text-xs">
{JSON.stringify({
  universe: strategy.config.universe,
  rebalance_frequency: strategy.config.rebalance,
  position_type: strategy.config.longOnly ? 'long_only' : 'long_short',
  top_n_holdings: strategy.config.topN,
  lookback_window_days: strategy.config.lookbackWindow,
  holding_period_days: strategy.config.holdingPeriod,
  cost_model: strategy.config.costModel,
  risk_model: 'barra_us_equity_v4',
  execution_model: 'vwap_slippage',
  start_date: strategy.startDate,
  end_date: strategy.endDate,
  initial_capital: 10000000,
  max_position_size: 0.05,
  max_leverage: 1.5
}, null, 2)}
              </pre>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Data Quality Checks</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {strategy.audit.warnings.length === 0 ? (
              <>
                <div className="flex items-center gap-2 text-sm">
                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                  <span>No missing price data</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                  <span>All corporate actions applied correctly</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                  <span>No impossible fills detected</span>
                </div>
                <div className="flex items-center gap-2 text-sm">
                  <CheckCircle2 className="h-4 w-4 text-green-500" />
                  <span>All delisted stocks handled properly</span>
                </div>
              </>
            ) : (
              strategy.audit.warnings.map((warning, idx) => (
                <div key={idx} className="flex items-center gap-2 text-sm">
                  <AlertTriangle className="h-4 w-4 text-orange-500" />
                  <span>{warning}</span>
                </div>
              ))
            )}
          </div>
        </CardContent>
      </Card>
      
      <Card>
        <CardHeader>
          <CardTitle>Run Metadata</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <span className="text-muted-foreground">Run ID:</span>
              <span className="ml-2 font-mono">{strategy.id}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Created At:</span>
              <span className="ml-2">{new Date(strategy.audit.createdAt).toLocaleString()}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Execution Time:</span>
              <span className="ml-2">2m 34s</span>
            </div>
            <div>
              <span className="text-muted-foreground">Engine Version:</span>
              <span className="ml-2 font-mono">v3.2.1</span>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}