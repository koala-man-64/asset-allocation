// Signal Monitor Page - Hot List of Strongest Current Signals

import { useState, useEffect } from 'react';
import { getSignals } from '@/data/dataProvider';
import { TradingSignal } from '@/types/strategy';
import { useApp } from '@/contexts/AppContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/app/components/ui/table';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import { Input } from '@/app/components/ui/input';
import { 
  TrendingUp, 
  TrendingDown, 
  X, 
  ArrowUpRight, 
  ArrowDownRight,
  Search,
  Filter,
  RefreshCw,
  Zap
} from 'lucide-react';

export function SignalMonitorPage() {
  const { dataSource } = useApp();
  const [signals, setSignals] = useState<TradingSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [signalTypeFilter, setSignalTypeFilter] = useState<string>('all');
  const [strengthFilter, setStrengthFilter] = useState<string>('all');
  
  // Load signals based on data source
  useEffect(() => {
    loadSignals();
  }, [dataSource]);
  
  async function loadSignals() {
    setLoading(true);
    const data = await getSignals(dataSource);
    setSignals(data);
    setLoading(false);
  }
  
  // Filter signals
  const filteredSignals = signals.filter(signal => {
    // Search filter
    const matchesSearch = searchTerm === '' || 
      signal.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
      signal.strategyName.toLowerCase().includes(searchTerm.toLowerCase()) ||
      signal.sector.toLowerCase().includes(searchTerm.toLowerCase());
    
    // Signal type filter
    const matchesType = signalTypeFilter === 'all' || signal.signalType === signalTypeFilter;
    
    // Strength filter
    const matchesStrength = strengthFilter === 'all' ||
      (strengthFilter === 'high' && signal.strength >= 85) ||
      (strengthFilter === 'medium' && signal.strength >= 70 && signal.strength < 85) ||
      (strengthFilter === 'low' && signal.strength < 70);
    
    return matchesSearch && matchesType && matchesStrength;
  });
  
  // Calculate summary stats
  const buySignals = filteredSignals.filter(s => s.signalType === 'BUY').length;
  const sellSignals = filteredSignals.filter(s => s.signalType === 'SELL').length;
  const exitSignals = filteredSignals.filter(s => s.signalType === 'EXIT').length;
  const avgStrength = filteredSignals.length > 0
    ? Math.round(filteredSignals.reduce((sum, s) => sum + s.strength, 0) / filteredSignals.length)
    : 0;
  
  // Format time ago
  function formatTimeAgo(isoString: string): string {
    const date = new Date(isoString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  }
  
  // Get badge variant based on signal type
  function getSignalBadgeVariant(type: string) {
    switch (type) {
      case 'BUY': return 'default';
      case 'SELL': return 'destructive';
      case 'EXIT': return 'secondary';
      default: return 'outline';
    }
  }
  
  // Get strength badge color
  function getStrengthBadge(strength: number) {
    if (strength >= 85) {
      return <Badge className="bg-green-600 text-white">High: {strength}</Badge>;
    } else if (strength >= 70) {
      return <Badge className="bg-blue-600 text-white">Med: {strength}</Badge>;
    } else {
      return <Badge variant="outline">Low: {strength}</Badge>;
    }
  }
  
  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-muted-foreground">Loading signals...</div>
      </div>
    );
  }
  
  return (
    <div className="space-y-8">
      <div>
        <div className="flex items-center gap-3 mb-2">
          <Zap className="h-7 w-7 text-yellow-600" />
          <h1>Signal Monitor</h1>
        </div>
        <p className="text-muted-foreground">
          Real-time trading signals from all active strategies, ranked by confidence strength
        </p>
      </div>
      
      {/* Summary Cards */}
      <div className="grid grid-cols-4 gap-6">
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Total Signals
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-semibold">{filteredSignals.length}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Active opportunities
            </p>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Buy Signals
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <div className="text-3xl font-semibold text-green-600">{buySignals}</div>
              <TrendingUp className="h-5 w-5 text-green-600" />
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              Long opportunities
            </p>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Sell Signals
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-baseline gap-2">
              <div className="text-3xl font-semibold text-red-600">{sellSignals}</div>
              <TrendingDown className="h-5 w-5 text-red-600" />
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              Short opportunities
            </p>
          </CardContent>
        </Card>
        
        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Avg Strength
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="text-3xl font-semibold">{avgStrength}</div>
            <p className="text-xs text-muted-foreground mt-1">
              Confidence score
            </p>
          </CardContent>
        </Card>
      </div>
      
      {/* Filters */}
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle className="flex items-center gap-2">
              <Filter className="h-5 w-5" />
              Filters & Search
            </CardTitle>
            <Button 
              variant="outline" 
              size="sm"
              onClick={loadSignals}
              className="gap-2"
            >
              <RefreshCw className="h-4 w-4" />
              Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-4 gap-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search symbol, strategy, sector..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9"
              />
            </div>
            
            <Select value={signalTypeFilter} onValueChange={setSignalTypeFilter}>
              <SelectTrigger>
                <SelectValue placeholder="Signal Type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Types</SelectItem>
                <SelectItem value="BUY">Buy Only</SelectItem>
                <SelectItem value="SELL">Sell Only</SelectItem>
                <SelectItem value="EXIT">Exit Only</SelectItem>
              </SelectContent>
            </Select>
            
            <Select value={strengthFilter} onValueChange={setStrengthFilter}>
              <SelectTrigger>
                <SelectValue placeholder="Strength" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Strengths</SelectItem>
                <SelectItem value="high">High (85+)</SelectItem>
                <SelectItem value="medium">Medium (70-84)</SelectItem>
                <SelectItem value="low">Low (&lt;70)</SelectItem>
              </SelectContent>
            </Select>
            
            {(searchTerm || signalTypeFilter !== 'all' || strengthFilter !== 'all') && (
              <Button
                variant="ghost"
                onClick={() => {
                  setSearchTerm('');
                  setSignalTypeFilter('all');
                  setStrengthFilter('all');
                }}
                className="gap-2"
              >
                <X className="h-4 w-4" />
                Clear Filters
              </Button>
            )}
          </div>
        </CardContent>
      </Card>
      
      {/* Signals Table */}
      <Card>
        <CardHeader>
          <CardTitle>Active Signals ({filteredSignals.length})</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="border rounded-lg">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[100px]">Strength</TableHead>
                  <TableHead className="w-[80px]">Type</TableHead>
                  <TableHead>Symbol</TableHead>
                  <TableHead>Sector</TableHead>
                  <TableHead>Strategy</TableHead>
                  <TableHead className="text-right">Price</TableHead>
                  <TableHead className="text-right">24h Chg</TableHead>
                  <TableHead className="text-right">Exp. Return</TableHead>
                  <TableHead className="text-right">Target</TableHead>
                  <TableHead className="text-right">Stop Loss</TableHead>
                  <TableHead>Horizon</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                  <TableHead>Catalysts</TableHead>
                  <TableHead className="text-right">Generated</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredSignals.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={14} className="text-center text-muted-foreground py-8">
                      No signals found matching your filters
                    </TableCell>
                  </TableRow>
                ) : (
                  filteredSignals.map((signal) => (
                    <TableRow key={signal.id} className="hover:bg-muted/50">
                      <TableCell>
                        {getStrengthBadge(signal.strength)}
                      </TableCell>
                      <TableCell>
                        <Badge variant={getSignalBadgeVariant(signal.signalType)}>
                          {signal.signalType}
                        </Badge>
                      </TableCell>
                      <TableCell className="font-semibold">
                        {signal.symbol}
                      </TableCell>
                      <TableCell>
                        <span className="text-sm text-muted-foreground">
                          {signal.sector}
                        </span>
                      </TableCell>
                      <TableCell>
                        <span className="text-sm">
                          {signal.strategyName}
                        </span>
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        ${signal.currentPrice.toFixed(2)}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className={`flex items-center justify-end gap-1 ${
                          signal.priceChange24h > 0 ? 'text-green-600' : 'text-red-600'
                        }`}>
                          {signal.priceChange24h > 0 ? (
                            <ArrowUpRight className="h-3 w-3" />
                          ) : (
                            <ArrowDownRight className="h-3 w-3" />
                          )}
                          <span className="font-mono text-sm">
                            {signal.priceChange24h > 0 ? '+' : ''}
                            {signal.priceChange24h.toFixed(2)}%
                          </span>
                        </div>
                      </TableCell>
                      <TableCell className="text-right">
                        <span className={`font-mono font-semibold ${
                          signal.expectedReturn > 0 ? 'text-green-600' : 'text-red-600'
                        }`}>
                          {signal.expectedReturn > 0 ? '+' : ''}
                          {signal.expectedReturn.toFixed(2)}%
                        </span>
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {signal.targetPrice ? `$${signal.targetPrice.toFixed(2)}` : '-'}
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {signal.stopLoss ? `$${signal.stopLoss.toFixed(2)}` : '-'}
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline" className="font-mono">
                          {signal.timeHorizon}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        <span className="text-sm font-mono">
                          {signal.positionSize.toFixed(1)}%
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1 max-w-[200px]">
                          {signal.catalysts.map((catalyst, idx) => (
                            <Badge
                              key={idx}
                              variant="secondary"
                              className="text-xs"
                            >
                              {catalyst}
                            </Badge>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell className="text-right text-sm text-muted-foreground">
                        {formatTimeAgo(signal.generatedAt)}
                      </TableCell>
                    </TableRow>
                  ))
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
