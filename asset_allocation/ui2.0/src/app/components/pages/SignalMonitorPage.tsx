import { useState } from 'react';
import { List as VirtualList } from 'react-window';
// @ts-ignore: Runtime requires named import, but TS definitions are conflicting
import { AutoSizer as Sizer } from 'react-virtualized-auto-sizer';

import { DataService } from '@/services/DataService';
import { TradingSignal } from '@/types/strategy';
import { useApp } from '@/contexts/AppContext';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
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
    ArrowUpRight,
    ArrowDownRight,
    Filter,
    RefreshCw,
    Zap
} from 'lucide-react';

import { useSignalsQuery } from '@/hooks/useDataQueries';

export function SignalMonitorPage() {
    const { dataSource } = useApp();
    const { data: signals = [], isLoading: loading, refetch } = useSignalsQuery();

    const [searchTerm, setSearchTerm] = useState('');
    const [signalTypeFilter, setSignalTypeFilter] = useState<string>('all');
    const [strengthFilter, setStrengthFilter] = useState<string>('all');

    // Filter signals
    const filteredSignals = signals.filter(signal => {
        // Search filter
        const matchesSearch = searchTerm === '' ||
            (signal.symbol || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
            (signal.strategyName || '').toLowerCase().includes(searchTerm.toLowerCase()) ||
            (signal.sector || '').toLowerCase().includes(searchTerm.toLowerCase());

        // Signal type filter
        const matchesType = signalTypeFilter === 'all' || signal.signalType === signalTypeFilter;

        // Strength filter
        const strength = signal.strength ?? 0;
        const matchesStrength = strengthFilter === 'all' ||
            (strengthFilter === 'high' && strength >= 85) ||
            (strengthFilter === 'medium' && strength >= 70 && strength < 85) ||
            (strengthFilter === 'low' && strength < 70);

        return matchesSearch && matchesType && matchesStrength;
    });

    // Calculate summary stats
    const buySignals = filteredSignals.filter(s => s.signalType === 'BUY').length;
    const sellSignals = filteredSignals.filter(s => s.signalType === 'SELL').length;
    // Unused: const exitSignals = filteredSignals.filter(s => s.signalType === 'EXIT').length;
    const avgStrength = filteredSignals.length > 0
        ? Math.round(filteredSignals.reduce((sum, s) => sum + (s.strength ?? 0), 0) / filteredSignals.length)
        : 0;

    // Format time ago
    function formatTimeAgo(isoString?: string): string {
        if (!isoString) return '-';
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
    function getSignalBadgeVariant(type?: string) {
        switch (type) {
            case 'BUY': return 'default';
            case 'SELL': return 'destructive';
            case 'EXIT': return 'secondary';
            default: return 'outline';
        }
    }

    // Get strength badge color
    function getStrengthBadge(strength?: number) {
        const val = strength ?? 0;
        if (val >= 85) {
            return <Badge className="bg-green-600 text-white">High: {val}</Badge>;
        } else if (val >= 70) {
            return <Badge className="bg-blue-600 text-white">Med: {val}</Badge>;
        } else {
            return <Badge variant="outline">Low: {val}</Badge>;
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
                            onClick={() => refetch()}
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
                            {/* <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" /> */}
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
                                {/* <X className="h-4 w-4" /> */}
                                Clear Filters
                            </Button>
                        )}
                    </div>
                </CardContent>
            </Card>

            {/* Signals Table - Virtualized */}
            <Card className="flex-1 flex flex-col min-h-[500px]">
                <CardHeader>
                    <CardTitle>Active Signals ({filteredSignals.length})</CardTitle>
                </CardHeader>
                <CardContent className="flex-1 p-0">
                    <div className="h-[600px] w-full flex flex-col">
                        {/* Header Row */}
                        <div className="flex border-b bg-muted/50 font-medium text-xs px-4 py-3">
                            <div className="w-[100px] shrink-0">Strength</div>
                            <div className="w-[80px] shrink-0">Type</div>
                            <div className="w-[80px] shrink-0">Symbol</div>
                            <div className="flex-1 min-w-[100px]">Strategy</div>
                            <div className="w-[120px] shrink-0">Sector</div>
                            <div className="w-[80px] text-right shrink-0">Price</div>
                            <div className="w-[80px] text-right shrink-0">24h Chg</div>
                            <div className="w-[80px] text-right shrink-0">Exp. Ret</div>
                            <div className="w-[80px] text-right shrink-0">Target</div>
                            <div className="w-[80px] text-right shrink-0">Stop</div>
                            <div className="w-[80px] shrink-0">Horizon</div>
                            <div className="w-[60px] text-right shrink-0">Size</div>
                            <div className="w-[150px] shrink-0 pl-2">Catalysts</div>
                            <div className="w-[100px] text-right shrink-0">Generated</div>
                        </div>

                        {/* Virtualized Body */}
                        <div className="flex-1">
                            {loading ? (
                                <div className="flex items-center justify-center h-full text-muted-foreground">
                                    Loading signals...
                                </div>
                            ) : filteredSignals.length === 0 ? (
                                <div className="flex items-center justify-center h-full text-muted-foreground">
                                    No signals found matching your filters
                                </div>
                            ) : (
                                <Sizer>
                                    {({ height, width }: { height: number, width: number }) => (
                                        <VirtualList
                                            style={{ height, width }}
                                            rowCount={filteredSignals.length}
                                            rowHeight={60}
                                            rowProps={{ data: filteredSignals }}
                                            rowComponent={({ index, style, data }: any) => {
                                                const signal = data[index];
                                                const currentPrice = signal.currentPrice ?? 0;
                                                const priceChange24h = signal.priceChange24h ?? 0;
                                                const expectedReturn = signal.expectedReturn ?? 0;
                                                const positionSize = signal.positionSize ?? 0;

                                                return (
                                                    <div style={style} className="flex border-b items-center text-sm px-4 hover:bg-muted/50 transition-colors">
                                                        <div className="w-[100px] shrink-0">
                                                            {getStrengthBadge(signal.strength)}
                                                        </div>
                                                        <div className="w-[80px] shrink-0">
                                                            <Badge variant={getSignalBadgeVariant(signal.signalType)}>
                                                                {signal.signalType || '?'}
                                                            </Badge>
                                                        </div>
                                                        <div className="w-[80px] font-semibold shrink-0">
                                                            {signal.symbol || '-'}
                                                        </div>
                                                        <div className="flex-1 min-w-[100px] truncate pr-2" title={signal.strategyName}>
                                                            {signal.strategyName || '-'}
                                                        </div>
                                                        <div className="w-[120px] text-muted-foreground text-xs truncate pr-2 shrink-0">
                                                            {signal.sector || '-'}
                                                        </div>
                                                        <div className="w-[80px] text-right font-mono shrink-0">
                                                            ${currentPrice.toFixed(2)}
                                                        </div>
                                                        <div className="w-[80px] text-right shrink-0">
                                                            <div className={`flex items-center justify-end gap-1 ${priceChange24h > 0 ? 'text-green-600' : 'text-red-600'}`}>
                                                                {priceChange24h > 0 ? <ArrowUpRight className="h-3 w-3" /> : <ArrowDownRight className="h-3 w-3" />}
                                                                <span className="font-mono text-xs">
                                                                    {Math.abs(priceChange24h).toFixed(2)}%
                                                                </span>
                                                            </div>
                                                        </div>
                                                        <div className="w-[80px] text-right shrink-0">
                                                            <span className={`font-mono font-semibold ${expectedReturn > 0 ? 'text-green-600' : 'text-red-600'}`}>
                                                                {expectedReturn > 0 ? '+' : ''}{expectedReturn.toFixed(1)}%
                                                            </span>
                                                        </div>
                                                        <div className="w-[80px] text-right font-mono text-xs shrink-0">
                                                            {signal.targetPrice ? `$${signal.targetPrice.toFixed(0)}` : '-'}
                                                        </div>
                                                        <div className="w-[80px] text-right font-mono text-xs shrink-0">
                                                            {signal.stopLoss ? `$${signal.stopLoss.toFixed(0)}` : '-'}
                                                        </div>
                                                        <div className="w-[80px] shrink-0">
                                                            <Badge variant="outline" className="font-mono text-[10px]">
                                                                {signal.timeHorizon || '-'}
                                                            </Badge>
                                                        </div>
                                                        <div className="w-[60px] text-right font-mono text-xs shrink-0">
                                                            {positionSize.toFixed(1)}%
                                                        </div>
                                                        <div className="w-[150px] shrink-0 pl-2 overflow-hidden">
                                                            <div className="flex flex-wrap gap-1 h-full items-center overflow-hidden">
                                                                {(signal.catalysts || []).slice(0, 1).map((catalyst: string, idx: number) => (
                                                                    <Badge key={idx} variant="secondary" className="text-[10px] px-1 h-5 truncate max-w-full">
                                                                        {catalyst}
                                                                    </Badge>
                                                                ))}
                                                                {(signal.catalysts || []).length > 1 && (
                                                                    <span className="text-[10px] text-muted-foreground">+{signal.catalysts!.length - 1}</span>
                                                                )}
                                                            </div>
                                                        </div>
                                                        <div className="w-[100px] text-right text-xs text-muted-foreground shrink-0">
                                                            {formatTimeAgo(signal.generatedAt)}
                                                        </div>
                                                    </div>
                                                );
                                            }}
                                        />
                                    )}
                                </Sizer>
                            )}
                        </div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
