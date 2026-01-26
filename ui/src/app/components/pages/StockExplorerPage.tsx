import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { ArrowRight, Globe, Loader2, Search, TrendingUp } from 'lucide-react';

import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Input } from '@/app/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { DataService } from '@/services/DataService';

// Limited set of "Major" assets to show on the landing page to prove live data connectivity
// without overwhelming the backend with 500 requests at once.
const WATCHLIST = ['SPY', 'QQQ', 'IWM', 'GLD', 'AAPL', 'MSFT', 'GOOGL', 'NVDA'];

interface WatchlistStock {
    symbol: string;
    price: number | null;
    change: number | null;
    changePercent: number | null;
    volume: number | null;
    loading: boolean;
    error: boolean;
}

export function StockExplorerPage() {
    const navigate = useNavigate();
    const [searchTerm, setSearchTerm] = useState('');
    const [watchlist, setWatchlist] = useState<WatchlistStock[]>(() =>
        WATCHLIST.map((symbol) => ({
            symbol,
            price: null,
            change: null,
            changePercent: null,
            volume: null,
            loading: true,
            error: false,
        })),
    );

    const handleSearch = (event: React.FormEvent) => {
        event.preventDefault();
        const symbol = searchTerm.trim().toUpperCase();
        if (!symbol) return;
        navigate(`/stock/${symbol}`);
    };

    useEffect(() => {
        // Fetch data for the watchlist one by one (or parallel)
        WATCHLIST.forEach(async (symbol) => {
            try {
                const data = await DataService.getMarketData(symbol, 'silver'); // Silver layer for price
                if (data && data.length > 0) {
                    const latest = data[data.length - 1];
                    const prev = data.length > 1 ? data[data.length - 2] : null;
                    const change = prev ? latest.close - prev.close : 0;
                    const changePercent = prev ? (change / prev.close) * 100 : 0;

                    setWatchlist((prevList) =>
                        prevList.map((item) =>
                            item.symbol === symbol
                                ? {
                                      ...item,
                                      price: latest.close,
                                      change,
                                      changePercent,
                                      volume: latest.volume,
                                      loading: false,
                                  }
                                : item,
                        ),
                    );
                } else {
                    setWatchlist((prevList) =>
                        prevList.map((item) =>
                            item.symbol === symbol ? { ...item, loading: false, error: true } : item,
                        ),
                    );
                }
            } catch {
                setWatchlist((prevList) =>
                    prevList.map((item) => (item.symbol === symbol ? { ...item, loading: false, error: true } : item)),
                );
            }
        });
    }, []);

    return (
        <div className="space-y-4 h-full flex flex-col">
            {/* Top Toolbar */}
            <div className="flex items-center justify-between p-4 bg-card border-b">
                <div className="flex items-center gap-3">
                    <div className="p-2 bg-primary/10 rounded-md">
                        <Globe className="h-5 w-5 text-primary" />
                    </div>
                    <div>
                        <h1 className="text-lg font-bold tracking-tight">MARKET MONITOR</h1>
                        <p className="text-[10px] font-mono text-muted-foreground uppercase tracking-wider">
                            Real-time Feed • {watchlist.length} Assets • Silver Layer
                        </p>
                    </div>
                </div>

                <form onSubmit={handleSearch} className="flex items-center gap-2">
                    <div className="relative">
                        <Search className="absolute left-3 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                        <Input
                            placeholder="SEARCH TICKER..."
                            className="pl-9 h-9 w-64 font-mono uppercase text-xs"
                            value={searchTerm}
                            onChange={(e) => setSearchTerm(e.target.value)}
                        />
                    </div>
                    <Button type="submit" size="sm" variant="secondary" className="h-9">
                        <ArrowRight className="h-4 w-4" />
                    </Button>
                </form>
            </div>

            {/* Content Grid */}
            <div className="flex-1 p-6 grid grid-cols-12 gap-6 overflow-y-auto">
                {/* Market Overview Cards */}
                <div className="col-span-12 grid grid-cols-1 md:grid-cols-4 gap-4">
                    {watchlist.slice(0, 4).map((stock) => (
                        <Card
                            key={stock.symbol}
                            className="border-l-4 border-l-primary/50 shadow-sm hover:shadow-md transition-shadow cursor-pointer"
                            onClick={() => navigate(`/stock/${stock.symbol}`)}
                        >
                            <CardContent className="p-4">
                                <div className="flex justify-between items-start mb-2">
                                    <span className="font-black font-mono text-xl">{stock.symbol}</span>
                                    {stock.changePercent !== null && (
                                        <Badge variant={stock.changePercent >= 0 ? 'default' : 'destructive'} className="font-mono text-[10px]">
                                            {stock.changePercent > 0 ? '+' : ''}{stock.changePercent.toFixed(2)}%
                                        </Badge>
                                    )}
                                </div>
                                <div className="flex justify-between items-end">
                                    <div className="text-xs text-muted-foreground uppercase font-bold">Last Price</div>
                                    <div className="font-mono font-bold text-lg">
                                        {stock.loading ? '...' : stock.price ? `$${stock.price.toFixed(2)}` : 'N/A'}
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    ))}
                </div>

                {/* Main Watchlist Table */}
                <div className="col-span-12">
                    <Card>
                        <CardHeader className="py-3 px-4 border-b bg-muted/40 flex flex-row items-center justify-between">
                            <CardTitle className="text-xs font-bold uppercase tracking-widest text-muted-foreground flex items-center gap-2">
                                <TrendingUp className="h-3.5 w-3.5" /> Active Watchlist
                            </CardTitle>
                            <div className="flex gap-2">
                                <Badge variant="outline" className="text-[10px] font-mono">LIVE CONNECTED</Badge>
                            </div>
                        </CardHeader>
                        <CardContent className="p-0">
                            <Table>
                                <TableHeader>
                                    <TableRow className="bg-muted/20 hover:bg-muted/20">
                                        <TableHead className="w-[120px] font-bold text-[10px] uppercase tracking-wider h-8">Symbol</TableHead>
                                        <TableHead className="text-right font-bold text-[10px] uppercase tracking-wider h-8">Last Price</TableHead>
                                        <TableHead className="text-right font-bold text-[10px] uppercase tracking-wider h-8">Change</TableHead>
                                        <TableHead className="text-right font-bold text-[10px] uppercase tracking-wider h-8">% Daily</TableHead>
                                        <TableHead className="text-right font-bold text-[10px] uppercase tracking-wider h-8">Volume</TableHead>
                                        <TableHead className="w-[80px] h-8"></TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {watchlist.map((stock) => (
                                        <TableRow
                                            key={stock.symbol}
                                            className="cursor-pointer hover:bg-accent/50 group border-b-border/50 text-xs"
                                            onClick={() => navigate(`/stock/${stock.symbol}`)}
                                        >
                                            <TableCell className="font-bold font-mono text-primary group-hover:underline decoration-dotted underline-offset-4">
                                                {stock.symbol}
                                            </TableCell>
                                            <TableCell className="text-right font-mono font-medium">
                                                {stock.loading ? (
                                                    <Loader2 className="h-3 w-3 animate-spin ml-auto text-muted-foreground" />
                                                ) : stock.error ? (
                                                    <span className="text-muted-foreground">-</span>
                                                ) : (
                                                    `$${stock.price?.toFixed(2)}`
                                                )}
                                            </TableCell>
                                            <TableCell className="text-right font-mono">
                                                {!stock.loading && !stock.error && stock.change !== null && (
                                                    <span className={stock.change >= 0 ? 'text-green-600 dark:text-green-400' : 'text-red-600 dark:text-red-400'}>
                                                        {stock.change > 0 ? '+' : ''}{stock.change.toFixed(2)}
                                                    </span>
                                                )}
                                            </TableCell>
                                            <TableCell className="text-right font-mono">
                                                {!stock.loading && !stock.error && stock.changePercent !== null && (
                                                    <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold ${stock.changePercent >= 0
                                                        ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400'
                                                        : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400'
                                                        }`}>
                                                        {stock.changePercent > 0 ? '+' : ''}{stock.changePercent.toFixed(2)}%
                                                    </span>
                                                )}
                                            </TableCell>
                                            <TableCell className="text-right font-mono text-muted-foreground">
                                                {stock.volume ? (stock.volume / 1000000).toFixed(2) + 'M' : '-'}
                                            </TableCell>
                                            <TableCell className="text-center">
                                                <ArrowRight className="h-3 w-3 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity mx-auto" />
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}
