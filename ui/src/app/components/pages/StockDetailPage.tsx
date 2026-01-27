import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { DataService } from '@/services/DataService';
import { MarketData, FinanceData } from '@/types/data';
import { CandlestickChart } from '@/app/components/CandlestickChart';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';

import { Search, Activity, Table as TableIcon, AlertCircle, Loader2, TrendingUp } from 'lucide-react';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/app/components/ui/table';

export function StockDetailPage() {
    const { ticker: paramTicker } = useParams();
    const navigate = useNavigate();
    const [ticker, setTicker] = useState(paramTicker || '');
    const [stats, setStats] = useState<MarketData[]>([]);
    const [finance, setFinance] = useState<FinanceData[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Effect to load data when ticker changes (from URL)
    useEffect(() => {
        if (paramTicker) {
            setTicker(paramTicker);
            loadData(paramTicker);
        }
    }, [paramTicker]);

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (ticker) {
            navigate(`/stock/${ticker.toUpperCase()}`);
        }
    };

    const loadData = async (sym: string) => {
        setLoading(true);
        setError(null);
        try {
            // Parallel fetch
            const [marketRes, financeRes] = await Promise.allSettled([
                DataService.getMarketData(sym, 'silver'), // Default to silver layer
                DataService.getFinanceData(sym, 'summary', 'silver')
            ]);

            if (marketRes.status === 'fulfilled') {
                setStats(marketRes.value);
            } else {
                console.warn('Market data failed', marketRes.reason);
            }

            if (financeRes.status === 'fulfilled') {
                setFinance(financeRes.value);
            } else {
                console.warn('Finance data failed', financeRes.reason);
            }

            if (marketRes.status === 'rejected' && financeRes.status === 'rejected') {
                setError('Could not retrieve data for this symbol.');
            }

        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
        } finally {
            setLoading(false);
        }
    };

    const latestPrice = stats.length > 0 ? stats[stats.length - 1] : null;
    const prevPrice = stats.length > 1 ? stats[stats.length - 2] : null;
    const priceChange = latestPrice && prevPrice ? latestPrice.close - prevPrice.close : 0;
    const percentChange = latestPrice && prevPrice ? (priceChange / prevPrice.close) * 100 : 0;

    return (
        <div className="space-y-6 container mx-auto max-w-[1600px] p-6 font-sans">
            {/* Top Bar: Search & Title */}
            <div className="flex flex-col md:flex-row gap-4 items-center justify-between border-b border-slate-200 pb-6">
                <div className="flex items-center gap-3">
                    <div className="p-2 bg-indigo-50 border border-indigo-100 rounded-lg">
                        <TrendingUp className="h-6 w-6 text-indigo-600" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-black text-slate-900 tracking-tight">LIVE MARKET DATA</h1>
                        <p className="text-xs text-slate-500 font-mono tracking-widest uppercase">
                            DATA LAYER: SILVER • SOURCE: DATA ENGINE
                        </p>
                    </div>
                </div>

                <form onSubmit={handleSearch} className="flex gap-2 w-full md:w-auto">
                    <div className="relative">
                        <Search className="absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
                        <Input
                            placeholder="ENTER SYMBOL (e.g. SPY)"
                            value={ticker}
                            onChange={(e) => setTicker(e.target.value)}
                            className="pl-9 w-64 font-mono uppercase"
                        />
                    </div>
                    <Button type="submit" disabled={loading}>
                        {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : 'LOAD'}
                    </Button>
                </form>
            </div>

            {error && (
                <div className="p-4 bg-red-50 text-red-700 border border-red-200 rounded-md flex items-center gap-2">
                    <AlertCircle className="h-5 w-5" />
                    <span>{error}</span>
                </div>
            )}

            {/* Main Content Area */}
            {stats.length > 0 && (
                <div className="grid grid-cols-12 gap-6 animate-in fade-in slide-in-from-bottom-4 duration-500">

                    {/* Header Stats Card */}
                    <div className="col-span-12">
                        <Card className="bg-slate-900 text-white border-slate-800">
                            <CardContent className="p-6 flex items-center justify-between">
                                <div>
                                    <h2 className="text-4xl font-black tracking-tighter mb-1">{paramTicker?.toUpperCase()}</h2>
                                    <div className="flex items-center gap-4 text-sm font-mono text-slate-400">
                                        <span>NASD</span>
                                        <span>•</span>
                                        <span>{stats.length} DATA POINTS</span>
                                    </div>
                                </div>
                                <div className="text-right">
                                    <div className="text-3xl font-bold font-mono">
                                        ${latestPrice?.close.toFixed(2)}
                                    </div>
                                    <div className={`font-mono text-sm flex items-center justify-end gap-1 ${priceChange >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                                        {priceChange > 0 ? '+' : ''}{priceChange.toFixed(2)} ({percentChange.toFixed(2)}%)
                                    </div>
                                </div>
                            </CardContent>
                        </Card>
                    </div>

                    {/* Chart Section */}
                    <div className="col-span-12 lg:col-span-8">
                        <Card className="h-[500px] flex flex-col">
                            <CardHeader className="border-b pb-3">
                                <div className="flex items-center justify-between">
                                    <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-slate-500">
                                        <Activity className="h-4 w-4" /> Price Action
                                    </CardTitle>
                                    <div className="flex gap-2">
                                        {['1M', '3M', '6M', '1Y', 'ALL'].map(range => (
                                            <button key={range} className="text-[10px] font-bold text-slate-400 hover:text-slate-900 px-2 py-1 rounded hover:bg-slate-100 transition-colors">
                                                {range}
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            </CardHeader>
                            <CardContent className="flex-1 min-h-0 p-4">
                                <CandlestickChart data={stats} height={400} />
                            </CardContent>
                        </Card>
                    </div>

                    {/* Side Data Panel */}
                    <div className="col-span-12 lg:col-span-4 space-y-6">
                        {/* Latest Quote Details */}
                        <Card>
                            <CardHeader className="border-b pb-3 bg-slate-50/50">
                                <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-slate-500">
                                    <TableIcon className="h-4 w-4" /> Quote Detail
                                </CardTitle>
                            </CardHeader>
                            <CardContent className="p-0">
                                <Table>
                                    <TableBody>
                                        <TableRow>
                                            <TableCell className="text-xs font-bold text-slate-500 uppercase">Open</TableCell>
                                            <TableCell className="text-right font-mono text-sm">${latestPrice?.open.toFixed(2)}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell className="text-xs font-bold text-slate-500 uppercase">High</TableCell>
                                            <TableCell className="text-right font-mono text-sm">${latestPrice?.high.toFixed(2)}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell className="text-xs font-bold text-slate-500 uppercase">Low</TableCell>
                                            <TableCell className="text-right font-mono text-sm">${latestPrice?.low.toFixed(2)}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell className="text-xs font-bold text-slate-500 uppercase">Volume</TableCell>
                                            <TableCell className="text-right font-mono text-sm">{latestPrice?.volume.toLocaleString()}</TableCell>
                                        </TableRow>
                                        <TableRow>
                                            <TableCell className="text-xs font-bold text-slate-500 uppercase">Date</TableCell>
                                            <TableCell className="text-right font-mono text-sm">{latestPrice && new Date(latestPrice.date).toLocaleDateString()}</TableCell>
                                        </TableRow>
                                    </TableBody>
                                </Table>
                            </CardContent>
                        </Card>

                        {/* Raw Finance Data (if any) */}
                        <Card className="flex-1">
                            <CardHeader className="border-b pb-3 bg-slate-50/50">
                                <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-slate-500">
                                    <TableIcon className="h-4 w-4" /> Fundamental Data
                                </CardTitle>
                            </CardHeader>
                            <CardContent className="p-0 max-h-[250px] overflow-y-auto">
                                {finance.length > 0 ? (
                                    <Table>
                                        <TableHeader>
                                            <TableRow>
                                                <TableHead>Field</TableHead>
                                                <TableHead className="text-right">Value</TableHead>
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                            {finance.map((row, idx) => (
                                                Object.entries(row).map(([k, v]) => (
                                                    k !== 'symbol' && k !== 'date' && k !== 'sub_domain' && (
                                                        <TableRow key={`${idx}-${k}`}>
                                                            <TableCell className="text-xs font-mono text-slate-500">{k}</TableCell>
                                                            <TableCell className="text-right font-mono text-xs">{String(v)}</TableCell>
                                                        </TableRow>
                                                    )
                                                ))
                                            ))}
                                        </TableBody>
                                    </Table>
                                ) : (
                                    <div className="p-6 text-center text-xs text-slate-400 font-mono">
                                        NO FUNDAMENTAL DATA AVAILABLE
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    </div>
                </div>
            )}

            {!loading && stats.length === 0 && !error && (
                <div className="flex flex-col items-center justify-center h-64 border-2 border-dashed border-slate-200 rounded-lg bg-slate-50">
                    <TrendingUp className="h-12 w-12 text-slate-300 mb-4" />
                    <p className="text-slate-500 font-medium">Enter a symbol to view live market data</p>
                </div>
            )}
        </div>
    );
}
