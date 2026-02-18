import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { DataService } from '@/services/DataService';
import { MarketData, FinanceData } from '@/types/data';
import { CandlestickChart } from '@/app/components/CandlestickChart';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';

import {
  Search,
  Activity,
  Table as TableIcon,
  AlertCircle,
  Loader2,
  TrendingUp
} from 'lucide-react';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { PageLoader } from '@/app/components/common/PageLoader';

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
      setError(formatSystemStatusText(err));
    } finally {
      setLoading(false);
    }
  };

  const latestPrice = stats.length > 0 ? stats[stats.length - 1] : null;
  const prevPrice = stats.length > 1 ? stats[stats.length - 2] : null;
  const priceChange = latestPrice && prevPrice ? latestPrice.close - prevPrice.close : 0;
  const percentChange = latestPrice && prevPrice ? (priceChange / prevPrice.close) * 100 : 0;

  return (
    <div className="page-shell">
      {/* Top Bar: Search & Title */}
      <div className="page-header-row border-b border-border/40 pb-6">
        <div className="flex items-center gap-3">
          <div className="rounded-lg border border-mcm-walnut/30 bg-mcm-paper p-2">
            <TrendingUp className="h-6 w-6 text-mcm-teal" />
          </div>
          <div>
            <p className="page-kicker">Market Intelligence</p>
            <h1 className="page-title">Live Market Data</h1>
            <p className="text-xs font-mono uppercase tracking-widest text-muted-foreground">
              DATA LAYER: SILVER • SOURCE: BACKTEST ENGINE
            </p>
          </div>
        </div>

        <form onSubmit={handleSearch} className="flex gap-2 w-full md:w-auto">
          <div className="relative">
            <Search className="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
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
        <div className="flex items-center gap-2 rounded-md border border-destructive/30 bg-destructive/10 p-4 text-destructive">
          <AlertCircle className="h-5 w-5" />
          <span>{error}</span>
        </div>
      )}

      {/* Main Content Area */}
      {loading ? (
        <PageLoader text="Loading Live Market Data..." className="h-[60vh]" />
      ) : stats.length > 0 ? (
        <div className="grid grid-cols-12 gap-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
          {/* Header Stats Card */}
          <div className="col-span-12">
            <Card className="mcm-panel">
              <CardContent className="p-6 flex items-center justify-between">
                <div>
                  <h2 className="mb-1 text-4xl font-black tracking-tight text-foreground">
                    {paramTicker?.toUpperCase()}
                  </h2>
                  <div className="flex items-center gap-4 text-sm font-mono text-muted-foreground">
                    <span>NASD</span>
                    <span>•</span>
                    <span>{stats.length} DATA POINTS</span>
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-3xl font-bold font-mono text-foreground">
                    ${latestPrice?.close.toFixed(2)}
                  </div>
                  <div
                    className={`font-mono text-sm flex items-center justify-end gap-1 ${priceChange >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}
                  >
                    {priceChange > 0 ? '+' : ''}
                    {priceChange.toFixed(2)} ({percentChange.toFixed(2)}%)
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Chart Section */}
          <div className="col-span-12 lg:col-span-8">
            <Card className="mcm-panel h-[500px] flex flex-col">
              <CardHeader className="border-b pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-muted-foreground">
                    <Activity className="h-4 w-4" /> Price Action
                  </CardTitle>
                  <div className="flex gap-2">
                    {['1M', '3M', '6M', '1Y', 'ALL'].map((range) => (
                      <button
                        key={range}
                        className="rounded px-2 py-1 text-[10px] font-bold text-muted-foreground transition-colors hover:bg-muted/40 hover:text-foreground"
                      >
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
            <Card className="mcm-panel">
              <CardHeader className="border-b bg-muted/20 pb-3">
                <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-muted-foreground">
                  <TableIcon className="h-4 w-4" /> Quote Detail
                </CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <Table>
                  <TableBody>
                    <TableRow>
                      <TableCell className="text-xs font-bold uppercase text-muted-foreground">
                        Open
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        ${latestPrice?.open.toFixed(2)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="text-xs font-bold uppercase text-muted-foreground">
                        High
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        ${latestPrice?.high.toFixed(2)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="text-xs font-bold uppercase text-muted-foreground">
                        Low
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        ${latestPrice?.low.toFixed(2)}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="text-xs font-bold uppercase text-muted-foreground">
                        Volume
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {latestPrice?.volume.toLocaleString()}
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell className="text-xs font-bold uppercase text-muted-foreground">
                        Date
                      </TableCell>
                      <TableCell className="text-right font-mono text-sm">
                        {latestPrice && new Date(latestPrice.date).toLocaleDateString()}
                      </TableCell>
                    </TableRow>
                  </TableBody>
                </Table>
              </CardContent>
            </Card>

            {/* Raw Finance Data (if any) */}
            <Card className="mcm-panel flex-1">
              <CardHeader className="border-b bg-muted/20 pb-3">
                <CardTitle className="flex items-center gap-2 text-sm font-bold uppercase tracking-widest text-muted-foreground">
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
                      {finance.map((row, idx) =>
                        Object.entries(row).map(
                          ([k, v]) =>
                            k !== 'symbol' &&
                            k !== 'date' &&
                            k !== 'sub_domain' && (
                              <TableRow key={`${idx}-${k}`}>
                                <TableCell className="text-xs font-mono text-muted-foreground">
                                  {k}
                                </TableCell>
                                <TableCell className="text-right font-mono text-xs">
                                  {String(v)}
                                </TableCell>
                              </TableRow>
                            )
                        )
                      )}
                    </TableBody>
                  </Table>
                ) : (
                  <div className="p-6 text-center text-xs font-mono text-muted-foreground">
                    NO FUNDAMENTAL DATA AVAILABLE
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </div>
      ) : null}

      {!loading && stats.length === 0 && !error && (
        <div className="mcm-panel flex h-64 flex-col items-center justify-center border-2 border-dashed border-border/50 bg-muted/20">
          <TrendingUp className="mb-4 h-12 w-12 text-muted-foreground/60" />
          <p className="font-medium text-muted-foreground">Enter a symbol to view live market data</p>
        </div>
      )}
    </div>
  );
}
