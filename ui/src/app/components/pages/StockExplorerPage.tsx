import { useState, useEffect, useMemo } from 'react';
import { getStocks } from '@/data/mockStockData';
import { Stock } from '@/types/stock';
import { mapping } from '@/utils/mapping'; // Ensure mapping util exists or remove if unused, user code looked clean so we stick to it
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Checkbox } from '@/app/components/ui/checkbox';
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from '@/app/components/ui/table';
import {
    Tooltip,
    TooltipContent,
    TooltipProvider,
    TooltipTrigger,
} from '@/app/components/ui/tooltip';
import {
    Search,
    Filter,
    TrendingUp,
    TrendingDown,
    AlertTriangle,
    DollarSign,
    Activity,
    X,
    ChevronDown,
    ChevronUp,
    Loader2,
    Info,
    Building2,
    Percent,
    Target,
    Globe,
    LayoutGrid,
    List
} from 'lucide-react';
import { LineChart, Line, ResponsiveContainer } from 'recharts';
// @ts-ignore - Importing our new component
import { CandlestickChart } from '@/app/components/CandlestickChart';

type SortField = 'symbol' | 'name' | 'marketCap' | 'price' | 'priceChange1Y' | 'pe' | 'dividendYield' | 'beta' | 'volume';
type SortDirection = 'asc' | 'desc';

interface FilterState {
    sectors: string[];
    minMarketCap: number | null;
    maxMarketCap: number | null;
    minPE: number | null;
    maxPE: number | null;
    minDividendYield: number | null;
    minROE: number | null;
    minRevenueGrowth: number | null;
    exchanges: string[];
    showHighVolatility: boolean;
    showNegativeEarnings: boolean;
    showLowLiquidity: boolean;
}

export function StockExplorerPage() {
    const dataSource = 'mock';
    const [stocks, setStocks] = useState<Stock[]>([]);
    const [loading, setLoading] = useState(true);
    const [searchTerm, setSearchTerm] = useState('');
    const [sortField, setSortField] = useState<SortField>('marketCap');
    const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
    const [showFilters, setShowFilters] = useState(false);
    const [expandedRow, setExpandedRow] = useState<string | null>(null);
    const [selectedStocks, setSelectedStocks] = useState<Set<string>>(new Set());

    const [filters, setFilters] = useState<FilterState>({
        sectors: [],
        minMarketCap: null,
        maxMarketCap: null,
        minPE: null,
        maxPE: null,
        minDividendYield: null,
        minROE: null,
        minRevenueGrowth: null,
        exchanges: [],
        showHighVolatility: true,
        showNegativeEarnings: true,
        showLowLiquidity: true,
    });

    // Load stocks based on data source
    useEffect(() => {
        async function loadStocks() {
            setLoading(true);
            try {
                const data = await getStocks(dataSource || 'mock');
                setStocks(data);
            } catch (err) {
                console.error("Failed to load stocks", err);
            } finally {
                setLoading(false);
            }
        }
        loadStocks();
    }, [dataSource]);

    // Extract unique sectors and exchanges
    const allSectors = useMemo(() => {
        const sectorSet = new Set<string>();
        stocks.forEach(s => sectorSet.add(s.sector));
        return Array.from(sectorSet).sort();
    }, [stocks]);

    const allExchanges = useMemo(() => {
        const exchangeSet = new Set<string>();
        stocks.forEach(s => exchangeSet.add(s.exchange));
        return Array.from(exchangeSet).sort();
    }, [stocks]);

    // Filter and sort stocks
    const filteredStocks = useMemo(() => {
        let result = stocks.filter(stock => {
            // Search filter
            if (searchTerm) {
                const search = searchTerm.toLowerCase();
                if (!stock.symbol.toLowerCase().includes(search) &&
                    !stock.name.toLowerCase().includes(search)) {
                    return false;
                }
            }

            // Sector filter
            if (filters.sectors.length > 0 && !filters.sectors.includes(stock.sector)) {
                return false;
            }

            // Exchange filter
            if (filters.exchanges.length > 0 && !filters.exchanges.includes(stock.exchange)) {
                return false;
            }

            // Market cap filter
            if (filters.minMarketCap !== null && stock.marketCap < filters.minMarketCap) return false;
            if (filters.maxMarketCap !== null && stock.marketCap > filters.maxMarketCap) return false;

            // Valuation filters
            if (filters.minPE !== null && (stock.pe === null || stock.pe < filters.minPE)) return false;
            if (filters.maxPE !== null && (stock.pe === null || stock.pe > filters.maxPE)) return false;

            // Dividend filter
            if (filters.minDividendYield !== null &&
                (stock.dividendYield === null || stock.dividendYield < filters.minDividendYield)) {
                return false;
            }

            // Profitability filter
            if (filters.minROE !== null && (stock.roe === null || stock.roe < filters.minROE)) return false;

            // Growth filter
            if (filters.minRevenueGrowth !== null &&
                (stock.revenueGrowth === null || stock.revenueGrowth < filters.minRevenueGrowth)) {
                return false;
            }

            // Risk flag filters
            if (!filters.showHighVolatility && stock.highVolatility) return false;
            if (!filters.showNegativeEarnings && stock.negativeEarnings) return false;
            if (!filters.showLowLiquidity && stock.lowLiquidity) return false;

            return true;
        });

        // Sort
        result.sort((a, b) => {
            let aVal: any = a[sortField];
            let bVal: any = b[sortField];

            // Handle null values
            if (aVal === null) return 1;
            if (bVal === null) return -1;

            if (typeof aVal === 'string' && typeof bVal === 'string') {
                return sortDirection === 'asc'
                    ? aVal.localeCompare(bVal)
                    : bVal.localeCompare(aVal);
            }

            const aNum = Number(aVal);
            const bNum = Number(bVal);
            return sortDirection === 'asc' ? aNum - bNum : bNum - aNum;
        });

        return result;
    }, [stocks, searchTerm, filters, sortField, sortDirection]);

    const handleSort = (field: SortField) => {
        if (sortField === field) {
            setSortDirection(prev => prev === 'asc' ? 'desc' : 'asc');
        } else {
            setSortField(field);
            setSortDirection('desc');
        }
    };

    const handleStockSelect = (symbol: string, checked: boolean) => {
        setSelectedStocks(prev => {
            const next = new Set(prev);
            if (checked) {
                next.add(symbol);
            } else {
                next.delete(symbol);
            }
            return next;
        });
    };

    const toggleRowExpand = (id: string) => {
        setExpandedRow(expandedRow === id ? null : id);
    };

    const clearFilters = () => {
        setFilters({
            sectors: [],
            minMarketCap: null,
            maxMarketCap: null,
            minPE: null,
            maxPE: null,
            minDividendYield: null,
            minROE: null,
            minRevenueGrowth: null,
            exchanges: [],
            showHighVolatility: true,
            showNegativeEarnings: true,
            showLowLiquidity: true,
        });
        setSearchTerm('');
    };

    const activeFilterCount = useMemo(() => {
        let count = 0;
        if (filters.sectors.length > 0) count++;
        if (filters.exchanges.length > 0) count++;
        if (filters.minMarketCap !== null || filters.maxMarketCap !== null) count++;
        if (filters.minPE !== null || filters.maxPE !== null) count++;
        if (filters.minDividendYield !== null) count++;
        if (filters.minROE !== null) count++;
        if (filters.minRevenueGrowth !== null) count++;
        if (!filters.showHighVolatility || !filters.showNegativeEarnings || !filters.showLowLiquidity) count++;
        return count;
    }, [filters]);

    const SortIcon = ({ field }: { field: SortField }) => {
        if (sortField !== field) return <span className="w-4 ml-1 inline-block" />;
        return sortDirection === 'asc' ?
            <ChevronUp className="h-3 w-3 inline ml-1 text-primary" /> :
            <ChevronDown className="h-3 w-3 inline ml-1 text-primary" />;
    };

    const formatMarketCap = (cap: number) => {
        if (cap >= 1000) return `$${(cap / 1000).toFixed(2)}T`;
        return `$${cap.toFixed(1)}B`;
    };

    const formatVolume = (vol: number) => {
        if (vol >= 1000) return `${(vol / 1000).toFixed(1)}B`;
        return `${vol.toFixed(1)}M`;
    };

    const PriceChangeCell = ({ change }: { change: number }) => {
        const isPositive = change >= 0;
        return (
            <span className={`font-mono flex justify-end items-center gap-1 ${isPositive ? 'text-green-600 dark:text-green-500' : 'text-red-600 dark:text-red-500'}`}>
                {isPositive ? '+' : ''}{change.toFixed(2)}%
            </span>
        );
    };

    if (loading) {
        return (
            <div className="flex items-center justify-center h-[calc(100vh-100px)]">
                <div className="flex flex-col items-center gap-3 text-muted-foreground animate-pulse">
                    <Loader2 className="h-6 w-6 animate-spin" />
                    <span className="font-mono text-xs tracking-widest uppercase">Initializing Stock Universe...</span>
                </div>
            </div>
        );
    }

    return (
        <TooltipProvider>
            <div className="space-y-4 max-w-[1920px] mx-auto">
                {/* Header */}
                <div className="flex items-end justify-between border-b pb-4 border-dashed border-border/60">
                    <div>
                        <h1 className="text-xl font-bold tracking-tight text-foreground flex items-center gap-2">
                            <Globe className="h-5 w-5 stroke-[1.5]" />
                            STOCK EXPLORER
                        </h1>
                        <p className="text-xs font-mono text-muted-foreground mt-1 tracking-wide">
                            UNIVERSE: {filteredStocks.length} ASSETS
                            {selectedStocks.size > 0 && <span className="text-primary ml-2">[{selectedStocks.size} SELECTED]</span>}
                        </p>
                    </div>

                    <div className="flex items-center gap-2">
                        {activeFilterCount > 0 && (
                            <Button variant="ghost" size="sm" onClick={clearFilters} className="h-8 text-xs font-mono">
                                <X className="h-3.5 w-3.5 mr-1.5" />
                                CLEAR ({activeFilterCount})
                            </Button>
                        )}
                        <Button
                            variant={showFilters ? "secondary" : "outline"}
                            size="sm"
                            onClick={() => setShowFilters(!showFilters)}
                            className="h-8 text-xs font-mono border-dashed"
                        >
                            <Filter className="h-3.5 w-3.5 mr-1.5" />
                            FILTERS
                        </Button>
                    </div>
                </div>

                {/* Search Bar - Integrated with minimal height */}
                <div className="relative">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
                    <Input
                        placeholder="SEARCH BY SYMBOL OR COMPANY NAME..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="pl-9 h-9 font-mono text-sm bg-muted/20 border-border/50 focus-visible:ring-1 focus-visible:ring-offset-0"
                    />
                </div>

                {/* Filters Panel */}
                {showFilters && (
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 p-4 bg-muted/20 rounded-lg border border-border/50 animate-in fade-in slide-in-from-top-2">
                        {/* Sector & Exchange */}
                        <div className="space-y-4 col-span-1 md:col-span-2">
                            <div className="space-y-2">
                                <label className="text-[10px] font-mono font-semibold uppercase text-muted-foreground tracking-wider">Sectors</label>
                                <div className="flex flex-wrap gap-1.5">
                                    {allSectors.map(sector => (
                                        <Badge
                                            key={sector}
                                            variant="outline"
                                            className={`cursor-pointer font-mono text-[10px] h-5 rounded-none border-dashed ${filters.sectors.includes(sector) ? 'bg-primary text-primary-foreground border-primary' : 'hover:bg-accent'}`}
                                            onClick={() => {
                                                setFilters(prev => ({
                                                    ...prev,
                                                    sectors: prev.sectors.includes(sector)
                                                        ? prev.sectors.filter(s => s !== sector)
                                                        : [...prev.sectors, sector]
                                                }));
                                            }}
                                        >
                                            {sector.toUpperCase()}
                                        </Badge>
                                    ))}
                                </div>
                            </div>
                        </div>

                        {/* Metrics */}
                        <div className="space-y-3">
                            <label className="text-[10px] font-mono font-semibold uppercase text-muted-foreground tracking-wider">Metrics Range</label>
                            <div className="grid grid-cols-2 gap-2">
                                <Input type="number" placeholder="MIN CAP (B)" className="h-7 text-xs font-mono" onChange={e => setFilters(prev => ({ ...prev, minMarketCap: e.target.value ? Number(e.target.value) : null }))} />
                                <Input type="number" placeholder="MAX CAP (B)" className="h-7 text-xs font-mono" onChange={e => setFilters(prev => ({ ...prev, maxMarketCap: e.target.value ? Number(e.target.value) : null }))} />
                                <Input type="number" placeholder="MIN P/E" className="h-7 text-xs font-mono" onChange={e => setFilters(prev => ({ ...prev, minPE: e.target.value ? Number(e.target.value) : null }))} />
                                <Input type="number" placeholder="MIN YIELD %" className="h-7 text-xs font-mono" onChange={e => setFilters(prev => ({ ...prev, minDividendYield: e.target.value ? Number(e.target.value) : null }))} />
                            </div>
                        </div>

                        {/* Flags */}
                        <div className="space-y-3">
                            <label className="text-[10px] font-mono font-semibold uppercase text-muted-foreground tracking-wider">Risk Filtering</label>
                            <div className="space-y-2">
                                <div className="flex items-center space-x-2">
                                    <Checkbox id="highVol" checked={filters.showHighVolatility} onCheckedChange={(c) => setFilters(p => ({ ...p, showHighVolatility: !!c }))} />
                                    <label htmlFor="highVol" className="text-xs font-mono">SHOW HIGH VOLATILITY</label>
                                </div>
                                <div className="flex items-center space-x-2">
                                    <Checkbox id="negEarn" checked={filters.showNegativeEarnings} onCheckedChange={(c) => setFilters(p => ({ ...p, showNegativeEarnings: !!c }))} />
                                    <label htmlFor="negEarn" className="text-xs font-mono">SHOW NEGATIVE EARNINGS</label>
                                </div>
                            </div>
                        </div>
                    </div>
                )}

                {/* Results Table */}
                <div className="border border-border/60 rounded-md overflow-hidden bg-card">
                    <Table>
                        <TableHeader className="bg-muted/30">
                            <TableRow className="hover:bg-transparent border-b-border/60">
                                <TableHead className="w-10">
                                    <Checkbox
                                        checked={selectedStocks.size === filteredStocks.length && filteredStocks.length > 0}
                                        onCheckedChange={(checked) => {
                                            if (checked) setSelectedStocks(new Set(filteredStocks.map(s => s.symbol)));
                                            else setSelectedStocks(new Set());
                                        }}
                                    />
                                </TableHead>
                                <TableHead className="w-8"></TableHead>
                                <TableHead className="w-[100px] cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('symbol')}>
                                    <span className="flex items-center font-mono text-xs">SYMBOL <SortIcon field="symbol" /></span>
                                </TableHead>
                                <TableHead className="cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('name')}>
                                    <span className="flex items-center font-mono text-xs">NAME <SortIcon field="name" /></span>
                                </TableHead>
                                <TableHead className="h-9"><span className="font-mono text-xs">SECTOR</span></TableHead>
                                <TableHead className="text-right cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('marketCap')}>
                                    <span className="flex items-center justify-end font-mono text-xs">CAP <SortIcon field="marketCap" /></span>
                                </TableHead>
                                <TableHead className="text-right cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('price')}>
                                    <span className="flex items-center justify-end font-mono text-xs">PRICE <SortIcon field="price" /></span>
                                </TableHead>
                                <TableHead className="text-right cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('priceChange1Y')}>
                                    <span className="flex items-center justify-end font-mono text-xs">1Y CHG <SortIcon field="priceChange1Y" /></span>
                                </TableHead>
                                <TableHead className="text-right cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('pe')}>
                                    <span className="flex items-center justify-end font-mono text-xs">P/E <SortIcon field="pe" /></span>
                                </TableHead>
                                <TableHead className="text-right cursor-pointer hover:text-primary transition-colors h-9" onClick={() => handleSort('dividendYield')}>
                                    <span className="flex items-center justify-end font-mono text-xs">YIELD <SortIcon field="dividendYield" /></span>
                                </TableHead>
                                <TableHead className="w-[120px] h-9"><span className="font-mono text-xs">TREND</span></TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {filteredStocks.length === 0 ? (
                                <TableRow>
                                    <TableCell colSpan={11} className="h-24 text-center text-muted-foreground font-mono text-sm">
                                        NO MATCHING ASSETS FOUND
                                    </TableCell>
                                </TableRow>
                            ) : (
                                filteredStocks.map((stock) => (
                                    <>
                                        <TableRow
                                            key={stock.id}
                                            className={`group hover:bg-muted/30 border-b-border/40 transition-colors data-[state=open]:bg-muted/50`}
                                            data-state={expandedRow === stock.id ? "open" : "closed"}
                                        >
                                            <TableCell className="py-1.5"><Checkbox checked={selectedStocks.has(stock.symbol)} onCheckedChange={(c) => handleStockSelect(stock.symbol, !!c)} /></TableCell>
                                            <TableCell className="py-1.5">
                                                <Button variant="ghost" size="icon" className="h-5 w-5" onClick={() => toggleRowExpand(stock.id)}>
                                                    {expandedRow === stock.id ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
                                                </Button>
                                            </TableCell>
                                            <TableCell className="font-mono font-bold text-primary py-1.5">{stock.symbol}</TableCell>
                                            <TableCell className="max-w-[200px] truncate text-xs py-1.5 font-medium text-muted-foreground group-hover:text-foreground">{stock.name}</TableCell>
                                            <TableCell className="py-1.5"><Badge variant="secondary" className="rounded-sm px-1.5 text-[10px] font-normal uppercase tracking-wider">{stock.sector}</Badge></TableCell>
                                            <TableCell className="text-right font-mono text-xs py-1.5">{formatMarketCap(stock.marketCap)}</TableCell>
                                            <TableCell className="text-right font-mono text-xs py-1.5">${stock.price.toFixed(2)}</TableCell>
                                            <TableCell className="text-right text-xs py-1.5"><PriceChangeCell change={stock.priceChange1Y} /></TableCell>
                                            <TableCell className="text-right font-mono text-xs py-1.5 text-muted-foreground">{stock.pe !== null ? stock.pe.toFixed(1) : '—'}</TableCell>
                                            <TableCell className="text-right font-mono text-xs py-1.5 text-muted-foreground">{stock.dividendYield !== null ? `${stock.dividendYield.toFixed(2)}%` : '—'}</TableCell>
                                            <TableCell className="py-1 pl-0 pr-2">
                                                <div className="h-8 w-24 ml-auto">
                                                    <ResponsiveContainer width="100%" height="100%">
                                                        <LineChart data={stock.priceHistory}>
                                                            <Line type="monotone" dataKey="price" stroke={stock.priceChange1Y >= 0 ? '#22c55e' : '#ef4444'} strokeWidth={1.5} dot={false} isAnimationActive={false} />
                                                        </LineChart>
                                                    </ResponsiveContainer>
                                                </div>
                                            </TableCell>
                                        </TableRow>
                                        {expandedRow === stock.id && (
                                            <TableRow key={`${stock.id}-expanded`} className="hover:bg-muted/50 bg-muted/20 border-b-border/40">
                                                <TableCell colSpan={11} className="p-0">
                                                    <div className="p-4 grid grid-cols-12 gap-6 border-b border-dashed border-border/50 animate-in fade-in zoom-in-95 duration-200">

                                                        {/* Left Column: Chart */}
                                                        <div className="col-span-12 xl:col-span-7 space-y-2">
                                                            <div className="flex items-center justify-between">
                                                                <h4 className="text-xs font-mono font-semibold uppercase flex items-center gap-2 text-muted-foreground">
                                                                    <Activity className="h-3.5 w-3.5" /> Price Action (12M)
                                                                </h4>
                                                                <div className="flex gap-2">
                                                                    <Badge variant="outline" className="font-mono text-[10px]">{stock.exchange}</Badge>
                                                                    <Badge variant="outline" className="font-mono text-[10px]">{stock.country}</Badge>
                                                                </div>
                                                            </div>
                                                            <div className="h-[300px] border border-border/50 bg-background rounded-md p-2 shadow-sm">
                                                                <CandlestickChart data={stock.priceHistory} height={280} />
                                                            </div>
                                                        </div>

                                                        {/* Right Column: Data Grid */}
                                                        <div className="col-span-12 xl:col-span-5 grid grid-cols-2 gap-4">
                                                            <div className="space-y-3">
                                                                <h4 className="text-xs font-mono font-semibold uppercase flex items-center gap-2 text-muted-foreground border-b border-border/30 pb-1">
                                                                    <Building2 className="h-3.5 w-3.5" /> Fundamentals
                                                                </h4>
                                                                <div className="space-y-1.5">
                                                                    <DataRow label="Industry" value={stock.industry} />
                                                                    <DataRow label="Employees" value={stock.employees.toLocaleString()} />
                                                                    <DataRow label="IPO Date" value={new Date(stock.ipoDate).getFullYear().toString()} />
                                                                    <DataRow label="ESG Score" value={stock.esgScore?.toFixed(0) || 'N/A'} />
                                                                    <DataRow label="Analyst" value={stock.analystRating || 'N/A'} highlight />
                                                                </div>
                                                            </div>

                                                            <div className="space-y-3">
                                                                <h4 className="text-xs font-mono font-semibold uppercase flex items-center gap-2 text-muted-foreground border-b border-border/30 pb-1">
                                                                    <DollarSign className="h-3.5 w-3.5" /> Valuation
                                                                </h4>
                                                                <div className="space-y-1.5">
                                                                    <DataRow label="P/E Ratio" value={stock.pe?.toFixed(2) || 'N/A'} />
                                                                    <DataRow label="P/B Ratio" value={stock.pb?.toFixed(2) || 'N/A'} />
                                                                    <DataRow label="P/S Ratio" value={stock.ps?.toFixed(2) || 'N/A'} />
                                                                    <DataRow label="PEG Ratio" value={stock.pegRatio?.toFixed(2) || 'N/A'} />
                                                                    <DataRow label="EV/EBITDA" value={stock.evToEbitda?.toFixed(2) || 'N/A'} />
                                                                </div>
                                                            </div>

                                                            <div className="space-y-3">
                                                                <h4 className="text-xs font-mono font-semibold uppercase flex items-center gap-2 text-muted-foreground border-b border-border/30 pb-1">
                                                                    <Percent className="h-3.5 w-3.5" /> Performance
                                                                </h4>
                                                                <div className="space-y-1.5">
                                                                    <DataRow label="ROE" value={stock.roe ? `${stock.roe.toFixed(1)}%` : 'N/A'} />
                                                                    <DataRow label="ROA" value={stock.roa ? `${stock.roa.toFixed(1)}%` : 'N/A'} />
                                                                    <DataRow label="Margin" value={stock.profitMargin ? `${stock.profitMargin.toFixed(1)}%` : 'N/A'} />
                                                                    <DataRow label="Rev Growth" value={stock.revenueGrowth ? `${stock.revenueGrowth.toFixed(1)}%` : 'N/A'} color={stock.revenueGrowth && stock.revenueGrowth > 0 ? 'text-green-500' : 'text-red-500'} />
                                                                    <DataRow label="EPS Growth" value={stock.earningsGrowth ? `${stock.earningsGrowth.toFixed(1)}%` : 'N/A'} color={stock.earningsGrowth && stock.earningsGrowth > 0 ? 'text-green-500' : 'text-red-500'} />
                                                                </div>
                                                            </div>

                                                            <div className="space-y-3">
                                                                <h4 className="text-xs font-mono font-semibold uppercase flex items-center gap-2 text-muted-foreground border-b border-border/30 pb-1">
                                                                    <Target className="h-3.5 w-3.5" /> Technicals
                                                                </h4>
                                                                <div className="space-y-1.5">
                                                                    <DataRow label="Beta" value={stock.beta.toFixed(2)} />
                                                                    <DataRow label="RSI (14)" value={stock.rsi14.toFixed(0)} color={stock.rsi14 > 70 ? 'text-red-500' : stock.rsi14 < 30 ? 'text-green-500' : undefined} />
                                                                    <DataRow label="Volatility" value={`${stock.volatility52W.toFixed(1)}%`} />
                                                                    <DataRow label="Avg Vol" value={formatVolume(stock.avgVolume)} />
                                                                </div>
                                                            </div>
                                                        </div>

                                                        {/* Risk Footer */}
                                                        {(stock.highVolatility || stock.negativeEarnings || stock.lowLiquidity) && (
                                                            <div className="col-span-12 flex items-center gap-4 pt-2 border-t border-dashed border-border/50">
                                                                <div className="flex items-center gap-1.5 text-yellow-600 dark:text-yellow-500">
                                                                    <AlertTriangle className="h-4 w-4" />
                                                                    <span className="text-xs font-bold font-mono uppercase">Risk Flags Detected</span>
                                                                </div>
                                                                <div className="flex gap-2">
                                                                    {stock.highVolatility && <Badge variant="destructive" className="font-mono text-[10px] uppercase">High Volatility</Badge>}
                                                                    {stock.negativeEarnings && <Badge variant="destructive" className="font-mono text-[10px] uppercase">Negative Earnings</Badge>}
                                                                    {stock.lowLiquidity && <Badge variant="destructive" className="font-mono text-[10px] uppercase">Low Liquidity</Badge>}
                                                                </div>
                                                            </div>
                                                        )}
                                                    </div>
                                                </TableCell>
                                            </TableRow>
                                        )}
                                    </>
                                ))
                            )}
                        </TableBody>
                    </Table>
                </div>
            </div>
        </TooltipProvider>
    );
}

function DataRow({ label, value, highlight, color }: { label: string, value: string, highlight?: boolean, color?: string }) {
    return (
        <div className="flex justify-between items-center text-xs">
            <span className="text-muted-foreground">{label}</span>
            <span className={`font-mono ${highlight ? 'font-bold text-foreground' : ''} ${color || 'text-foreground'}`}>{value}</span>
        </div>
    );
}
