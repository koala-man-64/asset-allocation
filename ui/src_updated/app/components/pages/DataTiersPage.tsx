// Data Tiers Page - View Bronze/Silver/Gold/Platinum data layers

import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/app/components/ui/tabs';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
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
import { InfoTooltip } from '@/app/components/ui/metric-tooltip';
import {
  Database,
  Filter,
  Sparkles,
  Award,
  ArrowRight,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  Download,
  RefreshCw,
  Calendar,
  Clock,
  FileText,
  Layers,
  TrendingUp,
  Shield,
  Zap,
  FolderOpen
} from 'lucide-react';

// Mock data for different tiers
const bronzeData = [
  { 
    id: 1, 
    timestamp: '2024-01-20 09:30:01.234', 
    symbol: 'AAPL', 
    price: '182.45', 
    volume: '15432', 
    source: 'NYSE_Feed_A',
    raw_flags: 'T,LO,SLD',
    quality_score: null
  },
  { 
    id: 2, 
    timestamp: '2024-01-20 09:30:01.235', 
    symbol: 'AAPL', 
    price: '182.46', 
    volume: '8921', 
    source: 'NYSE_Feed_A',
    raw_flags: 'T',
    quality_score: null
  },
  { 
    id: 3, 
    timestamp: '2024-01-20 09:30:01.567', 
    symbol: 'MSFT', 
    price: '404.12', 
    volume: '21034', 
    source: 'NASDAQ_TotalView',
    raw_flags: 'T,LO',
    quality_score: null
  },
  { 
    id: 4, 
    timestamp: '2024-01-20 09:30:02.101', 
    symbol: 'AAPL', 
    price: null, 
    volume: '0', 
    source: 'NYSE_Feed_A',
    raw_flags: 'ERR',
    quality_score: null
  },
  { 
    id: 5, 
    timestamp: '2024-01-20 09:30:02.234', 
    symbol: 'GOOGL', 
    price: '142.89', 
    volume: '12453', 
    source: 'NASDAQ_TotalView',
    raw_flags: 'T',
    quality_score: null
  },
];

const silverData = [
  { 
    id: 1, 
    timestamp: '2024-01-20T09:30:01.234Z', 
    symbol: 'AAPL', 
    price: 182.45, 
    volume: 15432, 
    source: 'NYSE',
    is_valid: true,
    outlier_flag: false,
    data_quality: 0.98
  },
  { 
    id: 2, 
    timestamp: '2024-01-20T09:30:01.235Z', 
    symbol: 'AAPL', 
    price: 182.46, 
    volume: 8921, 
    source: 'NYSE',
    is_valid: true,
    outlier_flag: false,
    data_quality: 0.99
  },
  { 
    id: 3, 
    timestamp: '2024-01-20T09:30:01.567Z', 
    symbol: 'MSFT', 
    price: 404.12, 
    volume: 21034, 
    source: 'NASDAQ',
    is_valid: true,
    outlier_flag: false,
    data_quality: 0.97
  },
  { 
    id: 4, 
    timestamp: '2024-01-20T09:30:02.234Z', 
    symbol: 'GOOGL', 
    price: 142.89, 
    volume: 12453, 
    source: 'NASDAQ',
    is_valid: true,
    outlier_flag: false,
    data_quality: 0.99
  },
];

const goldData = [
  {
    symbol: 'AAPL',
    date: '2024-01-20',
    open: 182.35,
    high: 184.22,
    low: 181.98,
    close: 183.45,
    volume: 48234521,
    vwap: 183.12,
    trades: 125432,
    returns_1d: 0.0082,
    volatility_20d: 0.0245
  },
  {
    symbol: 'MSFT',
    date: '2024-01-20',
    open: 403.89,
    high: 406.45,
    low: 403.12,
    close: 405.78,
    volume: 21456789,
    vwap: 404.92,
    trades: 89234,
    returns_1d: 0.0125,
    volatility_20d: 0.0198
  },
  {
    symbol: 'GOOGL',
    date: '2024-01-20',
    open: 142.45,
    high: 143.89,
    low: 142.12,
    close: 143.23,
    volume: 18923456,
    vwap: 143.01,
    trades: 72341,
    returns_1d: 0.0156,
    volatility_20d: 0.0267
  },
  {
    symbol: 'TSLA',
    date: '2024-01-20',
    open: 218.34,
    high: 223.45,
    low: 217.89,
    close: 221.67,
    volume: 95234567,
    vwap: 220.45,
    trades: 234561,
    returns_1d: 0.0234,
    volatility_20d: 0.0421
  },
];

const platinumData = [
  {
    symbol: 'AAPL',
    date: '2024-01-20',
    signal_score: 0.72,
    momentum_12_1: 0.185,
    value_score: 0.45,
    quality_score: 0.88,
    rank_percentile: 78,
    recommendation: 'BUY',
    target_weight: 4.2,
    risk_contribution: 2.8,
    predicted_return: 0.125
  },
  {
    symbol: 'MSFT',
    date: '2024-01-20',
    signal_score: 0.68,
    momentum_12_1: 0.156,
    value_score: 0.52,
    quality_score: 0.91,
    rank_percentile: 72,
    recommendation: 'BUY',
    target_weight: 3.8,
    risk_contribution: 2.3,
    predicted_return: 0.098
  },
  {
    symbol: 'GOOGL',
    date: '2024-01-20',
    signal_score: 0.58,
    momentum_12_1: 0.089,
    value_score: 0.67,
    quality_score: 0.85,
    rank_percentile: 65,
    recommendation: 'HOLD',
    target_weight: 2.5,
    risk_contribution: 1.9,
    predicted_return: 0.067
  },
  {
    symbol: 'TSLA',
    date: '2024-01-20',
    signal_score: 0.38,
    momentum_12_1: -0.045,
    value_score: 0.23,
    quality_score: 0.56,
    rank_percentile: 32,
    recommendation: 'SELL',
    target_weight: 0.0,
    risk_contribution: 0.0,
    predicted_return: -0.023
  },
];

export function DataTiersPage() {
  const [activeTab, setActiveTab] = useState('bronze');
  const [refreshing, setRefreshing] = useState(false);
  
  // Sub-folder selections for each tier
  const [bronzeDataset, setBronzeDataset] = useState('raw_trades');
  const [silverDataset, setSilverDataset] = useState('cleaned_trades');
  const [goldDataset, setGoldDataset] = useState('daily_bars');
  const [platinumDataset, setPlatinumDataset] = useState('signals');

  const handleRefresh = () => {
    setRefreshing(true);
    setTimeout(() => setRefreshing(false), 1500);
  };

  return (
    <TooltipProvider>
      <div className="space-y-6">
        {/* Page Header */}
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-3xl font-bold mb-2">Data Tier Architecture</h1>
            <p className="text-muted-foreground">
              View raw and processed data across Bronze, Silver, Gold, and Platinum layers
            </p>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={handleRefresh} disabled={refreshing}>
              <RefreshCw className={`h-4 w-4 mr-2 ${refreshing ? 'animate-spin' : ''}`} />
              Refresh All
            </Button>
            <Button variant="outline">
              <Download className="h-4 w-4 mr-2" />
              Export Schema
            </Button>
          </div>
        </div>

        {/* Data Flow Overview */}
        <Card className="border-primary/20 bg-gradient-to-r from-primary/5 to-purple-500/5">
          <CardHeader>
            <div className="flex items-center gap-2">
              <Layers className="h-5 w-5 text-primary" />
              <CardTitle>Data Processing Pipeline</CardTitle>
              <InfoTooltip
                content={
                  <div className="space-y-2">
                    <p className="font-semibold">Multi-Tier Data Architecture</p>
                    <p className="text-xs">Data flows through increasingly refined layers, from raw ingestion to production-ready signals.</p>
                  </div>
                }
              />
            </div>
          </CardHeader>
          <CardContent>
            <div className="flex items-center justify-between gap-4">
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  <Database className="h-5 w-5 text-amber-600" />
                  <span className="font-semibold">Bronze</span>
                </div>
                <p className="text-sm text-muted-foreground">Raw ingestion from exchanges</p>
              </div>
              <ArrowRight className="h-5 w-5 text-muted-foreground flex-shrink-0" />
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  <Filter className="h-5 w-5 text-gray-400" />
                  <span className="font-semibold">Silver</span>
                </div>
                <p className="text-sm text-muted-foreground">Cleaned & validated</p>
              </div>
              <ArrowRight className="h-5 w-5 text-muted-foreground flex-shrink-0" />
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  <Sparkles className="h-5 w-5 text-yellow-500" />
                  <span className="font-semibold">Gold</span>
                </div>
                <p className="text-sm text-muted-foreground">Aggregated OHLCV bars</p>
              </div>
              <ArrowRight className="h-5 w-5 text-muted-foreground flex-shrink-0" />
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  <Award className="h-5 w-5 text-purple-500" />
                  <span className="font-semibold">Platinum</span>
                </div>
                <p className="text-sm text-muted-foreground">Strategy-ready signals</p>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Quality Metrics Overview */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Database className="h-4 w-4 text-amber-600" />
                Bronze Records
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">2.4M</div>
              <p className="text-xs text-muted-foreground mt-1">Last 24 hours</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Shield className="h-4 w-4 text-gray-400" />
                Data Quality
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-600">98.7%</div>
              <p className="text-xs text-muted-foreground mt-1">Silver validation rate</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Clock className="h-4 w-4 text-yellow-500" />
                Pipeline Latency
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">142ms</div>
              <p className="text-xs text-muted-foreground mt-1">Bronze → Platinum</p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
                <Zap className="h-4 w-4 text-purple-500" />
                Signal Coverage
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">1,247</div>
              <p className="text-xs text-muted-foreground mt-1">Securities with signals</p>
            </CardContent>
          </Card>
        </div>

        {/* Data Tier Tabs */}
        <Card>
          <CardContent className="pt-6">
            <Tabs value={activeTab} onValueChange={setActiveTab}>
              <TabsList className="grid w-full grid-cols-4 mb-6">
                <TabsTrigger value="bronze" className="flex items-center gap-2">
                  <Database className="h-4 w-4" />
                  Bronze
                </TabsTrigger>
                <TabsTrigger value="silver" className="flex items-center gap-2">
                  <Filter className="h-4 w-4" />
                  Silver
                </TabsTrigger>
                <TabsTrigger value="gold" className="flex items-center gap-2">
                  <Sparkles className="h-4 w-4" />
                  Gold
                </TabsTrigger>
                <TabsTrigger value="platinum" className="flex items-center gap-2">
                  <Award className="h-4 w-4" />
                  Platinum
                </TabsTrigger>
              </TabsList>

              {/* Bronze Tier */}
              <TabsContent value="bronze" className="space-y-4">
                <div className="flex items-start gap-3 p-4 bg-amber-50 border border-amber-200 rounded-lg">
                  <Database className="h-5 w-5 text-amber-600 mt-0.5" />
                  <div className="flex-1">
                    <h3 className="font-semibold text-amber-900 mb-1">Bronze Layer - Raw Data</h3>
                    <p className="text-sm text-amber-800">
                      Raw, unprocessed data directly from exchange feeds. No transformations applied. 
                      May contain errors, duplicates, and invalid records. Used for audit trails and reprocessing.
                    </p>
                    <div className="flex gap-4 mt-2 text-xs">
                      <span className="flex items-center gap-1">
                        <CheckCircle2 className="h-3 w-3" />
                        Complete history preserved
                      </span>
                      <span className="flex items-center gap-1">
                        <AlertTriangle className="h-3 w-3" />
                        Not validated
                      </span>
                      <span className="flex items-center gap-1">
                        <Clock className="h-3 w-3" />
                        Real-time ingestion
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <FolderOpen className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Dataset:</span>
                  </div>
                  <Select value={bronzeDataset} onValueChange={setBronzeDataset}>
                    <SelectTrigger className="w-72">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="raw_trades">raw_trades (Tick-level trades)</SelectItem>
                      <SelectItem value="raw_quotes">raw_quotes (Bid/Ask quotes)</SelectItem>
                      <SelectItem value="raw_market_data">raw_market_data (Indices, ETFs)</SelectItem>
                      <SelectItem value="raw_corporate_actions">raw_corporate_actions (Dividends, splits)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="rounded-md border overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>ID</TableHead>
                        <TableHead>
                          Timestamp
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Raw timestamp from exchange feed (microsecond precision)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead>Symbol</TableHead>
                        <TableHead>Price</TableHead>
                        <TableHead>Volume</TableHead>
                        <TableHead>
                          Source
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Exchange feed identifier</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead>
                          Raw Flags
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">T=Trade, LO=Limit Order, SLD=Sold, ERR=Error</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {bronzeData.map((row) => (
                        <TableRow key={row.id} className={row.raw_flags.includes('ERR') ? 'bg-red-50' : ''}>
                          <TableCell className="font-mono text-xs">{row.id}</TableCell>
                          <TableCell className="font-mono text-xs">{row.timestamp}</TableCell>
                          <TableCell className="font-semibold">{row.symbol}</TableCell>
                          <TableCell className={row.price === null ? 'text-red-500' : ''}>
                            {row.price || 'NULL'}
                          </TableCell>
                          <TableCell className="font-mono text-xs">{row.volume}</TableCell>
                          <TableCell className="text-xs">
                            <Badge variant="outline" className="text-xs">{row.source}</Badge>
                          </TableCell>
                          <TableCell className="font-mono text-xs">
                            <div className="flex gap-1">
                              {row.raw_flags.split(',').map((flag, idx) => (
                                <Badge 
                                  key={idx} 
                                  variant={flag === 'ERR' ? 'destructive' : 'secondary'}
                                  className="text-xs"
                                >
                                  {flag}
                                </Badge>
                              ))}
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                <div className="text-xs text-muted-foreground">
                  Showing 5 of 2,437,891 records from the last 24 hours
                </div>
              </TabsContent>

              {/* Silver Tier */}
              <TabsContent value="silver" className="space-y-4">
                <div className="flex items-start gap-3 p-4 bg-gray-50 border border-gray-200 rounded-lg">
                  <Filter className="h-5 w-5 text-gray-600 mt-0.5" />
                  <div className="flex-1">
                    <h3 className="font-semibold text-gray-900 mb-1">Silver Layer - Cleaned Data</h3>
                    <p className="text-sm text-gray-800">
                      Validated and cleaned data. Invalid records removed, timestamps normalized, duplicates eliminated. 
                      Outlier detection applied. Ready for aggregation and feature engineering.
                    </p>
                    <div className="flex gap-4 mt-2 text-xs">
                      <span className="flex items-center gap-1">
                        <CheckCircle2 className="h-3 w-3" />
                        Schema validated
                      </span>
                      <span className="flex items-center gap-1">
                        <Shield className="h-3 w-3" />
                        Quality scored
                      </span>
                      <span className="flex items-center gap-1">
                        <XCircle className="h-3 w-3" />
                        Errors filtered
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <FolderOpen className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Dataset:</span>
                  </div>
                  <Select value={silverDataset} onValueChange={setSilverDataset}>
                    <SelectTrigger className="w-72">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="cleaned_trades">cleaned_trades (Validated trades)</SelectItem>
                      <SelectItem value="cleaned_quotes">cleaned_quotes (Normalized quotes)</SelectItem>
                      <SelectItem value="validated_market_data">validated_market_data (QA-passed data)</SelectItem>
                      <SelectItem value="processed_corporate_actions">processed_corporate_actions (Standardized)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="rounded-md border overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>ID</TableHead>
                        <TableHead>
                          Timestamp (ISO)
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Normalized to ISO 8601 UTC format</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead>Symbol</TableHead>
                        <TableHead>Price</TableHead>
                        <TableHead>Volume</TableHead>
                        <TableHead>Source</TableHead>
                        <TableHead>
                          Valid
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Passed validation checks (schema, range, logic)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead>
                          Outlier
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Statistical outlier detection (Z-score &gt; 3)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead>
                          Quality
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Overall data quality score (0-1)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {silverData.map((row) => (
                        <TableRow key={row.id}>
                          <TableCell className="font-mono text-xs">{row.id}</TableCell>
                          <TableCell className="font-mono text-xs">{row.timestamp}</TableCell>
                          <TableCell className="font-semibold">{row.symbol}</TableCell>
                          <TableCell className="font-mono">${row.price.toFixed(2)}</TableCell>
                          <TableCell className="font-mono text-xs">{row.volume.toLocaleString()}</TableCell>
                          <TableCell>
                            <Badge variant="outline" className="text-xs">{row.source}</Badge>
                          </TableCell>
                          <TableCell>
                            {row.is_valid ? (
                              <CheckCircle2 className="h-4 w-4 text-green-600" />
                            ) : (
                              <XCircle className="h-4 w-4 text-red-600" />
                            )}
                          </TableCell>
                          <TableCell>
                            {row.outlier_flag ? (
                              <AlertTriangle className="h-4 w-4 text-orange-500" />
                            ) : (
                              <span className="text-muted-foreground">-</span>
                            )}
                          </TableCell>
                          <TableCell>
                            <div className="flex items-center gap-2">
                              <div className="w-16 h-2 bg-muted rounded-full overflow-hidden">
                                <div 
                                  className="h-full bg-green-500" 
                                  style={{ width: `${row.data_quality * 100}%` }}
                                />
                              </div>
                              <span className="text-xs font-mono">{(row.data_quality * 100).toFixed(0)}%</span>
                            </div>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                <div className="text-xs text-muted-foreground">
                  Showing 4 of 2,405,234 validated records (98.7% pass rate)
                </div>
              </TabsContent>

              {/* Gold Tier */}
              <TabsContent value="gold" className="space-y-4">
                <div className="flex items-start gap-3 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
                  <Sparkles className="h-5 w-5 text-yellow-600 mt-0.5" />
                  <div className="flex-1">
                    <h3 className="font-semibold text-yellow-900 mb-1">Gold Layer - Aggregated Data</h3>
                    <p className="text-sm text-yellow-800">
                      Business-ready aggregated data. Tick data rolled up into OHLCV bars with derived features like VWAP, returns, and volatility. 
                      Optimized for analytics and backtesting.
                    </p>
                    <div className="flex gap-4 mt-2 text-xs">
                      <span className="flex items-center gap-1">
                        <TrendingUp className="h-3 w-3" />
                        Time-series aggregated
                      </span>
                      <span className="flex items-center gap-1">
                        <CheckCircle2 className="h-3 w-3" />
                        Features computed
                      </span>
                      <span className="flex items-center gap-1">
                        <Database className="h-3 w-3" />
                        Analytics-ready
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <FolderOpen className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Dataset:</span>
                  </div>
                  <Select value={goldDataset} onValueChange={setGoldDataset}>
                    <SelectTrigger className="w-72">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="daily_bars">daily_bars (Daily OHLCV)</SelectItem>
                      <SelectItem value="intraday_5min_bars">intraday_5min_bars (5-minute bars)</SelectItem>
                      <SelectItem value="fundamentals">fundamentals (Financial statements)</SelectItem>
                      <SelectItem value="reference_data">reference_data (Security metadata)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="rounded-md border overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Symbol</TableHead>
                        <TableHead>Date</TableHead>
                        <TableHead className="text-right">
                          Open
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">First trade price of the day</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">High</TableHead>
                        <TableHead className="text-right">Low</TableHead>
                        <TableHead className="text-right">Close</TableHead>
                        <TableHead className="text-right">
                          Volume
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Total shares traded during the day</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          VWAP
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Volume-Weighted Average Price</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Return (1D)
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Daily return: (Close - Prev Close) / Prev Close</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Vol (20D)
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">20-day realized volatility</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {goldData.map((row) => (
                        <TableRow key={row.symbol}>
                          <TableCell className="font-semibold">{row.symbol}</TableCell>
                          <TableCell className="font-mono text-xs">{row.date}</TableCell>
                          <TableCell className="text-right font-mono">${row.open.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono">${row.high.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono">${row.low.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono font-semibold">${row.close.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono text-xs">{(row.volume / 1000000).toFixed(1)}M</TableCell>
                          <TableCell className="text-right font-mono">${row.vwap.toFixed(2)}</TableCell>
                          <TableCell className={`text-right font-mono font-semibold ${row.returns_1d >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {(row.returns_1d * 100).toFixed(2)}%
                          </TableCell>
                          <TableCell className="text-right font-mono text-xs">{(row.volatility_20d * 100).toFixed(2)}%</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                <div className="text-xs text-muted-foreground">
                  Showing 4 of 1,247 securities with complete OHLCV data
                </div>
              </TabsContent>

              {/* Platinum Tier */}
              <TabsContent value="platinum" className="space-y-4">
                <div className="flex items-start gap-3 p-4 bg-purple-50 border border-purple-200 rounded-lg">
                  <Award className="h-5 w-5 text-purple-600 mt-0.5" />
                  <div className="flex-1">
                    <h3 className="font-semibold text-purple-900 mb-1">Platinum Layer - Strategy Signals</h3>
                    <p className="text-sm text-purple-800">
                      Production-ready signals and scores. Multi-factor models applied, predictions generated, portfolio optimization complete. 
                      This is what strategies consume for live trading decisions.
                    </p>
                    <div className="flex gap-4 mt-2 text-xs">
                      <span className="flex items-center gap-1">
                        <Award className="h-3 w-3" />
                        Production-grade
                      </span>
                      <span className="flex items-center gap-1">
                        <TrendingUp className="h-3 w-3" />
                        Alpha signals
                      </span>
                      <span className="flex items-center gap-1">
                        <Shield className="h-3 w-3" />
                        Risk-adjusted
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <FolderOpen className="h-4 w-4 text-muted-foreground" />
                    <span className="text-sm font-medium">Dataset:</span>
                  </div>
                  <Select value={platinumDataset} onValueChange={setPlatinumDataset}>
                    <SelectTrigger className="w-72">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="signals">signals (Trading signals)</SelectItem>
                      <SelectItem value="factor_scores">factor_scores (Multi-factor rankings)</SelectItem>
                      <SelectItem value="portfolio_weights">portfolio_weights (Optimized allocations)</SelectItem>
                      <SelectItem value="risk_metrics">risk_metrics (Risk decomposition)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="rounded-md border overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Symbol</TableHead>
                        <TableHead>Date</TableHead>
                        <TableHead className="text-right">
                          Signal Score
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Composite signal strength (0-1)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Momentum
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">12-month momentum excluding last month</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Value
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Value score based on P/E, P/B, dividend yield</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Quality
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Quality score based on ROE, profit margins, debt</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Rank
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Percentile rank within universe</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead>
                          Recommendation
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Trading action: BUY/HOLD/SELL</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Target Weight
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Optimal portfolio weight (%)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                        <TableHead className="text-right">
                          Predicted Return
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help text-muted-foreground">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Expected return (next 30 days)</p>
                            </TooltipContent>
                          </Tooltip>
                        </TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {platinumData.map((row) => (
                        <TableRow key={row.symbol}>
                          <TableCell className="font-semibold">{row.symbol}</TableCell>
                          <TableCell className="font-mono text-xs">{row.date}</TableCell>
                          <TableCell className="text-right">
                            <div className="flex items-center justify-end gap-2">
                              <div className="w-16 h-2 bg-muted rounded-full overflow-hidden">
                                <div 
                                  className="h-full bg-primary" 
                                  style={{ width: `${row.signal_score * 100}%` }}
                                />
                              </div>
                              <span className="text-xs font-mono">{row.signal_score.toFixed(2)}</span>
                            </div>
                          </TableCell>
                          <TableCell className={`text-right font-mono ${row.momentum_12_1 >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {(row.momentum_12_1 * 100).toFixed(1)}%
                          </TableCell>
                          <TableCell className="text-right font-mono">{row.value_score.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono">{row.quality_score.toFixed(2)}</TableCell>
                          <TableCell className="text-right font-mono">
                            <Badge variant="outline">{row.rank_percentile}th</Badge>
                          </TableCell>
                          <TableCell>
                            <Badge 
                              className={
                                row.recommendation === 'BUY' ? 'bg-green-100 text-green-800 border-green-200' :
                                row.recommendation === 'SELL' ? 'bg-red-100 text-red-800 border-red-200' :
                                'bg-gray-100 text-gray-800 border-gray-200'
                              }
                            >
                              {row.recommendation}
                            </Badge>
                          </TableCell>
                          <TableCell className="text-right font-mono font-semibold">
                            {row.target_weight.toFixed(1)}%
                          </TableCell>
                          <TableCell className={`text-right font-mono font-semibold ${row.predicted_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {(row.predicted_return * 100).toFixed(1)}%
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>

                <div className="text-xs text-muted-foreground">
                  Showing 4 of 1,247 securities with platinum-tier signals updated every 15 minutes
                </div>
              </TabsContent>
            </Tabs>
          </CardContent>
        </Card>

        {/* Schema Documentation */}
        <Card>
          <CardHeader>
            <div className="flex items-center gap-2">
              <FileText className="h-5 w-5 text-muted-foreground" />
              <CardTitle>Schema Documentation</CardTitle>
              <InfoTooltip
                content={
                  <div className="space-y-2">
                    <p className="font-semibold">Data Schema Lineage</p>
                    <p className="text-xs">Each tier has defined schemas and data contracts to ensure consistency and reliability.</p>
                  </div>
                }
              />
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Database className="h-4 w-4 text-amber-600" />
                  <h4 className="font-semibold text-sm">Bronze Schema</h4>
                </div>
                <ul className="text-xs space-y-1 text-muted-foreground">
                  <li>• Raw string timestamps</li>
                  <li>• Nullable price fields</li>
                  <li>• Source identifiers</li>
                  <li>• Raw exchange flags</li>
                  <li>• No validation applied</li>
                </ul>
              </div>

              <div className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Filter className="h-4 w-4 text-gray-600" />
                  <h4 className="font-semibold text-sm">Silver Schema</h4>
                </div>
                <ul className="text-xs space-y-1 text-muted-foreground">
                  <li>• ISO 8601 timestamps</li>
                  <li>• Non-null constraints</li>
                  <li>• Boolean validation flags</li>
                  <li>• Quality scores (0-1)</li>
                  <li>• Outlier indicators</li>
                </ul>
              </div>

              <div className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Sparkles className="h-4 w-4 text-yellow-600" />
                  <h4 className="font-semibold text-sm">Gold Schema</h4>
                </div>
                <ul className="text-xs space-y-1 text-muted-foreground">
                  <li>• OHLCV bar data</li>
                  <li>• Computed features</li>
                  <li>• VWAP, returns, volatility</li>
                  <li>• Daily aggregation</li>
                  <li>• Time-series indexed</li>
                </ul>
              </div>

              <div className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Award className="h-4 w-4 text-purple-600" />
                  <h4 className="font-semibold text-sm">Platinum Schema</h4>
                </div>
                <ul className="text-xs space-y-1 text-muted-foreground">
                  <li>• Multi-factor signals</li>
                  <li>• Portfolio recommendations</li>
                  <li>• Risk contributions</li>
                  <li>• Predicted returns</li>
                  <li>• Production-ready</li>
                </ul>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </TooltipProvider>
  );
}