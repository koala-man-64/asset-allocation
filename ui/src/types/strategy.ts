// Core data types for the Strategy & Backtest Evaluation Dashboard

export interface StrategyRun {
  id: string;
  name: string;
  tags: string[];
  startDate: string;
  endDate: string;
  // Metrics
  cagr: number;
  annVol: number;
  sharpe: number;
  sortino: number;
  calmar: number;
  maxDD: number;
  timeToRecovery: number; // days
  turnoverAnn: number; // %
  avgLeverage: number;
  netGrossDelta: number; // bps
  betaToBenchmark: number;
  avgCorrelation: number;
  // Flags
  regimeFragility: boolean;
  costSensitive: boolean;
  tailRisk: boolean;
  // Series data
  equityCurve: TimeSeriesPoint[];
  drawdownCurve: TimeSeriesPoint[];
  monthlyReturns: MonthlyReturn[];
  rollingMetrics: RollingMetrics;
  // Holdings
  holdings: HoldingSnapshot[];
  trades: Trade[];
  // Attribution
  contributions: Contribution[];
  // Config
  config: StrategyConfig;
  // Audit
  audit: AuditTrail;
}

export interface TimeSeriesPoint {
  date: string;
  value: number;
}

export interface MonthlyReturn {
  year: number;
  month: number;
  return: number;
}

export interface RollingMetrics {
  sharpe: TimeSeriesPoint[];
  volatility: TimeSeriesPoint[];
  beta: TimeSeriesPoint[];
  correlation: TimeSeriesPoint[];
  maxDD: TimeSeriesPoint[];
  turnover: TimeSeriesPoint[];
}

export interface HoldingSnapshot {
  date: string;
  symbol: string;
  weight: number;
  sector: string;
  marketCap: number;
}

export interface Trade {
  date: string;
  symbol: string;
  side: 'BUY' | 'SELL';
  shares: number;
  price: number;
  commission: number;
  slippage: number;
  pnl?: number; // Realized P&L for this trade (for sells)
  pnlPercent?: number; // P&L as percentage of entry price
}

export interface Contribution {
  name: string; // symbol, sector, or factor
  type: 'symbol' | 'sector' | 'factor';
  contribution: number; // total P&L
}

export interface StrategyConfig {
  universe: string;
  rebalance: string;
  longOnly: boolean;
  topN: number;
  lookbackWindow: number;
  holdingPeriod: number;
  costModel: string;
}

export interface AuditTrail {
  gitSha: string;
  dataVersionId: string;
  configHash: string;
  createdAt: string;
  runDate: string;
  warnings: string[];
}

export interface Drawdown {
  startDate: string;
  troughDate: string;
  endDate?: string;
  depth: number;
  duration: number; // days
  recovery?: number; // days
}

export interface StressEvent {
  name: string;
  date: string;
  strategyReturn: number;
  benchmarkReturn: number;
}

export interface DataDomain {
  name: string;
  type: 'blob' | 'delta';
  path: string;
  lastUpdated: string | null;
  status: 'healthy' | 'stale' | 'error';
  version?: number | null;
  description?: string;
  portalUrl?: string;
  jobUrl?: string;
  jobName?: string;
  triggerUrl?: string;
  frequency?: string;
  cron?: string;
  maxAgeSeconds?: number;
}

export interface DomainDateRange {
  min?: string | null;
  max?: string | null;
  column?: string | null;
  source?: 'partition' | 'stats' | null;
}

export interface DomainMetadata {
  layer: 'bronze' | 'silver' | 'gold' | 'platinum';
  domain: string;
  container: string;
  type: 'blob' | 'delta';
  computedAt: string;
  cachedAt?: string | null;
  cacheSource?: 'snapshot' | 'live-refresh' | null;
  symbolCount?: number | null;
  blacklistedSymbolCount?: number | null;
  dateRange?: DomainDateRange | null;
  totalRows?: number | null;
  fileCount?: number | null;
  totalBytes?: number | null;
  deltaVersion?: number | null;
  tablePath?: string | null;
  prefix?: string | null;
  warnings?: string[];
}

export interface DataLayer {
  name: string;
  description: string;
  status: 'healthy' | 'stale' | 'error' | 'degraded' | 'critical' | 'warning';
  lastUpdated: string;
  dataVersion?: string;
  recordCount?: number;
  refreshFrequency: string;
  maxAgeSeconds?: number;
  nextExpectedUpdate?: string;
  domains?: DataDomain[];
  portalUrl?: string;
  jobUrl?: string;
  triggerUrl?: string;
}

export interface JobRun {
  jobName: string;
  jobType: 'backtest' | 'data-ingest' | 'attribution' | 'risk-calc' | 'portfolio-build';
  status: 'success' | 'failed' | 'running' | 'pending';
  startTime: string;
  duration?: number; // seconds
  recordsProcessed?: number;
  gitSha?: string;
  triggeredBy: string;
  errors?: string[];
  warnings?: string[];
}

export interface SystemAlert {
  id?: string;
  severity: 'critical' | 'error' | 'warning' | 'info';
  title?: string;
  component: string;
  timestamp: string;
  message: string;
  acknowledged: boolean;
  acknowledgedAt?: string | null;
  acknowledgedBy?: string | null;
  snoozedUntil?: string | null;
  resolvedAt?: string | null;
  resolvedBy?: string | null;
}

export interface ResourceHealth {
  name: string;
  resourceType: string;
  status: 'healthy' | 'warning' | 'error' | 'unknown';
  lastChecked: string;
  details?: string;
  azureId?: string;
  runningState?: string;
  signals?: ResourceSignal[];
}

export interface ResourceSignal {
  name: string;
  value: number | null;
  unit: string;
  timestamp: string;
  status: 'healthy' | 'warning' | 'error' | 'unknown';
  source?: 'metrics' | 'logs';
}

export interface SystemHealth {
  overall: 'healthy' | 'degraded' | 'critical';
  dataLayers: DataLayer[];
  recentJobs: JobRun[];
  alerts: SystemAlert[];
  resources?: ResourceHealth[];
}

export interface TradingSignal {
  id: string;
  date: string;
  generatedAt?: string;
  symbol: string;
  strategy?: string;
  strategyId?: string;
  strategyName?: string;
  signal?: number; // -1 to 1
  signalType?: string;
  strength?: number;
  confidence?: number;
  rank?: number;
  nSymbols?: number;
  score?: number | null;
  direction?: 'LONG' | 'SHORT' | 'FLAT';
  sector?: string;
  targetPrice?: number;
  stopLoss?: number;
  expectedReturn?: number;
  timeHorizon?: string;
  positionSize?: number;
  riskScore?: number;
  catalysts?: string[];
  currentPrice?: number;
  priceChange24h?: number;
}
