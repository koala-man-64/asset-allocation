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