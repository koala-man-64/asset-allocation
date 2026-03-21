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
  entryPrice?: number;
  exitPrice?: number;
  exitReason?: string;
  exitRuleId?: string;
  barsHeld?: number;
  intrabarConflictCount?: number;
}

export interface Contribution {
  name: string; // symbol, sector, or factor
  type: 'symbol' | 'sector' | 'factor';
  contribution: number; // total P&L
}

export type ExitRuleType =
  | 'stop_loss_fixed'
  | 'take_profit_fixed'
  | 'trailing_stop_pct'
  | 'trailing_stop_atr'
  | 'time_stop';

export type ExitRuleScope = 'position';
export type ExitRuleAction = 'exit_full';
export type ExitRulePriceField = 'open' | 'high' | 'low' | 'close';
export type ExitRuleReference = 'entry_price' | 'highest_since_entry';
export type IntrabarConflictPolicy = 'stop_first' | 'take_profit_first' | 'priority_order';
export type RegimeCode =
  | 'trending_bull'
  | 'trending_bear'
  | 'choppy_mean_reversion'
  | 'high_vol'
  | 'unclassified';
export type RegimeBlockedAction = 'skip_entries' | 'skip_rebalance';
export type UniverseSource = 'postgres_gold';
export type UniverseGroupOperator = 'and' | 'or';
export type UniverseConditionOperator =
  | 'eq'
  | 'ne'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'in'
  | 'not_in'
  | 'is_null'
  | 'is_not_null';
export type UniverseValue = string | number | boolean;
export type UniverseValueKind = 'string' | 'number' | 'boolean' | 'date' | 'datetime';
export type RankingTransformType =
  | 'percentile_rank'
  | 'zscore'
  | 'minmax'
  | 'clip'
  | 'winsorize'
  | 'coalesce'
  | 'log1p'
  | 'negate'
  | 'abs';
export type RankingDirection = 'asc' | 'desc';
export type RankingMissingValuePolicy = 'exclude' | 'zero';
export type RankingCatalogValueKind = 'number' | 'boolean';

export interface ExitRule {
  id: string;
  type: ExitRuleType;
  scope: ExitRuleScope;
  priceField?: ExitRulePriceField;
  value?: number;
  atrColumn?: string;
  priority?: number;
  action: ExitRuleAction;
  minHoldBars: number;
  reference?: ExitRuleReference;
}

export interface UniverseCondition {
  kind: 'condition';
  table: string;
  column: string;
  operator: UniverseConditionOperator;
  value?: UniverseValue;
  values?: UniverseValue[];
}

export interface UniverseGroup {
  kind: 'group';
  operator: UniverseGroupOperator;
  clauses: UniverseNode[];
}

export type UniverseNode = UniverseGroup | UniverseCondition;

export interface UniverseDefinition {
  source: UniverseSource;
  root: UniverseGroup;
}

export interface UniverseCatalogColumn {
  name: string;
  dataType: string;
  valueKind: UniverseValueKind;
  operators: UniverseConditionOperator[];
}

export interface UniverseCatalogTable {
  name: string;
  asOfColumn: string;
  columns: UniverseCatalogColumn[];
}

export interface UniverseCatalogResponse {
  source: UniverseSource;
  tables: UniverseCatalogTable[];
}

export interface UniversePreviewResponse {
  source: UniverseSource;
  symbolCount: number;
  sampleSymbols: string[];
  tablesUsed: string[];
  warnings: string[];
}

export interface TargetGrossExposureByRegime {
  trending_bull: number;
  trending_bear: number;
  choppy_mean_reversion: number;
  high_vol: number;
  unclassified: number;
}

export interface RegimePolicy {
  modelName: string;
  targetGrossExposureByRegime: TargetGrossExposureByRegime;
  blockOnTransition: boolean;
  blockOnUnclassified: boolean;
  honorHaltFlag: boolean;
  onBlocked: RegimeBlockedAction;
}

export interface StrategyConfig {
  universeConfigName?: string;
  universe?: UniverseDefinition;
  rebalance: string;
  longOnly: boolean;
  topN: number;
  lookbackWindow: number;
  holdingPeriod: number;
  costModel: string;
  rankingSchemaName?: string;
  intrabarConflictPolicy: IntrabarConflictPolicy;
  regimePolicy?: RegimePolicy;
  exits: ExitRule[];
}

export interface StrategySummary {
  name: string;
  type: string;
  description?: string;
  output_table_name?: string;
  updated_at?: string;
}

export interface StrategyDetail extends StrategySummary {
  config: StrategyConfig;
}

export interface RankingTransform {
  type: RankingTransformType;
  params: Record<string, string | number | boolean | null>;
}

export interface RankingFactor {
  name: string;
  table: string;
  column: string;
  weight: number;
  direction: RankingDirection;
  missingValuePolicy: RankingMissingValuePolicy;
  transforms: RankingTransform[];
}

export interface RankingGroup {
  name: string;
  weight: number;
  factors: RankingFactor[];
  transforms: RankingTransform[];
}

export interface RankingSchemaConfig {
  universeConfigName?: string;
  groups: RankingGroup[];
  overallTransforms: RankingTransform[];
}

export interface UniverseConfigSummary {
  name: string;
  description?: string;
  version: number;
  updated_at?: string;
}

export interface UniverseConfigDetail extends UniverseConfigSummary {
  config: UniverseDefinition;
}

export interface RankingSchemaSummary {
  name: string;
  description?: string;
  version: number;
  updated_at?: string;
}

export interface RankingSchemaDetail extends RankingSchemaSummary {
  config: RankingSchemaConfig;
}

export interface RankingCatalogColumn {
  name: string;
  dataType: string;
  valueKind: RankingCatalogValueKind;
}

export interface RankingCatalogTable {
  name: string;
  asOfColumn: string;
  columns: RankingCatalogColumn[];
}

export interface RankingCatalogResponse {
  source: UniverseSource;
  tables: RankingCatalogTable[];
}

export interface RankingPreviewRow {
  symbol: string;
  rank: number;
  score: number;
}

export interface RankingPreviewResponse {
  strategyName: string;
  asOfDate: string;
  rowCount: number;
  rows: RankingPreviewRow[];
  warnings: string[];
}

export interface RankingMaterializationSummary {
  runId: string;
  strategyName: string;
  rankingSchemaName: string;
  rankingSchemaVersion: number;
  outputTableName: string;
  startDate?: string;
  endDate?: string;
  rowCount: number;
  dateCount: number;
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
  source?: 'partition' | 'stats' | 'artifact' | null;
}

export interface DomainMetadata {
  layer: 'bronze' | 'silver' | 'gold' | 'platinum';
  domain: string;
  container: string;
  type: 'blob' | 'delta';
  computedAt: string;
  folderLastModified?: string | null;
  cachedAt?: string | null;
  cacheSource?: 'snapshot' | 'live-refresh' | null;
  symbolCount?: number | null;
  columns?: string[];
  columnCount?: number | null;
  financeSubfolderSymbolCounts?: Record<
    'balance_sheet' | 'income_statement' | 'cash_flow' | 'valuation',
    number
  > | null;
  blacklistedSymbolCount?: number | null;
  metadataPath?: string | null;
  metadataSource?: 'artifact' | 'scan' | null;
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
  status: 'success' | 'warning' | 'failed' | 'running' | 'pending';
  statusCode?: string;
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
}

export interface ResourceHealth {
  name: string;
  resourceType: string;
  status: 'healthy' | 'warning' | 'error' | 'unknown';
  lastChecked: string;
  details?: string;
  azureId?: string;
  runningState?: string;
  lastModifiedAt?: string | null;
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
