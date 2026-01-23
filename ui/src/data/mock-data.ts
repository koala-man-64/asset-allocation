import type {
  Drawdown,
  MonthlyReturn,
  RollingMetrics,
  StrategyRun,
  SystemHealth,
  TimeSeriesPoint,
  TradingSignal,
  StressEvent,
} from '@/types/strategy';

function makeSeries(dates: string[], values: number[]): TimeSeriesPoint[] {
  return dates.map((date, idx) => ({ date, value: values[idx] ?? 0 }));
}

function makeRollingMetrics(dates: string[]): RollingMetrics {
  return {
    sharpe: makeSeries(dates, [0.9, 1.1, 1.3, 1.0, 1.2]),
    volatility: makeSeries(dates, [18, 17, 19, 16, 18]),
    beta: makeSeries(dates, [0.95, 1.0, 1.05, 0.98, 1.02]),
    correlation: makeSeries(dates, [0.6, 0.55, 0.62, 0.58, 0.61]),
    maxDD: makeSeries(dates, [-4, -6, -8, -7, -5]),
    turnover: makeSeries(dates, [40, 45, 42, 38, 41]),
  };
}

function makeMonthlyReturns(): MonthlyReturn[] {
  return [
    { year: 2024, month: 1, return: 1.8 },
    { year: 2024, month: 2, return: -0.7 },
    { year: 2024, month: 3, return: 2.4 },
    { year: 2024, month: 4, return: 0.9 },
    { year: 2024, month: 5, return: -1.2 },
    { year: 2024, month: 6, return: 1.1 },
  ];
}

function makeStrategy(id: string, name: string): StrategyRun {
  const dates = ['2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01'];
  const equity = makeSeries(dates, [100, 103, 101, 107, 110]);
  const drawdown = makeSeries(dates, [0, -1.5, -3.2, -0.8, 0]);

  return {
    id,
    name,
    tags: ['mock', 'demo'],
    startDate: '2020-01-01',
    endDate: '2025-01-01',
    cagr: 18.4,
    annVol: 21.7,
    sharpe: 1.12,
    sortino: 1.58,
    calmar: 0.94,
    maxDD: -12.3,
    timeToRecovery: 61,
    turnoverAnn: 44,
    avgLeverage: 1.05,
    netGrossDelta: 12,
    betaToBenchmark: 1.02,
    avgCorrelation: 0.61,
    regimeFragility: false,
    costSensitive: true,
    tailRisk: false,
    equityCurve: equity,
    drawdownCurve: drawdown,
    monthlyReturns: makeMonthlyReturns(),
    rollingMetrics: makeRollingMetrics(dates),
    holdings: [
      { date: '2024-05-01', symbol: 'AAPL', weight: 0.12, sector: 'Technology', marketCap: 2800 },
      { date: '2024-05-01', symbol: 'MSFT', weight: 0.1, sector: 'Technology', marketCap: 3000 },
    ],
    trades: [
      { date: '2024-02-15', symbol: 'AAPL', side: 'BUY', shares: 100, price: 182.45, commission: 1, slippage: 2 },
      { date: '2024-04-10', symbol: 'AAPL', side: 'SELL', shares: 100, price: 195.2, commission: 1, slippage: 2, pnl: 1275, pnlPercent: 6.99 },
    ],
    contributions: [{ name: 'AAPL', type: 'symbol', contribution: 820 }],
    config: {
      universe: 'SP500',
      rebalance: 'monthly',
      longOnly: true,
      topN: 20,
      lookbackWindow: 252,
      holdingPeriod: 20,
      costModel: 'simple-bps',
    },
    audit: {
      gitSha: 'mock-git-sha',
      dataVersionId: 'mock-data-version',
      configHash: 'mock-config-hash',
      createdAt: '2024-06-15T00:00:00Z',
      runDate: '2024-06-15',
      warnings: [],
    },
  };
}

export const mockStrategies: StrategyRun[] = [
  makeStrategy('RUN-MOCK-001', 'Momentum Alpha (Mock)'),
  makeStrategy('RUN-MOCK-002', 'Quality + Value Tilt (Mock)'),
];

export const mockSystemHealth: SystemHealth = {
  overall: 'healthy',
  dataLayers: [
    {
      name: 'market/silver',
      description: 'Silver market data (daily bars + features)',
      status: 'healthy',
      lastUpdated: '2024-06-15T00:00:00Z',
      refreshFrequency: 'daily',
    },
    {
      name: 'ranking/platinum',
      description: 'Platinum ranking signals',
      status: 'stale',
      lastUpdated: '2024-06-10T00:00:00Z',
      refreshFrequency: 'daily',
      nextExpectedUpdate: '2024-06-16T00:00:00Z',
    },
  ],
  recentJobs: [
    {
      jobName: 'platinum-ranking-job',
      jobType: 'data-ingest',
      status: 'success',
      startTime: '2024-06-15T00:00:00Z',
      duration: 124,
      recordsProcessed: 12345,
      gitSha: 'mock-git-sha',
      triggeredBy: 'mock',
    },
  ],
  alerts: [
    {
      id: 'mock-alert-1',
      severity: 'warning',
      component: 'ranking',
      timestamp: '2024-06-15T00:00:00Z',
      message: 'Ranking signals have not been refreshed in 5 days.',
      acknowledged: false,
      title: 'Ranking stale',
    },
  ],
};

export const mockSignals: TradingSignal[] = [
  {
    id: 'SIG-001',
    date: '2024-06-14',
    symbol: 'AAPL',
    strategyName: 'Momentum Alpha (Mock)',
    signalType: 'ENTRY',
    direction: 'LONG',
    strength: 0.72,
    confidence: 0.63,
  },
  {
    id: 'SIG-002',
    date: '2024-06-14',
    symbol: 'MSFT',
    strategyName: 'Quality + Value Tilt (Mock)',
    signalType: 'EXIT',
    direction: 'FLAT',
    strength: 0.41,
    confidence: 0.52,
  },
];

export const stressEvents: StressEvent[] = [
  { name: 'COVID Crash', date: '2020-03-16', strategyReturn: -9.4, benchmarkReturn: -12.0 },
  { name: '2022 Rate Shock', date: '2022-06-13', strategyReturn: -3.1, benchmarkReturn: -3.9 },
];

export function getTopDrawdowns(strategy: StrategyRun): Drawdown[] {
  const points = strategy.drawdownCurve ?? [];
  if (points.length === 0) return [];

  const trough = points.reduce((min, p) => (p.value < min.value ? p : min), points[0]);
  const start = points[0];
  const end = points[points.length - 1];

  return [
    {
      startDate: start.date,
      troughDate: trough.date,
      endDate: end.date,
      depth: trough.value,
      duration: points.length * 30,
      recovery: trough.value < 0 ? points.length * 15 : undefined,
    },
  ];
}
