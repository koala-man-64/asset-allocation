// Mock data generator for the Strategy & Backtest Evaluation Dashboard

import { StrategyRun, TimeSeriesPoint, MonthlyReturn, Drawdown, StressEvent, Trade } from '@/types/strategy';

// Helper to generate date series
function generateDateSeries(startDate: Date, endDate: Date, intervalDays: number = 1): Date[] {
  const dates: Date[] = [];
  let currentDate = new Date(startDate);
  while (currentDate <= endDate) {
    dates.push(new Date(currentDate));
    currentDate.setDate(currentDate.getDate() + intervalDays);
  }
  return dates;
}

// Generate realistic equity curve with drawdowns
function generateEquityCurve(
  startDate: Date,
  endDate: Date,
  cagr: number,
  volatility: number,
  maxDD: number
): TimeSeriesPoint[] {
  const dates = generateDateSeries(startDate, endDate);
  const curve: TimeSeriesPoint[] = [];
  let value = 100;
  
  const dailyReturn = cagr / 252;
  const dailyVol = volatility / Math.sqrt(252);
  
  dates.forEach((date, i) => {
    // Random walk with drift
    const shock = (Math.random() - 0.5) * 2 * dailyVol;
    const drift = dailyReturn;
    value *= (1 + drift + shock);
    
    // Inject periodic drawdowns
    if (i % 252 === 0 && i > 0) {
      value *= (1 - Math.random() * maxDD * 0.3);
    }
    
    curve.push({
      date: date.toISOString().split('T')[0],
      value: value
    });
  });
  
  return curve;
}

// Generate drawdown curve from equity curve
function generateDrawdownCurve(equityCurve: TimeSeriesPoint[]): TimeSeriesPoint[] {
  const drawdowns: TimeSeriesPoint[] = [];
  let peak = equityCurve[0].value;
  
  equityCurve.forEach(point => {
    if (point.value > peak) peak = point.value;
    const dd = ((point.value - peak) / peak) * 100;
    drawdowns.push({
      date: point.date,
      value: dd
    });
  });
  
  return drawdowns;
}

// Generate monthly returns
function generateMonthlyReturns(equityCurve: TimeSeriesPoint[]): MonthlyReturn[] {
  const monthlyReturns: MonthlyReturn[] = [];
  const monthlyValues: { [key: string]: number[] } = {};
  
  equityCurve.forEach(point => {
    const date = new Date(point.date);
    const key = `${date.getFullYear()}-${date.getMonth()}`;
    if (!monthlyValues[key]) monthlyValues[key] = [];
    monthlyValues[key].push(point.value);
  });
  
  Object.keys(monthlyValues).forEach(key => {
    const [year, month] = key.split('-').map(Number);
    const values = monthlyValues[key];
    const ret = ((values[values.length - 1] - values[0]) / values[0]) * 100;
    monthlyReturns.push({ year, month: month + 1, return: ret });
  });
  
  return monthlyReturns;
}

// Generate rolling metrics
function generateRollingMetrics(equityCurve: TimeSeriesPoint[]): any {
  const window = 63; // ~3 months
  const metrics = {
    sharpe: [] as TimeSeriesPoint[],
    volatility: [] as TimeSeriesPoint[],
    beta: [] as TimeSeriesPoint[],
    correlation: [] as TimeSeriesPoint[],
    maxDD: [] as TimeSeriesPoint[],
    turnover: [] as TimeSeriesPoint[]
  };
  
  for (let i = window; i < equityCurve.length; i++) {
    const date = equityCurve[i].date;
    metrics.sharpe.push({ date, value: 0.5 + Math.random() * 2 });
    metrics.volatility.push({ date, value: 10 + Math.random() * 15 });
    metrics.beta.push({ date, value: 0.5 + Math.random() * 1.5 });
    metrics.correlation.push({ date, value: 0.3 + Math.random() * 0.6 });
    metrics.maxDD.push({ date, value: -5 - Math.random() * 15 });
    metrics.turnover.push({ date, value: 50 + Math.random() * 200 });
  }
  
  return metrics;
}

// Generate realistic trade history
function generateTrades(equityCurve: TimeSeriesPoint[], turnover: number): Trade[] {
  const trades: Trade[] = [];
  const symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'JPM', 'V', 'WMT', 'UNH', 'JNJ', 'PG', 'MA', 'HD'];
  
  // Calculate approximate number of trades based on turnover
  // Higher turnover = more trades
  const tradesPerYear = Math.floor((turnover / 100) * 50); // Approximate trades per year
  const totalYears = equityCurve.length / 252;
  const totalTrades = Math.floor(tradesPerYear * totalYears);
  
  // Generate trades at semi-random intervals
  const interval = Math.floor(equityCurve.length / totalTrades);
  
  // Track position entry prices for P&L calculation
  const positionEntries: { [symbol: string]: number } = {};
  
  for (let i = 0; i < totalTrades; i++) {
    const dateIndex = Math.min(i * interval + Math.floor(Math.random() * interval), equityCurve.length - 1);
    const date = equityCurve[dateIndex].date;
    const symbol = symbols[Math.floor(Math.random() * symbols.length)];
    
    // Decide if this is a BUY or SELL
    // If we have a position, 50% chance to sell it, otherwise buy
    const hasPosition = positionEntries[symbol] !== undefined;
    const side: 'BUY' | 'SELL' = hasPosition && Math.random() > 0.5 ? 'SELL' : 'BUY';
    
    const shares = Math.floor(Math.random() * 500 + 100);
    const price = 50 + Math.random() * 300; // Random price between $50-$350
    const notional = shares * price;
    const commission = notional * 0.0005; // 5 bps
    const slippage = notional * 0.0002; // 2 bps
    
    let pnl: number | undefined = undefined;
    let pnlPercent: number | undefined = undefined;
    
    // Calculate P&L for SELL trades
    if (side === 'SELL' && hasPosition) {
      const entryPrice = positionEntries[symbol];
      const exitPrice = price;
      const grossPnL = (exitPrice - entryPrice) * shares;
      const costs = commission + slippage;
      pnl = grossPnL - costs;
      pnlPercent = ((exitPrice - entryPrice) / entryPrice) * 100;
      
      // Clear position
      delete positionEntries[symbol];
    } else if (side === 'BUY') {
      // Record entry price for future P&L calculation
      positionEntries[symbol] = price;
    }
    
    trades.push({
      date,
      symbol,
      side,
      shares,
      price,
      commission,
      slippage,
      pnl,
      pnlPercent
    });
  }
  
  // Sort trades by date
  trades.sort((a, b) => a.date.localeCompare(b.date));
  
  return trades;
}

// Create a single mock strategy run
function createMockStrategyRun(
  id: string,
  name: string,
  params: {
    cagr: number;
    volatility: number;
    sharpe: number;
    maxDD: number;
    turnover: number;
  }
): StrategyRun {
  const startDate = new Date('2020-01-01');
  const endDate = new Date('2025-01-01');
  
  const equityCurve = generateEquityCurve(startDate, endDate, params.cagr / 100, params.volatility / 100, params.maxDD / 100);
  const drawdownCurve = generateDrawdownCurve(equityCurve);
  const monthlyReturns = generateMonthlyReturns(equityCurve);
  const rollingMetrics = generateRollingMetrics(equityCurve);
  const trades = generateTrades(equityCurve, params.turnover);
  
  return {
    id,
    name,
    tags: ['Momentum', 'Long-Short', 'Equity'],
    startDate: startDate.toISOString().split('T')[0],
    endDate: endDate.toISOString().split('T')[0],
    cagr: params.cagr,
    annVol: params.volatility,
    sharpe: params.sharpe,
    sortino: params.sharpe * 1.2,
    calmar: params.cagr / Math.abs(params.maxDD),
    maxDD: params.maxDD,
    timeToRecovery: Math.floor(Math.random() * 180 + 30),
    turnoverAnn: params.turnover,
    avgLeverage: 1.0 + Math.random() * 0.5,
    netGrossDelta: Math.floor(Math.random() * 200 - 100),
    betaToBenchmark: 0.5 + Math.random() * 1.0,
    avgCorrelation: 0.3 + Math.random() * 0.4,
    regimeFragility: params.sharpe < 1.0,
    costSensitive: params.turnover > 300,
    tailRisk: params.maxDD < -30,
    equityCurve,
    drawdownCurve,
    monthlyReturns,
    rollingMetrics,
    holdings: [],
    trades,
    contributions: [
      { name: 'AAPL', type: 'symbol', contribution: 15000 },
      { name: 'MSFT', type: 'symbol', contribution: 12000 },
      { name: 'GOOGL', type: 'symbol', contribution: 8500 },
      { name: 'AMZN', type: 'symbol', contribution: -3200 },
      { name: 'Technology', type: 'sector', contribution: 25000 },
      { name: 'Healthcare', type: 'sector', contribution: 8000 },
    ],
    config: {
      universe: 'S&P 500',
      rebalance: 'Weekly',
      longOnly: false,
      topN: 50,
      lookbackWindow: 20,
      holdingPeriod: 5,
      costModel: 'Passive'
    },
    audit: {
      gitSha: 'a1b2c3d4e5f6',
      dataVersionId: 'v2024.12.01',
      configHash: 'cfg_' + id,
      createdAt: new Date().toISOString(),
      warnings: []
    }
  };
}

// Generate portfolio of mock strategies
export const mockStrategies: StrategyRun[] = [
  createMockStrategyRun('run_001', 'Momentum Alpha v3', {
    cagr: 18.5,
    volatility: 16.2,
    sharpe: 1.85,
    maxDD: -12.3,
    turnover: 180
  }),
  createMockStrategyRun('run_002', 'Mean Reversion Quality', {
    cagr: 12.3,
    volatility: 10.5,
    sharpe: 1.45,
    maxDD: -8.7,
    turnover: 240
  }),
  createMockStrategyRun('run_003', 'Low Vol Defensive', {
    cagr: 8.9,
    volatility: 7.2,
    sharpe: 1.23,
    maxDD: -5.4,
    turnover: 60
  }),
  createMockStrategyRun('run_004', 'Factor Combo Long-Short', {
    cagr: 15.2,
    volatility: 18.5,
    sharpe: 0.82,
    maxDD: -22.1,
    turnover: 380
  }),
  createMockStrategyRun('run_005', 'Sector Rotation', {
    cagr: 10.7,
    volatility: 12.3,
    sharpe: 0.87,
    maxDD: -15.6,
    turnover: 120
  }),
  createMockStrategyRun('run_006', 'Trend Following', {
    cagr: 14.3,
    volatility: 14.8,
    sharpe: 0.97,
    maxDD: -18.2,
    turnover: 95
  }),
  createMockStrategyRun('run_007', 'Carry & Value', {
    cagr: 11.2,
    volatility: 9.8,
    sharpe: 1.14,
    maxDD: -11.3,
    turnover: 140
  }),
  createMockStrategyRun('run_008', 'High Frequency Alpha', {
    cagr: 22.1,
    volatility: 24.5,
    sharpe: 0.90,
    maxDD: -32.4,
    turnover: 780
  })
];

// Helper function to get top drawdowns from a strategy
export function getTopDrawdowns(strategy: StrategyRun): Drawdown[] {
  const drawdowns: Drawdown[] = [];
  let inDrawdown = false;
  let ddStart = '';
  let ddTrough = '';
  let ddDepth = 0;
  let troughValue = 0;
  
  strategy.drawdownCurve.forEach((point, idx) => {
    if (!inDrawdown && point.value < 0) {
      // Starting new drawdown
      inDrawdown = true;
      ddStart = point.date;
      ddTrough = point.date;
      ddDepth = point.value;
      troughValue = point.value;
    } else if (inDrawdown) {
      if (point.value < troughValue) {
        // New trough
        ddTrough = point.date;
        ddDepth = point.value;
        troughValue = point.value;
      }
      if (point.value >= 0) {
        // Drawdown recovered
        const startIdx = strategy.drawdownCurve.findIndex(p => p.date === ddStart);
        const troughIdx = strategy.drawdownCurve.findIndex(p => p.date === ddTrough);
        const duration = troughIdx - startIdx;
        const recovery = idx - troughIdx;
        
        drawdowns.push({
          startDate: ddStart,
          troughDate: ddTrough,
          endDate: point.date,
          depth: ddDepth,
          duration,
          recovery
        });
        
        inDrawdown = false;
      }
    }
  });
  
  // If still in drawdown at end
  if (inDrawdown) {
    const startIdx = strategy.drawdownCurve.findIndex(p => p.date === ddStart);
    const troughIdx = strategy.drawdownCurve.findIndex(p => p.date === ddTrough);
    const duration = troughIdx - startIdx;
    
    drawdowns.push({
      startDate: ddStart,
      troughDate: ddTrough,
      depth: ddDepth,
      duration
    });
  }
  
  // Sort by depth and return top 5
  return drawdowns.sort((a, b) => a.depth - b.depth).slice(0, 5);
}

// Additional mock data
export const gitSHA = 'a1b2c3d4e5f6';
export const dataVersion = 'v2024.12.01';
export const maxDDDate = '2022-10-12';
export const runId = 'run_001';

// Stress test events
export const stressEvents: StressEvent[] = [
  { name: 'COVID-19 Crash', date: '2020-03-16', strategyReturn: -8.5, benchmarkReturn: -12.0 },
  { name: 'Fed Rate Hike', date: '2022-03-16', strategyReturn: -3.2, benchmarkReturn: -2.8 },
  { name: 'SVB Collapse', date: '2023-03-10', strategyReturn: -1.8, benchmarkReturn: -3.5 },
  { name: 'China Evergrande', date: '2021-09-20', strategyReturn: -2.1, benchmarkReturn: -1.9 },
  { name: 'Ukraine Invasion', date: '2022-02-24', strategyReturn: -4.3, benchmarkReturn: -3.1 },
  { name: 'Tech Selloff 2022', date: '2022-11-10', strategyReturn: -5.2, benchmarkReturn: -4.8 },
  { name: 'Inflation Spike', date: '2021-11-10', strategyReturn: -1.5, benchmarkReturn: -0.9 },
  { name: 'Banking Crisis 2023', date: '2023-03-13', strategyReturn: -2.7, benchmarkReturn: -4.2 }
];