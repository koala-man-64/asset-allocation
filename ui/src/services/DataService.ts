import type {
  Alert,
  AlertConfig,
  ExecutionMetrics,
  FinanceData,
  MarketData,
  Order,
  Position,
  RiskMetrics,
} from '@/types/data';
import type { StrategyRun, StressEvent, SystemHealth, TradingSignal } from '@/types/strategy';
import { backtestApi } from '@/services/backtestApi';

export type { FinanceData, MarketData };

export const DataService = {
  getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
    return backtestApi.getMarketData(ticker, layer);
  },

  getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
    return backtestApi.getFinanceData(ticker, subDomain, layer);
  },

  getStrategies(): Promise<StrategyRun[]> {
    return backtestApi.getStrategies();
  },

  getSystemHealth(): Promise<SystemHealth> {
    return backtestApi.getSystemHealth();
  },

  getLineage(): Promise<unknown> {
    return backtestApi.getLineage();
  },

  getSignals(params: { date?: string; limit?: number } = {}): Promise<TradingSignal[]> {
    return backtestApi.getSignals(params);
  },

  getStressEvents(): Promise<StressEvent[]> {
    return backtestApi.getStressEvents();
  },

  async getPositions(_strategyId?: string): Promise<Position[]> {
    return [];
  },

  async getOrders(_strategyId?: string): Promise<Order[]> {
    return [];
  },

  async getAlerts(): Promise<Alert[]> {
    return [];
  },

  async getAlertConfigs(): Promise<AlertConfig[]> {
    return [];
  },

  async getRiskMetrics(_strategyId: string): Promise<RiskMetrics> {
    throw new Error('Risk metrics are not available in this deployment.');
  },

  async getExecutionMetrics(_strategyId: string): Promise<ExecutionMetrics> {
    throw new Error('Execution metrics are not available in this deployment.');
  },
};
