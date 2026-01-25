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

  async getSystemHealth(): Promise<SystemHealth> {
    console.info('[DataService] Fetching system health');
    try {
      const data = await backtestApi.getSystemHealth();
      console.info('[DataService] System health loaded', {
        overall: data.overall,
        dataLayers: data.dataLayers?.length ?? 0,
        alerts: data.alerts?.length ?? 0,
        resources: data.resources?.length ?? 0,
      });
      return data;
    } catch (err) {
      console.error('[DataService] System health failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  },

  async getLineage(): Promise<unknown> {
    console.info('[DataService] Fetching lineage');
    try {
      const data = await backtestApi.getLineage();
      const impacts = (data as { impactsByDomain?: unknown })?.impactsByDomain;
      console.info('[DataService] Lineage loaded', {
        domains: impacts && typeof impacts === 'object' ? Object.keys(impacts as object).length : 0,
      });
      return data;
    } catch (err) {
      console.error('[DataService] Lineage failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  },

  async getSignals(params: { date?: string; limit?: number } = {}): Promise<TradingSignal[]> {
    console.info('[DataService] Fetching signals', { params });
    try {
      const data = await backtestApi.getSignals(params);
      console.info('[DataService] Signals loaded', { count: data.length });
      return data;
    } catch (err) {
      console.error('[DataService] Signals failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
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
