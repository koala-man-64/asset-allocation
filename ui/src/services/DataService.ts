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
import type { JobLogsResponse, StockScreenerResponse } from '@/services/apiClient';
import { apiClient } from '@/services/apiClient';

export type { FinanceData, MarketData };

export const DataService = {
  getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
    return apiClient.getMarketData(ticker, layer);
  },

  getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
    return apiClient.getFinanceData(ticker, subDomain, layer);
  },

  getStrategies(): Promise<StrategyRun[]> {
    return apiClient.getStrategies();
  },

  async getSystemHealth(): Promise<SystemHealth> {
    console.info('[DataService] getSystemHealth');
    try {
      const data = await apiClient.getSystemHealth();
      console.info('[DataService] getSystemHealth success', {
        overall: data?.overall,
        layers: data?.dataLayers?.length ?? 0,
        alerts: data?.alerts?.length ?? 0,
      });
      return data;
    } catch (error) {
      console.error('[DataService] getSystemHealth error', error);
      throw error;
    }
  },

  getLineage(): Promise<unknown> {
    return apiClient.getLineage();
  },

  getSignals(params: { date?: string; limit?: number } = {}): Promise<TradingSignal[]> {
    return apiClient.getSignals(params);
  },

  getStressEvents(): Promise<StressEvent[]> {
    return apiClient.getStressEvents();
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

  getJobLogs(
    jobName: string,
    params: { runs?: number } = {},
    signal?: AbortSignal,
  ): Promise<JobLogsResponse> {
    return apiClient.getJobLogs(jobName, params, signal);
  },

  getStockScreener(
    params: { q?: string; limit?: number; offset?: number; asOf?: string; sort?: string; direction?: 'asc' | 'desc' } = {},
    signal?: AbortSignal,
  ): Promise<StockScreenerResponse> {
    return apiClient.getStockScreener(params, signal);
  },
};
