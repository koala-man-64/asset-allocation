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
import type { DomainMetadata, SystemHealth } from '@/types/strategy';
import type { DebugSymbolsResponse, JobLogsResponse, PurgeRequest, PurgeResponse } from '@/services/apiService';
import type { StockScreenerResponse } from '@/services/apiService';
import { apiService } from '@/services/apiService';

export type { FinanceData, MarketData };

export const DataService = {
  getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
    return apiService.getMarketData(ticker, layer);
  },

  getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
    return apiService.getFinanceData(ticker, subDomain, layer);
  },

  async getSystemHealth(params: { refresh?: boolean } = {}): Promise<SystemHealth> {
    console.info('[DataService] getSystemHealth');
    try {
      const data = await apiService.getSystemHealth(params);
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

  getDomainMetadata(
    layer: 'bronze' | 'silver' | 'gold' | 'platinum',
    domain: string,
    params: { refresh?: boolean } = {},
  ): Promise<DomainMetadata> {
    return apiService.getDomainMetadata(layer, domain, params);
  },

  getLineage(): Promise<unknown> {
    return apiService.getLineage();
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
    return apiService.getJobLogs(jobName, params, signal);
  },

  getStockScreener(
    params: { q?: string; limit?: number; offset?: number; asOf?: string; sort?: string; direction?: 'asc' | 'desc' } = {},
    signal?: AbortSignal,
  ): Promise<StockScreenerResponse> {
    return apiService.getStockScreener(params, signal);
  },

  getGenericData(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string,
    ticker?: string,
    limit?: number
  ): Promise<Record<string, unknown>[]> {
    return apiService.getGenericData(layer, domain, ticker, limit);
  },

  purgeData(payload: PurgeRequest): Promise<PurgeResponse> {
    return apiService.purgeData(payload);
  },

  getDebugSymbols(): Promise<DebugSymbolsResponse> {
    return apiService.getDebugSymbols();
  },

  setDebugSymbols(payload: { enabled: boolean; symbols?: string }): Promise<DebugSymbolsResponse> {
    return apiService.setDebugSymbols(payload);
  },
};
