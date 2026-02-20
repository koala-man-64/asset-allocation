import type {
  Alert,
  AlertConfig,
  ExecutionMetrics,
  FinanceData,
  MarketData,
  Order,
  Position,
  RiskMetrics
} from '@/types/data';
import type { DomainMetadata, SystemHealth } from '@/types/strategy';
import type {
  ContainerAppLogsResponse,
  ContainerAppControlResponse,
  ContainerAppsStatusResponse,
  DomainColumnsResponse,
  DebugSymbolsResponse,
  JobLogsResponse,
  PurgeRequest,
  PurgeOperationResponse,
  ResponseWithMeta,
  RuntimeConfigCatalogResponse,
  RuntimeConfigItem,
  RuntimeConfigListResponse,
  PurgeCandidatesResponse,
  ValidationReport,
  SymbolSyncState,
  DataProfilingResponse,
  StorageUsageResponse
} from '@/services/apiService';
import type { StockScreenerResponse } from '@/services/apiService';
import { apiService } from '@/services/apiService';

export type { FinanceData, MarketData };

const ENABLE_DATA_SERVICE_LOGS =
  import.meta.env.DEV ||
  ['1', 'true', 'yes', 'y', 'on'].includes(
    String(import.meta.env.VITE_DEBUG_API ?? '')
      .trim()
      .toLowerCase()
  );

function dataServiceInfo(message: string, meta: Record<string, unknown> = {}): void {
  if (!ENABLE_DATA_SERVICE_LOGS) return;
  if (Object.keys(meta).length > 0) {
    console.info(message, meta);
    return;
  }
  console.info(message);
}

export const DataService = {
  getMarketData(
    ticker: string,
    layer: 'silver' | 'gold' = 'silver',
    signal?: AbortSignal
  ): Promise<MarketData[]> {
    return apiService.getMarketData(ticker, layer, signal);
  },

  getFinanceData(
    ticker: string,
    subDomain: string,
    layer: 'silver' | 'gold' = 'silver',
    signal?: AbortSignal
  ): Promise<FinanceData[]> {
    return apiService.getFinanceData(ticker, subDomain, layer, signal);
  },

  async getSystemHealth(params: { refresh?: boolean } = {}): Promise<SystemHealth> {
    dataServiceInfo('[DataService] getSystemHealth');
    try {
      const data = await apiService.getSystemHealth(params);
      dataServiceInfo('[DataService] getSystemHealth success', {
        overall: data?.overall,
        layers: data?.dataLayers?.length ?? 0,
        alerts: data?.alerts?.length ?? 0
      });
      return data;
    } catch (error) {
      console.error('[DataService] getSystemHealth error', error);
      throw error;
    }
  },

  async getSystemHealthWithMeta(
    params: { refresh?: boolean } = {}
  ): Promise<ResponseWithMeta<SystemHealth>> {
    dataServiceInfo('[DataService] getSystemHealthWithMeta');
    try {
      const response = await apiService.getSystemHealthWithMeta(params);
      dataServiceInfo('[DataService] getSystemHealthWithMeta success', {
        overall: response.data?.overall,
        layers: response.data?.dataLayers?.length ?? 0,
        alerts: response.data?.alerts?.length ?? 0,
        status: response.meta.status,
        durationMs: response.meta.durationMs,
        cacheHint: response.meta.cacheHint,
        cacheDegraded: response.meta.cacheDegraded,
        stale: response.meta.stale,
        requestId: response.meta.requestId
      });
      return response;
    } catch (error) {
      console.error('[DataService] getSystemHealthWithMeta error', error);
      throw error;
    }
  },

  getDomainMetadata(
    layer: 'bronze' | 'silver' | 'gold' | 'platinum',
    domain: string,
    params: { refresh?: boolean; cacheOnly?: boolean } = {}
  ): Promise<DomainMetadata> {
    return apiService.getDomainMetadata(layer, domain, params);
  },

  getDomainColumns(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string
  ): Promise<DomainColumnsResponse> {
    return apiService.getDomainColumns(layer, domain);
  },

  refreshDomainColumns(payload: {
    layer: 'bronze' | 'silver' | 'gold';
    domain: string;
    sample_limit?: number;
  }): Promise<DomainColumnsResponse> {
    return apiService.refreshDomainColumns(payload);
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
    signal?: AbortSignal
  ): Promise<JobLogsResponse> {
    return apiService.getJobLogs(jobName, params, signal);
  },

  getContainerApps(
    params: { probe?: boolean } = {},
    signal?: AbortSignal
  ): Promise<ContainerAppsStatusResponse> {
    return apiService.getContainerApps(params, signal);
  },

  startContainerApp(appName: string, signal?: AbortSignal): Promise<ContainerAppControlResponse> {
    return apiService.startContainerApp(appName, signal);
  },

  stopContainerApp(appName: string, signal?: AbortSignal): Promise<ContainerAppControlResponse> {
    return apiService.stopContainerApp(appName, signal);
  },

  getContainerAppLogs(
    appName: string,
    params: { minutes?: number; tail?: number } = {},
    signal?: AbortSignal
  ): Promise<ContainerAppLogsResponse> {
    return apiService.getContainerAppLogs(appName, params, signal);
  },

  getStockScreener(
    params: {
      q?: string;
      limit?: number;
      offset?: number;
      asOf?: string;
      sort?: string;
      direction?: 'asc' | 'desc';
    } = {},
    signal?: AbortSignal
  ): Promise<StockScreenerResponse> {
    return apiService.getStockScreener(params, signal);
  },

  getGenericData(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string,
    ticker?: string,
    limit?: number,
    signal?: AbortSignal
  ): Promise<Record<string, unknown>[]> {
    return apiService.getGenericData(layer, domain, ticker, limit, signal);
  },

  getDataQualityValidation(
    layer: string,
    domain: string,
    tickerOrSignal?: string | AbortSignal,
    signal?: AbortSignal
  ): Promise<ValidationReport> {
    return apiService.getDataQualityValidation(layer, domain, tickerOrSignal, signal);
  },

  getDataProfile(
    layer: 'bronze' | 'silver' | 'gold',
    domain: string,
    column: string,
    params: {
      ticker?: string;
      bins?: number;
      sampleRows?: number;
      topValues?: number;
    } = {},
    signal?: AbortSignal
  ): Promise<DataProfilingResponse> {
    return apiService.getDataProfile(layer, domain, column, params, signal);
  },

  getStorageUsage(signal?: AbortSignal): Promise<StorageUsageResponse> {
    return apiService.getStorageUsage(signal);
  },

  purgeData(payload: PurgeRequest): Promise<PurgeOperationResponse> {
    return apiService.purgeData(payload);
  },

  getPurgeOperation(operationId: string): Promise<PurgeOperationResponse> {
    return apiService.getPurgeOperation(operationId);
  },

  getPurgeCandidates(
    payload: Parameters<typeof apiService.getPurgeCandidates>[0]
  ): Promise<PurgeCandidatesResponse> {
    return apiService.getPurgeCandidates(payload);
  },

  purgeSymbolsBatch(payload: Parameters<typeof apiService.purgeSymbolsBatch>[0]): Promise<PurgeOperationResponse> {
    return apiService.purgeSymbolsBatch(payload);
  },

  getDebugSymbols(): Promise<DebugSymbolsResponse> {
    return apiService.getDebugSymbols();
  },

  setDebugSymbols(payload: { enabled: boolean; symbols?: string }): Promise<DebugSymbolsResponse> {
    return apiService.setDebugSymbols(payload);
  },

  getRuntimeConfigCatalog(): Promise<RuntimeConfigCatalogResponse> {
    return apiService.getRuntimeConfigCatalog();
  },

  getRuntimeConfig(scope: string = 'global'): Promise<RuntimeConfigListResponse> {
    return apiService.getRuntimeConfig(scope);
  },

  setRuntimeConfig(payload: {
    key: string;
    scope?: string;
    enabled: boolean;
    value: string;
    description?: string;
  }): Promise<RuntimeConfigItem> {
    return apiService.setRuntimeConfig(payload);
  },

  deleteRuntimeConfig(
    key: string,
    scope: string = 'global'
  ): Promise<{ scope: string; key: string; deleted: boolean }> {
    return apiService.deleteRuntimeConfig(key, scope);
  },

  getSymbolSyncState(): Promise<SymbolSyncState> {
    return apiService.getSymbolSyncState();
  }
};
