import {
    FinanceData,
    MarketData,
} from '@/types/data';
import { StrategyRun, SystemHealth, TradingSignal, StressEvent } from '@/types/strategy';

interface WindowWithConfig extends Window {
    __API_UI_CONFIG__?: { apiBaseUrl?: string };
}
const runtimeConfig = (window as WindowWithConfig).__API_UI_CONFIG__ || {};
const API_BASE_URL = runtimeConfig.apiBaseUrl || '/api';

interface RequestConfig extends RequestInit {
    params?: Record<string, string | number | boolean | undefined>;
}

async function request<T>(endpoint: string, config: RequestConfig = {}): Promise<T> {
    const { params, headers, ...customConfig } = config;

    let url = `${API_BASE_URL}${endpoint}`;
    if (params) {
        const searchParams = new URLSearchParams();
        Object.entries(params).forEach(([key, value]) => {
            if (value !== undefined) {
                searchParams.append(key, String(value));
            }
        });
        const queryString = searchParams.toString();
        if (queryString) {
            url += `?${queryString}`;
        }
    }

    const response = await fetch(url, {
        headers: {
            'Content-Type': 'application/json',
            ...headers,
        },
        ...customConfig,
    });

    if (!response.ok) {
        const errorBody = await response.text();
        throw new Error(`API Error: ${response.status} ${response.statusText} - ${errorBody}`);
    }

    // Handle void response
    if (response.status === 204) {
        return {} as T;
    }

    return response.json();
}

export interface JobLogsResponse {
    logs: string[];
    offset: number;
    hasMore: boolean;
}

export interface StockScreenerResponse {
    items: unknown[];
    total: number;
    offset: number;
    limit: number;
}

export const apiService = {
    // --- Data Endpoints ---

    getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
        return request<MarketData[]>(`/data/market/${ticker}`, { params: { layer } });
    },

    getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
        return request<FinanceData[]>(`/data/finance/${ticker}/${subDomain}`, { params: { layer } });
    },

    // --- Ranking/Strategy Endpoints ---

    getStrategies(): Promise<StrategyRun[]> {
        return request<StrategyRun[]>('/ranking/strategies');
    },

    getSystemHealth(): Promise<SystemHealth> {
        return request<SystemHealth>('/system/health');
    },

    getLineage(): Promise<unknown> {
        return request<unknown>('/system/lineage');
    },

    getSignals(params: { date?: string; limit?: number } = {}): Promise<TradingSignal[]> {
        return request<TradingSignal[]>('/ranking/signals', { params });
    },

    getStressEvents(): Promise<StressEvent[]> {
        return request<StressEvent[]>('/ranking/stress-events');
    },

    getJobLogs(
        jobName: string,
        params: { runs?: number } = {},
        signal?: AbortSignal
    ): Promise<JobLogsResponse> {
        return request<JobLogsResponse>(`/system/jobs/${jobName}/logs`, {
            params,
            signal
        });
    },

    getStockScreener(
        params: { q?: string; limit?: number; offset?: number; asOf?: string; sort?: string; direction?: 'asc' | 'desc' } = {},
        signal?: AbortSignal
    ): Promise<StockScreenerResponse> {
        return request<StockScreenerResponse>('/data/screener', {
            params,
            signal
        });
    },

    getGenericData(
        layer: 'silver' | 'gold',
        domain: string,
        ticker?: string,
        limit?: number
    ): Promise<Record<string, unknown>[]> {
        const endpoint = `/data/${layer}/${domain}`;
        return request<Record<string, unknown>[]>(endpoint, {
            params: { ticker, limit }
        });
    },
};
