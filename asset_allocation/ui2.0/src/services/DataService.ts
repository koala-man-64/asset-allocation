import { config } from '@/config';
import { MarketData, FinanceData } from '@/types/data'; // Ensure these types exist or are compatible
import { StrategyRun, SystemHealth, TradingSignal, StressEvent } from '@/types/strategy';
import { mockStrategies, mockSystemHealth, mockSignals, stressEvents } from '@/data/mockData';

// Re-export types if needed for compatibility
export type { MarketData, FinanceData };

export interface IDataService {
    // Existing methods
    getMarketData(ticker: string, layer?: 'silver' | 'gold'): Promise<MarketData[]>;
    getFinanceData(ticker: string, subDomain: string, layer?: 'silver' | 'gold'): Promise<FinanceData[]>;

    // New methods for UI 2.0
    getStrategies(): Promise<StrategyRun[]>;
    getSystemHealth(): Promise<SystemHealth>;
    getSignals(): Promise<TradingSignal[]>;
    getStressEvents(): Promise<StressEvent[]>;
}

class MockDataService implements IDataService {
    async getMarketData(ticker: string): Promise<MarketData[]> {
        await new Promise(r => setTimeout(r, 500));
        // Return dummy data (existing logic)
        return Array.from({ length: 30 }, (_, i) => ({
            date: new Date(Date.now() - i * 86400000).toISOString().split('T')[0],
            open: 100 + Math.random() * 10,
            high: 110 + Math.random() * 10,
            low: 90 + Math.random() * 10,
            close: 105 + Math.random() * 10,
            volume: 1000000 + Math.random() * 100000
        })).reverse();
    }

    async getFinanceData(ticker: string, subDomain: string): Promise<FinanceData[]> {
        await new Promise(r => setTimeout(r, 500));
        return [{
            date: "2023-12-31",
            symbol: ticker,
            total_revenue: 1000000,
            net_income: 50000,
            sub_domain: subDomain
        }];
    }

    async getStrategies(): Promise<StrategyRun[]> {
        await new Promise(r => setTimeout(r, 600));
        return mockStrategies;
    }

    async getSystemHealth(): Promise<SystemHealth> {
        await new Promise(r => setTimeout(r, 300));
        return mockSystemHealth;
    }

    async getSignals(): Promise<TradingSignal[]> {
        await new Promise(r => setTimeout(r, 400));
        return mockSignals;
    }

    async getStressEvents(): Promise<StressEvent[]> {
        await new Promise(r => setTimeout(r, 200));
        return stressEvents;
    }
}

class ApiDataService implements IDataService {
    private baseUrl = `${config.apiBaseUrl}/data`;

    async getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
        const url = `${this.baseUrl}/${layer}/market?ticker=${ticker}`;
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Failed to fetch market data: ${response.statusText}`);
        return response.json();
    }

    async getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
        const url = `${this.baseUrl}/${layer}/finance/${subDomain}?ticker=${ticker}`;
        const response = await fetch(url);
        if (!response.ok) throw new Error(`Failed to fetch finance data: ${response.statusText}`);
        return response.json();
    }

    // New methods - Falling back to Mocks for now as backend is not ready
    async getStrategies(): Promise<StrategyRun[]> {
        console.warn('API endpoint for strategies not implemented. Returning mock data.');
        return mockStrategies;
    }

    async getSystemHealth(): Promise<SystemHealth> {
        console.warn('API endpoint for system health not implemented. Returning mock data.');
        return mockSystemHealth;
    }

    async getSignals(): Promise<TradingSignal[]> {
        console.warn('API endpoint for signals not implemented. Returning mock data.');
        return mockSignals;
    }

    async getStressEvents(): Promise<StressEvent[]> {
        console.warn('API endpoint for stress events not implemented. Returning mock data.');
        return stressEvents;
    }
}


class ProxyDataService implements IDataService {
    private mockService = new MockDataService();
    private apiService = new ApiDataService();
    private mode: 'mock' | 'live' = config.useMockData ? 'mock' : 'live';

    setMode(mode: 'mock' | 'live') {
        this.mode = mode;
        console.log(`[DataService] Switched to ${mode} mode`);
    }

    async getMarketData(ticker: string, layer?: 'silver' | 'gold'): Promise<MarketData[]> {
        return this.mode === 'mock' ? this.mockService.getMarketData(ticker) : this.apiService.getMarketData(ticker, layer);
    }

    async getFinanceData(ticker: string, subDomain: string, layer?: 'silver' | 'gold'): Promise<FinanceData[]> {
        return this.mode === 'mock' ? this.mockService.getFinanceData(ticker, subDomain) : this.apiService.getFinanceData(ticker, subDomain, layer);
    }

    async getStrategies(): Promise<StrategyRun[]> {
        return this.mode === 'mock' ? this.mockService.getStrategies() : this.apiService.getStrategies();
    }

    async getSystemHealth(): Promise<SystemHealth> {
        return this.mode === 'mock' ? this.mockService.getSystemHealth() : this.apiService.getSystemHealth();
    }

    async getSignals(): Promise<TradingSignal[]> {
        return this.mode === 'mock' ? this.mockService.getSignals() : this.apiService.getSignals();
    }

    async getStressEvents(): Promise<StressEvent[]> {
        return this.mode === 'mock' ? this.mockService.getStressEvents() : this.apiService.getStressEvents();
    }
}

export const DataService = new ProxyDataService();

