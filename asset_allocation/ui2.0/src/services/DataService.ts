import { config } from '../config';

// Define Interfaces (or import from types/data.ts)
export interface MarketData {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    // Add other fields from schema_definitions if needed
}

export interface FinanceData {
    date: string;
    symbol: string;
    // Common fields or intersection
    [key: string]: any;
}

export interface IDataService {
    getMarketData(ticker: string, layer?: 'silver' | 'gold'): Promise<MarketData[]>;
    getFinanceData(ticker: string, subDomain: string, layer?: 'silver' | 'gold'): Promise<FinanceData[]>;
}

class MockDataService implements IDataService {
    async getMarketData(ticker: string): Promise<MarketData[]> {
        await new Promise(r => setTimeout(r, 500));
        // Return dummy data
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
}

export const DataService: IDataService = config.useMockData
    ? new MockDataService()
    : new ApiDataService();
