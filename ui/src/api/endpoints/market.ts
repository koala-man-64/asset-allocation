import { apiClient } from '@/api/client';
import { ApiResponse } from '@/api/types';

// TODO: Define strict interfaces for Market Data
export interface MarketData {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

export const marketApi = {
    /**
     * Fetch market data for a specific ticker
     */
    getData: async (layer: 'silver' | 'gold', ticker: string): Promise<MarketData[]> => {
        return apiClient.get<MarketData[]>(`/data/${layer}/market-data/${ticker}`);
    },

    /**
     * Get available tickers (if endpoint exists)
     */
    getTickers: async (): Promise<string[]> => {
        return apiClient.get<string[]>('/data/market/tickers');
    }
};
