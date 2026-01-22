import { config } from '@/config';
import { MarketData, FinanceData, Position, Order, Alert, AlertConfig, RiskMetrics, ExecutionMetrics } from '@/types/data';
import { StrategyRun, SystemHealth, TradingSignal, StressEvent } from '@/types/strategy';
import { backtestApi } from '@/services/backtestApi';
import { mockStrategies, mockSystemHealth, mockSignals, stressEvents } from '@/data/mock-data';

export type { MarketData, FinanceData };

export interface IDataService {
    getMarketData(ticker: string, layer?: 'silver' | 'gold'): Promise<MarketData[]>;
    getFinanceData(ticker: string, subDomain: string, layer?: 'silver' | 'gold'): Promise<FinanceData[]>;
    getStrategies(): Promise<StrategyRun[]>;
    getSystemHealth(): Promise<SystemHealth>;
    getLiveSystemHealth(): Promise<SystemHealth>;
    getSignals(): Promise<TradingSignal[]>;
    getStressEvents(): Promise<StressEvent[]>;
    getPositions(strategyId?: string): Promise<Position[]>;
    getOrders(strategyId?: string): Promise<Order[]>;
    getAlerts(): Promise<Alert[]>;
    getAlertConfigs(): Promise<AlertConfig[]>;
    getRiskMetrics(strategyId: string): Promise<RiskMetrics>;
    getExecutionMetrics(strategyId: string): Promise<ExecutionMetrics>;
}

class MockDataService implements IDataService {
    async getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
        await new Promise(r => setTimeout(r, 100));
        return [];
    }

    async getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
        await new Promise(r => setTimeout(r, 100));
        return [];
    }

    async getStrategies(): Promise<StrategyRun[]> {
        await new Promise(r => setTimeout(r, 500));
        return mockStrategies as any;
    }

    async getSystemHealth(): Promise<SystemHealth> {
        await new Promise(r => setTimeout(r, 300));
        return mockSystemHealth as any;
    }

    async getLiveSystemHealth(): Promise<SystemHealth> {
        // Mock implementation of "live" health would just be the same 
        // but for testing we could simulate a failure if needed.
        return this.getSystemHealth();
    }

    async getSignals(): Promise<TradingSignal[]> {
        await new Promise(r => setTimeout(r, 300));
        return mockSignals as any;
    }

    async getStressEvents(): Promise<StressEvent[]> {
        await new Promise(r => setTimeout(r, 300));
        return stressEvents as any;
    }

    async getPositions(strategyId?: string): Promise<Position[]> {
        await new Promise(r => setTimeout(r, 300));
        return [
            { symbol: 'AAPL', shares: 500, price: 182.45, value: 91225, allocation: 25.4, pnl: 2055, pnlPercent: 2.30, strategy: 'Momentum Alpha' },
            { symbol: 'MSFT', shares: 300, price: 418.92, value: 125676, allocation: 21.8, pnl: 2007, pnlPercent: 1.62, strategy: 'Tech Sector' }
        ];
    }

    async getOrders(strategyId?: string): Promise<Order[]> {
        await new Promise(r => setTimeout(r, 300));
        return [
            { id: 'ORD-001', date: '09:32:14', symbol: 'AAPL', side: 'BUY', quantity: 100, status: 'WORKING', price: 181.50, strategy: 'Momentum Alpha' },
            { id: 'ORD-002', date: '09:28:43', symbol: 'MSFT', side: 'SELL', quantity: 50, status: 'FILLED', price: 418.92, strategy: 'Tech Sector' }
        ];
    }

    async getAlerts(): Promise<Alert[]> {
        await new Promise(r => setTimeout(r, 300));
        return [
            { id: 1, severity: 'warning', title: 'Margin Utilization Warning', message: 'Account IB-001 is using 87% of available margin.', timestamp: '2 minutes ago', status: 'active' },
            { id: 2, severity: 'info', title: 'Order Filled', message: 'MSFT SELL 50 shares @ $418.92', timestamp: '5 minutes ago', status: 'resolved' }
        ];
    }

    async getAlertConfigs(): Promise<AlertConfig[]> {
        await new Promise(r => setTimeout(r, 300));
        return [
            { id: 'AC-001', name: 'PnL Warning', type: 'pnl', condition: 'pnl < -5%', enabled: true, channels: ['email', 'slack'], priority: 'high', strategy: 'All', createdAt: '2025-01-01', triggeredCount: 12 }
        ];
    }

    async getRiskMetrics(strategyId: string): Promise<RiskMetrics> {
        await new Promise(r => setTimeout(r, 300));
        return {
            var95: -2.1,
            upCapture: 1.12,
            downCapture: 0.78,
            factorExposures: [
                { factor: 'Value', loading: 0.25 },
                { factor: 'Momentum', loading: 0.68 },
                { factor: 'Size', loading: -0.12 }
            ]
        };
    }

    async getExecutionMetrics(strategyId: string): Promise<ExecutionMetrics> {
        await new Promise(r => setTimeout(r, 300));
        return {
            totalCostDragBps: 312,
            avgHoldingPeriodDays: 8.5,
            costBreakdown: [
                { name: 'Commissions', value: 35, color: '#3b82f6' },
                { name: 'Slippage', value: 45, color: '#10b981' },
                { name: 'Financing', value: 20, color: '#f59e0b' }
            ]
        };
    }
}

class ApiDataService implements IDataService {
    private baseUrl = config.apiBaseUrl;

    async getMarketData(ticker: string, layer: 'silver' | 'gold' = 'silver'): Promise<MarketData[]> {
        const response = await fetch(`${this.baseUrl}/market/${layer}/${ticker}`);
        if (!response.ok) throw new Error('API request failed');
        return response.json();
    }

    async getFinanceData(ticker: string, subDomain: string, layer: 'silver' | 'gold' = 'silver'): Promise<FinanceData[]> {
        const response = await fetch(`${this.baseUrl}/finance/${layer}/${subDomain}/${ticker}`);
        if (!response.ok) throw new Error('API request failed');
        return response.json();
    }

    async getStrategies(): Promise<StrategyRun[]> {
        const response = await fetch(`${this.baseUrl}/strategies`);
        if (!response.ok) return mockStrategies as any;
        return response.json();
    }

    async getSystemHealth(): Promise<SystemHealth> {
        return mockSystemHealth as any;
    }

    async getLiveSystemHealth(): Promise<SystemHealth> {
        return backtestApi.getSystemHealth();
    }

    async getSignals(): Promise<TradingSignal[]> {
        return mockSignals as any;
    }

    async getStressEvents(): Promise<StressEvent[]> {
        return stressEvents as any;
    }

    async getPositions(strategyId?: string): Promise<Position[]> {
        return [];
    }

    async getOrders(strategyId?: string): Promise<Order[]> {
        return [];
    }

    async getAlerts(): Promise<Alert[]> {
        return [];
    }

    async getAlertConfigs(): Promise<AlertConfig[]> {
        return [];
    }

    async getRiskMetrics(strategyId: string): Promise<RiskMetrics> {
        return { var95: 0, upCapture: 0, downCapture: 0, factorExposures: [] };
    }

    async getExecutionMetrics(strategyId: string): Promise<ExecutionMetrics> {
        return { totalCostDragBps: 0, avgHoldingPeriodDays: 0, costBreakdown: [] };
    }
}

class ProxyDataService implements IDataService {
    private mockService = new MockDataService();
    private apiService = new ApiDataService();
    private _mode: 'mock' | 'live' = config.useMockData ? 'mock' : 'live';

    get mode(): 'mock' | 'live' {
        return this._mode;
    }

    setMode(mode: 'mock' | 'live') {
        this._mode = mode;
    }

    async getMarketData(ticker: string, layer?: 'silver' | 'gold'): Promise<MarketData[]> {
        return this.mode === 'mock' ? this.mockService.getMarketData(ticker, layer) : this.apiService.getMarketData(ticker, layer);
    }

    async getFinanceData(ticker: string, subDomain: string, layer?: 'silver' | 'gold'): Promise<FinanceData[]> {
        return this.mode === 'mock' ? this.mockService.getFinanceData(ticker, subDomain, layer) : this.apiService.getFinanceData(ticker, subDomain, layer);
    }

    async getStrategies(): Promise<StrategyRun[]> {
        return this.mode === 'mock' ? this.mockService.getStrategies() : this.apiService.getStrategies();
    }

    async getSystemHealth(): Promise<SystemHealth> {
        return this.mode === 'mock' ? this.mockService.getSystemHealth() : this.apiService.getSystemHealth();
    }

    async getLiveSystemHealth(): Promise<SystemHealth> {
        // Always use API service for live health
        return this.apiService.getLiveSystemHealth();
    }

    async getSignals(): Promise<TradingSignal[]> {
        return this.mode === 'mock' ? this.mockService.getSignals() : this.apiService.getSignals();
    }

    async getStressEvents(): Promise<StressEvent[]> {
        return this.mode === 'mock' ? this.mockService.getStressEvents() : this.apiService.getStressEvents();
    }

    async getPositions(strategyId?: string): Promise<Position[]> {
        return this.mode === 'mock' ? this.mockService.getPositions(strategyId) : this.apiService.getPositions(strategyId);
    }

    async getOrders(strategyId?: string): Promise<Order[]> {
        return this.mode === 'mock' ? this.mockService.getOrders(strategyId) : this.apiService.getOrders(strategyId);
    }

    async getAlerts(): Promise<Alert[]> {
        return this.mode === 'mock' ? this.mockService.getAlerts() : this.apiService.getAlerts();
    }

    async getAlertConfigs(): Promise<AlertConfig[]> {
        return this.mode === 'mock' ? this.mockService.getAlertConfigs() : this.apiService.getAlertConfigs();
    }

    async getRiskMetrics(strategyId: string): Promise<RiskMetrics> {
        return this.mode === 'mock' ? this.mockService.getRiskMetrics(strategyId) : this.apiService.getRiskMetrics(strategyId);
    }

    async getExecutionMetrics(strategyId: string): Promise<ExecutionMetrics> {
        return this.mode === 'mock' ? this.mockService.getExecutionMetrics(strategyId) : this.apiService.getExecutionMetrics(strategyId);
    }
}

export const DataService = new ProxyDataService();
