export interface Position {
    symbol: string;
    shares: number;
    price: number;
    value: number;
    allocation: number;
    pnl: number;
    pnlPercent: number;
    strategy?: string;
}

export interface Order {
    id: string;
    date: string;
    symbol: string;
    side: 'BUY' | 'SELL';
    quantity: number;
    status: 'FILLED' | 'WORKING' | 'CANCELLED' | 'REJECTED';
    price: number;
    strategy?: string;
}

export interface AlertConfig {
    id: string;
    name: string;
    type: 'pnl' | 'signal' | 'risk' | 'system';
    condition: string;
    enabled: boolean;
    channels: string[];
    priority: 'high' | 'medium' | 'low';
    strategy: string;
    createdAt: string;
    triggeredCount: number;
    lastTriggered?: string;
}

export interface Alert {
    id: number;
    severity: 'critical' | 'warning' | 'info';
    title: string;
    message: string;
    timestamp: string;
    status: 'active' | 'resolved';
    strategy?: string;
}

export interface MarketData {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

export interface FinanceData {
    date: string;
    symbol: string;
    sub_domain: string;
    [key: string]: any;
}

export interface FactorExposure {
    factor: string;
    loading: number;
}

export interface RiskMetrics {
    var95: number;
    upCapture: number;
    downCapture: number;
    factorExposures: FactorExposure[];
}

export interface CostComponent {
    name: string;
    value: number;
    color: string;
}

export interface ExecutionMetrics {
    totalCostDragBps: number;
    avgHoldingPeriodDays: number;
    costBreakdown: CostComponent[];
}


