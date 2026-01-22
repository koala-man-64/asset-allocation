// Live Trading Monitor Page - Monitor and trade with connected brokerage accounts

import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import {
    TrendingUp,
    TrendingDown,
    Activity,
    DollarSign,
    AlertCircle,
    CheckCircle,
    Clock,
    XCircle,
    BarChart3,
    RefreshCw
} from 'lucide-react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { useState, useEffect } from 'react';
import { DataService } from '@/services/DataService';
import { Position, Order, Alert } from '@/types/data';

// Mock data for connected brokerage accounts (kept here for now as they are account-level)
const mockAccounts = [
    {
        id: 'ib-001',
        broker: 'Interactive Brokers',
        accountNumber: '••••3847',
        status: 'connected',
        balance: 2847293.45,
        cashBalance: 423847.23,
        marginUsed: 1247832.12,
        buyingPower: 3294823.84,
        dayPnL: 14273.82,
        dayPnLPct: 0.51,
        totalPnL: 247293.45,
        totalPnLPct: 9.52
    },
    {
        id: 'td-002',
        broker: 'TD Ameritrade',
        accountNumber: '••••6291',
        status: 'connected',
        balance: 1523847.92,
        cashBalance: 234928.44,
        marginUsed: 892341.23,
        buyingPower: 1823947.23,
        dayPnL: -3284.23,
        dayPnLPct: -0.22,
        totalPnL: 123847.92,
        totalPnLPct: 8.83
    }
];

// Mock intraday P&L data
const generateIntradayPnL = () => {
    const data = [];
    const baseTime = new Date();
    baseTime.setHours(9, 30, 0, 0);

    let cumPnL = 0;
    for (let i = 0; i < 120; i += 5) {
        const time = new Date(baseTime.getTime() + i * 60000);
        cumPnL += (Math.random() - 0.48) * 2000;
        data.push({
            time: time.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
            pnl: cumPnL
        });
    }
    return data;
};

const intradayPnLData = generateIntradayPnL();

// Mock strategy allocation data
const strategyAllocation = [
    { name: 'Momentum Alpha', value: 25.4 },
    { name: 'Tech Sector', value: 21.8 },
    { name: 'Market Neutral', value: 18.3 },
    { name: 'Value Mean Rev', value: 14.2 },
    { name: 'Statistical Arb', value: 12.1 },
    { name: 'Short Vol', value: 8.2 }
];

export function LiveTradingPage() {
    const [selectedAccount, setSelectedAccount] = useState(mockAccounts[0].id);
    const [positions, setPositions] = useState<Position[]>([]);
    const [orders, setOrders] = useState<Order[]>([]);
    const [alerts, setAlerts] = useState<Alert[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        async function loadData() {
            setLoading(true);
            try {
                const [posData, orderData, alertData] = await Promise.all([
                    DataService.getPositions(),
                    DataService.getOrders(),
                    DataService.getAlerts()
                ]);
                setPositions(posData);
                setOrders(orderData);
                setAlerts(alertData);
            } catch (error) {
                console.error("Failed to load live trading data:", error);
            } finally {
                setLoading(false);
            }
        }
        loadData();
    }, []);

    const currentAccount = mockAccounts.find(a => a.id === selectedAccount) || mockAccounts[0];

    const totalPositionPnL = positions.reduce((sum, p) => sum + p.pnl, 0);
    const longExposure = positions.filter(p => p.shares > 0).reduce((sum, p) => sum + p.value, 0);
    const shortExposure = positions.filter(p => p.shares < 0).reduce((sum, p) => sum + Math.abs(p.value), 0);
    const netExposure = longExposure - shortExposure;

    const getStatusBadge = (status: string) => {
        switch (status) {
            case 'connected':
                return <Badge className="bg-green-100 text-green-800 border-green-200"><CheckCircle className="h-3 w-3 mr-1" />Connected</Badge>;
            case 'disconnected':
                return <Badge className="bg-red-100 text-red-800 border-red-200"><XCircle className="h-3 w-3 mr-1" />Disconnected</Badge>;
            case 'connecting':
                return <Badge className="bg-yellow-100 text-yellow-800 border-yellow-200"><Clock className="h-3 w-3 mr-1" />Connecting</Badge>;
            default:
                return <Badge variant="outline">{status}</Badge>;
        }
    };

    const getOrderStatusBadge = (status: string) => {
        switch (status) {
            case 'FILLED':
                return <Badge className="bg-green-100 text-green-800 border-green-200">Filled</Badge>;
            case 'WORKING':
                return <Badge className="bg-blue-100 text-blue-800 border-blue-200">Working</Badge>;
            case 'CANCELLED':
                return <Badge className="bg-gray-100 text-gray-800 border-gray-200">Cancelled</Badge>;
            case 'REJECTED':
                return <Badge className="bg-red-100 text-red-800 border-red-200">Rejected</Badge>;
            default:
                return <Badge variant="outline">{status}</Badge>;
        }
    };

    if (loading) {
        return <div className="p-8 text-center text-muted-foreground">Loading trading data...</div>;
    }

    return (
        <div className="space-y-6">
            {/* Header */}
            <Card>
                <CardHeader>
                    <div className="flex items-center justify-between">
                        <div>
                            <CardTitle>Live Trading Monitor</CardTitle>
                            <p className="text-sm text-muted-foreground mt-1">
                                Real-time monitoring and trading across connected brokerage accounts
                            </p>
                        </div>
                        <Button variant="outline" size="sm">
                            <RefreshCw className="h-4 w-4 mr-2" />
                            Refresh
                        </Button>
                    </div>
                </CardHeader>
            </Card>

            {/* Connected Accounts */}
            <Card>
                <CardHeader>
                    <CardTitle>Connected Accounts</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        {mockAccounts.map(account => (
                            <div
                                key={account.id}
                                className={`border rounded-lg p-4 cursor-pointer transition-all ${selectedAccount === account.id ? 'border-primary bg-secondary/30' : 'border-border hover:border-primary/50'
                                    }`}
                                onClick={() => setSelectedAccount(account.id)}
                            >
                                <div className="flex items-center justify-between mb-3">
                                    <div>
                                        <div className="font-semibold">{account.broker}</div>
                                        <div className="text-sm text-muted-foreground">{account.accountNumber}</div>
                                    </div>
                                    {getStatusBadge(account.status)}
                                </div>

                                <div className="grid grid-cols-2 gap-3 text-sm">
                                    <div>
                                        <div className="text-muted-foreground">Balance</div>
                                        <div className="font-mono font-semibold">${account.balance.toLocaleString(undefined, { minimumFractionDigits: 2 })}</div>
                                    </div>
                                    <div>
                                        <div className="text-muted-foreground">Day P&L</div>
                                        <div className={`font-mono font-semibold ${account.dayPnL >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                            {account.dayPnL >= 0 ? '+' : ''}{account.dayPnL.toLocaleString(undefined, { minimumFractionDigits: 2 })} ({account.dayPnLPct >= 0 ? '+' : ''}{account.dayPnLPct.toFixed(2)}%)
                                        </div>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </CardContent>
            </Card>

            {/* Account Summary KPIs */}
            <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <div className="text-sm text-muted-foreground mb-1">Total Equity</div>
                                <div className="text-xl font-bold">${currentAccount.balance.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                            </div>
                            <DollarSign className="h-8 w-8 text-muted-foreground" />
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <div className="text-sm text-muted-foreground mb-1">Day P&L</div>
                                <div className={`text-xl font-bold ${currentAccount.dayPnL >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {currentAccount.dayPnL >= 0 ? '+' : ''}${currentAccount.dayPnL.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                                </div>
                            </div>
                            {currentAccount.dayPnL >= 0 ?
                                <TrendingUp className="h-8 w-8 text-green-600" /> :
                                <TrendingDown className="h-8 w-8 text-red-600" />
                            }
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <div className="text-sm text-muted-foreground mb-1">Buying Power</div>
                                <div className="text-xl font-bold">${currentAccount.buyingPower.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                            </div>
                            <Activity className="h-8 w-8 text-muted-foreground" />
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <div className="text-sm text-muted-foreground mb-1">Margin Used</div>
                                <div className="text-xl font-bold">${currentAccount.marginUsed.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                            </div>
                            <BarChart3 className="h-8 w-8 text-muted-foreground" />
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardContent className="pt-6">
                        <div className="flex items-center justify-between">
                            <div>
                                <div className="text-sm text-muted-foreground mb-1">Total P&L</div>
                                <div className={`text-xl font-bold ${currentAccount.totalPnL >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {currentAccount.totalPnL >= 0 ? '+' : ''}${currentAccount.totalPnL.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                                </div>
                            </div>
                            <TrendingUp className="h-8 w-8 text-green-600" />
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Intraday P&L Chart */}
            <Card>
                <CardHeader>
                    <CardTitle>Intraday P&L</CardTitle>
                </CardHeader>
                <CardContent>
                    <ResponsiveContainer width="100%" height={250}>
                        <AreaChart data={intradayPnLData}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                            <XAxis
                                dataKey="time"
                                tick={{ fontSize: 11 }}
                                interval={4}
                            />
                            <YAxis
                                tick={{ fontSize: 11 }}
                                tickFormatter={(value) => `$${(value / 1000).toFixed(0)}k`}
                            />
                            <Tooltip
                                contentStyle={{ backgroundColor: 'white', border: '1px solid #ccc' }}
                                formatter={(value: number) => [`$${value.toFixed(0)}`, 'P&L']}
                            />
                            <Area
                                type="monotone"
                                dataKey="pnl"
                                stroke="#0F172A"
                                fill={intradayPnLData[intradayPnLData.length - 1].pnl >= 0 ? '#D1FAE5' : '#FEE2E2'}
                                strokeWidth={2}
                            />
                        </AreaChart>
                    </ResponsiveContainer>
                </CardContent>
            </Card>

            {/* Positions and Exposure */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Card className="md:col-span-2">
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <CardTitle>Current Positions</CardTitle>
                            <div className="text-sm font-mono">
                                Total P&L: <span className={totalPositionPnL >= 0 ? 'text-green-600 font-semibold' : 'text-red-600 font-semibold'}>
                                    {totalPositionPnL >= 0 ? '+' : ''}${totalPositionPnL.toLocaleString(undefined, { minimumFractionDigits: 2 })}
                                </span>
                            </div>
                        </div>
                    </CardHeader>
                    <CardContent>
                        <div className="overflow-x-auto">
                            <table className="w-full text-sm">
                                <thead>
                                    <tr className="border-b">
                                        <th className="text-left p-2">Symbol</th>
                                        <th className="text-right p-2">Qty</th>
                                        <th className="text-right p-2">Price</th>
                                        <th className="text-right p-2">P&L</th>
                                        <th className="text-right p-2">P&L %</th>
                                        <th className="text-right p-2">Mkt Value</th>
                                        <th className="text-left p-2">Strategy</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {positions.map((pos, idx) => (
                                        <tr key={idx} className="border-b hover:bg-muted/50">
                                            <td className="p-2 font-semibold">{pos.symbol}</td>
                                            <td className={`text-right p-2 font-mono ${pos.shares > 0 ? 'text-green-600' : 'text-red-600'}`}>
                                                {pos.shares > 0 ? '+' : ''}{pos.shares}
                                            </td>
                                            <td className="text-right p-2 font-mono">${pos.price.toFixed(2)}</td>
                                            <td className={`text-right p-2 font-mono ${pos.pnl >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                                {pos.pnl >= 0 ? '+' : ''}${pos.pnl.toFixed(2)}
                                            </td>
                                            <td className={`text-right p-2 font-mono ${pos.pnlPercent >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                                {pos.pnlPercent >= 0 ? '+' : ''}{pos.pnlPercent.toFixed(2)}%
                                            </td>
                                            <td className="text-right p-2 font-mono">${pos.value.toLocaleString()}</td>
                                            <td className="text-left p-2 text-muted-foreground">{pos.strategy}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader>
                        <CardTitle>Exposure Summary</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-4">
                            <div>
                                <div className="flex items-center justify-between mb-1">
                                    <span className="text-sm text-muted-foreground">Long Exposure</span>
                                    <span className="text-sm font-mono font-semibold text-green-600">
                                        ${longExposure.toLocaleString()}
                                    </span>
                                </div>
                                <div className="h-2 bg-muted rounded-full overflow-hidden">
                                    <div
                                        className="h-full bg-green-500"
                                        style={{ width: `${(longExposure / (longExposure + shortExposure || 1)) * 100}%` }}
                                    />
                                </div>
                            </div>

                            <div>
                                <div className="flex items-center justify-between mb-1">
                                    <span className="text-sm text-muted-foreground">Short Exposure</span>
                                    <span className="text-sm font-mono font-semibold text-red-600">
                                        ${shortExposure.toLocaleString()}
                                    </span>
                                </div>
                                <div className="h-2 bg-muted rounded-full overflow-hidden">
                                    <div
                                        className="h-full bg-red-500"
                                        style={{ width: `${(shortExposure / (longExposure + shortExposure || 1)) * 100}%` }}
                                    />
                                </div>
                            </div>

                            <div className="pt-3 border-t">
                                <div className="flex items-center justify-between">
                                    <span className="text-sm font-semibold">Net Exposure</span>
                                    <span className={`text-sm font-mono font-semibold ${netExposure >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                        ${netExposure.toLocaleString()}
                                    </span>
                                </div>
                            </div>

                            <div className="pt-3 border-t">
                                <div className="text-sm text-muted-foreground mb-3">Strategy Allocation</div>
                                <div className="space-y-2">
                                    {strategyAllocation.map((strat, idx) => (
                                        <div key={idx}>
                                            <div className="flex items-center justify-between text-xs mb-1">
                                                <span>{strat.name}</span>
                                                <span className="font-mono">{strat.value}%</span>
                                            </div>
                                            <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                                                <div
                                                    className="h-full bg-primary"
                                                    style={{ width: `${strat.value}%` }}
                                                />
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Active Orders */}
            <Card>
                <CardHeader>
                    <CardTitle>Active Orders</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead>
                                <tr className="border-b">
                                    <th className="text-left p-2">Order ID</th>
                                    <th className="text-left p-2">Symbol</th>
                                    <th className="text-left p-2">Side</th>
                                    <th className="text-right p-2">Qty</th>
                                    <th className="text-right p-2">Status</th>
                                    <th className="text-right p-2">Price</th>
                                    <th className="text-left p-2">Strategy</th>
                                    <th className="text-left p-2">Time</th>
                                    <th className="text-left p-2">Actions</th>
                                </tr>
                            </thead>
                            <tbody>
                                {orders.map((order) => (
                                    <tr key={order.id} className="border-b hover:bg-muted/50">
                                        <td className="p-2 font-mono text-xs">{order.id}</td>
                                        <td className="p-2 font-semibold">{order.symbol}</td>
                                        <td className="p-2">
                                            <Badge className={order.side === 'BUY' ? 'bg-green-100 text-green-800 border-green-200' : 'bg-red-100 text-red-800 border-red-200'}>
                                                {order.side}
                                            </Badge>
                                        </td>
                                        <td className="text-right p-2 font-mono">{order.quantity}</td>
                                        <td className="text-right p-2">{getOrderStatusBadge(order.status)}</td>
                                        <td className="text-right p-2 font-mono">${order.price.toFixed(2)}</td>
                                        <td className="p-2 text-muted-foreground">{order.strategy}</td>
                                        <td className="p-2 font-mono text-xs">{order.date}</td>
                                        <td className="p-2">
                                            {order.status === 'WORKING' && (
                                                <Button variant="ghost" size="sm" className="h-7 text-xs">
                                                    Cancel
                                                </Button>
                                            )}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </CardContent>
            </Card>

            {/* Risk Alerts */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center">
                        <AlertCircle className="h-5 w-5 mr-2 text-yellow-600" />
                        Risk Alerts & Notifications
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="space-y-3">
                        {alerts.map((alert) => (
                            <div key={alert.id} className={`flex items-start gap-3 p-3 border rounded-lg ${alert.severity === 'critical' ? 'bg-red-50 border-red-200' :
                                    alert.severity === 'warning' ? 'bg-yellow-50 border-yellow-200' :
                                        'bg-blue-50 border-blue-200'
                                }`}>
                                {alert.severity === 'critical' ? <XCircle className="h-5 w-5 text-red-600 mt-0.5" /> :
                                    alert.severity === 'warning' ? <AlertCircle className="h-5 w-5 text-yellow-600 mt-0.5" /> :
                                        <InfoCircleIcon className="h-5 w-5 text-blue-600 mt-0.5" />}
                                <div className="flex-1">
                                    <div className="font-semibold text-sm">{alert.title}</div>
                                    <div className="text-sm text-muted-foreground">
                                        {alert.message}
                                    </div>
                                    <div className="text-xs text-muted-foreground mt-1">{alert.timestamp}</div>
                                </div>
                            </div>
                        ))}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}

function InfoCircleIcon({ className }: { className?: string }) {
    return (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            width="24"
            height="24"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className={className}
        >
            <circle cx="12" cy="12" r="10" />
            <line x1="12" x2="12" y1="16" y2="12" />
            <line x1="12" x2="12.01" y1="8" y2="8" />
        </svg>
    )
}
