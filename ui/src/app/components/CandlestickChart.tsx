import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

interface PricePoint {
    date: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

interface CandlestickChartProps {
    data: PricePoint[];
    height?: number;
}

export function CandlestickChart({ data, height = 300 }: CandlestickChartProps) {
    const minValue = Math.min(...data.map(d => d.low));
    const maxValue = Math.max(...data.map(d => d.high));
    const domainPadding = (maxValue - minValue) * 0.1;
    const isPositive = data[0].close < data[data.length - 1].close;
    const color = isPositive ? '#22c55e' : '#ef4444';

    return (
        <ResponsiveContainer width="100%" height={height}>
            <AreaChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <defs>
                    <linearGradient id={`colorPrice-${height}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={color} stopOpacity={0.2} />
                        <stop offset="95%" stopColor={color} stopOpacity={0} />
                    </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" opacity={0.3} />
                <XAxis
                    dataKey="date"
                    tickFormatter={(val) => new Date(val).toLocaleDateString()}
                    minTickGap={40}
                    tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                    axisLine={false}
                    tickLine={false}
                />
                <YAxis
                    domain={[minValue - domainPadding, maxValue + domainPadding]}
                    tickFormatter={(val) => val.toFixed(0)}
                    tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                    width={40}
                    axisLine={false}
                    tickLine={false}
                />
                <Tooltip
                    content={({ active, payload, label }) => {
                        if (active && payload && payload.length) {
                            const d = payload[0].payload;
                            const isUp = d.close > d.open;
                            return (
                                <div className="bg-popover/95 border border-border p-2 rounded shadow-xl text-xs font-mono backdrop-blur-sm">
                                    <div className="font-semibold mb-1 text-muted-foreground">{new Date(label).toLocaleDateString()}</div>
                                    <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                                        <span>Open:</span> <span className="text-right">{d.open.toFixed(2)}</span>
                                        <span>High:</span> <span className="text-right">{d.high.toFixed(2)}</span>
                                        <span>Low:</span> <span className="text-right">{d.low.toFixed(2)}</span>
                                        <span>Close:</span> <span className={`text-right ${isUp ? 'text-green-500' : 'text-red-500'}`}>{d.close.toFixed(2)}</span>
                                    </div>
                                </div>
                            );
                        }
                        return null;
                    }}
                />
                <Area
                    type="monotone"
                    dataKey="close"
                    stroke={color}
                    fillOpacity={1}
                    fill={`url(#colorPrice-${height})`}
                    strokeWidth={1.5}
                />
            </AreaChart>
        </ResponsiveContainer>
    );
}
