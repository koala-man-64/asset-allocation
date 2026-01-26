import { ResponsiveContainer, ComposedChart, Area, Bar, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

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
    if (!data || data.length === 0) return null;

    const minPrice = Math.min(...data.map(d => d.low));
    const maxPrice = Math.max(...data.map(d => d.high));
    const pricePadding = (maxPrice - minPrice) * 0.1;

    const maxVol = Math.max(...data.map(d => d.volume));

    const isPositive = data[0].close < data[data.length - 1].close;
    const color = isPositive ? '#10b981' : '#f43f5e'; // emerald-500 : rose-500

    return (
        <ResponsiveContainer width="100%" height={height}>
            <ComposedChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                <defs>
                    <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={color} stopOpacity={0.2} />
                        <stop offset="95%" stopColor={color} stopOpacity={0} />
                    </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" opacity={0.4} />

                <XAxis
                    dataKey="date"
                    tickFormatter={(val) => new Date(val).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}
                    minTickGap={50}
                    tick={{ fontSize: 10, fill: '#94a3b8' }} // slate-400
                    axisLine={false}
                    tickLine={false}
                    dy={10}
                />

                {/* Price Axis (Left) */}
                <YAxis
                    yAxisId="price"
                    domain={[minPrice - pricePadding, maxPrice + pricePadding]}
                    tickFormatter={(val) => val.toFixed(0)}
                    tick={{ fontSize: 10, fill: '#94a3b8' }}
                    width={40}
                    axisLine={false}
                    tickLine={false}
                    orientation="right"
                />

                {/* Volume Axis (Right/Hidden, scaled down) */}
                <YAxis
                    yAxisId="volume"
                    domain={[0, maxVol * 4]} // Scale so volume bars only take bottom 1/4
                    hide={true}
                />

                <Tooltip
                    content={({ active, payload, label }) => {
                        if (active && payload && payload.length) {
                            const d = payload[0].payload as PricePoint;
                            const isUp = d.close > d.open;
                            return (
                                <div className="bg-white/95 border border-slate-200 p-3 rounded-lg shadow-xl text-xs font-mono backdrop-blur-sm ring-1 ring-slate-200">
                                    <div className="font-bold mb-2 text-slate-700 border-b border-slate-100 pb-1">
                                        {new Date(label).toLocaleDateString(undefined, { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric' })}
                                    </div>
                                    <div className="grid grid-cols-2 gap-x-6 gap-y-1.5 text-slate-500">
                                        <span>Open</span> <span className="text-right font-medium text-slate-900">{d.open.toFixed(2)}</span>
                                        <span>High</span> <span className="text-right font-medium text-slate-900">{d.high.toFixed(2)}</span>
                                        <span>Low</span> <span className="text-right font-medium text-slate-900">{d.low.toFixed(2)}</span>
                                        <span>Close</span> <span className={`text-right font-bold ${isUp ? 'text-emerald-600' : 'text-rose-600'}`}>{d.close.toFixed(2)}</span>
                                        <span>Vol</span> <span className="text-right font-medium text-slate-700">{(d.volume / 1000000).toFixed(2)}M</span>
                                    </div>
                                </div>
                            );
                        }
                        return null;
                    }}
                />

                <Bar
                    yAxisId="volume"
                    dataKey="volume"
                    fill="#e2e8f0"
                    radius={[2, 2, 0, 0]}
                    barSize={data.length > 100 ? 2 : 5}
                />

                <Area
                    yAxisId="price"
                    type="monotone"
                    dataKey="close"
                    stroke={color}
                    fillOpacity={1}
                    fill="url(#colorPrice)"
                    strokeWidth={2}
                    activeDot={{ r: 4, strokeWidth: 0 }}
                />
            </ComposedChart>
        </ResponsiveContainer>
    );
}
