// Portfolio Builder Page

import { useApp } from '@/contexts/AppContext';
import { DataService } from '@/services/DataService';
import { StrategyRun } from '@/types/strategy';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Slider } from '@/app/components/ui/slider';
import { Button } from '@/app/components/ui/button';
import { useState, useMemo, useEffect } from 'react';
import { LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { TimeSeriesPoint } from '@/types/strategy';

export function PortfolioPage() {
  const { selectedRuns } = useApp();
  const [strategies, setStrategies] = useState<StrategyRun[]>([]);
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadStrategies() {
      setLoading(true);
      try {
        const data = await DataService.getStrategies();
        setStrategies(data);
      } catch (error) {
        console.error("Failed to load strategies for portfolio:", error);
      } finally {
        setLoading(false);
      }
    }
    loadStrategies();
  }, []);

  const selectedStrategies = strategies.filter(s => selectedRuns.has(s.id));

  // Initialize weights when strategies are loaded or selection changes
  useEffect(() => {
    if (!loading) {
      setWeights(prev => {
        const newWeights = { ...prev };
        // Add missing weights
        selectedStrategies.forEach(s => {
          if (newWeights[s.id] === undefined) {
            newWeights[s.id] = 100 / selectedStrategies.length;
          }
        });
        return newWeights;
      });
    }
  }, [loading, selectedStrategies.length]);

  const updateWeight = (id: string, value: number) => {
    setWeights(prev => ({ ...prev, [id]: value }));
  };

  const totalWeight = Object.values(weights).filter((_, i) => i < selectedStrategies.length).reduce((sum, w) => sum + w, 0);

  // Calculate portfolio metrics
  const portfolioCagr = selectedStrategies.reduce((sum, s) =>
    sum + (s.cagr * ((weights[s.id] || 0) / 100)), 0
  );

  const portfolioVol = Math.sqrt(
    selectedStrategies.reduce((sum, s) =>
      sum + Math.pow(s.annVol * ((weights[s.id] || 0) / 100), 2), 0
    )
  ) * 1.2; // Simplified - assumes some correlation

  const portfolioSharpe = portfolioVol > 0 ? portfolioCagr / portfolioVol : 0;

  // Generate blended portfolio historical data
  const portfolioHistoricalData = useMemo(() => {
    if (selectedStrategies.length === 0) return { equity: [], drawdown: [], rollingSharpe: [] };

    // Blend equity curves based on weights
    const blendedEquity: TimeSeriesPoint[] = [];
    const firstStrategy = selectedStrategies[0];

    firstStrategy.equityCurve.forEach((point, idx) => {
      let blendedValue = 0;

      selectedStrategies.forEach(strategy => {
        const weight = (weights[strategy.id] || 0) / 100;
        const strategyPoint = strategy.equityCurve[idx];
        if (strategyPoint) {
          // Convert to percentage change from 100, then blend
          const pctChange = (strategyPoint.value - 100) / 100;
          blendedValue += pctChange * weight;
        }
      });

      blendedEquity.push({
        date: point.date,
        value: 100 * (1 + blendedValue)
      });
    });

    // Generate drawdown from blended equity
    const blendedDrawdown: TimeSeriesPoint[] = [];
    let peak = blendedEquity[0].value;

    blendedEquity.forEach(point => {
      if (point.value > peak) peak = point.value;
      const dd = ((point.value - peak) / peak) * 100;
      blendedDrawdown.push({
        date: point.date,
        value: dd
      });
    });

    // Generate rolling Sharpe (simplified - 63 day window)
    const rollingSharpe: TimeSeriesPoint[] = [];
    const window = 63;

    for (let i = window; i < blendedEquity.length; i++) {
      const windowData = blendedEquity.slice(i - window, i);
      const returns = windowData.map((p, idx) => {
        if (idx === 0) return 0;
        return (p.value - windowData[idx - 1].value) / windowData[idx - 1].value;
      });

      const avgReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
      const variance = returns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / returns.length;
      const volatility = Math.sqrt(variance) * Math.sqrt(252);
      const annualizedReturn = avgReturn * 252;
      const sharpe = volatility > 0 ? annualizedReturn / volatility : 0;

      rollingSharpe.push({
        date: blendedEquity[i].date,
        value: sharpe
      });
    }

    return {
      equity: blendedEquity,
      drawdown: blendedDrawdown,
      rollingSharpe
    };
  }, [selectedStrategies, weights]);

  // Format data for recharts
  const equityChartData = portfolioHistoricalData.equity.map((point, idx) => ({
    date: new Date(point.date).toLocaleDateString('en-US', { month: 'short', year: '2-digit' }),
    Portfolio: point.value,
    ...selectedStrategies.reduce((acc, strategy) => ({
      ...acc,
      [strategy.name]: strategy.equityCurve[idx]?.value || 0
    }), {})
  }));

  const drawdownChartData = portfolioHistoricalData.drawdown.map(point => ({
    date: new Date(point.date).toLocaleDateString('en-US', { month: 'short', year: '2-digit' }),
    Drawdown: point.value
  }));

  const sharpeChartData = portfolioHistoricalData.rollingSharpe.map(point => ({
    date: new Date(point.date).toLocaleDateString('en-US', { month: 'short', year: '2-digit' }),
    Sharpe: point.value
  }));

  // Sample data less frequently for cleaner charts
  const sampleInterval = 5;
  const sampledEquityData = equityChartData.filter((_, idx) => idx % sampleInterval === 0);
  const sampledDrawdownData = drawdownChartData.filter((_, idx) => idx % sampleInterval === 0);
  const sampledSharpeData = sharpeChartData.filter((_, idx) => idx % sampleInterval === 0);

  // Color palette for strategies
  const strategyColors = ['#8B5CF6', '#10B981', '#F59E0B', '#EF4444', '#3B82F6', '#EC4899', '#14B8A6', '#F97316'];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="text-muted-foreground">Loading portfolio data...</div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Portfolio Builder</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Combine strategies into a meta-portfolio. Adjust weights to optimize risk-return profile.
          </p>
        </CardContent>
      </Card>

      {selectedStrategies.length === 0 ? (
        <Card>
          <CardContent className="py-12 text-center">
            <p className="text-muted-foreground">No strategies selected. Add strategies from the Overview page.</p>
          </CardContent>
        </Card>
      ) : (
        <>
          <Card>
            <CardHeader>
              <div className="flex items-center justify-between">
                <CardTitle>Strategy Weights</CardTitle>
                <div className="text-sm">
                  Total: <span className={`font-mono font-bold ${Math.abs(totalWeight - 100) < 0.1 ? 'text-green-500' : 'text-red-500'}`}>
                    {totalWeight.toFixed(1)}%
                  </span>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-6">
                {selectedStrategies.map(strategy => (
                  <div key={strategy.id}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="font-medium">{strategy.name}</span>
                      <span className="font-mono text-sm">{(weights[strategy.id] || 0).toFixed(1)}%</span>
                    </div>
                    <Slider
                      value={[weights[strategy.id] || 0]}
                      onValueChange={([value]) => updateWeight(strategy.id, value)}
                      max={100}
                      step={1}
                      className="w-full"
                    />
                  </div>
                ))}
              </div>

              <div className="mt-6 flex gap-2">
                <Button
                  variant="outline"
                  onClick={() => {
                    const equalWeight = 100 / selectedStrategies.length;
                    setWeights(Object.fromEntries(selectedStrategies.map(s => [s.id, equalWeight])));
                  }}
                >
                  Equal Weight
                </Button>
                <Button
                  variant="outline"
                  onClick={() => {
                    // Risk parity (simplified - inverse volatility)
                    const invVols = selectedStrategies.map(s => 1 / s.annVol);
                    const sumInvVols = invVols.reduce((a, b) => a + b, 0);
                    setWeights(Object.fromEntries(
                      selectedStrategies.map((s, i) => [s.id, (invVols[i] / sumInvVols) * 100])
                    ));
                  }}
                >
                  Risk Parity
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Portfolio Metrics</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                <div>
                  <div className="text-sm text-muted-foreground mb-1">Expected CAGR</div>
                  <div className="text-2xl font-bold">{portfolioCagr.toFixed(2)}%</div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground mb-1">Expected Volatility</div>
                  <div className="text-2xl font-bold">{portfolioVol.toFixed(2)}%</div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground mb-1">Expected Sharpe</div>
                  <div className="text-2xl font-bold text-green-500">{portfolioSharpe.toFixed(2)}</div>
                </div>
                <div>
                  <div className="text-sm text-muted-foreground mb-1">Diversification</div>
                  <div className="text-2xl font-bold">{selectedStrategies.length}</div>
                </div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Component Strategies</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left p-2">Strategy</th>
                      <th className="text-right p-2">Weight</th>
                      <th className="text-right p-2">CAGR</th>
                      <th className="text-right p-2">Volatility</th>
                      <th className="text-right p-2">Sharpe</th>
                      <th className="text-right p-2">Contribution to Return</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedStrategies.map(strategy => {
                      const contribution = (strategy.cagr * ((weights[strategy.id] || 0) / 100));
                      return (
                        <tr key={strategy.id} className="border-b hover:bg-muted/50">
                          <td className="p-2 font-medium">{strategy.name}</td>
                          <td className="text-right p-2 font-mono">{(weights[strategy.id] || 0).toFixed(1)}%</td>
                          <td className="text-right p-2 font-mono">{strategy.cagr.toFixed(2)}%</td>
                          <td className="text-right p-2 font-mono">{strategy.annVol.toFixed(2)}%</td>
                          <td className="text-right p-2 font-mono">{strategy.sharpe.toFixed(2)}</td>
                          <td className="text-right p-2 font-mono text-green-500">+{contribution.toFixed(2)}%</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Historical Performance</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-8">
                {/* Cumulative Returns Chart */}
                <div>
                  <h3 className="text-base font-semibold mb-3">Cumulative Returns (2020-2025)</h3>
                  <ResponsiveContainer width="100%" height={320}>
                    <LineChart
                      data={sampledEquityData}
                      margin={{
                        top: 5,
                        right: 30,
                        left: 20,
                        bottom: 5,
                      }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11 }}
                        interval="preserveStartEnd"
                      />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        label={{ value: 'Portfolio Value ($)', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }}
                      />
                      <Tooltip
                        contentStyle={{ backgroundColor: 'white', border: '1px solid #ccc', fontSize: 12 }}
                        formatter={(value: number) => value.toFixed(2)}
                      />
                      <Legend wrapperStyle={{ paddingTop: '20px', fontSize: 12 }} />
                      <Line
                        type="monotone"
                        dataKey="Portfolio"
                        stroke="#0F172A"
                        strokeWidth={3}
                        dot={false}
                        activeDot={{ r: 6 }}
                      />
                      {selectedStrategies.map((strategy, idx) => (
                        <Line
                          key={strategy.id}
                          type="monotone"
                          dataKey={strategy.name}
                          stroke={strategyColors[idx % strategyColors.length]}
                          strokeWidth={1.5}
                          strokeDasharray="5 5"
                          dot={false}
                        />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                </div>

                {/* Drawdown Chart */}
                <div>
                  <h3 className="text-base font-semibold mb-3">Portfolio Drawdown</h3>
                  <ResponsiveContainer width="100%" height={280}>
                    <AreaChart
                      data={sampledDrawdownData}
                      margin={{
                        top: 5,
                        right: 30,
                        left: 20,
                        bottom: 5,
                      }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11 }}
                        interval="preserveStartEnd"
                      />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        label={{ value: 'Drawdown (%)', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }}
                      />
                      <Tooltip
                        contentStyle={{ backgroundColor: 'white', border: '1px solid #ccc', fontSize: 12 }}
                        formatter={(value: number) => `${value.toFixed(2)}%`}
                      />
                      <Legend wrapperStyle={{ paddingTop: '20px', fontSize: 12 }} />
                      <Area
                        type="monotone"
                        dataKey="Drawdown"
                        stroke="#DC2626"
                        fill="#FEE2E2"
                        strokeWidth={2}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>

                {/* Rolling Sharpe Chart */}
                <div>
                  <h3 className="text-base font-semibold mb-3">Rolling Sharpe Ratio (63-Day Window)</h3>
                  <ResponsiveContainer width="100%" height={280}>
                    <LineChart
                      data={sampledSharpeData}
                      margin={{
                        top: 5,
                        right: 30,
                        left: 20,
                        bottom: 5,
                      }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                      <XAxis
                        dataKey="date"
                        tick={{ fontSize: 11 }}
                        interval="preserveStartEnd"
                      />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        label={{ value: 'Sharpe Ratio', angle: -90, position: 'insideLeft', style: { fontSize: 11 } }}
                      />
                      <Tooltip
                        contentStyle={{ backgroundColor: 'white', border: '1px solid #ccc', fontSize: 12 }}
                        formatter={(value: number) => value.toFixed(2)}
                      />
                      <Legend wrapperStyle={{ paddingTop: '20px', fontSize: 12 }} />
                      <Line
                        type="monotone"
                        dataKey="Sharpe"
                        stroke="#0F172A"
                        strokeWidth={2}
                        dot={false}
                        activeDot={{ r: 6 }}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
