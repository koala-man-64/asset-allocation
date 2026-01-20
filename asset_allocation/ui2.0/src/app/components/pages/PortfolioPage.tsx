// Portfolio Builder Page

import { useApp } from '@/contexts/AppContext';
import { mockStrategies } from '@/data/strategies';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Slider } from '@/app/components/ui/slider';
import { Button } from '@/app/components/ui/button';
import { useState } from 'react';

export function PortfolioPage() {
  const { selectedRuns } = useApp();
  const selectedStrategies = mockStrategies.filter(s => selectedRuns.has(s.id));

  const [weights, setWeights] = useState<Record<string, number>>(
    Object.fromEntries(selectedStrategies.map(s => [s.id, 100 / selectedStrategies.length]))
  );

  const updateWeight = (id: string, value: number) => {
    setWeights(prev => ({ ...prev, [id]: value }));
  };

  const totalWeight = Object.values(weights).reduce((sum, w) => sum + w, 0);

  // Calculate portfolio metrics
  const portfolioCagr = selectedStrategies.reduce((sum, s) =>
    sum + (s.cagr * (weights[s.id] / 100)), 0
  );

  const portfolioVol = Math.sqrt(
    selectedStrategies.reduce((sum, s) =>
      sum + Math.pow(s.annVol * (weights[s.id] / 100), 2), 0
    )
  ) * 1.2; // Simplified - assumes some correlation

  const portfolioSharpe = portfolioCagr / portfolioVol;

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
                      <span className="font-mono text-sm">{weights[strategy.id].toFixed(1)}%</span>
                    </div>
                    <Slider
                      value={[weights[strategy.id]]}
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
                      const contribution = (strategy.cagr * (weights[strategy.id] / 100));
                      return (
                        <tr key={strategy.id} className="border-b hover:bg-muted/50">
                          <td className="p-2 font-medium">{strategy.name}</td>
                          <td className="text-right p-2 font-mono">{weights[strategy.id].toFixed(1)}%</td>
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
        </>
      )}
    </div>
  );
}
