// Robustness & Sensitivity Page

import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';

export function RobustnessPage() {
  // Generate parameter sensitivity heatmap data
  const lookbackValues = [10, 15, 20, 25, 30];
  const holdingPeriodValues = [3, 5, 7, 10, 15];

  const heatmapData = lookbackValues.map((lookback) =>
    holdingPeriodValues.map((holding) => {
      // Simulate sharpe ratio that peaks in middle
      const optimalLookback = 20;
      const optimalHolding = 7;
      const lookbackDist = Math.abs(lookback - optimalLookback);
      const holdingDist = Math.abs(holding - optimalHolding);
      const sharpe = 1.8 - lookbackDist * 0.05 - holdingDist * 0.08 + (Math.random() * 0.2 - 0.1);
      return sharpe;
    })
  );

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Strategy Workbench</p>
        <h1 className="page-title">Robustness</h1>
        <p className="page-subtitle">
          Stress-test model sensitivity across parameter ranges and walk-forward windows.
        </p>
      </div>

      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle>Parameter Sensitivity Analysis</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="mb-4">
            <p className="text-sm text-muted-foreground">
              Testing robustness by varying key parameters. We want broad “plateaus” of good
              performance, not single “peaks” which suggest overfitting.
            </p>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr>
                  <th className="p-2 border">Lookback \ Holding Period</th>
                  {holdingPeriodValues.map((h) => (
                    <th key={h} className="p-2 border text-center">
                      {h} days
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {lookbackValues.map((lookback, i) => (
                  <tr key={lookback}>
                    <td className="p-2 border font-semibold">{lookback} days</td>
                    {holdingPeriodValues.map((holding, j) => {
                      const sharpe = heatmapData[i][j];
                      let bgColor = 'bg-destructive';
                      if (sharpe > 1.6) bgColor = 'bg-mcm-teal';
                      else if (sharpe > 1.3) bgColor = 'bg-mcm-olive';
                      else if (sharpe > 1.0) bgColor = 'bg-mcm-mustard';
                      else if (sharpe > 0.7) bgColor = 'bg-mcm-walnut/80';

                      return (
                        <td key={j} className="p-2 border">
                          <div
                            className={`${bgColor} text-white rounded px-2 py-1 text-center font-mono`}
                          >
                            {sharpe.toFixed(2)}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="mt-4 flex items-center gap-4 text-xs">
            <span className="font-semibold">Sharpe Ratio Legend:</span>
            <div className="flex items-center gap-1">
              <div className="w-6 h-4 rounded bg-mcm-teal" />
              <span>&gt; 1.6</span>
            </div>
            <div className="flex items-center gap-1">
              <div className="w-6 h-4 rounded bg-mcm-olive" />
              <span>1.3-1.6</span>
            </div>
            <div className="flex items-center gap-1">
              <div className="w-6 h-4 rounded bg-mcm-mustard" />
              <span>1.0-1.3</span>
            </div>
            <div className="flex items-center gap-1">
              <div className="w-6 h-4 rounded bg-mcm-walnut/80" />
              <span>0.7-1.0</span>
            </div>
            <div className="flex items-center gap-1">
              <div className="w-6 h-4 rounded bg-destructive" />
              <span>&lt; 0.7</span>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle>Walk-Forward Analysis</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            <p className="text-sm text-muted-foreground">
              Testing how the strategy performs on out-of-sample data vs. in-sample training
              periods.
            </p>

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left p-3">Period</th>
                    <th className="text-left p-3">Type</th>
                    <th className="text-right p-3">Sharpe Ratio</th>
                    <th className="text-right p-3">CAGR</th>
                    <th className="text-right p-3">Max DD</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b hover:bg-muted/50">
                    <td className="p-3 font-mono">2020-01 to 2021-12</td>
                    <td className="p-3">
                      <span className="rounded bg-mcm-teal px-2 py-1 text-xs text-mcm-paper">
                        In-Sample
                      </span>
                    </td>
                    <td className="text-right p-3 font-mono">1.92</td>
                    <td className="text-right p-3 font-mono">19.5%</td>
                    <td className="text-right p-3 font-mono text-destructive">-11.2%</td>
                  </tr>
                  <tr className="border-b hover:bg-muted/50">
                    <td className="p-3 font-mono">2022-01 to 2022-12</td>
                    <td className="p-3">
                      <span className="rounded bg-mcm-olive px-2 py-1 text-xs text-mcm-paper">
                        Out-of-Sample
                      </span>
                    </td>
                    <td className="text-right p-3 font-mono">1.64</td>
                    <td className="text-right p-3 font-mono">16.8%</td>
                    <td className="text-right p-3 font-mono text-destructive">-13.5%</td>
                  </tr>
                  <tr className="border-b hover:bg-muted/50">
                    <td className="p-3 font-mono">2022-01 to 2023-12</td>
                    <td className="p-3">
                      <span className="rounded bg-mcm-teal px-2 py-1 text-xs text-mcm-paper">
                        In-Sample
                      </span>
                    </td>
                    <td className="text-right p-3 font-mono">1.78</td>
                    <td className="text-right p-3 font-mono">17.9%</td>
                    <td className="text-right p-3 font-mono text-destructive">-10.8%</td>
                  </tr>
                  <tr className="border-b hover:bg-muted/50">
                    <td className="p-3 font-mono">2024-01 to 2024-12</td>
                    <td className="p-3">
                      <span className="rounded bg-mcm-olive px-2 py-1 text-xs text-mcm-paper">
                        Out-of-Sample
                      </span>
                    </td>
                    <td className="text-right p-3 font-mono">1.52</td>
                    <td className="text-right p-3 font-mono">15.2%</td>
                    <td className="text-right p-3 font-mono text-destructive">-14.2%</td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div className="bg-muted/50 rounded-lg p-4">
              <h4 className="font-semibold mb-2">Analysis Summary</h4>
              <div className="text-sm space-y-1">
                <p>
                  • Average OOS Degradation: <span className="font-mono font-semibold">-14.6%</span>{' '}
                  (acceptable)
                </p>
                <p>
                  • OOS Sharpe remains above <span className="font-mono">1.5</span> threshold ✓
                </p>
                <p>• No regime breaks detected in walk-forward windows</p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle>Overfitting Diagnostics</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="border rounded-lg p-4">
              <div className="text-sm text-muted-foreground mb-2">Parameter Count</div>
              <div className="text-3xl font-bold">7</div>
              <p className="text-xs text-muted-foreground mt-2">Relatively simple model</p>
            </div>

            <div className="border rounded-lg p-4">
              <div className="text-sm text-muted-foreground mb-2">Data Points / Parameters</div>
              <div className="text-3xl font-bold text-mcm-teal">178</div>
              <p className="text-xs text-muted-foreground mt-2">Good ratio (should be &gt; 30)</p>
            </div>

            <div className="border rounded-lg p-4">
              <div className="text-sm text-muted-foreground mb-2">Sensitivity Score</div>
              <div className="text-3xl font-bold text-mcm-teal">0.73</div>
              <p className="text-xs text-muted-foreground mt-2">Low sensitivity to parameters</p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
