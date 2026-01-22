// Strategy Configuration Modal - View detailed strategy parameters and settings

import { StrategyRun } from '@/types/strategy';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { 
  X, 
  Settings, 
  Database, 
  Target, 
  Calendar, 
  DollarSign,
  TrendingUp,
  Shield,
  Zap,
  GitBranch,
  Clock,
  BarChart3,
  FileCode,
  Layers,
  Filter,
  Activity
} from 'lucide-react';
import { InfoTooltip } from '@/app/components/ui/metric-tooltip';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/app/components/ui/tooltip';

interface StrategyConfigModalProps {
  strategy: StrategyRun | null;
  open: boolean;
  onClose: () => void;
}

export function StrategyConfigModal({ strategy, open, onClose }: StrategyConfigModalProps) {
  if (!open || !strategy) return null;

  // Mock configuration data (in real app, this would come from strategy.config)
  const config = {
    universe: {
      type: strategy.name.includes('Tech') ? 'Tech Sector' : strategy.name.includes('Value') ? 'Value Stocks' : 'S&P 500',
      size: strategy.name.includes('Tech') ? 150 : strategy.name.includes('Small') ? 500 : 250,
      filters: ['Market Cap > $1B', 'Avg Daily Volume > 500K', 'Price > $5'],
      exclusions: ['ADR', 'OTC']
    },
    signals: {
      primary: strategy.name.includes('Momentum') ? 'Price Momentum (12-1)' : 
               strategy.name.includes('Value') ? 'P/E Ratio + Book Value' :
               strategy.name.includes('Vol') ? 'Realized Volatility (20d)' : 'Multi-Factor Score',
      secondary: ['Volume Trend', 'Relative Strength'],
      lookback: strategy.name.includes('Momentum') ? '252 days' : '60 days',
      rebalanceFreq: strategy.name.includes('Intraday') ? 'Daily' : 'Weekly'
    },
    portfolio: {
      targetPositions: strategy.name.includes('Concentrated') ? 20 : strategy.name.includes('Small') ? 50 : 30,
      maxPositionSize: '5%',
      minPositionSize: '1%',
      cashBuffer: '5%',
      rebalanceThreshold: '2%'
    },
    risk: {
      maxLeverage: strategy.name.includes('Vol') ? '1.0x' : strategy.name.includes('Momentum') ? '1.5x' : '1.2x',
      maxDrawdownLimit: '25%',
      dailyVaR: '3%',
      sectorLimit: '30%',
      correlationLimit: '0.7'
    },
    execution: {
      orderType: 'Limit',
      timeInForce: 'Day',
      executionWindow: '10:00 - 15:30 ET',
      slippageModel: 'Linear Market Impact',
      maxSpread: '0.5%',
      minLiquidity: '20x target position'
    },
    costs: {
      commission: '$0.005/share',
      spreadAssumption: '0.05%',
      marketImpact: 'Square-root model',
      borrowCostShort: '0.5% annual'
    },
    data: {
      priceSource: 'Bloomberg',
      fundamentalSource: 'FactSet',
      alternativeData: strategy.name.includes('Sentiment') ? 'Twitter Sentiment, News Analytics' : 'None',
      updateFrequency: 'Real-time',
      dataDelay: '0ms'
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-background rounded-lg shadow-2xl max-w-6xl w-full max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="border-b px-6 py-4 flex items-center justify-between bg-gradient-to-r from-primary/5 to-purple-500/5">
          <div>
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-primary/10">
                <Settings className="h-6 w-6 text-primary" />
              </div>
              <div>
                <h2 className="text-xl font-bold">{strategy.name}</h2>
                <p className="text-sm text-muted-foreground">Strategy Configuration & Parameters</p>
              </div>
            </div>
          </div>
          <Button variant="ghost" size="icon" onClick={onClose}>
            <X className="h-5 w-5" />
          </Button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          <TooltipProvider>
            <div className="space-y-6">
              {/* Strategy Metadata */}
              <Card>
                <CardHeader>
                  <div className="flex items-center gap-2">
                    <FileCode className="h-5 w-5 text-muted-foreground" />
                    <CardTitle>Strategy Metadata</CardTitle>
                    <InfoTooltip 
                      content={
                        <div className="space-y-2">
                          <p className="font-semibold">Strategy Metadata</p>
                          <p className="text-xs">Version control and audit trail information for reproducibility and compliance.</p>
                        </div>
                      }
                    />
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div>
                      <div className="text-sm text-muted-foreground mb-1 flex items-center gap-1">
                        <GitBranch className="h-3 w-3" />
                        Run ID
                      </div>
                      <div className="font-mono text-sm">{strategy.id}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Git SHA</div>
                      <div className="font-mono text-sm">{strategy.audit.gitSha}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Data Version</div>
                      <div className="font-mono text-sm">{strategy.audit.dataVersionId}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1 flex items-center gap-1">
                        <Clock className="h-3 w-3" />
                        Run Date
                      </div>
                      <div className="text-sm">{strategy.audit.runDate}</div>
                    </div>
                  </div>
                  <div className="mt-4 flex gap-2">
                    {strategy.tags.map(tag => (
                      <Badge key={tag} variant="outline">{tag}</Badge>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {/* Universe Configuration */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Target className="h-5 w-5 text-blue-600" />
                      <CardTitle>Universe Selection</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Universe Selection</p>
                            <p className="text-xs">Defines which stocks/assets the strategy can trade. Filters ensure quality and liquidity.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Universe Type</div>
                      <div className="font-semibold">{config.universe.type}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Universe Size
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Number of securities in the tradable universe</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="font-semibold">{config.universe.size} securities</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-2 flex items-center gap-1">
                        <Filter className="h-3 w-3" />
                        Selection Filters
                      </div>
                      <div className="space-y-1">
                        {config.universe.filters.map((filter, idx) => (
                          <div key={idx} className="text-sm bg-muted/50 px-2 py-1 rounded font-mono">
                            {filter}
                          </div>
                        ))}
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-2">Exclusions</div>
                      <div className="flex gap-1">
                        {config.universe.exclusions.map((excl, idx) => (
                          <Badge key={idx} variant="outline" className="text-xs">{excl}</Badge>
                        ))}
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Signal Configuration */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Activity className="h-5 w-5 text-purple-600" />
                      <CardTitle>Signal Generation</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Signal Generation</p>
                            <p className="text-xs">How the strategy identifies trading opportunities. Primary signals drive position sizing, secondary signals provide confirmation.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Primary Signal</div>
                      <div className="font-semibold text-primary">{config.signals.primary}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-2">Secondary Signals</div>
                      <div className="space-y-1">
                        {config.signals.secondary.map((signal, idx) => (
                          <div key={idx} className="text-sm">{signal}</div>
                        ))}
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">
                          Lookback Period
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Historical data window used for signal calculation</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <div className="font-semibold">{config.signals.lookback}</div>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">
                          Rebalance Freq
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">How often positions are adjusted based on new signals</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <div className="font-semibold">{config.signals.rebalanceFreq}</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Portfolio Construction */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Layers className="h-5 w-5 text-green-600" />
                      <CardTitle>Portfolio Construction</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Portfolio Construction</p>
                            <p className="text-xs">Rules for building and maintaining the portfolio. Position limits ensure diversification.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Target Positions</div>
                        <div className="font-semibold">{config.portfolio.targetPositions}</div>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Cash Buffer</div>
                        <div className="font-semibold">{config.portfolio.cashBuffer}</div>
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Max Position Size
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Maximum % of portfolio allocated to any single position</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="font-semibold">{config.portfolio.maxPositionSize}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Min Position Size</div>
                      <div className="font-semibold">{config.portfolio.minPositionSize}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Rebalance Threshold
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Minimum deviation required to trigger rebalancing (reduces unnecessary trading)</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="font-semibold">{config.portfolio.rebalanceThreshold}</div>
                    </div>
                  </CardContent>
                </Card>

                {/* Risk Management */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Shield className="h-5 w-5 text-orange-600" />
                      <CardTitle>Risk Management</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Risk Management</p>
                            <p className="text-xs">Hard limits to prevent excessive losses or concentration. Strategy stops trading if limits are breached.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Max Leverage</div>
                        <div className="font-semibold">{config.risk.maxLeverage}</div>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Daily VaR Limit</div>
                        <div className="font-semibold">{config.risk.dailyVaR}</div>
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Max Drawdown Limit
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Trading halts if drawdown exceeds this threshold</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="font-semibold text-red-600">{config.risk.maxDrawdownLimit}</div>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Sector Limit</div>
                        <div className="font-semibold">{config.risk.sectorLimit}</div>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">
                          Correlation Limit
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Max correlation between any two positions (ensures diversification)</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <div className="font-semibold">{config.risk.correlationLimit}</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Execution Settings */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Zap className="h-5 w-5 text-yellow-600" />
                      <CardTitle>Execution Settings</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Execution Settings</p>
                            <p className="text-xs">How orders are placed and executed in the market. Critical for minimizing slippage and market impact.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Order Type</div>
                        <Badge variant="outline">{config.execution.orderType}</Badge>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Time in Force</div>
                        <Badge variant="outline">{config.execution.timeInForce}</Badge>
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Execution Window
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Time window during which trades can be executed</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="font-semibold">{config.execution.executionWindow}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Slippage Model</div>
                      <div className="text-sm">{config.execution.slippageModel}</div>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Max Spread</div>
                        <div className="font-semibold">{config.execution.maxSpread}</div>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">
                          Min Liquidity
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Minimum daily liquidity required (multiple of position size)</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <div className="text-sm">{config.execution.minLiquidity}</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                {/* Cost Assumptions */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <DollarSign className="h-5 w-5 text-green-600" />
                      <CardTitle>Cost Assumptions</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Cost Assumptions</p>
                            <p className="text-xs">Trading costs used in backtest. Accurate cost modeling is critical for realistic performance estimates.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Commission Rate</div>
                      <div className="font-semibold">{config.costs.commission}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Spread Assumption
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Bid-ask spread cost as % of trade value</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="font-semibold">{config.costs.spreadAssumption}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">
                        Market Impact Model
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="ml-1 cursor-help">ⓘ</span>
                          </TooltipTrigger>
                          <TooltipContent>
                            <p className="text-xs">Model for price impact when trading large positions</p>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                      <div className="text-sm">{config.costs.marketImpact}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Borrow Cost (Short)</div>
                      <div className="font-semibold">{config.costs.borrowCostShort}</div>
                    </div>
                  </CardContent>
                </Card>

                {/* Data Sources */}
                <Card>
                  <CardHeader>
                    <div className="flex items-center gap-2">
                      <Database className="h-5 w-5 text-indigo-600" />
                      <CardTitle>Data Sources</CardTitle>
                      <InfoTooltip 
                        content={
                          <div className="space-y-2">
                            <p className="font-semibold">Data Sources</p>
                            <p className="text-xs">Where the strategy gets its data from. Data quality and timeliness directly impact performance.</p>
                          </div>
                        }
                      />
                    </div>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Price Data</div>
                      <div className="font-semibold">{config.data.priceSource}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Fundamental Data</div>
                      <div className="font-semibold">{config.data.fundamentalSource}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Alternative Data</div>
                      <div className="text-sm">{config.data.alternativeData}</div>
                    </div>
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">Update Frequency</div>
                        <Badge className="bg-green-100 text-green-800 border-green-200">
                          {config.data.updateFrequency}
                        </Badge>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-1">
                          Data Delay
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="ml-1 cursor-help">ⓘ</span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p className="text-xs">Latency from market event to data availability</p>
                            </TooltipContent>
                          </Tooltip>
                        </div>
                        <div className="font-semibold">{config.data.dataDelay}</div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Performance Summary */}
              <Card className="border-primary/20 bg-gradient-to-br from-primary/5 to-purple-500/5">
                <CardHeader>
                  <div className="flex items-center gap-2">
                    <BarChart3 className="h-5 w-5 text-primary" />
                    <CardTitle>Backtest Performance Summary</CardTitle>
                  </div>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">CAGR</div>
                      <div className="text-2xl font-bold text-primary">{strategy.cagr.toFixed(1)}%</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Sharpe Ratio</div>
                      <div className="text-2xl font-bold">{strategy.sharpe.toFixed(2)}</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Max Drawdown</div>
                      <div className="text-2xl font-bold text-red-600">{strategy.maxDD.toFixed(1)}%</div>
                    </div>
                    <div>
                      <div className="text-sm text-muted-foreground mb-1">Turnover</div>
                      <div className="text-2xl font-bold">{strategy.turnoverAnn.toFixed(0)}%</div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TooltipProvider>
        </div>

        {/* Footer */}
        <div className="border-t px-6 py-4 bg-muted/30 flex items-center justify-between">
          <div className="text-sm text-muted-foreground">
            Configuration snapshot from backtest run on {strategy.audit.runDate}
          </div>
          <div className="flex gap-2">
            <Button variant="outline" size="sm">
              Export Config
            </Button>
            <Button variant="outline" size="sm">
              Clone Strategy
            </Button>
            <Button onClick={onClose}>Close</Button>
          </div>
        </div>
      </div>
    </div>
  );
}