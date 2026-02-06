// Reusable metric tooltips with educational explanations

import { ReactNode } from 'react';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { HelpCircle } from 'lucide-react';

interface MetricTooltipProps {
  metric: string;
  children: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
  customContent?: ReactNode;
}

// Educational explanations for common financial metrics
const metricExplanations: Record<string, { title: string; description: string; formula?: string }> =
  {
    // Performance Metrics
    cagr: {
      title: 'CAGR (Compound Annual Growth Rate)',
      description:
        'The average annual return over the entire period, accounting for compounding. Higher is better.',
      formula: '((Final Value / Initial Value) ^ (1 / Years)) - 1'
    },
    sharpe: {
      title: 'Sharpe Ratio',
      description:
        'Risk-adjusted return measuring excess return per unit of volatility. Values above 1.0 are good, above 2.0 are excellent.',
      formula: '(Return - Risk Free Rate) / Volatility'
    },
    sortino: {
      title: 'Sortino Ratio',
      description:
        'Similar to Sharpe but only penalizes downside volatility. Preferred for strategies with asymmetric returns.',
      formula: '(Return - Risk Free Rate) / Downside Deviation'
    },
    calmar: {
      title: 'Calmar Ratio',
      description:
        'Return relative to maximum drawdown. Measures how much return you get for the worst-case loss. Higher is better.',
      formula: 'CAGR / |Maximum Drawdown|'
    },

    // Risk Metrics
    volatility: {
      title: 'Volatility (Annualized)',
      description:
        'Standard deviation of returns, measuring how much returns fluctuate. Lower means more stable.',
      formula: 'Std Dev(Daily Returns) × √252'
    },
    maxDrawdown: {
      title: 'Maximum Drawdown',
      description:
        'Largest peak-to-trough decline. Shows the worst loss from a previous high. More negative is worse.',
      formula: 'Min((Current Value - Peak Value) / Peak Value)'
    },
    var: {
      title: 'Value at Risk (VaR)',
      description:
        'Maximum expected loss over a time period at a given confidence level (typically 95%). Example: "95% confident we won\'t lose more than this amount".'
    },
    cvar: {
      title: 'Conditional VaR (CVaR)',
      description:
        'Expected loss in the worst-case scenarios beyond VaR. Also called Expected Shortfall.'
    },
    beta: {
      title: 'Beta',
      description:
        'Sensitivity to market movements. Beta of 1.0 moves with market, <1.0 less volatile, >1.0 more volatile than market.'
    },

    // Trading Metrics
    turnover: {
      title: 'Portfolio Turnover',
      description:
        'How frequently positions are traded. 100% means the entire portfolio is replaced once per year. Higher turnover = higher transaction costs.'
    },
    winRate: {
      title: 'Win Rate',
      description:
        'Percentage of profitable trades. Not the only important metric - a 40% win rate with large winners can outperform 60% with small winners.'
    },
    profitFactor: {
      title: 'Profit Factor',
      description:
        'Gross profits divided by gross losses. Values above 1.5 are generally considered good.',
      formula: 'Total Winning $ / Total Losing $'
    },
    avgTrade: {
      title: 'Average Trade',
      description:
        'Average profit/loss per trade. Positive is good, but look at win rate and profit factor too.'
    },

    // Execution Metrics
    slippage: {
      title: 'Slippage',
      description:
        'Difference between expected and actual execution price. Lower is better. Affected by market impact and timing.'
    },
    fillRate: {
      title: 'Fill Rate',
      description:
        'Percentage of orders successfully executed. 100% is ideal but aggressive limit orders may have lower fill rates.'
    },
    commission: {
      title: 'Commission Costs',
      description:
        'Brokerage fees per trade. Important to include in backtest for realistic performance.'
    },

    // Exposure Metrics
    leverage: {
      title: 'Leverage',
      description:
        'Amount of borrowed capital used. 1.0 = no leverage, 2.0 = 2x your capital. Higher leverage amplifies both gains and losses.'
    },
    netExposure: {
      title: 'Net Exposure',
      description:
        'Long positions minus short positions as % of capital. Shows directional market bias.',
      formula: '(Long Value - Short Value) / Portfolio Value'
    },
    grossExposure: {
      title: 'Gross Exposure',
      description:
        'Sum of absolute values of all positions. Shows total capital deployed including leverage.',
      formula: '(|Long Value| + |Short Value|) / Portfolio Value'
    },

    // Risk-Adjusted Metrics
    informationRatio: {
      title: 'Information Ratio',
      description:
        'Excess return vs benchmark per unit of tracking error. Measures skill in active management.',
      formula: '(Return - Benchmark Return) / Tracking Error'
    },
    alpha: {
      title: 'Alpha',
      description:
        'Excess return above what would be predicted by beta. Positive alpha suggests skill rather than market exposure.'
    },

    // System Metrics
    dataFreshness: {
      title: 'Data Freshness',
      description: 'How recent the data is. Real-time < 1s, near real-time < 1min, delayed > 15min.'
    },
    latency: {
      title: 'System Latency',
      description:
        'Time delay from signal generation to order execution. Critical for high-frequency strategies.'
    }
  };

export function MetricTooltip({
  metric,
  children,
  side = 'top',
  customContent
}: MetricTooltipProps) {
  const explanation = metricExplanations[metric];

  if (!explanation && !customContent) {
    // If no explanation exists, just render children without tooltip
    return <>{children}</>;
  }

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="cursor-help inline-flex items-center gap-1">
          {children}
          <HelpCircle className="h-3.5 w-3.5 text-muted-foreground/60 inline" />
        </span>
      </TooltipTrigger>
      <TooltipContent side={side} className="max-w-xs">
        {customContent || (
          <div className="space-y-2">
            <p className="font-semibold text-sm">{explanation.title}</p>
            <p className="text-xs leading-relaxed">{explanation.description}</p>
            {explanation.formula && (
              <p className="text-xs font-mono bg-muted/50 p-2 rounded mt-2">
                {explanation.formula}
              </p>
            )}
          </div>
        )}
      </TooltipContent>
    </Tooltip>
  );
}

// Convenience component for icon-only tooltips
export function InfoTooltip({
  content,
  side = 'top'
}: {
  content: ReactNode;
  side?: 'top' | 'right' | 'bottom' | 'left';
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <HelpCircle className="h-4 w-4 text-muted-foreground/60 cursor-help" />
      </TooltipTrigger>
      <TooltipContent side={side} className="max-w-xs">
        {content}
      </TooltipContent>
    </Tooltip>
  );
}
