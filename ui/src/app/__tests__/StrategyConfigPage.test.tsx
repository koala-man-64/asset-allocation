import { screen, fireEvent, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';
import { StrategyConfigPage } from '@/app/components/pages/StrategyConfigPage';
import { backtestApi } from '@/services/backtestApi';
import { rankingApi } from '@/services/rankingApi';
import { strategyApi } from '@/services/strategyApi';
import { universeApi } from '@/services/universeApi';
import { renderWithProviders } from '@/test/utils';

// Mock dependencies
vi.mock('@/services/strategyApi', () => ({
  strategyApi: {
    listStrategies: vi.fn(),
    saveStrategy: vi.fn(),
    getStrategy: vi.fn(),
    getStrategyDetail: vi.fn(),
    deleteStrategy: vi.fn(),
    getUniverseCatalog: vi.fn(),
    previewUniverse: vi.fn()
  }
}));

vi.mock('@/services/backtestApi', () => ({
  backtestApi: {
    listRuns: vi.fn(),
    submitRun: vi.fn()
  }
}));

vi.mock('@/services/rankingApi', () => ({
  rankingApi: {
    listRankingSchemas: vi.fn()
  }
}));

vi.mock('@/services/universeApi', () => ({
  universeApi: {
    listUniverseConfigs: vi.fn()
  }
}));

// Mock ResizeObserver for Radix UI
global.ResizeObserver = class ResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
};

describe('StrategyConfigPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (rankingApi.listRankingSchemas as Mock).mockResolvedValue([
      {
        name: 'quality-momentum',
        description: 'desc',
        version: 1,
        updated_at: '2026-03-08T00:00:00Z'
      }
    ]);
    (universeApi.listUniverseConfigs as Mock).mockResolvedValue([
      {
        name: 'large-cap-quality',
        description: 'desc',
        version: 1,
        updated_at: '2026-03-08T00:00:00Z'
      }
    ]);
    (backtestApi.listRuns as Mock).mockResolvedValue({ runs: [], limit: 6, offset: 0 });
    (backtestApi.submitRun as Mock).mockResolvedValue({
      run_id: 'run-1',
      status: 'queued',
      submitted_at: '2026-03-08T00:00:00Z'
    });
  });

  it('renders loading state initially', () => {
    (strategyApi.listStrategies as Mock).mockReturnValue(new Promise(() => {})); // pending promise

    renderWithProviders(<StrategyConfigPage />);

    expect(screen.getByText(/loading strategies/i)).toBeInTheDocument();
  });

  it('renders strategies list when data is available', async () => {
    const mockStrategies = [
      { name: 'strat-1', type: 'configured', description: 'desc 1', updated_at: '2023-01-01' },
      { name: 'strat-2', type: 'code-based', description: 'desc 2', updated_at: '2023-01-02' }
    ];
    (strategyApi.listStrategies as Mock).mockResolvedValue(mockStrategies);
    (strategyApi.getStrategyDetail as Mock).mockResolvedValue({
      name: 'strat-1',
      type: 'configured',
      description: 'desc 1',
      config: {
        universeConfigName: 'large-cap-quality',
        rebalance: 'weekly',
        longOnly: true,
        topN: 20,
        lookbackWindow: 63,
        holdingPeriod: 21,
        costModel: 'default',
        intrabarConflictPolicy: 'stop_first',
        exits: []
      }
    });

    renderWithProviders(<StrategyConfigPage />);

    await waitFor(() => {
      expect(screen.getByText('strat-1')).toBeInTheDocument();
      expect(screen.getByText('strat-2')).toBeInTheDocument();
    });

    expect(
      screen.getByRole('button', { name: /view run configuration strat-1/i })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /edit run configuration strat-1/i })
    ).toBeInTheDocument();
    expect(
      screen.getByRole('button', { name: /delete run configuration strat-1/i })
    ).toBeInTheDocument();
  });

  it('loads strategy detail when viewing and editing an existing strategy', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([
      { name: 'strat-1', type: 'configured', description: 'desc 1', updated_at: '2023-01-01' }
    ]);
    (strategyApi.getStrategyDetail as Mock).mockResolvedValue({
      name: 'strat-1',
      type: 'configured',
      description: 'desc 1',
      config: {
        universeConfigName: 'large-cap-quality',
        rebalance: 'weekly',
        longOnly: true,
        topN: 25,
        lookbackWindow: 90,
        holdingPeriod: 30,
        costModel: 'default',
        intrabarConflictPolicy: 'stop_first',
        exits: [
          {
            id: 'stop-8',
            enabled: true,
            type: 'stop_loss_fixed',
            scope: 'position',
            action: 'exit_full',
            minHoldBars: 0,
            priceField: 'low',
            reference: 'entry_price',
            value: 0.08,
            priority: 0
          },
          {
            id: 'take-15',
            enabled: true,
            type: 'take_profit_fixed',
            scope: 'position',
            action: 'exit_full',
            minHoldBars: 0,
            priceField: 'high',
            reference: 'entry_price',
            value: 0.15,
            priority: 1
          }
        ]
      }
    });

    renderWithProviders(<StrategyConfigPage />);

    await waitFor(() => {
      expect(screen.getByText('strat-1')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /view run configuration strat-1/i }));

    await waitFor(() => {
      expect(strategyApi.getStrategyDetail).toHaveBeenCalledWith('strat-1');
    });

    await waitFor(() => {
      expect(screen.getByText(/Top 25 with 90-bar lookback/i)).toBeInTheDocument();
    });
    expect(screen.getByText('large-cap-quality')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^Edit Run Configuration$/i }));

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: /^Edit Run Configuration$/i })
      ).toBeInTheDocument();
    });

    expect(screen.getByDisplayValue('stop-8')).toBeInTheDocument();
    expect(screen.getByDisplayValue('take-15')).toBeInTheDocument();
  });

  it('opens editor when New Run Configuration button is clicked', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([]);

    renderWithProviders(<StrategyConfigPage />);

    await waitFor(() => {
      expect(screen.getByText(/new run configuration/i)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/new run configuration/i));

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: /^New Run Configuration$/ })).toBeInTheDocument();
    });

    expect(strategyApi.getStrategyDetail).not.toHaveBeenCalled();
  });

  it('submits a backtest from the detail panel', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([
      { name: 'strat-1', type: 'configured', description: 'desc 1', updated_at: '2023-01-01' }
    ]);
    (strategyApi.getStrategyDetail as Mock).mockResolvedValue({
      name: 'strat-1',
      type: 'configured',
      description: 'desc 1',
      config: {
        universeConfigName: 'large-cap-quality',
        rebalance: 'weekly',
        longOnly: true,
        topN: 20,
        lookbackWindow: 63,
        holdingPeriod: 21,
        costModel: 'default',
        intrabarConflictPolicy: 'stop_first',
        exits: []
      }
    });

    renderWithProviders(<StrategyConfigPage />);

    await waitFor(() => {
      expect(screen.getByText('strat-1')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /view run configuration strat-1/i }));

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /run backtest/i })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /^Run Backtest$/i }));

    fireEvent.change(screen.getByLabelText(/run name/i), {
      target: { value: 'Smoke intraday' }
    });
    fireEvent.change(screen.getByLabelText(/start timestamp/i), {
      target: { value: '2026-03-03T08:30' }
    });
    fireEvent.change(screen.getByLabelText(/end timestamp/i), {
      target: { value: '2026-03-03T09:30' }
    });
    fireEvent.change(screen.getByLabelText(/bar size/i), {
      target: { value: '5m' }
    });

    fireEvent.click(screen.getByRole('button', { name: /submit backtest/i }));

    await waitFor(() => {
      expect(backtestApi.submitRun).toHaveBeenCalledTimes(1);
    });
  });

  it('deletes a strategy from the page actions', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([
      { name: 'strat-1', type: 'configured', description: 'desc 1', updated_at: '2023-01-01' }
    ]);
    (strategyApi.deleteStrategy as Mock).mockResolvedValue({
      status: 'success',
      message: "Strategy 'strat-1' deleted successfully"
    });

    renderWithProviders(<StrategyConfigPage />);

    await waitFor(() => {
      expect(
        screen.getByRole('button', { name: /delete run configuration strat-1/i })
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /delete run configuration strat-1/i }));

    await waitFor(() => {
      expect(
        screen.getByRole('heading', { name: /delete run configuration/i })
      ).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /delete from postgres/i }));

    await waitFor(() => {
      expect(strategyApi.deleteStrategy).toHaveBeenCalledWith('strat-1');
    });
  });
});
