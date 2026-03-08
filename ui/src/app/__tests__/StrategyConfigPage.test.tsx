import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';
import { StrategyConfigPage } from '@/app/components/pages/StrategyConfigPage';
import { strategyApi } from '@/services/strategyApi';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

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

// Mock ResizeObserver for Radix UI
global.ResizeObserver = class ResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
};

// Setup QueryClient
const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false
      }
    }
  });

describe('StrategyConfigPage', () => {
  let queryClient: QueryClient;
  const mockUniverse = {
    source: 'postgres_gold' as const,
    root: {
      kind: 'group' as const,
      operator: 'and' as const,
      clauses: [
        {
          kind: 'condition' as const,
          table: 'market_data',
          column: 'close',
          operator: 'gt' as const,
          value: 10
        }
      ]
    }
  };

  const mockCatalog = {
    source: 'postgres_gold' as const,
    tables: [
      {
        name: 'market_data',
        asOfColumn: 'date',
        columns: [
          {
            name: 'close',
            dataType: 'double precision',
            valueKind: 'number' as const,
            operators: ['eq', 'gt']
          }
        ]
      }
    ]
  };

  beforeEach(() => {
    queryClient = createTestQueryClient();
    vi.clearAllMocks();
    (strategyApi.getUniverseCatalog as Mock).mockResolvedValue(mockCatalog);
    (strategyApi.previewUniverse as Mock).mockResolvedValue({
      source: 'postgres_gold',
      symbolCount: 2,
      sampleSymbols: ['AAPL', 'MSFT'],
      tablesUsed: ['market_data'],
      warnings: []
    });
  });

  it('renders loading state initially', () => {
    (strategyApi.listStrategies as Mock).mockReturnValue(new Promise(() => {})); // pending promise

    render(
      <QueryClientProvider client={queryClient}>
        <StrategyConfigPage />
      </QueryClientProvider>
    );

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
        universe: mockUniverse,
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

    render(
      <QueryClientProvider client={queryClient}>
        <StrategyConfigPage />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(screen.getByText('strat-1')).toBeInTheDocument();
      expect(screen.getByText('strat-2')).toBeInTheDocument();
    });

    expect(screen.getByRole('button', { name: /view strategy strat-1/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /edit strategy strat-1/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /delete strategy strat-1/i })).toBeInTheDocument();
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
        universe: mockUniverse,
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

    render(
      <QueryClientProvider client={queryClient}>
        <StrategyConfigPage />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(screen.getByText('strat-1')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /view strategy strat-1/i }));

    await waitFor(() => {
      expect(strategyApi.getStrategyDetail).toHaveBeenCalledWith('strat-1');
    });

    await waitFor(() => {
      expect(screen.getByText(/Top 25 with 90-bar lookback/i)).toBeInTheDocument();
    });
    expect(screen.getByText(/1 condition across 1 table/i)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^Edit Strategy$/i }));

    await waitFor(() => {
      expect(screen.getByLabelText(/gold table/i)).toHaveValue('market_data');
    });

    expect(screen.getByDisplayValue('stop-8')).toBeInTheDocument();
    expect(screen.getByDisplayValue('take-15')).toBeInTheDocument();
  });

  it('opens editor when New Strategy button is clicked', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([]);

    render(
      <QueryClientProvider client={queryClient}>
        <StrategyConfigPage />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(screen.getByText(/new strategy/i)).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/new strategy/i));

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: /^New Strategy$/ })).toBeInTheDocument();
    });

    expect(strategyApi.getStrategyDetail).not.toHaveBeenCalled();
  });

  it('deletes a strategy from the page actions', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([
      { name: 'strat-1', type: 'configured', description: 'desc 1', updated_at: '2023-01-01' }
    ]);
    (strategyApi.deleteStrategy as Mock).mockResolvedValue({
      status: 'success',
      message: "Strategy 'strat-1' deleted successfully"
    });

    render(
      <QueryClientProvider client={queryClient}>
        <StrategyConfigPage />
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(screen.getByRole('button', { name: /delete strategy strat-1/i })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /delete strategy strat-1/i }));

    await waitFor(() => {
      expect(screen.getByRole('heading', { name: /delete strategy/i })).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole('button', { name: /delete from postgres/i }));

    await waitFor(() => {
      expect(strategyApi.deleteStrategy).toHaveBeenCalledWith('strat-1');
    });
  });
});
