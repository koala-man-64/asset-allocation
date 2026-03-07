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
    getStrategyDetail: vi.fn()
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

  beforeEach(() => {
    queryClient = createTestQueryClient();
    vi.clearAllMocks();
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
        universe: 'SP500',
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
  });

  it('loads strategy detail when editing an existing strategy', async () => {
    (strategyApi.listStrategies as Mock).mockResolvedValue([
      { name: 'strat-1', type: 'configured', description: 'desc 1', updated_at: '2023-01-01' }
    ]);
    (strategyApi.getStrategyDetail as Mock).mockResolvedValue({
      name: 'strat-1',
      type: 'configured',
      description: 'desc 1',
      config: {
        universe: 'NDX',
        rebalance: 'weekly',
        longOnly: true,
        topN: 25,
        lookbackWindow: 90,
        holdingPeriod: 30,
        costModel: 'default',
        intrabarConflictPolicy: 'stop_first',
        exits: [{ id: 'stop-8', enabled: true, type: 'stop_loss_fixed', scope: 'position', action: 'exit_full', minHoldBars: 0, priceField: 'low', reference: 'entry_price', value: 0.08, priority: 0 }]
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

    fireEvent.click(screen.getByText('strat-1'));

    await waitFor(() => {
      expect(strategyApi.getStrategyDetail).toHaveBeenCalledWith('strat-1');
    });

    await waitFor(() => {
      expect(screen.getByLabelText(/universe/i)).toHaveValue('NDX');
    });

    expect(screen.getByDisplayValue('stop-8')).toBeInTheDocument();
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
});
