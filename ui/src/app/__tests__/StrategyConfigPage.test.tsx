import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import { StrategyConfigPage } from '@/app/components/pages/StrategyConfigPage';
import { strategyApi } from '@/services/strategyApi';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Mock dependencies
vi.mock('@/services/strategyApi', () => ({
  strategyApi: {
    listStrategies: vi.fn(),
    saveStrategy: vi.fn(),
    getStrategy: vi.fn()
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
    (strategyApi.listStrategies as any).mockReturnValue(new Promise(() => {})); // pending promise

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
    (strategyApi.listStrategies as any).mockResolvedValue(mockStrategies);

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

  it('opens editor when New Strategy button is clicked', async () => {
    (strategyApi.listStrategies as any).mockResolvedValue([]);

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
  });
});
