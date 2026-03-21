import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, Mock, vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { UniverseConfigPage } from '@/app/components/pages/UniverseConfigPage';
import { strategyApi } from '@/services/strategyApi';
import { universeApi } from '@/services/universeApi';

vi.mock('@/services/universeApi', () => ({
  universeApi: {
    listUniverseConfigs: vi.fn(),
    getUniverseConfigDetail: vi.fn(),
    saveUniverseConfig: vi.fn(),
    deleteUniverseConfig: vi.fn()
  }
}));

vi.mock('@/services/strategyApi', () => ({
  strategyApi: {
    getUniverseCatalog: vi.fn(),
    previewUniverse: vi.fn()
  }
}));

global.ResizeObserver = class ResizeObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
};

const createTestQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: {
        retry: false
      }
    }
  });

describe('UniverseConfigPage', () => {
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

  beforeEach(() => {
    queryClient = createTestQueryClient();
    vi.clearAllMocks();

    (universeApi.listUniverseConfigs as Mock).mockResolvedValue([
      {
        name: 'large-cap-quality',
        description: 'Large cap cohort',
        version: 2,
        updated_at: '2026-03-08T00:00:00Z'
      }
    ]);
    (universeApi.getUniverseConfigDetail as Mock).mockResolvedValue({
      name: 'large-cap-quality',
      description: 'Large cap cohort',
      version: 2,
      config: mockUniverse
    });
    (universeApi.saveUniverseConfig as Mock).mockResolvedValue({
      status: 'success',
      message: 'saved',
      version: 3
    });
    (strategyApi.getUniverseCatalog as Mock).mockResolvedValue({
      source: 'postgres_gold',
      tables: [
        {
          name: 'market_data',
          asOfColumn: 'date',
          columns: [
            {
              name: 'close',
              dataType: 'double precision',
              valueKind: 'number',
              operators: ['eq', 'gt']
            }
          ]
        }
      ]
    });
    (strategyApi.previewUniverse as Mock).mockResolvedValue({
      source: 'postgres_gold',
      symbolCount: 2,
      sampleSymbols: ['AAPL', 'MSFT'],
      tablesUsed: ['market_data'],
      warnings: []
    });
  });

  it('renders the universe configuration catalog and detail', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <UniverseConfigPage />
      </QueryClientProvider>
    );

    expect(await screen.findByDisplayValue('Large cap cohort')).toBeInTheDocument();
    expect(screen.getAllByText('large-cap-quality').length).toBeGreaterThan(0);
  });

  it('saves the current universe configuration draft', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <UniverseConfigPage />
      </QueryClientProvider>
    );

    expect(await screen.findByDisplayValue('Large cap cohort')).toBeInTheDocument();

    fireEvent.change(screen.getByDisplayValue('Large cap cohort'), {
      target: { value: 'Updated large cap cohort' }
    });
    fireEvent.click(screen.getByRole('button', { name: /save universe configuration/i }));

    await waitFor(() => {
      expect(universeApi.saveUniverseConfig).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'large-cap-quality',
          description: 'Updated large cap cohort',
          config: expect.objectContaining({
            source: 'postgres_gold'
          })
        })
      );
    });
  });
});
