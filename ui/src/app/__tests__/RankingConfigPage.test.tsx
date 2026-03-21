import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, Mock, vi } from 'vitest';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RankingConfigPage } from '@/app/components/pages/RankingConfigPage';
import { rankingApi } from '@/services/rankingApi';
import { strategyApi } from '@/services/strategyApi';
import { universeApi } from '@/services/universeApi';

vi.mock('@/services/rankingApi', () => ({
  rankingApi: {
    listRankingSchemas: vi.fn(),
    getRankingSchemaDetail: vi.fn(),
    getRankingCatalog: vi.fn(),
    saveRankingSchema: vi.fn(),
    deleteRankingSchema: vi.fn(),
    previewRanking: vi.fn(),
    materializeRankings: vi.fn()
  }
}));

vi.mock('@/services/strategyApi', () => ({
  strategyApi: {
    listStrategies: vi.fn()
  }
}));

vi.mock('@/services/universeApi', () => ({
  universeApi: {
    listUniverseConfigs: vi.fn()
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

describe('RankingConfigPage', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = createTestQueryClient();
    vi.clearAllMocks();
    (rankingApi.listRankingSchemas as Mock).mockResolvedValue([
      {
        name: 'quality-momentum',
        description: 'Composite ranking',
        version: 2,
        updated_at: '2026-03-08T00:00:00Z'
      }
    ]);
    (rankingApi.getRankingSchemaDetail as Mock).mockResolvedValue({
      name: 'quality-momentum',
      description: 'Composite ranking',
      version: 2,
      config: {
        universeConfigName: 'large-cap-quality',
        groups: [
          {
            name: 'quality',
            weight: 1,
            transforms: [],
            factors: [
              {
                name: 'f1',
                table: 'market_data',
                column: 'return_20d',
                weight: 1,
                direction: 'desc',
                missingValuePolicy: 'exclude',
                transforms: []
              }
            ]
          }
        ],
        overallTransforms: []
      }
    });
    (rankingApi.getRankingCatalog as Mock).mockResolvedValue({
      source: 'postgres_gold',
      tables: [
        {
          name: 'market_data',
          asOfColumn: 'date',
          columns: [{ name: 'return_20d', dataType: 'double precision', valueKind: 'number' }]
        }
      ]
    });
    (rankingApi.previewRanking as Mock).mockResolvedValue({
      strategyName: 'mom-spy-res',
      asOfDate: '2026-03-08',
      rowCount: 2,
      rows: [
        { symbol: 'AAPL', rank: 1, score: 0.9 },
        { symbol: 'MSFT', rank: 2, score: 0.8 }
      ],
      warnings: []
    });
    (rankingApi.saveRankingSchema as Mock).mockResolvedValue({
      status: 'success',
      message: 'saved',
      version: 3
    });
    (strategyApi.listStrategies as Mock).mockResolvedValue([
      { name: 'mom-spy-res', type: 'configured', description: 'desc', updated_at: '2026-03-08' }
    ]);
    (universeApi.listUniverseConfigs as Mock).mockResolvedValue([
      {
        name: 'large-cap-quality',
        description: 'desc',
        version: 1,
        updated_at: '2026-03-08T00:00:00Z'
      }
    ]);
  });

  it('renders schema catalog and detail', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <RankingConfigPage />
      </QueryClientProvider>
    );

    expect(await screen.findByDisplayValue('Composite ranking')).toBeInTheDocument();
    expect(screen.getAllByText('quality-momentum').length).toBeGreaterThan(0);
  });

  it('previews the current draft schema against a strategy', async () => {
    render(
      <QueryClientProvider client={queryClient}>
        <RankingConfigPage />
      </QueryClientProvider>
    );

    expect(await screen.findByDisplayValue('Composite ranking')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^Preview$/i }));

    await waitFor(() => {
      expect(rankingApi.previewRanking).toHaveBeenCalled();
    });
    expect(await screen.findByText('AAPL')).toBeInTheDocument();
  });
});
