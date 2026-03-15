import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { StrategyDataCatalogPage } from '@/app/components/pages/StrategyDataCatalogPage';
import { PostgresService } from '@/services/PostgresService';

vi.mock('@/services/PostgresService', () => ({
  PostgresService: {
    listGoldLookupTables: vi.fn(),
    listGoldColumnLookup: vi.fn(),
  },
}));

describe('StrategyDataCatalogPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(PostgresService.listGoldLookupTables).mockResolvedValue([
      'market_data',
      'finance_data',
    ]);
    vi.mocked(PostgresService.listGoldColumnLookup).mockResolvedValue({
      rows: [
        {
          schema: 'gold',
          table: 'market_data',
          column: 'trend_50_200',
          data_type: 'double precision',
          description: 'Trend feature from moving-average spread.',
          calculation_type: 'derived_python',
          calculation_notes: 'Computed by market gold pipeline.',
          calculation_expression: null,
          calculation_dependencies: ['sma_50d', 'sma_200d'],
          source_job: 'tasks.market_data.gold_market_data',
          status: 'reviewed',
          updated_at: '2026-03-15T00:00:00+00:00',
        },
      ],
      limit: 5000,
      offset: 0,
      has_more: false,
    });
  });

  it('loads lookup tables and shows column catalog rows', async () => {
    renderWithProviders(<StrategyDataCatalogPage />);

    expect(await screen.findByText('market_data')).toBeInTheDocument();
    expect(await screen.findByText('trend_50_200')).toBeInTheDocument();
    expect(screen.getByText('Trend feature from moving-average spread.')).toBeInTheDocument();
  });

  it('adds selected columns to export list without duplicates', async () => {
    renderWithProviders(<StrategyDataCatalogPage />);
    await screen.findByText('trend_50_200');

    const columnCheckbox = screen.getByRole('checkbox', { name: /select column trend_50_200/i });
    fireEvent.click(columnCheckbox);

    const addButton = screen.getByRole('button', { name: /add to export list/i });
    fireEvent.click(addButton);
    fireEvent.click(addButton);

    await waitFor(() => {
      expect(screen.getAllByText('trend_50_200').length).toBeGreaterThan(0);
    });

    const exportRows = screen.getAllByRole('button', { name: /remove trend_50_200/i });
    expect(exportRows).toHaveLength(1);
  });
});
