import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import { StrategyDataCatalogPage } from '@/app/components/pages/StrategyDataCatalogPage';
import { useSystemStatusViewQuery } from '@/hooks/useSystemStatusView';
import { PostgresService } from '@/services/PostgresService';

vi.mock('@/hooks/useSystemStatusView', () => ({
  useSystemStatusViewQuery: vi.fn()
}));

vi.mock('@/services/PostgresService', () => ({
  PostgresService: {
    listSchemas: vi.fn(),
    listTables: vi.fn(),
    getTableMetadata: vi.fn(),
    listGoldColumnLookup: vi.fn()
  }
}));

describe('StrategyDataCatalogPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    vi.mocked(useSystemStatusViewQuery).mockReturnValue({
      data: {
        version: 1,
        generatedAt: '2026-03-22T12:00:00Z',
        sources: {
          systemHealth: 'cache',
          metadataSnapshot: 'persisted-snapshot'
        },
        systemHealth: {
          overall: 'healthy',
          recentJobs: [],
          alerts: [],
          dataLayers: [
            {
              name: 'Bronze',
              description: 'Landing zone for raw vendor payloads.',
              status: 'healthy',
              lastUpdated: '2026-03-22T09:00:00Z',
              refreshFrequency: 'daily',
              domains: [
                {
                  name: 'Market',
                  description: 'Unmodeled market payloads before enrichment.',
                  type: 'delta',
                  path: '/bronze/market',
                  lastUpdated: '2026-03-22T09:00:00Z',
                  status: 'healthy'
                }
              ]
            },
            {
              name: 'Gold',
              description: 'Serving contracts used by rankings and diagnostics.',
              status: 'healthy',
              lastUpdated: '2026-03-22T10:00:00Z',
              refreshFrequency: 'daily',
              domains: [
                {
                  name: 'Market',
                  description: 'Curated market factors and prices.',
                  type: 'delta',
                  path: '/gold/market',
                  lastUpdated: '2026-03-22T10:00:00Z',
                  status: 'healthy'
                },
                {
                  name: 'Regime',
                  description: 'Daily regime classifications and transitions.',
                  type: 'delta',
                  path: '/gold/regime',
                  lastUpdated: '2026-03-22T10:00:00Z',
                  status: 'healthy'
                }
              ]
            }
          ]
        },
        metadataSnapshot: {
          version: 1,
          updatedAt: '2026-03-22T10:05:00Z',
          warnings: [],
          entries: {
            'bronze/market': {
              layer: 'bronze',
              domain: 'market',
              container: 'lake',
              type: 'delta',
              computedAt: '2026-03-22T09:05:00Z',
              symbolCount: 8200,
              columnCount: 12,
              totalBytes: 4096,
              columns: ['date', 'symbol'],
              warnings: []
            },
            'gold/market': {
              layer: 'gold',
              domain: 'market',
              container: 'lake',
              type: 'delta',
              computedAt: '2026-03-22T10:05:00Z',
              symbolCount: 5100,
              columnCount: 18,
              totalBytes: 8192,
              columns: ['date', 'symbol', 'close'],
              dateRange: {
                min: '2026-01-01',
                max: '2026-03-21'
              },
              warnings: []
            },
            'gold/regime': {
              layer: 'gold',
              domain: 'regime',
              container: 'lake',
              type: 'delta',
              computedAt: '2026-03-22T10:05:00Z',
              symbolCount: 1,
              columnCount: 7,
              totalBytes: 2048,
              columns: ['as_of_date', 'regime_label'],
              warnings: []
            }
          }
        }
      },
      isLoading: false,
      isFetching: false,
      error: null,
      refresh: vi.fn()
    } as unknown as ReturnType<typeof useSystemStatusViewQuery>);

    vi.mocked(PostgresService.listSchemas).mockResolvedValue(['bronze', 'gold']);
    vi.mocked(PostgresService.listTables).mockImplementation(async (schema) => {
      if (schema === 'bronze') {
        return ['market_data'];
      }
      if (schema === 'gold') {
        return ['market_data', 'regime_history'];
      }
      return [];
    });
    vi.mocked(PostgresService.getTableMetadata).mockImplementation(async (schema, table) => {
      if (schema === 'gold' && table === 'regime_history') {
        return {
          schema_name: 'gold',
          table_name: 'regime_history',
          primary_key: ['as_of_date'],
          can_edit: false,
          edit_reason: 'Derived output.',
          columns: [
            {
              name: 'as_of_date',
              data_type: 'date',
              description: 'Trading date for the assigned market regime.',
              nullable: false,
              primary_key: true,
              editable: false
            },
            {
              name: 'regime_label',
              data_type: 'text',
              description: 'Canonical label emitted by the regime classifier.',
              nullable: false,
              primary_key: false,
              editable: false
            }
          ]
        };
      }

      return {
        schema_name: schema,
        table_name: table,
        primary_key: ['date', 'symbol'],
        can_edit: false,
        edit_reason: 'Serving contract is read only.',
        columns: [
          {
            name: 'date',
            data_type: 'date',
            description: 'Trading session date for the row.',
            nullable: false,
            primary_key: true,
            editable: false
          },
          {
            name: 'symbol',
            data_type: 'text',
            description: 'Ticker symbol for the asset.',
            nullable: false,
            primary_key: true,
            editable: false
          },
          {
            name: 'close',
            data_type: 'double precision',
            description: '',
            nullable: true,
            primary_key: false,
            editable: false
          }
        ]
      };
    });
    vi.mocked(PostgresService.listGoldColumnLookup).mockResolvedValue({
      rows: [
        {
          schema: 'gold',
          table: 'market_data',
          column: 'close',
          data_type: 'double precision',
          description: 'End-of-day close used by downstream ranking models.',
          calculation_type: 'source',
          calculation_notes: 'Copied from the gold market serving contract.',
          calculation_expression: null,
          calculation_dependencies: [],
          source_job: 'tasks.market_data.gold_market_data',
          status: 'reviewed',
          updated_at: '2026-03-22T10:00:00Z'
        }
      ],
      limit: 5000,
      offset: 0,
      has_more: false
    });
  });

  it('renders the atlas and hydrates the selected table contract', async () => {
    renderWithProviders(<StrategyDataCatalogPage />);

    expect(await screen.findByText('Domain Atlas')).toBeInTheDocument();
    expect(screen.getByText('Medallion Strips')).toBeInTheDocument();
    expect((await screen.findAllByText('market_data')).length).toBeGreaterThan(0);
    expect(await screen.findByText('Trading session date for the row.')).toBeInTheDocument();
    expect(screen.getAllByText('date').length).toBeGreaterThan(0);
    expect(screen.getByText('Ticker symbol for the asset.')).toBeInTheDocument();
  });

  it('focuses the atlas when a domain card is selected', async () => {
    renderWithProviders(<StrategyDataCatalogPage />);

    await screen.findAllByText('market_data');

    fireEvent.click(screen.getByRole('button', { name: /focus gold regime domain/i }));

    await waitFor(() => {
      expect(screen.getAllByText('regime_history').length).toBeGreaterThan(0);
      expect(screen.getByText('regime_label')).toBeInTheDocument();
      expect(
        screen.getByText('Canonical label emitted by the regime classifier.')
      ).toBeInTheDocument();
    });
  });
});
