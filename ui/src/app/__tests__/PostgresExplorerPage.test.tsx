import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import { fireEvent, screen, waitFor, within } from '@testing-library/react';

import { PostgresExplorerPage } from '@/app/components/pages/PostgresExplorerPage';
import { PostgresService } from '@/services/PostgresService';

vi.mock('@/services/PostgresService', () => ({
  PostgresService: {
    listSchemas: vi.fn(),
    listTables: vi.fn(),
    getTableMetadata: vi.fn(),
    queryTable: vi.fn(),
    updateRow: vi.fn(),
    purgeTable: vi.fn()
  }
}));

describe('PostgresExplorerPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(PostgresService.listSchemas).mockResolvedValue([
      'public',
      'information_schema',
      'core',
      'gold'
    ]);
    vi.mocked(PostgresService.listTables).mockImplementation(async (schema: string) => {
      if (schema === 'core') {
        return ['symbols', 'runtime_config'];
      }
      if (schema === 'gold') {
        return ['market_features'];
      }
      return ['should_not_be_used'];
    });
    vi.mocked(PostgresService.getTableMetadata).mockImplementation(
      async (schema: string, table: string) => ({
        schema_name: schema,
        table_name: table,
        primary_key: ['symbol'],
        can_edit: true,
        edit_reason: null,
        columns: [
          {
            name: 'symbol',
            data_type: 'TEXT',
            nullable: false,
            primary_key: true,
            editable: true,
            edit_reason: null
          },
          {
            name: 'company_name',
            data_type: 'TEXT',
            nullable: true,
            primary_key: false,
            editable: true,
            edit_reason: null
          }
        ]
      })
    );
    vi.mocked(PostgresService.queryTable).mockResolvedValue([
      { symbol: 'AAPL', company_name: 'Apple' }
    ]);
    vi.mocked(PostgresService.updateRow).mockResolvedValue({
      schema_name: 'core',
      table_name: 'symbols',
      row_count: 1,
      updated_columns: ['company_name']
    });
    vi.mocked(PostgresService.purgeTable).mockResolvedValue({
      schema_name: 'core',
      table_name: 'symbols',
      row_count: 12
    });
  });

  it('hides public and information_schema and auto-selects the first visible schema', async () => {
    renderWithProviders(<PostgresExplorerPage />);

    await waitFor(() => {
      expect(PostgresService.listTables).toHaveBeenCalledWith('core');
    });

    const schemaSelect = screen.getByRole('combobox', { name: /schema/i });
    const schemaOptions = screen.getAllByRole('option').map((option) => option.textContent);

    expect(schemaSelect).toHaveValue('core');
    expect(schemaOptions).toContain('core');
    expect(schemaOptions).toContain('gold');
    expect(schemaOptions).not.toContain('public');
    expect(schemaOptions).not.toContain('information_schema');
  });

  it('opens a row editor and saves field updates', async () => {
    renderWithProviders(<PostgresExplorerPage />);

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'symbols');
    });

    fireEvent.click(screen.getByRole('button', { name: /query table/i }));

    await screen.findByText('AAPL');

    fireEvent.click(screen.getByText('AAPL'));

    const nameField = await screen.findByLabelText(/company_name/i);
    fireEvent.change(nameField, { target: { value: 'Apple Inc' } });
    fireEvent.click(screen.getByRole('button', { name: /save row/i }));

    await waitFor(() => {
      expect(PostgresService.updateRow).toHaveBeenCalledWith({
        schema_name: 'core',
        table_name: 'symbols',
        match: { symbol: 'AAPL' },
        values: {
          symbol: 'AAPL',
          company_name: 'Apple Inc'
        }
      });
    });
  });

  it('adds server-side filters to the query request', async () => {
    renderWithProviders(<PostgresExplorerPage />);

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'symbols');
    });

    fireEvent.click(screen.getByRole('button', { name: /add filter/i }));
    fireEvent.change(screen.getByLabelText(/value/i), {
      target: { value: 'AAP' }
    });

    fireEvent.click(screen.getByRole('button', { name: /query table/i }));

    await waitFor(() => {
      expect(PostgresService.queryTable).toHaveBeenCalledWith({
        schema_name: 'core',
        table_name: 'symbols',
        limit: 100,
        filters: [
          {
            column_name: 'symbol',
            operator: 'contains',
            value: 'AAP'
          }
        ]
      });
    });
  });

  it('sorts query results by column when a header is clicked', async () => {
    vi.mocked(PostgresService.queryTable).mockResolvedValue([
      { symbol: 'MSFT', company_name: 'Microsoft' },
      { symbol: 'AAPL', company_name: 'Apple' },
      { symbol: 'GOOG', company_name: 'Google' }
    ]);

    renderWithProviders(<PostgresExplorerPage />);

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'symbols');
    });

    fireEvent.click(screen.getByRole('button', { name: /query table/i }));
    await screen.findByText('MSFT');

    fireEvent.click(screen.getByRole('button', { name: /symbol/i }));

    const ascRows = screen.getAllByRole('row').slice(1);
    expect(within(ascRows[0]).getAllByRole('cell')[1]).toHaveTextContent('AAPL');
    expect(within(ascRows[1]).getAllByRole('cell')[1]).toHaveTextContent('GOOG');
    expect(within(ascRows[2]).getAllByRole('cell')[1]).toHaveTextContent('MSFT');

    fireEvent.click(screen.getByRole('button', { name: /symbol/i }));

    const descRows = screen.getAllByRole('row').slice(1);
    expect(within(descRows[0]).getAllByRole('cell')[1]).toHaveTextContent('MSFT');
    expect(within(descRows[1]).getAllByRole('cell')[1]).toHaveTextContent('GOOG');
    expect(within(descRows[2]).getAllByRole('cell')[1]).toHaveTextContent('AAPL');
  });

  it('does not load metadata for the previously selected table after a schema change', async () => {
    vi.mocked(PostgresService.getTableMetadata).mockImplementation(
      async (schema: string, table: string) => {
        if (schema === 'gold' && table === 'symbols') {
          throw new Error('stale metadata request');
        }
        return {
          schema_name: schema,
          table_name: table,
          primary_key: ['symbol'],
          can_edit: true,
          edit_reason: null,
          columns: [
            {
              name: 'symbol',
              data_type: 'TEXT',
              nullable: false,
              primary_key: true,
              editable: true,
              edit_reason: null
            }
          ]
        };
      }
    );

    renderWithProviders(<PostgresExplorerPage />);

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'symbols');
    });

    fireEvent.change(screen.getByRole('combobox', { name: /schema/i }), {
      target: { value: 'gold' }
    });

    await waitFor(() => {
      expect(PostgresService.listTables).toHaveBeenCalledWith('gold');
    });

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('gold', 'market_features');
    });

    expect(PostgresService.getTableMetadata).not.toHaveBeenCalledWith('gold', 'symbols');
    expect(screen.queryByText(/stale metadata request/i)).not.toBeInTheDocument();
  });

  it('clears the visible error when the table dropdown changes', async () => {
    vi.mocked(PostgresService.queryTable).mockRejectedValueOnce(new Error('query exploded'));

    renderWithProviders(<PostgresExplorerPage />);

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'symbols');
    });

    fireEvent.click(screen.getByRole('button', { name: /query table/i }));

    await screen.findByText(/query exploded/i);

    fireEvent.change(screen.getByRole('combobox', { name: /table/i }), {
      target: { value: 'runtime_config' }
    });

    await waitFor(() => {
      expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'runtime_config');
    });

    await waitFor(() => {
      expect(screen.queryByText(/query exploded/i)).not.toBeInTheDocument();
    });
  });

  it('purges the selected table after confirmation', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);

    try {
      renderWithProviders(<PostgresExplorerPage />);

      await waitFor(() => {
        expect(PostgresService.getTableMetadata).toHaveBeenCalledWith('core', 'symbols');
      });

      fireEvent.click(screen.getByRole('button', { name: /purge table/i }));

      await waitFor(() => {
        expect(PostgresService.purgeTable).toHaveBeenCalledWith({
          schema_name: 'core',
          table_name: 'symbols'
        });
      });

      expect(confirmSpy).toHaveBeenCalledWith(
        'Purge all rows from core.symbols? This action cannot be undone.'
      );
      expect(await screen.findByText(/Purged 12 rows from core\.symbols\./i)).toBeInTheDocument();
    } finally {
      confirmSpy.mockRestore();
    }
  });
});
