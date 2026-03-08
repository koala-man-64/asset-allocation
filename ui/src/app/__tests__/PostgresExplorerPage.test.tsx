import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import { fireEvent, screen, waitFor } from '@testing-library/react';

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
    vi.mocked(PostgresService.getTableMetadata).mockImplementation(async (schema: string, table: string) => ({
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
    }));
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

  it('purges the selected table after confirmation', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);

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
    expect(screen.getByText(/Purged 12 rows from core\.symbols\./i)).toBeInTheDocument();

    confirmSpy.mockRestore();
  });
});
