import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import { screen, waitFor } from '@testing-library/react';

import { PostgresExplorerPage } from '@/app/components/pages/PostgresExplorerPage';
import { PostgresService } from '@/services/PostgresService';

vi.mock('@/services/PostgresService', () => ({
  PostgresService: {
    listSchemas: vi.fn(),
    listTables: vi.fn(),
    queryTable: vi.fn()
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
    vi.mocked(PostgresService.queryTable).mockResolvedValue([]);
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
});
