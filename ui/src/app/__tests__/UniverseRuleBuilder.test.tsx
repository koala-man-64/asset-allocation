import { fireEvent, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { useState } from 'react';

import { UniverseRuleBuilder } from '@/app/components/pages/strategy-editor/UniverseRuleBuilder';
import { buildEmptyUniverse } from '@/app/components/pages/strategy-editor/universeUtils';
import { strategyApi } from '@/services/strategyApi';
import { renderWithProviders } from '@/test/utils';
import type { UniverseDefinition } from '@/types/strategy';

vi.mock('@/services/strategyApi', () => ({
  strategyApi: {
    getUniverseCatalog: vi.fn(),
    previewUniverse: vi.fn()
  }
}));

function BuilderHarness() {
  const [value, setValue] = useState<UniverseDefinition>(buildEmptyUniverse());

  return (
    <>
      <UniverseRuleBuilder value={value} onChange={setValue} />
      <pre data-testid="universe-json">{JSON.stringify(value)}</pre>
    </>
  );
}

describe('UniverseRuleBuilder', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    vi.mocked(strategyApi.getUniverseCatalog).mockResolvedValue({
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
        },
        {
          name: 'finance_data',
          asOfColumn: 'obs_date',
          columns: [
            {
              name: 'f_score',
              dataType: 'integer',
              valueKind: 'number',
              operators: ['gte', 'lte']
            }
          ]
        }
      ]
    });

    vi.mocked(strategyApi.previewUniverse).mockResolvedValue({
      source: 'postgres_gold',
      symbolCount: 2,
      sampleSymbols: ['AAPL', 'MSFT'],
      tablesUsed: ['finance_data'],
      warnings: []
    });
  });

  it('supports nested groups, resets dependent selections on table change, and previews the current universe', async () => {
    renderWithProviders(<BuilderHarness />);

    await waitFor(() => {
      expect(screen.getByLabelText(/gold table/i)).toHaveValue('market_data');
    });

    fireEvent.click(screen.getByRole('button', { name: /add group to universe group root/i }));

    await waitFor(() => {
      expect(screen.getByText(/nested group/i)).toBeInTheDocument();
    });

    fireEvent.change(screen.getAllByLabelText(/gold table/i)[0], {
      target: { value: 'finance_data' }
    });

    await waitFor(() => {
      expect(screen.getAllByLabelText(/column/i)[0]).toHaveValue('f_score');
      expect(screen.getAllByLabelText(/operator/i)[0]).toHaveValue('gte');
    });

    fireEvent.change(screen.getAllByLabelText(/value/i)[0], {
      target: { value: '7' }
    });

    fireEvent.click(screen.getByRole('button', { name: /preview universe/i }));

    await waitFor(() => {
      expect(strategyApi.previewUniverse).toHaveBeenCalledWith({
        universe: expect.objectContaining({
          source: 'postgres_gold',
          root: expect.objectContaining({
            clauses: expect.arrayContaining([
              expect.objectContaining({
                kind: 'condition',
                table: 'finance_data',
                column: 'f_score',
                operator: 'gte',
                value: 7
              })
            ])
          })
        }),
        sampleLimit: 12
      });
    });

    expect(await screen.findByText(/2 symbols/i)).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('MSFT')).toBeInTheDocument();
    expect(screen.getByTestId('universe-json').textContent).toContain('"table":"finance_data"');
  });
});
