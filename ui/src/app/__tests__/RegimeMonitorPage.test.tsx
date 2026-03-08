import { fireEvent, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RegimeMonitorPage } from '@/app/components/pages/RegimeMonitorPage';
import { regimeApi } from '@/services/regimeApi';
import { renderWithProviders } from '@/test/utils';

vi.mock('@/services/regimeApi', () => ({
  regimeApi: {
    listModels: vi.fn(),
    getModel: vi.fn(),
    getCurrent: vi.fn(),
    getHistory: vi.fn(),
    createModel: vi.fn(),
    activateModel: vi.fn()
  }
}));

describe('RegimeMonitorPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(regimeApi.listModels).mockResolvedValue({
      models: [
        {
          name: 'default-regime',
          description: 'Default',
          version: 1,
          active_version: 1,
          updated_at: '2026-03-08T00:00:00Z'
        }
      ]
    });
    vi.mocked(regimeApi.getModel).mockResolvedValue({
      model: {
        name: 'default-regime',
        description: 'Default',
        version: 1,
        active_version: 1,
        updated_at: '2026-03-08T00:00:00Z'
      },
      activeRevision: {
        name: 'default-regime',
        version: 1,
        description: 'Default',
        config: {}
      },
      revisions: [
        {
          name: 'default-regime',
          version: 1,
          description: 'Default',
          config: {},
          published_at: '2026-03-08T00:00:00Z',
          activated_at: '2026-03-08T00:00:00Z'
        }
      ],
      latest: null
    });
    vi.mocked(regimeApi.getCurrent).mockResolvedValue({
      as_of_date: '2026-03-07',
      effective_from_date: '2026-03-10',
      model_name: 'default-regime',
      model_version: 1,
      regime_code: 'trending_bull',
      regime_status: 'confirmed',
      matched_rule_id: 'trending_bull',
      halt_flag: false,
      halt_reason: null,
      spy_return_20d: 0.0421,
      rvol_10d_ann: 13.2,
      vix_spot_close: 18.4,
      vix3m_close: 19.1,
      vix_slope: 0.7,
      trend_state: 'positive',
      curve_state: 'contango',
      vix_gt_32_streak: 0,
      computed_at: '2026-03-08T00:00:00Z'
    });
    vi.mocked(regimeApi.getHistory).mockResolvedValue({
      modelName: 'default-regime',
      modelVersion: 1,
      limit: 24,
      rows: [
        {
          as_of_date: '2026-03-07',
          effective_from_date: '2026-03-10',
          model_name: 'default-regime',
          model_version: 1,
          regime_code: 'trending_bull',
          regime_status: 'confirmed',
          matched_rule_id: 'trending_bull',
          halt_flag: false,
          halt_reason: null
        }
      ]
    });
    vi.mocked(regimeApi.createModel).mockResolvedValue({
      model: {
        name: 'new-model',
        description: 'Created',
        version: 1
      }
    });
    vi.mocked(regimeApi.activateModel).mockResolvedValue({
      model: 'default-regime',
      activatedRevision: {
        name: 'default-regime',
        version: 1,
        description: 'Default',
        config: {}
      }
    });
  });

  it('renders the current snapshot and history rows', async () => {
    renderWithProviders(<RegimeMonitorPage />);

    expect((await screen.findAllByText(/trending bull/i)).length).toBeGreaterThan(0);
    expect(screen.getByText(/History/i)).toBeInTheDocument();
    expect(screen.getAllByText(/Mar 7, 2026/i).length).toBeGreaterThan(0);
  });

  it('saves a new regime model revision', async () => {
    renderWithProviders(<RegimeMonitorPage />);

    fireEvent.change(await screen.findByLabelText(/Name/i), {
      target: { value: 'new-model' }
    });
    fireEvent.click(screen.getByRole('button', { name: /save model revision/i }));

    await waitFor(() => {
      expect(regimeApi.createModel).toHaveBeenCalledWith(
        expect.objectContaining({ name: 'new-model' })
      );
    });
  });
});
