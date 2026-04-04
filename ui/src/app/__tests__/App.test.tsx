import { describe, it, expect, vi } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import App from '../App';

vi.mock('@/hooks/useRealtime', () => ({
  useRealtime: () => undefined
}));

vi.mock('@/hooks/useDataQueries', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useDataQueries')>();

  return {
    ...actual,
    useSystemHealthQuery: () => ({
      data: {
        overall: 'healthy',
        dataLayers: [],
        recentJobs: [],
        alerts: []
      },
      isLoading: false,
      error: null
    }),
    useLineageQuery: () => ({
      data: { impactsByDomain: {} },
      isLoading: false,
      error: null
    })
  };
});

vi.mock('@/features/system-status/SystemStatusPage', () => ({
  SystemStatusPage: () => <div data-testid="mock-system-status">Mock System Status</div>
}));

vi.mock('@/features/data-explorer/DataExplorerPage', () => ({
  DataExplorerPage: () => <div data-testid="mock-data-explorer">Mock Data Explorer</div>
}));

vi.mock('@/features/regimes/RegimeMonitorPage', () => ({
  RegimeMonitorPage: () => <div data-testid="mock-regime-monitor">Mock Regime Monitor</div>
}));

vi.mock('@/features/strategies/StrategyConfigPage', () => ({
  StrategyConfigPage: () => <div data-testid="mock-strategy-config">Mock Strategy Workbench</div>
}));

vi.mock('@/features/universes/UniverseConfigPage', () => ({
  UniverseConfigPage: () => <div data-testid="mock-universe-config">Mock Universe Workbench</div>
}));

vi.mock('@/features/rankings/RankingConfigPage', () => ({
  RankingConfigPage: () => <div data-testid="mock-ranking-config">Mock Ranking Workbench</div>
}));

vi.mock('@/features/strategy-exploration/StrategyDataCatalogPage', () => ({
  StrategyDataCatalogPage: () => (
    <div data-testid="mock-strategy-data-catalog">Mock Strategy Data Catalog</div>
  )
}));

describe('App Smoke Test', () => {
  it('renders without crashing', async () => {
    window.history.pushState({}, 'System Status', '/system-status');
    renderWithProviders(<App />);
    expect(await screen.findByTestId('mock-system-status')).toBeInTheDocument();
  });

  it('shows a transition indicator when navigating between routes', async () => {
    window.history.pushState({}, 'System Status', '/system-status');
    renderWithProviders(<App />);
    expect(await screen.findByTestId('mock-system-status')).toBeInTheDocument();

    const indicator = screen.getByTestId('route-transition-indicator');
    expect(indicator).toHaveAttribute('data-state', 'idle');

    act(() => {
      window.history.pushState({}, 'Data Explorer', '/data-explorer');
      window.dispatchEvent(new PopStateEvent('popstate'));
    });

    await waitFor(() => {
      expect(screen.getByTestId('mock-data-explorer')).toBeInTheDocument();
      expect(indicator).not.toHaveAttribute('data-state', 'idle');
    });

    await waitFor(
      () => {
        expect(indicator).toHaveAttribute('data-state', 'idle');
      },
      { timeout: 2000 }
    );
  });

  it('renders the strategy workbench route through the application shell', async () => {
    window.history.pushState({}, 'Strategies', '/strategies');
    renderWithProviders(<App />);

    expect(await screen.findByTestId('mock-strategy-config')).toBeInTheDocument();
  });

  it('renders the ranking workbench route through the application shell', async () => {
    window.history.pushState({}, 'Rankings', '/rankings');
    renderWithProviders(<App />);

    expect(await screen.findByTestId('mock-ranking-config')).toBeInTheDocument();
  });

  it('renders the regime monitor route through the application shell', async () => {
    window.history.pushState({}, 'Regimes', '/regimes');
    renderWithProviders(<App />);

    expect(await screen.findByTestId('mock-regime-monitor')).toBeInTheDocument();
  });

  it('renders the universe workbench route through the application shell', async () => {
    window.history.pushState({}, 'Universes', '/universes');
    renderWithProviders(<App />);

    expect(await screen.findByTestId('mock-universe-config')).toBeInTheDocument();
  });

  it.each([
    '/run-configurations',
    '/universe-configurations',
    '/ranking-configurations',
    '/strategy-exploration/data-catalog',
    '/data-admin/symbol-purge'
  ])('redirects removed alias route %s back to the canonical shell entrypoint', async (path) => {
    window.history.pushState({}, 'Legacy Route', path);
    renderWithProviders(<App />);

    expect(await screen.findByTestId('mock-system-status')).toBeInTheDocument();
  });
});
