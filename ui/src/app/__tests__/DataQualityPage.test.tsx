import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { DataQualityPage } from '@/app/components/pages/DataQualityPage';
import { DataService } from '@/services/DataService';
import type { SystemHealth } from '@/types/strategy';

const { mockUseSystemHealthQuery, mockUseLineageQuery, mockGetLastSystemHealthMeta } = vi.hoisted(
  () => ({
    mockUseSystemHealthQuery: vi.fn(),
    mockUseLineageQuery: vi.fn(),
    mockGetLastSystemHealthMeta: vi.fn(() => null)
  })
);

vi.mock('@/hooks/useDataQueries', () => ({
  useSystemHealthQuery: mockUseSystemHealthQuery,
  useLineageQuery: mockUseLineageQuery,
  getLastSystemHealthMeta: mockGetLastSystemHealthMeta,
  queryKeys: {
    systemHealth: () => ['systemHealth']
  }
}));

vi.mock('@/services/DataService', () => ({
  DataService: {
    getSystemHealthWithMeta: vi.fn(),
    getDataQualityValidation: vi.fn(),
    getStorageUsage: vi.fn()
  }
}));

function makeHealthData(): SystemHealth {
  return {
    overall: 'healthy',
    dataLayers: [
      {
        name: 'silver',
        status: 'healthy',
        description: '',
        lastUpdated: '2026-02-06T00:00:00Z',
        refreshFrequency: 'Daily',
        domains: [
          {
            name: 'market',
            type: 'delta',
            path: 'market-data-by-date',
            lastUpdated: '2026-02-06T00:00:00Z',
            status: 'healthy',
            portalUrl: 'https://portal.azure.com/#resource/foo',
            jobUrl: 'https://portal.azure.com/#resource/bar',
            triggerUrl: 'https://portal.azure.com/#resource/baz'
          }
        ]
      }
    ],
    recentJobs: [],
    alerts: []
  };
}

describe('DataQualityPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseSystemHealthQuery.mockReturnValue({
      data: makeHealthData(),
      isLoading: false,
      error: null,
      isFetching: false,
      dataUpdatedAt: Date.now()
    });
    mockUseLineageQuery.mockReturnValue({
      data: { impactsByDomain: { market: ['strategy-1'] } },
      isLoading: false,
      error: null
    });
    vi.mocked(DataService.getSystemHealthWithMeta).mockResolvedValue({
      data: makeHealthData(),
      meta: {
        requestId: 'req-test-1',
        status: 200,
        durationMs: 25,
        url: '/api/system/health',
        cacheHint: 'miss',
        stale: false
      }
    });
    vi.mocked(DataService.getDataQualityValidation).mockResolvedValue({
      layer: 'silver',
      domain: 'market',
      status: 'healthy',
      rowCount: 42,
      columns: [],
      timestamp: '2026-02-06T00:00:00Z',
      sampleLimit: 1000
    });
    vi.mocked(DataService.getStorageUsage).mockResolvedValue({
      generatedAt: '2026-02-17T00:00:00Z',
      scanLimit: 200000,
      containers: [
        {
          layer: 'bronze',
          layerLabel: 'Bronze',
          container: 'bronze',
          totalFiles: 120,
          totalBytes: 1048576,
          truncated: false,
          folders: [
            {
              path: 'market-data/',
              fileCount: 40,
              totalBytes: 419430,
              truncated: false
            },
            {
              path: 'finance-data/',
              fileCount: 40,
              totalBytes: 327680,
              truncated: false
            }
          ]
        }
      ]
    });
  });

  vi.mock('@/app/components/pages/data-quality/DataPipelinePanel', () => ({
    DataPipelinePanel: () => <div data-testid="mock-data-pipeline-panel">Mock Pipeline Panel</div>
  }));

  it('renders main dashboard sections', async () => {
    renderWithProviders(<DataQualityPage />);
    expect(await screen.findByRole('heading', { name: /data quality/i })).toBeInTheDocument();

    // Header status card
    expect(screen.getByText(/^SYSTEM$/i)).toBeInTheDocument();
    expect(screen.getByText(/Overall/i)).toBeInTheDocument();
    expect(screen.getAllByText(/HEALTHY/i)[0]).toBeInTheDocument();
    expect(screen.getByText(/Failures/i)).toBeInTheDocument();
    expect(screen.getByText(/Stale\/Warn/i)).toBeInTheDocument();
    expect(screen.getByText(/Probes Failed/i)).toBeInTheDocument();

    // DataPipelinePanel (Mocked - Lazy Loaded)
    expect(await screen.findByTestId('mock-data-pipeline-panel')).toBeInTheDocument();

    // Ledger Table (Inline)
    const tables = screen.getAllByRole('table');
    expect(tables.length).toBeGreaterThan(0);

    const ledgerTable = tables.find((table) =>
      within(table).queryByRole('columnheader', { name: /Domain/i })
    );
    if (!ledgerTable) {
      throw new Error('Validation ledger table not found');
    }

    expect(within(ledgerTable).getByRole('columnheader', { name: /Layer/i })).toBeInTheDocument();
    expect(within(ledgerTable).getByRole('columnheader', { name: /Domain/i })).toBeInTheDocument();
  });

  it('renders loading state', () => {
    mockUseSystemHealthQuery.mockReturnValue({
      data: null,
      isLoading: true,
      error: null,
      isFetching: false,
      dataUpdatedAt: 0
    });
    renderWithProviders(<DataQualityPage />);
    expect(screen.getByText(/loading validation ledger/i)).toBeInTheDocument();
  });

  it('sanitizes unsafe outbound links', async () => {
    const healthData = makeHealthData();
    const layer = healthData.dataLayers[0];
    if (layer && layer.domains && layer.domains[0]) {
      layer.domains[0].portalUrl = 'javascript:alert(1)';
      layer.domains[0].jobUrl = 'data:text/html,foo';
      layer.domains[0].triggerUrl = 'https://evil.example.com';
    }

    mockUseSystemHealthQuery.mockReturnValue({
      data: healthData,
      isLoading: false,
      error: null,
      isFetching: false,
      dataUpdatedAt: Date.now()
    });

    renderWithProviders(<DataQualityPage />);
    expect(await screen.findByRole('heading', { name: /data quality/i })).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /open portal/i })).toBeNull();
    expect(screen.queryByRole('link', { name: /open job/i })).toBeNull();
    expect(screen.queryByRole('link', { name: /trigger/i })).toBeNull();
  });

  it('forces refresh with refresh=true on click', async () => {
    renderWithProviders(<DataQualityPage />);
    const refreshButton = await screen.findByRole('button', { name: /^refresh$/i });
    fireEvent.click(refreshButton);
    expect(DataService.getSystemHealthWithMeta).toHaveBeenCalledWith({ refresh: true });
  });

  it('allows probe actions without requiring a symbol', async () => {
    renderWithProviders(<DataQualityPage />);

    const runButtons = await screen.findAllByRole('button', { name: /run probes/i });
    expect(runButtons.length).toBeGreaterThan(0);
    for (const button of runButtons) {
      expect(button).toBeEnabled();
    }

    const symbolInput = screen.getByLabelText(/symbol/i);
    fireEvent.change(symbolInput, { target: { value: 'AAPL' } });

    for (const button of runButtons) {
      expect(button).toBeEnabled();
    }
  });
});
