import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { DataQualityPage } from '@/app/components/pages/DataQualityPage';
import { DataService, type MarketData, type FinanceData } from '@/services/DataService';

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
    getMarketData: vi.fn(),
    getFinanceData: vi.fn(),
    getGenericData: vi.fn()
  }
}));

function makeHealthData() {
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
    ]
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
    vi.mocked(DataService.getMarketData).mockResolvedValue([{ symbol: 'SPY' }] as unknown as MarketData[]);
    vi.mocked(DataService.getFinanceData).mockResolvedValue([{ symbol: 'SPY' }] as unknown as FinanceData[]);
    vi.mocked(DataService.getGenericData).mockResolvedValue([{ symbol: 'SPY' }] as unknown as Record<string, unknown>[]);
  });

  it('renders main dashboard sections', async () => {
    renderWithProviders(<DataQualityPage />);
    expect(await screen.findByRole('heading', { name: /data quality/i })).toBeInTheDocument();
    expect(screen.getByText(/validation ledger/i)).toBeInTheDocument();
    expect(screen.getAllByText(/cross-layer lag/i).length).toBeGreaterThan(0);
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
    healthData.dataLayers[0].domains[0].portalUrl = 'javascript:alert(1)';
    healthData.dataLayers[0].domains[0].jobUrl = 'data:text/html,foo';
    healthData.dataLayers[0].domains[0].triggerUrl = 'https://evil.example.com';

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
});
