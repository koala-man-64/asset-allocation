import { beforeEach, describe, expect, it, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { DomainLayerComparisonPanel } from '@/app/components/pages/system-status/DomainLayerComparisonPanel';
import { DataService } from '@/services/DataService';
import { renderWithProviders } from '@/test/utils';
import type { DataLayer, JobRun } from '@/types/strategy';

const setJobSuspendedMock = vi.fn().mockResolvedValue(undefined);
const triggerJobMock = vi.fn().mockResolvedValue(undefined);

vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    warning: vi.fn()
  }
}));

vi.mock('@/hooks/useJobSuspend', () => ({
  useJobSuspend: () => ({
    jobControl: null,
    setJobSuspended: setJobSuspendedMock
  })
}));

vi.mock('@/hooks/useJobTrigger', () => ({
  useJobTrigger: () => ({
    triggeringJob: null,
    triggerJob: triggerJobMock
  })
}));

vi.mock('@/app/components/pages/system-status/DomainListViewerSheet', () => ({
  DomainListViewerSheet: () => null
}));

vi.mock('@/app/components/pages/system-status/JobKillSwitchPanel', () => ({
  JobKillSwitchInline: () => null
}));

vi.mock('@/services/DataService', () => ({
  DataService: {
    getDomainMetadataSnapshot: vi.fn(),
    getDomainMetadata: vi.fn(),
    invalidateSystemHealth: vi.fn()
  }
}));

const NOW = '2026-03-03T12:00:00Z';

function primeSnapshot(entry: {
  layer: 'bronze' | 'silver' | 'gold' | 'platinum';
  domain: string;
  container: string;
  type: 'delta' | 'blob';
  computedAt: string;
  symbolCount: number;
  columnCount?: number;
  totalBytes?: number;
  dateRange?: {
    min?: string;
    max?: string;
    source?: 'artifact' | 'partition' | 'stats';
    column?: string;
  };
  warnings: string[];
}) {
  const key = `${entry.layer}/${entry.domain}`;
  const payload = {
    version: 1,
    updatedAt: NOW,
    entries: {
      [key]: entry
    }
  };
  vi.mocked(DataService.getDomainMetadataSnapshot).mockResolvedValue(payload);
}

function makeLayers(): DataLayer[] {
  return [
    {
      name: 'Bronze',
      description: 'Raw ingestion',
      status: 'healthy',
      lastUpdated: NOW,
      refreshFrequency: 'daily',
      domains: [
        {
          name: 'market',
          type: 'delta',
          path: 'market-data',
          lastUpdated: NOW,
          status: 'healthy',
          jobName: 'aca-job-market'
        }
      ]
    }
  ];
}

function makeLayersWithEmptyPlatinum(): DataLayer[] {
  return [
    ...makeLayers(),
    {
      name: 'Platinum',
      description: 'Serving layer',
      status: 'healthy',
      lastUpdated: NOW,
      refreshFrequency: 'daily',
      domains: []
    }
  ];
}

function makeLayerTriggerLayers(): DataLayer[] {
  return [
    {
      name: 'Bronze',
      description: 'Raw ingestion',
      status: 'healthy',
      lastUpdated: NOW,
      refreshFrequency: 'daily',
      domains: [
        {
          name: 'market',
          type: 'delta',
          path: 'market-data',
          lastUpdated: NOW,
          status: 'healthy',
          jobName: 'aca-job-market-bronze'
        },
        {
          name: 'earnings',
          type: 'delta',
          path: 'earnings-data',
          lastUpdated: NOW,
          status: 'healthy',
          jobName: 'aca-job-earnings-bronze'
        }
      ]
    },
    {
      name: 'Silver',
      description: 'Normalized data',
      status: 'healthy',
      lastUpdated: NOW,
      refreshFrequency: 'daily',
      domains: [
        {
          name: 'market',
          type: 'delta',
          path: 'market-data',
          lastUpdated: NOW,
          status: 'healthy',
          jobName: 'aca-job-market-silver'
        }
      ]
    }
  ];
}

function makeJobs(status: JobRun['status'] = 'success', statusCode?: string): JobRun[] {
  return [
    {
      jobName: 'aca-job-market',
      jobType: 'data-ingest',
      status,
      statusCode,
      startTime: NOW,
      triggeredBy: 'test'
    }
  ];
}

describe('DomainLayerComparisonPanel refresh menu', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.localStorage.clear();

    vi.mocked(DataService.getDomainMetadataSnapshot).mockResolvedValue({
      version: 1,
      updatedAt: NOW,
      entries: {}
    });
    vi.mocked(DataService.getDomainMetadata).mockResolvedValue({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      metadataSource: 'artifact',
      symbolCount: 123,
      columnCount: 9,
      warnings: []
    });
  });

  it('refreshes both status and metadata from the layer header action', async () => {
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={onRefresh}
        isRefreshing={false}
        isFetching={false}
      />
    );

    const refreshLayerButton = await screen.findByRole('button', { name: 'Refresh Bronze layer' });
    await user.click(refreshLayerButton);

    await waitFor(() => {
      expect(onRefresh).toHaveBeenCalledTimes(1);
    });
    await waitFor(() => {
      expect(DataService.getDomainMetadata).toHaveBeenCalledWith('bronze', 'market', {
        refresh: true
      });
    });
    expect(await screen.findByText(/updated Mar 3,?\s+06:00 CST/)).toBeInTheDocument();
  });

  it('shows the metadata timestamp when cached entries only have computedAt', async () => {
    primeSnapshot({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      symbolCount: 123,
      warnings: []
    });

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect(await screen.findByText(/updated Mar 3,?\s+06:00 CST/)).toBeInTheDocument();
  });

  it('shows WARN for medallion-domain jobs with warning status codes', async () => {
    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs('warning', 'SucceededWithWarnings')}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect(await screen.findAllByText('WARN')).not.toHaveLength(0);
    expect(screen.getAllByTitle('SucceededWithWarnings').length).toBeGreaterThan(0);
  });

  it('keeps the panel header focused on the title without status badges', async () => {
    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="critical"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect(await screen.findByText('Domain Layer Coverage')).toBeInTheDocument();
    expect(screen.queryByText('Release')).not.toBeInTheDocument();
    expect(screen.queryByText('System status')).not.toBeInTheDocument();
    expect(screen.queryByText(/Uptime clock/i)).not.toBeInTheDocument();
  });

  it('shows the column count in the medallion-domain coverage panel', async () => {
    primeSnapshot({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      symbolCount: 123,
      columnCount: 9,
      warnings: []
    });

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect((await screen.findAllByText('9 cols')).length).toBeGreaterThan(0);
  });

  it('shows the storage size in the medallion-domain coverage panel', async () => {
    primeSnapshot({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      symbolCount: 123,
      columnCount: 9,
      totalBytes: 2048,
      warnings: []
    });

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect((await screen.findAllByText('9 cols • 2.0 KB')).length).toBeGreaterThan(0);
  });

  it('shows the date range in medallion-domain metadata and detail subpanels', async () => {
    primeSnapshot({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      symbolCount: 123,
      dateRange: {
        min: '2026-01-02T00:00:00Z',
        max: '2026-03-03T00:00:00Z',
        source: 'stats',
        column: 'Date'
      },
      warnings: []
    });

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect(await screen.findByText('range 2026-01-02 → 2026-03-03')).toBeInTheDocument();
    expect(screen.getAllByText('date range:').length).toBeGreaterThan(0);
    expect(screen.getAllByTitle('column=Date • source=stats').length).toBeGreaterThan(0);
  });

  it('omits the timestamp line when metadata has no computedAt', async () => {
    primeSnapshot({
      layer: 'bronze' as const,
      domain: 'market',
      container: 'bronze',
      type: 'delta' as const,
      computedAt: '',
      symbolCount: 123,
      warnings: []
    });

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    await waitFor(() => {
      expect(DataService.getDomainMetadataSnapshot).toHaveBeenCalledTimes(1);
    });
    expect((await screen.findAllByText('market')).length).toBeGreaterThan(0);
    expect(screen.queryByText(/^updated /i)).not.toBeInTheDocument();
  });

  it('shows a row-level refreshing indicator in the medallion-domain view during refresh', async () => {
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    let resolveMetadata: ((value: Awaited<ReturnType<typeof DataService.getDomainMetadata>>) => void) | null =
      null;
    const metadataPromise = new Promise<Awaited<ReturnType<typeof DataService.getDomainMetadata>>>((resolve) => {
      resolveMetadata = resolve;
    });
    vi.mocked(DataService.getDomainMetadata).mockReturnValue(metadataPromise);

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={onRefresh}
        isRefreshing={false}
        isFetching={false}
      />
    );

    const refreshLayerButton = await screen.findByRole('button', { name: 'Refresh Bronze layer' });
    await user.click(refreshLayerButton);

    expect(await screen.findByTestId('domain-refresh-indicator-market')).toBeInTheDocument();
    expect(screen.getByTestId('cell-refresh-icon-summary-market-bronze')).toBeInTheDocument();
    expect(screen.getByTestId('cell-refresh-icon-detail-market-bronze')).toBeInTheDocument();

    resolveMetadata?.({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      metadataSource: 'artifact',
      symbolCount: 123,
      warnings: []
    });

    await waitFor(() => {
      expect(screen.queryByTestId('domain-refresh-indicator-market')).not.toBeInTheDocument();
      expect(screen.queryByTestId('cell-refresh-icon-summary-market-bronze')).not.toBeInTheDocument();
      expect(screen.queryByTestId('cell-refresh-icon-detail-market-bronze')).not.toBeInTheDocument();
    });
  });

  it('refreshes merged header coverage action with live metadata and updates zero counts', async () => {
    const user = userEvent.setup();
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    vi.mocked(DataService.getDomainMetadata).mockResolvedValue({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      metadataSource: 'artifact',
      symbolCount: 0,
      warnings: []
    });

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        onRefresh={onRefresh}
        isRefreshing={false}
        isFetching={false}
      />
    );

    const refreshButton = await screen.findByRole('button', { name: 'Refresh domain layer coverage' });
    await user.click(refreshButton);

    await waitFor(() => {
      expect(onRefresh).toHaveBeenCalledTimes(1);
    });
    await waitFor(() => {
      expect(DataService.getDomainMetadata).toHaveBeenCalledWith('bronze', 'market', {
        refresh: true
      });
    });
    await waitFor(() => {
      expect(screen.getAllByText('0 symbols').length).toBeGreaterThan(0);
    });
  });

  it('omits empty medallion layer columns', async () => {
    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayersWithEmptyPlatinum()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    expect(await screen.findAllByText('Bronze')).not.toHaveLength(0);
    expect(screen.queryByText('Platinum')).not.toBeInTheDocument();
  });

  it('triggers all configured jobs for a layer from the medallion header', async () => {
    const user = userEvent.setup();

    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayerTriggerLayers()}
        recentJobs={makeJobs()}
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
      />
    );

    const layerTriggerButton = await screen.findByRole('button', { name: 'Trigger Bronze layer jobs' });
    await waitFor(() => {
      expect(layerTriggerButton).toBeEnabled();
    });
    await user.click(layerTriggerButton);

    await waitFor(() => {
      expect(triggerJobMock).toHaveBeenCalledTimes(2);
    });
    expect(triggerJobMock).toHaveBeenNthCalledWith(1, 'aca-job-market-bronze');
    expect(triggerJobMock).toHaveBeenNthCalledWith(2, 'aca-job-earnings-bronze');
  });
});
