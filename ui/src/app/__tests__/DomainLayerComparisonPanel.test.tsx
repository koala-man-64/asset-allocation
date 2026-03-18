import { beforeEach, describe, expect, it, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { ComponentProps } from 'react';

import { DomainLayerComparisonPanel } from '@/app/components/pages/system-status/DomainLayerComparisonPanel';
import { DataService } from '@/services/DataService';
import { renderWithProviders } from '@/test/utils';
import type { DataLayer, DomainMetadata, JobRun } from '@/types/strategy';

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
    getDomainMetadata: vi.fn(),
    invalidateSystemHealth: vi.fn()
  }
}));

const NOW = '2026-03-03T12:00:00Z';

function makeSnapshot(entry: {
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
  return {
    version: 1,
    updatedAt: NOW,
    entries: {
      [key]: entry
    },
    warnings: []
  };
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

  const renderPanel = (
    overrides: Partial<ComponentProps<typeof DomainLayerComparisonPanel>> = {}
  ) =>
    renderWithProviders(
      <DomainLayerComparisonPanel
        overall="healthy"
        dataLayers={makeLayers()}
        recentJobs={makeJobs()}
        metadataSnapshot={{
          version: 1,
          updatedAt: NOW,
          entries: {},
          warnings: []
        }}
        metadataUpdatedAt={NOW}
        metadataSource="persisted-snapshot"
        onRefresh={vi.fn().mockResolvedValue(undefined)}
        isRefreshing={false}
        isFetching={false}
        {...overrides}
      />
    );

  it('refreshes both status and metadata from the layer header action', async () => {
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();

    renderPanel({ onRefresh });

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
    expect((await screen.findAllByText(/updated Mar 3,?\s+06:00 CST/)).length).toBeGreaterThan(0);
  });

  it('shows the metadata timestamp when cached entries only have computedAt', async () => {
    renderPanel({
      metadataSnapshot: makeSnapshot({
        layer: 'bronze',
        domain: 'market',
        container: 'bronze',
        type: 'delta',
        computedAt: NOW,
        symbolCount: 123,
        warnings: []
      })
    });

    expect((await screen.findAllByText(/updated Mar 3,?\s+06:00 CST/)).length).toBeGreaterThan(0);
  });

  it('shows WARN for medallion-domain jobs with warning status codes', async () => {
    renderPanel({
      recentJobs: makeJobs('warning', 'SucceededWithWarnings')
    });

    expect(await screen.findAllByText('WARN')).not.toHaveLength(0);
    expect(screen.getAllByTitle('SucceededWithWarnings').length).toBeGreaterThan(0);
  });

  it('keeps the panel header focused on the title without status badges', async () => {
    renderPanel({
      overall: 'critical'
    });

    expect(await screen.findByText('Domain Layer Coverage')).toBeInTheDocument();
    expect(screen.queryByText('Release')).not.toBeInTheDocument();
    expect(screen.queryByText('System status')).not.toBeInTheDocument();
    expect(screen.queryByText(/Uptime clock/i)).not.toBeInTheDocument();
  });

  it('shows the column count in the medallion-domain coverage panel', async () => {
    renderPanel({
      metadataSnapshot: makeSnapshot({
        layer: 'bronze',
        domain: 'market',
        container: 'bronze',
        type: 'delta',
        computedAt: NOW,
        symbolCount: 123,
        columnCount: 9,
        warnings: []
      })
    });

    expect((await screen.findAllByText('9 cols')).length).toBeGreaterThan(0);
  });

  it('shows the storage size in the medallion-domain coverage panel', async () => {
    renderPanel({
      metadataSnapshot: makeSnapshot({
        layer: 'bronze',
        domain: 'market',
        container: 'bronze',
        type: 'delta',
        computedAt: NOW,
        symbolCount: 123,
        columnCount: 9,
        totalBytes: 2048,
        warnings: []
      })
    });

    expect((await screen.findAllByText('9 cols • 2.0 KB')).length).toBeGreaterThan(0);
  });

  it('uses an explicit disclosure button for inline domain details', async () => {
    const user = userEvent.setup();

    renderPanel();

    const disclosureButton = await screen.findByRole('button', { name: 'Expand market details' });

    expect(disclosureButton).toHaveAttribute('aria-expanded', 'false');
    expect(disclosureButton).toHaveAttribute('aria-controls');
    expect(screen.queryByText('date range:')).not.toBeInTheDocument();

    await user.click(disclosureButton);

    expect(disclosureButton).toHaveAttribute('aria-expanded', 'true');
    expect(disclosureButton).toHaveAttribute('aria-label', 'Collapse market details');
    expect(screen.getByText('date range:')).toBeInTheDocument();
  });

  it('shows the date range in medallion-domain metadata and detail subpanels', async () => {
    const user = userEvent.setup();

    renderPanel({
      metadataSnapshot: makeSnapshot({
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
      })
    });

    expect(await screen.findByText('range 2026-01-02 → 2026-03-03')).toBeInTheDocument();
    expect(screen.queryByText('date range:')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Expand market details' }));
    expect(screen.getByText('date range:')).toBeInTheDocument();
    expect(screen.getAllByTitle('column=Date • source=stats').length).toBeGreaterThan(0);
  });

  it('omits the timestamp line when metadata has no computedAt', async () => {
    renderPanel({
      metadataSnapshot: makeSnapshot({
        layer: 'bronze' as const,
        domain: 'market',
        container: 'bronze',
        type: 'delta' as const,
        computedAt: '',
        symbolCount: 123,
        warnings: []
      })
    });

    expect((await screen.findAllByText('market')).length).toBeGreaterThan(0);
    expect(screen.queryByText(/^updated /i)).not.toBeInTheDocument();
  });

  it('shows a row-level refreshing indicator in the medallion-domain view during refresh', async () => {
    const onRefresh = vi.fn().mockResolvedValue(undefined);
    const user = userEvent.setup();
    let resolveMetadata: ((value: DomainMetadata) => void) | null = null;
    const metadataPromise = new Promise<DomainMetadata>((resolve) => {
      resolveMetadata = resolve;
    });
    vi.mocked(DataService.getDomainMetadata).mockReturnValue(metadataPromise);

    renderPanel({ onRefresh });

    const refreshLayerButton = await screen.findByRole('button', { name: 'Refresh Bronze layer' });
    await user.click(refreshLayerButton);

    expect(await screen.findByTestId('domain-refresh-indicator-market')).toBeInTheDocument();
    expect(screen.getByTestId('cell-refresh-icon-summary-market-bronze')).toBeInTheDocument();
    expect(screen.queryByTestId('cell-refresh-icon-detail-market-bronze')).not.toBeInTheDocument();

    if (resolveMetadata) {
      resolveMetadata({
        layer: 'bronze',
        domain: 'market',
        container: 'bronze',
        type: 'delta',
        computedAt: NOW,
        metadataSource: 'artifact',
        symbolCount: 123,
        warnings: []
      });
    }

    await waitFor(() => {
      expect(screen.queryByTestId('domain-refresh-indicator-market')).not.toBeInTheDocument();
      expect(
        screen.queryByTestId('cell-refresh-icon-summary-market-bronze')
      ).not.toBeInTheDocument();
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

    renderPanel({ onRefresh });

    const refreshButton = await screen.findByRole('button', {
      name: 'Refresh domain layer coverage'
    });
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
    renderPanel({
      dataLayers: makeLayersWithEmptyPlatinum()
    });

    expect(await screen.findAllByText('Bronze')).not.toHaveLength(0);
    expect(screen.queryByText('Platinum')).not.toBeInTheDocument();
  });

  it('triggers all configured jobs for a layer from the medallion header', async () => {
    const user = userEvent.setup();

    renderPanel({
      dataLayers: makeLayerTriggerLayers()
    });

    const layerTriggerButton = await screen.findByRole('button', {
      name: 'Trigger Bronze layer jobs'
    });
    await waitFor(() => {
      expect(layerTriggerButton).toBeEnabled();
    });
    await user.click(layerTriggerButton);

    await waitFor(() => {
      expect(triggerJobMock).toHaveBeenCalledTimes(2);
    });
    expect(triggerJobMock).toHaveBeenNthCalledWith(1, 'aca-job-market-bronze', [
      ['systemStatusView'],
      ['systemHealth']
    ]);
    expect(triggerJobMock).toHaveBeenNthCalledWith(2, 'aca-job-earnings-bronze', [
      ['systemStatusView'],
      ['systemHealth']
    ]);
  });
});
