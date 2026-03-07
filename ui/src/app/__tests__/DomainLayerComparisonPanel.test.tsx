import { beforeEach, describe, expect, it, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { DomainLayerComparisonPanel } from '@/app/components/pages/system-status/DomainLayerComparisonPanel';
import { DataService } from '@/services/DataService';
import { renderWithProviders } from '@/test/utils';
import type { DataLayer, JobRun } from '@/types/strategy';

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
    setJobSuspended: vi.fn().mockResolvedValue(undefined)
  })
}));

vi.mock('@/hooks/useJobTrigger', () => ({
  useJobTrigger: () => ({
    triggeringJob: null,
    triggerJob: vi.fn().mockResolvedValue(undefined)
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
    getPersistedDomainMetadataSnapshotCache: vi.fn(),
    getDomainMetadataSnapshot: vi.fn(),
    savePersistedDomainMetadataSnapshotCache: vi.fn(),
    getDomainMetadata: vi.fn(),
    invalidateSystemHealth: vi.fn()
  }
}));

const NOW = '2026-03-03T12:00:00Z';

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

function makeJobs(): JobRun[] {
  return [
    {
      jobName: 'aca-job-market',
      jobType: 'data-ingest',
      status: 'success',
      startTime: NOW,
      triggeredBy: 'test'
    }
  ];
}

describe('DomainLayerComparisonPanel refresh menu', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    vi.mocked(DataService.getPersistedDomainMetadataSnapshotCache).mockResolvedValue({
      version: 1,
      updatedAt: NOW,
      entries: {}
    });
    vi.mocked(DataService.getDomainMetadataSnapshot).mockResolvedValue({
      version: 1,
      updatedAt: NOW,
      entries: {}
    });
    vi.mocked(DataService.savePersistedDomainMetadataSnapshotCache).mockResolvedValue({
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
      symbolCount: 123,
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

    resolveMetadata?.({
      layer: 'bronze',
      domain: 'market',
      container: 'bronze',
      type: 'delta',
      computedAt: NOW,
      symbolCount: 123,
      warnings: []
    });

    await waitFor(() => {
      expect(screen.queryByTestId('domain-refresh-indicator-market')).not.toBeInTheDocument();
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
});
