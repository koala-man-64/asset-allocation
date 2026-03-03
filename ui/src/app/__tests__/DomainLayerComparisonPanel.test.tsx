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

  it('refreshes both status and metadata from the row actions menu', async () => {
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

    const moreButton = await screen.findByRole('button', { name: 'More actions for market' });
    await user.click(moreButton);

    const refreshMenuItem = await screen.findByRole('menuitem', {
      name: 'Refresh domain status + counts'
    });
    await user.click(refreshMenuItem);

    await waitFor(() => {
      expect(onRefresh).toHaveBeenCalledTimes(1);
    });
    await waitFor(() => {
      expect(DataService.getDomainMetadata).toHaveBeenCalledWith('bronze', 'market', {
        refresh: true
      });
    });
  });
});
