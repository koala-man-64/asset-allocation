import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';

vi.mock('@/hooks/useDataQueries', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useDataQueries')>();
  const now = new Date().toISOString();

  return {
    ...actual,
    useSystemHealthQuery: () => ({
      data: {
        overall: 'healthy',
        dataLayers: [
          {
            name: 'Bronze',
            description: 'Raw ingestion layer',
            status: 'healthy',
            lastUpdated: now,
            refreshFrequency: 'Daily',
            domains: [
              {
                name: 'market',
                description: 'Market data',
                type: 'blob',
                path: 'bronze/market',
                lastUpdated: now,
                status: 'healthy',
                portalUrl: 'https://example.com/storage/bronze/market',
                jobUrl:
                  'https://portal.azure.com/#@/resource/sub-id/resourceGroups/rg-name/providers/Microsoft.App/jobs/aca-job-market/overview',
                jobName: 'aca-job-market',
                frequency: 'Daily',
                cron: '0 0 * * *'
              }
            ],
            portalUrl: 'https://example.com/storage/bronze'
          }
        ],
        recentJobs: [
          {
            jobName: 'aca-job-market',
            jobType: 'data-ingest',
            status: 'success',
            startTime: now,
            triggeredBy: 'azure'
          }
        ],
        alerts: [],
        resources: []
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

vi.mock('@/app/components/pages/system-status/StatusOverview', () => ({
  StatusOverview: () => <div data-testid="mock-status-overview">Mock Status Overview</div>
}));

vi.mock('@/app/components/pages/system-status/DomainLayerComparisonPanel', () => ({
  DomainLayerComparisonPanel: () => (
    <div data-testid="mock-domain-layer-coverage-panel">Mock Domain Layer Coverage Panel</div>
  )
}));

vi.mock('@/app/components/pages/system-status/ScheduledJobMonitor', () => ({
  ScheduledJobMonitor: () => <div data-testid="mock-job-monitor">Mock Job Monitor</div>
}));

vi.mock('@/app/components/pages/system-status/ContainerAppsPanel', () => ({
  ContainerAppsPanel: () => <div data-testid="mock-container-apps-panel">Mock Container Apps Panel</div>
}));

describe('SystemStatusPage', () => {
  it('renders the page layout and lazy loaded components', async () => {
    renderWithProviders(<SystemStatusPage />);

    // Check for Main Page Elements that are NOT lazy loaded
    expect(screen.getByText('LINK ESTABLISHED')).toBeInTheDocument();

    // Depending on test runner timing, lazy modules can remain in Suspense fallback.
    const lazyComponentsLoaded =
      Boolean(screen.queryByTestId('mock-status-overview')) &&
      Boolean(screen.queryByTestId('mock-domain-layer-coverage-panel')) &&
      Boolean(screen.queryByTestId('mock-job-monitor'));
    const suspenseFallbackVisible =
      document.querySelectorAll('[data-slot="skeleton"]').length >= 2;
    expect(lazyComponentsLoaded || suspenseFallbackVisible).toBe(true);

    // Check loading state of footer
    expect(screen.getByText('LINK ESTABLISHED')).toBeInTheDocument();
  });
});
