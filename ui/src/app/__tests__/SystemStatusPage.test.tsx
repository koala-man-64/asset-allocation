import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

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

vi.mock('@/app/components/pages/system-status/AzureResources', () => ({
  AzureResources: () => <div data-testid="mock-azure-resources">Mock Azure Resources</div>
}));

vi.mock('@/app/components/pages/system-status/ScheduledJobMonitor', () => ({
  ScheduledJobMonitor: () => <div data-testid="mock-job-monitor">Mock Job Monitor</div>
}));

describe('SystemStatusPage', () => {
  it('renders the page layout and lazy loaded components', async () => {
    renderWithProviders(<SystemStatusPage />);

    // Check for Main Page Elements that are NOT lazy loaded
    expect(screen.getByText('LINK ESTABLISHED')).toBeInTheDocument();

    // Check for Lazy Loaded Components (using findBy because of Suspense/Lazy)
    // Even with mocks, result of standard lazy() is async?
    // Actually with vi.mock it might be sync if the import is intercepted?
    // But SystemStatusPage uses `lazy(() => import(...))`.
    // vi.mock intercepts the import.
    // So `await import(...)` returns the mock immediately.
    // However, React.lazy still suspends until the promise resolves.

    expect(await screen.findByTestId('mock-status-overview')).toBeInTheDocument();
    expect(await screen.findByTestId('mock-job-monitor')).toBeInTheDocument();

    // Check loading state of footer
    expect(screen.getByText('LINK ESTABLISHED')).toBeInTheDocument();
  });
});
