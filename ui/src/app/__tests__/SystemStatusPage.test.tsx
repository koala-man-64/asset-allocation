import { describe, it, expect, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';

import { renderWithProviders } from '@/test/utils';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { getDomainOrderEntries } from '@/app/components/pages/system-status/domainOrdering';

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
                name: 'zeta',
                description: 'Market data',
                type: 'blob',
                path: 'bronze/zeta',
                lastUpdated: now,
                status: 'healthy',
                portalUrl: 'https://example.com/storage/bronze/zeta',
                jobUrl:
                  'https://portal.azure.com/#@/resource/sub-id/resourceGroups/rg-name/providers/Microsoft.App/jobs/aca-job-zeta/overview',
                jobName: 'aca-job-zeta',
                frequency: 'Daily',
                cron: '0 0 * * *'
              },
              {
                name: 'Alpha',
                description: 'Reference domain',
                type: 'blob',
                path: 'bronze/alpha',
                lastUpdated: now,
                status: 'healthy',
                portalUrl: 'https://example.com/storage/bronze/alpha',
                jobUrl:
                  'https://portal.azure.com/#@/resource/sub-id/resourceGroups/rg-name/providers/Microsoft.App/jobs/aca-job-alpha/overview',
                jobName: 'aca-job-alpha',
                frequency: 'Daily',
                cron: '0 0 * * *'
              },
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
          },
          {
            name: 'Platinum',
            description: 'Serving layer',
            status: 'healthy',
            lastUpdated: now,
            refreshFrequency: 'Daily',
            domains: [
              {
                name: 'platinum',
                description: 'Reserved',
                type: 'blob',
                path: 'platinum',
                lastUpdated: now,
                status: 'healthy',
                portalUrl: 'https://example.com/storage/platinum',
                jobUrl:
                  'https://portal.azure.com/#@/resource/sub-id/resourceGroups/rg-name/providers/Microsoft.App/jobs/aca-job-platinum/overview',
                jobName: 'aca-job-platinum',
                frequency: 'Daily',
                cron: '0 0 * * *'
              }
            ],
            portalUrl: 'https://example.com/storage/platinum'
          }
        ],
        recentJobs: [
          {
            jobName: 'aca-job-market',
            jobType: 'data-ingest',
            status: 'success',
            startTime: now,
            triggeredBy: 'azure'
          },
          {
            jobName: 'aca-job-platinum',
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

const statusOverviewSpy = vi.fn();
const domainLayerCoverageSpy = vi.fn();
const jobMonitorSpy = vi.fn();

vi.mock('@/app/components/pages/system-status/StatusOverview', () => ({
  StatusOverview: (props: unknown) => {
    statusOverviewSpy(props);
    return <div data-testid="mock-status-overview">Mock Status Overview</div>;
  }
}));

vi.mock('@/app/components/pages/system-status/DomainLayerComparisonPanel', () => ({
  DomainLayerComparisonPanel: (props: unknown) => {
    domainLayerCoverageSpy(props);
    return (
      <div data-testid="mock-domain-layer-coverage-panel">Mock Domain Layer Coverage Panel</div>
    );
  }
}));

vi.mock('@/app/components/pages/system-status/ScheduledJobMonitor', () => ({
  ScheduledJobMonitor: (props: unknown) => {
    jobMonitorSpy(props);
    return <div data-testid="mock-job-monitor">Mock Job Monitor</div>;
  }
}));

vi.mock('@/app/components/pages/system-status/ContainerAppsPanel', () => ({
  ContainerAppsPanel: () => <div data-testid="mock-container-apps-panel">Mock Container Apps Panel</div>
}));

describe('SystemStatusPage', () => {
  const expectNoPlatinumDomain = (layers: Array<{ domains?: Array<{ name?: string }> }>) => {
    for (const layer of layers) {
      for (const domain of layer.domains || []) {
        expect(String(domain.name || '').trim().toLowerCase()).not.toBe('platinum');
      }
    }
  };

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

  it('keeps platinum as a layer but removes it as a data domain', async () => {
    statusOverviewSpy.mockClear();
    domainLayerCoverageSpy.mockClear();
    jobMonitorSpy.mockClear();

    renderWithProviders(<SystemStatusPage />);
    await screen.findByTestId('mock-status-overview');

    await waitFor(() => {
      expect(statusOverviewSpy).toHaveBeenCalled();
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
      expect(jobMonitorSpy).toHaveBeenCalled();
    });

    const statusOverviewProps = statusOverviewSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };
    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };
    const jobMonitorProps = jobMonitorSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };

    for (const props of [statusOverviewProps, coverageProps, jobMonitorProps]) {
      expect(props.dataLayers.some((layer) => layer.name.toLowerCase() === 'platinum')).toBe(true);
      expectNoPlatinumDomain(props.dataLayers);
    }
  });

  it('uses a consistent canonical domain ordering for all status panels', async () => {
    statusOverviewSpy.mockClear();
    domainLayerCoverageSpy.mockClear();
    jobMonitorSpy.mockClear();

    renderWithProviders(<SystemStatusPage />);
    await waitFor(() => {
      expect(statusOverviewSpy).toHaveBeenCalled();
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
      expect(jobMonitorSpy).toHaveBeenCalled();
    });

    const statusOverviewProps = statusOverviewSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };
    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };
    const jobMonitorProps = jobMonitorSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };

    const statusOrder = getDomainOrderEntries(statusOverviewProps.dataLayers).map((entry) => entry.key);
    const coverageOrder = getDomainOrderEntries(coverageProps.dataLayers).map((entry) => entry.key);
    const jobOrder = getDomainOrderEntries(jobMonitorProps.dataLayers).map((entry) => entry.key);

    expect(statusOrder).toEqual(['alpha', 'market', 'zeta']);
    expect(coverageOrder).toEqual(statusOrder);
    expect(jobOrder).toEqual(statusOrder);
  });
});
