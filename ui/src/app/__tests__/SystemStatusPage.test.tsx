import { afterEach, describe, it, expect, vi } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter } from 'react-router-dom';

import { renderWithProviders } from '@/test/utils';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { getDomainOrderEntries } from '@/app/components/pages/system-status/domainOrdering';
import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';

const { MOCK_RUN_TIMESTAMPS, domainLayerCoverageSpy, jobLogStreamSpy } = vi.hoisted(() => ({
  MOCK_RUN_TIMESTAMPS: {
    latest: '2026-03-11T12:00:00.000Z',
    older: '2026-03-10T12:00:00.000Z'
  },
  domainLayerCoverageSpy: vi.fn(),
  jobLogStreamSpy: vi.fn()
}));

vi.mock('@/hooks/useDataQueries', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/hooks/useDataQueries')>();
  const now = MOCK_RUN_TIMESTAMPS.latest;

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
            jobName: 'aca-job-market',
            jobType: 'data-ingest',
            status: 'failed',
            startTime: MOCK_RUN_TIMESTAMPS.older,
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
        resources: [
          {
            name: 'aca-job-market',
            resourceType: 'Microsoft.App/jobs',
            status: 'healthy',
            lastChecked: now,
            runningState: 'Running',
            lastModifiedAt: now
          },
          {
            name: 'aca-job-zeta',
            resourceType: 'Microsoft.App/jobs',
            status: 'warning',
            lastChecked: now,
            runningState: 'Suspended',
            lastModifiedAt: now
          }
        ]
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

vi.mock('@/app/components/pages/system-status/DomainLayerComparisonPanel', () => ({
  DomainLayerComparisonPanel: (props: unknown) => {
    domainLayerCoverageSpy(props);
    return (
      <div data-testid="mock-domain-layer-coverage-panel">Mock Domain Layer Coverage Panel</div>
    );
  }
}));

vi.mock('@/app/components/pages/system-status/ContainerAppsPanel', () => ({
  ContainerAppsPanel: () => (
    <div data-testid="mock-container-apps-panel">Mock Container Apps Panel</div>
  )
}));

vi.mock('@/app/components/pages/system-status/JobLogStreamPanel', () => ({
  JobLogStreamPanel: (props: unknown) => {
    jobLogStreamSpy(props);
    return <div data-testid="mock-job-log-stream-panel">Mock Job Log Stream Panel</div>;
  }
}));

describe('SystemStatusPage', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    domainLayerCoverageSpy.mockClear();
    jobLogStreamSpy.mockClear();
  });

  const createQueryClient = () =>
    new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
          gcTime: 0,
          staleTime: 0
        }
      }
    });

  const expectNoPlatinumDomain = (layers: Array<{ domains?: Array<{ name?: string }> }>) => {
    for (const layer of layers) {
      for (const domain of layer.domains || []) {
        expect(
          String(domain.name || '')
            .trim()
            .toLowerCase()
        ).not.toBe('platinum');
      }
    }
  };

  it('renders the page layout and lazy loaded components', async () => {
    renderWithProviders(<SystemStatusPage />);

    // Check for Main Page Elements that are NOT lazy loaded
    expect(screen.getByText('LINK ESTABLISHED')).toBeInTheDocument();

    // Depending on test runner timing, lazy modules can remain in Suspense fallback.
    const lazyComponentsLoaded =
      Boolean(screen.queryByTestId('mock-domain-layer-coverage-panel')) &&
      Boolean(screen.queryByTestId('mock-container-apps-panel')) &&
      Boolean(screen.queryByTestId('mock-job-log-stream-panel'));
    const suspenseFallbackVisible = document.querySelectorAll('[data-slot="skeleton"]').length >= 3;
    expect(lazyComponentsLoaded || suspenseFallbackVisible).toBe(true);

    // Check loading state of footer
    expect(screen.getByText('LINK ESTABLISHED')).toBeInTheDocument();
  });

  it('keeps platinum as a layer but removes it as a data domain', async () => {
    domainLayerCoverageSpy.mockClear();

    renderWithProviders(<SystemStatusPage />);
    await screen.findByTestId('mock-domain-layer-coverage-panel');

    await waitFor(() => {
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
    });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };

    expect(coverageProps.dataLayers.some((layer) => layer.name.toLowerCase() === 'platinum')).toBe(
      true
    );
    expectNoPlatinumDomain(coverageProps.dataLayers);

    const coveragePanelProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      managedContainerJobs: Array<{ name: string; lastModifiedAt?: string | null }>;
    };
    expect(coveragePanelProps.managedContainerJobs.map((job) => job.name)).toEqual([
      'aca-job-market',
      'aca-job-zeta'
    ]);
    expect(
      coveragePanelProps.managedContainerJobs.every((job) => Boolean(job.lastModifiedAt))
    ).toBe(true);
  });

  it('uses canonical domain ordering in domain layer coverage panel', async () => {
    domainLayerCoverageSpy.mockClear();

    renderWithProviders(<SystemStatusPage />);
    await waitFor(() => {
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
    });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: Array<{ name: string; domains?: Array<{ name?: string }> }>;
    };

    const coverageOrder = getDomainOrderEntries(coverageProps.dataLayers).map((entry) => entry.key);

    expect(coverageOrder).toEqual(['alpha', 'market', 'zeta']);
  });

  it('passes the latest job run status and start time to the job console stream panel', async () => {
    jobLogStreamSpy.mockClear();

    renderWithProviders(<SystemStatusPage />);
    await waitFor(() => {
      expect(jobLogStreamSpy).toHaveBeenCalled();
    });

    const jobStreamProps = jobLogStreamSpy.mock.calls.at(-1)?.[0] as {
      jobs: Array<{
        name: string;
        runningState?: string | null;
        recentStatus?: string | null;
        startTime?: string | null;
      }>;
    };

    expect(jobStreamProps.jobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'aca-job-market',
          runningState: 'Running',
          recentStatus: 'success',
          startTime: MOCK_RUN_TIMESTAMPS.latest
        })
      ])
    );
  });

  it('merges optimistic running overrides into the system status props', async () => {
    domainLayerCoverageSpy.mockClear();

    const now = new Date().toISOString();
    const queryClient = createQueryClient();
    queryClient.setQueryData(queryKeys.systemHealthJobOverrides(), {
      'aca-job-zeta': {
        jobName: 'aca-job-zeta',
        jobKey: 'aca-job-zeta',
        status: 'running',
        runningState: 'Running',
        startTime: now,
        triggeredBy: 'manual',
        executionId: 'exec-zeta',
        executionName: 'exec-zeta',
        expiresAt: new Date(Date.now() + 60_000).toISOString()
      }
    });

    render(
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <SystemStatusPage />
        </BrowserRouter>
      </QueryClientProvider>
    );

    await waitFor(() => {
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
    });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      jobStates: Record<string, string>;
      recentJobs: Array<{ jobName: string; status: string; triggeredBy?: string }>;
      managedContainerJobs: Array<{ name: string; runningState?: string | null }>;
    };

    expect(coverageProps.jobStates['aca-job-zeta']).toBe('Running');
    expect(coverageProps.recentJobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          jobName: 'aca-job-zeta',
          status: 'running',
          triggeredBy: 'manual'
        })
      ])
    );
    expect(coverageProps.managedContainerJobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'aca-job-zeta',
          runningState: 'Running'
        })
      ])
    );
  });

  it('polls medallion-domain job status every 10 seconds without touching domain metadata queries', async () => {
    vi.useFakeTimers();
    domainLayerCoverageSpy.mockClear();

    const now = new Date().toISOString();
    const getSystemHealthSpy = vi.spyOn(DataService, 'getSystemHealth').mockResolvedValue({
      overall: 'warning',
      dataLayers: [],
      recentJobs: [
        {
          jobName: 'aca-job-zeta',
          jobType: 'data-ingest',
          status: 'running',
          startTime: now,
          triggeredBy: 'azure'
        }
      ],
      alerts: [],
      resources: [
        {
          name: 'aca-job-zeta',
          resourceType: 'Microsoft.App/jobs',
          status: 'warning',
          lastChecked: now,
          runningState: 'Running',
          lastModifiedAt: now
        }
      ]
    });

    renderWithProviders(<SystemStatusPage />);
    await vi.advanceTimersByTimeAsync(1);

    expect(domainLayerCoverageSpy).toHaveBeenCalled();

    expect(getSystemHealthSpy).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });

    expect(getSystemHealthSpy).toHaveBeenCalledWith({ refresh: true });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      overall: string;
      jobStates: Record<string, string>;
      recentJobs: Array<{ jobName: string; status: string }>;
      managedContainerJobs: Array<{ name: string; runningState?: string | null }>;
    };

    expect(coverageProps.overall).toBe('warning');
    expect(coverageProps.jobStates['aca-job-zeta']).toBe('Running');
    expect(coverageProps.recentJobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          jobName: 'aca-job-zeta',
          status: 'running'
        })
      ])
    );
    expect(coverageProps.managedContainerJobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'aca-job-zeta',
          runningState: 'Running'
        })
      ])
    );
  });
});
