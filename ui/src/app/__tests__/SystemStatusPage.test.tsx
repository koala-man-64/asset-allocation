import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter } from 'react-router-dom';

import { renderWithProviders } from '@/test/utils';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { getDomainOrderEntries } from '@/app/components/pages/system-status/domainOrdering';
import { queryKeys } from '@/hooks/useDataQueries';
import { upsertRunningJobOverride } from '@/hooks/useSystemHealthJobOverrides';
import { DataService } from '@/services/DataService';
import type { SystemStatusViewResponse } from '@/services/apiService';
import type { DataLayer } from '@/types/strategy';

const { MOCK_RUN_TIMESTAMPS, domainLayerCoverageSpy, jobLogStreamSpy } = vi.hoisted(() => ({
  MOCK_RUN_TIMESTAMPS: {
    latest: '2026-03-11T12:00:00.000Z',
    older: '2026-03-10T12:00:00.000Z'
  },
  domainLayerCoverageSpy: vi.fn(),
  jobLogStreamSpy: vi.fn()
}));

vi.mock('@/services/DataService', () => ({
  DataService: {
    getSystemStatusView: vi.fn()
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

function buildSystemStatusView(
  overrides: Partial<SystemStatusViewResponse> = {}
): SystemStatusViewResponse {
  const generatedAt = new Date().toISOString();
  return {
    version: 1,
    generatedAt,
    sources: {
      systemHealth: 'live-refresh',
      metadataSnapshot: 'persisted-snapshot'
    },
    metadataSnapshot: {
      version: 1,
      updatedAt: generatedAt,
      entries: {
        'bronze/market': {
          layer: 'bronze',
          domain: 'market',
          container: 'bronze',
          type: 'delta',
          computedAt: generatedAt,
          cacheSource: 'snapshot',
          symbolCount: 321,
          columnCount: 9,
          warnings: []
        }
      },
      warnings: []
    },
    systemHealth: {
      overall: 'healthy',
      dataLayers: [
        {
          name: 'Bronze',
          description: 'Raw ingestion layer',
          status: 'healthy',
          lastUpdated: MOCK_RUN_TIMESTAMPS.latest,
          refreshFrequency: 'Daily',
          domains: [
            {
              name: 'zeta',
              description: 'Market data',
              type: 'blob',
              path: 'bronze/zeta',
              lastUpdated: MOCK_RUN_TIMESTAMPS.latest,
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
              lastUpdated: MOCK_RUN_TIMESTAMPS.latest,
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
              lastUpdated: MOCK_RUN_TIMESTAMPS.latest,
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
          lastUpdated: MOCK_RUN_TIMESTAMPS.latest,
          refreshFrequency: 'Daily',
          domains: [
            {
              name: 'platinum',
              description: 'Reserved',
              type: 'blob',
              path: 'platinum',
              lastUpdated: MOCK_RUN_TIMESTAMPS.latest,
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
          startTime: MOCK_RUN_TIMESTAMPS.latest,
          triggeredBy: 'azure'
        },
        {
          jobName: 'aca-job-market',
          jobType: 'data-ingest',
          status: 'running',
          startTime: MOCK_RUN_TIMESTAMPS.older,
          triggeredBy: 'azure'
        },
        {
          jobName: 'aca-job-platinum',
          jobType: 'data-ingest',
          status: 'success',
          startTime: MOCK_RUN_TIMESTAMPS.latest,
          triggeredBy: 'azure'
        }
      ],
      alerts: [],
      resources: [
        {
          name: 'aca-job-market',
          resourceType: 'Microsoft.App/jobs',
          status: 'healthy',
          lastChecked: MOCK_RUN_TIMESTAMPS.latest,
          runningState: 'Running',
          lastModifiedAt: MOCK_RUN_TIMESTAMPS.latest,
          signals: [
            {
              name: 'CpuUsage',
              value: 52.3,
              unit: 'Percent',
              timestamp: MOCK_RUN_TIMESTAMPS.latest,
              status: 'healthy',
              source: 'metrics'
            },
            {
              name: 'MemoryWorkingSetBytes',
              value: 805306368,
              unit: 'Bytes',
              timestamp: MOCK_RUN_TIMESTAMPS.latest,
              status: 'healthy',
              source: 'metrics'
            }
          ]
        },
        {
          name: 'aca-job-zeta',
          resourceType: 'Microsoft.App/jobs',
          status: 'warning',
          lastChecked: MOCK_RUN_TIMESTAMPS.latest,
          runningState: 'Suspended',
          lastModifiedAt: MOCK_RUN_TIMESTAMPS.latest
        }
      ]
    },
    ...overrides
  };
}

describe('SystemStatusPage', () => {
  beforeEach(() => {
    window.sessionStorage.clear();
    vi.mocked(DataService.getSystemStatusView).mockResolvedValue(buildSystemStatusView());
  });

  afterEach(() => {
    window.sessionStorage.clear();
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

  const expectNoPlatinumDomain = (layers: DataLayer[]) => {
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

    await screen.findByTestId('mock-domain-layer-coverage-panel');

    expect(screen.getByText(/VIEW UPDATED/i)).toBeInTheDocument();
    expect(await screen.findByTestId('mock-container-apps-panel')).toBeInTheDocument();
    expect(await screen.findByTestId('mock-job-log-stream-panel')).toBeInTheDocument();
  });

  it('keeps platinum as a layer but removes it as a data domain', async () => {
    domainLayerCoverageSpy.mockClear();

    renderWithProviders(<SystemStatusPage />);
    await screen.findByTestId('mock-domain-layer-coverage-panel');

    await waitFor(() => {
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
    });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: DataLayer[];
      managedContainerJobs: Array<{
        name: string;
        lastModifiedAt?: string | null;
        signals?: Array<{
          name?: string | null;
          value?: number | null;
          unit?: string | null;
        }> | null;
      }>;
      metadataSource?: string | null;
    };

    expect(coverageProps.dataLayers.some((layer) => layer.name.toLowerCase() === 'platinum')).toBe(
      true
    );
    expectNoPlatinumDomain(coverageProps.dataLayers);
    expect(coverageProps.managedContainerJobs.map((job) => job.name)).toEqual([
      'aca-job-market',
      'aca-job-zeta'
    ]);
    expect(coverageProps.managedContainerJobs.every((job) => Boolean(job.lastModifiedAt))).toBe(
      true
    );
    expect(coverageProps.managedContainerJobs).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'aca-job-market',
          signals: expect.arrayContaining([
            expect.objectContaining({
              name: 'CpuUsage',
              value: 52.3,
              unit: 'Percent'
            })
          ])
        })
      ])
    );
    expect(coverageProps.metadataSource).toBe('persisted-snapshot');
  });

  it('uses canonical domain ordering in domain layer coverage panel', async () => {
    renderWithProviders(<SystemStatusPage />);

    await waitFor(() => {
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
    });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      dataLayers: DataLayer[];
    };

    const coverageOrder = getDomainOrderEntries(coverageProps.dataLayers).map((entry) => entry.key);

    expect(coverageOrder).toEqual(['alpha', 'market', 'zeta']);
  });

  it('passes the anchored active job run status and start time to the job console stream panel', async () => {
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
          recentStatus: 'running',
          startTime: MOCK_RUN_TIMESTAMPS.older,
          signals: expect.arrayContaining([
            expect.objectContaining({
              name: 'CpuUsage',
              value: 52.3,
              unit: 'Percent'
            }),
            expect.objectContaining({
              name: 'MemoryWorkingSetBytes',
              value: 805306368,
              unit: 'Bytes'
            })
          ])
        })
      ])
    );
  });

  it('merges optimistic running overrides into the system status props', async () => {
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

  it('restores optimistic running overrides after a hard reload before server telemetry catches up', async () => {
    const now = new Date().toISOString();
    const seededClient = createQueryClient();

    upsertRunningJobOverride(seededClient, {
      jobName: 'aca-job-zeta',
      startTime: now,
      triggeredBy: 'manual',
      response: {
        executionId: 'exec-zeta',
        executionName: 'exec-zeta'
      }
    });

    const payload = buildSystemStatusView();
    vi.mocked(DataService.getSystemStatusView).mockResolvedValue({
      ...payload,
      systemHealth: {
        ...payload.systemHealth,
        recentJobs: payload.systemHealth.recentJobs.filter((job) => job.jobName !== 'aca-job-zeta'),
        resources: (payload.systemHealth.resources || []).map((resource) =>
          resource.name === 'aca-job-zeta'
            ? {
                ...resource,
                runningState: undefined,
                lastModifiedAt: MOCK_RUN_TIMESTAMPS.older
              }
            : resource
        )
      }
    });

    const reloadedClient = createQueryClient();

    render(
      <QueryClientProvider client={reloadedClient}>
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

  it('forces a live refresh when the coverage panel refresh handler is invoked', async () => {
    const now = new Date().toISOString();

    vi.mocked(DataService.getSystemStatusView)
      .mockResolvedValueOnce(buildSystemStatusView())
      .mockResolvedValueOnce(
        buildSystemStatusView({
          systemHealth: {
            overall: 'degraded',
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
          }
        })
      );

    renderWithProviders(<SystemStatusPage />);

    await waitFor(() => {
      expect(domainLayerCoverageSpy).toHaveBeenCalled();
    });

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      onRefresh?: () => Promise<void>;
      overall: string;
    };

    expect(coverageProps.overall).toBe('healthy');

    await act(async () => {
      await coverageProps.onRefresh?.();
    });

    expect(vi.mocked(DataService.getSystemStatusView).mock.calls.at(-1)).toEqual([
      { refresh: true }
    ]);

    await waitFor(() => {
      const latestProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as { overall: string };
      expect(latestProps.overall).toBe('degraded');
    });
  });

  it('loads the unified status view from cache and forces a refresh every 10 seconds', async () => {
    vi.useFakeTimers();
    const now = new Date().toISOString();

    vi.mocked(DataService.getSystemStatusView)
      .mockResolvedValueOnce(buildSystemStatusView())
      .mockResolvedValueOnce(
        buildSystemStatusView({
          systemHealth: {
            overall: 'degraded',
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
          }
        })
      );

    renderWithProviders(<SystemStatusPage />);

    await vi.dynamicImportSettled();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(domainLayerCoverageSpy).toHaveBeenCalled();
    expect(vi.mocked(DataService.getSystemStatusView).mock.calls[0]).toEqual([]);
    const initialCallCount = vi.mocked(DataService.getSystemStatusView).mock.calls.length;
    expect(initialCallCount).toBeGreaterThan(0);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
      await Promise.resolve();
    });

    expect(vi.mocked(DataService.getSystemStatusView).mock.calls.length).toBeGreaterThan(
      initialCallCount
    );
    expect(vi.mocked(DataService.getSystemStatusView).mock.calls.at(-1)).toEqual([
      { refresh: true }
    ]);

    const coverageProps = domainLayerCoverageSpy.mock.calls.at(-1)?.[0] as {
      overall: string;
      jobStates: Record<string, string>;
      recentJobs: Array<{ jobName: string; status: string }>;
      managedContainerJobs: Array<{ name: string; runningState?: string | null }>;
    };

    expect(coverageProps.overall).toBe('degraded');
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
