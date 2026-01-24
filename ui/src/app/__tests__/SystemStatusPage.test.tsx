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
                jobUrl: 'https://portal.azure.com/#resource/sub-id/resourceGroups/rg-name/providers/Microsoft.App/jobs/aca-job-market/overview',
                frequency: 'Daily',
                cron: '0 0 * * *',
              },
            ],
            portalUrl: 'https://example.com/storage/bronze',
          },
        ],
        recentJobs: [
          {
            jobName: 'aca-job-market',
            jobType: 'data-ingest',
            status: 'success',
            startTime: now,
            triggeredBy: 'azure',
          },
        ],
        alerts: [],
        resources: [],
      },
      isLoading: false,
      error: null,
    }),
  };
});

describe('SystemStatusPage', () => {
  it('renders domain folder + job details in Data Layer Freshness', () => {
    renderWithProviders(<SystemStatusPage />);

    expect(screen.getByRole('link', { name: 'bronze/market' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'aca-job-market' })).toBeInTheDocument();
  });
});

