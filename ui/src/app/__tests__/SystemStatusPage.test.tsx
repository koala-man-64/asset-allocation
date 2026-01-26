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
                jobUrl: 'https://portal.azure.com/#@/resource/sub-id/resourceGroups/rg-name/providers/Microsoft.App/jobs/aca-job-market/overview',
                jobName: 'aca-job-market',
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
    useLineageQuery: () => ({
      data: { impactsByDomain: {} },
      isLoading: false,
      error: null,
    }),
    useSignalsQuery: () => ({
      data: [],
      isLoading: false,
      error: null,
    }),
  };
});

describe('SystemStatusPage', () => {
  it('renders Industrial Dashboard elements', () => {
    renderWithProviders(<SystemStatusPage />);

    // Check for Hero Header
    expect(screen.getByText('SYSTEM STATUS')).toBeInTheDocument();

    // Check for Uptime Clock Header
    expect(screen.getByText('UPTIME CLOCK')).toBeInTheDocument();

    // Check for Matrix Table Headers
    expect(screen.getByText('DOMAIN')).toBeInTheDocument();
    expect(screen.getByText('STATUS')).toBeInTheDocument();
    expect(screen.getByText('LINKS')).toBeInTheDocument();

    // Check for Layer Name in Matrix
    expect(screen.getByText('Bronze')).toBeInTheDocument();

    // Check for Domain Name in Matrix
    expect(screen.getByText('market')).toBeInTheDocument();

    // Check for Status Badge (Uppercase)
    expect(screen.getByText('HEALTHY')).toHaveClass('font-mono');
  });
});
