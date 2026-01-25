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
                portalLinkToken: 'token-domain-portal',
                jobLinkToken: 'token-domain-job',
                jobName: 'aca-job-market',
                frequency: 'Daily',
                cron: '0 0 * * *',
              },
            ],
            portalLinkToken: 'token-layer-portal',
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

    // Check for Scheduled Jobs panel
    expect(screen.getByText('Scheduled Jobs')).toBeInTheDocument();

    // Check for Layer Name in Matrix / Panels
    expect(screen.getAllByText('Bronze').length).toBeGreaterThan(0);

    // Check for Domain Name in Matrix
    expect(screen.getAllByText('market').length).toBeGreaterThan(0);

    // Check for Status Badge (Uppercase)
    expect(screen.getByText('HEALTHY')).toHaveClass('font-mono');
  });
});
