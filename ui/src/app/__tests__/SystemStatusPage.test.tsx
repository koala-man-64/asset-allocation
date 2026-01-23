import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import { AuthProvider } from '@/contexts/AuthContext';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { backtestApi } from '@/services/backtestApi';

vi.mock('@/hooks/useDataQueries', () => ({
  useLiveSystemHealthQuery: () => ({
    data: {
      overall: 'healthy',
      dataLayers: [],
      recentJobs: [
        {
          jobName: 'platinum-ranking-job',
          jobType: 'data-ingest',
          status: 'success',
          startTime: new Date().toISOString(),
          triggeredBy: 'azure',
        },
      ],
      alerts: [],
      resources: [],
    },
    isLoading: false,
    error: null,
  }),
}));

describe('SystemStatusPage', () => {
  it('triggers a job run when clicking the run icon', async () => {
    const user = userEvent.setup();

    const triggerJobSpy = vi
      .spyOn(backtestApi, 'triggerJob')
      .mockResolvedValue({ jobName: 'platinum-ranking-job', status: 'queued' });

    renderWithProviders(
      <AuthProvider>
        <SystemStatusPage />
      </AuthProvider>,
    );

    const button = screen.getByLabelText('Run platinum-ranking-job');
    await user.click(button);

    expect(triggerJobSpy).toHaveBeenCalledWith('platinum-ranking-job');
  });
});

