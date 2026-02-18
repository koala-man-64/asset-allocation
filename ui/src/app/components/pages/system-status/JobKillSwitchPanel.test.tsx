import { describe, it, expect, beforeEach, vi } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '@/test/utils';
import { backtestApi } from '@/services/backtestApi';
import { toast } from 'sonner';
import { JobKillSwitchPanel } from './JobKillSwitchPanel';

vi.mock('@/services/backtestApi', () => ({
  backtestApi: {
    stopJob: vi.fn(),
    suspendJob: vi.fn(),
    resumeJob: vi.fn()
  }
}));

vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn()
  }
}));

describe('JobKillSwitchPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('stops currently running jobs then suspends all jobs when enabled', async () => {
    vi.mocked(backtestApi.stopJob).mockResolvedValue({
      jobName: 'job-running',
      action: 'stop',
      runningState: 'Stopped'
    });
    vi.mocked(backtestApi.suspendJob).mockResolvedValue({
      jobName: 'job-running',
      action: 'suspend',
      runningState: 'Suspended'
    });

    const user = userEvent.setup();
    renderWithProviders(
      <JobKillSwitchPanel
        jobs={[
          { name: 'job-running', runningState: 'Running' },
          { name: 'job-idle', runningState: 'Succeeded' }
        ]}
      />
    );

    await user.click(screen.getByRole('switch', { name: 'Toggle job kill switch' }));

    await waitFor(() => {
      expect(backtestApi.stopJob).toHaveBeenCalledTimes(1);
      expect(backtestApi.stopJob).toHaveBeenCalledWith('job-running');
    });

    await waitFor(() => {
      expect(backtestApi.suspendJob).toHaveBeenCalledTimes(2);
      expect(backtestApi.suspendJob).toHaveBeenNthCalledWith(1, 'job-running');
      expect(backtestApi.suspendJob).toHaveBeenNthCalledWith(2, 'job-idle');
    });

    const stopOrder = vi.mocked(backtestApi.stopJob).mock.invocationCallOrder[0];
    const suspendOrder = vi.mocked(backtestApi.suspendJob).mock.invocationCallOrder[0];
    expect(stopOrder).toBeLessThan(suspendOrder);
    expect(toast.success).toHaveBeenCalledWith(
      'Kill switch engaged. Stopped 1 running job(s) and suspended 2 job(s).'
    );
  });

  it('resumes all jobs when disabled', async () => {
    vi.mocked(backtestApi.resumeJob).mockResolvedValue({
      jobName: 'job-a',
      action: 'resume',
      runningState: 'Running'
    });

    const user = userEvent.setup();
    renderWithProviders(
      <JobKillSwitchPanel
        jobs={[
          { name: 'job-a', runningState: 'Suspended' },
          { name: 'job-b', runningState: 'Suspended' }
        ]}
      />
    );

    await user.click(screen.getByRole('switch', { name: 'Toggle job kill switch' }));

    await waitFor(() => {
      expect(backtestApi.resumeJob).toHaveBeenCalledTimes(2);
      expect(backtestApi.resumeJob).toHaveBeenNthCalledWith(1, 'job-a');
      expect(backtestApi.resumeJob).toHaveBeenNthCalledWith(2, 'job-b');
    });

    expect(toast.success).toHaveBeenCalledWith('Kill switch disengaged. Resumed 2 job(s).');
  });

  it('disables the switch when no jobs are available', () => {
    renderWithProviders(<JobKillSwitchPanel jobs={[]} />);
    expect(screen.getByRole('switch', { name: 'Toggle job kill switch' })).toBeDisabled();
  });
});
