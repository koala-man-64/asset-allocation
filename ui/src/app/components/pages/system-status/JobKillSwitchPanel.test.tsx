import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
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
    vi.spyOn(window, 'confirm').mockReturnValue(true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('stops currently running jobs', async () => {
    vi.mocked(backtestApi.stopJob).mockResolvedValue({
      jobName: 'job-running',
      action: 'stop',
      runningState: 'Stopped'
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

    await user.click(screen.getByRole('button', { name: 'Stop running jobs' }));

    await waitFor(() => {
      expect(backtestApi.stopJob).toHaveBeenCalledTimes(1);
      expect(backtestApi.stopJob).toHaveBeenCalledWith('job-running');
    });

    expect(toast.success).toHaveBeenCalledWith('Stopped 1 running job(s).');
  });

  it('suspends all jobs', async () => {
    vi.mocked(backtestApi.suspendJob).mockResolvedValue({
      jobName: 'job-a',
      action: 'suspend',
      runningState: 'Suspended'
    });

    const user = userEvent.setup();
    renderWithProviders(
      <JobKillSwitchPanel
        jobs={[
          { name: 'job-a', runningState: 'Running' },
          { name: 'job-b', runningState: 'Succeeded' }
        ]}
      />
    );

    await user.click(screen.getByRole('button', { name: 'Suspend all jobs' }));

    await waitFor(() => {
      expect(backtestApi.suspendJob).toHaveBeenCalledTimes(2);
      expect(backtestApi.suspendJob).toHaveBeenNthCalledWith(1, 'job-a');
      expect(backtestApi.suspendJob).toHaveBeenNthCalledWith(2, 'job-b');
    });

    expect(toast.success).toHaveBeenCalledWith('Suspended 2 job(s).');
  });

  it('resumes all jobs', async () => {
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

    await user.click(screen.getByRole('button', { name: 'Resume all jobs' }));

    await waitFor(() => {
      expect(backtestApi.resumeJob).toHaveBeenCalledTimes(2);
      expect(backtestApi.resumeJob).toHaveBeenNthCalledWith(1, 'job-a');
      expect(backtestApi.resumeJob).toHaveBeenNthCalledWith(2, 'job-b');
    });

    expect(toast.success).toHaveBeenCalledWith('Resumed 2 job(s).');
  });

  it('disables action buttons when no jobs are available', () => {
    renderWithProviders(<JobKillSwitchPanel jobs={[]} />);
    expect(screen.getByRole('button', { name: 'Stop running jobs' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Suspend all jobs' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Resume all jobs' })).toBeDisabled();
  });
});
