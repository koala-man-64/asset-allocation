import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import { DataService } from '@/services/DataService';
import {
  emitConsoleLogStream,
  REALTIME_SUBSCRIBE_EVENT,
  REALTIME_UNSUBSCRIBE_EVENT,
} from '@/services/realtimeBus';
import {
  JobLogStreamPanel,
  type JobLogStreamTarget,
} from '@/app/components/pages/system-status/JobLogStreamPanel';

vi.mock('@/services/DataService', () => ({
  DataService: {
    getJobLogs: vi.fn(),
  },
}));

const JOBS: JobLogStreamTarget[] = [
  {
    name: 'alpha-job',
    label: 'Bronze / market / alpha-job',
    layerName: 'Bronze',
    domainName: 'market',
    recentStatus: 'success',
    startTime: '2026-03-10T12:00:00Z',
  },
  {
    name: 'beta-job',
    label: 'Silver / finance / beta-job',
    layerName: 'Silver',
    domainName: 'finance',
    runningState: 'Running',
    recentStatus: 'running',
    startTime: '2026-03-11T12:00:00Z',
  },
];

describe('JobLogStreamPanel', () => {
  beforeEach(() => {
    vi.mocked(DataService.getJobLogs).mockReset();
    if (!Element.prototype.hasPointerCapture) {
      Object.defineProperty(Element.prototype, 'hasPointerCapture', {
        configurable: true,
        value: () => false,
      });
    }
    if (!Element.prototype.setPointerCapture) {
      Object.defineProperty(Element.prototype, 'setPointerCapture', {
        configurable: true,
        value: () => {},
      });
    }
    if (!Element.prototype.releasePointerCapture) {
      Object.defineProperty(Element.prototype, 'releasePointerCapture', {
        configurable: true,
        value: () => {},
      });
    }
    if (!Element.prototype.scrollIntoView) {
      Object.defineProperty(Element.prototype, 'scrollIntoView', {
        configurable: true,
        value: () => {},
      });
    }
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('streams one selected job at a time and switches topics when the dropdown changes', async () => {
    const subscribeTopics: string[][] = [];
    const unsubscribeTopics: string[][] = [];
    const captureSubscribe = (event: Event) => {
      subscribeTopics.push(((event as CustomEvent<{ topics: string[] }>).detail?.topics || []).slice());
    };
    const captureUnsubscribe = (event: Event) => {
      unsubscribeTopics.push(((event as CustomEvent<{ topics: string[] }>).detail?.topics || []).slice());
    };
    window.addEventListener(REALTIME_SUBSCRIBE_EVENT, captureSubscribe);
    window.addEventListener(REALTIME_UNSUBSCRIBE_EVENT, captureUnsubscribe);

    vi.mocked(DataService.getJobLogs)
      .mockResolvedValueOnce({
        jobName: 'beta-job',
        runsRequested: 1,
        runsReturned: 1,
        tailLines: 10,
        runs: [
          {
            executionName: 'beta-exec-001',
            startTime: '2026-03-11T12:00:00Z',
            tail: ['beta snapshot'],
            consoleLogs: [
              {
                timestamp: '2026-03-11T12:00:01Z',
                stream_s: 'stdout',
                executionName: 'beta-exec-001',
                message: 'beta snapshot',
              },
            ],
          },
        ],
      })
      .mockResolvedValueOnce({
        jobName: 'alpha-job',
        runsRequested: 1,
        runsReturned: 1,
        tailLines: 10,
        runs: [
          {
            executionName: 'alpha-exec-001',
            startTime: '2026-03-10T12:00:00Z',
            tail: ['alpha snapshot'],
            consoleLogs: [
              {
                timestamp: '2026-03-10T12:00:01Z',
                stream_s: 'stdout',
                executionName: 'alpha-exec-001',
                message: 'alpha snapshot',
              },
            ],
          },
        ],
      });

    const user = userEvent.setup();
    renderWithProviders(<JobLogStreamPanel jobs={JOBS} />);

    await waitFor(() => {
      expect(DataService.getJobLogs).toHaveBeenCalledWith(
        'beta-job',
        { runs: 1 },
        expect.any(AbortSignal)
      );
    });

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'timestamp' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'stream_s' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'message' })).toBeInTheDocument();
    expect(screen.getByText('stdout')).toBeInTheDocument();
    expect(subscribeTopics).toEqual(expect.arrayContaining([['job-logs:beta-job']]));

    await user.click(screen.getByRole('combobox', { name: /monitored job/i }));
    expect((await screen.findAllByRole('option')).map((option) => option.textContent)).toEqual([
      'Silver / finance / beta-job',
      'Bronze / market / alpha-job',
    ]);
    await user.click(await screen.findByRole('option', { name: 'Bronze / market / alpha-job' }));

    await waitFor(() => {
      expect(DataService.getJobLogs).toHaveBeenLastCalledWith(
        'alpha-job',
        { runs: 1 },
        expect.any(AbortSignal)
      );
    });

    expect(unsubscribeTopics).toEqual(expect.arrayContaining([['job-logs:beta-job']]));
    expect(subscribeTopics).toEqual(expect.arrayContaining([['job-logs:alpha-job']]));
    expect(await screen.findByText('alpha snapshot')).toBeInTheDocument();

    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:alpha-job',
        resourceType: 'job',
        resourceName: 'alpha-job',
        lines: [
          {
            id: 'line-1',
            message: 'alpha live line',
            timestamp: '2026-03-10T12:00:02Z',
            stream_s: 'stderr',
          },
        ],
      });
    });

    await waitFor(() => {
      expect(screen.getByText('alpha live line')).toBeInTheDocument();
    });
    expect(screen.getByText('stderr')).toBeInTheDocument();

    window.removeEventListener(REALTIME_SUBSCRIBE_EVENT, captureSubscribe);
    window.removeEventListener(REALTIME_UNSUBSCRIBE_EVENT, captureUnsubscribe);
  });

  it('shows the latest run status before falling back to container running state', async () => {
    const job: JobLogStreamTarget = {
      ...JOBS[1],
      recentStatus: 'success',
    };

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot'],
        },
      ],
    });

    renderWithProviders(<JobLogStreamPanel jobs={[job]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByText('SUCCESS')).toBeInTheDocument();
    expect(screen.queryByText('RUNNING')).not.toBeInTheDocument();
  });

  it('keeps streaming without refetching when job metadata refreshes for the same run', async () => {
    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          executionName: 'beta-exec-001',
          startTime: '2026-03-11T12:00:00Z',
          tail: ['beta snapshot'],
          consoleLogs: [
            {
              timestamp: '2026-03-11T12:00:01Z',
              stream_s: 'stdout',
              executionName: 'beta-exec-001',
              message: 'beta snapshot',
            },
          ],
        },
      ],
    });

    const view = renderWithProviders(<JobLogStreamPanel jobs={[JOBS[1]]} />);

    await waitFor(() => {
      expect(DataService.getJobLogs).toHaveBeenCalledTimes(1);
    });
    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();

    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:beta-job',
        resourceType: 'job',
        resourceName: 'beta-job',
        lines: [
          {
            id: 'line-live-1',
            message: 'beta live line',
            timestamp: '2026-03-11T12:00:02Z',
            stream_s: 'stdout',
          },
        ],
      });
    });

    expect(await screen.findByText('beta live line')).toBeInTheDocument();

    view.rerender(
      <JobLogStreamPanel
        jobs={[
          {
            ...JOBS[1],
            recentStatus: 'success',
            runningState: 'Succeeded',
            startTime: '2026-03-11T12:00:00Z',
          },
        ]}
      />
    );

    await waitFor(() => {
      expect(screen.getByText('beta live line')).toBeInTheDocument();
    });
    expect(DataService.getJobLogs).toHaveBeenCalledTimes(1);
  });
});
