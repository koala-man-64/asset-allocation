import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import { DataService } from '@/services/DataService';
import {
  emitConsoleLogStream,
  REALTIME_SUBSCRIBE_EVENT,
  REALTIME_UNSUBSCRIBE_EVENT
} from '@/services/realtimeBus';
import {
  JobLogStreamPanel,
  type JobLogStreamTarget
} from '@/app/components/pages/system-status/JobLogStreamPanel';

vi.mock('@/services/DataService', () => ({
  DataService: {
    getJobLogs: vi.fn(),
    getSystemHealth: vi.fn()
  }
}));

const JOBS: JobLogStreamTarget[] = [
  {
    name: 'alpha-job',
    label: 'Bronze / market / alpha-job',
    layerName: 'Bronze',
    domainName: 'market',
    recentStatus: 'success',
    startTime: '2026-03-10T12:00:00Z'
  },
  {
    name: 'beta-job',
    label: 'Silver / finance / beta-job',
    layerName: 'Silver',
    domainName: 'finance',
    runningState: 'Running',
    recentStatus: 'running',
    startTime: '2026-03-11T12:00:00Z'
  }
];

describe('JobLogStreamPanel', () => {
  beforeEach(() => {
    vi.mocked(DataService.getJobLogs).mockReset();
    vi.mocked(DataService.getSystemHealth).mockReset();
    vi.mocked(DataService.getSystemHealth).mockResolvedValue({
      overall: 'healthy',
      dataLayers: [],
      recentJobs: [],
      alerts: [],
      resources: []
    });
    if (!Element.prototype.hasPointerCapture) {
      Object.defineProperty(Element.prototype, 'hasPointerCapture', {
        configurable: true,
        value: () => false
      });
    }
    if (!Element.prototype.setPointerCapture) {
      Object.defineProperty(Element.prototype, 'setPointerCapture', {
        configurable: true,
        value: () => {}
      });
    }
    if (!Element.prototype.releasePointerCapture) {
      Object.defineProperty(Element.prototype, 'releasePointerCapture', {
        configurable: true,
        value: () => {}
      });
    }
    if (!Element.prototype.scrollIntoView) {
      Object.defineProperty(Element.prototype, 'scrollIntoView', {
        configurable: true,
        value: () => {}
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
      subscribeTopics.push(
        ((event as CustomEvent<{ topics: string[] }>).detail?.topics || []).slice()
      );
    };
    const captureUnsubscribe = (event: Event) => {
      unsubscribeTopics.push(
        ((event as CustomEvent<{ topics: string[] }>).detail?.topics || []).slice()
      );
    };
    window.addEventListener(REALTIME_SUBSCRIBE_EVENT, captureSubscribe);
    window.addEventListener(REALTIME_UNSUBSCRIBE_EVENT, captureUnsubscribe);

    vi.mocked(DataService.getJobLogs)
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
                message: 'alpha snapshot'
              }
            ]
          }
        ]
      })
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
                message: 'beta snapshot'
              }
            ]
          }
        ]
      });

    const user = userEvent.setup();
    renderWithProviders(<JobLogStreamPanel jobs={JOBS} />);

    await waitFor(() => {
      expect(DataService.getJobLogs).toHaveBeenCalledWith(
        'alpha-job',
        { runs: 1 },
        expect.any(AbortSignal)
      );
    });

    expect(await screen.findByText('alpha snapshot')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'timestamp' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'stream_s' })).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'message' })).toBeInTheDocument();
    expect(screen.getByText('stdout')).toBeInTheDocument();
    expect(subscribeTopics).toEqual(
      expect.arrayContaining([['job-logs:alpha-job/executions/alpha-exec-001']])
    );

    await user.click(screen.getByRole('combobox', { name: /monitored job/i }));
    expect((await screen.findAllByRole('option')).map((option) => option.textContent)).toEqual([
      'Bronze / market / alpha-job',
      'Silver / finance / beta-job'
    ]);
    await user.click(await screen.findByRole('option', { name: 'Silver / finance / beta-job' }));

    await waitFor(() => {
      expect(DataService.getJobLogs).toHaveBeenLastCalledWith(
        'beta-job',
        { runs: 1 },
        expect.any(AbortSignal)
      );
    });

    expect(unsubscribeTopics).toEqual(
      expect.arrayContaining([['job-logs:alpha-job/executions/alpha-exec-001']])
    );
    expect(subscribeTopics).toEqual(
      expect.arrayContaining([['job-logs:beta-job/executions/beta-exec-001']])
    );
    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();

    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:beta-job/executions/beta-exec-001',
        resourceType: 'job',
        resourceName: 'beta-job',
        lines: [
          {
            id: 'line-1',
            message: 'beta live line',
            timestamp: '2026-03-11T12:00:02Z',
            stream_s: 'stderr'
          }
        ]
      });
    });

    await waitFor(() => {
      expect(screen.getByText('beta live line')).toBeInTheDocument();
    });
    expect(screen.getByText('stderr')).toBeInTheDocument();

    window.removeEventListener(REALTIME_SUBSCRIBE_EVENT, captureSubscribe);
    window.removeEventListener(REALTIME_UNSUBSCRIBE_EVENT, captureUnsubscribe);
  });

  it('prefers the live running state over the last completed run status', async () => {
    const job: JobLogStreamTarget = {
      ...JOBS[1],
      recentStatus: 'success'
    };

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot']
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[job]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByText('RUNNING')).toBeInTheDocument();
    expect(screen.queryByText('SUCCESS')).not.toBeInTheDocument();
  });

  it('anchors the console tail to an older active execution when one is still running', async () => {
    const subscribeTopics: string[][] = [];
    const captureSubscribe = (event: Event) => {
      subscribeTopics.push(
        ((event as CustomEvent<{ topics: string[] }>).detail?.topics || []).slice()
      );
    };
    window.addEventListener(REALTIME_SUBSCRIBE_EVENT, captureSubscribe);

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 2,
      runsReturned: 2,
      tailLines: 10,
      runs: [
        {
          executionName: 'beta-exec-002',
          status: 'Succeeded',
          startTime: '2026-03-11T12:00:00Z',
          tail: ['latest finished snapshot']
        },
        {
          executionName: 'beta-exec-001',
          status: 'Running',
          startTime: '2026-03-10T12:00:00Z',
          tail: ['older active snapshot']
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[JOBS[1]]} />);

    expect(await screen.findByText('older active snapshot')).toBeInTheDocument();
    expect(screen.queryByText('latest finished snapshot')).not.toBeInTheDocument();
    expect(subscribeTopics).toEqual(
      expect.arrayContaining([['job-logs:beta-job/executions/beta-exec-001']])
    );

    window.removeEventListener(REALTIME_SUBSCRIBE_EVENT, captureSubscribe);
  });

  it('shows cpu and memory usage when resource signals are available', async () => {
    const job: JobLogStreamTarget = {
      ...JOBS[1],
      signals: [
        {
          name: 'CpuUsage',
          value: 68.4,
          unit: 'Percent',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'warning',
          source: 'metrics'
        },
        {
          name: 'MemoryWorkingSetBytes',
          value: 1610612736,
          unit: 'Bytes',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        }
      ]
    };

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot']
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[job]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByText('CPU Usage')).toBeInTheDocument();
    expect(screen.getByText('68%')).toBeInTheDocument();
    expect(screen.getByText('Memory Usage')).toBeInTheDocument();
    expect(screen.getByText(/1\.5 GiB/)).toBeInTheDocument();
  });

  it('formats Azure Monitor job usage metrics using current Azure metric names', async () => {
    const job: JobLogStreamTarget = {
      ...JOBS[1],
      signals: [
        {
          name: 'UsageNanoCores',
          value: 750000000,
          unit: 'NanoCores',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        },
        {
          name: 'UsageBytes',
          value: 2147483648,
          unit: 'Bytes',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        }
      ]
    };

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot']
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[job]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByText('CPU Usage')).toBeInTheDocument();
    expect(screen.getByText('0.75 cores')).toBeInTheDocument();
    expect(screen.getByText('Memory Usage')).toBeInTheDocument();
    expect(screen.getByText('2 GiB')).toBeInTheDocument();
  });

  it('prefers raw job usage values when both raw and percent signals exist', async () => {
    const job: JobLogStreamTarget = {
      ...JOBS[1],
      signals: [
        {
          name: 'UsageNanoCores',
          value: 750000000,
          unit: 'NanoCores',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        },
        {
          name: 'UsageBytes',
          value: 2147483648,
          unit: 'Bytes',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        },
        {
          name: 'CpuPercent',
          value: 37.5,
          unit: 'Percent',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'unknown',
          source: 'metrics'
        },
        {
          name: 'MemoryPercent',
          value: 50.0,
          unit: 'Percent',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'unknown',
          source: 'metrics'
        }
      ]
    };

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot']
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[job]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByText('0.75 cores')).toBeInTheDocument();
    expect(screen.getByText('2 GiB')).toBeInTheDocument();
    expect(screen.queryByText('38%')).not.toBeInTheDocument();
    expect(screen.queryByText('50%')).not.toBeInTheDocument();
  });

  it('hydrates job usage from live system health refreshes when the initial snapshot has no signals', async () => {
    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot']
        }
      ]
    });
    vi.mocked(DataService.getSystemHealth).mockResolvedValueOnce({
      overall: 'healthy',
      dataLayers: [],
      recentJobs: [],
      alerts: [],
      resources: [
        {
          name: 'beta-job',
          resourceType: 'Microsoft.App/jobs',
          status: 'healthy',
          lastChecked: '2026-03-11T12:00:00Z',
          details: 'live metrics',
          signals: [
            {
              name: 'Usage Nano Cores',
              value: 500000000,
              unit: 'NanoCores',
              timestamp: '2026-03-11T12:00:05Z',
              status: 'healthy',
              source: 'metrics'
            },
            {
              name: 'Usage Bytes',
              value: 1073741824,
              unit: 'Bytes',
              timestamp: '2026-03-11T12:00:05Z',
              status: 'healthy',
              source: 'metrics'
            }
          ]
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[JOBS[1]]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    await waitFor(() => {
      expect(DataService.getSystemHealth).toHaveBeenCalledWith({ refresh: true });
    });
    expect(await screen.findByText('0.5 cores')).toBeInTheDocument();
    expect(screen.getByText('1 GiB')).toBeInTheDocument();
  });

  it('matches Azure metric names even when they include spaces', async () => {
    const job: JobLogStreamTarget = {
      ...JOBS[1],
      signals: [
        {
          name: 'Usage Nano Cores',
          value: 250000000,
          unit: 'NanoCores',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        },
        {
          name: 'Working Set Bytes',
          value: 536870912,
          unit: 'Bytes',
          timestamp: '2026-03-11T12:00:00Z',
          status: 'healthy',
          source: 'metrics'
        }
      ]
    };

    vi.mocked(DataService.getJobLogs).mockResolvedValueOnce({
      jobName: 'beta-job',
      runsRequested: 1,
      runsReturned: 1,
      tailLines: 10,
      runs: [
        {
          tail: ['beta snapshot']
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[job]} />);

    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();
    expect(screen.getByText('0.25 cores')).toBeInTheDocument();
    expect(screen.getByText('512 MiB')).toBeInTheDocument();
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
              message: 'beta snapshot'
            }
          ]
        }
      ]
    });

    const view = renderWithProviders(<JobLogStreamPanel jobs={[JOBS[1]]} />);

    await waitFor(() => {
      expect(DataService.getJobLogs).toHaveBeenCalledTimes(1);
    });
    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();

    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:beta-job/executions/beta-exec-001',
        resourceType: 'job',
        resourceName: 'beta-job',
        lines: [
          {
            id: 'line-live-1',
            message: 'beta live line',
            timestamp: '2026-03-11T12:00:02Z',
            stream_s: 'stdout'
          }
        ]
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
            startTime: '2026-03-11T12:00:00Z'
          }
        ]}
      />
    );

    await waitFor(() => {
      expect(screen.getByText('beta live line')).toBeInTheDocument();
    });
    expect(DataService.getJobLogs).toHaveBeenCalledTimes(1);
  });

  it('auto-scrolls while at bottom and pauses when manually scrolled up', async () => {
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
              message: 'beta snapshot'
            }
          ]
        }
      ]
    });

    renderWithProviders(<JobLogStreamPanel jobs={[JOBS[1]]} />);
    expect(await screen.findByText('beta snapshot')).toBeInTheDocument();

    const tail = screen.getByTestId('job-log-stream-tail');
    let simulatedScrollHeight = 200;
    Object.defineProperty(tail, 'clientHeight', { configurable: true, value: 100 });
    Object.defineProperty(tail, 'scrollHeight', {
      configurable: true,
      get: () => simulatedScrollHeight
    });

    tail.scrollTop = 100;
    fireEvent.scroll(tail);

    simulatedScrollHeight = 240;
    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:beta-job/executions/beta-exec-001',
        resourceType: 'job',
        resourceName: 'beta-job',
        lines: [
          {
            id: 'follow-line-1',
            message: 'line while following',
            timestamp: '2026-03-11T12:00:02Z',
            stream_s: 'stdout'
          }
        ]
      });
    });

    await waitFor(() => {
      expect(screen.getByText('line while following')).toBeInTheDocument();
      expect(tail.scrollTop).toBe(240);
    });

    tail.scrollTop = 24;
    fireEvent.scroll(tail);

    simulatedScrollHeight = 300;
    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:beta-job/executions/beta-exec-001',
        resourceType: 'job',
        resourceName: 'beta-job',
        lines: [
          {
            id: 'paused-line-1',
            message: 'line while paused',
            timestamp: '2026-03-11T12:00:03Z',
            stream_s: 'stderr'
          }
        ]
      });
    });

    await waitFor(() => {
      expect(screen.getByText('line while paused')).toBeInTheDocument();
      expect(tail.scrollTop).toBe(24);
    });

    tail.scrollTop = 200;
    fireEvent.scroll(tail);

    simulatedScrollHeight = 360;
    await act(async () => {
      emitConsoleLogStream({
        topic: 'job-logs:beta-job/executions/beta-exec-001',
        resourceType: 'job',
        resourceName: 'beta-job',
        lines: [
          {
            id: 'resume-line-1',
            message: 'line after resume',
            timestamp: '2026-03-11T12:00:04Z',
            stream_s: 'stdout'
          }
        ]
      });
    });

    await waitFor(() => {
      expect(screen.getByText('line after resume')).toBeInTheDocument();
      expect(tail.scrollTop).toBe(360);
    });
  });

  it('shows a neutral notice when job log streaming is not configured', async () => {
    vi.mocked(DataService.getJobLogs).mockRejectedValueOnce(
      new Error(
        'API Error: 503 Service Unavailable [requestId=req-123] - {"detail":"Log Analytics is not configured for job log retrieval."}'
      )
    );

    renderWithProviders(<JobLogStreamPanel jobs={[JOBS[0]]} />);

    expect(
      await screen.findByText('Live job logs are not configured for this environment.')
    ).toBeInTheDocument();
    expect(screen.queryByText(/Failed to load logs:/i)).not.toBeInTheDocument();
  });
});
