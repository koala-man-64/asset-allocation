import { describe, expect, it } from 'vitest';

import type { JobRun } from '@/types/strategy';
import {
  buildAnchoredJobRunIndex,
  effectiveJobStatus,
  hasActiveJobRunningState,
  isSuspendedJobRunningState,
  normalizeJobStatus,
  selectAnchoredJobRun
} from '@/app/components/pages/system-status/SystemStatusHelpers';

describe('SystemStatusHelpers', () => {
  it('prefers active live running state over the last completed run', () => {
    expect(effectiveJobStatus('success', 'Running')).toBe('running');
  });

  it('maps suspended live state to pending', () => {
    expect(effectiveJobStatus('success', 'Suspended')).toBe('pending');
  });

  it('shares running-state detection across helpers', () => {
    expect(hasActiveJobRunningState('queued')).toBe(true);
    expect(isSuspendedJobRunningState('Suspended')).toBe(true);
  });

  it('normalizes spaced and hyphenated Azure status variants', () => {
    expect(hasActiveJobRunningState('In Progress')).toBe(true);
    expect(normalizeJobStatus('Succeeded With Warnings')).toBe('warning');
    expect(effectiveJobStatus('In-Progress', null)).toBe('running');
  });

  it('anchors to an active run even when a newer run finished later', () => {
    expect(
      selectAnchoredJobRun([
        {
          status: 'success',
          startTime: '2026-03-21T12:00:00Z'
        },
        {
          status: 'running',
          startTime: '2026-03-20T12:00:00Z'
        }
      ])
    ).toEqual({
      status: 'running',
      startTime: '2026-03-20T12:00:00Z'
    });
  });

  it('indexes each job by its anchored run rather than the newest completed run', () => {
    const anchored = buildAnchoredJobRunIndex([
      {
        jobName: 'bronze-market-job',
        status: 'success',
        startTime: '2026-03-21T12:00:00Z'
      },
      {
        jobName: 'bronze-market-job',
        status: 'running',
        startTime: '2026-03-20T12:00:00Z'
      }
    ] as JobRun[]);

    expect(anchored.get('bronze-market-job')).toEqual(
      expect.objectContaining({
        status: 'running',
        startTime: '2026-03-20T12:00:00Z'
      })
    );
  });
});
