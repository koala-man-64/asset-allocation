import { describe, expect, it } from 'vitest';

import {
  effectiveJobStatus,
  hasActiveJobRunningState,
  isSuspendedJobRunningState
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
});
