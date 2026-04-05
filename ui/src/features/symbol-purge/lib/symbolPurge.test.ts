import { describe, expect, it } from 'vitest';

import { buildPurgeExpression, extractBatchResult } from './symbolPurge';

describe('symbolPurge helpers', () => {
  it('builds top-percent expressions with the rolling aggregation label', () => {
    expect(buildPurgeExpression('top_percent', 'close', 15, 'avg', 5)).toBe(
      'top 15% by avg(close) over last 5 rows'
    );
  });

  it('normalizes batch progress fields when the backend omits derived counters', () => {
    expect(
      extractBatchResult({
        operationId: 'op-1',
        status: 'succeeded',
        scope: 'symbols',
        createdAt: '2026-04-04T00:00:00Z',
        updatedAt: '2026-04-04T00:00:00Z',
        startedAt: '2026-04-04T00:00:00Z',
        completedAt: '2026-04-04T00:00:01Z',
        error: null,
        result: {
          scope: 'symbols',
          requestedSymbolCount: 3,
          succeeded: 2,
          failed: 1,
          totalDeleted: 7,
          symbolResults: []
        }
      })
    ).toMatchObject({
      requestedSymbolCount: 3,
      completed: 3,
      pending: 0,
      succeeded: 2,
      failed: 1,
      totalDeleted: 7
    });
  });
});
