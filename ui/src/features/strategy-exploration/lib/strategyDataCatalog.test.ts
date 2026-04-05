import { describe, expect, it } from 'vitest';

import { formatDateRangeLabel, normalizeDomainKey, titleCase } from './strategyDataCatalog';

describe('strategyDataCatalog helpers', () => {
  it('normalizes legacy targets naming to price-target', () => {
    expect(normalizeDomainKey('Targets')).toBe('price-target');
    expect(normalizeDomainKey('price target')).toBe('price-target');
  });

  it('formats date ranges using a stable single-day shortcut', () => {
    expect(
      formatDateRangeLabel({
        dateRange: {
          min: '2026-04-01T00:00:00Z',
          max: '2026-04-01T12:00:00Z'
        }
      } as never)
    ).toBe('2026-04-01');
  });

  it('title-cases medallion labels for navigator badges', () => {
    expect(titleCase('price-target')).toBe('Price Target');
  });
});
