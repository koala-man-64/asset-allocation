import { describe, expect, it } from 'vitest';

import type { DataLayer } from '@/types/strategy';

import {
  getDomainOrderEntries,
  getDomainOrderIndex
} from './domainOrdering';

const makeDomain = (name: string) => ({
  name,
  description: '',
  type: 'blob' as const,
  path: String(name).trim(),
  lastUpdated: '2026-02-01T00:00:00Z',
  status: 'healthy' as const
});

const makeLayer = (name: string, domains: string[]): DataLayer => ({
  name,
  description: `${name} layer`,
  status: 'healthy',
  lastUpdated: '2026-02-01T00:00:00Z',
  refreshFrequency: 'Daily',
  domains: domains.map(makeDomain)
});

describe('domainOrdering utilities', () => {
  it('deduplicates duplicate domain names across layers by normalized key', () => {
    const dataLayers: DataLayer[] = [
      makeLayer('bronze', ['Market', 'Market', 'zeta']),
      makeLayer('silver', ['market', 'alpha'])
    ];

    const entries = getDomainOrderEntries(dataLayers);

    expect(entries).toEqual([
      { key: 'alpha', label: 'alpha' },
      { key: 'market', label: 'Market' },
      { key: 'zeta', label: 'zeta' }
    ]);
    expect(getDomainOrderIndex(dataLayers)).toEqual(
      new Map([
        ['alpha', 0],
        ['market', 1],
        ['zeta', 2]
      ])
    );
  });

  it('normalizes case using normalizeDomainKey behavior', () => {
    const dataLayers: DataLayer[] = [
      makeLayer('bronze', ['AuRoRa']),
      makeLayer('silver', ['aURORA'])
    ];

    const entries = getDomainOrderEntries(dataLayers);

    expect(entries).toEqual([{ key: 'aurora', label: 'AuRoRa' }]);
  });

  it('orders normalized keys deterministically and alphabetically', () => {
    const dataLayers: DataLayer[] = [
      makeLayer('bronze', ['zulu', 'market']),
      makeLayer('silver', ['Alpha']),
      makeLayer('gold', ['beta', 'Price-target'])
    ];

    expect(getDomainOrderEntries(dataLayers).map((entry) => entry.key)).toEqual([
      'alpha',
      'beta',
      'market',
      'price-target',
      'zulu'
    ]);
  });

  it('normalizes "targets" to canonical "price-target"', () => {
    const dataLayers: DataLayer[] = [
      makeLayer('bronze', ['targets']),
      makeLayer('silver', ['price-target'])
    ];

    expect(getDomainOrderEntries(dataLayers).map((entry) => entry.key)).toEqual(['price-target']);
  });
});
