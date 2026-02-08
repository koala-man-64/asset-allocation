import { describe, expect, it } from 'vitest';
import {
  computeLayerDrift,
  getProbeIdForRow,
  isValidTickerSymbol,
  parseImpactsByDomain
} from '@/app/components/pages/data-quality/dataQualityUtils';
import type { DataLayer } from '@/types/strategy';

describe('dataQualityUtils', () => {
  it('computes drift with SLA from maxAgeSeconds', () => {
    const layers: DataLayer[] = [
      {
        name: 'silver',
        description: '',
        status: 'healthy',
        lastUpdated: '2026-02-06T00:00:00Z',
        refreshFrequency: '',
        domains: [
          {
            name: 'market',
            type: 'delta',
            path: 'market',
            lastUpdated: '2026-02-06T00:00:00Z',
            status: 'healthy',
            maxAgeSeconds: 3600
          }
        ]
      },
      {
        name: 'gold',
        description: '',
        status: 'healthy',
        lastUpdated: '2026-02-06T00:20:00Z',
        refreshFrequency: '',
        domains: [
          {
            name: 'market',
            type: 'delta',
            path: 'market',
            lastUpdated: '2026-02-06T00:20:00Z',
            status: 'healthy',
            maxAgeSeconds: 1800
          }
        ]
      }
    ];

    const rows = computeLayerDrift(layers);
    expect(rows).toHaveLength(1);
    expect(rows[0].lagSeconds).toBe(20 * 60);
    expect(rows[0].slaSeconds).toBe(1800);
  });

  it('parses impacts safely and ignores malformed entries', () => {
    const parsed = parseImpactsByDomain({
      market: ['strategy-a', 123, null],
      finance: 'invalid',
      earnings: []
    });
    expect(parsed.market).toEqual(['strategy-a', '123', 'null']);
    expect(parsed.finance).toBeUndefined();
    expect(parsed.earnings).toEqual([]);
  });

  it('validates ticker symbols', () => {
    expect(isValidTickerSymbol('AAPL')).toBe(true);
    expect(isValidTickerSymbol('BRK.B')).toBe(true);
    expect(isValidTickerSymbol('bad/ticker')).toBe(false);
    expect(isValidTickerSymbol('')).toBe(false);
  });

  it('maps probe IDs by layer/domain and optional ticker', () => {
    expect(getProbeIdForRow('silver', 'market')).toBe('probe:silver:market');
    expect(getProbeIdForRow('gold', 'finance', 'aapl')).toBe('probe:gold:finance:AAPL');
    expect(getProbeIdForRow('platinum', 'market', 'SPY')).toBe('probe:platinum:market:SPY');
    expect(getProbeIdForRow('silver', 'market', 'bad/ticker')).toBeNull();
  });
});
