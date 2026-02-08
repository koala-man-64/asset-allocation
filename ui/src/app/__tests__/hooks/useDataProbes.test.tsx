import { renderHook, act, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import { useDataProbes } from '@/hooks/useDataProbes';
import { DataService } from '@/services/DataService';
import type { DomainRow } from '@/app/components/pages/data-quality/dataQualityUtils';

vi.mock('@/services/DataService', () => ({
  DataService: {
    getDataQualityValidation: vi.fn()
  }
}));

describe('useDataProbes', () => {
  const mockRows: DomainRow[] = [
    {
      layerName: 'Silver',
      domain: {
        name: 'market',
        path: 'market-data-by-date',
        type: 'delta',
        lastUpdated: new Date().toISOString(),
        status: 'healthy'
      }
    }
  ];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('initializes with idle state', () => {
    const { result } = renderHook(() => useDataProbes({ ticker: 'AAPL', rows: mockRows }));
    expect(result.current.probeResults).toEqual({});
    expect(result.current.isRunningAll).toBe(false);
  });

  it('runs a probe successfully with ticker-scoped validation', async () => {
    vi.mocked(DataService.getDataQualityValidation).mockResolvedValue({
      layer: 'silver',
      domain: 'market',
      status: 'healthy',
      rowCount: 7,
      columns: [],
      timestamp: new Date().toISOString()
    });

    const { result } = renderHook(() => useDataProbes({ ticker: 'AAPL', rows: mockRows }));

    await act(async () => {
      await result.current.probeForRow(mockRows[0]);
    });

    const probeResult = result.current.probeResults['probe:silver:market:AAPL'];
    expect(probeResult).toBeDefined();
    expect(probeResult.status).toBe('pass');
    expect(probeResult.detail).toContain('Rows: 7');
    expect(DataService.getDataQualityValidation).toHaveBeenCalledWith(
      'silver',
      'market',
      'AAPL',
      expect.any(AbortSignal)
    );
  });

  it('marks probe as failed when validation status is error', async () => {
    vi.mocked(DataService.getDataQualityValidation).mockResolvedValue({
      layer: 'silver',
      domain: 'market',
      status: 'error',
      rowCount: 0,
      columns: [],
      timestamp: new Date().toISOString(),
      error: 'validation backend failed'
    });

    const { result } = renderHook(() => useDataProbes({ ticker: 'AAPL', rows: mockRows }));

    await act(async () => {
      await result.current.probeForRow(mockRows[0]);
    });

    const probeResult = result.current.probeResults['probe:silver:market:AAPL'];
    expect(probeResult).toBeDefined();
    expect(probeResult.status).toBe('fail');
    expect(probeResult.detail).toContain('validation backend failed');
  });

  it('requires a valid ticker before running probes', async () => {
    const { result } = renderHook(() => useDataProbes({ ticker: 'BAD/TICKER', rows: mockRows }));

    await act(async () => {
      await result.current.probeForRow(mockRows[0]);
    });

    expect(DataService.getDataQualityValidation).not.toHaveBeenCalled();
    expect(result.current.probeResults).toEqual({});
    expect(result.current.runAllStatusMessage).toContain('Enter a valid symbol');
  });

  it('runs all probes for the active symbol', async () => {
    vi.mocked(DataService.getDataQualityValidation).mockResolvedValue({
      layer: 'silver',
      domain: 'market',
      status: 'healthy',
      rowCount: 3,
      columns: [],
      timestamp: new Date().toISOString()
    });

    const { result } = renderHook(() => useDataProbes({ ticker: 'MSFT', rows: mockRows }));

    await act(async () => {
      void result.current.runAll();
    });

    await waitFor(() => {
      expect(result.current.isRunningAll).toBe(false);
    });

    const probeResult = result.current.probeResults['probe:silver:market:MSFT'];
    expect(probeResult?.status).toBe('pass');
    expect(result.current.runAllStatusMessage).toBe('Probe run complete.');
  });
});
