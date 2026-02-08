import { renderHook, act, waitFor } from '@testing-library/react';
import { useDataProbes } from '@/hooks/useDataProbes';
import { DataService } from '@/services/DataService';
import { DomainRow } from '@/app/components/pages/data-quality/dataQualityUtils';
import { vi, describe, it, expect, beforeEach, Mock } from 'vitest';

// Mock DataService
vi.mock('@/services/DataService');

describe('useDataProbes', () => {
    const mockRows: DomainRow[] = [
        {
            layerName: 'Silver',
            domain: { name: 'market', path: 'silver/market', type: 'blob', lastUpdated: new Date().toISOString(), status: 'healthy' }
        }
    ];

    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should initialize with idle state', () => {
        const { result } = renderHook(() =>
            useDataProbes({ financeSubDomain: '', ticker: 'AAPL', rows: mockRows })
        );

        expect(result.current.probeResults).toEqual({});
        expect(result.current.isRunningAll).toBe(false);
    });

    it('should run a probe successfully', async () => {
        (DataService.getMarketData as Mock).mockResolvedValue(['data']);

        const { result } = renderHook(() =>
            useDataProbes({ financeSubDomain: '', ticker: 'AAPL', rows: mockRows })
        );

        const row = mockRows[0];

        await act(async () => {
            await result.current.probeForRow(row);
        });

        const probeId = 'probe:silver:market';
        const probeResult = result.current.probeResults[probeId];

        expect(probeResult).toBeDefined();
        expect(probeResult.status).toBe('pass');
        expect(probeResult.detail).toContain('Rows: 1');
    });

    it('should handle probe failure', async () => {
        (DataService.getMarketData as Mock).mockResolvedValue([]);

        const { result } = renderHook(() =>
            useDataProbes({ financeSubDomain: '', ticker: 'AAPL', rows: mockRows })
        );

        const row = mockRows[0];

        await act(async () => {
            await result.current.probeForRow(row);
        });

        const probeId = 'probe:silver:market';
        const probeResult = result.current.probeResults[probeId];

        expect(probeResult).toBeDefined();
        expect(probeResult.status).toBe('fail');
        expect(probeResult.detail).toContain('No rows returned');
    });

    it('should fail validation for invalid ticker', async () => {
        const { result } = renderHook(() =>
            useDataProbes({ financeSubDomain: '', ticker: 'INVALID!', rows: mockRows })
        );

        const row = mockRows[0];

        await act(async () => {
            await result.current.probeForRow(row);
        });

        // The ID generation logic in the hook uses getProbeIdForRow or falls back.
        // Normalized layer: 'silver', domain: 'market' -> 'probe:silver:market'
        const probeId = 'probe:silver:market';
        const probeResult = result.current.probeResults[probeId];

        expect(probeResult).toBeDefined();
        expect(probeResult.status).toBe('fail');
        expect(probeResult.detail).toContain('Ticker must match');
    });

    it('should run all probes', async () => {
        (DataService.getMarketData as Mock).mockResolvedValue(['data']);

        const { result } = renderHook(() =>
            useDataProbes({ financeSubDomain: '', ticker: 'AAPL', rows: mockRows })
        );

        // Initial state
        expect(result.current.isRunningAll).toBe(false);

        let runAllPromise;
        await act(async () => {
            runAllPromise = result.current.runAll();
        });

        // We need to wait for the async operation to complete
        await waitFor(() => {
            expect(result.current.isRunningAll).toBe(false);
        });

        const probeResult = result.current.probeResults['probe:silver:market'];
        expect(probeResult?.status).toBe('pass');
        expect(result.current.runAllStatusMessage).toBe('Probe run complete.');
    });
});
