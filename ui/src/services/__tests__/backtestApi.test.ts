import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { backtestApi } from '../backtestApi';

// Mock the global fetch
const fetchMock = vi.fn();
global.fetch = fetchMock;

describe('backtestApi', () => {
    beforeEach(() => {
        fetchMock.mockReset();
    });

    afterEach(() => {
        vi.clearAllMocks();
    });

    describe('getJobLogs', () => {
        it('fetches logs with default parameters', async () => {
            fetchMock.mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    jobName: 'test-job',
                    runs: [],
                    tailLines: 100,
                    runsRequested: 1,
                    runsReturned: 0
                }),
            });

            await backtestApi.getJobLogs('test-job');

            expect(fetchMock).toHaveBeenCalledTimes(1);
            const url = new URL(fetchMock.mock.calls[0][0] as string, 'http://localhost');

            // Check path encoding
            expect(url.pathname).toContain('/system/jobs/test-job/logs');

            // Check default query params
            expect(url.searchParams.get('runs')).toBe('1');
        });

        it('fetches logs with custom run count', async () => {
            fetchMock.mockResolvedValueOnce({
                ok: true,
                json: async () => ({ jobName: 'test-job', runs: [] }),
            });

            await backtestApi.getJobLogs('test-job', { runs: 5 });

            const url = new URL(fetchMock.mock.calls[0][0] as string, 'http://localhost');
            expect(url.searchParams.get('runs')).toBe('5');
        });
    });

    describe('job control', () => {
        it('posts suspend job endpoint', async () => {
            fetchMock.mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    jobName: 'test-job',
                    action: 'suspend',
                    runningState: 'Suspended',
                }),
            });

            await backtestApi.suspendJob('test-job');

            expect(fetchMock).toHaveBeenCalledTimes(1);
            const url = new URL(fetchMock.mock.calls[0][0] as string, 'http://localhost');
            expect(url.pathname).toBe('/api/system/jobs/test-job/suspend');
        });

        it('posts resume job endpoint', async () => {
            fetchMock.mockResolvedValueOnce({
                ok: true,
                json: async () => ({
                    jobName: 'test-job',
                    action: 'resume',
                    runningState: 'Running',
                }),
            });

            await backtestApi.resumeJob('test-job');

            expect(fetchMock).toHaveBeenCalledTimes(1);
            const url = new URL(fetchMock.mock.calls[0][0] as string, 'http://localhost');
            expect(url.pathname).toBe('/api/system/jobs/test-job/resume');
        });
    });

    describe('getDomainData', () => {
        it('endpoints are correctly constructed and encoded', async () => {
            fetchMock.mockResolvedValueOnce({
                ok: true,
                json: async () => ([]),
            });

            await backtestApi.getDomainData('AAPL', 'earnings', 'silver');

            expect(fetchMock).toHaveBeenCalledTimes(1);
            const url = new URL(fetchMock.mock.calls[0][0] as string, 'http://localhost');

            // Check structure: /data/{layer}/{domain}?ticker={ticker}
            // Domain 'earnings' should be encoded but it's safe chars
            expect(url.pathname).toBe('/api/data/silver/earnings');
            expect(url.searchParams.get('ticker')).toBe('AAPL');
        });

        it('handles special characters in domain names', async () => {
            fetchMock.mockResolvedValueOnce({
                ok: true,
                json: async () => ([]),
            });

            // 'price-target' contains a dash
            await backtestApi.getDomainData('MSFT', 'price-target', 'gold');

            const url = new URL(fetchMock.mock.calls[0][0] as string, 'http://localhost');
            expect(url.pathname).toBe('/api/data/gold/price-target');
        });
    });
});
