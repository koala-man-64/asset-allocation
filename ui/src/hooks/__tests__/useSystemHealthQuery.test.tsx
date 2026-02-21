import { describe, it, expect, vi, afterEach } from 'vitest';
import React from 'react';
import { renderWithProviders } from '@/test/utils';
import { DataService } from '@/services/DataService';
import { useSystemHealthQuery } from '@/hooks/useDataQueries';
import { waitFor } from '@testing-library/react';

function Probe() {
  useSystemHealthQuery();
  return null;
}

function ProbeAutoRefresh() {
  useSystemHealthQuery({ autoRefresh: true });
  return null;
}

function ProbeNoAutoRefresh() {
  useSystemHealthQuery({ autoRefresh: false });
  return null;
}

describe('useSystemHealthQuery', () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('stops polling when the endpoint returns 404', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'info').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});

    const getSystemHealthSpy = vi
      .spyOn(DataService, 'getSystemHealthWithMeta')
      .mockRejectedValue(new Error('API Error: 404 Not Found - {"detail":"Not Found"}'));

    renderWithProviders(<ProbeAutoRefresh />);

    await Promise.all([
      waitFor(() => {
        expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
      }),
      vi.advanceTimersByTimeAsync(1000)
    ]);

    await vi.advanceTimersByTimeAsync(60_000);

    expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
  });

  it('does not poll when auto refresh is disabled', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'info').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});

    const getSystemHealthSpy = vi.spyOn(DataService, 'getSystemHealthWithMeta').mockResolvedValue({
      data: {
        overall: 'healthy',
        dataLayers: [],
        recentJobs: [],
        alerts: [],
        resources: []
      },
      meta: {
        status: 200,
        durationMs: 10,
        url: '/api/system/health',
        cacheDegraded: false,
        stale: false,
        requestId: 'test-request-id'
      }
    });

    renderWithProviders(<ProbeNoAutoRefresh />);

    await Promise.all([
      waitFor(() => {
        expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
      }),
      vi.advanceTimersByTimeAsync(1000)
    ]);

    await vi.advanceTimersByTimeAsync(90_000);

    expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
  });

  it('does not poll by default', async () => {
    vi.useFakeTimers();
    vi.spyOn(console, 'info').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});

    const getSystemHealthSpy = vi.spyOn(DataService, 'getSystemHealthWithMeta').mockResolvedValue({
      data: {
        overall: 'healthy',
        dataLayers: [],
        recentJobs: [],
        alerts: [],
        resources: []
      },
      meta: {
        status: 200,
        durationMs: 10,
        url: '/api/system/health',
        cacheDegraded: false,
        stale: false,
        requestId: 'test-request-id'
      }
    });

    renderWithProviders(<Probe />);

    await Promise.all([
      waitFor(() => {
        expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
      }),
      vi.advanceTimersByTimeAsync(1000)
    ]);

    await vi.advanceTimersByTimeAsync(90_000);

    expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
  });
});
