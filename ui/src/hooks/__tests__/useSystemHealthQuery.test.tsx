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
      .spyOn(DataService, 'getSystemHealth')
      .mockRejectedValue(new Error('API Error: 404 Not Found - {"detail":"Not Found"}'));

    renderWithProviders(<Probe />);

    await waitFor(() => {
      expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
    });

    vi.advanceTimersByTime(30_000);

    expect(getSystemHealthSpy).toHaveBeenCalledTimes(1);
  });
});

