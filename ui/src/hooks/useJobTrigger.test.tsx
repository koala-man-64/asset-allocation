import React, { type PropsWithChildren } from 'react';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { queryKeys } from '@/hooks/useDataQueries';
import { backtestApi } from '@/services/backtestApi';
import { toast } from 'sonner';

import { useJobTrigger } from './useJobTrigger';

vi.mock('@/services/backtestApi', () => ({
  backtestApi: {
    triggerJob: vi.fn()
  },
  ApiError: class ApiError extends Error {
    status: number;

    constructor(status: number, message: string) {
      super(message);
      this.status = status;
    }
  }
}));

vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn()
  }
}));

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
        staleTime: 0
      }
    }
  });
}

describe('useJobTrigger', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('stores a temporary running override after a successful trigger', async () => {
    vi.mocked(backtestApi.triggerJob).mockResolvedValue({
      jobName: 'aca-job-zeta',
      status: 'queued',
      executionId: 'exec-1',
      executionName: 'exec-1'
    });

    const queryClient = createQueryClient();
    const wrapper = ({ children }: PropsWithChildren) => (
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    );

    const { result } = renderHook(() => useJobTrigger(), { wrapper });

    await act(async () => {
      await result.current.triggerJob('aca-job-zeta');
    });

    await waitFor(() => {
      const overrides = queryClient.getQueryData<Record<string, Record<string, unknown>>>(
        queryKeys.systemHealthJobOverrides()
      );
      expect(overrides?.['aca-job-zeta']).toMatchObject({
        jobName: 'aca-job-zeta',
        jobKey: 'aca-job-zeta',
        status: 'running',
        runningState: 'Running',
        triggeredBy: 'manual',
        executionId: 'exec-1',
        executionName: 'exec-1'
      });
    });

    expect(toast.success).toHaveBeenCalledWith('Triggered aca-job-zeta');
    expect(result.current.triggeringJob).toBeNull();
  });
});
