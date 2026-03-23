import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { queryKeys } from '@/hooks/useDataQueries';
import { ApiError, backtestApi } from '@/services/backtestApi';
import { clearJobOverride } from '@/hooks/useSystemHealthJobOverrides';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

type JobControlAction = 'suspend' | 'resume' | 'stop';
type QueryKeyLike = readonly string[];
const DEFAULT_INVALIDATION_KEYS = [queryKeys.systemStatusView(), queryKeys.systemHealth()] as const;

function normalizeInvalidationKeys(
  queryKey: QueryKeyLike | ReadonlyArray<QueryKeyLike> = DEFAULT_INVALIDATION_KEYS
): QueryKeyLike[] {
  if (Array.isArray(queryKey[0])) {
    return (queryKey as ReadonlyArray<QueryKeyLike>).filter((entry) => entry.length > 0);
  }
  return [(queryKey as QueryKeyLike).filter(Boolean)];
}

export function useJobSuspend() {
  const queryClient = useQueryClient();
  const [jobControl, setJobControl] = useState<{
    jobName: string;
    action: JobControlAction;
  } | null>(null);

  const invalidateStatusQueries = async (
    queryKey: QueryKeyLike | ReadonlyArray<QueryKeyLike> = DEFAULT_INVALIDATION_KEYS
  ) => {
    await Promise.all(
      normalizeInvalidationKeys(queryKey).map((key) =>
        queryClient.invalidateQueries({ queryKey: key })
      )
    );
  };

  const setJobSuspended = async (
    jobName: string,
    suspended: boolean,
    queryKey: QueryKeyLike | ReadonlyArray<QueryKeyLike> = DEFAULT_INVALIDATION_KEYS
  ) => {
    const action: JobControlAction = suspended ? 'stop' : 'resume';
    setJobControl({ jobName, action });
    try {
      if (suspended) {
        await backtestApi.stopJob(jobName);
        clearJobOverride(queryClient, jobName);
        toast.success(`Stopped ${jobName}`);
      } else {
        await backtestApi.resumeJob(jobName);
        clearJobOverride(queryClient, jobName);
        toast.success(`Resumed ${jobName}`);
      }
      await invalidateStatusQueries(queryKey);
    } catch (err: unknown) {
      const message =
        err instanceof ApiError
          ? `${err.status}: ${formatSystemStatusText(err.message)}`
          : formatSystemStatusText(err);
      toast.error(`Failed to ${action} ${jobName}: ${message}`);
    } finally {
      setJobControl(null);
    }
  };

  const stopJob = async (
    jobName: string,
    queryKey: QueryKeyLike | ReadonlyArray<QueryKeyLike> = DEFAULT_INVALIDATION_KEYS
  ) => {
    setJobControl({ jobName, action: 'stop' });
    try {
      await backtestApi.stopJob(jobName);
      clearJobOverride(queryClient, jobName);
      toast.success(`Stopped ${jobName}`);
      await invalidateStatusQueries(queryKey);
    } catch (err: unknown) {
      const message =
        err instanceof ApiError
          ? `${err.status}: ${formatSystemStatusText(err.message)}`
          : formatSystemStatusText(err);
      toast.error(`Failed to stop ${jobName}: ${message}`);
    } finally {
      setJobControl(null);
    }
  };

  return {
    jobControl,
    setJobSuspended,
    stopJob
  };
}
