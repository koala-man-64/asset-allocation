import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { queryKeys } from '@/hooks/useDataQueries';
import { ApiError, backtestApi } from '@/services/backtestApi';
import { upsertRunningJobOverride } from '@/hooks/useSystemHealthJobOverrides';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

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

export function useJobTrigger() {
  const queryClient = useQueryClient();
  const [triggeringJob, setTriggeringJob] = useState<string | null>(null);

  const triggerJob = async (
    jobName: string,
    queryKey: QueryKeyLike | ReadonlyArray<QueryKeyLike> = DEFAULT_INVALIDATION_KEYS
  ) => {
    setTriggeringJob(jobName);
    try {
      const response = await backtestApi.triggerJob(jobName);
      upsertRunningJobOverride(queryClient, {
        jobName,
        response
      });
      toast.success(`Triggered ${jobName}`);
      await Promise.all(
        normalizeInvalidationKeys(queryKey).map((key) =>
          queryClient.invalidateQueries({ queryKey: key })
        )
      );
    } catch (err: unknown) {
      const message =
        err instanceof ApiError
          ? `${err.status}: ${formatSystemStatusText(err.message)}`
          : formatSystemStatusText(err);
      toast.error(`Failed to trigger ${jobName}: ${message}`);
    } finally {
      setTriggeringJob(null);
    }
  };

  return {
    triggeringJob,
    triggerJob
  };
}
