import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { ApiError, backtestApi } from '@/services/backtestApi';

type JobControlAction = 'suspend' | 'resume';

export function useJobSuspend() {
  const queryClient = useQueryClient();
  const [jobControl, setJobControl] = useState<{
    jobName: string;
    action: JobControlAction;
  } | null>(null);

  const setJobSuspended = async (
    jobName: string,
    suspended: boolean,
    queryKey: string[] = ['systemHealth']
  ) => {
    const action: JobControlAction = suspended ? 'suspend' : 'resume';
    setJobControl({ jobName, action });
    try {
      if (suspended) {
        await backtestApi.suspendJob(jobName);
        toast.success(`Suspended ${jobName}`);
      } else {
        await backtestApi.resumeJob(jobName);
        toast.success(`Resumed ${jobName}`);
      }
      void queryClient.invalidateQueries({ queryKey });
    } catch (err: unknown) {
      const message =
        err instanceof ApiError
          ? `${err.status}: ${err.message}`
          : err instanceof Error
            ? err.message
            : String(err);
      toast.error(`Failed to ${action} ${jobName}: ${message}`);
    } finally {
      setJobControl(null);
    }
  };

  return {
    jobControl,
    setJobSuspended
  };
}
