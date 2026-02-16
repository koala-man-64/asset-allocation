import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { ApiError, backtestApi } from '@/services/backtestApi';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

type JobControlAction = 'suspend' | 'resume' | 'stop';

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
    const action: JobControlAction = suspended ? 'stop' : 'resume';
    setJobControl({ jobName, action });
    try {
      if (suspended) {
        await backtestApi.stopJob(jobName);
        toast.success(`Stopped ${jobName}`);
      } else {
        await backtestApi.resumeJob(jobName);
        toast.success(`Resumed ${jobName}`);
      }
      void queryClient.invalidateQueries({ queryKey });
    } catch (err: unknown) {
      const message = err instanceof ApiError
        ? `${err.status}: ${formatSystemStatusText(err.message)}`
        : formatSystemStatusText(err);
      toast.error(`Failed to ${action} ${jobName}: ${message}`);
    } finally {
      setJobControl(null);
    }
  };

  const stopJob = async (jobName: string, queryKey: string[] = ['systemHealth']) => {
    setJobControl({ jobName, action: 'stop' });
    try {
      await backtestApi.stopJob(jobName);
      toast.success(`Stopped ${jobName}`);
      void queryClient.invalidateQueries({ queryKey });
    } catch (err: unknown) {
      const message = err instanceof ApiError
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
