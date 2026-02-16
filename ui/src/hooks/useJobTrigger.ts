import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { ApiError, backtestApi } from '@/services/backtestApi';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

export function useJobTrigger() {
  const queryClient = useQueryClient();
  const [triggeringJob, setTriggeringJob] = useState<string | null>(null);

  const triggerJob = async (jobName: string, queryKey: string[] = ['systemHealth']) => {
    setTriggeringJob(jobName);
    try {
      await backtestApi.triggerJob(jobName);
      toast.success(`Triggered ${jobName}`);
      void queryClient.invalidateQueries({ queryKey });
    } catch (err: unknown) {
      const message = err instanceof ApiError
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
