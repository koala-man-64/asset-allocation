import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { ApiError, backtestApi } from '@/services/backtestApi';

export function useJobTrigger() {
    const queryClient = useQueryClient();
    const [triggeringJob, setTriggeringJob] = useState<string | null>(null);

    const triggerJob = async (jobName: string, queryKey: string[] = ['liveSystemHealth']) => {
        setTriggeringJob(jobName);
        try {
            await backtestApi.triggerJob(jobName);
            toast.success(`Triggered ${jobName}`);
            void queryClient.invalidateQueries({ queryKey });
        } catch (err) {
            const message =
                err instanceof ApiError
                    ? `${err.status}: ${err.message}`
                    : (err as any)?.message || String(err);
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
