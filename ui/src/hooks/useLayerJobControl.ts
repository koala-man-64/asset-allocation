import { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { queryKeys } from '@/hooks/useDataQueries';
import { backtestApi } from '@/services/backtestApi';
import { DataLayer } from '@/types/strategy';
import {
  normalizeAzureJobName,
  resolveManagedJobName
} from '@/app/components/pages/system-status/SystemStatusHelpers';

type LayerAction = 'stop' | 'resume' | 'trigger';

interface LayerControlState {
  [layerName: string]: {
    action: LayerAction;
    isLoading: boolean;
  };
}

export function useLayerJobControl() {
  const queryClient = useQueryClient();
  const [layerStates, setLayerStates] = useState<LayerControlState>({});

  const getJobNames = (layer: DataLayer): string[] => {
    const jobNames = new Set<string>();
    for (const domain of layer.domains || []) {
      const jobName = resolveManagedJobName({
        jobName: domain.jobName,
        jobUrl: domain.jobUrl,
        layerName: layer.name,
        domainName: domain.name
      });

      if (jobName) {
        const normalizedKey = normalizeAzureJobName(jobName);
        if (normalizedKey) {
          jobNames.add(normalizedKey);
        }
      }
    }
    return Array.from(jobNames);
  };

  const triggerLayerJobs = async (layer: DataLayer) => {
    const jobs = getJobNames(layer);
    if (jobs.length === 0) {
      toast.info(`No jobs found for layer ${layer.name}`);
      return;
    }

    setLayerStates((prev) => ({
      ...prev,
      [layer.name]: { action: 'trigger', isLoading: true }
    }));

    try {
      toast.info(`Triggering ${jobs.length} jobs for ${layer.name}...`);
      await Promise.allSettled(jobs.map((job) => backtestApi.triggerJob(job)));
      toast.success(`Trigger commands sent for ${layer.name}`);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemStatusView() });
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (err: unknown) {
      console.error(err);
      toast.error(`Failed to trigger jobs for ${layer.name}`);
    } finally {
      setLayerStates((prev) => {
        const newState = { ...prev };
        delete newState[layer.name];
        return newState;
      });
    }
  };

  const suspendLayerJobs = async (layer: DataLayer, suspend: boolean) => {
    const jobs = getJobNames(layer);
    if (jobs.length === 0) {
      toast.info(`No jobs found for layer ${layer.name}`);
      return;
    }

    const action: LayerAction = suspend ? 'stop' : 'resume';
    setLayerStates((prev) => ({
      ...prev,
      [layer.name]: { action, isLoading: true }
    }));

    try {
      toast.info(`${suspend ? 'Stopping' : 'Resuming'} ${jobs.length} jobs for ${layer.name}...`);
      const requests = jobs.map((job) =>
        suspend ? backtestApi.stopJob(job) : backtestApi.resumeJob(job)
      );
      await Promise.allSettled(requests);
      toast.success(`${suspend ? 'Stop' : 'Resume'} commands sent for ${layer.name}`);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemStatusView() });
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (err: unknown) {
      console.error(err);
      toast.error(`Failed to ${action} jobs for ${layer.name}`);
    } finally {
      setLayerStates((prev) => {
        const newState = { ...prev };
        delete newState[layer.name];
        return newState;
      });
    }
  };

  return {
    layerStates,
    triggerLayerJobs,
    suspendLayerJobs
  };
}
