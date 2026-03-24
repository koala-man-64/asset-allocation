import { useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { queryKeys } from '@/hooks/useDataQueries';
import { backtestApi } from '@/services/backtestApi';
import type { ResourceSignal } from '@/types/strategy';
import { formatSystemStatusText } from './systemStatusText';

export interface ManagedContainerJob {
  name: string;
  runningState?: string | null;
  lastModifiedAt?: string | null;
  signals?: ResourceSignal[] | null;
}

type KillSwitchVariant = 'panel' | 'inline';

type JobAction = 'stop' | 'suspend' | 'resume';

type ActionSummary = {
  total: number;
  succeeded: number;
  failed: string[];
};

function normalizeState(value?: string | null): string {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function isRunningState(value?: string | null): boolean {
  const normalized = normalizeState(value);
  if (!normalized) return false;
  if (normalized.includes('suspend')) return false;
  if (normalized.includes('stop')) return false;
  return normalized.includes('run') || normalized.includes('start');
}

function isSuspendedState(value?: string | null): boolean {
  const normalized = normalizeState(value);
  if (!normalized) return false;
  return normalized.includes('suspend') || normalized.includes('disable');
}

async function runAction(jobNames: string[], action: JobAction): Promise<ActionSummary> {
  if (jobNames.length === 0) {
    return { total: 0, succeeded: 0, failed: [] };
  }

  const requests = jobNames.map((jobName) => {
    if (action === 'stop') return backtestApi.stopJob(jobName);
    if (action === 'suspend') return backtestApi.suspendJob(jobName);
    return backtestApi.resumeJob(jobName);
  });

  const results = await Promise.allSettled(requests);
  const failed: string[] = [];
  let succeeded = 0;
  results.forEach((result, index) => {
    if (result.status === 'fulfilled') {
      succeeded += 1;
      return;
    }
    failed.push(jobNames[index]);
  });

  return {
    total: jobNames.length,
    succeeded,
    failed
  };
}

function formatFailureSuffix(failed: string[]): string {
  if (failed.length === 0) return '';
  const preview = failed.slice(0, 3).join(', ');
  if (!preview) return '';
  return failed.length > 3 ? ` (${preview}, +${failed.length - 3} more)` : ` (${preview})`;
}

function confirmMessage(action: JobAction, count: number): string {
  if (action === 'stop') return `Stop ${count} running job(s)?`;
  if (action === 'suspend') return `Suspend ${count} job(s)?`;
  return `Resume ${count} job(s)?`;
}

function successMessage(action: JobAction, succeeded: number): string {
  if (action === 'stop') return `Stopped ${succeeded} running job(s).`;
  if (action === 'suspend') return `Suspended ${succeeded} job(s).`;
  return `Resumed ${succeeded} job(s).`;
}

function failurePrefix(action: JobAction): string {
  if (action === 'stop') return 'Stop';
  if (action === 'suspend') return 'Suspend';
  return 'Resume';
}

export function JobKillSwitchPanel({ jobs }: { jobs: ManagedContainerJob[] }) {
  return <KillSwitchControl jobs={jobs} variant="panel" />;
}

export function JobKillSwitchInline({ jobs }: { jobs: ManagedContainerJob[] }) {
  return <KillSwitchControl jobs={jobs} variant="inline" />;
}

function KillSwitchControl({
  jobs,
  variant
}: {
  jobs: ManagedContainerJob[];
  variant: KillSwitchVariant;
}) {
  const queryClient = useQueryClient();
  const [isApplyingAction, setIsApplyingAction] = useState<JobAction | null>(null);

  const jobNames = useMemo(
    () => jobs.map((job) => String(job.name || '').trim()).filter((name) => Boolean(name)),
    [jobs]
  );

  const runningJobNames = useMemo(
    () =>
      jobs
        .filter((job) => isRunningState(job.runningState))
        .map((job) => String(job.name || '').trim())
        .filter((name) => Boolean(name)),
    [jobs]
  );

  const suspendedCount = useMemo(
    () => jobs.filter((job) => isSuspendedState(job.runningState)).length,
    [jobs]
  );

  const statusText =
    suspendedCount === jobNames.length && jobNames.length > 0
      ? 'All detected jobs are suspended.'
      : runningJobNames.length > 0
        ? `${runningJobNames.length} job(s) currently running.`
        : 'No detected jobs are actively running.';
  const helperText =
    jobNames.length > 0
      ? 'Use imperative maintenance actions instead of a persistent kill-switch state.'
      : 'No Azure jobs were detected in the latest system-health payload.';

  useEffect(() => {
    if (isApplyingAction === null) return;
    const timeoutId = window.setTimeout(() => setIsApplyingAction(null), 45_000);
    return () => window.clearTimeout(timeoutId);
  }, [isApplyingAction]);

  const applyAction = async (action: JobAction) => {
    if (isApplyingAction || jobNames.length === 0) return;

    const targetJobNames = action === 'stop' ? runningJobNames : jobNames;
    if (targetJobNames.length === 0) {
      toast.error(
        action === 'stop'
          ? 'No running jobs are available to stop.'
          : action === 'suspend'
            ? 'No jobs are available to suspend.'
            : 'No jobs are available to resume.'
      );
      return;
    }

    if (!window.confirm(confirmMessage(action, targetJobNames.length))) {
      return;
    }

    setIsApplyingAction(action);
    try {
      const summary = await runAction(targetJobNames, action);
      if (summary.failed.length > 0) {
        toast.error(
          `${failurePrefix(action)} partially failed. ${summary.failed.length} command(s) failed${formatFailureSuffix(summary.failed)}.`
        );
      } else {
        toast.success(successMessage(action, summary.succeeded));
      }
    } catch (error: unknown) {
      toast.error(
        `Failed to ${action === 'stop' ? 'stop running jobs' : action === 'suspend' ? 'suspend jobs' : 'resume jobs'}: ${formatSystemStatusText(error)}`
      );
    } finally {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.systemStatusView() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() })
      ]);
      setIsApplyingAction(null);
    }
  };

  const actionButtons = (
    <>
      <Button
        type="button"
        variant="outline"
        disabled={Boolean(isApplyingAction) || runningJobNames.length === 0}
        onClick={() => {
          void applyAction('stop');
        }}
      >
        {isApplyingAction === 'stop' ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
        Stop running jobs
      </Button>
      <Button
        type="button"
        variant="outline"
        disabled={Boolean(isApplyingAction) || jobNames.length === 0}
        onClick={() => {
          void applyAction('suspend');
        }}
      >
        {isApplyingAction === 'suspend' ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
        Suspend all jobs
      </Button>
      <Button
        type="button"
        disabled={Boolean(isApplyingAction) || jobNames.length === 0}
        onClick={() => {
          void applyAction('resume');
        }}
      >
        {isApplyingAction === 'resume' ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
        Resume all jobs
      </Button>
    </>
  );

  if (variant === 'inline') {
    return (
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border-2 border-mcm-walnut/15 bg-mcm-cream/55 px-4 py-2.5 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.08)]">
        <div className="min-w-0">
          <p className="text-sm font-semibold text-mcm-walnut">{statusText}</p>
        </div>
        <div className="flex flex-wrap items-center gap-2 shrink-0">
          <Badge variant="outline" className="h-6 text-[9px]">
            {jobNames.length} job(s)
          </Badge>
          <Badge variant="outline" className="h-6 text-[9px]">
            {runningJobNames.length} running
          </Badge>
          {actionButtons}
        </div>
      </div>
    );
  }

  return (
    <Card className="h-full">
      <CardHeader className="gap-2">
        <CardTitle>Container App Job Controls</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border bg-muted/20 p-3">
          <div className="space-y-1">
            <p className="text-sm font-semibold">{statusText}</p>
            <p className="text-xs text-muted-foreground">{helperText}</p>
          </div>
          <div className="flex flex-wrap items-center gap-2">{actionButtons}</div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">{jobNames.length} total job(s)</Badge>
          <Badge variant="outline">{runningJobNames.length} running job(s)</Badge>
          <Badge variant="outline">{suspendedCount} suspended job(s)</Badge>
        </div>

        {jobNames.length === 0 ? (
          <div className="flex items-center gap-2 rounded-md border border-amber-500/30 bg-amber-500/10 p-2 text-xs text-amber-700">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            Configure `SYSTEM_HEALTH_ARM_JOBS` and ensure job resources are returned by system
            health.
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
