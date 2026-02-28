import { useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, Loader2, ShieldAlert, ShieldCheck } from 'lucide-react';
import { toast } from 'sonner';
import { Badge } from '@/app/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Switch } from '@/app/components/ui/switch';
import { queryKeys } from '@/hooks/useDataQueries';
import { backtestApi } from '@/services/backtestApi';
import { formatSystemStatusText } from './systemStatusText';

export interface ManagedContainerJob {
  name: string;
  runningState?: string | null;
  lastModifiedAt?: string | null;
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
  const [isApplying, setIsApplying] = useState(false);
  const [optimisticChecked, setOptimisticChecked] = useState<boolean | null>(null);

  const jobNames = useMemo(
    () =>
      jobs
        .map((job) => String(job.name || '').trim())
        .filter((name) => Boolean(name)),
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

  const inferredChecked = jobNames.length > 0 && suspendedCount === jobNames.length;
  const checked = optimisticChecked ?? inferredChecked;
  const statusText = checked ? 'Kill switch is ON (jobs disabled)' : 'Kill switch is OFF (jobs enabled)';
  const inlineStatusText = checked ? 'Kill switch ON' : 'Kill switch OFF';
  const helperText =
    jobNames.length > 0
      ? 'Use only for emergency stop or maintenance windows.'
      : 'No Azure jobs were detected in the latest system-health payload.';
  useEffect(() => {
    if (optimisticChecked === null) return;
    if (optimisticChecked === inferredChecked) {
      setOptimisticChecked(null);
    }
  }, [optimisticChecked, inferredChecked]);

  useEffect(() => {
    if (optimisticChecked === null) return;
    const timeoutId = window.setTimeout(() => setOptimisticChecked(null), 45_000);
    return () => window.clearTimeout(timeoutId);
  }, [optimisticChecked]);

  const applyKillSwitch = async (nextChecked: boolean) => {
    if (isApplying || jobNames.length === 0) return;

    setIsApplying(true);
    try {
      if (nextChecked) {
        const stopSummary = await runAction(runningJobNames, 'stop');
        const suspendSummary = await runAction(jobNames, 'suspend');
        const failedCount = stopSummary.failed.length + suspendSummary.failed.length;

        if (failedCount > 0) {
          setOptimisticChecked(null);
          const failed = [...stopSummary.failed, ...suspendSummary.failed];
          toast.error(
            `Kill switch partially applied. ${failedCount} command(s) failed${formatFailureSuffix(failed)}.`
          );
        } else {
          setOptimisticChecked(true);
          toast.success(
            `Kill switch engaged. Stopped ${stopSummary.succeeded} running job(s) and suspended ${suspendSummary.succeeded} job(s).`
          );
        }
      } else {
        const resumeSummary = await runAction(jobNames, 'resume');
        if (resumeSummary.failed.length > 0) {
          setOptimisticChecked(null);
          toast.error(
            `Kill switch release partially failed. ${resumeSummary.failed.length} command(s) failed${formatFailureSuffix(resumeSummary.failed)}.`
          );
        } else {
          setOptimisticChecked(false);
          toast.success(`Kill switch disengaged. Resumed ${resumeSummary.succeeded} job(s).`);
        }
      }
    } catch (error: unknown) {
      setOptimisticChecked(null);
      toast.error(`Failed to apply kill switch: ${formatSystemStatusText(error)}`);
    } finally {
      await queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
      setIsApplying(false);
    }
  };

  if (variant === 'inline') {
    return (
      <div
        className={`flex flex-wrap items-center justify-between gap-3 rounded-2xl border-2 px-4 py-2.5 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.08)] ${
          checked
            ? 'border-rose-700/40 bg-rose-100/30'
            : 'border-mcm-walnut/15 bg-mcm-cream/55'
        }`}
      >
        <div className="min-w-0">
          <p className="text-sm font-semibold text-mcm-walnut">{inlineStatusText}</p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <Badge variant="outline" className="h-6 text-[9px]">
            {jobNames.length} job(s)
          </Badge>
          <Badge variant="outline" className="h-6 text-[9px]">
            {runningJobNames.length} running
          </Badge>
          {isApplying ? <Loader2 className="h-4 w-4 animate-spin text-mcm-walnut/65" /> : null}
          <Switch
            checked={checked}
            disabled={isApplying || jobNames.length === 0}
            onCheckedChange={(next) => {
              void applyKillSwitch(next);
            }}
            aria-label="Toggle job kill switch"
          />
        </div>
      </div>
    );
  }

  return (
    <Card className="h-full">
      <CardHeader className="gap-2">
        <CardTitle className="flex items-center gap-2">
          {checked ? <ShieldAlert className="h-5 w-5" /> : <ShieldCheck className="h-5 w-5" />}
          Container App Job Kill Switch
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border bg-muted/20 p-3">
          <div className="space-y-1">
            <p className="text-sm font-semibold">{statusText}</p>
            <p className="text-xs text-muted-foreground">{helperText}</p>
          </div>
          <div className="flex items-center gap-2">
            {isApplying ? <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" /> : null}
            <Switch
              checked={checked}
              disabled={isApplying || jobNames.length === 0}
              onCheckedChange={(next) => {
                void applyKillSwitch(next);
              }}
              aria-label="Toggle job kill switch"
            />
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">{jobNames.length} total job(s)</Badge>
          <Badge variant="outline">{runningJobNames.length} running job(s)</Badge>
          <Badge variant="outline">{suspendedCount} suspended job(s)</Badge>
        </div>

        {jobNames.length === 0 ? (
          <div className="flex items-center gap-2 rounded-md border border-amber-500/30 bg-amber-500/10 p-2 text-xs text-amber-700">
            <AlertTriangle className="h-4 w-4 shrink-0" />
            Configure `SYSTEM_HEALTH_ARM_JOBS` and ensure job resources are returned by system health.
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
