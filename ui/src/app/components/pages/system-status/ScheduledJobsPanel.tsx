import React, { useMemo } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { CalendarClock, Loader2, Play, ScrollText } from 'lucide-react';
import { DataLayer, JobRun } from '@/types/strategy';
import { formatTimeAgo, getStatusConfig } from './SystemStatusHelpers';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import { openSystemLink } from '@/utils/openSystemLink';

interface ScheduledJobsPanelProps {
  dataLayers: DataLayer[];
  recentJobs: JobRun[];
}

type ScheduledJobRow = {
  key: string;
  layerName: string;
  domainName: string;
  scheduleText: string;
  jobName: string;
  jobLinkToken?: string;
};

export function ScheduledJobsPanel({ dataLayers, recentJobs }: ScheduledJobsPanelProps) {
  const { triggeringJob, triggerJob } = useJobTrigger();

  const recentJobIndex = useMemo(() => {
    const index = new Map<string, JobRun>();
    for (const job of recentJobs || []) {
      if (!job?.jobName) continue;
      const existing = index.get(job.jobName);
      if (!existing || String(job.startTime || '') > String(existing.startTime || '')) {
        index.set(job.jobName, job);
      }
    }
    return index;
  }, [recentJobs]);

  const scheduledJobs = useMemo(() => {
    const rows: ScheduledJobRow[] = [];
    for (const layer of dataLayers || []) {
      for (const domain of layer.domains || []) {
        const jobName = String(domain.jobName || '').trim();
        if (!jobName) continue;
        const scheduleText = String(domain.frequency || domain.cron || '').trim() || '—';
        rows.push({
          key: `${layer.name}:${domain.name}:${jobName}`,
          layerName: layer.name,
          domainName: domain.name,
          scheduleText,
          jobName,
          jobLinkToken: domain.jobLinkToken,
        });
      }
    }
    return rows;
  }, [dataLayers]);

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <CalendarClock className="h-5 w-5" />
          Scheduled Jobs
        </CardTitle>
        <CardDescription>Schedule + last execution status</CardDescription>
      </CardHeader>

      <CardContent className="flex-1 overflow-auto">
        <div className="rounded-md border">
          <Table className="text-[11px]">
            <TableHeader>
              <TableRow>
                <TableHead>Domain</TableHead>
                <TableHead>Schedule</TableHead>
                <TableHead className="text-center w-[80px]">Status</TableHead>
                <TableHead className="text-right w-[110px]">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {scheduledJobs.map((job) => {
                const run = recentJobIndex.get(job.jobName);
                const statusConfig = getStatusConfig(run?.status || 'pending');
                const StatusIcon = statusConfig.icon;
                const statusAnim =
                  statusConfig.animation === 'spin'
                    ? 'animate-spin'
                    : statusConfig.animation === 'pulse'
                      ? 'animate-pulse'
                      : '';
                const lastRunText = run ? `${run.status.toUpperCase()} • ${formatTimeAgo(run.startTime)} ago` : 'NO RECENT RUN';
                const isTriggering = triggeringJob === job.jobName;

                return (
                  <TableRow key={job.key}>
                    <TableCell className="py-2">
                      <div className="flex items-center gap-2">
                        <span className="font-semibold text-slate-900">{job.domainName}</span>
                        <span className="font-mono text-[10px] uppercase tracking-widest text-slate-500">
                          {job.layerName}
                        </span>
                      </div>
                    </TableCell>
                    <TableCell className="py-2 font-mono text-slate-600">
                      {job.scheduleText}
                    </TableCell>
                    <TableCell className="py-2 text-center">
                      <Tooltip>
                        <TooltipTrigger asChild>
                          {job.jobLinkToken ? (
                            <button
                              type="button"
                              onClick={() => void openSystemLink(job.jobLinkToken!)}
                              className="inline-flex h-7 w-7 items-center justify-center rounded hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-sky-500/30"
                              aria-label={`Open job (${lastRunText})`}
                            >
                              <StatusIcon
                                className={`h-4 w-4 ${statusAnim}`}
                                style={{ color: statusConfig.text }}
                              />
                            </button>
                          ) : (
                            <span
                              className="inline-flex h-7 w-7 items-center justify-center rounded opacity-40 cursor-not-allowed"
                              aria-label={`Job link not configured (${lastRunText})`}
                            >
                              <StatusIcon
                                className={`h-4 w-4 ${statusAnim}`}
                                style={{ color: statusConfig.text }}
                              />
                            </span>
                          )}
                        </TooltipTrigger>
                        <TooltipContent side="left">{lastRunText}</TooltipContent>
                      </Tooltip>
                    </TableCell>
                    <TableCell className="py-2 text-right">
                      <div className="inline-flex items-center justify-end gap-1">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            {run?.logLinkToken ? (
                              <button
                                type="button"
                                onClick={() => void openSystemLink(run.logLinkToken!)}
                                className="p-1 hover:bg-slate-100 text-slate-500 hover:text-sky-600 rounded"
                                aria-label={`Open ${job.domainName} last run logs`}
                              >
                                <ScrollText className="h-4 w-4" />
                              </button>
                            ) : (
                              <span
                                className="p-1 text-slate-300 rounded cursor-not-allowed"
                                aria-label={`No log link for ${job.domainName}`}
                              >
                                <ScrollText className="h-4 w-4" />
                              </span>
                            )}
                          </TooltipTrigger>
                          <TooltipContent side="left">
                            {run?.logLinkToken ? 'Open last run logs' : 'Last run log link not available'}
                          </TooltipContent>
                        </Tooltip>

                        <Tooltip>
                          <TooltipTrigger asChild>
                            <button
                              type="button"
                              onClick={() => void triggerJob(job.jobName)}
                              disabled={Boolean(triggeringJob)}
                              className="p-1 hover:bg-slate-100 text-slate-500 hover:text-emerald-600 disabled:opacity-30 disabled:cursor-not-allowed rounded"
                              aria-label={`Trigger ${job.domainName} job`}
                            >
                              {isTriggering ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <Play className="h-4 w-4" />
                              )}
                            </button>
                          </TooltipTrigger>
                          <TooltipContent side="left">
                            {isTriggering ? 'Triggering job…' : 'Trigger job'}
                          </TooltipContent>
                        </Tooltip>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}

              {scheduledJobs.length === 0 && (
                <TableRow>
                  <TableCell colSpan={4} className="text-center text-muted-foreground text-sm py-6">
                    No scheduled jobs found
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}

