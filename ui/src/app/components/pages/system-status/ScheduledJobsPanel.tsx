import React, { useMemo, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { CalendarClock, ChevronDown, ChevronRight, Loader2, Play, ScrollText } from 'lucide-react';
import { DataLayer, JobRun } from '@/types/strategy';
import { formatDuration, formatRecordCount, formatTimeAgo, getStatusBadge, getStatusConfig } from './SystemStatusHelpers';
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
  cronText: string;
  jobName: string;
  jobLinkToken?: string;
};

export function ScheduledJobsPanel({ dataLayers, recentJobs }: ScheduledJobsPanelProps) {
  const { triggeringJob, triggerJob } = useJobTrigger();
  const [expandedKeys, setExpandedKeys] = useState<Set<string>>(() => new Set());

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
        const scheduleText = String(domain.frequency || '').trim() || '—';
        const cronText = String(domain.cron || '').trim() || '—';
        rows.push({
          key: `${layer.name}:${domain.name}:${jobName}`,
          layerName: layer.name,
          domainName: domain.name,
          scheduleText,
          cronText,
          jobName,
          jobLinkToken: domain.jobLinkToken,
        });
      }
    }
    return rows;
  }, [dataLayers]);

  const toggleExpanded = (key: string) => {
    setExpandedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <CalendarClock className="h-5 w-5" />
          Scheduled Jobs
        </CardTitle>
        <CardDescription>Most recent execution per job (expand rows for full metadata)</CardDescription>
      </CardHeader>

      <CardContent className="flex-1 overflow-auto">
        <div className="rounded-md border">
          <Table className="text-[11px]">
            <TableHeader>
              <TableRow>
                <TableHead className="w-[240px]">Domain</TableHead>
                <TableHead className="w-[120px]">Last Start</TableHead>
                <TableHead className="w-[110px]">Duration</TableHead>
                <TableHead className="w-[120px]">Job Status</TableHead>
                <TableHead className="text-right w-[110px]">Links</TableHead>
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
                const expanded = expandedKeys.has(job.key);
                const ExpandIcon = expanded ? ChevronDown : ChevronRight;

                return (
                  <React.Fragment key={job.key}>
                    <TableRow className={expanded ? 'bg-slate-50/40' : undefined}>
                      <TableCell className="py-2">
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            onClick={() => toggleExpanded(job.key)}
                            className="inline-flex h-7 w-7 items-center justify-center rounded hover:bg-slate-100 text-slate-500 focus:outline-none focus:ring-2 focus:ring-sky-500/30"
                            aria-label={`${expanded ? 'Collapse' : 'Expand'} ${job.domainName} ${job.layerName} job details`}
                          >
                            <ExpandIcon className="h-4 w-4" />
                          </button>
                          <div className="min-w-0">
                            <div className="flex items-center gap-2">
                              <span className="font-semibold text-slate-900 truncate">{job.domainName}</span>
                              <span className="font-mono text-[10px] uppercase tracking-widest text-slate-500">
                                {job.layerName}
                              </span>
                            </div>
                          </div>
                        </div>
                      </TableCell>

                      <TableCell className="py-2 font-mono text-slate-700">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="cursor-default">
                              {run?.startTime ? `${formatTimeAgo(run.startTime)} ago` : '—'}
                            </span>
                          </TooltipTrigger>
                          <TooltipContent side="left">{run?.startTime || 'No recent run timestamp'}</TooltipContent>
                        </Tooltip>
                      </TableCell>

                      <TableCell className="py-2 font-mono text-slate-700">
                        {formatDuration(run?.duration ?? null)}
                      </TableCell>

                      <TableCell className="py-2">
                        <div className="flex items-center gap-2">
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
                          <div className="min-w-0">{getStatusBadge(run?.status || 'pending')}</div>
                        </div>
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

                    {expanded && (
                      <TableRow className="bg-slate-50/40 hover:bg-slate-50/40">
                        <TableCell colSpan={5} className="py-3">
                          <div className="grid gap-3">
                            <div className="grid gap-2 md:grid-cols-4">
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Job
                                </div>
                                <div className="font-mono text-[11px] text-slate-900">{job.jobName}</div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Schedule
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {job.scheduleText} • {job.cronText}
                                </div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Triggered By
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">{run?.triggeredBy || '—'}</div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Execution
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {run?.executionName || '—'}
                                </div>
                              </div>
                            </div>

                            <div className="grid gap-2 md:grid-cols-4">
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Started
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">{run?.startTime || '—'}</div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Ended
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">{run?.endTime || '—'}</div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Duration
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {formatDuration(run?.duration ?? null)}
                                </div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Job Type
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">{run?.jobType || '—'}</div>
                              </div>
                            </div>

                            <div className="grid gap-2 md:grid-cols-4">
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Records
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {formatRecordCount(run?.recordsProcessed ?? null)}
                                </div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Git SHA
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {run?.gitSha ? run.gitSha.slice(0, 10) : '—'}
                                </div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Warnings
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {run ? run.warnings?.length ?? 0 : '—'}
                                </div>
                              </div>
                              <div>
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Errors
                                </div>
                                <div className="font-mono text-[11px] text-slate-700">
                                  {run ? run.errors?.length ?? 0 : '—'}
                                </div>
                              </div>
                            </div>

                            {run?.details && (
                              <div className="rounded border bg-white px-3 py-2 text-[11px] text-slate-700">
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Details
                                </div>
                                <div className="mt-1 font-mono">{run.details}</div>
                              </div>
                            )}

                            {(run?.errors?.length || run?.warnings?.length) && (
                              <div className="rounded border bg-white px-3 py-2 text-[11px]">
                                <div className="text-[10px] font-mono uppercase tracking-widest text-slate-500">
                                  Messages
                                </div>
                                <div className="mt-1 space-y-1 font-mono">
                                  {(run.errors || []).map((message, idx) => (
                                    <div key={`err-${idx}`} className="text-red-700">
                                      ERR {message}
                                    </div>
                                  ))}
                                  {(run.warnings || []).map((message, idx) => (
                                    <div key={`warn-${idx}`} className="text-amber-700">
                                      WRN {message}
                                    </div>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    )}
                  </React.Fragment>
                );
              })}

              {scheduledJobs.length === 0 && (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-muted-foreground text-sm py-6">
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
