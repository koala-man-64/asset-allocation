import React, { useEffect, useMemo, useState } from 'react';

import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';

import { useJobTrigger } from '@/hooks/useJobTrigger';
import type { DataLayer, JobRun } from '@/types/strategy';
import { formatSchedule, formatTimeAgo, formatTimestamp, getAzureJobExecutionsUrl, getStatusBadge, normalizeAzureJobName, normalizeAzurePortalUrl } from './SystemStatusHelpers';
import { apiService } from '@/services/apiService';

import { CalendarDays, ExternalLink, Loader2, Play, ScrollText } from 'lucide-react';

type ScheduledJobRow = {
  jobName: string;
  layerName: string;
  domainName: string;
  schedule: string;
  jobRun: JobRun | null;
};

interface ScheduledJobMonitorProps {
  dataLayers: DataLayer[];
  recentJobs: JobRun[];
  jobLinks?: Record<string, string>;
}

export function ScheduledJobMonitor({ dataLayers, recentJobs, jobLinks = {} }: ScheduledJobMonitorProps) {
  const { triggeringJob, triggerJob } = useJobTrigger();
  const [logLines, setLogLines] = useState<string[]>([]);
  const [logLoading, setLogLoading] = useState(false);
  const [logError, setLogError] = useState<string | null>(null);
  const [logJobName, setLogJobName] = useState<string | null>(null);

  const getJobPortalLink = (jobName: string) => {
    const normalizedName = normalizeAzureJobName(jobName);
    const rawDirect = jobLinks[jobName];
    const rawNormalized = normalizedName ? jobLinks[normalizedName] : undefined;
    const raw = rawDirect || rawNormalized;
    return normalizeAzurePortalUrl(raw);
  };

  const jobIndex = useMemo(() => {
    const index = new Map<string, JobRun>();
    for (const job of recentJobs || []) {
      if (!job?.jobName) continue;
      const key = normalizeAzureJobName(job.jobName);
      if (!key) continue;
      const existing = index.get(key);
      if (!existing || String(job.startTime || '') > String(existing.startTime || '')) {
        index.set(key, job);
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
        const jobKey = normalizeAzureJobName(jobName);

        const scheduleRaw = domain.cron || domain.frequency || layer.refreshFrequency || '';
        const schedule = String(scheduleRaw || '').trim() || '-';

        rows.push({
          jobName,
          layerName: layer.name,
          domainName: domain.name,
          schedule,
          jobRun: (jobKey ? jobIndex.get(jobKey) : null) ?? null,
        });
      }
    }

    rows.sort((a, b) => {
      const layerCmp = a.layerName.localeCompare(b.layerName);
      if (layerCmp !== 0) return layerCmp;
      const domainCmp = a.domainName.localeCompare(b.domainName);
      if (domainCmp !== 0) return domainCmp;
      return a.jobName.localeCompare(b.jobName);
    });

    return rows;
  }, [dataLayers, jobIndex]);

  const latestRun = useMemo(() => {
    if (!recentJobs?.length) return null;
    return recentJobs.reduce<JobRun | null>((latest, job) => {
      if (!job?.startTime) return latest;
      if (!latest?.startTime) return job;
      return new Date(job.startTime).getTime() > new Date(latest.startTime).getTime() ? job : latest;
    }, null);
  }, [recentJobs]);

  useEffect(() => {
    if (!latestRun?.jobName) {
      setLogLines([]);
      setLogLoading(false);
      setLogError(null);
      setLogJobName(null);
      return;
    }

    const controller = new AbortController();
    setLogLoading(true);
    setLogError(null);
    setLogJobName(latestRun.jobName);

    apiService
      .getJobLogs(latestRun.jobName, { runs: 1 }, controller.signal)
      .then((response) => {
        const logs = Array.isArray(response?.logs) ? response.logs : [];
        setLogLines(logs.slice(-50));
        setLogLoading(false);
      })
      .catch((error) => {
        if (controller.signal.aborted) return;
        setLogLines([]);
        setLogLoading(false);
        setLogError(error instanceof Error ? error.message : String(error));
      });

    return () => controller.abort();
  }, [latestRun?.jobName]);

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <CalendarDays className="h-5 w-5" />
            Scheduled Jobs
          </CardTitle>
          <div className="text-sm font-mono text-muted-foreground">{scheduledJobs.length}</div>
        </div>
        <CardDescription>Schedules inferred from domain cron/frequency</CardDescription>
      </CardHeader>
      <CardContent className="flex-1 overflow-auto">
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Job</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Last Start</TableHead>
                <TableHead>Schedule</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {scheduledJobs.map((job) => (
                <TableRow key={`${job.layerName}:${job.domainName}:${job.jobName}`}>
                  <TableCell className="py-2">
                    <div className="flex flex-col gap-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm">{job.jobName}</span>
                        {(() => {
                          const portalLink = getJobPortalLink(job.jobName);
                          if (!portalLink) return null;

                          const runStatus = job.jobRun?.status ? String(job.jobRun.status).toUpperCase() : 'UNKNOWN';
                          const runTimeAgo = job.jobRun?.startTime ? `${formatTimeAgo(job.jobRun.startTime)} ago` : 'UNKNOWN';

                          return (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <a
                                href={portalLink}
                                target="_blank"
                                rel="noreferrer"
                                className="text-muted-foreground hover:text-primary transition-colors"
                                aria-label={`Open ${job.jobName} in Azure`}
                              >
                                <ExternalLink className="h-3.5 w-3.5" />
                              </a>
                            </TooltipTrigger>
                            <TooltipContent side="right">
                              {job.jobRun ? `Last run: ${runStatus} • ${runTimeAgo}` : 'No recent run info'}
                            </TooltipContent>
                          </Tooltip>
                          );
                        })()}
                      </div>
                      <span className="text-xs text-muted-foreground">
                        {job.layerName} • {job.domainName}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="py-2">{getStatusBadge(job.jobRun?.status || 'unknown')}</TableCell>
                  <TableCell className="py-2 font-mono text-sm">{formatTimestamp(job.jobRun?.startTime || null)}</TableCell>
                  <TableCell className="py-2 font-mono text-sm">
                    <span className="text-slate-700" title={job.schedule}>
                      {formatSchedule(job.schedule)}
                    </span>
                  </TableCell>
                  <TableCell className="py-2 text-right">
                    <div className="flex items-center justify-end gap-1">
                      {(() => {
                        const executionsUrl = getAzureJobExecutionsUrl(getJobPortalLink(job.jobName));
                        return (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              {executionsUrl ? (
                                <Button
                                  asChild
                                  variant="ghost"
                                  size="icon"
                                  className="h-7 w-7"
                                  aria-label={`Open ${job.jobName} executions in Azure`}
                                >
                                  <a href={executionsUrl} target="_blank" rel="noreferrer">
                                    <ScrollText className="h-4 w-4" />
                                  </a>
                                </Button>
                              ) : (
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  className="h-7 w-7"
                                  disabled
                                  aria-label={`No Azure portal link for ${job.jobName}`}
                                >
                                  <ScrollText className="h-4 w-4" />
                                </Button>
                              )}
                            </TooltipTrigger>
                            <TooltipContent side="left">
                              {executionsUrl ? 'Open execution history' : 'Azure link not configured'}
                            </TooltipContent>
                          </Tooltip>
                        );
                      })()}

                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            disabled={Boolean(triggeringJob)}
                            onClick={() => void triggerJob(job.jobName)}
                            aria-label={`Run ${job.jobName}`}
                          >
                            {triggeringJob === job.jobName ? (
                              <Loader2 className="h-4 w-4 animate-spin" />
                            ) : (
                              <Play className="h-4 w-4" />
                            )}
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent side="left">Trigger job</TooltipContent>
                      </Tooltip>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
              {scheduledJobs.length === 0 && (
                <TableRow>
                  <TableCell colSpan={5} className="text-center text-muted-foreground text-sm py-4">
                    No scheduled jobs found
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>

        <div className="mt-6 space-y-4">
          <div className="flex flex-col gap-1">
            <div className="flex items-center justify-between text-sm font-semibold">
              <span>Most Recent Job Run</span>
              {latestRun?.startTime && (
                <span className="text-xs text-muted-foreground">
                  {formatTimeAgo(latestRun.startTime)} ago
                </span>
              )}
            </div>
            <div className="text-xs text-muted-foreground">
              {latestRun?.jobName ? `${latestRun.jobName} • console log (last 50 lines)` : 'No recent job runs available'}
            </div>
          </div>

          {latestRun && (
            <div className="grid gap-3 md:grid-cols-3">
              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-[11px] uppercase tracking-wider text-muted-foreground">Status</div>
                <div className="mt-2 flex items-center gap-2 text-sm">{getStatusBadge(latestRun.status)}</div>
              </div>
              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-[11px] uppercase tracking-wider text-muted-foreground">Started</div>
                <div className="mt-2 text-sm font-mono">{formatTimestamp(latestRun.startTime)}</div>
              </div>
              <div className="rounded-md border bg-muted/20 p-3">
                <div className="text-[11px] uppercase tracking-wider text-muted-foreground">Triggered By</div>
                <div className="mt-2 text-sm">{latestRun.triggeredBy || 'Schedule'}</div>
              </div>
            </div>
          )}

          <div className="rounded-md border bg-background">
            <div className="border-b px-3 py-2 text-xs font-semibold text-muted-foreground">Console Log</div>
            <div className="max-h-64 overflow-auto px-3 py-2 text-xs font-mono leading-relaxed">
              {logLoading && <div className="text-muted-foreground">Loading logs…</div>}
              {!logLoading && logError && (
                <div className="text-destructive">Failed to load logs{logJobName ? ` for ${logJobName}` : ''}: {logError}</div>
              )}
              {!logLoading && !logError && logLines.length === 0 && (
                <div className="text-muted-foreground">No log output available.</div>
              )}
              {!logLoading && !logError && logLines.length > 0 && (
                <div className="space-y-1">
                  {logLines.map((line, index) => (
                    <div key={`${logJobName ?? 'log'}-${index}`} className="whitespace-pre-wrap text-foreground/90">
                      {line}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
