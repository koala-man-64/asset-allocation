import React, { useEffect, useMemo, useRef, useState } from 'react';

import { Button } from '@/app/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { cn } from '@/app/components/ui/utils';

import { useJobTrigger } from '@/hooks/useJobTrigger';
import { useJobSuspend } from '@/hooks/useJobSuspend';
import type { DataLayer, JobRun } from '@/types/strategy';
import {
  formatDuration,
  formatRecordCount,
  formatSchedule,
  formatTimeAgo,
  formatTimestamp,
  getAzureJobExecutionsUrl,
  getStatusBadge,
  getStatusIcon,
  normalizeAzureJobName,
  normalizeAzurePortalUrl
} from './SystemStatusHelpers';
import { formatSystemStatusText } from './systemStatusText';
import { apiService } from '@/services/apiService';

import { CalendarDays, ChevronDown, ExternalLink, Loader2, Play, ScrollText, Square } from 'lucide-react';

const runStartEpoch = (raw?: string | null): number => {
  const value = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
};

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

type LogState = {
  lines: string[];
  loading: boolean;
  error: string | null;
  runStart: string | null;
};

type LogResponseLike = {
  logs?: Array<string | number>;
  consoleLogs?: Array<string | number>;
  runs?: Array<{
    tail?: Array<string | number>;
    consoleLogs?: Array<string | number>;
    error?: string | null;
  }>;
};

export function ScheduledJobMonitor({
  dataLayers,
  recentJobs,
  jobLinks = {}
}: ScheduledJobMonitorProps) {
  const { triggeringJob, triggerJob } = useJobTrigger();
  const { jobControl, setJobSuspended } = useJobSuspend();
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [logStateByJob, setLogStateByJob] = useState<Record<string, LogState>>({});
  const logControllers = useRef<Record<string, AbortController>>({});

  const layerStyleFor = (layerName: string) => {
    const key = String(layerName || '')
      .trim()
      .toLowerCase();
    if (key === 'bronze') {
      return {
        stripe: 'bg-mcm-walnut/90',
        headerBg: 'bg-gradient-to-r from-mcm-walnut/10 via-mcm-cream/60 to-mcm-cream/60',
        chip: 'border-mcm-walnut/30 bg-mcm-walnut/10 text-mcm-walnut',
        dot: 'bg-mcm-walnut',
        count: 'border-mcm-walnut/25 bg-mcm-paper text-mcm-walnut/80'
      };
    }
    if (key === 'silver') {
      return {
        stripe: 'bg-mcm-teal/90',
        headerBg: 'bg-gradient-to-r from-mcm-teal/12 via-mcm-cream/60 to-mcm-cream/60',
        chip: 'border-mcm-teal/35 bg-mcm-teal/12 text-mcm-teal',
        dot: 'bg-mcm-teal',
        count: 'border-mcm-teal/25 bg-mcm-paper text-mcm-walnut/80'
      };
    }
    if (key === 'gold') {
      return {
        stripe: 'bg-mcm-mustard/95',
        headerBg: 'bg-gradient-to-r from-mcm-mustard/16 via-mcm-cream/60 to-mcm-cream/60',
        chip: 'border-mcm-mustard/45 bg-mcm-mustard/18 text-mcm-walnut',
        dot: 'bg-mcm-mustard',
        count: 'border-mcm-mustard/30 bg-mcm-paper text-mcm-walnut/80'
      };
    }
    if (key === 'platinum') {
      return {
        stripe: 'bg-mcm-olive/90',
        headerBg: 'bg-gradient-to-r from-mcm-olive/12 via-mcm-cream/60 to-mcm-cream/60',
        chip: 'border-mcm-olive/35 bg-mcm-olive/12 text-mcm-olive',
        dot: 'bg-mcm-olive',
        count: 'border-mcm-olive/25 bg-mcm-paper text-mcm-walnut/80'
      };
    }

    return {
      stripe: 'bg-mcm-walnut/60',
      headerBg: 'bg-mcm-cream/60',
      chip: 'border-mcm-walnut/25 bg-mcm-paper text-mcm-walnut',
      dot: 'bg-mcm-walnut/60',
      count: 'border-mcm-walnut/20 bg-mcm-paper text-mcm-walnut/80'
    };
  };

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
      const jobStart = runStartEpoch(job.startTime);
      const existingStart = runStartEpoch(existing?.startTime);
      if (!existing || jobStart > existingStart) {
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
          jobRun: (jobKey ? jobIndex.get(jobKey) : null) ?? null
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

  const groupedJobs = useMemo(() => {
    const groups: Array<{
      key: string;
      layerName: string;
      items: ScheduledJobRow[];
    }> = [];
    const index = new Map<string, (typeof groups)[number]>();

    for (const job of scheduledJobs) {
      const key = job.layerName;
      let group = index.get(key);
      if (!group) {
        group = {
          key,
          layerName: job.layerName,
          items: []
        };
        index.set(key, group);
        groups.push(group);
      }
      group.items.push(job);
    }

    return groups;
  }, [scheduledJobs]);

  const fetchLogs = (jobName: string, runStart: string | null) => {
    logControllers.current[jobName]?.abort();
    const controller = new AbortController();
    logControllers.current[jobName] = controller;

    setLogStateByJob((prev) => ({
      ...prev,
      [jobName]: {
        lines: prev[jobName]?.lines ?? [],
        loading: true,
        error: null,
        runStart
      }
    }));

    apiService
      .getJobLogs(jobName, { runs: 1 }, controller.signal)
      .then((response) => {
        const payload = response as LogResponseLike;
        const combined = [
          ...(payload?.logs ?? []),
          ...(payload?.consoleLogs ?? []),
          ...(payload?.runs ?? []).flatMap((run) => [
            ...(run?.tail ?? []),
            ...(run?.consoleLogs ?? [])
          ])
        ]
          .filter((line) => line !== undefined && line !== null)
          .map((line) => formatSystemStatusText(line))
          .filter((line) => line.length > 0);

        const firstError = (payload?.runs ?? []).find((run) => Boolean(run?.error))?.error ?? null;
        const formattedFirstError = formatSystemStatusText(firstError);
        const logs = combined.slice(-50);
        setLogStateByJob((prev) => ({
          ...prev,
          [jobName]: {
            lines: logs,
            loading: false,
            error: logs.length === 0 && formattedFirstError ? formattedFirstError : null,
            runStart
          }
        }));
      })
      .catch((error) => {
        if (controller.signal.aborted) return;
        setLogStateByJob((prev) => ({
          ...prev,
          [jobName]: {
            lines: [],
            loading: false,
            error: formatSystemStatusText(error),
            runStart
          }
        }));
      });
  };

  useEffect(() => {
    const controllers = logControllers.current;
    return () => {
      Object.values(controllers).forEach((controller) => controller.abort());
    };
  }, []);

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <div className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <CardTitle className="flex items-center gap-2 whitespace-nowrap">
                <CalendarDays className="h-5 w-5" />
                Scheduled Jobs
              </CardTitle>
              <CardDescription className="text-sm">
                Schedules inferred from domain cron/frequency
              </CardDescription>
            </div>
            <div className="text-sm font-mono text-muted-foreground">{scheduledJobs.length}</div>
          </div>
        </div>
      </CardHeader>
      <CardContent className="flex-1 overflow-auto">
        <div className="rounded-md border overflow-hidden">
          <div className="-my-2">
            <Table className="[&_[data-slot=table-head]]:px-5 [&_[data-slot=table-cell]]:px-5">
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
                {groupedJobs.map((group) => (
                  <React.Fragment key={group.key}>
                    {(() => {
                      const style = layerStyleFor(group.layerName);
                      return (
                        <TableRow className="hover:[&>td]:bg-mcm-cream/60">
                          <TableCell
                            colSpan={5}
                            className={cn(
                              'relative py-3 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.06)]',
                              'border-mcm-walnut/35',
                              style.headerBg
                            )}
                          >
                            <span
                              aria-hidden="true"
                              className={cn(
                                'absolute inset-y-0 left-0 w-1.5 rounded-l-2xl',
                                style.stripe
                              )}
                            />
                            <div className="flex items-center justify-between gap-4">
                              <div className="flex items-center gap-3 min-w-0">
                                <span
                                  className={cn(
                                    'inline-flex items-center gap-2 rounded-full border-2 px-3 py-1 text-[10px] font-black uppercase tracking-widest',
                                    style.chip
                                  )}
                                >
                                  <span
                                    aria-hidden="true"
                                    className={cn('h-2 w-2 rounded-full', style.dot)}
                                  />
                                  {group.layerName}
                                </span>
                                <span
                                  className={cn(
                                    'hidden sm:inline text-[11px] italic text-mcm-olive'
                                  )}
                                >
                                  Medallion layer
                                </span>
                              </div>
                              <span
                                className={cn(
                                  'inline-flex items-center whitespace-nowrap rounded-full border px-3 py-1 text-[11px] font-mono tracking-tight',
                                  style.count
                                )}
                              >
                                {group.items.length} jobs
                              </span>
                            </div>
                          </TableCell>
                        </TableRow>
                      );
                    })()}
                    {group.items.map((job) => {
                      const rowKey = `${job.layerName}:${job.domainName}:${job.jobName}`;
                      const isExpanded = expandedRow === rowKey;
                      const runStart = job.jobRun?.startTime ?? null;
                      const logState = logStateByJob[job.jobName];

                      const handleToggle = () => {
                        if (!isExpanded) {
                          if (!logState || logState.runStart !== runStart) {
                            fetchLogs(job.jobName, runStart);
                          }
                        }
                        setExpandedRow(isExpanded ? null : rowKey);
                      };

                      return (
                        <React.Fragment key={rowKey}>
                          <TableRow>
                            <TableCell className="py-2">
                              <div className="flex flex-col gap-1">
                                <div className="flex items-center gap-2">
                                  <span className="font-medium text-sm">{job.jobName}</span>
                                  {(() => {
                                    const portalLink = getJobPortalLink(job.jobName);
                                    if (!portalLink) return null;

                                    const runStatus = job.jobRun?.status
                                      ? String(job.jobRun.status).toUpperCase()
                                      : 'UNKNOWN';
                                    const runTimeAgo = job.jobRun?.startTime
                                      ? `${formatTimeAgo(job.jobRun.startTime)} ago`
                                      : 'UNKNOWN';

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
                                            <ExternalLink className="h-4 w-4" />
                                          </a>
                                        </TooltipTrigger>
                                        <TooltipContent side="right">
                                          {job.jobRun
                                            ? `Last run: ${runStatus} • ${runTimeAgo}`
                                            : 'No recent run info'}
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
                            <TableCell className="py-2">
                              <div className="flex items-center gap-2">
                                {getStatusIcon(job.jobRun?.status || 'unknown')}
                                {getStatusBadge(job.jobRun?.status || 'unknown')}
                              </div>
                            </TableCell>
                            <TableCell className="py-2 font-mono text-sm">
                              {formatTimestamp(job.jobRun?.startTime || null)}
                            </TableCell>
                            <TableCell className="py-2 font-mono text-sm">
                              <span className="text-slate-700" title={job.schedule}>
                                {formatSchedule(job.schedule)}
                              </span>
                            </TableCell>
                            <TableCell className="py-2 text-right">
                              <div className="flex items-center justify-end gap-1">
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <Button
                                      variant="ghost"
                                      size="icon"
                                      className="h-7 w-7"
                                      onClick={handleToggle}
                                      aria-label={`${isExpanded ? 'Collapse' : 'Expand'} ${job.jobName} details`}
                                      aria-expanded={isExpanded}
                                    >
                                      <ChevronDown
                                        className={`h-4 w-4 transition-transform duration-300 ${isExpanded ? 'rotate-180' : ''}`}
                                      />
                                    </Button>
                                  </TooltipTrigger>
                                  <TooltipContent side="left">
                                    {isExpanded ? 'Hide details' : 'View details'}
                                  </TooltipContent>
                                </Tooltip>

                                {(() => {
                                  const executionsUrl = getAzureJobExecutionsUrl(
                                    getJobPortalLink(job.jobName)
                                  );
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
                                            <a
                                              href={executionsUrl}
                                              target="_blank"
                                              rel="noreferrer"
                                            >
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
                                        {executionsUrl
                                          ? 'Open execution history'
                                          : 'Azure link not configured'}
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
                                      disabled={Boolean(triggeringJob) || Boolean(jobControl)}
                                      onClick={() =>
                                        String(job.jobRun?.status || '').trim().toLowerCase() === 'running'
                                          ? void setJobSuspended(job.jobName, true)
                                          : void triggerJob(job.jobName)
                                      }
                                      aria-label={
                                        String(job.jobRun?.status || '').trim().toLowerCase() === 'running'
                                          ? `Stop ${job.jobName}`
                                          : `Run ${job.jobName}`
                                      }
                                    >
                                      {triggeringJob === job.jobName ? (
                                        <Loader2 className="h-4 w-4 animate-spin" />
                                      ) : String(job.jobRun?.status || '').trim().toLowerCase() === 'running' ? (
                                        <Square className="h-4 w-4" />
                                      ) : (
                                        <Play className="h-4 w-4" />
                                      )}
                                    </Button>
                                  </TooltipTrigger>
                                  <TooltipContent side="left">
                                    {String(job.jobRun?.status || '').trim().toLowerCase() === 'running'
                                      ? 'Stop job'
                                      : 'Trigger job'}
                                  </TooltipContent>
                                </Tooltip>
                              </div>
                            </TableCell>
                          </TableRow>
                          <TableRow className="border-0 hover:bg-transparent">
                            <TableCell colSpan={5} className="bg-muted/20 p-0">
                              <div
                                className={`will-change-[max-height,opacity,transform] transition-[max-height,opacity,transform] duration-450 ease-[cubic-bezier(0.34,1.56,0.64,1)] ${
                                  isExpanded
                                    ? 'max-h-[520px] opacity-100 translate-y-0 overflow-auto'
                                    : 'max-h-0 opacity-0 -translate-y-2 overflow-hidden pointer-events-none'
                                }`}
                                aria-hidden={!isExpanded}
                              >
                                <div className="space-y-4 p-4">
                                  <div className="flex items-center justify-between">
                                    <div className="text-sm font-semibold">Latest Run Details</div>
                                    {job.jobRun?.startTime && (
                                      <span className="text-xs text-muted-foreground">
                                        {formatTimeAgo(job.jobRun.startTime)} ago
                                      </span>
                                    )}
                                  </div>

                                  <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Status
                                      </div>
                                      <div className="mt-2 flex items-center gap-2 text-sm">
                                        {getStatusIcon(job.jobRun?.status || 'unknown')}
                                        {getStatusBadge(job.jobRun?.status || 'unknown')}
                                      </div>
                                    </div>
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Started
                                      </div>
                                      <div className="mt-2 text-sm font-mono">
                                        {formatTimestamp(job.jobRun?.startTime || null)}
                                      </div>
                                    </div>
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Duration
                                      </div>
                                      <div className="mt-2 text-sm font-mono">
                                        {formatDuration(job.jobRun?.duration)}
                                      </div>
                                    </div>
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Triggered By
                                      </div>
                                      <div className="mt-2 text-sm">
                                        {job.jobRun?.triggeredBy || 'Schedule'}
                                      </div>
                                    </div>
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Records
                                      </div>
                                      <div className="mt-2 text-sm font-mono">
                                        {formatRecordCount(job.jobRun?.recordsProcessed)}
                                      </div>
                                    </div>
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Git SHA
                                      </div>
                                      <div className="mt-2 text-sm font-mono">
                                        {job.jobRun?.gitSha?.substring(0, 7) || '-'}
                                      </div>
                                    </div>
                                    <div className="rounded-md border bg-muted/20 p-3">
                                      <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                                        Job Type
                                      </div>
                                      <div className="mt-2 text-sm">
                                        {job.jobRun?.jobType || '-'}
                                      </div>
                                    </div>
                                  </div>

                                  <div className="rounded-md border bg-background">
                                    <div className="border-b px-3 py-2 text-xs font-semibold text-muted-foreground">
                                      Console Logs
                                    </div>
                                    <div className="max-h-64 overflow-auto overflow-x-hidden break-words px-3 py-2 text-xs font-mono leading-relaxed">
                                      {logState?.loading && (
                                        <div className="text-muted-foreground">Loading logs…</div>
                                      )}
                                      {!logState?.loading && logState?.error && (
                                        <div className="break-words text-destructive">
                                          Failed to load logs: {logState.error}
                                        </div>
                                      )}
                                      {!logState?.loading &&
                                        !logState?.error &&
                                        (logState?.lines?.length ?? 0) === 0 && (
                                          <div className="text-muted-foreground">
                                            No log output available.
                                          </div>
                                        )}
                                      {!logState?.loading &&
                                        !logState?.error &&
                                        (logState?.lines?.length ?? 0) > 0 && (
                                          <div className="space-y-1">
                                            {(logState?.lines ?? [])
                                              .slice(-50)
                                              .map((line, index) => (
                                                <div
                                                  key={`${job.jobName}-log-${index}`}
                                                  className={`whitespace-pre-wrap break-words text-foreground/90 px-2 py-1 max-w-full ${
                                                    index % 2 === 0
                                                      ? 'bg-muted/30'
                                                      : 'bg-transparent'
                                                  }`}
                                                >
                                                  {line}
                                                </div>
                                              ))}
                                          </div>
                                        )}
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </TableCell>
                          </TableRow>
                        </React.Fragment>
                      );
                    })}
                  </React.Fragment>
                ))}
                {scheduledJobs.length === 0 && (
                  <TableRow>
                    <TableCell
                      colSpan={5}
                      className="text-center text-muted-foreground text-sm py-4"
                    >
                      No scheduled jobs found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
