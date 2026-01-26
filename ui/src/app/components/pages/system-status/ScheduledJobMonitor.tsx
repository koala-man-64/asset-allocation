import React, { useMemo } from 'react';

import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';

import { useJobTrigger } from '@/hooks/useJobTrigger';
import type { DataLayer, JobRun } from '@/types/strategy';
import { formatTimestamp, getStatusBadge } from './SystemStatusHelpers';

import { CalendarDays, ExternalLink, Loader2, Play, ScrollText } from 'lucide-react';

type ScheduledJobRow = {
  jobName: string;
  layerName: string;
  domainName: string;
  schedule: string;
  jobRun: JobRun | null;
};
```
import React, { useMemo } from 'react';

import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';

import { useJobTrigger } from '@/hooks/useJobTrigger';
import type { DataLayer, JobRun } from '@/types/strategy';
import { formatTimestamp, getStatusBadge } from './SystemStatusHelpers';

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
  onViewJobLogs?: (jobName: string) => void;
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

        const scheduleRaw = domain.cron || domain.frequency || layer.refreshFrequency || '';
        const schedule = String(scheduleRaw || '').trim() || '-';

        rows.push({
          jobName,
          layerName: layer.name,
          domainName: domain.name,
          schedule,
          jobRun: jobIndex.get(jobName) ?? null,
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

  return (
    <Card className={`h - full flex flex - col ${ className || '' } `}>
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
                <TableRow key={`${ job.layerName }:${ job.domainName }:${ job.jobName } `}>
                  <TableCell className="py-2">
                    <div className="flex flex-col gap-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-sm">{job.jobName}</span>
                        {jobLinks[job.jobName] && (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <a
                                href={jobLinks[job.jobName]}
                                target="_blank"
                                rel="noreferrer"
                                className="text-muted-foreground hover:text-primary transition-colors"
                                aria-label={`Open ${ job.jobName } in Azure`}
                              >
                                <ExternalLink className="h-3.5 w-3.5" />
                              </a>
                            </TooltipTrigger>
                            <TooltipContent side="right">Open job</TooltipContent>
                          </Tooltip>
                        )}
                      </div>
                      <span className="text-xs text-muted-foreground">
                        {job.layerName} • {job.domainName}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="py-2">{getStatusBadge(job.jobRun?.status || 'unknown')}</TableCell>
                  <TableCell className="py-2 font-mono text-sm">{formatTimestamp(job.jobRun?.startTime || null)}</TableCell>
                  <TableCell className="py-2 font-mono text-sm">
                    <span className="text-slate-700">{job.schedule}</span>
                  </TableCell>
                  <TableCell className="py-2 text-right">
                    <div className="flex items-center justify-end gap-1">
                      {onViewJobLogs && (
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="h-7 w-7"
                              onClick={() => onViewJobLogs(job.jobName)}
                              aria-label={`View ${ job.jobName } logs`}
                            >
                              <ScrollText className="h-4 w-4" />
                            </Button>
                          </TooltipTrigger>
                          <TooltipContent side="left">View latest logs</TooltipContent>
                        </Tooltip>
                      )}

                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-7 w-7"
                            disabled={Boolean(triggeringJob)}
                            onClick={() => void triggerJob(job.jobName)}
                            aria-label={`Run ${ job.jobName } `}
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
      </CardContent>
    </Card>
  );
}
