import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Button } from '@/app/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { ExternalLink, Loader2, Play, PlayCircle } from 'lucide-react';
import { formatDuration, formatRecordCount, formatTimestamp, getStatusBadge } from './SystemStatusHelpers';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import { JobRun } from '@/types/strategy';
import { openSystemLink } from '@/utils/openSystemLink';

interface JobMonitorProps {
    recentJobs: JobRun[];
    jobLinkTokens?: Record<string, string>;
}

export function JobMonitor({ recentJobs, jobLinkTokens = {} }: JobMonitorProps) {
    const { triggeringJob, triggerJob } = useJobTrigger();
    const successJobs = recentJobs.filter(j => j.status === 'success').length;
    const runningJobs = recentJobs.filter(j => j.status === 'running').length;
    const failedJobs = recentJobs.filter(j => j.status === 'failed').length;

    return (
        <Card className="h-full flex flex-col">
            <CardHeader>
                <div className="flex items-center justify-between">
                    <CardTitle className="flex items-center gap-2">
                        <PlayCircle className="h-5 w-5" />
                        Recent Jobs
                    </CardTitle>
                    <div className="flex gap-3 text-sm">
                        <span className="flex items-center gap-1">
                            <div className="h-2 w-2 rounded-full bg-green-500" />
                            {successJobs}
                        </span>
                        <span className="flex items-center gap-1">
                            <div className="h-2 w-2 rounded-full bg-blue-500" />
                            {runningJobs}
                        </span>
                        <span className="flex items-center gap-1">
                            <div className="h-2 w-2 rounded-full bg-red-500" />
                            {failedJobs}
                        </span>
                    </div>
                </div>
                <CardDescription>
                    Execution history (last {recentJobs.length})
                </CardDescription>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto">
                <div className="rounded-md border">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Job</TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead>Time</TableHead>
                                <TableHead className="text-right">Actions</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {recentJobs.slice(0, 5).map((job, idx) => (
                                <TableRow key={idx}>
                                    <TableCell className="py-2">
                                        <div className="flex flex-col gap-1">
                                            <div className="flex items-center gap-2">
                                                <span className="font-medium text-sm">{job.jobName}</span>
                                                {jobLinkTokens[job.jobName] && (
                                                    <Tooltip>
                                                        <TooltipTrigger asChild>
                                                            <button
                                                                type="button"
                                                                onClick={() => void openSystemLink(jobLinkTokens[job.jobName])}
                                                                className="text-muted-foreground hover:text-primary transition-colors"
                                                                aria-label={`Open ${job.jobName} in Azure`}
                                                            >
                                                                <ExternalLink className="h-3.5 w-3.5" />
                                                            </button>
                                                        </TooltipTrigger>
                                                        <TooltipContent side="right">Open job</TooltipContent>
                                                    </Tooltip>
                                                )}
                                            </div>
                                            <span className="text-xs text-muted-foreground">{job.jobType}</span>
                                            <div className="flex flex-wrap gap-2 text-[11px] text-muted-foreground">
                                                <span>Trigger: {job.triggeredBy || 'schedule'}</span>
                                                <span>Duration: {formatDuration(job.duration)}</span>
                                                {job.recordsProcessed !== undefined && (
                                                    <span>Records: {formatRecordCount(job.recordsProcessed)}</span>
                                                )}
                                            </div>
                                        </div>
                                    </TableCell>
                                    <TableCell className="py-2">
                                        {getStatusBadge(job.status)}
                                    </TableCell>
                                    <TableCell className="py-2 font-mono text-sm">
                                        <div>{formatTimestamp(job.startTime)}</div>
                                    </TableCell>
                                    <TableCell className="py-2 text-right">
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
                                    </TableCell>
                                </TableRow>
                            ))}
                            {recentJobs.length === 0 && (
                                <TableRow>
                                    <TableCell colSpan={4} className="text-center text-muted-foreground text-sm py-4">
                                        No recent jobs found
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
