import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Button } from '@/app/components/ui/button';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { AlertTriangle, Loader2, Play, PlayCircle, XCircle } from 'lucide-react';
import { getJobTypeIcon, getStatusBadge, formatDuration, formatTimestamp, formatRecordCount } from './SystemStatusHelpers';
import { useJobTrigger } from '@/hooks/useJobTrigger';

// Use same interface as backend/strategy.ts if possible, or define here
// Based on recent monolith analysis:
interface Job {
    jobName: string;
    jobType: string;
    status: string;
    startTime: string;
    duration?: number;
    recordsProcessed?: number; // Monolith used recordsProcessed
    recordCount?: number;      // Old component used recordCount
    gitSha?: string;
    triggeredBy?: string;      // Monolith used triggeredBy
    trigger?: string;          // Old component used trigger
    errors?: string[];
    warnings?: string[];
}

interface JobExecutionHistoryProps {
    recentJobs: Job[];
}

export function JobExecutionHistory({ recentJobs }: JobExecutionHistoryProps) {
    const { triggeringJob, triggerJob } = useJobTrigger();

    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <PlayCircle className="h-5 w-5" />
                    Recent Job Executions
                </CardTitle>
                <CardDescription>
                    Last 10 job runs across all pipeline components
                </CardDescription>
            </CardHeader>
            <CardContent>
                <div className="rounded-md border">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Job Name</TableHead>
                                <TableHead>Type</TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead>Started</TableHead>
                                <TableHead>Duration</TableHead>
                                <TableHead className="text-right">Records</TableHead>
                                <TableHead>Git SHA</TableHead>
                                <TableHead>Triggered By</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {recentJobs.map((job, idx) => (
                                <TableRow key={idx}>
                                    <TableCell>
                                        <div className="flex items-start justify-between gap-2">
                                            <div className="font-medium">{job.jobName}</div>
                                            <Tooltip>
                                                <TooltipTrigger asChild>
                                                    <Button
                                                        variant="ghost"
                                                        size="icon"
                                                        className="h-7 w-7"
                                                        disabled={Boolean(triggeringJob)}
                                                        onClick={() => triggerJob(job.jobName)}
                                                        aria-label={`Run ${job.jobName}`}
                                                    >
                                                        {triggeringJob === job.jobName ? (
                                                            <Loader2 className="h-4 w-4 animate-spin" />
                                                        ) : (
                                                            <Play className="h-4 w-4" />
                                                        )}
                                                    </Button>
                                                </TooltipTrigger>
                                                <TooltipContent side="left">Run job</TooltipContent>
                                            </Tooltip>
                                        </div>
                                        {(job.errors || job.warnings) && (
                                            <div className="text-xs mt-1 space-y-0.5">
                                                {job.errors?.map((err, i) => (
                                                    <div key={i} className="text-red-600 flex items-start gap-1">
                                                        <XCircle className="h-3 w-3 mt-0.5 flex-shrink-0" />
                                                        <span>{err}</span>
                                                    </div>
                                                ))}
                                                {job.warnings?.map((warn, i) => (
                                                    <div key={i} className="text-yellow-600 flex items-start gap-1">
                                                        <AlertTriangle className="h-3 w-3 mt-0.5 flex-shrink-0" />
                                                        <span>{warn}</span>
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </TableCell>
                                    <TableCell>
                                        <div className="flex items-center gap-2">
                                            {getJobTypeIcon(job.jobType)}
                                            <span className="text-sm">{job.jobType}</span>
                                        </div>
                                    </TableCell>
                                    <TableCell>
                                        {getStatusBadge(job.status)}
                                    </TableCell>
                                    <TableCell className="font-mono text-sm">
                                        {formatTimestamp(job.startTime)}
                                    </TableCell>
                                    <TableCell className="font-mono text-sm">
                                        {formatDuration(job.duration)}
                                    </TableCell>
                                    <TableCell className="text-right font-mono text-sm">
                                        {formatRecordCount(job.recordsProcessed ?? job.recordCount)}
                                    </TableCell>
                                    <TableCell className="font-mono text-xs text-muted-foreground">
                                        {job.gitSha?.substring(0, 7) || '-'}
                                    </TableCell>
                                    <TableCell className="text-sm text-muted-foreground">
                                        {job.triggeredBy ?? job.trigger ?? 'Schedule'}
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </div>
            </CardContent>
        </Card>
    );
}
