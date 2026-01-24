import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { PlayCircle } from 'lucide-react';
import { getStatusBadge, formatTimestamp } from './SystemStatusHelpers';

interface Job {
    jobName: string;
    jobType: string;
    status: string;
    startTime: string;
}

interface JobMonitorProps {
    recentJobs: Job[];
}

export function JobMonitor({ recentJobs }: JobMonitorProps) {
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
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {recentJobs.slice(0, 5).map((job, idx) => (
                                <TableRow key={idx}>
                                    <TableCell className="py-2">
                                        <div className="flex flex-col">
                                            <span className="font-medium text-sm">{job.jobName}</span>
                                            <span className="text-xs text-muted-foreground">{job.jobType}</span>
                                        </div>
                                    </TableCell>
                                    <TableCell className="py-2">
                                        {getStatusBadge(job.status)}
                                    </TableCell>
                                    <TableCell className="py-2 font-mono text-sm">
                                        {formatTimestamp(job.startTime)}
                                    </TableCell>
                                </TableRow>
                            ))}
                            {recentJobs.length === 0 && (
                                <TableRow>
                                    <TableCell colSpan={3} className="text-center text-muted-foreground text-sm py-4">
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
