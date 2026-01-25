import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Button } from '@/app/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { Activity, Clock, ExternalLink, Loader2, Play } from 'lucide-react';
import { formatTimestamp, getAzurePortalUrl, getStatusBadge, getStatusIcon } from './SystemStatusHelpers';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import { ResourceHealth, ResourceSignal } from '@/types/strategy';

interface AzureResourcesProps {
    resources: ResourceHealth[];
}

export function AzureResources({ resources }: AzureResourcesProps) {
    const { triggeringJob, triggerJob } = useJobTrigger();
    const jobs = resources.filter(r => r.resourceType === 'Microsoft.App/jobs');
    const infra = resources.filter(r => r.resourceType !== 'Microsoft.App/jobs');
    const showAzureResources = resources.length > 0;

    if (!showAzureResources) return null;

    const formatSignals = (signals?: ResourceSignal[]) => {
        if (!signals || signals.length === 0) return '';
        return signals
            .slice(0, 2)
            .map(signal => {
                const value = signal.value === null || signal.value === undefined ? 'n/a' : signal.value;
                const unit = signal.unit ? ` ${signal.unit}` : '';
                return `${signal.name}=${value}${unit}`;
            })
            .join(' | ');
    };

    return (
        <div className="space-y-6">
            {/* Scheduled Jobs Status */}
            {jobs.length > 0 && (
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Clock className="h-5 w-5" />
                            Scheduled Jobs Status
                        </CardTitle>
                        <CardDescription>
                            Configuration and health status of background jobs
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="rounded-md border">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>Job Name</TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead>Last Checked</TableHead>
                                        <TableHead>Details</TableHead>
                                        <TableHead className="text-right">Run</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {jobs.map((job, idx) => (
                                        <TableRow key={idx}>
                                            <TableCell className="font-medium">
                                                <div className="flex items-center gap-2">
                                                    <span>{job.name}</span>
                                                    {job.azureId && (
                                                        <Tooltip>
                                                            <TooltipTrigger asChild>
                                                                <a
                                                                    href={getAzurePortalUrl(job.azureId)}
                                                                    target="_blank"
                                                                    rel="noreferrer"
                                                                    className="text-muted-foreground hover:text-primary transition-colors"
                                                                    aria-label={`Open ${job.name} in Azure`}
                                                                >
                                                                    <ExternalLink className="h-3.5 w-3.5" />
                                                                </a>
                                                            </TooltipTrigger>
                                                            <TooltipContent side="right">Open job</TooltipContent>
                                                        </Tooltip>
                                                    )}
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center gap-2">
                                                    {getStatusIcon(job.status)}
                                                    {getStatusBadge(job.status)}
                                                </div>
                                            </TableCell>
                                            <TableCell className="font-mono text-sm">
                                                {formatTimestamp(job.lastChecked)}
                                            </TableCell>
                                            <TableCell className="text-base text-muted-foreground">
                                                <div>{job.details || '-'}</div>
                                                {formatSignals(job.signals) && (
                                                    <div className="mt-1 text-xs font-mono text-muted-foreground/80">
                                                        {formatSignals(job.signals)}
                                                    </div>
                                                )}
                                            </TableCell>
                                            <TableCell className="text-right">
                                                <Tooltip>
                                                    <TooltipTrigger asChild>
                                                        <Button
                                                            variant="ghost"
                                                            size="icon"
                                                            className="h-7 w-7"
                                                            disabled={Boolean(triggeringJob)}
                                                            onClick={() => void triggerJob(job.name)}
                                                            aria-label={`Run ${job.name}`}
                                                        >
                                                            {triggeringJob === job.name ? (
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
                                </TableBody>
                            </Table>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Azure Resource Health (Other) */}
            {infra.length > 0 && (
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Activity className="h-5 w-5" />
                            Azure Infrastructure Health
                        </CardTitle>
                        <CardDescription>
                            Control-plane status for container apps and other resources
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="rounded-md border">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>Name</TableHead>
                                        <TableHead>Type</TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead>Last Checked</TableHead>
                                        <TableHead>Details</TableHead>
                                        <TableHead>Azure ID</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {infra.map((resource, idx) => (
                                        <TableRow key={idx}>
                                            <TableCell className="font-medium">
                                                <div className="flex items-center gap-2">
                                                    <span>{resource.name}</span>
                                                    {resource.azureId && (
                                                        <Tooltip>
                                                            <TooltipTrigger asChild>
                                                                <a
                                                                    href={getAzurePortalUrl(resource.azureId)}
                                                                    target="_blank"
                                                                    rel="noreferrer"
                                                                    className="text-muted-foreground hover:text-primary transition-colors"
                                                                    aria-label={`Open ${resource.name} in Azure`}
                                                                >
                                                                    <ExternalLink className="h-3.5 w-3.5" />
                                                                </a>
                                                            </TooltipTrigger>
                                                            <TooltipContent side="right">Open resource</TooltipContent>
                                                        </Tooltip>
                                                    )}
                                                </div>
                                            </TableCell>
                                            <TableCell className="text-sm text-muted-foreground">
                                                {resource.resourceType}
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center gap-2">
                                                    {getStatusIcon(resource.status)}
                                                    {getStatusBadge(resource.status)}
                                                </div>
                                            </TableCell>
                                            <TableCell className="font-mono text-sm">
                                                {formatTimestamp(resource.lastChecked)}
                                            </TableCell>
                                            <TableCell className="text-base text-muted-foreground">
                                                <div>{resource.details || '-'}</div>
                                                {formatSignals(resource.signals) && (
                                                    <div className="mt-1 text-xs font-mono text-muted-foreground/80">
                                                        {formatSignals(resource.signals)}
                                                    </div>
                                                )}
                                            </TableCell>
                                            <TableCell className="font-mono text-xs text-muted-foreground max-w-[240px] truncate">
                                                {resource.azureId ? (
                                                    <a
                                                        href={getAzurePortalUrl(resource.azureId)}
                                                        target="_blank"
                                                        rel="noreferrer"
                                                        className="hover:text-primary transition-colors"
                                                        title={resource.azureId}
                                                    >
                                                        {resource.azureId}
                                                    </a>
                                                ) : (
                                                    '-'
                                                )}
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </div>
                    </CardContent>
                </Card>
            )}
        </div>
    );
}
