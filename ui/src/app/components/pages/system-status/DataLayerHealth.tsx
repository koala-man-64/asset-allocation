import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Database, ExternalLink, Folder, PlayCircle, Clock } from 'lucide-react';
import { getStatusIcon, getStatusBadge, formatTimestamp } from './SystemStatusHelpers';
import { DataDomain, DataLayer, JobRun } from '@/types/strategy';

interface DataLayerHealthProps {
    dataLayers: DataLayer[];
    recentJobs: JobRun[];
}

function extractAzureJobName(jobUrl?: string | null): string | null {
    if (!jobUrl) return null;
    const match = jobUrl.match(/\/jobs\/([^/?#]+)/);
    if (!match) return null;
    try {
        return decodeURIComponent(match[1]);
    } catch {
        return match[1];
    }
}

export function DataLayerHealth({ dataLayers, recentJobs }: DataLayerHealthProps) {
    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <Database className="h-5 w-5" />
                    Data Layer Freshness
                </CardTitle>
                <CardDescription>
                    Monitor the last update time and status of all data layers
                </CardDescription>
            </CardHeader>
            <CardContent>
                <div className="rounded-md border">
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead title="The name of the data layer (Bronze, Silver, Gold, etc.)">Layer</TableHead>
                                <TableHead title="Current health status (Healthy, Stale, Error)">Status</TableHead>
                                <TableHead title="Timestamp of the most recent update to any dataset in this layer">Last Updated</TableHead>
                                <TableHead title="How often this layer is expected to update">Refresh Frequency</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {dataLayers.map((layer, idx) => (
                                <React.Fragment key={idx}>
                                    <TableRow className={layer.domains?.length ? "border-b-0" : ""}>
                                        <TableCell>
                                            <div>
                                                <div className="font-medium flex items-center gap-2 text-base">
                                                    {layer.name}
                                                    {layer.portalUrl && (
                                                        <a
                                                            href={layer.portalUrl}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            className="text-muted-foreground hover:text-primary transition-colors"
                                                            title="View Container in Azure Portal"
                                                        >
                                                            <ExternalLink className="h-4.5 w-4.5" />
                                                        </a>
                                                    )}
                                                </div>
                                                <div className="text-sm text-muted-foreground">{layer.description}</div>
                                            </div>
                                        </TableCell>
                                        <TableCell>
                                            <div className="flex items-center gap-2">
                                                {getStatusIcon(layer.status)}
                                                {getStatusBadge(layer.status)}
                                            </div>
                                        </TableCell>
                                        <TableCell className="font-mono text-sm">
                                            {formatTimestamp(layer.lastUpdated)}
                                        </TableCell>
                                        <TableCell className="text-sm text-muted-foreground">
                                            {layer.refreshFrequency}
                                        </TableCell>
                                    </TableRow>
                                    {(layer.domains || []).map((domain: DataDomain, dIdx: number) => {
                                        const jobName = extractAzureJobName(domain.jobUrl);
                                        const latestJob = jobName ? recentJobs.find(j => j.jobName === jobName) : null;

                                        return (
                                            <TableRow key={`${idx}-d-${dIdx}`} className="bg-muted/30 border-t-0 group">
                                                <TableCell className="pl-6">
                                                    <div className="relative flex items-start gap-2">
                                                        <div className="w-2 h-px bg-border flex-shrink-0 mt-3" />
                                                        <div className="flex flex-col min-w-0 gap-1">
                                                            <span className="text-sm text-muted-foreground capitalize font-medium">
                                                                {domain.name}
                                                            </span>
                                                            <div className="flex flex-col gap-1 text-xs text-muted-foreground/80">
                                                                <div className="flex items-center gap-1.5 min-w-0">
                                                                    <Folder className="h-3.5 w-3.5 flex-shrink-0" />
                                                                    {domain.portalUrl ? (
                                                                        <a
                                                                            href={domain.portalUrl}
                                                                            target="_blank"
                                                                            rel="noopener noreferrer"
                                                                            className="hover:text-primary transition-colors min-w-0"
                                                                            title="View folder in Azure Storage"
                                                                        >
                                                                            <span className="font-mono truncate block" title={domain.path}>
                                                                                {domain.path || '-'}
                                                                            </span>
                                                                        </a>
                                                                    ) : (
                                                                        <span className="font-mono truncate block" title={domain.path}>
                                                                            {domain.path || '-'}
                                                                        </span>
                                                                    )}
                                                                </div>
                                                                <div className="flex items-center gap-1.5 min-w-0">
                                                                    <PlayCircle className="h-3.5 w-3.5 flex-shrink-0" />
                                                                    {domain.jobUrl ? (
                                                                        <a
                                                                            href={domain.jobUrl}
                                                                            target="_blank"
                                                                            rel="noopener noreferrer"
                                                                            className="hover:text-primary transition-colors min-w-0"
                                                                            title="View domain job in Azure Portal"
                                                                        >
                                                                            <span className="font-mono truncate block" title={jobName || domain.jobUrl}>
                                                                                {jobName || 'job'}
                                                                            </span>
                                                                        </a>
                                                                    ) : (
                                                                        <span className="font-mono truncate block" title={jobName || undefined}>
                                                                            {jobName || '-'}
                                                                        </span>
                                                                    )}
                                                                </div>
                                                            </div>
                                                            {domain.description && (
                                                                <span className="text-xs text-muted-foreground/70">{domain.description}</span>
                                                            )}
                                                        </div>
                                                    </div>
                                                </TableCell>
                                                <TableCell>
                                                    <div className="flex items-center gap-2">
                                                        {getStatusIcon(domain.status)}
                                                        <span className="text-sm text-muted-foreground uppercase tracking-wider font-mono">
                                                            {domain.status}
                                                        </span>
                                                    </div>
                                                </TableCell>
                                                <TableCell className="font-mono text-sm text-muted-foreground">
                                                    {formatTimestamp(domain.lastUpdated)}
                                                </TableCell>
                                                <TableCell className="text-sm text-muted-foreground">
                                                    <div className="flex flex-col gap-1">
                                                        <div className="font-mono text-sm">{domain.frequency || domain.cron || '-'}</div>
                                                        {latestJob && (
                                                            <div className="flex items-center gap-2 text-xs text-muted-foreground/80 mt-1">
                                                                <div className="flex items-center gap-1" title={`Job: ${latestJob.jobName}`}>
                                                                    <span className="opacity-70">Job:</span>
                                                                    <span className={
                                                                        latestJob.status === 'success' ? 'text-green-600' :
                                                                            latestJob.status === 'failed' ? 'text-red-600' : ''
                                                                    }>
                                                                        {latestJob.status}
                                                                    </span>
                                                                </div>
                                                                <div className="hidden xl:flex items-center gap-1">
                                                                    <Clock className="h-3 w-3" />
                                                                    {formatTimestamp(latestJob.startTime)}
                                                                </div>
                                                            </div>
                                                        )}
                                                    </div>
                                                </TableCell>
                                            </TableRow>
                                        );
                                    })}
                                </React.Fragment>
                            ))}
                        </TableBody>
                    </Table>
                </div>
            </CardContent>
        </Card>
    );
}
