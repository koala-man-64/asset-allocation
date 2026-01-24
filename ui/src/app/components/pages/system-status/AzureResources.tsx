import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Activity, Clock } from 'lucide-react';
import { getStatusIcon, getStatusBadge, formatTimestamp } from './SystemStatusHelpers';

interface AzureResource {
    name: string;
    resourceType: string;
    status: string;
    lastChecked: string;
    details?: string;
    azureId?: string;
}

interface AzureResourcesProps {
    resources: AzureResource[];
}

export function AzureResources({ resources }: AzureResourcesProps) {
    const jobs = resources.filter(r => r.resourceType === 'Microsoft.App/jobs');
    const infra = resources.filter(r => r.resourceType !== 'Microsoft.App/jobs');
    const showAzureResources = resources.length > 0;

    if (!showAzureResources) return null;

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
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {jobs.map((job, idx) => (
                                        <TableRow key={idx}>
                                            <TableCell className="font-medium">{job.name}</TableCell>
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
                                                {job.details || '-'}
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
                                            <TableCell className="font-medium">{resource.name}</TableCell>
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
                                                {resource.details || '-'}
                                            </TableCell>
                                            <TableCell className="font-mono text-xs text-muted-foreground max-w-[240px] truncate">
                                                {resource.azureId || '-'}
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
