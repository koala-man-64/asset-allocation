
import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Button } from '@/app/components/ui/button';
import { useLiveSystemHealthQuery } from '@/hooks/useDataQueries';
import { useAuth } from '@/contexts/AuthContext';
import { ApiError } from '@/services/backtestApi';
import {
    Activity,
    Database,
    CheckCircle2,
    AlertTriangle,
    XCircle,
    Clock,
    PlayCircle,
    Loader2,
    AlertCircle,
    Info,
    TrendingUp,
    ExternalLink,
    Folder,
    Zap
} from 'lucide-react';

export function SystemStatusPage() {
    const auth = useAuth();
    const { data: systemHealth, isLoading: loading, error } = useLiveSystemHealthQuery();

    if (loading) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="text-muted-foreground">Loading system health...</div>
            </div>
        );
    }

    if (error && !systemHealth) {
        const isUnauthorized = error instanceof ApiError && error.status === 401;

        return (
            <div className="space-y-4">
                <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4 flex items-start gap-3">
                    <XCircle className="h-5 w-5 text-destructive mt-0.5 flex-shrink-0" />
                    <div className="text-base text-destructive">
                        <p className="font-semibold">{isUnauthorized ? 'Authentication required' : 'Live Data Connection Error'}</p>
                        <p className="text-sm opacity-90 mt-1">
                            {isUnauthorized
                                ? 'The Backtest API rejected this request. Configure auth (API key or OIDC) and try again.'
                                : (error as any).message || 'Failed to connect to live system health API.'}
                        </p>
                        {isUnauthorized && auth.enabled && !auth.authenticated && (
                            <div className="mt-3">
                                <Button variant="secondary" onClick={auth.signIn}>
                                    Sign in
                                </Button>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        );
    }

    if (!systemHealth) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="text-muted-foreground">System health unavailable.</div>
            </div>
        );
    }

    const { overall, dataLayers, recentJobs, alerts, resources = [] } = systemHealth;

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'healthy':
            case 'success':
                return <CheckCircle2 className="h-4 w-4 text-green-600" />;
            case 'degraded':
            case 'stale':
            case 'warning':
                return <AlertTriangle className="h-4 w-4 text-yellow-600" />;
            case 'critical':
            case 'error':
            case 'failed':
                return <XCircle className="h-4 w-4 text-red-600" />;
            case 'running':
                return <Loader2 className="h-4 w-4 text-blue-600 animate-spin" />;
            case 'pending':
                return <Clock className="h-4 w-4 text-gray-400" />;
            default:
                return <AlertCircle className="h-4 w-4 text-gray-400" />;
        }
    };

    const getStatusBadge = (status: string) => {
        const styles: Record<string, string> = {
            healthy: 'bg-green-100 text-green-800 hover:bg-green-100 border-green-200',
            success: 'bg-green-100 text-green-800 hover:bg-green-100 border-green-200',
            degraded: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100 border-yellow-200',
            stale: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100 border-yellow-200',
            warning: 'bg-yellow-100 text-yellow-800 hover:bg-yellow-100 border-yellow-200',
            critical: 'bg-red-100 text-red-800 hover:bg-red-100 border-red-200',
            error: 'bg-red-100 text-red-800 hover:bg-red-100 border-red-200',
            failed: 'bg-red-100 text-red-800 hover:bg-red-100 border-red-200',
            running: 'bg-blue-100 text-blue-800 hover:bg-blue-100 border-blue-200',
            pending: 'bg-gray-100 text-gray-800 hover:bg-gray-100 border-gray-200'
        };

        return (
            <Badge variant="outline" className={`font-mono text-xs border ${styles[status] || 'bg-gray-100 text-gray-800 border-gray-200'}`}>
                {status.toUpperCase()}
            </Badge>
        );
    };

    const getSeverityIcon = (severity: string) => {
        switch (severity) {
            case 'critical':
                return <XCircle className="h-4 w-4 text-red-600" />;
            case 'error':
                return <AlertCircle className="h-4 w-4 text-red-500" />;
            case 'warning':
                return <AlertTriangle className="h-4 w-4 text-yellow-600" />;
            case 'info':
                return <Info className="h-4 w-4 text-blue-600" />;
            default:
                return <Info className="h-4 w-4 text-gray-400" />;
        }
    };

    const formatDuration = (seconds?: number) => {
        if (!seconds) return '-';
        const mins = Math.floor(seconds / 60);
        const secs = seconds % 60;
        return `${mins}m ${secs}s`;
    };

    const formatTimestamp = (timestamp: string) => {
        const date = new Date(timestamp);
        const now = new Date();
        const diffMs = now.getTime() - date.getTime();
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMins / 60);
        const diffDays = Math.floor(diffHours / 24);

        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        if (diffDays < 7) return `${diffDays}d ago`;
        return date.toLocaleDateString();
    };

    const formatRecordCount = (count?: number) => {
        if (!count) return '-';
        if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
        if (count >= 1_000) return `${(count / 1_000).toFixed(1)}K`;
        return count.toString();
    };

    const getJobTypeIcon = (jobType: string) => {
        switch (jobType) {
            case 'backtest':
                return <TrendingUp className="h-4 w-4" />;
            case 'data-ingest':
                return <Database className="h-4 w-4" />;
            case 'attribution':
                return <Activity className="h-4 w-4" />;
            case 'risk-calc':
                return <AlertTriangle className="h-4 w-4" />;
            case 'portfolio-build':
                return <PlayCircle className="h-4 w-4" />;
            default:
                return <Activity className="h-4 w-4" />;
        }
    };

    const healthyLayers = dataLayers.filter(l => l.status === 'healthy').length;
    const staleLayers = dataLayers.filter(l => l.status === 'stale').length;
    const errorLayers = dataLayers.filter(l => l.status === 'error').length;

    const successJobs = recentJobs.filter(j => j.status === 'success').length;
    const runningJobs = recentJobs.filter(j => j.status === 'running').length;
    const failedJobs = recentJobs.filter(j => j.status === 'failed').length;

    const unacknowledgedAlerts = alerts.filter(a => !a.acknowledged);
    const showAzureResources = resources.length > 0;

    return (
        <div className="space-y-6">
            {/* Error Banner */}
            {error && (
                <div className="bg-destructive/10 border border-destructive/20 rounded-lg p-4 flex items-start gap-3">
                    <XCircle className="h-5 w-5 text-destructive mt-0.5 flex-shrink-0" />
                    <div className="text-base text-destructive">
                        <p className="font-semibold">Live Data Connection Error</p>
                        <p className="text-sm opacity-90 mt-1">
                            {(error as any).message || 'Failed to connect to live system health API'}. Metrics and tables will remain empty until connection is established.
                        </p>
                    </div>
                </div>
            )}

            {/* Header */}
            <div>
                <h1 className="text-3xl font-bold">System Status & Health</h1>
                <p className="text-base text-muted-foreground mt-1">
                    Monitor data freshness, job execution, and system alerts
                </p>
            </div>

            {/* Unified System Overview */}
            <Card>
                <CardHeader className="pb-4">
                    <CardTitle className="flex items-center gap-2">
                        <Activity className="h-5 w-5" />
                        System Health Overview
                    </CardTitle>
                    <CardDescription>
                        Real-time status of data layers and pipeline jobs
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                        {/* Overall Status Metrics */}
                        <div className="flex flex-col justify-center gap-4">
                            <div className="flex flex-col items-center justify-center p-4 bg-muted/20 rounded-lg border border-muted h-full">
                                <div className="scale-150 mb-4">{getStatusIcon(overall)}</div>
                                <div className="text-4xl font-extrabold capitalize mb-1">{overall}</div>
                                <p className="text-base text-muted-foreground text-center">System Operational Status</p>
                            </div>
                        </div>

                        {/* Detailed Layer Status */}
                        <div className="lg:col-span-2 grid gap-4">
                            {dataLayers.map((layer, idx) => (
                                <div key={idx} className="flex flex-col p-2 border rounded-md hover:bg-muted/50 transition-colors gap-2">
                                    {/* Layer Header */}
                                    <div className="flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            {getStatusIcon(layer.status)}
                                            <div>
                                                <div className="font-bold text-base flex items-center gap-2">
                                                    {layer.name}
                                                    <div className="flex items-center gap-1 border-l pl-2 ml-1 opacity-70">
                                                        {(layer.domains || []).map((domain: any, dIdx: number) => (
                                                            <div key={dIdx} title={`${domain.name}: ${domain.status}`}>
                                                                {getStatusIcon(domain.status)}
                                                            </div>
                                                        ))}
                                                    </div>

                                                    <div className="flex items-center gap-1.5 ml-2">
                                                        {layer.portalUrl && (
                                                            <a href={layer.portalUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="View Azure Container">
                                                                <Database className="h-4 w-4" />
                                                            </a>
                                                        )}
                                                        {layer.jobUrl && (
                                                            <a href={layer.jobUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="View Pipeline Job">
                                                                <PlayCircle className="h-4 w-4" />
                                                            </a>
                                                        )}
                                                        {layer.triggerUrl && (
                                                            <a href={layer.triggerUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="View Logic App Trigger">
                                                                <Zap className="h-4 w-4" />
                                                            </a>
                                                        )}
                                                    </div>
                                                </div>
                                                <div className="text-xs text-muted-foreground">
                                                    Updated: {formatTimestamp(layer.lastUpdated)}
                                                </div>
                                            </div>
                                        </div>
                                        {getStatusBadge(layer.status)}
                                    </div>

                                    {/* Domains List */}
                                    <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-1 pl-7">
                                        {(layer.domains || []).map((domain: any, dIdx: number) => {
                                            const jName = domain.jobUrl?.split('/jobs/')[1]?.split('/')[0];
                                            const job = jName ? recentJobs.find((j: any) => j.jobName === jName) : null;

                                            return (
                                                <div key={dIdx} className="flex items-center justify-between text-sm bg-muted/30 p-2 rounded">
                                                    <div className="flex items-center gap-2">
                                                        {getStatusIcon(domain.status)}
                                                        <span className="font-medium">{domain.name}</span>
                                                        <div className="flex items-center gap-1 ml-1 opacity-50">
                                                            {domain.portalUrl && (
                                                                <a href={domain.portalUrl} target="_blank" rel="noopener noreferrer" className="hover:text-blue-500 transition-colors" title="View Azure Resource">
                                                                    <Database className="h-3.5 w-3.5" />
                                                                </a>
                                                            )}
                                                            {domain.jobUrl && (
                                                                <a href={domain.jobUrl} target="_blank" rel="noopener noreferrer" className="hover:text-blue-500 transition-colors" title="View Domain Job">
                                                                    <PlayCircle className="h-3.5 w-3.5" />
                                                                </a>
                                                            )}
                                                            {domain.triggerUrl && (
                                                                <a href={domain.triggerUrl} target="_blank" rel="noopener noreferrer" className="hover:text-blue-500 transition-colors" title="Trigger Domain Logic">
                                                                    <Zap className="h-3.5 w-3.5" />
                                                                </a>
                                                            )}
                                                        </div>
                                                    </div>

                                                    {job ? (
                                                        <div className="flex items-center gap-1.5" title={`Job: ${job.jobName} (${job.status})`}>
                                                            <div className={`h-2 w-2 rounded-full ${job.status === 'success' ? 'bg-green-500' :
                                                                job.status === 'failed' ? 'bg-red-500' :
                                                                    job.status === 'running' ? 'bg-blue-500 animate-pulse' : 'bg-gray-300'
                                                                }`} />
                                                            <span className="text-xs text-muted-foreground capitalize">{job.status}</span>
                                                        </div>
                                                    ) : (
                                                        <span className="text-xs text-muted-foreground italic opacity-50">No job</span>
                                                    )}
                                                </div>
                                            );
                                        })}
                                        {(!layer.domains || layer.domains.length === 0) && (
                                            <div className="text-[10px] text-muted-foreground italic col-span-full">No domains configured</div>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                </CardContent>
            </Card>

            {/* Split View: Recent Jobs & Alerts */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {/* Recent Job Executions */}
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

                {/* Active Alerts */}
                <Card className="h-full flex flex-col">
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <CardTitle className="flex items-center gap-2">
                                <AlertCircle className="h-5 w-5" />
                                Active Alerts
                            </CardTitle>
                            {unacknowledgedAlerts.length > 0 && (
                                <Badge variant="destructive">
                                    {unacknowledgedAlerts.length} Active
                                </Badge>
                            )}
                        </div>
                        <CardDescription>
                            System alerts requiring attention
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="flex-1 overflow-auto">
                        {unacknowledgedAlerts.length > 0 ? (
                            <div className="space-y-3">
                                {unacknowledgedAlerts.map((alert, idx) => (
                                    <div key={idx} className="flex items-start gap-3 p-3 border rounded-lg">
                                        <div className="mt-0.5">{getSeverityIcon(alert.severity)}</div>
                                        <div className="flex-1 min-w-0">
                                            <div className="flex items-center gap-2 mb-1">
                                                <span className="text-sm font-semibold">{alert.component}</span>
                                                <span className="text-xs text-muted-foreground ml-auto">
                                                    {formatTimestamp(alert.timestamp)}
                                                </span>
                                            </div>
                                            <p className="text-sm text-muted-foreground line-clamp-2">{alert.message}</p>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        ) : (
                            <div className="flex flex-col items-center justify-center h-32 text-muted-foreground">
                                <CheckCircle2 className="h-8 w-8 mb-2 opacity-20" />
                                <p className="text-sm">No active alerts</p>
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>



            {/* Data Layer Freshness */}
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
                                        {(layer.domains || []).map((domain: any, dIdx: number) => {
                                            const jobName = domain.jobUrl ? domain.jobUrl.split('/jobs/')[1]?.split('/')[0] : null;
                                            const latestJob = jobName ? recentJobs.find(j => j.jobName === jobName) : null;

                                            return (
                                                <TableRow key={`${idx}-d-${dIdx}`} className="bg-muted/30 border-t-0 group">
                                                    <TableCell className="pl-6">
                                                        <div className="relatve flex items-center gap-2">
                                                            <div className="w-2 h-px bg-border flex-shrink-0" />
                                                            <div className="flex flex-col">
                                                                <span className="text-sm text-muted-foreground capitalize font-medium">{domain.name}</span>
                                                                {domain.description && (
                                                                    <span className="text-xs text-muted-foreground/70">{domain.description}</span>
                                                                )}
                                                            </div>
                                                            <div className="flex items-center gap-1 ml-2 opacity-0 group-hover:opacity-100 transition-opacity">
                                                                {domain.portalUrl && (
                                                                    <a
                                                                        href={domain.portalUrl}
                                                                        target="_blank"
                                                                        rel="noopener noreferrer"
                                                                        className="text-muted-foreground hover:text-primary transition-colors"
                                                                        title="View Folder in Azure Storage"
                                                                    >
                                                                        <Folder className="h-4.5 w-4.5" />
                                                                    </a>
                                                                )}
                                                                {domain.jobUrl && (
                                                                    <a
                                                                        href={domain.jobUrl}
                                                                        target="_blank"
                                                                        rel="noopener noreferrer"
                                                                        className="text-muted-foreground hover:text-primary transition-colors"
                                                                        title="View Job in Azure Portal"
                                                                    >
                                                                        <PlayCircle className="h-4.5 w-4.5" />
                                                                    </a>
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

            {/* Scheduled Jobs Status */}
            {resources.filter(r => r.resourceType === 'Microsoft.App/jobs').length > 0 && (
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
                                    {resources
                                        .filter(r => r.resourceType === 'Microsoft.App/jobs')
                                        .map((job, idx) => (
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
            {showAzureResources && resources.some(r => r.resourceType !== 'Microsoft.App/jobs') && (
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
                                    {resources
                                        .filter(r => r.resourceType !== 'Microsoft.App/jobs')
                                        .map((resource, idx) => (
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


            {/* All Alerts History */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <AlertCircle className="h-5 w-5" />
                        Alert History
                    </CardTitle>
                    <CardDescription>
                        Complete history of system alerts and notifications
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="space-y-2">
                        {alerts.map((alert, idx) => (
                            <div
                                key={idx}
                                className={`flex items-start gap-3 p-3 border rounded-lg ${alert.acknowledged ? 'opacity-50' : ''
                                    }`}
                            >
                                <div className="mt-0.5">{getSeverityIcon(alert.severity)}</div>
                                <div className="flex-1 min-w-0">
                                    <div className="flex items-center gap-2 mb-1 flex-wrap">
                                        <Badge variant={
                                            alert.severity === 'critical' || alert.severity === 'error'
                                                ? 'destructive'
                                                : alert.severity === 'warning'
                                                    ? 'secondary'
                                                    : 'outline'
                                        }>
                                            {alert.severity.toUpperCase()}
                                        </Badge>
                                        <span className="text-sm text-muted-foreground">{alert.component}</span>
                                        {alert.acknowledged && (
                                            <Badge variant="outline" className="text-sm">
                                                <CheckCircle2 className="h-3.5 w-3.5 mr-1" />
                                                Acknowledged
                                            </Badge>
                                        )}
                                        <span className="text-sm text-muted-foreground ml-auto">
                                            {formatTimestamp(alert.timestamp)}
                                        </span>
                                    </div>
                                    <p className="text-base">{alert.message}</p>
                                </div>
                            </div>
                        ))}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
