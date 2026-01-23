// System Status & Health Monitoring Page

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
    TrendingUp
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
        const variants: Record<string, 'default' | 'secondary' | 'destructive' | 'outline'> = {
            healthy: 'default',
            success: 'default',
            degraded: 'secondary',
            stale: 'secondary',
            warning: 'secondary',
            critical: 'destructive',
            error: 'destructive',
            failed: 'destructive',
            running: 'outline',
            pending: 'outline'
        };

        return (
            <Badge variant={variants[status] || 'outline'} className="font-mono text-xs">
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
                <h1 className="text-2xl font-semibold">System Status & Health</h1>
                <p className="text-sm text-muted-foreground mt-1">
                    Monitor data freshness, job execution, and system alerts
                </p>
            </div>

            {/* Overall Health Summary */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-medium flex items-center gap-2">
                            <Activity className="h-4 w-4" />
                            Overall Status
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="flex items-center gap-2">
                            {getStatusIcon(overall)}
                            <span className="text-2xl font-semibold capitalize">{overall}</span>
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-medium flex items-center gap-2">
                            <Database className="h-4 w-4" />
                            Data Layers
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="flex items-center gap-6 text-sm">
                            <div className="flex items-center gap-2">
                                <span className="text-muted-foreground w-16">Healthy:</span>
                                <span className="font-semibold text-green-600">{healthyLayers}</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <span className="text-muted-foreground w-16">Stale:</span>
                                <span className="font-semibold text-yellow-600">{staleLayers}</span>
                            </div>
                            <div className="flex items-center gap-2">
                                <span className="text-muted-foreground w-16">Error:</span>
                                <span className="font-semibold text-red-600">{errorLayers}</span>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>

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
                            <div className="flex gap-3 text-xs">
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
                                                    <span className="font-medium text-xs">{job.jobName}</span>
                                                    <span className="text-[10px] text-muted-foreground">{job.jobType}</span>
                                                </div>
                                            </TableCell>
                                            <TableCell className="py-2">
                                                {getStatusBadge(job.status)}
                                            </TableCell>
                                            <TableCell className="py-2 font-mono text-xs">
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
                                                <span className="text-xs font-semibold">{alert.component}</span>
                                                <span className="text-[10px] text-muted-foreground ml-auto">
                                                    {formatTimestamp(alert.timestamp)}
                                                </span>
                                            </div>
                                            <p className="text-xs text-muted-foreground line-clamp-2">{alert.message}</p>
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
                                    <TableHead>Layer</TableHead>
                                    <TableHead>Status</TableHead>
                                    <TableHead>Last Updated</TableHead>
                                    <TableHead>Version</TableHead>
                                    <TableHead className="text-right">Records</TableHead>
                                    <TableHead>Refresh Frequency</TableHead>
                                    <TableHead>Next Expected</TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {dataLayers.map((layer, idx) => (
                                    <>
                                        <TableRow key={idx} className={layer.domains?.length ? "border-b-0" : ""}>
                                            <TableCell>
                                                <div>
                                                    <div className="font-medium">{layer.name}</div>
                                                    <div className="text-xs text-muted-foreground">{layer.description}</div>
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
                                            <TableCell className="font-mono text-xs text-muted-foreground">
                                                {layer.dataVersion || '-'}
                                            </TableCell>
                                            <TableCell className="text-right font-mono">
                                                {formatRecordCount(layer.recordCount)}
                                            </TableCell>
                                            <TableCell className="text-sm text-muted-foreground">
                                                {layer.refreshFrequency}
                                            </TableCell>
                                            <TableCell className="font-mono text-xs text-muted-foreground">
                                                {layer.nextExpectedUpdate ? formatTimestamp(layer.nextExpectedUpdate) : '-'}
                                            </TableCell>
                                        </TableRow>
                                        {(layer.domains || []).map((domain: any, dIdx: number) => (
                                            <TableRow key={`${idx}-d-${dIdx}`} className="bg-muted/30 border-t-0">
                                                <TableCell className="pl-6">
                                                    <div className="relatve flex items-center gap-2">
                                                        <div className="w-2 h-px bg-border" />
                                                        <span className="text-sm text-muted-foreground capitalize">{domain.name}</span>
                                                    </div>
                                                </TableCell>
                                                <TableCell>
                                                    <div className="flex items-center gap-2 scale-90 origin-left">
                                                        {getStatusIcon(domain.status)}
                                                        <span className="text-xs text-muted-foreground uppercase tracking-wider font-mono">
                                                            {domain.status}
                                                        </span>
                                                    </div>
                                                </TableCell>
                                                <TableCell className="font-mono text-xs text-muted-foreground">
                                                    {formatTimestamp(domain.lastUpdated)}
                                                </TableCell>
                                                <TableCell colSpan={4} />
                                            </TableRow>
                                        ))}
                                    </>
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
                                                <TableCell className="font-mono text-xs">
                                                    {formatTimestamp(job.lastChecked)}
                                                </TableCell>
                                                <TableCell className="text-sm text-muted-foreground">
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
                                                <TableCell className="font-mono text-xs">
                                                    {formatTimestamp(resource.lastChecked)}
                                                </TableCell>
                                                <TableCell className="text-sm text-muted-foreground">
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
                                        <span className="text-xs text-muted-foreground">{alert.component}</span>
                                        {alert.acknowledged && (
                                            <Badge variant="outline" className="text-xs">
                                                <CheckCircle2 className="h-3 w-3 mr-1" />
                                                Acknowledged
                                            </Badge>
                                        )}
                                        <span className="text-xs text-muted-foreground ml-auto">
                                            {formatTimestamp(alert.timestamp)}
                                        </span>
                                    </div>
                                    <p className="text-sm">{alert.message}</p>
                                </div>
                            </div>
                        ))}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
