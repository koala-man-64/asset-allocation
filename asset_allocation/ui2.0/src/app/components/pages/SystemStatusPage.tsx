// System Status & Health Monitoring Page

import { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Badge } from '@/app/components/ui/badge';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { DataService } from '@/services/DataService';
import { useApp } from '@/contexts/AppContext';
import { SystemHealth } from '@/types/strategy';
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
    RefreshCw
} from 'lucide-react';
import { DataLayer, JobRun, SystemAlert } from '@/types/strategy';

export function SystemStatusPage() {
    const { dataSource } = useApp();
    const [systemHealth, setSystemHealth] = useState<SystemHealth | null>(null);
    const [loading, setLoading] = useState(true);

    // Load system health based on data source
    useEffect(() => {
        async function loadSystemHealth() {
            setLoading(true);
            const data = await DataService.getSystemHealth();
            setSystemHealth(data);
            setLoading(false);
        }
        loadSystemHealth();

        // Auto-refresh every 30 seconds
        const interval = setInterval(loadSystemHealth, 30000);
        return () => clearInterval(interval);
    }, [dataSource]);

    if (loading || !systemHealth) {
        return (
            <div className="flex items-center justify-center h-64">
                <div className="text-muted-foreground">Loading system health...</div>
            </div>
        );
    }

    const { overall, dataLayers, recentJobs, alerts } = systemHealth;

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'healthy':
            case 'success':
                return <CheckCircle2 className="h-4 w-4 text-green-600" />;
            case 'stale':
            case 'warning':
                return <AlertTriangle className="h-4 w-4 text-yellow-600" />;
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
            stale: 'secondary',
            warning: 'secondary',
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

    return (
        <div className="space-y-6">
            {/* Header */}
            <div>
                <h1 className="text-2xl font-semibold">System Status & Health</h1>
                <p className="text-sm text-muted-foreground mt-1">
                    Monitor data freshness, job execution, and system alerts
                </p>
            </div>

            {/* Overall Health Summary */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
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
                        <div className="space-y-1 text-sm">
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Healthy:</span>
                                <span className="font-semibold text-green-600">{healthyLayers}</span>
                            </div>
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Stale:</span>
                                <span className="font-semibold text-yellow-600">{staleLayers}</span>
                            </div>
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Error:</span>
                                <span className="font-semibold text-red-600">{errorLayers}</span>
                            </div>
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-medium flex items-center gap-2">
                            <PlayCircle className="h-4 w-4" />
                            Recent Jobs
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-1 text-sm">
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Success:</span>
                                <span className="font-semibold text-green-600">{successJobs}</span>
                            </div>
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Running:</span>
                                <span className="font-semibold text-blue-600">{runningJobs}</span>
                            </div>
                            <div className="flex justify-between">
                                <span className="text-muted-foreground">Failed:</span>
                                <span className="font-semibold text-red-600">{failedJobs}</span>
                            </div>
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="pb-3">
                        <CardTitle className="text-sm font-medium flex items-center gap-2">
                            <AlertCircle className="h-4 w-4" />
                            Active Alerts
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-semibold">
                            {unacknowledgedAlerts.length}
                        </div>
                        <p className="text-xs text-muted-foreground mt-1">
                            {alerts.length} total alerts
                        </p>
                    </CardContent>
                </Card>
            </div>

            {/* Active Alerts */}
            {unacknowledgedAlerts.length > 0 && (
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <AlertCircle className="h-5 w-5" />
                            Active Alerts
                        </CardTitle>
                        <CardDescription>
                            Unacknowledged system alerts requiring attention
                        </CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-3">
                            {unacknowledgedAlerts.map((alert, idx) => (
                                <div key={idx} className="flex items-start gap-3 p-3 border rounded-lg">
                                    <div className="mt-0.5">{getSeverityIcon(alert.severity)}</div>
                                    <div className="flex-1 min-w-0">
                                        <div className="flex items-center gap-2 mb-1">
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
            )}

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
                                    <TableRow key={idx}>
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
                                ))}
                            </TableBody>
                        </Table>
                    </div>
                </CardContent>
            </Card>

            {/* Recent Job Runs */}
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
                                            <div className="font-medium">{job.jobName}</div>
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
                                            <div className="flex items-center gap-2">
                                                {getStatusIcon(job.status)}
                                                {getStatusBadge(job.status)}
                                            </div>
                                        </TableCell>
                                        <TableCell className="font-mono text-xs">
                                            {formatTimestamp(job.startTime)}
                                        </TableCell>
                                        <TableCell className="font-mono text-sm">
                                            {formatDuration(job.duration)}
                                        </TableCell>
                                        <TableCell className="text-right font-mono">
                                            {formatRecordCount(job.recordsProcessed)}
                                        </TableCell>
                                        <TableCell className="font-mono text-xs text-muted-foreground">
                                            {job.gitSha || '-'}
                                        </TableCell>
                                        <TableCell className="text-sm text-muted-foreground">
                                            {job.triggeredBy}
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </div>
                </CardContent>
            </Card>

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
