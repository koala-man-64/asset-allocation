import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { BarChart3, Calendar, Clock, Database, DollarSign, ExternalLink, Folder, PlayCircle, Target, TrendingUp, Zap } from 'lucide-react';
import { getStatusIcon, getStatusBadge, formatTimestamp } from './SystemStatusHelpers';
import { DataDomain, DataLayer, JobRun, TradingSignal } from '@/types/strategy';
import { openSystemLink } from '@/utils/openSystemLink';

interface DataLayerHealthProps {
    dataLayers: DataLayer[];
    recentJobs: JobRun[];
    impactsByDomain?: Record<string, string[]>;
    signals?: TradingSignal[];
}

function getDomainIcon(domain: DataDomain) {
    const name = (domain.name || '').toLowerCase();
    if (name.includes('market')) return TrendingUp;
    if (name.includes('finance')) return DollarSign;
    if (name.includes('earnings')) return Calendar;
    if (name.includes('price') || name.includes('target')) return Target;
    if (name.includes('ranking')) return BarChart3;
    if (name.includes('signal')) return Zap;
    return Folder;
}

function computeFreshness(domain: DataDomain): { ageSeconds?: number; behindSeconds?: number } {
    if (!domain.lastUpdated) return {};
    const updatedMs = Date.parse(domain.lastUpdated);
    if (Number.isNaN(updatedMs)) return {};
    const ageSeconds = Math.max(0, Math.floor((Date.now() - updatedMs) / 1000));
    const maxAge = domain.maxAgeSeconds;
    if (typeof maxAge !== 'number' || !Number.isFinite(maxAge)) return { ageSeconds };
    const behindSeconds = Math.max(0, ageSeconds - Math.max(0, Math.floor(maxAge)));
    return { ageSeconds, behindSeconds };
}

function formatCompactDuration(totalSeconds: number): string {
    const seconds = Math.max(0, Math.floor(totalSeconds));
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    const remMinutes = minutes % 60;
    return remMinutes ? `${hours}h ${remMinutes}m` : `${hours}h`;
}

export function DataLayerHealth({ dataLayers, recentJobs, impactsByDomain, signals }: DataLayerHealthProps) {
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
                                                    {layer.portalLinkToken && (
                                                        <button
                                                            type="button"
                                                            onClick={() => void openSystemLink(layer.portalLinkToken!)}
                                                            className="text-muted-foreground hover:text-primary transition-colors"
                                                            title="View Container in Azure Portal"
                                                        >
                                                            <ExternalLink className="h-4.5 w-4.5" />
                                                        </button>
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
                                        const jobName = domain.jobName;
                                        const latestJob = jobName ? recentJobs.find(j => j.jobName === jobName) : null;
                                        const { behindSeconds } = computeFreshness(domain);
                                        const DomainIcon = getDomainIcon(domain);
                                        const impactedStrategies =
                                            domain.status !== 'healthy'
                                                ? impactsByDomain?.[domain.name] || []
                                                : [];
                                        const buySignals =
                                            domain.status !== 'healthy' && signals
                                                ? signals.filter(
                                                    (s) =>
                                                        s.signalType === 'BUY' &&
                                                        impactedStrategies.includes(String(s.strategyName || s.strategyId || '')),
                                                ).length
                                                : 0;

                                        return (
                                            <TableRow key={`${idx}-d-${dIdx}`} className="bg-muted/30 border-t-0 group">
                                                <TableCell className="pl-6">
                                                    <div className="relative flex items-start gap-2">
                                                        <div className="w-2 h-px bg-border flex-shrink-0 mt-3" />
                                                        <div className="flex flex-col min-w-0 gap-1">
                                                            <span className="text-sm text-muted-foreground capitalize font-medium flex items-center gap-2">
                                                                <DomainIcon className="h-4 w-4 text-muted-foreground/70" />
                                                                {domain.name}
                                                            </span>
                                                            <div className="flex flex-col gap-1 text-xs text-muted-foreground/80">
                                                                <div className="flex items-center gap-1.5 min-w-0">
                                                                    <Folder className="h-3.5 w-3.5 flex-shrink-0" />
                                                                    {domain.portalLinkToken ? (
                                                                        <button
                                                                            type="button"
                                                                            onClick={() => void openSystemLink(domain.portalLinkToken!)}
                                                                            className="hover:text-primary transition-colors min-w-0"
                                                                            title="View folder in Azure Storage"
                                                                        >
                                                                            <span className="font-mono truncate block" title={domain.path}>
                                                                                {domain.path || '-'}
                                                                            </span>
                                                                        </button>
                                                                    ) : (
                                                                        <span className="font-mono truncate block" title={domain.path}>
                                                                            {domain.path || '-'}
                                                                        </span>
                                                                    )}
                                                                </div>
                                                                <div className="flex items-center gap-1.5 min-w-0">
                                                                    <PlayCircle className="h-3.5 w-3.5 flex-shrink-0" />
                                                                    {domain.jobLinkToken ? (
                                                                        <button
                                                                            type="button"
                                                                            onClick={() => void openSystemLink(domain.jobLinkToken!)}
                                                                            className="hover:text-primary transition-colors min-w-0"
                                                                            title="View domain job in Azure Portal"
                                                                        >
                                                                            <span className="font-mono truncate block" title={jobName || undefined}>
                                                                                {jobName || 'job'}
                                                                            </span>
                                                                        </button>
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
                                                            {impactedStrategies.length > 0 && (
                                                                <div className="flex flex-wrap items-center gap-2 text-xs mt-1">
                                                                    <span className="text-muted-foreground/80">
                                                                        Impact: {impactedStrategies.slice(0, 3).join(', ')}
                                                                        {impactedStrategies.length > 3 ? ` +${impactedStrategies.length - 3}` : ''}
                                                                    </span>
                                                                    {buySignals > 0 && (
                                                                        <span className="text-emerald-600 font-medium">
                                                                            BUY signals: {buySignals}
                                                                        </span>
                                                                    )}
                                                                </div>
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
                                                        {typeof behindSeconds === 'number' && (
                                                            <div className="flex items-center gap-1 text-xs">
                                                                <Clock className="h-3 w-3" />
                                                                <span className={behindSeconds > 0 ? 'text-orange-600' : 'text-muted-foreground/80'}>
                                                                    {behindSeconds > 0
                                                                        ? `${formatCompactDuration(behindSeconds)} behind SLA`
                                                                        : 'On time'}
                                                                </span>
                                                            </div>
                                                        )}
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
