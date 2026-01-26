import React, { useMemo } from 'react';
import { DataLayer, JobRun } from '@/types/strategy';
import { formatTimeAgo, getStatusConfig } from './SystemStatusHelpers';
import { StatusTypos, StatusColors } from './StatusTokens';
import { CalendarDays, Database, FolderOpen, Loader2, Play, ScrollText, ExternalLink } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { useJobTrigger } from '@/hooks/useJobTrigger';

interface StatusOverviewProps {
    overall: string;
    dataLayers: DataLayer[];
    recentJobs: JobRun[];
}

export function StatusOverview({ overall, dataLayers, recentJobs }: StatusOverviewProps) {
    const sysConfig = getStatusConfig(overall);
    const apiAnim = sysConfig.animation === 'spin' ? 'animate-spin' :
        sysConfig.animation === 'pulse' ? 'animate-pulse' : '';
    const { triggeringJob, triggerJob } = useJobTrigger();

    const domainNames = useMemo(() => {
        const names: string[] = [];
        const seen = new Set<string>();

        for (const layer of dataLayers) {
            for (const domain of layer.domains || []) {
                if (!domain?.name) continue;
                if (seen.has(domain.name)) continue;
                seen.add(domain.name);
                names.push(domain.name);
            }
        }
        return names;
    }, [dataLayers]);

    const jobIndex = useMemo(() => {
        const index = new Map<string, JobRun>();
        for (const job of recentJobs) {
            if (!job.jobName) continue;
            const existing = index.get(job.jobName);
            if (!existing || String(job.startTime || '') > String(existing.startTime || '')) {
                index.set(job.jobName, job);
            }
        }
        return index;
    }, [recentJobs]);

    return (
        <div className="space-y-4 font-sans">
            {/* Header / Legend Block */}
            <div className="flex flex-col gap-4">
                <div className="flex items-center justify-between p-4 border rounded-none border-l-4"
                    style={{
                        backgroundColor: StatusColors.PANEL_BG,
                        borderColor: StatusColors.PANEL_BORDER,
                        borderLeftColor: sysConfig.text
                    }}>
                    <div className="flex items-center gap-4">
                        <sysConfig.icon className={`h-8 w-8 ${apiAnim}`}
                            style={{ color: sysConfig.text }} />
                        <div>
                            <h1 className={StatusTypos.HEADER}>SYSTEM STATUS</h1>
                            <div className="text-2xl font-black tracking-tighter uppercase"
                                style={{ color: sysConfig.text }}>
                                {overall}
                            </div>
                        </div>
                    </div>
                    <div className="text-right">
                        <div className={StatusTypos.HEADER}>UPTIME CLOCK</div>
                        <div className={`${StatusTypos.MONO} text-xl text-slate-500`}>
                            {new Date().toISOString().split('T')[1].split('.')[0]} UTC
                        </div>
                    </div>
                </div>
            </div>

            {/* Main Status Table */}
            <div className="border border-slate-200 rounded-sm overflow-hidden">
                <Table>
                    <TableHeader className="bg-slate-50">
                        <TableRow className="hover:bg-transparent border-slate-200">
                            <TableHead className="w-[200px] text-xs font-bold tracking-wider text-slate-500 uppercase">Layer / Domain</TableHead>
                            <TableHead className="w-[120px] text-xs font-bold tracking-wider text-slate-500 uppercase">Status</TableHead>
                            <TableHead className="text-xs font-bold tracking-wider text-slate-500 uppercase">Path / Resource</TableHead>
                            <TableHead className="w-[180px] text-xs font-bold tracking-wider text-slate-500 uppercase">Job</TableHead>
                            <TableHead className="w-[100px] text-end text-xs font-bold tracking-wider text-slate-500 uppercase">Actions</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {dataLayers.map((layer) => {
                            const layerStatus = getStatusConfig(layer.status);
                            return (
                                <React.Fragment key={layer.name}>
                                    {/* Layer Header Row */}
                                    <TableRow className="bg-slate-50/50 hover:bg-slate-100/50 border-slate-100">
                                        <TableCell className="font-bold text-slate-700 py-3">
                                            <div className="flex items-center gap-2">
                                                <Database className="h-3.5 w-3.5 text-slate-400" />
                                                {layer.name}
                                            </div>
                                        </TableCell>
                                        <TableCell>
                                            <span
                                                className="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wider"
                                                style={{ backgroundColor: layerStatus.bg, color: layerStatus.text }}
                                            >
                                                {layer.status}
                                            </span>
                                        </TableCell>
                                        <TableCell colSpan={2} className="text-xs text-slate-400 font-mono">
                                            {layer.portalUrl ? (
                                                <a href={layer.portalUrl} target="_blank" rel="noreferrer" className="hover:text-sky-600 inline-flex items-center gap-1">
                                                    {layer.description || 'Container Resource'} <ExternalLink className="h-3 w-3" />
                                                </a>
                                            ) : (
                                                layer.description || '-'
                                            )}
                                        </TableCell>
                                        <TableCell className="text-end text-xs text-slate-400 font-mono">
                                            {layer.refreshFrequency ? `Every ${layer.refreshFrequency}` : '-'}
                                        </TableCell>
                                    </TableRow>

                                    {/* Domain Rows */}
                                    {(layer.domains || []).map(domain => {
                                        const dStatus = getStatusConfig(domain.status);
                                        const jobName = domain.jobName || '';
                                        const jobRun = jobName ? jobIndex.get(jobName) : null;
                                        const isTriggering = Boolean(jobName) && triggeringJob === jobName;

                                        return (
                                            <TableRow key={domain.name} className="hover:bg-slate-50 border-slate-100">
                                                <TableCell className="pl-8">
                                                    <div className="flex items-center gap-2">
                                                        <div
                                                            className="w-1.5 h-1.5 rounded-full"
                                                            style={{ backgroundColor: dStatus.text }}
                                                        />
                                                        <span className="text-sm font-medium text-slate-700">{domain.name}</span>
                                                        {domain.type && (
                                                            <span className="text-[9px] uppercase tracking-widest text-slate-400 border border-slate-200 px-1 rounded">
                                                                {domain.type}
                                                            </span>
                                                        )}
                                                    </div>
                                                </TableCell>
                                                <TableCell>
                                                    <span className="text-xs text-slate-600 font-mono">
                                                        {formatTimeAgo(domain.lastUpdated)} ago
                                                    </span>
                                                </TableCell>
                                                <TableCell>
                                                    {domain.portalUrl ? (
                                                        <a
                                                            href={domain.portalUrl}
                                                            target="_blank"
                                                            rel="noreferrer"
                                                            className="flex items-center gap-2 text-xs font-mono text-slate-500 hover:text-sky-600 hover:underline max-w-[300px] truncate"
                                                        >
                                                            <FolderOpen className="h-3.5 w-3.5" />
                                                            {domain.path || 'Open Location'}
                                                        </a>
                                                    ) : (
                                                        <span className="flex items-center gap-2 text-xs font-mono text-slate-400 max-w-[300px] truncate">
                                                            <FolderOpen className="h-3.5 w-3.5 opacity-50" />
                                                            {domain.path || '-'}
                                                        </span>
                                                    )}
                                                </TableCell>
                                                <TableCell>
                                                    {jobName ? (
                                                        <div className="flex flex-col gap-0.5">
                                                            <a
                                                                href={domain.jobUrl || '#'}
                                                                target="_blank"
                                                                rel="noreferrer"
                                                                className={`text-xs font-medium ${domain.jobUrl ? 'text-slate-700 hover:text-sky-600' : 'text-slate-500 cursor-default'}`}
                                                            >
                                                                {jobName}
                                                            </a>
                                                            {jobRun && (
                                                                <div className="flex items-center gap-1.5 text-[10px] text-slate-500">
                                                                    <span
                                                                        className="w-1.5 h-1.5 rounded-full"
                                                                        style={{ backgroundColor: getStatusConfig(jobRun.status).text }}
                                                                    />
                                                                    <span>{jobRun.status}</span>
                                                                    <span className="text-slate-300">•</span>
                                                                    <span>{formatTimeAgo(jobRun.startTime)} ago</span>
                                                                </div>
                                                            )}
                                                        </div>
                                                    ) : (
                                                        <span className="text-xs text-slate-300 italic">No job</span>
                                                    )}
                                                </TableCell>
                                                <TableCell className="text-end">
                                                    {jobName ? (
                                                        <Tooltip>
                                                            <TooltipTrigger asChild>
                                                                <button
                                                                    onClick={() => triggerJob(jobName)}
                                                                    disabled={Boolean(triggeringJob)}
                                                                    className="p-1.5 hover:bg-slate-100 text-slate-400 hover:text-emerald-600 rounded transition-colors"
                                                                >
                                                                    {isTriggering ? (
                                                                        <Loader2 className="h-4 w-4 animate-spin text-emerald-600" />
                                                                    ) : (
                                                                        <Play className="h-4 w-4" />
                                                                    )}
                                                                </button>
                                                            </TooltipTrigger>
                                                            <TooltipContent>
                                                                Trigger {jobName}
                                                            </TooltipContent>
                                                        </Tooltip>
                                                    ) : null}
                                                </TableCell>
                                            </TableRow>
                                        );
                                    })}
                                </React.Fragment>
                            );
                        })}
                    </TableBody>
                </Table>
            </div>

            {/* Quick footer legend */}
            <div className="flex gap-4 text-[10px] text-slate-400 uppercase tracking-widest px-1">
                <span className="flex items-center gap-1"><Database className="h-3 w-3" /> Container/Layer</span>
                <span className="flex items-center gap-1"><FolderOpen className="h-3 w-3" /> Data Path</span>
                <span className="flex items-center gap-1"><Play className="h-3 w-3" /> Trigger Job</span>
            </div>
        </div>
    );
}
