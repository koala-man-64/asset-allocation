import React, { useMemo } from 'react';
import { DataDomain, DataLayer, JobRun } from '@/types/strategy';
import { formatTimeAgo, getStatusConfig } from './SystemStatusHelpers';
import { StatusTypos, StatusColors } from './StatusTokens';
import { CalendarDays, Database, FolderOpen, Loader2, Play, ScrollText } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { useJobTrigger } from '@/hooks/useJobTrigger';

interface StatusOverviewProps {
    overall: string;
    dataLayers: DataLayer[];
    recentJobs: JobRun[];
    onViewJobLogs?: (jobName: string, startTime?: string | null) => void;
}

export function StatusOverview({ overall, dataLayers, recentJobs, onViewJobLogs }: StatusOverviewProps) {
    const sysConfig = getStatusConfig(overall);
    const apiAnim = sysConfig.animation === 'spin' ? 'animate-spin' : sysConfig.animation === 'pulse' ? 'animate-pulse' : '';
    const { triggeringJob, triggerJob } = useJobTrigger();

    const domainNames = useMemo(() => {
        const seen = new Set<string>();
        const names: string[] = [];

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
            if (!job?.jobName) continue;
            const existing = index.get(job.jobName);
            if (!existing || String(job.startTime || '') > String(existing.startTime || '')) {
                index.set(job.jobName, job);
            }
        }
        return index;
    }, [recentJobs]);

    const domainsByLayer = useMemo(() => {
        const index = new Map<string, Map<string, DataDomain>>();

        for (const layer of dataLayers) {
            const domainIndex = new Map<string, DataDomain>();
            for (const domain of layer.domains || []) {
                if (domain?.name) domainIndex.set(domain.name, domain);
            }
            index.set(layer.name, domainIndex);
        }

        return index;
    }, [dataLayers]);

    return (
        <div className="grid gap-4 font-sans">
            {/* System Header - Manual inline styles for specific 'Industrial' theming overrides */}
            <div
                className="flex items-center justify-between p-4 border rounded-none border-l-4"
                style={{
                    backgroundColor: StatusColors.PANEL_BG,
                    borderColor: StatusColors.PANEL_BORDER,
                    borderLeftColor: sysConfig.text,
                }}
            >
                <div className="flex items-center gap-4">
                    <sysConfig.icon className={`h-10 w-10 ${apiAnim}`} style={{ color: sysConfig.text }} />
                    <div>
                        <h1 className={StatusTypos.HEADER}>SYSTEM STATUS</h1>
                        <div className="text-2xl font-black tracking-tighter uppercase" style={{ color: sysConfig.text }}>
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

            {/* Domain x Layer Matrix (Recovered from 1bba1b8f presentation) */}
            <div className="rounded-none border border-slate-200 bg-white overflow-x-auto">
                <Table className="text-[11px] table-fixed">
                    <TableHeader>
                        <TableRow className="bg-white hover:bg-white border-slate-200">
                            <TableHead
                                rowSpan={dataLayers.length ? 2 : 1}
                                className={`${StatusTypos.HEADER} bg-white text-slate-500 w-[220px]`}
                            >
                                DOMAIN
                            </TableHead>
                            {dataLayers.map((layer, layerIdx) => {
                                const layerStatus = getStatusConfig(layer.status);
                                const groupBorder = layerIdx === 0 ? '' : 'border-l border-slate-200';

                                return (
                                    <TableHead key={layer.name} colSpan={2} className={`bg-white ${groupBorder}`}>
                                        <div className="flex items-center justify-between gap-2">
                                            <div className="flex items-center gap-2 min-w-0">
                                                <span className="font-bold text-slate-900 truncate">{layer.name}</span>
                                                <span
                                                    className={`${StatusTypos.MONO} text-[10px] px-2 py-1 rounded-sm font-bold border opacity-80`}
                                                    style={{
                                                        backgroundColor: layerStatus.bg,
                                                        color: layerStatus.text,
                                                        borderColor: layerStatus.border,
                                                    }}
                                                >
                                                    {layer.status.toUpperCase()}
                                                </span>
                                            </div>
                                            <Tooltip>
                                                <TooltipTrigger asChild>
                                                    {layer.portalUrl ? (
                                                        <a
                                                            href={layer.portalUrl}
                                                            target="_blank"
                                                            rel="noreferrer"
                                                            className="p-1.5 hover:bg-slate-100 text-slate-500 hover:text-sky-600 rounded"
                                                            aria-label={`Open ${layer.name} container`}
                                                        >
                                                            <Database className="h-4 w-4" />
                                                        </a>
                                                    ) : (
                                                        <span
                                                            className="p-1.5 text-slate-300 rounded cursor-not-allowed"
                                                            aria-label={`No container link for ${layer.name}`}
                                                        >
                                                            <Database className="h-4 w-4" />
                                                        </span>
                                                    )}
                                                </TooltipTrigger>
                                                <TooltipContent side="bottom">
                                                    {layer.portalUrl ? 'Open container' : 'Container link not configured'}
                                                </TooltipContent>
                                            </Tooltip>
                                        </div>
                                    </TableHead>
                                );
                            })}
                        </TableRow>

                        {dataLayers.length > 0 && (
                            <TableRow className="bg-slate-50 hover:bg-slate-50 border-slate-200">
                                {dataLayers.map((layer, layerIdx) => {
                                    const groupBorder = layerIdx === 0 ? '' : 'border-l border-slate-200';

                                    return (
                                        <React.Fragment key={layer.name}>
                                            <TableHead
                                                className={`${StatusTypos.HEADER} h-8 text-slate-500 text-center w-[96px] ${groupBorder}`}
                                            >
                                                STATUS
                                            </TableHead>
                                            <TableHead className={`${StatusTypos.HEADER} h-8 text-slate-500 text-center w-[96px]`}>
                                                LINKS
                                            </TableHead>
                                        </React.Fragment>
                                    );
                                })}
                            </TableRow>
                        )}
                    </TableHeader>

                    <TableBody>
                        {domainNames.map((domainName) => (
                            <TableRow key={domainName} className="group border-slate-200 even:bg-slate-50/30 hover:bg-slate-50">
                                <TableCell className="text-sm font-semibold text-slate-900">{domainName}</TableCell>

                                {dataLayers.map((layer, layerIdx) => {
                                    const domain = domainsByLayer.get(layer.name)?.get(domainName);
                                    const groupBorder = layerIdx === 0 ? '' : 'border-l border-slate-200';

                                    return (
                                        <React.Fragment key={layer.name}>
                                            <TableCell className={`text-center ${groupBorder}`}>
                                                {domain ? (
                                                    <div className="inline-flex items-center justify-center gap-1 whitespace-nowrap">
                                                        {(() => {
                                                            const iconButtonClass =
                                                                'inline-flex h-7 w-7 items-center justify-center rounded hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-sky-500/30';
                                                            const iconDisabledClass =
                                                                'inline-flex h-7 w-7 items-center justify-center rounded opacity-40 cursor-not-allowed';
                                                            const iconClass = 'h-4 w-4';

                                                            const pathText = String(domain.path || '').toLowerCase();
                                                            const isByDate = pathText.includes('by-date') || pathText.includes('_by_date');

                                                            const byDateFolderUrl = isByDate ? domain.portalUrl : null;
                                                            const baseFolderUrl = (() => {
                                                                if (!domain.portalUrl) return null;
                                                                if (!isByDate) return domain.portalUrl;
                                                                const derived = domain.portalUrl
                                                                    .replace(/\/by-date\b/gi, '')
                                                                    .replace(/_by_date\b/gi, '');
                                                                return derived === domain.portalUrl ? domain.portalUrl : derived;
                                                            })();
                                                            const showByDateFolder = Boolean(byDateFolderUrl) && baseFolderUrl !== byDateFolderUrl;

                                                            const dataConfig = getStatusConfig(domain.status || 'pending');

                                                            const jobName = String(domain.jobName || '').trim();
                                                            const run = jobName ? jobIndex.get(jobName) : null;
                                                            const jobConfig = jobName
                                                                ? getStatusConfig(run?.status || 'pending')
                                                                : getStatusConfig('unknown');
                                                            const JobIcon = jobConfig.icon;
                                                            const jobAnim =
                                                                jobConfig.animation === 'spin'
                                                                    ? 'animate-spin'
                                                                    : jobConfig.animation === 'pulse'
                                                                        ? 'animate-pulse'
                                                                        : '';

                                                            return (
                                                                <>
                                                                    <Tooltip>
                                                                        <TooltipTrigger asChild>
                                                                            {baseFolderUrl ? (
                                                                                <a
                                                                                    href={baseFolderUrl}
                                                                                    target="_blank"
                                                                                    rel="noreferrer"
                                                                                    className={iconButtonClass}
                                                                                    aria-label={`Open data folder (${String(domain.status || 'unknown')})`}
                                                                                >
                                                                                    <FolderOpen className={iconClass} style={{ color: dataConfig.text }} />
                                                                                </a>
                                                                            ) : (
                                                                                <span
                                                                                    tabIndex={0}
                                                                                    className={iconDisabledClass}
                                                                                    aria-label={`Data ${String(domain.status || 'unknown')}`}
                                                                                >
                                                                                    <FolderOpen className={iconClass} style={{ color: dataConfig.text }} />
                                                                                </span>
                                                                            )}
                                                                        </TooltipTrigger>
                                                                        <TooltipContent side="bottom">
                                                                            Data • {(domain.status || 'unknown').toUpperCase()} • {formatTimeAgo(domain.lastUpdated)} ago
                                                                        </TooltipContent>
                                                                    </Tooltip>

                                                                    {showByDateFolder && (
                                                                        <Tooltip>
                                                                            <TooltipTrigger asChild>
                                                                                <a
                                                                                    href={byDateFolderUrl!}
                                                                                    target="_blank"
                                                                                    rel="noreferrer"
                                                                                    className={iconButtonClass}
                                                                                    aria-label={`Open by-date folder (${String(domain.status || 'unknown')})`}
                                                                                >
                                                                                    <CalendarDays className={iconClass} style={{ color: dataConfig.text }} />
                                                                                </a>
                                                                            </TooltipTrigger>
                                                                            <TooltipContent side="bottom">
                                                                                By-date • {(domain.status || 'unknown').toUpperCase()} • {formatTimeAgo(domain.lastUpdated)} ago
                                                                            </TooltipContent>
                                                                        </Tooltip>
                                                                    )}

                                                                    <Tooltip>
                                                                        <TooltipTrigger asChild>
                                                                            {domain.jobUrl ? (
                                                                                <a
                                                                                    href={domain.jobUrl}
                                                                                    target="_blank"
                                                                                    rel="noreferrer"
                                                                                    className={iconButtonClass}
                                                                                    aria-label={`Open job (${run?.status || 'unknown'})`}
                                                                                >
                                                                                    <JobIcon
                                                                                        className={`${iconClass} ${jobAnim}`}
                                                                                        style={{ color: jobConfig.text }}
                                                                                    />
                                                                                </a>
                                                                            ) : (
                                                                                <span
                                                                                    tabIndex={0}
                                                                                    className={iconDisabledClass}
                                                                                    aria-label={`Job ${run?.status || 'unknown'}`}
                                                                                >
                                                                                    <JobIcon
                                                                                        className={`${iconClass} ${jobAnim}`}
                                                                                        style={{ color: jobConfig.text }}
                                                                                    />
                                                                                </span>
                                                                            )}
                                                                        </TooltipTrigger>
                                                                        <TooltipContent side="bottom">
                                                                            {jobName
                                                                                ? run
                                                                                    ? `Job • ${run.status.toUpperCase()} • ${formatTimeAgo(run.startTime)} ago`
                                                                                    : 'Job • NO RECENT RUN'
                                                                                : 'Job not configured'}
                                                                        </TooltipContent>
                                                                    </Tooltip>
                                                                </>
                                                            );
                                                        })()}
                                                    </div>
                                                ) : (
                                                    <span className="text-slate-300">—</span>
                                                )}
                                            </TableCell>

                                            <TableCell className="text-center">
                                                {domain ? (
                                                    <div className="inline-flex items-center justify-center gap-0.5">
                                                        {(() => {
                                                            const jobName = String(domain.jobName || '').trim();
                                                            const run = jobName ? jobIndex.get(jobName) : null;
                                                            const isTriggering = Boolean(jobName) && triggeringJob === jobName;

                                                            return (
                                                                <>
                                                                    {onViewJobLogs && (
                                                                        <Tooltip>
                                                                            <TooltipTrigger asChild>
                                                                                {jobName ? (
                                                                                    <button
                                                                                        type="button"
                                                                                        onClick={() => onViewJobLogs(jobName, run?.startTime ?? null)}
                                                                                        className="p-1 hover:bg-slate-100 text-slate-500 hover:text-sky-600 rounded"
                                                                                        aria-label={`View ${domainName} logs`}
                                                                                    >
                                                                                        <ScrollText className="h-4 w-4" />
                                                                                    </button>
                                                                                ) : (
                                                                                    <span
                                                                                        className="p-1 text-slate-300 rounded cursor-not-allowed"
                                                                                        aria-label={`No job name for ${domainName}`}
                                                                                    >
                                                                                        <ScrollText className="h-4 w-4" />
                                                                                    </span>
                                                                                )}
                                                                            </TooltipTrigger>
                                                                            <TooltipContent side="bottom">
                                                                                {jobName
                                                                                    ? run
                                                                                        ? `View logs (${run.status.toUpperCase()}, ${formatTimeAgo(run.startTime)} ago)`
                                                                                        : 'View latest logs'
                                                                                    : 'Log viewing not configured'}
                                                                            </TooltipContent>
                                                                        </Tooltip>
                                                                    )}
                                                                    <Tooltip>
                                                                        <TooltipTrigger asChild>
                                                                            {jobName ? (
                                                                                <button
                                                                                    type="button"
                                                                                    onClick={() => void triggerJob(jobName)}
                                                                                    disabled={Boolean(triggeringJob)}
                                                                                    className="p-1 hover:bg-slate-100 text-slate-500 hover:text-emerald-600 disabled:opacity-30 disabled:cursor-not-allowed rounded"
                                                                                    aria-label={`Trigger ${domainName} job`}
                                                                                >
                                                                                    {isTriggering ? (
                                                                                        <Loader2 className="h-4 w-4 animate-spin" />
                                                                                    ) : (
                                                                                        <Play className="h-4 w-4" />
                                                                                    )}
                                                                                </button>
                                                                            ) : (
                                                                                <span
                                                                                    className="p-1 text-slate-300 rounded cursor-not-allowed"
                                                                                    aria-label={`No job name for ${domainName}`}
                                                                                >
                                                                                    <Play className="h-4 w-4" />
                                                                                </span>
                                                                            )}
                                                                        </TooltipTrigger>
                                                                        <TooltipContent side="bottom">
                                                                            {jobName ? (isTriggering ? 'Triggering job…' : 'Trigger job') : 'Job triggering not configured'}
                                                                        </TooltipContent>
                                                                    </Tooltip>
                                                                </>
                                                            );
                                                        })()}
                                                    </div>
                                                ) : (
                                                    <span className="text-slate-300">—</span>
                                                )}
                                            </TableCell>
                                        </React.Fragment>
                                    );
                                })}
                            </TableRow>
                        ))}

                        {domainNames.length === 0 && (
                            <TableRow className="hover:bg-transparent">
                                <TableCell
                                    colSpan={1 + dataLayers.length * 2}
                                    className="py-10 text-center text-xs text-slate-500 font-mono"
                                >
                                    No domains found
                                </TableCell>
                            </TableRow>
                        )}
                    </TableBody>
                </Table>
            </div>
        </div>
    );
}
