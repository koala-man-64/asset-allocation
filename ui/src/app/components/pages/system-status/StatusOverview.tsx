import React, { useMemo } from 'react';
import { DataLayer, JobRun } from '@/types/strategy';
import { formatDuration, formatTimeAgo, getStatusConfig } from './SystemStatusHelpers';
import { StatusTypos, StatusColors } from './StatusTokens';
import { Database, ExternalLink, FolderOpen, Loader2, Play } from 'lucide-react';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
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
    const jobIndex = useMemo(() => {
        const index = new Map<string, JobRun>();
        for (const job of recentJobs) {
            if (!job.jobName) continue;
            if (!index.has(job.jobName)) {
                index.set(job.jobName, job);
            }
        }
        return index;
    }, [recentJobs]);

    return (
        <div className="grid gap-4 font-sans">
            {/* System Header - Manual inline styles for specific 'Industrial' theming overrides */}
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

            {/* Dense Matrix Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-0 border border-slate-200 bg-white">
                {dataLayers.map((layer) => {
                    const layerStatus = getStatusConfig(layer.status);
                    const maxAgeLabel = formatDuration(layer.maxAgeSeconds);

                    return (
                        <div key={layer.name} className="p-4 border-b border-r border-slate-200 hover:bg-slate-50 transition-colors">
                            {/* Layer Header */}
                            <div className="flex justify-between items-start mb-4">
                                <div className="min-w-0">
                                    <div className={StatusTypos.HEADER}>LAYER</div>
                                    <div className="flex items-center gap-2">
                                        <div className="font-bold text-lg text-slate-900">{layer.name}</div>
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
                                                        <Database className="h-3.5 w-3.5" />
                                                    </a>
                                                ) : (
                                                    <span
                                                        className="p-1.5 text-slate-300 rounded cursor-not-allowed"
                                                        aria-label={`No container link for ${layer.name}`}
                                                    >
                                                        <Database className="h-3.5 w-3.5" />
                                                    </span>
                                                )}
                                            </TooltipTrigger>
                                            <TooltipContent side="left">
                                                {layer.portalUrl ? 'Open container' : 'Container link not configured'}
                                            </TooltipContent>
                                        </Tooltip>
                                    </div>
                                    {layer.description && (
                                        <div className="text-xs text-slate-500 mt-1">{layer.description}</div>
                                    )}
                                </div>
                                <div
                                    className={`${StatusTypos.MONO} text-xs px-2 py-1 rounded-sm font-bold opacity-80`}
                                    style={{ backgroundColor: layerStatus.bg, color: layerStatus.text }}
                                >
                                    {layer.status.toUpperCase()}
                                </div>
                            </div>

                            <div className="grid grid-cols-2 gap-2 mb-4">
                                <div className="border border-slate-200 px-2 py-1 flex items-center justify-between text-[10px] uppercase tracking-widest text-slate-500">
                                    <span>Refresh</span>
                                    <span className="font-mono text-slate-700">{layer.refreshFrequency || '-'}</span>
                                </div>
                                <div className="border border-slate-200 px-2 py-1 flex items-center justify-between text-[10px] uppercase tracking-widest text-slate-500">
                                    <span>Max Age</span>
                                    <span className="font-mono text-slate-700">{maxAgeLabel || '-'}</span>
                                </div>
                            </div>

                            {/* Domain Rows */}
                            <div className="space-y-2">
                                {(layer.domains || []).map((domain) => {
                                    const dStatus = getStatusConfig(domain.status);
                                    const jobName = domain.jobName || '';
                                    const jobRun = jobName ? jobIndex.get(jobName) : null;
                                    const schedule = domain.frequency || domain.cron || '-';
                                    const isTriggering = Boolean(jobName) && triggeringJob === jobName;

                                    return (
                                        <div
                                            key={domain.name}
                                            className="flex items-start justify-between gap-3 p-2 border border-transparent hover:border-slate-200 hover:bg-slate-50 transition-all rounded-sm group"
                                        >
                                            <div className="min-w-0">
                                                <div className="flex items-center gap-2">
                                                    <div
                                                        className="w-2 h-2 rounded-full shadow-[0_0_4px_0_rgba(0,0,0,0.5)]"
                                                        style={{ backgroundColor: dStatus.text, boxShadow: `0 0 8px ${dStatus.text}40` }}
                                                    />
                                                    <span className="text-sm font-semibold text-slate-900">{domain.name}</span>
                                                    {domain.type && (
                                                        <span className="text-[10px] uppercase tracking-widest text-slate-500 font-mono">
                                                            {domain.type}
                                                        </span>
                                                    )}
                                                </div>
                                                {domain.description && (
                                                    <div className="text-xs text-slate-500 mt-0.5">{domain.description}</div>
                                                )}
                                                <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] font-mono text-slate-500">
                                                    <span className="uppercase text-slate-600">Path</span>
                                                    <Tooltip>
                                                        <TooltipTrigger asChild>
                                                            {domain.portalUrl ? (
                                                                <a
                                                                    href={domain.portalUrl}
                                                                    target="_blank"
                                                                    rel="noreferrer"
                                                                    className="inline-flex items-center gap-1 text-slate-700 hover:text-sky-600 truncate max-w-[220px]"
                                                                    title={domain.path || ''}
                                                                >
                                                                    <FolderOpen className="h-3 w-3" />
                                                                    {domain.path || '-'}
                                                                </a>
                                                            ) : (
                                                                <span
                                                                    className="inline-flex items-center gap-1 text-slate-400 truncate max-w-[220px] cursor-not-allowed"
                                                                    title={domain.path || ''}
                                                                >
                                                                    <FolderOpen className="h-3 w-3" />
                                                                    {domain.path || '-'}
                                                                </span>
                                                            )}
                                                        </TooltipTrigger>
                                                        <TooltipContent side="left">
                                                            {domain.portalUrl ? 'Open data folder' : 'Folder link not configured'}
                                                        </TooltipContent>
                                                    </Tooltip>
                                                    <span className="uppercase text-slate-600">Schedule</span>
                                                    <span className="text-slate-700">{schedule}</span>
                                                    <span className="uppercase text-slate-600">Last</span>
                                                    <span className="text-slate-700">{formatTimeAgo(domain.lastUpdated)} ago</span>
                                                    {typeof domain.version === 'number' && (
                                                        <>
                                                            <span className="uppercase text-slate-600">Ver</span>
                                                            <span className="text-slate-700">v{domain.version}</span>
                                                        </>
                                                    )}
                                                </div>
                                                {jobName && (
                                                    <div className="mt-1 flex flex-wrap items-center gap-x-2 text-[10px] font-mono text-slate-500">
                                                        <span className="uppercase text-slate-600">Job</span>
                                                        <span className="text-slate-700">{jobName}</span>
                                                        <Tooltip>
                                                            <TooltipTrigger asChild>
                                                                {domain.jobUrl ? (
                                                                    <a
                                                                        href={domain.jobUrl}
                                                                        target="_blank"
                                                                        rel="noreferrer"
                                                                        className="inline-flex items-center text-slate-500 hover:text-sky-600"
                                                                        aria-label={`Open ${jobName} job`}
                                                                    >
                                                                        <ExternalLink className="h-3 w-3" />
                                                                    </a>
                                                                ) : (
                                                                    <span
                                                                        className="inline-flex items-center text-slate-300 cursor-not-allowed"
                                                                        aria-label={`No job link for ${jobName}`}
                                                                    >
                                                                        <ExternalLink className="h-3 w-3" />
                                                                    </span>
                                                                )}
                                                            </TooltipTrigger>
                                                            <TooltipContent side="left">
                                                                {domain.jobUrl ? 'Open job details' : 'Job link not configured'}
                                                            </TooltipContent>
                                                        </Tooltip>
                                                        {jobRun && (
                                                            <>
                                                                <span className="flex items-center gap-1 text-slate-700">
                                                                    <span
                                                                        className="w-1.5 h-1.5 rounded-full"
                                                                        style={{ backgroundColor: getStatusConfig(jobRun.status).text }}
                                                                    />
                                                                    {jobRun.status}
                                                                </span>
                                                                <span className="text-slate-700">
                                                                    {formatTimeAgo(jobRun.startTime)} ago
                                                                </span>
                                                            </>
                                                        )}
                                                    </div>
                                                )}
                                            </div>

                                            <div className="flex items-center gap-1">
                                                {jobName && (
                                                    <Tooltip>
                                                        <TooltipTrigger asChild>
                                                            <button
                                                                className="p-1.5 hover:bg-slate-100 text-slate-500 hover:text-emerald-600 disabled:opacity-30 rounded"
                                                                type="button"
                                                                aria-label={`Run ${jobName}`}
                                                                disabled={Boolean(triggeringJob)}
                                                                onClick={() => void triggerJob(jobName)}
                                                            >
                                                                {isTriggering ? (
                                                                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                                                ) : (
                                                                    <Play className="h-3.5 w-3.5" />
                                                                )}
                                                            </button>
                                                        </TooltipTrigger>
                                                        <TooltipContent side="left">Trigger job</TooltipContent>
                                                    </Tooltip>
                                                )}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                            <div className="mt-4 pt-3 border-t border-slate-200 flex justify-between items-center">
                                <div className={StatusTypos.HEADER}>LAST UPDATE</div>
                                <div className={`${StatusTypos.MONO} text-xs text-slate-500`}>
                                    {formatTimeAgo(layer.lastUpdated)} AGO
                                </div>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
