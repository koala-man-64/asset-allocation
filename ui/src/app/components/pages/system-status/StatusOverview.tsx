import React from 'react';
import { DataLayer, JobRun } from '@/types/strategy';
import { getStatusConfig, formatTimeAgo } from './SystemStatusHelpers';
import { StatusTypos, StatusColors } from './StatusTokens';
import { Zap, ExternalLink } from 'lucide-react';

interface StatusOverviewProps {
    overall: string;
    dataLayers: DataLayer[];
    recentJobs: JobRun[];
}

export function StatusOverview({ overall, dataLayers, recentJobs }: StatusOverviewProps) {
    const sysConfig = getStatusConfig(overall);
    const apiAnim = sysConfig.animation === 'spin' ? 'animate-spin' :
        sysConfig.animation === 'pulse' ? 'animate-pulse' : '';

    return (
        <div className="grid gap-4 font-sans">
            {/* System Header - Manual inline styles for specific 'Industrial' theming overrides */}
            <div className="flex items-center justify-between p-4 border rounded-none border-l-4"
                style={{
                    backgroundColor: StatusColors.PANEL_BG,
                    borderColor: sysConfig.border,
                    borderLeftColor: sysConfig.text
                }}>
                <div className="flex items-center gap-4">
                    <sysConfig.icon className={`h-8 w-8 ${apiAnim}`}
                        style={{ color: sysConfig.text }} />
                    <div>
                        <div className={StatusTypos.HEADER}>SYSTEM STATUS</div>
                        <div className="text-2xl font-black tracking-tighter uppercase"
                            style={{ color: sysConfig.text }}>
                            {overall}
                        </div>
                    </div>
                </div>
                <div className="text-right">
                    <div className={StatusTypos.HEADER}>UPTIME CLOCK</div>
                    <div className={`${StatusTypos.MONO} text-xl text-zinc-400`}>
                        {new Date().toISOString().split('T')[1].split('.')[0]} UTC
                    </div>
                </div>
            </div>

            {/* Dense Matrix Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-0 border border-zinc-800 bg-zinc-950">
                {dataLayers.map((layer) => (
                    <div key={layer.name} className="p-4 border-b border-r border-zinc-800 hover:bg-zinc-900/50 transition-colors">
                        {/* Layer Header */}
                        <div className="flex justify-between items-start mb-4">
                            <div>
                                <div className={StatusTypos.HEADER}>LAYER</div>
                                <div className="font-bold text-lg text-zinc-100">{layer.name}</div>
                            </div>
                            <div className={`${StatusTypos.MONO} text-xs px-2 py-1 rounded-sm font-bold opacity-80`}
                                style={{ backgroundColor: getStatusConfig(layer.status).bg, color: getStatusConfig(layer.status).text }}>
                                {layer.status.toUpperCase()}
                            </div>
                        </div>

                        {/* Domain Rows */}
                        <div className="space-y-1">
                            {(layer.domains || []).map((domain) => {
                                const dStatus = getStatusConfig(domain.status);
                                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                                const hasJob = domain.jobUrl;

                                return (
                                    <div key={domain.name} className="flex items-center justify-between p-2 hover:bg-zinc-800/50 border border-transparent hover:border-zinc-700 transition-all rounded-sm group">
                                        <div className="flex items-center gap-3">
                                            <div className="w-2 h-2 rounded-full shadow-[0_0_4px_0_rgba(0,0,0,0.5)]"
                                                style={{ backgroundColor: dStatus.text, boxShadow: `0 0 8px ${dStatus.text}40` }} />
                                            <span className="text-sm font-medium text-zinc-300">{domain.name}</span>
                                        </div>

                                        <div className="flex items-center opacity-40 group-hover:opacity-100 transition-opacity gap-1">
                                            {domain.triggerUrl && (
                                                <button className="p-1.5 hover:bg-zinc-700 text-zinc-500 hover:text-cyan-400 disabled:opacity-30 rounded"
                                                    title="Trigger Pipeline" type="button">
                                                    <Zap className="h-3.5 w-3.5" />
                                                </button>
                                            )}
                                            {domain.jobUrl && (
                                                <a href={domain.jobUrl} target="_blank" rel="noreferrer"
                                                    className="p-1.5 hover:bg-zinc-700 text-zinc-500 hover:text-blue-400 rounded">
                                                    <ExternalLink className="h-3.5 w-3.5" />
                                                </a>
                                            )}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                        <div className="mt-4 pt-3 border-t border-zinc-900 flex justify-between items-center">
                            <div className={StatusTypos.HEADER}>LAST UPDATE</div>
                            <div className={`${StatusTypos.MONO} text-xs text-zinc-500`}>
                                {formatTimeAgo(layer.lastUpdated)} AGO
                            </div>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}
