import React from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Activity, Database, PlayCircle, Zap } from 'lucide-react';
import { getStatusIcon, getStatusBadge, formatTimestamp } from './SystemStatusHelpers';
import type { DataLayer, JobRun } from '@/types/strategy';

interface StatusOverviewProps {
    overall: string;
    dataLayers: DataLayer[];
    recentJobs: JobRun[];
}

export function StatusOverview({ overall, dataLayers, recentJobs }: StatusOverviewProps) {
    return (
        <Card>
            <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-lg">
                    <Activity className="h-4 w-4" />
                    System Health Overview
                </CardTitle>
                <CardDescription className="text-xs">
                    Real-time status of data layers and pipeline jobs
                </CardDescription>
            </CardHeader>
            <CardContent>
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-2">
                    {/* Overall Status Metrics */}
                    <div className="flex flex-col justify-center gap-2">
                        <div className="flex flex-col items-center justify-center p-3 bg-muted/20 rounded-lg border border-muted h-full">
                            <div className="scale-110 mb-2">{getStatusIcon(overall)}</div>
                            <div className="text-3xl font-extrabold capitalize mb-0.5">{overall}</div>
                            <p className="text-xs text-muted-foreground text-center">System Operational Status</p>
                        </div>
                    </div>

                    {/* Detailed Layer Status */}
                    <div className="lg:col-span-2 grid gap-2">
                        {dataLayers.map((layer, idx) => (
                            <div key={idx} className="flex flex-col p-1.5 border rounded-md hover:bg-muted/50 transition-colors gap-1.5">
                                {/* Layer Header */}
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center gap-2">
                                        <div className="scale-75">{getStatusIcon(layer.status)}</div>
                                        <div>
                                            <div className="font-bold text-sm flex items-center gap-1.5">
                                                {layer.name}
                                                <div className="flex items-center gap-1 border-l pl-2 ml-1 opacity-70">
                                                    {(layer.domains || []).map((domain, dIdx: number) => (
                                                        <div key={dIdx} title={`${domain.name}: ${domain.status}`} className="scale-75">
                                                            {getStatusIcon(domain.status)}
                                                        </div>
                                                    ))}
                                                </div>

                                                <div className="flex items-center gap-1 ml-1.5">
                                                    {layer.portalUrl && (
                                                        <a href={layer.portalUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="View Azure Container">
                                                            <Database className="h-3 w-3" />
                                                        </a>
                                                    )}
                                                    {layer.jobUrl && (
                                                        <a href={layer.jobUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="View Pipeline Job">
                                                            <PlayCircle className="h-3 w-3" />
                                                        </a>
                                                    )}
                                                </div>
                                            </div>
                                            <div className="text-[10px] text-muted-foreground">
                                                Updated: {formatTimestamp(layer.lastUpdated)}
                                            </div>
                                        </div>
                                    </div>
                                    {getStatusBadge(layer.status)}
                                </div>

                                {/* Domains List */}
                                <div className="grid grid-cols-2 sm:grid-cols-4 gap-1.5 pl-6">
                                    {(layer.domains || []).map((domain, dIdx: number) => {
                                        const jName = domain.jobUrl?.split('/jobs/')[1]?.split('/')[0];
                                        const job = jName ? recentJobs.find((j) => j.jobName === jName) : null;

                                        return (
                                            <div key={dIdx} className="flex flex-col gap-0.5 border-l border-muted pl-1.5 py-0.5">
                                                <div className="flex items-center justify-between group">
                                                    <span className="text-[11px] font-medium truncate max-w-[80px]" title={domain.name}>
                                                        {domain.name}
                                                    </span>
                                                    <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                                                        {domain.jobUrl && (
                                                            <a href={domain.jobUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="View Domain Job">
                                                                <PlayCircle className="h-2.5 w-2.5" />
                                                            </a>
                                                        )}
                                                        {domain.triggerUrl && (
                                                            <a href={domain.triggerUrl} target="_blank" rel="noopener noreferrer" className="text-muted-foreground hover:text-blue-500 transition-colors" title="Trigger Domain Logic">
                                                                <Zap className="h-2.5 w-2.5" />
                                                            </a>
                                                        )}
                                                    </div>
                                                </div>

                                                {job ? (
                                                    <div className="flex items-center gap-1" title={`Job: ${job.jobName} (${job.status})`}>
                                                        <div className={`h-1.5 w-1.5 rounded-full ${job.status === 'success' ? 'bg-green-500' :
                                                            job.status === 'failed' ? 'bg-red-500' :
                                                                job.status === 'running' ? 'bg-blue-500 animate-pulse' : 'bg-gray-300'
                                                            }`} />
                                                        <span className="text-[10px] text-muted-foreground capitalize">{job.status}</span>
                                                    </div>
                                                ) : (
                                                    <span className="text-[10px] text-muted-foreground italic opacity-50">No job</span>
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
    );
}
