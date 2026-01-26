import React, { useEffect, useMemo, useState } from 'react';
import { useSystemHealthQuery } from '@/hooks/useDataQueries';
import { StatusOverview } from './system-status/StatusOverview';

import { AzureResources } from './system-status/AzureResources';
// JobMonitor and DataLayerHealth are redundant with the new dense StatusOverview or can be re-added below if needed.
// For "High Density" view, we prioritize the Matrix (StatusOverview).
import { JobMonitor } from './system-status/JobMonitor';
import { ScheduledJobMonitor } from './system-status/ScheduledJobMonitor';
import { getAzurePortalUrl } from './system-status/SystemStatusHelpers';
import { JobLogDrawer } from './system-status/JobLogDrawer';

export function SystemStatusPage() {
    const { data, isLoading, error, isFetching } = useSystemHealthQuery();
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, setTick] = useState(0);
    const [logTarget, setLogTarget] = useState<{ jobName: string } | null>(null);
    const jobLinks = useMemo(() => {
        if (!data) {
            return {};
        }

        const links: Record<string, string> = {};
        for (const layer of data.dataLayers || []) {
            for (const domain of layer.domains || []) {
                if (domain.jobName && domain.jobUrl) {
                    links[domain.jobName] = domain.jobUrl;
                }
            }
        }
        for (const resource of data.resources || []) {
            if (resource.resourceType === 'Microsoft.App/jobs' && resource.azureId) {
                links[resource.name] = getAzurePortalUrl(resource.azureId);
            }
        }
        return links;
    }, [data]);

    useEffect(() => {
        const errorMessage = error instanceof Error ? error.message : error ? String(error) : null;
        console.info('[SystemStatusPage] system health query state', {
            isLoading,
            isFetching,
            hasData: Boolean(data),
            error: errorMessage,
        });
    }, [isLoading, isFetching, data, error]);

    // Force re-render for clock
    useEffect(() => {
        const h = setInterval(() => setTick(t => t + 1), 1000);
        return () => clearInterval(h);
    }, []);

    if (isLoading) {
        return (
            <div className="flex items-center justify-center h-[calc(100vh-100px)]">
                <div className="flex flex-col items-center gap-4">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                    <p className="text-muted-foreground text-sm font-mono tracking-widest uppercase">Initializing System Link...</p>
                </div>
            </div>
        );
    }

    if (error || !data) {
        return (
            <div className="p-6 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive font-mono">
                <h3 className="text-lg font-bold mb-2 uppercase">System Link Failure</h3>
                <p>{error ? (error as Error).message : 'No telemetry available'}</p>
            </div>
        );
    }

    const { overall, dataLayers, recentJobs, resources } = data;

    return (
        <div className="space-y-8 pb-10">
            {/* Status Matrix - The Hero Component */}
            <StatusOverview
                overall={overall}
                dataLayers={dataLayers}
                recentJobs={recentJobs}
                onViewJobLogs={(jobName) => setLogTarget({ jobName })}
            />

            {/* Jobs */}
            <div className="grid gap-6 lg:grid-cols-5">
                <JobMonitor
                    className="lg:col-span-2"
                    recentJobs={recentJobs}
                    jobLinks={jobLinks}
                    onViewJobLogs={(jobName) => setLogTarget({ jobName })}
                />
                <ScheduledJobMonitor
                    className="lg:col-span-3"
                    dataLayers={dataLayers}
                    recentJobs={recentJobs}
                    jobLinks={jobLinks}
                />
            </div>

            {/* Connectors / Resources */}
            {resources && resources.length > 0 && (
                <AzureResources resources={resources} />
            )}

            {/* Footer Status Line */}
            <div className="flex justify-end border-t border-dashed border-zinc-800 pt-2 opacity-50">
                <div className="flex items-center gap-2 font-mono text-[10px]">
                    <span className={`w-2 h-2 rounded-full ${isFetching ? 'bg-cyan-500 animate-pulse' : 'bg-zinc-600'}`} />
                    {isFetching ? 'RECEIVING TELEMETRY...' : 'LINK ESTABLISHED'}
                </div>
            </div>

            <JobLogDrawer
                open={Boolean(logTarget)}
                onOpenChange={(open) => {
                    if (!open) setLogTarget(null);
                }}
                jobName={logTarget?.jobName ?? null}
            />
        </div>
    );
}
