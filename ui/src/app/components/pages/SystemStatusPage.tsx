import React, { useEffect, useMemo, useState } from 'react';
import { useSystemHealthQuery } from '@/hooks/useDataQueries';
import { StatusOverview } from './system-status/StatusOverview';
import { AzureResources } from './system-status/AzureResources';
import { ScheduledJobsPanel } from './system-status/ScheduledJobsPanel';

export function SystemStatusPage() {
    const { data, isLoading, error, isFetching } = useSystemHealthQuery();
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, setTick] = useState(0);
    const jobLinkTokens = useMemo(() => {
        if (!data) {
            return {};
        }

        const links: Record<string, string> = {};
        for (const layer of data.dataLayers || []) {
            for (const domain of layer.domains || []) {
                if (domain.jobName && domain.jobLinkToken) {
                    links[domain.jobName] = domain.jobLinkToken;
                }
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

        if (!data) return;

        const layers = (data.dataLayers || []).map((layer) => ({
            layer: layer.name,
            status: layer.status,
            domains: layer.domains?.length ?? 0,
            lastUpdated: layer.lastUpdated ?? null,
            hasLayerPortalToken: Boolean(layer.portalLinkToken),
        }));

        const domains = (data.dataLayers || []).flatMap((layer) =>
            (layer.domains || []).map((domain) => ({
                layer: layer.name,
                domain: domain.name,
                status: domain.status,
                path: domain.path ?? null,
                jobName: domain.jobName ?? null,
                lastUpdated: domain.lastUpdated ?? null,
                hasFolderPortalToken: Boolean(domain.portalLinkToken),
                hasJobPortalToken: Boolean(domain.jobLinkToken),
            })),
        );

        const resources = (data.resources || []).map((resource) => ({
            name: resource.name,
            resourceType: resource.resourceType,
            status: resource.status,
            hasAzureId: Boolean(resource.azureId),
            hasPortalToken: Boolean(resource.portalLinkToken),
        }));

        const recentJobs = (data.recentJobs || []).map((job) => ({
            jobName: job.jobName,
            status: job.status,
            startTime: job.startTime,
            duration: job.duration ?? null,
            hasJobLinkToken: Boolean(job.jobName && jobLinkTokens[job.jobName]),
        }));

        const hasAnyLinkToken =
            (data.dataLayers || []).some((layer) => Boolean(layer.portalLinkToken)) ||
            (data.dataLayers || []).some((layer) => (layer.domains || []).some((d) => Boolean(d.portalLinkToken || d.jobLinkToken))) ||
            (data.resources || []).some((r) => Boolean(r.portalLinkToken));

        console.groupCollapsed('[SystemStatusPage] system health payload', {
            overall: data.overall,
            layers: data.dataLayers?.length ?? 0,
            domains: domains.length,
            resources: data.resources?.length ?? 0,
            recentJobs: data.recentJobs?.length ?? 0,
            linkTokensPresent: hasAnyLinkToken,
        });
        console.table(layers);
        if (domains.length) console.table(domains);
        if (resources.length) console.table(resources);
        if (recentJobs.length) console.table(recentJobs);

        if ((data.dataLayers?.length ?? 0) === 0) {
            console.warn('[SystemStatusPage] No dataLayers returned from /api/system/health (check TEST_MODE and system-health config).');
        }
        if ((data.recentJobs?.length ?? 0) === 0) {
            console.warn('[SystemStatusPage] recentJobs is empty (ARM probes may be disabled; check SYSTEM_HEALTH_ARM_* env).');
        }
        if (!hasAnyLinkToken) {
            console.warn(
                '[SystemStatusPage] No portal/job link tokens present. Icons will be disabled. ' +
                'Common causes: missing SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID / SYSTEM_HEALTH_ARM_RESOURCE_GROUP / AZURE_STORAGE_ACCOUNT_NAME, ' +
                'or missing/invalid SYSTEM_HEALTH_LINK_TOKEN_SECRET.',
            );
        }

        console.info('[SystemStatusPage] jobLinkTokens index', {
            count: Object.keys(jobLinkTokens).length,
            sample: Object.keys(jobLinkTokens).slice(0, 10),
        });
        console.groupEnd();
    }, [isLoading, isFetching, data, error, jobLinkTokens]);

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
            <StatusOverview overall={overall} dataLayers={dataLayers} recentJobs={recentJobs} />

            {/* Secondary Details Grid */}
            <div className="grid grid-cols-1 gap-6">
                <ScheduledJobsPanel dataLayers={dataLayers} recentJobs={recentJobs} />
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
        </div>
    );
}
