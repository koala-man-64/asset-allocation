import React, { useEffect, useMemo, useState } from 'react';
import { useLineageQuery, useSignalsQuery, useSystemHealthQuery } from '@/hooks/useDataQueries';
import { StatusOverview } from './system-status/StatusOverview';
import { DataLayerHealth } from './system-status/DataLayerHealth';
import { JobMonitor } from './system-status/JobMonitor';
import { AlertHistory } from './system-status/AlertHistory';
import { AzureResources } from './system-status/AzureResources';
import { config } from '@/config';

export function SystemStatusPage() {
    const { data, isLoading, error, isFetching, dataUpdatedAt } = useSystemHealthQuery();
    const { data: lineage } = useLineageQuery();
    const { data: signals = [] } = useSignalsQuery();
    const [now, setNow] = useState(() => Date.now());

    useEffect(() => {
        console.info('[SystemStatusPage] Mounted', {
            apiBaseUrl: config.apiBaseUrl,
            origin: window.location.origin,
        });
        const handle = window.setInterval(() => setNow(Date.now()), 1000);
        return () => window.clearInterval(handle);
    }, []);

    useEffect(() => {
        if (!data) return;
        console.info('[SystemStatusPage] Data updated', {
            overall: data.overall,
            dataLayers: data.dataLayers?.length ?? 0,
            alerts: data.alerts?.length ?? 0,
            resources: data.resources?.length ?? 0,
            updatedAt: dataUpdatedAt,
        });
    }, [data, dataUpdatedAt]);

    useEffect(() => {
        if (!error) return;
        console.error('[SystemStatusPage] Data error', {
            error: error instanceof Error ? error.message : String(error),
        });
    }, [error]);

    const secondsSinceRefresh = useMemo(() => {
        if (!dataUpdatedAt) return null;
        return Math.max(0, Math.floor((now - dataUpdatedAt) / 1000));
    }, [dataUpdatedAt, now]);

    const impactsByDomain = useMemo<Record<string, string[]>>(() => {
        if (!lineage || typeof lineage !== 'object') return {};
        const impacts = (lineage as { impactsByDomain?: unknown }).impactsByDomain;
        if (!impacts || typeof impacts !== 'object') return {};
        return impacts as Record<string, string[]>;
    }, [lineage]);

    if (isLoading) {
        return (
            <div className="flex items-center justify-center h-full min-h-[50vh]">
                <div className="flex flex-col items-center gap-4">
                    <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                    <p className="text-muted-foreground text-sm">Loading system status...</p>
                </div>
            </div>
        );
    }

    if (error || !data) {
        return (
            <div className="p-6 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive">
                <h3 className="text-lg font-semibold mb-2">Error loading system status</h3>
                <p>{error ? (error as Error).message : 'No data available'}</p>
            </div>
        );
    }

    const { overall, dataLayers, recentJobs, alerts, resources } = data;

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">System Status</h1>
                    <p className="text-muted-foreground mt-1">Real-time monitoring of data layers, pipelines, and infrastructure</p>
                </div>
                <div className="flex items-center gap-3 text-sm text-muted-foreground">
                    <div className="flex items-center gap-2">
                        <span className={`h-2 w-2 rounded-full ${isFetching ? 'bg-emerald-500 animate-pulse' : 'bg-muted-foreground/40'}`} />
                        <span className="font-mono">
                            {secondsSinceRefresh === null ? '—' : `Last refresh: ${secondsSinceRefresh}s`}
                        </span>
                    </div>
                </div>
            </div>

            <StatusOverview overall={overall} dataLayers={dataLayers} recentJobs={recentJobs} />

            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                <JobMonitor recentJobs={recentJobs} />
                <AlertHistory alerts={alerts} />
            </div>

            <DataLayerHealth
                dataLayers={dataLayers}
                recentJobs={recentJobs}
                impactsByDomain={impactsByDomain}
                signals={signals}
            />

            {resources && resources.length > 0 && (
                <AzureResources resources={resources} />
            )}
        </div>
    );
}
