import React, { useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';

import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import { DataDomain, DataLayer, JobRun } from '@/types/strategy';
import {
  formatTimeAgo,
  getAzureJobExecutionsUrl,
  getStatusConfig,
  normalizeAzureJobName,
  normalizeAzurePortalUrl
} from './SystemStatusHelpers';
import { StatusTypos } from './StatusTokens';
import {
  AlertTriangle,
  CalendarDays,
  CirclePause,
  CirclePlay,
  Database,
  ExternalLink,
  FolderOpen,
  Loader2,
  MoreHorizontal,
  Play,
  RefreshCw,
  ScrollText,
  Trash2
} from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger
} from '@/app/components/ui/dropdown-menu';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle
} from '@/app/components/ui/alert-dialog';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import { useJobSuspend } from '@/hooks/useJobSuspend';
import { useLayerJobControl } from '@/hooks/useLayerJobControl';
import { Button } from '@/app/components/ui/button';
import { normalizeDomainKey, normalizeLayerKey } from './SystemPurgeControls';
import { DomainMetadataSheet, DomainMetadataSheetTarget } from './DomainMetadataSheet';

const ENABLE_STATUS_DIAGNOSTICS =
  import.meta.env.DEV ||
  ['1', 'true', 'yes', 'y', 'on'].includes(
    String(import.meta.env.VITE_DEBUG_API ?? '')
      .trim()
      .toLowerCase()
  );

interface StatusOverviewProps {
  overall: string;
  dataLayers: DataLayer[];
  recentJobs: JobRun[];
  jobStates?: Record<string, string>;
  onRefresh?: () => void;
  isRefreshing?: boolean;
  isFetching?: boolean;
}

export function StatusOverview({
  overall,
  dataLayers,
  recentJobs,
  jobStates,
  onRefresh,
  isRefreshing,
  isFetching
}: StatusOverviewProps) {
  const sysConfig = getStatusConfig(overall);
  const apiAnim =
    sysConfig.animation === 'spin'
      ? 'animate-spin'
      : sysConfig.animation === 'pulse'
        ? 'animate-pulse'
        : '';
  const { triggeringJob, triggerJob } = useJobTrigger();
  const { jobControl, setJobSuspended } = useJobSuspend();
  const { layerStates, triggerLayerJobs, suspendLayerJobs } = useLayerJobControl();
  const queryClient = useQueryClient();

  const [purgeTarget, setPurgeTarget] = useState<{
    layer: string;
    domain: string;
    displayLayer: string;
    displayDomain: string;
  } | null>(null);
  const [isPurging, setIsPurging] = useState(false);
  const [metadataTarget, setMetadataTarget] = useState<DomainMetadataSheetTarget | null>(null);

  const confirmPurge = async () => {
    if (!purgeTarget) return;
    setIsPurging(true);
    try {
      const result = await DataService.purgeData({
        scope: 'layer-domain',
        layer: purgeTarget.layer,
        domain: purgeTarget.domain,
        confirm: true
      });
      toast.success(`Purged ${result.totalDeleted} blob(s).`);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      toast.error(`Purge failed: ${message}`);
    } finally {
      setIsPurging(false);
      setPurgeTarget(null);
    }
  };

  const centralClock = (() => {
    const now = new Date();

    const time = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/Chicago',
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(now);

    const tzRaw =
      new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/Chicago',
        timeZoneName: 'short'
      })
        .formatToParts(now)
        .find((part) => part.type === 'timeZoneName')?.value ?? '';

    const tz = (() => {
      const value = String(tzRaw || '').trim();
      if (!value) return 'CST';
      if (value === 'CST' || value === 'CDT') return value;
      if (/central.*daylight/i.test(value)) return 'CDT';
      if (/central.*standard/i.test(value)) return 'CST';

      const offsetMatch = value.match(/(?:GMT|UTC)([+-]\d{1,2})(?::?(\d{2}))?/i);
      if (!offsetMatch) return 'CST';

      const hours = Number.parseInt(offsetMatch[1] || '0', 10);
      const minutes = Number.parseInt(offsetMatch[2] || '0', 10);
      const total = hours * 60 + (hours < 0 ? -minutes : minutes);
      if (total === -360) return 'CST';
      if (total === -300) return 'CDT';
      return 'CST';
    })();

    return { time, tz };
  })();

  const overallLabel = String(overall || '')
    .trim()
    .toUpperCase();

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
      const key = normalizeAzureJobName(job.jobName);
      if (!key) continue;
      const existing = index.get(key);
      if (!existing || String(job.startTime || '') > String(existing.startTime || '')) {
        index.set(key, job);
      }
    }
    return index;
  }, [recentJobs]);

  useEffect(() => {
    if (!ENABLE_STATUS_DIAGNOSTICS) return;

    const configuredJobs = new Set<string>();
    const configuredRawNames = new Map<string, string>();
    for (const layer of dataLayers) {
      for (const domain of layer.domains || []) {
        const rawJobName = String(domain?.jobName || '').trim();
        if (!rawJobName) continue;
        const key = normalizeAzureJobName(rawJobName);
        if (!key) continue;
        configuredJobs.add(key);
        configuredRawNames.set(key, rawJobName);
      }
    }

    if (configuredJobs.size === 0) {
      console.info('[StatusOverview] no configured job names found in dataLayers payload');
      return;
    }

    const missingConfiguredJobs = Array.from(configuredJobs).filter((key) => !jobIndex.has(key));
    if (missingConfiguredJobs.length === 0) {
      console.info('[StatusOverview] configured jobs matched recentJobs', {
        configuredCount: configuredJobs.size,
        recentJobsCount: recentJobs.length
      });
      return;
    }

    const configuredPreview = missingConfiguredJobs
      .slice(0, 20)
      .map((key) => configuredRawNames.get(key) || key);
    const recentPreview = Array.from(jobIndex.keys()).slice(0, 20);
    console.warn('[StatusOverview] configured jobs missing in recentJobs', {
      configuredCount: configuredJobs.size,
      missingCount: missingConfiguredJobs.length,
      recentJobsCount: recentJobs.length,
      missingConfiguredJobs: configuredPreview,
      recentJobKeys: recentPreview
    });
  }, [dataLayers, jobIndex, recentJobs]);

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

  const medallionMetrics = useMemo(() => {
    return dataLayers.map((layer) => {
      const containerStatusKey =
        String(layer.status || '')
          .trim()
          .toLowerCase() || 'pending';
      const containerConfig = getStatusConfig(containerStatusKey);
      const containerLabel = (() => {
        if (containerStatusKey === 'healthy' || containerStatusKey === 'success') return 'OK';
        if (
          containerStatusKey === 'stale' ||
          containerStatusKey === 'warning' ||
          containerStatusKey === 'degraded'
        )
          return 'STALE';
        if (
          containerStatusKey === 'error' ||
          containerStatusKey === 'failed' ||
          containerStatusKey === 'critical'
        )
          return 'ERR';
        if (containerStatusKey === 'pending') return 'PENDING';
        return containerStatusKey.toUpperCase();
      })();

      let total = 0;
      let running = 0;
      let failed = 0;
      let success = 0;
      let pending = 0;

      for (const domain of layer.domains || []) {
        if (!domain?.jobName) continue;
        total += 1;
        const key = normalizeAzureJobName(domain.jobName);
        const job = key ? jobIndex.get(key) : undefined;
        if (!job) {
          pending += 1;
          continue;
        }
        if (job.status === 'running') running += 1;
        else if (job.status === 'failed') failed += 1;
        else if (job.status === 'success') success += 1;
        else pending += 1;
      }

      const jobStatusKey = (() => {
        if (total === 0) return 'pending';
        if (failed > 0) return 'failed';
        if (running > 0) return 'running';
        if (pending > 0) return 'pending';
        if (success === total) return 'success';
        return 'pending';
      })();

      const jobConfig = getStatusConfig(jobStatusKey);

      const jobLabel = (() => {
        const key = String(jobStatusKey || '').toLowerCase();
        if (total === 0) return 'N/A';
        if (key === 'success') return 'OK';
        if (key === 'failed') return 'FAIL';
        if (key === 'running') return 'RUN';
        if (key === 'pending') return 'PENDING';
        return key.toUpperCase();
      })();

      return {
        layer: layer.name,
        containerStatusKey,
        containerConfig,
        containerLabel,
        total,
        running,
        failed,
        success,
        pending,
        jobStatusKey,
        jobConfig,
        jobLabel
      };
    });
  }, [dataLayers, jobIndex]);

  const medallionIndex = useMemo(() => {
    const index = new Map<string, (typeof medallionMetrics)[number]>();
    for (const metric of medallionMetrics) {
      index.set(metric.layer, metric);
    }
    return index;
  }, [medallionMetrics]);

  const matrixCell = 'bg-mcm-paper border border-mcm-walnut/15 rounded-none';
  const matrixHead = `${matrixCell} uppercase tracking-widest text-[10px] font-black text-mcm-walnut/70`;

  return (
    <div className="grid gap-6 font-sans">
      <AlertDialog
        open={Boolean(purgeTarget)}
        onOpenChange={(open) => (!open ? setPurgeTarget(null) : undefined)}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-destructive" />
              Confirm purge
            </AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete all blobs for{' '}
              <strong>
                {purgeTarget
                  ? `${purgeTarget.displayLayer} • ${purgeTarget.displayDomain}`
                  : 'selected scope'}
              </strong>
              . Containers remain, but the data cannot be recovered.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isPurging}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => void confirmPurge()}
              disabled={isPurging}
            >
              Purge
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <DomainMetadataSheet
        target={metadataTarget}
        open={Boolean(metadataTarget)}
        onOpenChange={(open) => {
          if (!open) setMetadataTarget(null);
        }}
      />

      {/* System Header - Manual inline styles for specific 'Industrial' theming overrides */}
      <div
        className="flex items-center gap-5 px-6 py-4 border-2 rounded-[1.6rem] border-l-[6px] border-mcm-walnut bg-mcm-paper shadow-[8px_8px_0px_0px_rgba(119,63,26,0.1)]"
        style={{
          borderLeftColor: sysConfig.text
        }}
      >
        <div className="flex items-center gap-3">
          <sysConfig.icon className={`h-8 w-8 ${apiAnim}`} style={{ color: sysConfig.text }} />
          <div>
            <h1 className={StatusTypos.HEADER}>SYSTEM STATUS</h1>
            <div
              className={`${StatusTypos.MONO} text-xl font-black tracking-tighter uppercase`}
              style={{ color: sysConfig.text }}
            >
              {overallLabel}
            </div>
          </div>
        </div>
        <div className="flex flex-1 items-center gap-6 min-w-0">
          <div className="hidden lg:flex flex-1 items-center justify-center rounded-[1.2rem] border-2 border-mcm-walnut/15 bg-mcm-cream/60 p-2 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.08)]">
            <div className="flex w-full flex-nowrap items-stretch gap-2 overflow-x-auto">
              {medallionMetrics.map((metric) => (
                <Tooltip key={metric.layer}>
                  <TooltipTrigger asChild>
                    <div className="min-w-[260px] shrink-0 flex-1 overflow-hidden rounded-[1rem] border-2 border-mcm-walnut/25 bg-mcm-paper px-3 py-2">
                      <div className="flex items-center justify-between gap-3">
                        <span className="text-[10px] font-black uppercase tracking-widest text-mcm-walnut">
                          {metric.layer}
                        </span>
                        <span className="inline-flex items-center gap-2">
                          <span className="inline-flex items-center gap-1">
                            <span className="inline-flex w-4 items-center justify-center shrink-0 text-mcm-walnut/60">
                              <Database className="h-3.5 w-3.5" />
                            </span>
                            <span
                              className="inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] font-black uppercase tracking-widest"
                              style={{
                                backgroundColor: metric.containerConfig.bg,
                                color: metric.containerConfig.text,
                                borderColor: metric.containerConfig.border
                              }}
                            >
                              {metric.containerLabel}
                            </span>
                          </span>
                          <span className="inline-flex items-center gap-1">
                            <span className="inline-flex w-4 items-center justify-center shrink-0 text-mcm-walnut/60">
                              <ScrollText className="h-3.5 w-3.5" />
                            </span>
                            <span
                              className="inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] font-black uppercase tracking-widest"
                              style={{
                                backgroundColor: metric.jobConfig.bg,
                                color: metric.jobConfig.text,
                                borderColor: metric.jobConfig.border
                              }}
                            >
                              {metric.jobLabel}
                            </span>
                          </span>
                        </span>
                      </div>
                      <div className="mt-1 flex items-center gap-3">
                        <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/80`}>
                          jobs {metric.total}
                        </span>
                        <span className={`${StatusTypos.MONO} text-[10px] text-mcm-teal`}>
                          run {metric.running}
                        </span>
                        <span className={`${StatusTypos.MONO} text-[10px] text-destructive`}>
                          fail {metric.failed}
                        </span>
                      </div>
                    </div>
                  </TooltipTrigger>
                  <TooltipContent side="bottom">
                    {metric.layer} • Data {metric.containerStatusKey.toUpperCase()} • Jobs{' '}
                    {metric.jobStatusKey.toUpperCase()} ( total {metric.total}, ok {metric.success},
                    run {metric.running}, fail {metric.failed}, pending {metric.pending})
                  </TooltipContent>
                </Tooltip>
              ))}
            </div>
          </div>
          <div className="flex shrink-0 flex-col items-end gap-2">
            <div className="inline-flex w-[220px] items-center gap-2 rounded-full border-2 border-mcm-walnut/15 bg-mcm-cream/60 px-3 py-1 shadow-[6px_6px_0px_0px_rgba(119,63,26,0.08)]">
              <span className={`${StatusTypos.HEADER} text-[10px] text-mcm-olive`}>
                UPTIME CLOCK
              </span>
              <span className={`${StatusTypos.MONO} text-sm text-mcm-walnut/70`}>
                {centralClock.time} {centralClock.tz}
              </span>
            </div>

            <Button
              variant="outline"
              size="sm"
              className="h-8 w-[220px] px-3 text-xs"
              onClick={onRefresh}
              disabled={!onRefresh || isFetching || isRefreshing}
            >
              <RefreshCw
                className={`h-4 w-4 ${isFetching || isRefreshing ? 'animate-spin' : ''}`}
              />
              Refresh now
            </Button>
          </div>
        </div>
      </div>

      {/* Domain x Layer Matrix (Recovered from 1bba1b8f presentation) */}
      <div className="rounded-[1.6rem] border-2 border-mcm-walnut bg-mcm-paper p-6 shadow-[8px_8px_0px_0px_rgba(119,63,26,0.1)] overflow-hidden">
        <div className="mb-4 flex items-start justify-between gap-4">
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
              <div className="flex items-center gap-2 whitespace-nowrap">
                <ScrollText className="h-4 w-4 text-mcm-walnut/70" />
                <h2 className="text-lg font-black tracking-tighter uppercase text-mcm-walnut">
                  Status Matrix
                </h2>
              </div>
              <p className="text-sm italic text-mcm-olive">
                At-a-glance health, freshness, and quick links across the medallion layers.
              </p>
            </div>
          </div>
          <div
            className={`${StatusTypos.HEADER} inline-flex items-center whitespace-nowrap rounded-full border-2 border-mcm-walnut/15 bg-mcm-cream/60 px-3 py-1 text-mcm-olive`}
          >
            Domain × Layer
          </div>
        </div>
        <div className="relative rounded-[1.6rem] overflow-hidden bg-mcm-paper">
          <Table className="text-[11px] table-fixed border-collapse border-spacing-0">
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead
                  rowSpan={dataLayers.length ? 2 : 1}
                  className={`${matrixHead} w-[220px]`}
                >
                  DOMAIN
                </TableHead>
                {dataLayers.map((layer) => {
                  const metric = medallionIndex.get(layer.name);
                  const layerUpdatedAgo = layer.lastUpdated
                    ? formatTimeAgo(layer.lastUpdated)
                    : '--';
                  const containerConfig = metric?.containerConfig ?? getStatusConfig(layer.status);
                  const containerLabel =
                    metric?.containerLabel ?? String(layer.status || '').toUpperCase();
                  const jobConfig = metric?.jobConfig ?? getStatusConfig('pending');
                  const jobLabel = metric?.jobLabel ?? 'N/A';
                  const jobStatusKey = metric?.jobStatusKey ?? 'pending';
                  const layerState = layerStates[layer.name];
                  const isLayerLoading = layerState?.isLoading;
                  const layerAction = layerState?.action;

                  return (
                    <TableHead key={layer.name} colSpan={2} className={matrixCell}>
                      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
                        <div className="flex items-center gap-2 min-w-0">
                          <span className="font-bold text-mcm-walnut truncate">{layer.name}</span>
                          <div className="flex items-center gap-1 shrink-0">
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span
                                  className="inline-flex items-center gap-1 rounded-sm border px-2 py-1 text-[9px] font-black uppercase tracking-widest opacity-90"
                                  style={{
                                    backgroundColor: containerConfig.bg,
                                    color: containerConfig.text,
                                    borderColor: containerConfig.border
                                  }}
                                >
                                  <span className="inline-flex w-4 items-center justify-center shrink-0">
                                    <Database className="h-3.5 w-3.5" />
                                  </span>
                                  {containerLabel}
                                </span>
                              </TooltipTrigger>
                              <TooltipContent side="bottom">
                                Container • {String(layer.status || 'unknown').toUpperCase()} •
                                Updated {layerUpdatedAgo} ago
                              </TooltipContent>
                            </Tooltip>
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span
                                  className="inline-flex items-center gap-1 rounded-sm border px-2 py-1 text-[9px] font-black uppercase tracking-widest opacity-90"
                                  style={{
                                    backgroundColor: jobConfig.bg,
                                    color: jobConfig.text,
                                    borderColor: jobConfig.border
                                  }}
                                >
                                  <span className="inline-flex w-4 items-center justify-center shrink-0">
                                    <ScrollText className="h-3.5 w-3.5" />
                                  </span>
                                  {jobLabel}
                                </span>
                              </TooltipTrigger>
                              <TooltipContent side="bottom">
                                Jobs • {String(jobStatusKey || 'pending').toUpperCase()}
                                {metric
                                  ? ` • total ${metric.total}, ok ${metric.success}, run ${metric.running}, fail ${metric.failed}, pending ${metric.pending}`
                                  : ''}
                              </TooltipContent>
                            </Tooltip>
                          </div>
                        </div>
                        <div className="flex items-center">
                          <DropdownMenu>
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <DropdownMenuTrigger asChild>
                                  <button
                                    type="button"
                                    className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-mcm-walnut/15 bg-mcm-cream/60 text-mcm-walnut/60 hover:bg-mcm-cream hover:text-mcm-teal focus:outline-none focus:ring-2 focus:ring-mcm-teal/30"
                                    aria-label={`${layer.name} tier actions`}
                                  >
                                    <MoreHorizontal className="h-4 w-4" />
                                  </button>
                                </DropdownMenuTrigger>
                              </TooltipTrigger>
                              <TooltipContent side="bottom">Tier actions</TooltipContent>
                            </Tooltip>
                            <DropdownMenuContent align="end" className="min-w-[200px]">
                              <DropdownMenuLabel className="text-xs">
                                {layer.name} tier
                              </DropdownMenuLabel>
                              <DropdownMenuItem
                                disabled={isLayerLoading}
                                onSelect={() => void suspendLayerJobs(layer, true)}
                              >
                                {isLayerLoading && layerAction === 'suspend' ? (
                                  <Loader2 className="h-4 w-4 shrink-0 animate-spin" />
                                ) : (
                                  <CirclePause className="h-4 w-4 shrink-0" />
                                )}
                                <span className="flex-1 leading-none">Suspend all jobs</span>
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={isLayerLoading}
                                onSelect={() => void suspendLayerJobs(layer, false)}
                              >
                                {isLayerLoading && layerAction === 'resume' ? (
                                  <Loader2 className="h-4 w-4 shrink-0 animate-spin" />
                                ) : (
                                  <CirclePlay className="h-4 w-4 shrink-0" />
                                )}
                                <span className="flex-1 leading-none">Resume all jobs</span>
                              </DropdownMenuItem>
                              <DropdownMenuItem
                                disabled={isLayerLoading}
                                onSelect={() => void triggerLayerJobs(layer)}
                              >
                                {isLayerLoading && layerAction === 'trigger' ? (
                                  <Loader2 className="h-4 w-4 shrink-0 animate-spin" />
                                ) : (
                                  <Play className="h-4 w-4 shrink-0" />
                                )}
                                <span className="flex-1 leading-none">Trigger all jobs</span>
                              </DropdownMenuItem>
                              <DropdownMenuSeparator />
                              {normalizeAzurePortalUrl(layer.portalUrl) ? (
                                <DropdownMenuItem asChild>
                                  <a
                                    href={normalizeAzurePortalUrl(layer.portalUrl)}
                                    target="_blank"
                                    rel="noreferrer"
                                    aria-label={`Open ${layer.name} container`}
                                  >
                                    <Database className="h-4 w-4 shrink-0" />
                                    <span className="flex-1 leading-none">Open container</span>
                                    <DropdownMenuShortcut>Azure</DropdownMenuShortcut>
                                  </a>
                                </DropdownMenuItem>
                              ) : (
                                <DropdownMenuItem disabled>
                                  <Database className="h-4 w-4 shrink-0" />
                                  <span className="flex-1 leading-none">Open container</span>
                                  <DropdownMenuShortcut>n/a</DropdownMenuShortcut>
                                </DropdownMenuItem>
                              )}
                            </DropdownMenuContent>
                          </DropdownMenu>
                        </div>
                      </div>
                    </TableHead>
                  );
                })}
              </TableRow>

              {dataLayers.length > 0 && (
                <TableRow className="hover:bg-transparent">
                  {dataLayers.map((layer) => {
                    return (
                      <React.Fragment key={layer.name}>
                        <TableHead className={`${matrixHead} h-8 text-center w-[96px]`}>
                          STATUS
                        </TableHead>
                        <TableHead className={`${matrixHead} h-8 text-center w-[140px]`}>
                          ACTIONS
                        </TableHead>
                      </React.Fragment>
                    );
                  })}
                </TableRow>
              )}
            </TableHeader>

            <TableBody>
              {domainNames.map((domainName) => {
                const domainKey = normalizeDomainKey(domainName);
                return (
                  <TableRow
                    className="group even:[&>td]:bg-mcm-cream/15 hover:[&>td]:bg-mcm-cream/35 [&>td]:transition-colors"
                    key={domainName}
                  >
                    <TableCell className={`${matrixCell} text-sm font-semibold text-mcm-walnut`}>
                      <span>{domainName}</span>
                    </TableCell>

                    {dataLayers.map((layer) => {
                      const domain = domainsByLayer.get(layer.name)?.get(domainName);
                      const layerKey = normalizeLayerKey(layer.name);

                      return (
                        <React.Fragment key={layer.name}>
                          <TableCell className={`${matrixCell} text-center`}>
                            {domain ? (
                              (() => {
                                const pathText = String(domain.path || '').toLowerCase();
                                const isByDate =
                                  pathText.includes('by-date') || pathText.includes('_by_date');

                                const domainPortalUrl = normalizeAzurePortalUrl(domain.portalUrl);
                                const byDateFolderUrl = isByDate ? domainPortalUrl : '';
                                const baseFolderUrl = (() => {
                                  if (!domainPortalUrl) return '';
                                  if (!isByDate) return domainPortalUrl;
                                  const derived = domainPortalUrl
                                    .replace(/\/by-date\b/gi, '')
                                    .replace(/-by-date\b/gi, '')
                                    .replace(/_by_date\b/gi, '');
                                  return derived === domainPortalUrl ? domainPortalUrl : derived;
                                })();
                                const showByDateFolder =
                                  Boolean(byDateFolderUrl) && baseFolderUrl !== byDateFolderUrl;

                                const extractAzureJobName = (
                                  jobUrl?: string | null
                                ): string | null => {
                                  const normalized = normalizeAzurePortalUrl(jobUrl);
                                  if (!normalized) return null;
                                  const match = normalized.match(/\/jobs\/([^/?#]+)/);
                                  if (!match) return null;
                                  try {
                                    return decodeURIComponent(match[1]);
                                  } catch {
                                    return match[1];
                                  }
                                };

                                const jobName =
                                  String(domain.jobName || '').trim() ||
                                  extractAzureJobName(domain.jobUrl) ||
                                  '';
                                const jobKey = normalizeAzureJobName(jobName);
                                const run = jobKey ? jobIndex.get(jobKey) : null;
                                const jobPortalUrl = normalizeAzurePortalUrl(domain.jobUrl);

                                const updatedAgo = domain.lastUpdated
                                  ? formatTimeAgo(domain.lastUpdated)
                                  : '--';

                                const dataStatusKey =
                                  String(domain.status || '')
                                    .trim()
                                    .toLowerCase() || 'pending';
                                const dataConfig = getStatusConfig(dataStatusKey);
                                const dataLabel = (() => {
                                  const key = String(dataStatusKey || '').toLowerCase();
                                  if (key === 'healthy') return 'OK';
                                  if (key === 'stale' || key === 'warning' || key === 'degraded')
                                    return 'STALE';
                                  if (key === 'error' || key === 'failed' || key === 'critical')
                                    return 'ERR';
                                  if (key === 'pending') return 'PENDING';
                                  return key.toUpperCase();
                                })();

                                const jobStatusKey = (() => {
                                  const key = String(run?.status || '')
                                    .trim()
                                    .toLowerCase();
                                  if (!jobName) return 'pending';
                                  if (!run) return 'pending';
                                  if (
                                    key === 'running' ||
                                    key === 'failed' ||
                                    key === 'success' ||
                                    key === 'pending'
                                  )
                                    return key;
                                  return 'pending';
                                })();

                                const jobConfig = getStatusConfig(jobStatusKey);
                                const jobLabel = (() => {
                                  if (!jobName) return 'N/A';
                                  if (!run) return 'NO RUN';

                                  const key = String(jobStatusKey || '').toLowerCase();
                                  if (key === 'success' || key === 'succeeded') return 'OK';
                                  if (key === 'failed' || key === 'error') return 'FAIL';
                                  if (key === 'running') return 'RUN';
                                  if (key === 'pending') return 'PENDING';
                                  return key.toUpperCase();
                                })();

                                const hasLinks =
                                  Boolean(baseFolderUrl) ||
                                  Boolean(showByDateFolder) ||
                                  Boolean(jobPortalUrl);

                                return (
                                  <div className="flex items-center justify-center gap-2 whitespace-nowrap py-1">
                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <span
                                          tabIndex={0}
                                          className="inline-flex flex-col items-start gap-0.5 rounded-md px-1 py-0.5 focus:outline-none focus:ring-2 focus:ring-mcm-teal/30"
                                        >
                                          <span className="inline-flex items-center gap-1">
                                            <span className="inline-flex w-4 items-center justify-center shrink-0 text-mcm-walnut/60">
                                              <CalendarDays className="h-3.5 w-3.5" />
                                            </span>
                                            <span
                                              className="inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] font-black uppercase tracking-widest"
                                              style={{
                                                backgroundColor: dataConfig.bg,
                                                color: dataConfig.text,
                                                borderColor: dataConfig.border
                                              }}
                                            >
                                              {dataLabel}
                                            </span>
                                            <span
                                              className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/60`}
                                            >
                                              {updatedAgo}
                                            </span>
                                          </span>
                                          <span className="inline-flex items-center gap-1">
                                            <span className="inline-flex w-4 items-center justify-center shrink-0 text-mcm-walnut/60">
                                              <ScrollText className="h-3.5 w-3.5" />
                                            </span>
                                            <span
                                              className="inline-flex items-center rounded-full border px-2 py-0.5 text-[9px] font-black uppercase tracking-widest"
                                              style={{
                                                backgroundColor: jobConfig.bg,
                                                color: jobConfig.text,
                                                borderColor: jobConfig.border
                                              }}
                                            >
                                              {jobLabel}
                                            </span>
                                          </span>
                                        </span>
                                      </TooltipTrigger>
                                      <TooltipContent side="bottom">
                                        Data • {String(domain.status || 'unknown').toUpperCase()} •
                                        Updated {updatedAgo} ago
                                        {jobName
                                          ? run
                                            ? ` • Job ${run.status.toUpperCase()} • ${formatTimeAgo(run.startTime)} ago`
                                            : ' • Job NO RECENT RUN'
                                          : ' • Job not configured'}
                                      </TooltipContent>
                                    </Tooltip>

                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <button
                                          type="button"
                                          className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-mcm-walnut/15 bg-mcm-cream/60 text-mcm-walnut/60 hover:bg-mcm-cream hover:text-mcm-teal focus:outline-none focus:ring-2 focus:ring-mcm-teal/30"
                                          aria-label={`View ${layer.name} ${domainName} metadata`}
                                          onClick={() =>
                                            setMetadataTarget({
                                              layer: layerKey as DomainMetadataSheetTarget['layer'],
                                              domain: domainKey,
                                              displayLayer: layer.name,
                                              displayDomain: domainName,
                                              lastUpdated: domain.lastUpdated
                                            })
                                          }
                                        >
                                          <Database className="h-4 w-4 shrink-0" />
                                        </button>
                                      </TooltipTrigger>
                                      <TooltipContent side="bottom">Metadata</TooltipContent>
                                    </Tooltip>

                                    {hasLinks ? (
                                      <DropdownMenu>
                                        <Tooltip>
                                          <TooltipTrigger asChild>
                                            <DropdownMenuTrigger asChild>
                                              <button
                                                type="button"
                                                className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-mcm-walnut/15 bg-mcm-cream/60 text-mcm-walnut/60 hover:bg-mcm-cream hover:text-mcm-teal focus:outline-none focus:ring-2 focus:ring-mcm-teal/30"
                                                aria-label={`Open ${layer.name} ${domainName} links`}
                                              >
                                                <ExternalLink className="h-4 w-4 shrink-0" />
                                              </button>
                                            </DropdownMenuTrigger>
                                          </TooltipTrigger>
                                          <TooltipContent side="bottom">Open links</TooltipContent>
                                        </Tooltip>
                                        <DropdownMenuContent
                                          align="center"
                                          className="min-w-[220px]"
                                        >
                                          <DropdownMenuLabel className="text-xs">
                                            {layer.name} • {domainName}
                                          </DropdownMenuLabel>
                                          {baseFolderUrl ? (
                                            <DropdownMenuItem asChild>
                                              <a
                                                href={baseFolderUrl}
                                                target="_blank"
                                                rel="noreferrer"
                                              >
                                                <FolderOpen className="h-4 w-4 shrink-0" />
                                                <span className="flex-1 leading-none">
                                                  Data folder
                                                </span>
                                                <DropdownMenuShortcut>
                                                  {updatedAgo}
                                                </DropdownMenuShortcut>
                                              </a>
                                            </DropdownMenuItem>
                                          ) : null}
                                          {showByDateFolder ? (
                                            <DropdownMenuItem asChild>
                                              <a
                                                href={byDateFolderUrl}
                                                target="_blank"
                                                rel="noreferrer"
                                              >
                                                <CalendarDays className="h-4 w-4 shrink-0" />
                                                <span className="flex-1 leading-none">
                                                  By-date folder
                                                </span>
                                                <DropdownMenuShortcut>
                                                  {updatedAgo}
                                                </DropdownMenuShortcut>
                                              </a>
                                            </DropdownMenuItem>
                                          ) : null}
                                          {(baseFolderUrl || showByDateFolder) && jobPortalUrl ? (
                                            <DropdownMenuSeparator />
                                          ) : null}
                                          {jobPortalUrl ? (
                                            <DropdownMenuItem asChild>
                                              <a
                                                href={jobPortalUrl}
                                                target="_blank"
                                                rel="noreferrer"
                                              >
                                                <ExternalLink className="h-4 w-4 shrink-0" />
                                                <span className="flex-1 leading-none">
                                                  Job in Azure
                                                </span>
                                                <DropdownMenuShortcut>
                                                  {jobName
                                                    ? run
                                                      ? `${run.status.toUpperCase()} • ${formatTimeAgo(run.startTime)}`
                                                      : 'NO RUN'
                                                    : 'n/a'}
                                                </DropdownMenuShortcut>
                                              </a>
                                            </DropdownMenuItem>
                                          ) : null}
                                        </DropdownMenuContent>
                                      </DropdownMenu>
                                    ) : (
                                      <Tooltip>
                                        <TooltipTrigger asChild>
                                          <span
                                            tabIndex={0}
                                            className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-mcm-walnut/10 text-mcm-walnut/25"
                                            aria-label="No links configured"
                                          >
                                            <ExternalLink className="h-4 w-4 shrink-0" />
                                          </span>
                                        </TooltipTrigger>
                                        <TooltipContent side="bottom">
                                          No links configured
                                        </TooltipContent>
                                      </Tooltip>
                                    )}
                                  </div>
                                );
                              })()
                            ) : (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <span tabIndex={0} className="text-mcm-walnut/30">
                                    —
                                  </span>
                                </TooltipTrigger>
                                <TooltipContent side="bottom">Not applicable</TooltipContent>
                              </Tooltip>
                            )}
                          </TableCell>

                          <TableCell className={`${matrixCell} text-center`}>
                            {domain ? (
                              (() => {
                                const extractAzureJobName = (
                                  jobUrl?: string | null
                                ): string | null => {
                                  const normalized = normalizeAzurePortalUrl(jobUrl);
                                  if (!normalized) return null;
                                  const match = normalized.match(/\/jobs\/([^/?#]+)/);
                                  if (!match) return null;
                                  try {
                                    return decodeURIComponent(match[1]);
                                  } catch {
                                    return match[1];
                                  }
                                };

                                const jobName =
                                  String(domain.jobName || '').trim() ||
                                  extractAzureJobName(domain.jobUrl) ||
                                  '';
                                const jobKey = normalizeAzureJobName(jobName);
                                const run = jobKey ? jobIndex.get(jobKey) : null;
                                const actionJobName = String(run?.jobName || jobName).trim();
                                const isTriggering =
                                  Boolean(actionJobName) && triggeringJob === actionJobName;
                                const runningState = jobKey ? jobStates?.[jobKey] : undefined;
                                const isSuspended =
                                  String(runningState || '')
                                    .trim()
                                    .toLowerCase() === 'suspended';
                                const isControlling =
                                  Boolean(actionJobName) && jobControl?.jobName === actionJobName;
                                const isControlDisabled =
                                  Boolean(triggeringJob) || Boolean(jobControl);
                                const executionsUrl = getAzureJobExecutionsUrl(domain.jobUrl);
                                const actionButtonBase =
                                  'inline-flex h-7 w-7 items-center justify-center rounded-md border border-mcm-walnut/15 bg-mcm-cream/60 text-mcm-walnut/60 hover:bg-mcm-cream hover:text-mcm-teal focus:outline-none focus:ring-2 focus:ring-mcm-teal/30 disabled:opacity-40';
                                const actionButtonDisabled =
                                  'inline-flex h-7 w-7 items-center justify-center rounded-md border border-mcm-walnut/10 bg-mcm-cream/40 text-mcm-walnut/25';
                                const actionButtonDestructive =
                                  'inline-flex h-7 w-7 items-center justify-center rounded-md border border-mcm-walnut/15 bg-mcm-cream/60 text-mcm-walnut/60 hover:bg-mcm-cream hover:text-destructive focus:outline-none focus:ring-2 focus:ring-destructive/30 disabled:opacity-40';

                                return (
                                  <div className="inline-flex items-center gap-1">
                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        {executionsUrl ? (
                                          <a
                                            href={executionsUrl}
                                            target="_blank"
                                            rel="noreferrer"
                                            className={actionButtonBase}
                                            aria-label={`Open ${domainName} execution history`}
                                          >
                                            <ScrollText className="h-4 w-4" />
                                          </a>
                                        ) : (
                                          <span
                                            tabIndex={0}
                                            className={actionButtonDisabled}
                                            aria-label="Execution history unavailable"
                                          >
                                            <ScrollText className="h-4 w-4" />
                                          </span>
                                        )}
                                      </TooltipTrigger>
                                      <TooltipContent side="bottom">
                                        Execution history
                                        {run
                                          ? ` • ${run.status.toUpperCase()} • ${formatTimeAgo(run.startTime)} ago`
                                          : ''}
                                      </TooltipContent>
                                    </Tooltip>

                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        {!jobName || isControlDisabled ? (
                                          <span
                                            tabIndex={0}
                                            className={actionButtonDisabled}
                                            aria-label="Suspend/resume unavailable"
                                          >
                                            {isSuspended ? (
                                              <CirclePlay className="h-4 w-4" />
                                            ) : (
                                              <CirclePause className="h-4 w-4" />
                                            )}
                                          </span>
                                        ) : (
                                          <button
                                            type="button"
                                            className={actionButtonBase}
                                            aria-label={isSuspended ? 'Resume job' : 'Suspend job'}
                                            disabled={!jobName || isControlDisabled}
                                            onClick={() =>
                                              void setJobSuspended(actionJobName, !isSuspended)
                                            }
                                          >
                                            {isControlling ? (
                                              <Loader2 className="h-4 w-4 animate-spin" />
                                            ) : isSuspended ? (
                                              <CirclePlay className="h-4 w-4" />
                                            ) : (
                                              <CirclePause className="h-4 w-4" />
                                            )}
                                          </button>
                                        )}
                                      </TooltipTrigger>
                                      <TooltipContent side="bottom">
                                        {isSuspended ? 'Resume job' : 'Suspend job'}
                                      </TooltipContent>
                                    </Tooltip>

                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        {!jobName || Boolean(triggeringJob) ? (
                                          <span
                                            tabIndex={0}
                                            className={actionButtonDisabled}
                                            aria-label="Trigger unavailable"
                                          >
                                            <Play className="h-4 w-4" />
                                          </span>
                                        ) : (
                                          <button
                                            type="button"
                                            className={actionButtonBase}
                                            aria-label="Trigger job"
                                            disabled={!jobName || Boolean(triggeringJob)}
                                            onClick={() => void triggerJob(actionJobName)}
                                          >
                                            {isTriggering ? (
                                              <Loader2 className="h-4 w-4 animate-spin" />
                                            ) : (
                                              <Play className="h-4 w-4" />
                                            )}
                                          </button>
                                        )}
                                      </TooltipTrigger>
                                      <TooltipContent side="bottom">Trigger job</TooltipContent>
                                    </Tooltip>

                                    <Tooltip>
                                      <TooltipTrigger asChild>
                                        <button
                                          type="button"
                                          className={actionButtonDestructive}
                                          aria-label="Purge data"
                                          disabled={isPurging}
                                          onClick={() =>
                                            setPurgeTarget({
                                              layer: layerKey,
                                              domain: domainKey,
                                              displayLayer: layer.name,
                                              displayDomain: domainName
                                            })
                                          }
                                        >
                                          <Trash2 className="h-4 w-4" />
                                        </button>
                                      </TooltipTrigger>
                                      <TooltipContent side="bottom">Purge data</TooltipContent>
                                    </Tooltip>
                                  </div>
                                );
                              })()
                            ) : (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <span tabIndex={0} className="text-mcm-walnut/30">
                                    —
                                  </span>
                                </TooltipTrigger>
                                <TooltipContent side="bottom">Not applicable</TooltipContent>
                              </Tooltip>
                            )}
                          </TableCell>
                        </React.Fragment>
                      );
                    })}
                  </TableRow>
                );
              })}

              {domainNames.length === 0 && (
                <TableRow className="hover:bg-transparent">
                  <TableCell
                    colSpan={1 + dataLayers.length * 2}
                    className={`${matrixCell} py-10 text-center text-xs text-mcm-olive font-mono`}
                  >
                    No domains found
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
          <div className="pointer-events-none absolute inset-0 rounded-[1.6rem] border border-mcm-walnut/25" />
        </div>
      </div>
    </div>
  );
}
