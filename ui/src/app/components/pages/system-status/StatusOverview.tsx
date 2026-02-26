import React, { useMemo } from 'react';
import { DataDomain, DataLayer, JobRun } from '@/types/strategy';
import {
  formatTimeAgo,
  getStatusConfig,
  normalizeAzureJobName,
  normalizeAzurePortalUrl
} from './SystemStatusHelpers';
import { StatusTypos } from './StatusTokens';
import { getDomainOrderEntries } from './domainOrdering';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuShortcut,
  DropdownMenuTrigger
} from '@/app/components/ui/dropdown-menu';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { useLayerJobControl } from '@/hooks/useLayerJobControl';
import { Button } from '@/app/components/ui/button';
import { normalizeDomainKey } from './SystemPurgeControls';

const runStartEpoch = (raw?: string | null): number => {
  const value = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(value) ? value : Number.NEGATIVE_INFINITY;
};

function extractAzureJobName(jobUrl?: string | null): string | null {
  const normalized = normalizeAzurePortalUrl(jobUrl);
  if (!normalized) return null;
  const match = normalized.match(/\/jobs\/([^/?#]+)/);
  if (!match) return null;
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return match[1];
  }
}

interface StatusOverviewProps {
  overall: string;
  dataLayers: DataLayer[];
  recentJobs: JobRun[];
}

export function StatusOverview({
  overall,
  dataLayers,
  recentJobs
}: StatusOverviewProps) {
  const sysConfig = getStatusConfig(overall);
  const apiAnim =
    sysConfig.animation === 'spin'
      ? 'animate-spin'
      : sysConfig.animation === 'pulse'
        ? 'animate-pulse'
        : '';
  const { layerStates, triggerLayerJobs, suspendLayerJobs } = useLayerJobControl();

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
    return getDomainOrderEntries(dataLayers).map((entry) => entry.label);
  }, [dataLayers]);

  const jobIndex = useMemo(() => {
    const index = new Map<string, JobRun>();
    for (const job of recentJobs) {
      if (!job?.jobName) continue;
      const key = normalizeAzureJobName(job.jobName);
      if (!key) continue;
      const existing = index.get(key);
      if (!existing || runStartEpoch(job.startTime) > runStartEpoch(existing.startTime)) {
        index.set(key, job);
      }
    }
    return index;
  }, [recentJobs]);

  const domainsByLayer = useMemo(() => {
    const index = new Map<string, Map<string, DataDomain>>();

    for (const layer of dataLayers) {
      const domainIndex = new Map<string, DataDomain>();
      for (const domain of layer.domains || []) {
        if (!domain?.name) continue;
        const domainKey = normalizeDomainKey(String(domain.name).trim());
        if (!domainKey) continue;
        if (!domainIndex.has(domainKey)) {
          domainIndex.set(domainKey, domain);
        }
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
                      <div className="flex items-center gap-3">
                        <span className="text-[10px] font-black uppercase tracking-widest text-mcm-walnut">
                          {metric.layer}
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
          </div>
        </div>
      </div>

      {/* Pipeline matrix intentionally hidden; controls and layer coverage moved to Domain Layer Coverage panel. */}
      <div className="hidden rounded-[1.6rem] border-2 border-mcm-walnut bg-mcm-paper p-6 shadow-[8px_8px_0px_0px_rgba(119,63,26,0.1)] overflow-hidden">
        <div className="mb-4">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
            <div className="min-w-0 flex-1">
              <div className="flex min-w-0 flex-wrap items-baseline gap-x-3 gap-y-1">
                <h2 className="text-lg font-black tracking-tighter uppercase text-mcm-walnut">
                  Pipeline Health & Control Matrix
                </h2>
                <p className="text-sm italic text-mcm-olive">
                  Layer-by-layer health, data freshness, and operational controls for each domain.
                </p>
              </div>
            </div>
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
                  const layerUpdatedAgo = layer.lastUpdated ? formatTimeAgo(layer.lastUpdated) : '--';
                  const containerConfig = metric?.containerConfig ?? getStatusConfig(layer.status);
                  const containerLabel =
                    metric?.containerLabel ?? String(layer.status || '').toUpperCase();
                  const jobConfig = metric?.jobConfig ?? getStatusConfig('pending');
                  const jobLabel = metric?.jobLabel ?? 'N/A';
                  const jobStatusKey = metric?.jobStatusKey ?? 'pending';
                  const layerState = layerStates[layer.name];
                  const isLayerLoading = layerState?.isLoading;

                  return (
                    <TableHead key={layer.name} colSpan={2} className={matrixCell}>
                      <div className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <span className="font-bold text-mcm-walnut truncate">{layer.name}</span>
                            <span
                              className="inline-flex items-center rounded-sm border px-2 py-1 text-[9px] font-black uppercase tracking-widest opacity-90"
                              style={{
                                backgroundColor: containerConfig.bg,
                                color: containerConfig.text,
                                borderColor: containerConfig.border
                              }}
                            >
                              {containerLabel}
                            </span>
                            <span
                              className="inline-flex items-center rounded-sm border px-2 py-1 text-[9px] font-black uppercase tracking-widest opacity-90"
                              style={{
                                backgroundColor: jobConfig.bg,
                                color: jobConfig.text,
                                borderColor: jobConfig.border
                              }}
                            >
                              {jobLabel}
                            </span>
                          </div>
                          <div className={`${StatusTypos.MONO} mt-1 text-[10px] text-mcm-walnut/60`}>
                            data {String(layer.status || 'unknown').toUpperCase()} • jobs{' '}
                            {String(jobStatusKey || 'pending').toUpperCase()} • updated {layerUpdatedAgo}
                          </div>
                        </div>
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              className="h-7 px-2 text-[10px] uppercase tracking-widest text-mcm-walnut/65 hover:text-mcm-walnut"
                              aria-label={`${layer.name} tier actions`}
                            >
                              Tier actions
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end" className="min-w-[200px]">
                            <DropdownMenuLabel className="text-xs">{layer.name} tier</DropdownMenuLabel>
                            <DropdownMenuItem
                              disabled={isLayerLoading}
                              onSelect={() => void suspendLayerJobs(layer, true)}
                            >
                              Stop all jobs
                            </DropdownMenuItem>
                            <DropdownMenuItem
                              disabled={isLayerLoading}
                              onSelect={() => void suspendLayerJobs(layer, false)}
                            >
                              Resume all jobs
                            </DropdownMenuItem>
                            <DropdownMenuItem
                              disabled={isLayerLoading}
                              onSelect={() => void triggerLayerJobs(layer)}
                            >
                              Trigger all jobs
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
                                  <span className="flex-1 leading-none">Open container</span>
                                  <DropdownMenuShortcut>Azure</DropdownMenuShortcut>
                                </a>
                              </DropdownMenuItem>
                            ) : (
                              <DropdownMenuItem disabled>
                                <span className="flex-1 leading-none">Open container</span>
                                <DropdownMenuShortcut>n/a</DropdownMenuShortcut>
                              </DropdownMenuItem>
                            )}
                          </DropdownMenuContent>
                        </DropdownMenu>
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
                      const domain = domainsByLayer.get(layer.name)?.get(domainKey);
                      return (
                        <React.Fragment key={layer.name}>
                          <TableCell className={`${matrixCell} text-left`}>
                            {domain ? (
                              (() => {
                                const jobName =
                                  String(domain.jobName || '').trim() ||
                                  extractAzureJobName(domain.jobUrl) ||
                                  '';
                                const jobKey = normalizeAzureJobName(jobName);
                                const run = jobKey ? jobIndex.get(jobKey) : null;
                                const updatedAgo = domain.lastUpdated
                                  ? formatTimeAgo(domain.lastUpdated)
                                  : '--';
                                const dataStatusKey =
                                  String(domain.status || '')
                                    .trim()
                                    .toLowerCase() || 'pending';
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

                                return (
                                  <div className="space-y-1 py-1">
                                    <div className="inline-flex items-center gap-2">
                                      <span
                                        className={`${StatusTypos.MONO} text-[10px] font-bold uppercase tracking-wide text-mcm-walnut/80`}
                                      >
                                        data {dataLabel}
                                      </span>
                                      <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/60`}>
                                        {updatedAgo}
                                      </span>
                                    </div>
                                    <div className="inline-flex items-center gap-2">
                                      <span
                                        className={`${StatusTypos.MONO} text-[10px] font-bold uppercase tracking-wide text-mcm-walnut/80`}
                                      >
                                        jobs {jobLabel}
                                      </span>
                                      <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/55`}>
                                        {jobName ? 'job configured' : 'job n/a'}
                                      </span>
                                    </div>
                                  </div>
                                );
                              })()
                            ) : (
                              <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/30`}>
                                —
                              </span>
                            )}
                          </TableCell>

                          <TableCell className={`${matrixCell} text-center`}>
                            {domain ? (
                              <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/55`}>
                                Controls moved to Domain Layer Coverage
                              </span>
                            ) : (
                              <span className={`${StatusTypos.MONO} text-[10px] text-mcm-walnut/30`}>
                                —
                              </span>
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
