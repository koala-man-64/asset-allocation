import { Fragment, useEffect, useMemo, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Activity, ChevronDown, ExternalLink, Loader2, ScrollText, Server } from 'lucide-react';
import { DataService } from '@/services/DataService';
import type { ContainerAppStatusItem } from '@/services/apiService';
import { Badge } from '@/app/components/ui/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Switch } from '@/app/components/ui/switch';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { cn } from '@/app/components/ui/utils';
import { formatTimeAgo, getAzurePortalUrl } from './SystemStatusHelpers';

const QUERY_KEY = ['system', 'container-apps'] as const;
const LOG_TAIL_LINES = 50;

type AppLogState = {
  lines: string[];
  loading: boolean;
  error: string | null;
  marker: string | null;
};

function normalizeState(value?: string | null): string {
  return String(value || '')
    .trim()
    .toLowerCase();
}

function isAppRunning(app: ContainerAppStatusItem): boolean {
  const runningState = normalizeState(app.runningState);
  if (runningState.includes('stop')) return false;
  if (runningState.includes('run')) return true;
  if (runningState.includes('start')) return true;

  const provisioning = normalizeState(app.provisioningState);
  if (provisioning === 'succeeded') return true;
  if (provisioning === 'failed') return false;
  return false;
}

function statusBadgeClass(status: string): string {
  const normalized = normalizeState(status);
  if (normalized === 'healthy') return 'bg-emerald-500/15 text-emerald-700 border-emerald-500/30';
  if (normalized === 'warning') return 'bg-amber-500/15 text-amber-700 border-amber-500/30';
  if (normalized === 'error') return 'bg-rose-500/15 text-rose-700 border-rose-500/30';
  return 'bg-muted/40 text-muted-foreground border-border/60';
}

function HealthBadge({ app }: { app: ContainerAppStatusItem }) {
  const healthStatus = normalizeState(app.health?.status || app.status || 'unknown');
  const label = healthStatus ? healthStatus.toUpperCase() : 'UNKNOWN';
  return (
    <Badge variant="outline" className={cn('font-mono text-[10px] tracking-widest', statusBadgeClass(healthStatus))}>
      {label}
    </Badge>
  );
}

export function ContainerAppsPanel() {
  const queryClient = useQueryClient();
  const [pendingByName, setPendingByName] = useState<Record<string, boolean>>({});
  const [expandedAppName, setExpandedAppName] = useState<string | null>(null);
  const [logStateByName, setLogStateByName] = useState<Record<string, AppLogState>>({});
  const logControllers = useRef<Record<string, AbortController>>({});

  const containerAppsQuery = useQuery({
    queryKey: QUERY_KEY,
    queryFn: ({ signal }) => DataService.getContainerApps({ probe: true }, signal),
    staleTime: 1000 * 20,
    refetchInterval: 1000 * 30
  });

  const apps = useMemo(() => containerAppsQuery.data?.apps || [], [containerAppsQuery.data?.apps]);

  const setPending = (name: string, pending: boolean) => {
    setPendingByName((prev) => {
      if (!pending) {
        const next = { ...prev };
        delete next[name];
        return next;
      }
      return { ...prev, [name]: true };
    });
  };

  const toggleApp = async (app: ContainerAppStatusItem, nextEnabled: boolean) => {
    const name = String(app.name || '').trim();
    if (!name) return;
    setPending(name, true);
    try {
      if (nextEnabled) {
        await DataService.startContainerApp(name);
        toast.success(`Start command sent for ${name}.`);
      } else {
        await DataService.stopContainerApp(name);
        toast.success(`Stop command sent for ${name}.`);
      }
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: QUERY_KEY }),
        queryClient.invalidateQueries({ queryKey: ['systemHealth'] })
      ]);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      toast.error(`Failed to update ${name}: ${message}`);
    } finally {
      setPending(name, false);
    }
  };

  const getLogMarker = (app: ContainerAppStatusItem): string | null => {
    return String(app.health?.checkedAt || app.checkedAt || '').trim() || null;
  };

  const fetchAppLogs = (appName: string, marker: string | null) => {
    logControllers.current[appName]?.abort();
    const controller = new AbortController();
    logControllers.current[appName] = controller;

    setLogStateByName((prev) => ({
      ...prev,
      [appName]: {
        lines: prev[appName]?.lines ?? [],
        loading: true,
        error: null,
        marker
      }
    }));

    DataService.getContainerAppLogs(appName, { tail: LOG_TAIL_LINES }, controller.signal)
      .then((response) => {
        const logs = Array.isArray(response?.logs)
          ? response.logs
              .filter((line) => line !== undefined && line !== null)
              .map((line) => String(line))
              .slice(-LOG_TAIL_LINES)
          : [];
        setLogStateByName((prev) => ({
          ...prev,
          [appName]: {
            lines: logs,
            loading: false,
            error: null,
            marker
          }
        }));
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        setLogStateByName((prev) => ({
          ...prev,
          [appName]: {
            lines: [],
            loading: false,
            error: error instanceof Error ? error.message : String(error),
            marker
          }
        }));
      });
  };

  const toggleAppLogs = (app: ContainerAppStatusItem) => {
    const appName = String(app.name || '').trim();
    if (!appName) return;

    const isExpanded = expandedAppName === appName;
    if (isExpanded) {
      setExpandedAppName(null);
      return;
    }

    const marker = getLogMarker(app);
    const logState = logStateByName[appName];
    if (!logState || logState.marker !== marker) {
      fetchAppLogs(appName, marker);
    }
    setExpandedAppName(appName);
  };

  useEffect(() => {
    if (!expandedAppName) return;
    const app = apps.find((candidate) => String(candidate.name || '').trim() === expandedAppName);
    if (!app) return;
    const marker = getLogMarker(app);
    const current = logStateByName[expandedAppName];
    if (!current || current.marker !== marker) {
      fetchAppLogs(expandedAppName, marker);
    }
  }, [apps, expandedAppName, logStateByName]);

  useEffect(() => {
    const controllers = logControllers.current;
    return () => {
      Object.values(controllers).forEach((controller) => controller.abort());
    };
  }, []);

  if (containerAppsQuery.isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Server className="h-5 w-5" />
            Container Apps
          </CardTitle>
          <CardDescription>Loading container app health and controls…</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  if (containerAppsQuery.error) {
    const message =
      containerAppsQuery.error instanceof Error
        ? containerAppsQuery.error.message
        : String(containerAppsQuery.error);
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Server className="h-5 w-5" />
            Container Apps
          </CardTitle>
          <CardDescription>Container app controls are unavailable.</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="font-mono text-xs text-rose-500">{message}</div>
        </CardContent>
      </Card>
    );
  }

  return (
      <Card className="h-full flex flex-col">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Server className="h-5 w-5" />
          Container Apps
        </CardTitle>
        <CardDescription>
          Toggle API/UI container apps and run live accessibility checks.
        </CardDescription>
      </CardHeader>
      <CardContent className="flex-1 overflow-hidden">
        <div className="rounded-md border overflow-hidden">
          <div className="-my-2">
            <Table className="[&_[data-slot=table-head]]:px-5 [&_[data-slot=table-cell]]:px-5">
            <TableHeader>
              <TableRow>
                <TableHead>App</TableHead>
                <TableHead className="text-center">Enabled</TableHead>
                <TableHead className="text-center">Health</TableHead>
                <TableHead>Runtime</TableHead>
                <TableHead>Probe URL</TableHead>
                <TableHead>Last Checked</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {apps.map((app) => {
                const name = String(app.name || '');
                const enabled = isAppRunning(app);
                const isPending = Boolean(pendingByName[name]);
                const probeUrl = app.health?.url || null;
                const lastChecked = app.health?.checkedAt || app.checkedAt || null;
                const runningState = app.runningState || app.provisioningState || 'Unknown';
                const isExpanded = expandedAppName === name;
                const logState = logStateByName[name];
                const loadingLogs = Boolean(isExpanded && logState?.loading);

                return (
                  <Fragment key={name}>
                    <TableRow>
                      <TableCell className="font-medium">
                        <div className="flex items-center gap-2">
                          <span>{name}</span>
                          {app.azureId && (
                            <a
                              href={getAzurePortalUrl(app.azureId)}
                              target="_blank"
                              rel="noreferrer"
                              className="text-muted-foreground hover:text-primary transition-colors"
                              aria-label={`Open ${name} in Azure`}
                            >
                              <ExternalLink className="h-4 w-4" />
                            </a>
                          )}
                        </div>
                      </TableCell>
                      <TableCell className="text-center">
                        <Switch
                          checked={enabled}
                          disabled={isPending}
                          onCheckedChange={(next) => {
                            void toggleApp(app, next);
                          }}
                          aria-label={`Toggle ${name}`}
                        />
                      </TableCell>
                      <TableCell className="text-center">
                        <div className="inline-flex items-center gap-2">
                          <Activity className="h-4 w-4 text-muted-foreground" />
                          <HealthBadge app={app} />
                        </div>
                      </TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground">{runningState}</TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground max-w-[320px] truncate">
                        {probeUrl ? (
                          <a
                            href={probeUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="hover:text-primary transition-colors"
                            title={probeUrl}
                          >
                            {probeUrl}
                          </a>
                        ) : (
                          '—'
                        )}
                      </TableCell>
                      <TableCell className="font-mono text-xs text-muted-foreground">
                        {lastChecked ? `${formatTimeAgo(lastChecked)} ago` : '—'}
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-7 w-7"
                          onClick={() => toggleAppLogs(app)}
                          aria-label={`${isExpanded ? 'Collapse' : 'Expand'} logs for ${name}`}
                          aria-expanded={isExpanded}
                        >
                          {loadingLogs ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <ChevronDown
                              className={cn('h-4 w-4 transition-transform duration-300', isExpanded && 'rotate-180')}
                            />
                          )}
                        </Button>
                      </TableCell>
                    </TableRow>
                    <TableRow className="border-0 hover:bg-transparent">
                      <TableCell colSpan={7} className="bg-muted/20 p-0">
                        <div
                          className={`will-change-[max-height,opacity,transform] transition-[max-height,opacity,transform] duration-450 ease-[cubic-bezier(0.34,1.56,0.64,1)] ${
                            isExpanded
                              ? 'max-h-[400px] opacity-100 translate-y-0 overflow-auto'
                              : 'max-h-0 opacity-0 -translate-y-2 overflow-hidden pointer-events-none'
                          }`}
                          aria-hidden={!isExpanded}
                        >
                          <div className="p-4">
                            <div className="rounded-md border bg-background">
                              <div className="border-b px-3 py-2 text-xs font-semibold text-muted-foreground flex items-center gap-2">
                                <ScrollText className="h-3.5 w-3.5" />
                                Recent Console Logs
                              </div>
                              <div className="max-h-64 overflow-auto overflow-x-hidden break-words px-3 py-2 text-xs font-mono leading-relaxed">
                                {logState?.loading && (
                                  <div className="text-muted-foreground">Loading logs…</div>
                                )}
                                {!logState?.loading && logState?.error && (
                                  <div className="break-words text-destructive">
                                    Failed to load logs: {logState.error}
                                  </div>
                                )}
                                {!logState?.loading &&
                                  !logState?.error &&
                                  (logState?.lines?.length ?? 0) === 0 && (
                                    <div className="text-muted-foreground">No log output available.</div>
                                  )}
                                {!logState?.loading &&
                                  !logState?.error &&
                                  (logState?.lines?.length ?? 0) > 0 && (
                                    <div className="space-y-1">
                                      {(logState?.lines ?? []).slice(-LOG_TAIL_LINES).map((line, index) => (
                                        <div
                                          key={`${name}-log-${index}`}
                                          className={`whitespace-pre-wrap break-words text-foreground/90 px-2 py-1 max-w-full ${
                                            index % 2 === 0 ? 'bg-muted/30' : 'bg-transparent'
                                          }`}
                                        >
                                          {line}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                              </div>
                            </div>
                          </div>
                        </div>
                      </TableCell>
                    </TableRow>
                  </Fragment>
                );
              })}
              {apps.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7} className="text-center text-sm text-muted-foreground py-8">
                    No container apps configured. Set `SYSTEM_HEALTH_ARM_CONTAINERAPPS`.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
            </Table>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
