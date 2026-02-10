import { useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Activity, ExternalLink, RefreshCw, Server } from 'lucide-react';
import { DataService } from '@/services/DataService';
import type { ContainerAppStatusItem } from '@/services/apiService';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
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
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-3">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Server className="h-5 w-5" />
              Container Apps
            </CardTitle>
            <CardDescription>
              Toggle API/UI container apps and run live accessibility checks.
            </CardDescription>
          </div>
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => void queryClient.invalidateQueries({ queryKey: QUERY_KEY })}
            disabled={containerAppsQuery.isFetching}
          >
            <RefreshCw className={cn('h-4 w-4', containerAppsQuery.isFetching && 'animate-spin')} />
            Refresh
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>App</TableHead>
                <TableHead className="text-center">Enabled</TableHead>
                <TableHead className="text-center">Health</TableHead>
                <TableHead>Runtime</TableHead>
                <TableHead>Probe URL</TableHead>
                <TableHead>Last Checked</TableHead>
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

                return (
                  <TableRow key={name}>
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
                  </TableRow>
                );
              })}
              {apps.length === 0 && (
                <TableRow>
                  <TableCell colSpan={6} className="text-center text-sm text-muted-foreground py-8">
                    No container apps configured. Set `SYSTEM_HEALTH_ARM_CONTAINERAPPS`.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
}
