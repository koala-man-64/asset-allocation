import { useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Pencil, RefreshCw, Trash2 } from 'lucide-react';
import { DataService } from '@/services/DataService';
import {
  useRuntimeConfigCatalogQuery,
  useRuntimeConfigQuery,
  queryKeys
} from '@/hooks/useDataQueries';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Input } from '@/app/components/ui/input';
import { Switch } from '@/app/components/ui/switch';
import { Badge } from '@/app/components/ui/badge';
import { Textarea } from '@/app/components/ui/textarea';
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from '@/app/components/ui/dialog';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { formatTimeAgo } from '@/app/components/pages/system-status/SystemStatusHelpers';
import type { RuntimeConfigItem } from '@/services/apiService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

type EditState = {
  key: string;
  enabled: boolean;
  value: string;
  description?: string;
};

function statusBadge(item: RuntimeConfigItem | undefined) {
  if (!item) {
    return (
      <Badge variant="outline" className="font-mono text-[10px] uppercase tracking-widest">
        ENV
      </Badge>
    );
  }

  if (!item.enabled) {
    return (
      <Badge variant="secondary" className="font-mono text-[10px] uppercase tracking-widest">
        DB OFF
      </Badge>
    );
  }

  return (
    <Badge variant="default" className="font-mono text-[10px] uppercase tracking-widest">
      DB ON
    </Badge>
  );
}

function formatValuePreview(value: string, maxLen = 120) {
  const text = String(value || '');
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen)}...`;
}

export function RuntimeConfigPage() {
  const scope = 'global';
  const queryClient = useQueryClient();

  const catalogQuery = useRuntimeConfigCatalogQuery();
  const configQuery = useRuntimeConfigQuery(scope);

  const [editing, setEditing] = useState<EditState | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeletingKey, setIsDeletingKey] = useState<string | null>(null);

  const byKey = useMemo(() => {
    const map = new Map<string, RuntimeConfigItem>();
    for (const item of configQuery.data?.items || []) {
      map.set(item.key, item);
    }
    return map;
  }, [configQuery.data]);

  const rows = useMemo(() => {
    const items = catalogQuery.data?.items || [];
    return items.map((catalogItem) => {
      const item = byKey.get(catalogItem.key);
      return {
        key: catalogItem.key,
        description: catalogItem.description,
        example: catalogItem.example,
        item
      };
    });
  }, [catalogQuery.data, byKey]);

  const openEdit = (key: string) => {
    const current = byKey.get(key);
    const catalog = (catalogQuery.data?.items || []).find((item) => item.key === key);
    setEditing({
      key,
      enabled: Boolean(current?.enabled),
      value: String(current?.value || ''),
      description: String(current?.description || catalog?.description || '')
    });
  };

  const closeEdit = () => setEditing(null);

  const save = async () => {
    if (!editing) return;
    setIsSaving(true);
    try {
      await DataService.setRuntimeConfig({
        key: editing.key,
        scope,
        enabled: editing.enabled,
        value: editing.value,
        description: editing.description
      });
      toast.success('Runtime config updated.');
      closeEdit();
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfig(scope) }),
        queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfigCatalog() })
      ]);
    } catch (err) {
      const message = formatSystemStatusText(err);
      toast.error(`Failed to update runtime config: ${message}`);
    } finally {
      setIsSaving(false);
    }
  };

  const remove = async (key: string) => {
    setIsDeletingKey(key);
    try {
      await DataService.deleteRuntimeConfig(key, scope);
      toast.success('Runtime config entry removed.');
      await queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfig(scope) });
    } catch (err) {
      const message = formatSystemStatusText(err);
      toast.error(`Failed to delete runtime config: ${message}`);
    } finally {
      setIsDeletingKey(null);
    }
  };

  const isLoading = catalogQuery.isLoading || configQuery.isLoading;
  const hasError = Boolean(catalogQuery.error || configQuery.error);

  if (hasError) {
    const message =
      formatSystemStatusText(catalogQuery.error) ||
      formatSystemStatusText(configQuery.error) ||
      'Runtime config is unavailable.';
    return (
      <div className="mcm-panel rounded-lg border border-destructive/30 bg-destructive/10 p-6 text-destructive">
        <p className="font-mono text-sm uppercase tracking-wide">Runtime Config Unavailable</p>
        <p className="mt-3 text-sm">{message}</p>
      </div>
    );
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title">Runtime Config</h1>
        <p className="page-subtitle">
          DB-backed overrides applied at ETL job and API startup (scope: {scope}).
        </p>
      </div>

      <Card className="mcm-panel">
        <CardHeader>
          <CardTitle className="flex items-center justify-between">
            <span>Overrides</span>
            <Button
              variant="outline"
              size="sm"
              onClick={() =>
                void queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfig(scope) })
              }
              disabled={isLoading}
              className="gap-2"
            >
              <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </CardTitle>
        </CardHeader>
        <CardContent>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[180px]">KEY</TableHead>
                <TableHead className="w-[120px]">SOURCE</TableHead>
                <TableHead>DESCRIPTION</TableHead>
                <TableHead className="w-[320px]">VALUE</TableHead>
                <TableHead className="w-[110px]">UPDATED</TableHead>
                <TableHead className="w-[160px] text-right">ACTIONS</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((row) => (
                <TableRow key={row.key}>
                  <TableCell className="font-mono text-xs">{row.key}</TableCell>
                  <TableCell>{statusBadge(row.item)}</TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {row.item?.description || row.description || ''}
                    {row.example ? (
                      <div className="mt-1 font-mono text-[10px] text-muted-foreground/70">
                        e.g. {row.example}
                      </div>
                    ) : null}
                  </TableCell>
                  <TableCell className="font-mono text-xs text-muted-foreground">
                    {row.item?.value ? (
                      <span title={row.item.value}>{formatValuePreview(row.item.value)}</span>
                    ) : (
                      '—'
                    )}
                  </TableCell>
                  <TableCell className="font-mono text-[11px] text-muted-foreground">
                    {row.item?.updatedAt ? formatTimeAgo(row.item.updatedAt) : '—'}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="inline-flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-2"
                        onClick={() => openEdit(row.key)}
                      >
                        <Pencil className="h-4 w-4" />
                        Edit
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-2"
                        onClick={() => void remove(row.key)}
                        disabled={!row.item || isDeletingKey === row.key}
                      >
                        <Trash2 className="h-4 w-4" />
                        Remove
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
              {rows.length === 0 && (
                <TableRow>
                  <TableCell
                    colSpan={6}
                    className="py-10 text-center text-sm text-muted-foreground"
                  >
                    No runtime config keys are available in this deployment.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </CardContent>
      </Card>

      <Dialog open={Boolean(editing)} onOpenChange={(open) => (!open ? closeEdit() : null)}>
        <DialogContent className="max-w-[720px]">
          <DialogHeader>
            <DialogTitle>Update Runtime Config</DialogTitle>
          </DialogHeader>

          {editing ? (
            <div className="space-y-4">
              <div className="flex items-center justify-between rounded-lg border border-border/60 bg-muted/20 px-4 py-3">
                <div>
                  <div className="text-xs uppercase text-muted-foreground">Apply Override</div>
                  <div className="text-sm">{editing.key}</div>
                </div>
                <Switch
                  checked={editing.enabled}
                  onCheckedChange={(checked) =>
                    setEditing((prev) => (prev ? { ...prev, enabled: Boolean(checked) } : prev))
                  }
                  aria-label="Toggle runtime config override"
                />
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <div className="space-y-2">
                  <label className="text-xs uppercase text-muted-foreground">Scope</label>
                  <Input value={scope} disabled className="font-mono text-sm" />
                </div>
                <div className="space-y-2">
                  <label className="text-xs uppercase text-muted-foreground">Key</label>
                  <Input value={editing.key} disabled className="font-mono text-sm" />
                </div>
              </div>

              <div className="space-y-2">
                <label className="text-xs uppercase text-muted-foreground">Value</label>
                <Textarea
                  value={editing.value}
                  onChange={(event) =>
                    setEditing((prev) => (prev ? { ...prev, value: event.target.value } : prev))
                  }
                  className="min-h-[120px] font-mono text-sm"
                />
              </div>

              <div className="space-y-2">
                <label className="text-xs uppercase text-muted-foreground">Description</label>
                <Textarea
                  value={editing.description || ''}
                  onChange={(event) =>
                    setEditing((prev) =>
                      prev ? { ...prev, description: event.target.value } : prev
                    )
                  }
                  className="min-h-[80px] text-sm"
                />
              </div>
            </div>
          ) : null}

          <DialogFooter>
            <Button variant="outline" onClick={closeEdit} disabled={isSaving}>
              Cancel
            </Button>
            <Button onClick={() => void save()} disabled={isSaving || !editing}>
              {isSaving ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
              Save
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
