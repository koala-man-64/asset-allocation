import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Eye, PencilLine, Plus, Trash2 } from 'lucide-react';
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
import { strategyApi } from '@/services/strategyApi';
import { StrategyEditor } from '@/app/components/pages/StrategyEditor';
import { Button } from '@/app/components/ui/button';
import { Badge } from '@/app/components/ui/badge';
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import { PageLoader } from '@/app/components/common/PageLoader';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { toast } from 'sonner';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import type { StrategyDetail, StrategySummary } from '@/types/strategy';

function formatTimestamp(value?: string): string {
  if (!value) return 'Never synced';

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;

  return new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(parsed);
}

function formatRuleType(value: string): string {
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function summarizeRule(strategy: StrategyDetail, ruleId: string): string {
  const rule = strategy.config.exits.find((item) => item.id === ruleId);
  if (!rule) return 'Unknown rule';

  if (rule.type === 'time_stop') {
    return `${String(rule.value ?? '')} bars on close`;
  }

  if (rule.type === 'trailing_stop_atr') {
    return `${String(rule.value ?? '')} ATR using ${rule.atrColumn || 'missing column'}`;
  }

  return `${String(rule.value ?? '')} via ${rule.priceField || 'n/a'}`;
}

export function StrategyConfigPage() {
  const queryClient = useQueryClient();
  const [isEditorOpen, setIsEditorOpen] = useState(false);
  const [selectedStrategyName, setSelectedStrategyName] = useState<string | null>(null);
  const [editorStrategy, setEditorStrategy] = useState<StrategySummary | null>(null);
  const [strategyPendingDelete, setStrategyPendingDelete] = useState<StrategySummary | null>(null);

  const { data: strategies = [], isLoading, isFetching, error } = useQuery({
    queryKey: ['strategies'],
    queryFn: () => strategyApi.listStrategies()
  });

  const selectedStrategy = strategies.find((strategy) => strategy.name === selectedStrategyName) || null;
  const selectedStrategyLabel = selectedStrategy?.name || selectedStrategyName;

  useEffect(() => {
    if (!selectedStrategyName || isFetching) return;
    const exists = strategies.some((strategy) => strategy.name === selectedStrategyName);
    if (!exists) {
      setSelectedStrategyName(null);
    }
  }, [isFetching, selectedStrategyName, strategies]);

  const detailQuery = useQuery({
    queryKey: ['strategies', 'detail', selectedStrategyName],
    queryFn: () => strategyApi.getStrategyDetail(String(selectedStrategyName)),
    enabled: Boolean(selectedStrategyName)
  });

  const deleteMutation = useMutation({
    mutationFn: (name: string) => strategyApi.deleteStrategy(name),
    onSuccess: async (_, name) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['strategies'] }),
        queryClient.invalidateQueries({ queryKey: ['strategies', 'detail', name] })
      ]);
      setSelectedStrategyName((current) => (current === name ? null : current));
      setStrategyPendingDelete(null);
      toast.success(`Run configuration ${name} deleted from Postgres`);
    },
    onError: (deleteError) => {
      toast.error(`Failed to delete run configuration: ${formatSystemStatusText(deleteError)}`);
    }
  });

  const handleCreate = () => {
    setEditorStrategy(null);
    setIsEditorOpen(true);
  };

  const handleView = (strategy: StrategySummary) => {
    setSelectedStrategyName(strategy.name);
  };

  const handleEdit = (strategy: StrategySummary) => {
    setSelectedStrategyName(strategy.name);
    setEditorStrategy(strategy);
    setIsEditorOpen(true);
  };

  const handleDelete = () => {
    if (!strategyPendingDelete?.name) return;
    deleteMutation.mutate(strategyPendingDelete.name);
  };

  const handleEditorOpenChange = (open: boolean) => {
    setIsEditorOpen(open);
    if (!open) {
      setEditorStrategy(null);
    }
  };

  const handleSaved = (strategy: StrategyDetail) => {
    setSelectedStrategyName(strategy.name);
  };

  const listError = formatSystemStatusText(error);
  const detailError = formatSystemStatusText(detailQuery.error);

  return (
    <div className="page-shell">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Run Configuration</p>
          <h1 className="page-title">Run Configurations</h1>
          <p className="page-subtitle">
            Manage the strategy-backed run settings that control cadence, selection, ranking attachment, and exits.
            Changes persist when you save or delete the record.
          </p>
        </div>
        <Button onClick={handleCreate} className="gap-2">
          <Plus className="mr-2 h-4 w-4" /> New Run Configuration
        </Button>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.95fr)]">
        <Card className="mcm-panel">
          <CardHeader className="border-b border-border/40">
            <div className="space-y-1">
              <CardTitle className="font-display text-xl">Run Configuration Catalog</CardTitle>
              <CardDescription>
                Select a saved run configuration to inspect its settings or open it for editing.
              </CardDescription>
            </div>
            <CardAction>
              <Badge variant="secondary">{strategies.length} total</Badge>
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-4 pt-6">
            {isLoading ? (
              <PageLoader text="Loading strategies..." className="h-64" />
            ) : listError ? (
              <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {listError}
              </div>
            ) : strategies.length === 0 ? (
              <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                No run configurations found yet. Create one, then click <span className="font-semibold text-foreground">Save to Postgres</span>.
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Last Updated</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {strategies.map((strategy) => {
                    const isSelected = selectedStrategyName === strategy.name;
                    return (
                      <TableRow
                        key={strategy.name}
                        data-state={isSelected ? 'selected' : undefined}
                        className="cursor-pointer"
                        onClick={() => handleView(strategy)}
                      >
                        <TableCell className="whitespace-normal">
                          <div className="space-y-1">
                            <div className="font-display text-base text-foreground">{strategy.name}</div>
                            <div className="text-xs text-muted-foreground">
                              {strategy.description || 'No description provided.'}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge variant={strategy.type === 'configured' ? 'default' : 'outline'}>
                            {strategy.type}
                          </Badge>
                        </TableCell>
                        <TableCell>{formatTimestamp(strategy.updated_at)}</TableCell>
                        <TableCell className="text-right">
                          <div className="flex justify-end gap-2">
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              aria-label={`View run configuration ${strategy.name}`}
                              onClick={(event) => {
                                event.stopPropagation();
                                handleView(strategy);
                              }}
                            >
                              <Eye className="h-4 w-4" />
                              View
                            </Button>
                            <Button
                              type="button"
                              variant="secondary"
                              size="sm"
                              aria-label={`Edit run configuration ${strategy.name}`}
                              onClick={(event) => {
                                event.stopPropagation();
                                handleEdit(strategy);
                              }}
                            >
                              <PencilLine className="h-4 w-4" />
                              Edit
                            </Button>
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              aria-label={`Delete run configuration ${strategy.name}`}
                              onClick={(event) => {
                                event.stopPropagation();
                                setStrategyPendingDelete(strategy);
                              }}
                            >
                              <Trash2 className="h-4 w-4" />
                              Delete
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card className="mcm-panel">
          <CardHeader className="border-b border-border/40">
            <div className="space-y-1">
              <CardTitle className="font-display text-xl">Run Configuration Detail</CardTitle>
              <CardDescription>
                Review the saved run configuration before opening the editor or deleting the record from Postgres.
              </CardDescription>
            </div>
            {selectedStrategyLabel ? (
              <CardAction>
                <Badge variant="secondary">{selectedStrategyLabel}</Badge>
              </CardAction>
            ) : null}
          </CardHeader>
          <CardContent className="space-y-5 pt-6">
            {!selectedStrategyName ? (
              <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                Select a run configuration from the catalog to view its saved settings.
              </div>
            ) : detailQuery.isLoading ? (
              <PageLoader text="Loading run configuration..." className="h-72" />
            ) : detailError ? (
              <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {detailError}
              </div>
            ) : detailQuery.data ? (
              <>
                <div className="space-y-3">
                  <div className="flex flex-wrap items-center gap-3">
                    <h2 className="font-display text-2xl text-foreground">{detailQuery.data.name}</h2>
                    <Badge variant={detailQuery.data.type === 'configured' ? 'default' : 'outline'}>
                      {detailQuery.data.type}
                    </Badge>
                  </div>
                  <p className="text-sm text-muted-foreground">
                    {detailQuery.data.description || 'No description provided.'}
                  </p>
                  <p className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
                    Last updated {formatTimestamp(detailQuery.data.updated_at)}
                  </p>
                </div>

                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-cream/65 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Universe Config</div>
                    <div className="mt-2 font-display text-lg text-foreground">
                      {detailQuery.data.config.universeConfigName || 'Not assigned'}
                    </div>
                    <div className="mt-2 text-xs text-muted-foreground">
                      {detailQuery.data.config.universe
                        ? 'Legacy embedded universe present on this record.'
                        : 'Saved universe reference used by this run configuration.'}
                    </div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-cream/65 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Rebalance</div>
                    <div className="mt-2 font-display text-lg text-foreground">{detailQuery.data.config.rebalance}</div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Selection</div>
                    <div className="mt-2 text-sm text-foreground">
                      Top {detailQuery.data.config.topN} with {detailQuery.data.config.lookbackWindow}-bar lookback
                    </div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Execution</div>
                    <div className="mt-2 text-sm text-foreground">
                      {detailQuery.data.config.longOnly ? 'Long only' : 'Long/short'} • hold {detailQuery.data.config.holdingPeriod} bars
                    </div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Cost Model</div>
                    <div className="mt-2 text-sm text-foreground">{detailQuery.data.config.costModel}</div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Ranking Schema</div>
                    <div className="mt-2 text-sm text-foreground">
                      {detailQuery.data.config.rankingSchemaName || 'None attached'}
                    </div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Conflict Policy</div>
                    <div className="mt-2 text-sm text-foreground">{detailQuery.data.config.intrabarConflictPolicy}</div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Platinum Output</div>
                    <div className="mt-2 text-sm text-foreground">
                      {detailQuery.data.output_table_name ? `platinum.${detailQuery.data.output_table_name}` : 'Not assigned'}
                    </div>
                  </div>
                </div>

                <div className="space-y-3">
                  <div className="flex items-center justify-between gap-3">
                    <h3 className="font-display text-lg text-foreground">Exit Stack</h3>
                    <Badge variant="secondary">{detailQuery.data.config.exits.length} rules</Badge>
                  </div>
                  {detailQuery.data.config.exits.length === 0 ? (
                    <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-4 text-sm text-muted-foreground">
                      No exit rules configured yet.
                    </div>
                  ) : (
                    <div className="space-y-3">
                      {detailQuery.data.config.exits.map((rule) => (
                        <div
                          key={rule.id}
                          className="rounded-2xl border border-mcm-walnut/25 bg-mcm-cream/55 p-4"
                        >
                          <div className="flex flex-wrap items-center justify-between gap-3">
                            <div>
                              <div className="font-display text-base text-foreground">{rule.id}</div>
                              <div className="text-xs text-muted-foreground">
                                {formatRuleType(rule.type)} • priority {rule.priority ?? 'auto'}
                              </div>
                            </div>
                            <Badge variant={rule.enabled ? 'default' : 'outline'}>
                              {rule.enabled ? 'Enabled' : 'Disabled'}
                            </Badge>
                          </div>
                          <div className="mt-3 text-sm text-foreground">
                            {summarizeRule(detailQuery.data, rule.id)}
                            {rule.minHoldBars > 0 ? ` • minimum hold ${rule.minHoldBars} bars` : ''}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="flex flex-wrap gap-3 border-t border-border/40 pt-5">
                  {selectedStrategy ? (
                    <Button type="button" variant="secondary" onClick={() => handleEdit(selectedStrategy)}>
                      <PencilLine className="h-4 w-4" />
                      Edit Run Configuration
                    </Button>
                  ) : null}
                  {selectedStrategy ? (
                    <Button
                      type="button"
                      variant="outline"
                      onClick={() => setStrategyPendingDelete(selectedStrategy)}
                    >
                      <Trash2 className="h-4 w-4" />
                      Delete Run Configuration
                    </Button>
                  ) : null}
                </div>
              </>
            ) : null}
          </CardContent>
        </Card>
      </div>

      <StrategyEditor
        strategy={editorStrategy}
        open={isEditorOpen}
        onOpenChange={handleEditorOpenChange}
        onSaved={handleSaved}
      />

      <AlertDialog
        open={Boolean(strategyPendingDelete)}
        onOpenChange={(open) => {
          if (!open && !deleteMutation.isPending) {
            setStrategyPendingDelete(null);
          }
        }}
      >
        <AlertDialogContent className="border-2 border-mcm-walnut bg-mcm-paper">
          <AlertDialogHeader>
            <AlertDialogTitle className="font-display text-2xl text-foreground">
              Delete run configuration
            </AlertDialogTitle>
            <AlertDialogDescription>
              Delete <span className="font-semibold text-foreground">{strategyPendingDelete?.name}</span> from Postgres. This removes the saved run configuration record from the catalog.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteMutation.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deleteMutation.isPending}
              onClick={handleDelete}
            >
              {deleteMutation.isPending ? 'Deleting...' : 'Delete from Postgres'}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
