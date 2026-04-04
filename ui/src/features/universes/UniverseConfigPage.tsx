import { useEffect, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Globe, Plus, Trash2 } from 'lucide-react';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import { Input } from '@/app/components/ui/input';
import { Label } from '@/app/components/ui/label';
import { PageLoader } from '@/app/components/common/PageLoader';
import { Textarea } from '@/app/components/ui/textarea';
import { UniverseRuleBuilder } from '@/app/components/pages/strategy-editor/UniverseRuleBuilder';
import {
  buildEmptyUniverse,
  collectUniverseTables,
  countUniverseConditions,
  summarizeUniverse
} from '@/app/components/pages/strategy-editor/universeUtils';
import { universeApi } from '@/services/universeApi';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import type { UniverseConfigDetail } from '@/types/strategy';
import { cn } from '@/app/components/ui/utils';
import { toast } from 'sonner';

function buildEmptyUniverseConfig(): UniverseConfigDetail {
  return {
    name: '',
    description: '',
    version: 1,
    config: buildEmptyUniverse()
  };
}

function formatTimestamp(value?: string): string {
  if (!value) return 'Never updated';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(parsed);
}

function MetricCard({ label, value, detail }: { label: string; value: string; detail: string }) {
  return (
    <div className="rounded-xl border border-border/60 bg-card px-4 py-3 shadow-sm">
      <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
        {label}
      </div>
      <div className="mt-2 font-mono text-xl font-semibold text-foreground">{value}</div>
      <div className="mt-1 text-xs text-muted-foreground">{detail}</div>
    </div>
  );
}

export function UniverseConfigPage() {
  const queryClient = useQueryClient();
  const [selectedUniverseName, setSelectedUniverseName] = useState<string | null>(null);
  const [draft, setDraft] = useState<UniverseConfigDetail>(buildEmptyUniverseConfig());

  const {
    data: universes = [],
    isLoading,
    error
  } = useQuery({
    queryKey: ['universe-configs'],
    queryFn: () => universeApi.listUniverseConfigs()
  });

  const detailQuery = useQuery({
    queryKey: ['universe-configs', 'detail', selectedUniverseName],
    queryFn: () => universeApi.getUniverseConfigDetail(String(selectedUniverseName)),
    enabled: Boolean(selectedUniverseName)
  });

  useEffect(() => {
    if (!selectedUniverseName && universes.length > 0) {
      setSelectedUniverseName(universes[0].name);
    }
  }, [selectedUniverseName, universes]);

  useEffect(() => {
    if (detailQuery.data) {
      setDraft(detailQuery.data);
    }
  }, [detailQuery.data]);

  const saveMutation = useMutation({
    mutationFn: () =>
      universeApi.saveUniverseConfig({
        name: draft.name,
        description: draft.description,
        config: draft.config
      }),
    onSuccess: async (result) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['universe-configs'] }),
        queryClient.invalidateQueries({ queryKey: ['universe-configs', 'detail', draft.name] }),
        queryClient.invalidateQueries({ queryKey: ['ranking-schemas'] }),
        queryClient.invalidateQueries({ queryKey: ['strategies'] })
      ]);
      setSelectedUniverseName(draft.name);
      setDraft((current) => ({ ...current, version: result.version }));
      toast.success(`Universe config ${draft.name} saved`);
    },
    onError: (saveError) => {
      toast.error(`Failed to save universe config: ${formatSystemStatusText(saveError)}`);
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (name: string) => universeApi.deleteUniverseConfig(name),
    onSuccess: async (_, name) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['universe-configs'] }),
        queryClient.invalidateQueries({ queryKey: ['universe-configs', 'detail', name] }),
        queryClient.invalidateQueries({ queryKey: ['ranking-schemas'] }),
        queryClient.invalidateQueries({ queryKey: ['strategies'] })
      ]);
      setSelectedUniverseName(null);
      setDraft(buildEmptyUniverseConfig());
      toast.success(`Universe config ${name} deleted`);
    },
    onError: (deleteError) => {
      toast.error(`Failed to delete universe config: ${formatSystemStatusText(deleteError)}`);
    }
  });

  const handleCreateNew = () => {
    setSelectedUniverseName(null);
    setDraft(buildEmptyUniverseConfig());
  };

  const listError = formatSystemStatusText(error);
  const detailError = formatSystemStatusText(detailQuery.error);
  const selectedUniverseLabel = selectedUniverseName || draft.name || 'New Universe Configuration';
  const tableCount = collectUniverseTables(draft.config.root).length;
  const conditionCount = countUniverseConditions(draft.config.root);
  const draftSummary = summarizeUniverse(draft.config);

  return (
    <div className="page-shell space-y-6">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div className="space-y-2">
          <div className="text-sm font-medium uppercase tracking-[0.08em] text-muted-foreground">
            Universe control plane
          </div>
          <h1 className="text-3xl font-semibold tracking-tight text-foreground">
            Universe Configurations
          </h1>
          <p className="max-w-3xl text-sm leading-6 text-muted-foreground">
            Define reusable symbol eligibility logic from Postgres gold data, validate the current
            matching set, and publish versioned definitions used by run configurations and ranking
            schemas.
          </p>
        </div>
        <div className="flex flex-wrap gap-3">
          <Button variant="outline" onClick={handleCreateNew}>
            <Plus className="h-4 w-4" />
            New Universe Configuration
          </Button>
          <Button
            onClick={() => saveMutation.mutate()}
            disabled={saveMutation.isPending || !draft.name.trim()}
          >
            {saveMutation.isPending ? 'Saving...' : 'Save Universe Configuration'}
          </Button>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          label="Saved definitions"
          value={String(universes.length)}
          detail="Versioned universes available for reuse."
        />
        <MetricCard
          label="Current version"
          value={`v${draft.version || 1}`}
          detail={
            selectedUniverseName
              ? 'Published revision loaded in the editor.'
              : 'Draft version for a new universe.'
          }
        />
        <MetricCard
          label="Conditions"
          value={String(conditionCount)}
          detail="Individual eligibility checks in the active rule tree."
        />
        <MetricCard
          label="Referenced tables"
          value={String(tableCount)}
          detail="Gold datasets currently used by the active definition."
        />
      </div>

      <div className="grid gap-6 xl:grid-cols-[340px_minmax(0,1fr)]">
        <Card className="border border-border/60 bg-card shadow-sm">
          <CardHeader className="border-b border-border/60">
            <div className="space-y-1">
              <CardTitle className="text-lg font-semibold">Universe Library</CardTitle>
              <CardDescription>
                Review published definitions and load one into the editor.
              </CardDescription>
            </div>
            <CardAction>
              <Badge variant="outline">{universes.length} total</Badge>
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-4 pt-5">
            {isLoading ? (
              <PageLoader text="Loading universe configurations..." className="h-64" />
            ) : listError ? (
              <div className="rounded-xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {listError}
              </div>
            ) : universes.length === 0 ? (
              <div className="rounded-xl border border-dashed border-border/70 bg-muted/20 p-5 text-sm text-muted-foreground">
                No universe configurations saved yet. Create one to reuse across strategies and
                rankings.
              </div>
            ) : (
              <div className="space-y-3">
                {universes.map((universe) => {
                  const isSelected = universe.name === selectedUniverseName;
                  return (
                    <button
                      key={universe.name}
                      type="button"
                      className={cn(
                        'w-full rounded-xl border px-4 py-4 text-left transition-colors',
                        isSelected
                          ? 'border-primary/40 bg-accent/30 shadow-sm'
                          : 'border-border/60 bg-background hover:bg-muted/25'
                      )}
                      onClick={() => setSelectedUniverseName(universe.name)}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0 space-y-1">
                          <div className="truncate font-mono text-sm font-semibold text-foreground">
                            {universe.name}
                          </div>
                          <div className="text-sm leading-5 text-muted-foreground">
                            {universe.description || 'No description provided.'}
                          </div>
                        </div>
                        <Badge variant={isSelected ? 'default' : 'outline'} className="shrink-0">
                          v{universe.version}
                        </Badge>
                      </div>
                      <div className="mt-3 flex items-center justify-between gap-3 text-xs text-muted-foreground">
                        <span>Updated {formatTimestamp(universe.updated_at)}</span>
                        {isSelected ? (
                          <span className="font-medium text-foreground">Selected</span>
                        ) : null}
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="border border-border/60 bg-card shadow-sm">
          <CardHeader className="gap-4 border-b border-border/60">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
              <div className="space-y-1">
                <CardTitle className="text-lg font-semibold">Universe Editor</CardTitle>
                <CardDescription>
                  Maintain the active definition, inspect its structure, and publish a versioned
                  update.
                </CardDescription>
              </div>
              <Badge variant="outline" className="w-fit gap-1.5 font-mono">
                <Globe className="h-3.5 w-3.5" />
                {selectedUniverseLabel}
              </Badge>
            </div>
          </CardHeader>
          <CardContent className="space-y-6 pt-6">
            {detailQuery.isLoading && selectedUniverseName ? (
              <PageLoader text="Loading universe configuration..." className="h-56" />
            ) : detailError ? (
              <div className="rounded-xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {detailError}
              </div>
            ) : (
              <>
                <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_280px]">
                  <div className="space-y-4">
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="grid gap-2">
                        <Label htmlFor="universe-name">Universe Name</Label>
                        <Input
                          id="universe-name"
                          readOnly={Boolean(selectedUniverseName)}
                          value={draft.name}
                          onChange={(event) =>
                            setDraft((current) => ({ ...current, name: event.target.value }))
                          }
                          placeholder="e.g. large-cap-quality"
                        />
                      </div>
                      <div className="grid gap-2">
                        <Label htmlFor="universe-description">Description</Label>
                        <Input
                          id="universe-description"
                          value={draft.description || ''}
                          onChange={(event) =>
                            setDraft((current) => ({ ...current, description: event.target.value }))
                          }
                          placeholder="Describe the eligible symbol set."
                        />
                      </div>
                    </div>

                    <div className="rounded-xl border border-border/60 bg-muted/20 p-4">
                      <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                        Definition summary
                      </div>
                      <div className="mt-2 text-sm font-medium text-foreground">{draftSummary}</div>
                      <div className="mt-1 text-sm text-muted-foreground">
                        Saved universes are reused by run configurations and ranking schemas after
                        publish.
                      </div>
                    </div>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-1">
                    <div className="rounded-xl border border-border/60 bg-background px-4 py-3">
                      <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                        Version
                      </div>
                      <div className="mt-2 font-mono text-lg font-semibold text-foreground">
                        v{draft.version || 1}
                      </div>
                    </div>
                    <div className="rounded-xl border border-border/60 bg-background px-4 py-3">
                      <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                        Source
                      </div>
                      <div className="mt-2 font-mono text-lg font-semibold text-foreground">
                        {draft.config.source}
                      </div>
                    </div>
                    <div className="rounded-xl border border-border/60 bg-background px-4 py-3">
                      <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                        Current scope
                      </div>
                      <div className="mt-2 text-sm font-medium text-foreground">
                        {conditionCount} conditions across {tableCount} tables
                      </div>
                    </div>
                  </div>
                </div>

                <UniverseRuleBuilder
                  value={draft.config}
                  onChange={(nextValue) =>
                    setDraft((current) => ({ ...current, config: nextValue }))
                  }
                />

                <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_auto] xl:items-start">
                  <div className="space-y-2">
                    <Label htmlFor="universe-config-preview">Serialized Definition</Label>
                    <Textarea
                      id="universe-config-preview"
                      readOnly
                      className="min-h-[220px] border-border/70 bg-background font-mono text-xs leading-5"
                      value={JSON.stringify(draft.config, null, 2)}
                    />
                  </div>

                  {selectedUniverseName ? (
                    <div className="flex justify-end xl:pt-8">
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => deleteMutation.mutate(selectedUniverseName)}
                        disabled={deleteMutation.isPending}
                      >
                        <Trash2 className="h-4 w-4" />
                        {deleteMutation.isPending ? 'Deleting...' : 'Delete Universe Configuration'}
                      </Button>
                    </div>
                  ) : null}
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
