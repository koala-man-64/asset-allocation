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
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Textarea } from '@/app/components/ui/textarea';
import { UniverseRuleBuilder } from '@/app/components/pages/strategy-editor/UniverseRuleBuilder';
import { buildEmptyUniverse } from '@/app/components/pages/strategy-editor/universeUtils';
import { universeApi } from '@/services/universeApi';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import type { UniverseConfigDetail } from '@/types/strategy';
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

export function UniverseConfigPage() {
  const queryClient = useQueryClient();
  const [selectedUniverseName, setSelectedUniverseName] = useState<string | null>(null);
  const [draft, setDraft] = useState<UniverseConfigDetail>(buildEmptyUniverseConfig());

  const { data: universes = [], isLoading, error } = useQuery({
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

  return (
    <div className="page-shell space-y-6">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Universe Configuration</p>
          <h1 className="page-title">Universe Configurations</h1>
          <p className="page-subtitle">
            Manage reusable Postgres-backed universe configurations for strategies and rankings.
          </p>
        </div>
        <div className="flex gap-3">
          <Button variant="outline" onClick={handleCreateNew}>
            <Plus className="h-4 w-4" />
            New Universe Configuration
          </Button>
          <Button onClick={() => saveMutation.mutate()} disabled={saveMutation.isPending || !draft.name.trim()}>
            {saveMutation.isPending ? 'Saving...' : 'Save Universe Configuration'}
          </Button>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.5fr)]">
        <Card className="mcm-panel">
          <CardHeader className="border-b border-border/40">
            <div className="space-y-1">
              <CardTitle className="font-display text-xl">Universe Configuration Catalog</CardTitle>
              <CardDescription>Select a saved universe configuration to review or edit it.</CardDescription>
            </div>
            <CardAction>
              <Badge variant="secondary">{universes.length} total</Badge>
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-4 pt-6">
            {isLoading ? (
              <PageLoader text="Loading universe configurations..." className="h-64" />
            ) : listError ? (
              <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {listError}
              </div>
            ) : universes.length === 0 ? (
              <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                No universe configurations saved yet. Create one to reuse across strategies and rankings.
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Version</TableHead>
                    <TableHead>Last Updated</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {universes.map((universe) => (
                    <TableRow
                      key={universe.name}
                      className="cursor-pointer"
                      data-state={universe.name === selectedUniverseName ? 'selected' : undefined}
                      onClick={() => setSelectedUniverseName(universe.name)}
                    >
                      <TableCell className="whitespace-normal">
                        <div className="space-y-1">
                          <div className="font-display text-base text-foreground">{universe.name}</div>
                          <div className="text-xs text-muted-foreground">
                            {universe.description || 'No description provided.'}
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">v{universe.version}</Badge>
                      </TableCell>
                      <TableCell>{formatTimestamp(universe.updated_at)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card className="mcm-panel">
          <CardHeader className="border-b border-border/40">
            <div className="space-y-1">
              <CardTitle className="font-display text-xl">Universe Configuration Editor</CardTitle>
              <CardDescription>Author a reusable universe once, then attach it from run and ranking configurations.</CardDescription>
            </div>
            <CardAction>
              <Badge variant="secondary">
                <Globe className="h-3.5 w-3.5" />
                {selectedUniverseLabel}
              </Badge>
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-5 pt-6">
            {detailQuery.isLoading && selectedUniverseName ? (
              <PageLoader text="Loading universe configuration..." className="h-56" />
            ) : detailError ? (
              <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {detailError}
              </div>
            ) : (
              <>
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="grid gap-2">
                    <Label htmlFor="universe-name">Universe Name</Label>
                    <Input
                      id="universe-name"
                      readOnly={Boolean(selectedUniverseName)}
                      value={draft.name}
                      onChange={(event) => setDraft((current) => ({ ...current, name: event.target.value }))}
                      placeholder="e.g. large-cap-quality"
                    />
                  </div>
                  <div className="grid gap-2">
                    <Label htmlFor="universe-description">Description</Label>
                    <Input
                      id="universe-description"
                      value={draft.description || ''}
                      onChange={(event) => setDraft((current) => ({ ...current, description: event.target.value }))}
                      placeholder="Describe the eligible symbol set."
                    />
                  </div>
                </div>

                <div className="grid gap-4 md:grid-cols-3">
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Version</div>
                    <div className="mt-2 font-display text-lg text-foreground">v{draft.version || 1}</div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Source</div>
                    <div className="mt-2 font-display text-lg text-foreground">{draft.config.source}</div>
                  </div>
                  <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                    <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">Preview</div>
                    <div className="mt-2 text-sm text-muted-foreground">Use the builder below to validate symbol membership.</div>
                  </div>
                </div>

                <UniverseRuleBuilder
                  value={draft.config}
                  onChange={(nextValue) => setDraft((current) => ({ ...current, config: nextValue }))}
                />

                <div className="space-y-2">
                  <Label htmlFor="universe-config-preview">Normalized Config Preview</Label>
                  <Textarea
                    id="universe-config-preview"
                    readOnly
                    className="min-h-[180px] font-mono text-xs"
                    value={JSON.stringify(draft.config, null, 2)}
                  />
                </div>

                {selectedUniverseName ? (
                  <div className="flex justify-end">
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
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
