import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Activity, AlertTriangle, Gauge, Layers3, Sparkles } from 'lucide-react';

import { PageLoader } from '@/app/components/common/PageLoader';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import { Input } from '@/app/components/ui/input';
import { Label } from '@/app/components/ui/label';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { Textarea } from '@/app/components/ui/textarea';
import { regimeApi } from '@/services/regimeApi';
import type { RegimeSnapshot } from '@/types/regime';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { toast } from 'sonner';

const DEFAULT_MODEL_CONFIG_JSON = JSON.stringify(
  {
    trendPositiveThreshold: 0.02,
    trendNegativeThreshold: -0.02,
    curveContangoThreshold: 0.5,
    curveInvertedThreshold: -0.5,
    highVolEnterThreshold: 28.0,
    highVolExitThreshold: 25.0,
    bearVolMin: 15.0,
    bearVolMaxExclusive: 25.0,
    bullVolMaxExclusive: 15.0,
    choppyVolMin: 10.0,
    choppyVolMaxExclusive: 18.0,
    haltVixThreshold: 32.0,
    haltVixStreakDays: 2,
    precedence: [
      'high_vol',
      'trending_bear',
      'trending_bull',
      'choppy_mean_reversion',
      'unclassified'
    ]
  },
  null,
  2
);

function formatTimestamp(value?: string | null): string {
  if (!value) return 'Not available';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(parsed);
}

function formatDate(value?: string | null): string {
  if (!value) return 'Not available';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat('en-US', { dateStyle: 'medium' }).format(parsed);
}

function formatMetric(value?: number | null, digits: number = 2): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return 'n/a';
  }
  return Number(value).toFixed(digits);
}

function regimeTone(
  snapshot?: RegimeSnapshot | null
): 'default' | 'secondary' | 'destructive' | 'outline' {
  if (!snapshot) return 'outline';
  if (snapshot.halt_flag || snapshot.regime_code === 'high_vol') return 'destructive';
  if (snapshot.regime_status === 'transition' || snapshot.regime_code === 'unclassified')
    return 'secondary';
  return 'default';
}

export function RegimeMonitorPage() {
  const queryClient = useQueryClient();
  const [selectedModelName, setSelectedModelName] = useState('default-regime');
  const [createDraft, setCreateDraft] = useState({
    name: '',
    description: '',
    configJson: DEFAULT_MODEL_CONFIG_JSON
  });

  const modelsQuery = useQuery({
    queryKey: ['regimes', 'models'],
    queryFn: () => regimeApi.listModels()
  });

  useEffect(() => {
    const models = modelsQuery.data?.models || [];
    if (!models.length) return;
    if (models.some((model) => model.name === selectedModelName)) return;
    setSelectedModelName(models[0].name);
  }, [modelsQuery.data?.models, selectedModelName]);

  const selectedModelDetailQuery = useQuery({
    queryKey: ['regimes', 'models', selectedModelName],
    queryFn: () => regimeApi.getModel(selectedModelName),
    enabled: Boolean(selectedModelName)
  });

  const currentQuery = useQuery({
    queryKey: ['regimes', 'current', selectedModelName],
    queryFn: () => regimeApi.getCurrent({ modelName: selectedModelName }),
    enabled: Boolean(selectedModelName)
  });

  const historyQuery = useQuery({
    queryKey: ['regimes', 'history', selectedModelName],
    queryFn: () => regimeApi.getHistory({ modelName: selectedModelName, limit: 24 }),
    enabled: Boolean(selectedModelName)
  });

  const createMutation = useMutation({
    mutationFn: async () => {
      const config = JSON.parse(createDraft.configJson);
      return regimeApi.createModel({
        name: createDraft.name.trim(),
        description: createDraft.description.trim(),
        config
      });
    },
    onSuccess: async (payload) => {
      await queryClient.invalidateQueries({ queryKey: ['regimes'] });
      setSelectedModelName(payload.model.name);
      setCreateDraft({
        name: '',
        description: '',
        configJson: DEFAULT_MODEL_CONFIG_JSON
      });
      toast.success(`Regime model ${payload.model.name} saved`);
    },
    onError: (error) => {
      toast.error(`Failed to save regime model: ${formatSystemStatusText(error)}`);
    }
  });

  const activateMutation = useMutation({
    mutationFn: ({ modelName, version }: { modelName: string; version: number }) =>
      regimeApi.activateModel(modelName, { version }),
    onSuccess: async (_, variables) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['regimes'] }),
        queryClient.invalidateQueries({ queryKey: ['regimes', 'models', variables.modelName] }),
        queryClient.invalidateQueries({ queryKey: ['regimes', 'current', variables.modelName] }),
        queryClient.invalidateQueries({ queryKey: ['regimes', 'history', variables.modelName] })
      ]);
      toast.success(`Activated ${variables.modelName} v${variables.version}`);
    },
    onError: (error) => {
      toast.error(`Failed to activate regime model: ${formatSystemStatusText(error)}`);
    }
  });

  const modelOptions = modelsQuery.data?.models || [];
  const currentSnapshot = currentQuery.data;
  const historyRows = historyQuery.data?.rows || [];
  const activeVersion = selectedModelDetailQuery.data?.activeRevision?.version;
  const latestRevisionVersion = useMemo(() => {
    const revisions = selectedModelDetailQuery.data?.revisions || [];
    return revisions.length ? revisions[0].version : undefined;
  }, [selectedModelDetailQuery.data?.revisions]);

  return (
    <div className="page-shell">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Live Operations</p>
          <h1 className="page-title flex items-center gap-2">
            <Sparkles className="h-6 w-6 text-mcm-olive" />
            Regime Monitor
          </h1>
          <p className="page-subtitle">
            Track the active gold regime model, inspect confirmed and transition states, and
            activate new model revisions without leaving the control plane.
          </p>
        </div>

        <div className="w-full max-w-sm rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/85 p-4">
          <Label htmlFor="regime-model-selector">Selected Model</Label>
          <select
            id="regime-model-selector"
            value={selectedModelName}
            onChange={(event) => setSelectedModelName(event.target.value)}
            className="mt-2 h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono"
          >
            {modelOptions.map((model) => (
              <option key={model.name} value={model.name}>
                {model.name}
              </option>
            ))}
          </select>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <Badge variant="secondary">Active v{activeVersion ?? 'n/a'}</Badge>
            <Badge variant="outline">Latest v{latestRevisionVersion ?? 'n/a'}</Badge>
          </div>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.35fr)_minmax(320px,0.95fr)]">
        <div className="space-y-6">
          <div className="grid gap-4 lg:grid-cols-3">
            <Card className="mcm-panel">
              <CardHeader className="border-b border-border/40">
                <CardTitle className="flex items-center gap-2 text-lg">
                  <Activity className="h-5 w-5 text-mcm-olive" />
                  Current Regime
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 pt-6">
                {currentQuery.isLoading ? (
                  <PageLoader text="Loading regime..." className="h-32" />
                ) : currentSnapshot ? (
                  <>
                    <Badge variant={regimeTone(currentSnapshot)} className="capitalize">
                      {currentSnapshot.regime_code.replaceAll('_', ' ')}
                    </Badge>
                    <div className="space-y-1 text-sm">
                      <div>
                        Status:{' '}
                        <span className="font-medium capitalize">
                          {currentSnapshot.regime_status}
                        </span>
                      </div>
                      <div>
                        As Of:{' '}
                        <span className="font-medium">
                          {formatDate(currentSnapshot.as_of_date)}
                        </span>
                      </div>
                      <div>
                        Effective:{' '}
                        <span className="font-medium">
                          {formatDate(currentSnapshot.effective_from_date)}
                        </span>
                      </div>
                    </div>
                  </>
                ) : (
                  <div className="text-sm text-muted-foreground">
                    No regime snapshot is available yet for this model.
                  </div>
                )}
              </CardContent>
            </Card>

            <Card className="mcm-panel">
              <CardHeader className="border-b border-border/40">
                <CardTitle className="flex items-center gap-2 text-lg">
                  <Gauge className="h-5 w-5 text-mcm-rust" />
                  Inputs
                </CardTitle>
              </CardHeader>
              <CardContent className="grid gap-3 pt-6 text-sm">
                <div>
                  SPY 20d:{' '}
                  <span className="font-medium">
                    {formatMetric(currentSnapshot?.spy_return_20d, 4)}
                  </span>
                </div>
                <div>
                  Realized Vol:{' '}
                  <span className="font-medium">
                    {formatMetric(currentSnapshot?.rvol_10d_ann, 2)}
                  </span>
                </div>
                <div>
                  VIX Spot:{' '}
                  <span className="font-medium">
                    {formatMetric(currentSnapshot?.vix_spot_close, 2)}
                  </span>
                </div>
                <div>
                  VIX 3M:{' '}
                  <span className="font-medium">
                    {formatMetric(currentSnapshot?.vix3m_close, 2)}
                  </span>
                </div>
                <div>
                  Curve Slope:{' '}
                  <span className="font-medium">{formatMetric(currentSnapshot?.vix_slope, 2)}</span>
                </div>
              </CardContent>
            </Card>

            <Card className="mcm-panel">
              <CardHeader className="border-b border-border/40">
                <CardTitle className="flex items-center gap-2 text-lg">
                  <AlertTriangle className="h-5 w-5 text-mcm-mustard" />
                  Halt Overlay
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 pt-6 text-sm">
                <Badge variant={currentSnapshot?.halt_flag ? 'destructive' : 'outline'}>
                  {currentSnapshot?.halt_flag ? 'Halt Active' : 'No Halt'}
                </Badge>
                <div>
                  Reason:{' '}
                  <span className="font-medium">{currentSnapshot?.halt_reason || 'n/a'}</span>
                </div>
                <div>
                  VIX Streak:{' '}
                  <span className="font-medium">{currentSnapshot?.vix_gt_32_streak ?? 'n/a'}</span>
                </div>
                <div>
                  Computed:{' '}
                  <span className="font-medium">
                    {formatTimestamp(currentSnapshot?.computed_at)}
                  </span>
                </div>
              </CardContent>
            </Card>
          </div>

          <Card className="mcm-panel">
            <CardHeader className="border-b border-border/40">
              <CardTitle className="flex items-center gap-2 text-lg">
                <Layers3 className="h-5 w-5 text-mcm-teal" />
                History
              </CardTitle>
              <CardDescription>
                Recent confirmed, transition, and unclassified states for the selected model.
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-6">
              {historyQuery.isLoading ? (
                <PageLoader text="Loading history..." className="h-40" />
              ) : historyRows.length === 0 ? (
                <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                  No regime history is available for this model yet.
                </div>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>As Of</TableHead>
                      <TableHead>Effective</TableHead>
                      <TableHead>Regime</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Rule</TableHead>
                      <TableHead className="text-right">Halt</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {historyRows.map((row) => (
                      <TableRow key={`${row.model_name}-${row.model_version}-${row.as_of_date}`}>
                        <TableCell>{formatDate(row.as_of_date)}</TableCell>
                        <TableCell>{formatDate(row.effective_from_date)}</TableCell>
                        <TableCell className="capitalize">
                          {row.regime_code.replaceAll('_', ' ')}
                        </TableCell>
                        <TableCell className="capitalize">{row.regime_status}</TableCell>
                        <TableCell>{row.matched_rule_id || 'n/a'}</TableCell>
                        <TableCell className="text-right">{row.halt_flag ? 'Yes' : 'No'}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="space-y-6">
          <Card className="mcm-panel">
            <CardHeader className="border-b border-border/40">
              <CardTitle className="text-lg">Create Model Revision</CardTitle>
              <CardDescription>
                Publish a new named regime model revision. Activation is separate so the monitor can
                stage changes first.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 pt-6">
              <div className="grid gap-2">
                <Label htmlFor="regime-create-name">Name</Label>
                <Input
                  id="regime-create-name"
                  value={createDraft.name}
                  onChange={(event) =>
                    setCreateDraft((current) => ({ ...current, name: event.target.value }))
                  }
                  placeholder="e.g. default-regime"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="regime-create-description">Description</Label>
                <Input
                  id="regime-create-description"
                  value={createDraft.description}
                  onChange={(event) =>
                    setCreateDraft((current) => ({ ...current, description: event.target.value }))
                  }
                  placeholder="Short description"
                />
              </div>
              <div className="grid gap-2">
                <Label htmlFor="regime-create-config">Config JSON</Label>
                <Textarea
                  id="regime-create-config"
                  value={createDraft.configJson}
                  onChange={(event) =>
                    setCreateDraft((current) => ({ ...current, configJson: event.target.value }))
                  }
                  rows={14}
                  className="font-mono text-xs"
                />
              </div>
              <Button
                className="w-full"
                disabled={createMutation.isPending || !createDraft.name.trim()}
                onClick={() => createMutation.mutate()}
              >
                {createMutation.isPending ? 'Saving…' : 'Save Model Revision'}
              </Button>
            </CardContent>
          </Card>

          <Card className="mcm-panel">
            <CardHeader className="border-b border-border/40">
              <CardTitle className="text-lg">Model Revisions</CardTitle>
              <CardDescription>
                Activate the revision that should drive the current gold regime outputs.
              </CardDescription>
            </CardHeader>
            <CardContent className="pt-6">
              {selectedModelDetailQuery.isLoading ? (
                <PageLoader text="Loading revisions..." className="h-40" />
              ) : selectedModelDetailQuery.data?.revisions?.length ? (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Version</TableHead>
                      <TableHead>Published</TableHead>
                      <TableHead>Activated</TableHead>
                      <TableHead className="text-right">Action</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {selectedModelDetailQuery.data.revisions.map((revision) => (
                      <TableRow key={`${revision.name}-${revision.version}`}>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <span className="font-medium">v{revision.version}</span>
                            {revision.version === activeVersion ? (
                              <Badge variant="secondary">Active</Badge>
                            ) : null}
                          </div>
                        </TableCell>
                        <TableCell>{formatTimestamp(revision.published_at)}</TableCell>
                        <TableCell>{formatTimestamp(revision.activated_at)}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={
                              activateMutation.isPending || revision.version === activeVersion
                            }
                            onClick={() =>
                              activateMutation.mutate({
                                modelName: selectedModelName,
                                version: revision.version
                              })
                            }
                          >
                            Activate
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              ) : (
                <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                  No revisions found for this model.
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
