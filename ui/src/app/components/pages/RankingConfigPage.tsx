import { useEffect, useMemo, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Eye, Layers3, Plus, RefreshCcw, Trash2 } from 'lucide-react';
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/app/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { Textarea } from '@/app/components/ui/textarea';
import { rankingApi } from '@/services/rankingApi';
import { strategyApi } from '@/services/strategyApi';
import { universeApi } from '@/services/universeApi';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import type {
  RankingCatalogColumn,
  RankingSchemaDetail,
  RankingTransform,
  RankingTransformType
} from '@/types/strategy';
import { toast } from 'sonner';

const TRANSFORM_OPTIONS: Array<{ value: RankingTransformType; label: string }> = [
  { value: 'percentile_rank', label: 'Percentile Rank' },
  { value: 'zscore', label: 'Z-Score' },
  { value: 'minmax', label: 'Min/Max' },
  { value: 'clip', label: 'Clip' },
  { value: 'winsorize', label: 'Winsorize' },
  { value: 'coalesce', label: 'Coalesce' },
  { value: 'log1p', label: 'Log1p' },
  { value: 'negate', label: 'Negate' },
  { value: 'abs', label: 'Absolute' }
];

const DIRECTION_OPTIONS = [
  { value: 'desc', label: 'Higher Is Better' },
  { value: 'asc', label: 'Lower Is Better' }
] as const;

const MISSING_POLICY_OPTIONS = [
  { value: 'exclude', label: 'Exclude Missing' },
  { value: 'zero', label: 'Fill Missing With Zero' }
] as const;

function buildEmptyTransform(type: RankingTransformType = 'percentile_rank'): RankingTransform {
  return { type, params: {} };
}

function buildEmptySchema(): RankingSchemaDetail {
  return {
    name: '',
    description: '',
    version: 1,
    config: {
      universeConfigName: undefined,
      groups: [
        {
          name: 'Composite',
          weight: 1,
          transforms: [buildEmptyTransform('percentile_rank')],
          factors: [
            {
              name: 'market-factor',
              table: 'market_data',
              column: 'return_20d',
              weight: 1,
              direction: 'desc',
              missingValuePolicy: 'exclude',
              transforms: [buildEmptyTransform('zscore')]
            }
          ]
        }
      ],
      overallTransforms: []
    }
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

function parseParamValue(value: string): string | number | boolean | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed === 'true') return true;
  if (trimmed === 'false') return false;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : trimmed;
}

function getTransformParamConfig(
  type: RankingTransformType
): Array<{ key: string; label: string }> {
  if (type === 'clip')
    return [
      { key: 'lower', label: 'Lower Bound' },
      { key: 'upper', label: 'Upper Bound' }
    ];
  if (type === 'winsorize') {
    return [
      { key: 'lowerQuantile', label: 'Lower Quantile' },
      { key: 'upperQuantile', label: 'Upper Quantile' }
    ];
  }
  if (type === 'coalesce') return [{ key: 'value', label: 'Fallback Value' }];
  return [];
}

function TransformListEditor({
  label,
  transforms,
  onChange
}: {
  label: string;
  transforms: RankingTransform[];
  onChange: (nextValue: RankingTransform[]) => void;
}) {
  return (
    <div className="space-y-3 rounded-2xl border border-border/50 bg-background/55 p-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <div className="text-xs font-black uppercase tracking-[0.18em] text-muted-foreground">
            {label}
          </div>
          <div className="text-xs text-muted-foreground">Transforms run in array order.</div>
        </div>
        <Button
          type="button"
          variant="secondary"
          size="sm"
          onClick={() => onChange([...transforms, buildEmptyTransform()])}
        >
          <Plus className="h-4 w-4" />
          Add Transform
        </Button>
      </div>

      {transforms.length === 0 ? (
        <div className="rounded-xl border border-dashed p-3 text-sm text-muted-foreground">
          No transforms configured.
        </div>
      ) : (
        <div className="space-y-3">
          {transforms.map((transform, index) => {
            const paramFields = getTransformParamConfig(transform.type);
            return (
              <div
                key={`${label}-${index}`}
                className="space-y-3 rounded-xl border border-border/50 bg-card/80 p-3"
              >
                <div className="flex flex-wrap items-center gap-3">
                  <div className="min-w-[220px] flex-1">
                    <Label htmlFor={`${label}-transform-${index}`}>Transform</Label>
                    <Select
                      value={transform.type}
                      onValueChange={(value) => {
                        const nextTransforms = transforms.slice();
                        nextTransforms[index] = { type: value as RankingTransformType, params: {} };
                        onChange(nextTransforms);
                      }}
                    >
                      <SelectTrigger id={`${label}-transform-${index}`}>
                        <SelectValue placeholder="Select transform" />
                      </SelectTrigger>
                      <SelectContent>
                        {TRANSFORM_OPTIONS.map((option) => (
                          <SelectItem key={option.value} value={option.value}>
                            {option.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      onChange(transforms.filter((_, itemIndex) => itemIndex !== index))
                    }
                  >
                    <Trash2 className="h-4 w-4" />
                    Remove
                  </Button>
                </div>

                {paramFields.length > 0 ? (
                  <div className="grid gap-3 md:grid-cols-2">
                    {paramFields.map((field) => (
                      <div key={field.key} className="grid gap-2">
                        <Label htmlFor={`${label}-${index}-${field.key}`}>{field.label}</Label>
                        <Input
                          id={`${label}-${index}-${field.key}`}
                          value={String(transform.params[field.key] ?? '')}
                          onChange={(event) => {
                            const nextTransforms = transforms.slice();
                            nextTransforms[index] = {
                              ...transform,
                              params: {
                                ...transform.params,
                                [field.key]: parseParamValue(event.target.value)
                              }
                            };
                            onChange(nextTransforms);
                          }}
                        />
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function RankingConfigPage() {
  const queryClient = useQueryClient();
  const [selectedSchemaName, setSelectedSchemaName] = useState<string | null>(null);
  const [draft, setDraft] = useState<RankingSchemaDetail>(buildEmptySchema());
  const [previewStrategyName, setPreviewStrategyName] = useState('');
  const [previewDate, setPreviewDate] = useState(() => new Date().toISOString().slice(0, 10));

  const {
    data: schemas = [],
    isLoading: isSchemasLoading,
    error: schemasError
  } = useQuery({
    queryKey: ['ranking-schemas'],
    queryFn: () => rankingApi.listRankingSchemas()
  });

  const {
    data: rankingCatalog,
    isLoading: isCatalogLoading,
    error: rankingCatalogError
  } = useQuery({
    queryKey: ['ranking-catalog'],
    queryFn: () => rankingApi.getRankingCatalog()
  });

  const { data: strategies = [] } = useQuery({
    queryKey: ['strategies'],
    queryFn: () => strategyApi.listStrategies()
  });
  const { data: universeConfigs = [] } = useQuery({
    queryKey: ['universe-configs'],
    queryFn: () => universeApi.listUniverseConfigs()
  });

  const detailQuery = useQuery({
    queryKey: ['ranking-schemas', 'detail', selectedSchemaName],
    queryFn: () => rankingApi.getRankingSchemaDetail(String(selectedSchemaName)),
    enabled: Boolean(selectedSchemaName)
  });

  useEffect(() => {
    if (!selectedSchemaName && schemas.length > 0) {
      setSelectedSchemaName(schemas[0].name);
    }
  }, [schemas, selectedSchemaName]);

  useEffect(() => {
    if (!previewStrategyName && strategies.length > 0) {
      setPreviewStrategyName(strategies[0].name);
    }
  }, [previewStrategyName, strategies]);

  useEffect(() => {
    if (detailQuery.data) {
      setDraft(detailQuery.data);
    }
  }, [detailQuery.data]);

  const catalogByTable = useMemo(() => {
    const tableMap = new Map<string, RankingCatalogColumn[]>();
    rankingCatalog?.tables.forEach((table) => {
      tableMap.set(table.name, table.columns);
    });
    return tableMap;
  }, [rankingCatalog]);

  const saveMutation = useMutation({
    mutationFn: () =>
      rankingApi.saveRankingSchema({
        name: draft.name,
        description: draft.description,
        config: draft.config
      }),
    onSuccess: async (result) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['ranking-schemas'] }),
        queryClient.invalidateQueries({ queryKey: ['ranking-schemas', 'detail', draft.name] }),
        queryClient.invalidateQueries({ queryKey: ['strategies'] })
      ]);
      setSelectedSchemaName(draft.name);
      setDraft((current) => ({ ...current, version: result.version }));
      toast.success(`Ranking schema ${draft.name} saved`);
    },
    onError: (error) => {
      toast.error(`Failed to save ranking schema: ${formatSystemStatusText(error)}`);
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (name: string) => rankingApi.deleteRankingSchema(name),
    onSuccess: async (_, name) => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ['ranking-schemas'] }),
        queryClient.invalidateQueries({ queryKey: ['ranking-schemas', 'detail', name] }),
        queryClient.invalidateQueries({ queryKey: ['strategies'] })
      ]);
      setSelectedSchemaName(null);
      setDraft(buildEmptySchema());
      toast.success(`Ranking schema ${name} deleted`);
    },
    onError: (error) => {
      toast.error(`Failed to delete ranking schema: ${formatSystemStatusText(error)}`);
    }
  });

  const previewMutation = useMutation({
    mutationFn: () =>
      rankingApi.previewRanking({
        strategyName: previewStrategyName,
        asOfDate: previewDate,
        schema: draft.config,
        limit: 25
      }),
    onError: (error) => {
      toast.error(`Failed to preview rankings: ${formatSystemStatusText(error)}`);
    }
  });

  const materializeMutation = useMutation({
    mutationFn: () =>
      rankingApi.materializeRankings({
        strategyName: previewStrategyName
      }),
    onSuccess: (result) => {
      toast.success(
        `Materialized ${result.rowCount} rows across ${result.dateCount} dates to platinum.${result.outputTableName}`
      );
    },
    onError: (error) => {
      toast.error(`Failed to materialize rankings: ${formatSystemStatusText(error)}`);
    }
  });

  const listError = formatSystemStatusText(schemasError);
  const detailError = formatSystemStatusText(detailQuery.error);
  const catalogError = formatSystemStatusText(rankingCatalogError);

  const selectedSchemaLabel = selectedSchemaName || draft.name || 'New Ranking Configuration';

  const updateDraft = (updater: (current: RankingSchemaDetail) => RankingSchemaDetail) => {
    setDraft((current) => updater(current));
  };

  const handleCreateNew = () => {
    setSelectedSchemaName(null);
    setDraft(buildEmptySchema());
  };

  return (
    <div className="page-shell space-y-6">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Ranking Configuration</p>
          <h1 className="page-title">Ranking Configurations</h1>
          <p className="page-subtitle">
            Manage Postgres-backed ranking schemas, attach a saved ranking universe, preview against
            strategy and ranking universe intersections, and materialize platinum outputs with score
            and rank.
          </p>
        </div>
        <div className="flex gap-3">
          <Button variant="outline" onClick={handleCreateNew}>
            <Plus className="h-4 w-4" />
            New Ranking Configuration
          </Button>
          <Button
            onClick={() => saveMutation.mutate()}
            disabled={
              saveMutation.isPending || !draft.name.trim() || !draft.config.universeConfigName
            }
          >
            {saveMutation.isPending ? 'Saving...' : 'Save Ranking Configuration'}
          </Button>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,0.85fr)_minmax(0,1.5fr)]">
        <Card className="mcm-panel">
          <CardHeader className="border-b border-border/40">
            <div className="space-y-1">
              <CardTitle className="font-display text-xl">Ranking Configuration Catalog</CardTitle>
              <CardDescription>
                Select a ranking configuration to review or edit its grouped factor stack.
              </CardDescription>
            </div>
            <CardAction>
              <Badge variant="secondary">{schemas.length} total</Badge>
            </CardAction>
          </CardHeader>
          <CardContent className="space-y-4 pt-6">
            {isSchemasLoading ? (
              <PageLoader text="Loading ranking schemas..." className="h-64" />
            ) : listError ? (
              <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                {listError}
              </div>
            ) : schemas.length === 0 ? (
              <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                No ranking schemas have been saved yet. Create one to begin scoring symbols by date.
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
                  {schemas.map((schema) => (
                    <TableRow
                      key={schema.name}
                      className="cursor-pointer"
                      data-state={schema.name === selectedSchemaName ? 'selected' : undefined}
                      onClick={() => setSelectedSchemaName(schema.name)}
                    >
                      <TableCell className="whitespace-normal">
                        <div className="space-y-1">
                          <div className="font-display text-base text-foreground">
                            {schema.name}
                          </div>
                          <div className="text-xs text-muted-foreground">
                            {schema.description || 'No description provided.'}
                          </div>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">v{schema.version}</Badge>
                      </TableCell>
                      <TableCell>{formatTimestamp(schema.updated_at)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card className="mcm-panel">
            <CardHeader className="border-b border-border/40">
              <div className="space-y-1">
                <CardTitle className="font-display text-xl">Ranking Configuration Editor</CardTitle>
                <CardDescription>
                  Groups combine weighted factors, then group and overall transforms run in array
                  order.
                </CardDescription>
              </div>
              <CardAction>
                <Badge variant="secondary">{selectedSchemaLabel}</Badge>
              </CardAction>
            </CardHeader>
            <CardContent className="space-y-5 pt-6">
              {detailQuery.isLoading && selectedSchemaName ? (
                <PageLoader text="Loading ranking schema..." className="h-56" />
              ) : detailError ? (
                <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                  {detailError}
                </div>
              ) : (
                <>
                  <div className="grid gap-4 md:grid-cols-2">
                    <div className="grid gap-2">
                      <Label htmlFor="ranking-name">Schema Name</Label>
                      <Input
                        id="ranking-name"
                        readOnly={Boolean(selectedSchemaName)}
                        value={draft.name}
                        onChange={(event) =>
                          updateDraft((current) => ({ ...current, name: event.target.value }))
                        }
                        placeholder="e.g. quality-momentum"
                      />
                    </div>
                    <div className="grid gap-2">
                      <Label htmlFor="ranking-description">Description</Label>
                      <Input
                        id="ranking-description"
                        value={draft.description || ''}
                        onChange={(event) =>
                          updateDraft((current) => ({
                            ...current,
                            description: event.target.value
                          }))
                        }
                        placeholder="Explain what this schema optimizes for."
                      />
                    </div>
                  </div>

                  <div className="grid gap-2">
                    <Label htmlFor="ranking-universe-config">Ranking Universe Config</Label>
                    <Select
                      value={draft.config.universeConfigName || '__none__'}
                      onValueChange={(value) =>
                        updateDraft((current) => ({
                          ...current,
                          config: {
                            ...current.config,
                            universeConfigName: value === '__none__' ? undefined : value
                          }
                        }))
                      }
                    >
                      <SelectTrigger id="ranking-universe-config">
                        <SelectValue placeholder="Select ranking universe config" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="__none__">No universe config</SelectItem>
                        {universeConfigs.map((universe) => (
                          <SelectItem key={universe.name} value={universe.name}>
                            {universe.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      Ranking schemas now require a separately managed universe config.
                      Materialization intersects this universe with the strategy universe.
                    </p>
                  </div>

                  <div className="grid gap-4 md:grid-cols-3">
                    <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                      <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">
                        Groups
                      </div>
                      <div className="mt-2 font-display text-lg text-foreground">
                        {draft.config.groups.length}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                      <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">
                        Factors
                      </div>
                      <div className="mt-2 font-display text-lg text-foreground">
                        {draft.config.groups.reduce((sum, group) => sum + group.factors.length, 0)}
                      </div>
                    </div>
                    <div className="rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/80 p-4">
                      <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">
                        Version
                      </div>
                      <div className="mt-2 font-display text-lg text-foreground">
                        v{draft.version || 1}
                      </div>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <div className="flex items-center justify-between gap-3">
                      <h2 className="font-display text-lg text-foreground">Ranking Groups</h2>
                      <Button
                        type="button"
                        variant="secondary"
                        onClick={() =>
                          updateDraft((current) => ({
                            ...current,
                            config: {
                              ...current.config,
                              groups: [
                                ...current.config.groups,
                                {
                                  name: `group-${current.config.groups.length + 1}`,
                                  weight: 1,
                                  factors: [],
                                  transforms: []
                                }
                              ]
                            }
                          }))
                        }
                      >
                        <Layers3 className="h-4 w-4" />
                        Add Group
                      </Button>
                    </div>

                    {draft.config.groups.map((group, groupIndex) => {
                      const groupLabel = `group-${groupIndex}`;
                      return (
                        <div
                          key={`${group.name}-${groupIndex}`}
                          className="space-y-4 rounded-3xl border border-border/60 bg-card/70 p-5"
                        >
                          <div className="flex flex-wrap items-center gap-3">
                            <div className="grid min-w-[220px] flex-1 gap-2">
                              <Label htmlFor={`${groupLabel}-name`}>Group Name</Label>
                              <Input
                                id={`${groupLabel}-name`}
                                value={group.name}
                                onChange={(event) =>
                                  updateDraft((current) => {
                                    const nextGroups = current.config.groups.slice();
                                    nextGroups[groupIndex] = { ...group, name: event.target.value };
                                    return {
                                      ...current,
                                      config: { ...current.config, groups: nextGroups }
                                    };
                                  })
                                }
                              />
                            </div>
                            <div className="grid min-w-[160px] gap-2">
                              <Label htmlFor={`${groupLabel}-weight`}>Group Weight</Label>
                              <Input
                                id={`${groupLabel}-weight`}
                                type="number"
                                step="0.1"
                                value={group.weight}
                                onChange={(event) =>
                                  updateDraft((current) => {
                                    const nextGroups = current.config.groups.slice();
                                    nextGroups[groupIndex] = {
                                      ...group,
                                      weight: Number(event.target.value) || 0
                                    };
                                    return {
                                      ...current,
                                      config: { ...current.config, groups: nextGroups }
                                    };
                                  })
                                }
                              />
                            </div>
                            <Button
                              type="button"
                              variant="outline"
                              onClick={() =>
                                updateDraft((current) => ({
                                  ...current,
                                  config: {
                                    ...current.config,
                                    groups: current.config.groups.filter(
                                      (_, itemIndex) => itemIndex !== groupIndex
                                    )
                                  }
                                }))
                              }
                            >
                              <Trash2 className="h-4 w-4" />
                              Remove Group
                            </Button>
                          </div>

                          <TransformListEditor
                            label={`Group Transforms ${group.name}`}
                            transforms={group.transforms}
                            onChange={(nextTransforms) =>
                              updateDraft((current) => {
                                const nextGroups = current.config.groups.slice();
                                nextGroups[groupIndex] = { ...group, transforms: nextTransforms };
                                return {
                                  ...current,
                                  config: { ...current.config, groups: nextGroups }
                                };
                              })
                            }
                          />

                          <div className="space-y-3">
                            <div className="flex items-center justify-between gap-3">
                              <div>
                                <div className="text-xs font-black uppercase tracking-[0.18em] text-muted-foreground">
                                  Factors
                                </div>
                                <div className="text-xs text-muted-foreground">
                                  Each factor references a numeric gold column.
                                </div>
                              </div>
                              <Button
                                type="button"
                                variant="secondary"
                                size="sm"
                                onClick={() =>
                                  updateDraft((current) => {
                                    const nextGroups = current.config.groups.slice();
                                    nextGroups[groupIndex] = {
                                      ...group,
                                      factors: [
                                        ...group.factors,
                                        {
                                          name: `${group.name}-factor-${group.factors.length + 1}`,
                                          table: rankingCatalog?.tables[0]?.name || 'market_data',
                                          column:
                                            rankingCatalog?.tables[0]?.columns[0]?.name ||
                                            'return_20d',
                                          weight: 1,
                                          direction: 'desc',
                                          missingValuePolicy: 'exclude',
                                          transforms: []
                                        }
                                      ]
                                    };
                                    return {
                                      ...current,
                                      config: { ...current.config, groups: nextGroups }
                                    };
                                  })
                                }
                              >
                                <Plus className="h-4 w-4" />
                                Add Factor
                              </Button>
                            </div>

                            {group.factors.length === 0 ? (
                              <div className="rounded-xl border border-dashed p-4 text-sm text-muted-foreground">
                                No factors in this group yet.
                              </div>
                            ) : (
                              group.factors.map((factor, factorIndex) => {
                                const availableColumns = catalogByTable.get(factor.table) || [];
                                return (
                                  <div
                                    key={`${factor.name}-${factorIndex}`}
                                    className="space-y-4 rounded-2xl border border-border/50 bg-background/60 p-4"
                                  >
                                    <div className="grid gap-4 xl:grid-cols-[1.2fr_1fr_1fr_1fr_auto]">
                                      <div className="grid gap-2">
                                        <Label htmlFor={`${groupLabel}-factor-name-${factorIndex}`}>
                                          Factor Name
                                        </Label>
                                        <Input
                                          id={`${groupLabel}-factor-name-${factorIndex}`}
                                          value={factor.name}
                                          onChange={(event) =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              const nextFactors = group.factors.slice();
                                              nextFactors[factorIndex] = {
                                                ...factor,
                                                name: event.target.value
                                              };
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: nextFactors
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        />
                                      </div>
                                      <div className="grid gap-2">
                                        <Label
                                          htmlFor={`${groupLabel}-factor-table-${factorIndex}`}
                                        >
                                          Gold Table
                                        </Label>
                                        <Select
                                          value={factor.table}
                                          onValueChange={(value) =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              const nextFactors = group.factors.slice();
                                              const nextColumns = catalogByTable.get(value) || [];
                                              nextFactors[factorIndex] = {
                                                ...factor,
                                                table: value,
                                                column: nextColumns[0]?.name || factor.column
                                              };
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: nextFactors
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        >
                                          <SelectTrigger
                                            id={`${groupLabel}-factor-table-${factorIndex}`}
                                          >
                                            <SelectValue placeholder="Select table" />
                                          </SelectTrigger>
                                          <SelectContent>
                                            {rankingCatalog?.tables.map((table) => (
                                              <SelectItem key={table.name} value={table.name}>
                                                {table.name}
                                              </SelectItem>
                                            ))}
                                          </SelectContent>
                                        </Select>
                                      </div>
                                      <div className="grid gap-2">
                                        <Label
                                          htmlFor={`${groupLabel}-factor-column-${factorIndex}`}
                                        >
                                          Column
                                        </Label>
                                        <Select
                                          value={factor.column}
                                          onValueChange={(value) =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              const nextFactors = group.factors.slice();
                                              nextFactors[factorIndex] = {
                                                ...factor,
                                                column: value
                                              };
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: nextFactors
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        >
                                          <SelectTrigger
                                            id={`${groupLabel}-factor-column-${factorIndex}`}
                                          >
                                            <SelectValue placeholder="Select column" />
                                          </SelectTrigger>
                                          <SelectContent>
                                            {availableColumns.map((column) => (
                                              <SelectItem key={column.name} value={column.name}>
                                                {column.name}
                                              </SelectItem>
                                            ))}
                                          </SelectContent>
                                        </Select>
                                      </div>
                                      <div className="grid gap-2">
                                        <Label
                                          htmlFor={`${groupLabel}-factor-weight-${factorIndex}`}
                                        >
                                          Weight
                                        </Label>
                                        <Input
                                          id={`${groupLabel}-factor-weight-${factorIndex}`}
                                          type="number"
                                          step="0.1"
                                          value={factor.weight}
                                          onChange={(event) =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              const nextFactors = group.factors.slice();
                                              nextFactors[factorIndex] = {
                                                ...factor,
                                                weight: Number(event.target.value) || 0
                                              };
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: nextFactors
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        />
                                      </div>
                                      <div className="flex items-end">
                                        <Button
                                          type="button"
                                          variant="outline"
                                          onClick={() =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: group.factors.filter(
                                                  (_, itemIndex) => itemIndex !== factorIndex
                                                )
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        >
                                          <Trash2 className="h-4 w-4" />
                                          Remove
                                        </Button>
                                      </div>
                                    </div>

                                    <div className="grid gap-4 md:grid-cols-2">
                                      <div className="grid gap-2">
                                        <Label
                                          htmlFor={`${groupLabel}-factor-direction-${factorIndex}`}
                                        >
                                          Direction
                                        </Label>
                                        <Select
                                          value={factor.direction}
                                          onValueChange={(value) =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              const nextFactors = group.factors.slice();
                                              nextFactors[factorIndex] = {
                                                ...factor,
                                                direction: value as 'asc' | 'desc'
                                              };
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: nextFactors
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        >
                                          <SelectTrigger
                                            id={`${groupLabel}-factor-direction-${factorIndex}`}
                                          >
                                            <SelectValue placeholder="Select direction" />
                                          </SelectTrigger>
                                          <SelectContent>
                                            {DIRECTION_OPTIONS.map((option) => (
                                              <SelectItem key={option.value} value={option.value}>
                                                {option.label}
                                              </SelectItem>
                                            ))}
                                          </SelectContent>
                                        </Select>
                                      </div>
                                      <div className="grid gap-2">
                                        <Label
                                          htmlFor={`${groupLabel}-factor-missing-${factorIndex}`}
                                        >
                                          Missing Policy
                                        </Label>
                                        <Select
                                          value={factor.missingValuePolicy}
                                          onValueChange={(value) =>
                                            updateDraft((current) => {
                                              const nextGroups = current.config.groups.slice();
                                              const nextFactors = group.factors.slice();
                                              nextFactors[factorIndex] = {
                                                ...factor,
                                                missingValuePolicy: value as 'exclude' | 'zero'
                                              };
                                              nextGroups[groupIndex] = {
                                                ...group,
                                                factors: nextFactors
                                              };
                                              return {
                                                ...current,
                                                config: { ...current.config, groups: nextGroups }
                                              };
                                            })
                                          }
                                        >
                                          <SelectTrigger
                                            id={`${groupLabel}-factor-missing-${factorIndex}`}
                                          >
                                            <SelectValue placeholder="Select policy" />
                                          </SelectTrigger>
                                          <SelectContent>
                                            {MISSING_POLICY_OPTIONS.map((option) => (
                                              <SelectItem key={option.value} value={option.value}>
                                                {option.label}
                                              </SelectItem>
                                            ))}
                                          </SelectContent>
                                        </Select>
                                      </div>
                                    </div>

                                    <TransformListEditor
                                      label={`Factor Transforms ${group.name}-${factor.name}`}
                                      transforms={factor.transforms}
                                      onChange={(nextTransforms) =>
                                        updateDraft((current) => {
                                          const nextGroups = current.config.groups.slice();
                                          const nextFactors = group.factors.slice();
                                          nextFactors[factorIndex] = {
                                            ...factor,
                                            transforms: nextTransforms
                                          };
                                          nextGroups[groupIndex] = {
                                            ...group,
                                            factors: nextFactors
                                          };
                                          return {
                                            ...current,
                                            config: { ...current.config, groups: nextGroups }
                                          };
                                        })
                                      }
                                    />
                                  </div>
                                );
                              })
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>

                  <TransformListEditor
                    label="Overall Transforms"
                    transforms={draft.config.overallTransforms}
                    onChange={(nextTransforms) =>
                      updateDraft((current) => ({
                        ...current,
                        config: { ...current.config, overallTransforms: nextTransforms }
                      }))
                    }
                  />

                  <div className="space-y-2">
                    <Label htmlFor="ranking-config-preview">Normalized Config Preview</Label>
                    <Textarea
                      id="ranking-config-preview"
                      readOnly
                      className="min-h-[180px] font-mono text-xs"
                      value={JSON.stringify(draft.config, null, 2)}
                    />
                  </div>

                  {selectedSchemaName ? (
                    <div className="flex justify-end">
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => deleteMutation.mutate(selectedSchemaName)}
                        disabled={deleteMutation.isPending}
                      >
                        <Trash2 className="h-4 w-4" />
                        {deleteMutation.isPending ? 'Deleting...' : 'Delete Schema'}
                      </Button>
                    </div>
                  ) : null}
                </>
              )}
            </CardContent>
          </Card>

          <Card className="mcm-panel">
            <CardHeader className="border-b border-border/40">
              <div className="space-y-1">
                <CardTitle className="font-display text-xl">Preview and Materialize</CardTitle>
                <CardDescription>
                  Use a saved strategy plus the current ranking universe to preview ranked symbols
                  for one date or materialize the full platinum output.
                </CardDescription>
              </div>
            </CardHeader>
            <CardContent className="space-y-5 pt-6">
              {isCatalogLoading ? (
                <PageLoader text="Loading gold ranking catalog..." className="h-40" />
              ) : catalogError ? (
                <div className="rounded-2xl border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                  {catalogError}
                </div>
              ) : (
                <>
                  <div className="grid gap-4 md:grid-cols-[1.2fr_1fr_auto_auto]">
                    <div className="grid gap-2">
                      <Label htmlFor="preview-strategy">Strategy</Label>
                      <Select value={previewStrategyName} onValueChange={setPreviewStrategyName}>
                        <SelectTrigger id="preview-strategy">
                          <SelectValue placeholder="Select strategy" />
                        </SelectTrigger>
                        <SelectContent>
                          {strategies.map((strategy) => (
                            <SelectItem key={strategy.name} value={strategy.name}>
                              {strategy.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="grid gap-2">
                      <Label htmlFor="preview-date">As Of Date</Label>
                      <Input
                        id="preview-date"
                        type="date"
                        value={previewDate}
                        onChange={(event) => setPreviewDate(event.target.value)}
                      />
                    </div>
                    <div className="flex items-end">
                      <Button
                        type="button"
                        variant="secondary"
                        disabled={
                          previewMutation.isPending ||
                          !previewStrategyName ||
                          !draft.config.universeConfigName
                        }
                        onClick={() => previewMutation.mutate()}
                      >
                        <Eye className="h-4 w-4" />
                        {previewMutation.isPending ? 'Previewing...' : 'Preview'}
                      </Button>
                    </div>
                    <div className="flex items-end">
                      <Button
                        type="button"
                        disabled={materializeMutation.isPending || !previewStrategyName}
                        onClick={() => materializeMutation.mutate()}
                      >
                        <RefreshCcw className="h-4 w-4" />
                        {materializeMutation.isPending ? 'Running...' : 'Materialize'}
                      </Button>
                    </div>
                  </div>

                  {previewMutation.data ? (
                    <div className="space-y-4 rounded-2xl border border-mcm-walnut/25 bg-mcm-paper/75 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div>
                          <div className="font-display text-lg text-foreground">
                            {previewMutation.data.strategyName} on {previewMutation.data.asOfDate}
                          </div>
                          <div className="text-sm text-muted-foreground">
                            {previewMutation.data.rowCount} symbols ranked with the current draft
                            configuration.
                          </div>
                        </div>
                        <Badge variant="secondary">{previewMutation.data.rows.length} shown</Badge>
                      </div>

                      {previewMutation.data.warnings.length > 0 ? (
                        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-700">
                          {previewMutation.data.warnings.join(' ')}
                        </div>
                      ) : null}

                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Rank</TableHead>
                            <TableHead>Symbol</TableHead>
                            <TableHead>Score</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {previewMutation.data.rows.map((row) => (
                            <TableRow key={`${row.symbol}-${row.rank}`}>
                              <TableCell>{row.rank}</TableCell>
                              <TableCell className="font-semibold">{row.symbol}</TableCell>
                              <TableCell>{row.score.toFixed(4)}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  ) : (
                    <div className="rounded-2xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/70 p-6 text-sm text-muted-foreground">
                      Select a strategy and date, then preview the current ranking draft before
                      materializing.
                    </div>
                  )}
                </>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
