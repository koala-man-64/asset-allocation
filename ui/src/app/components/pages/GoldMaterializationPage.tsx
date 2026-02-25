import { useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Play, RefreshCw, Save, Trash2 } from 'lucide-react';

import { DataService } from '@/services/DataService';
import { useJobTrigger } from '@/hooks/useJobTrigger';
import {
  queryKeys,
  useRuntimeConfigCatalogQuery,
  useRuntimeConfigQuery
} from '@/hooks/useDataQueries';
import type { RuntimeConfigItem } from '@/services/apiService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { formatTimeAgo } from '@/app/components/pages/system-status/SystemStatusHelpers';
import { PageLoader } from '@/app/components/common/PageLoader';

import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Input } from '@/app/components/ui/input';
import { Switch } from '@/app/components/ui/switch';
import { Textarea } from '@/app/components/ui/textarea';

type MaterializationFormState = {
  enabled: boolean;
  targetPath: string;
  sourcePrefix: string;
  columns: string;
  maxTables: string;
  yearMonth: string;
};

type ConfigKeyDef = {
  key: string;
  label: string;
  helper: string;
  placeholder?: string;
};

const SCOPE = 'global';
const GOLD_MARKET_JOB = 'gold-market-job';

const KEY_DEFS: ConfigKeyDef[] = [
  {
    key: 'GOLD_MARKET_BY_DATE_ENABLED',
    label: 'Enable by-date materialization',
    helper: 'When enabled, gold-market-job writes market_by_date after per-symbol feature generation.',
    placeholder: 'false'
  },
  {
    key: 'GOLD_MARKET_BY_DATE_PATH',
    label: 'Target path',
    helper: 'Delta table path in the Gold container for the consolidated by-date view.',
    placeholder: 'market_by_date'
  },
  {
    key: 'GOLD_MARKET_SOURCE_PREFIX',
    label: 'Source prefix',
    helper: 'Per-symbol Gold source prefix to scan for market tables.',
    placeholder: 'market'
  },
  {
    key: 'GOLD_MARKET_BY_DATE_COLUMNS',
    label: 'Included columns',
    helper: 'Comma-separated projection. date/symbol are always included by the materializer.',
    placeholder: 'close,volume,return_1d,vol_20d'
  },
  {
    key: 'GOLD_MARKET_BY_DATE_MAX_TABLES',
    label: 'Max source tables (optional)',
    helper: 'Debug throttle for source table discovery. Leave blank for full scan.',
    placeholder: '250'
  },
  {
    key: 'MATERIALIZE_YEAR_MONTH',
    label: 'Year-month partition (optional)',
    helper: 'Limit materialization to YYYY-MM for partial rebuilds.',
    placeholder: '2026-02'
  }
];

function parseBool(value: string, fallback = false): boolean {
  const lowered = String(value || '').trim().toLowerCase();
  if (['1', 'true', 't', 'yes', 'y', 'on'].includes(lowered)) return true;
  if (['0', 'false', 'f', 'no', 'n', 'off'].includes(lowered)) return false;
  return fallback;
}

function parseColumns(raw: string): string[] {
  return raw
    .split(/[,\n;]/)
    .map((token) => token.trim())
    .filter(Boolean)
    .map((token) => token.toLowerCase());
}

function sourceBadge(item: RuntimeConfigItem | undefined) {
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

export function GoldMaterializationPage() {
  const queryClient = useQueryClient();
  const runtimeConfigQuery = useRuntimeConfigQuery(SCOPE);
  const catalogQuery = useRuntimeConfigCatalogQuery();
  const { triggeringJob, triggerJob } = useJobTrigger();

  const [form, setForm] = useState<MaterializationFormState>({
    enabled: false,
    targetPath: 'market_by_date',
    sourcePrefix: 'market',
    columns: '',
    maxTables: '',
    yearMonth: ''
  });
  const [isDirty, setIsDirty] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isClearing, setIsClearing] = useState(false);

  const byKey = useMemo(() => {
    const map = new Map<string, RuntimeConfigItem>();
    for (const item of runtimeConfigQuery.data?.items || []) {
      map.set(item.key, item);
    }
    return map;
  }, [runtimeConfigQuery.data]);

  const catalogByKey = useMemo(() => {
    const map = new Map<string, { key: string; description: string; example: string }>();
    for (const item of catalogQuery.data?.items || []) {
      map.set(item.key, item);
    }
    return map;
  }, [catalogQuery.data]);

  useEffect(() => {
    if (isDirty) return;
    setForm({
      enabled: parseBool(byKey.get('GOLD_MARKET_BY_DATE_ENABLED')?.value || 'false', false),
      targetPath: byKey.get('GOLD_MARKET_BY_DATE_PATH')?.value || 'market_by_date',
      sourcePrefix: byKey.get('GOLD_MARKET_SOURCE_PREFIX')?.value || 'market',
      columns: byKey.get('GOLD_MARKET_BY_DATE_COLUMNS')?.value || '',
      maxTables: byKey.get('GOLD_MARKET_BY_DATE_MAX_TABLES')?.value || '',
      yearMonth: byKey.get('MATERIALIZE_YEAR_MONTH')?.value || ''
    });
  }, [byKey, isDirty]);

  const yearMonthTrimmed = form.yearMonth.trim();
  const maxTablesTrimmed = form.maxTables.trim();
  const columnsPreview = useMemo(() => parseColumns(form.columns), [form.columns]);

  const isYearMonthValid = !yearMonthTrimmed || /^\d{4}-\d{2}$/.test(yearMonthTrimmed);
  const isMaxTablesValid = !maxTablesTrimmed || /^[1-9]\d*$/.test(maxTablesTrimmed);
  const hasValidationError = !isYearMonthValid || !isMaxTablesValid;

  const refresh = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfig(SCOPE) }),
      queryClient.invalidateQueries({ queryKey: queryKeys.runtimeConfigCatalog() })
    ]);
  };

  const setField = <K extends keyof MaterializationFormState>(key: K, value: MaterializationFormState[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }));
    setIsDirty(true);
  };

  const saveOverrides = async () => {
    if (hasValidationError) return;

    const payload: Array<{ key: string; value: string }> = [
      { key: 'GOLD_MARKET_BY_DATE_ENABLED', value: form.enabled ? 'true' : 'false' },
      { key: 'GOLD_MARKET_BY_DATE_PATH', value: form.targetPath.trim() },
      { key: 'GOLD_MARKET_SOURCE_PREFIX', value: form.sourcePrefix.trim() },
      { key: 'GOLD_MARKET_BY_DATE_COLUMNS', value: form.columns.trim() },
      { key: 'GOLD_MARKET_BY_DATE_MAX_TABLES', value: maxTablesTrimmed },
      { key: 'MATERIALIZE_YEAR_MONTH', value: yearMonthTrimmed }
    ];

    setIsSaving(true);
    try {
      await Promise.all(
        payload.map((item) =>
          DataService.setRuntimeConfig({
            key: item.key,
            scope: SCOPE,
            enabled: true,
            value: item.value,
            description:
              byKey.get(item.key)?.description || catalogByKey.get(item.key)?.description || ''
          })
        )
      );

      toast.success('Gold materialization overrides saved.');
      setIsDirty(false);
      await refresh();
    } catch (error) {
      toast.error(`Failed to save overrides: ${formatSystemStatusText(error)}`);
    } finally {
      setIsSaving(false);
    }
  };

  const clearOverrides = async () => {
    const keysWithOverrides = KEY_DEFS.map((item) => item.key).filter((key) => byKey.has(key));
    if (!keysWithOverrides.length) {
      toast.message('No DB overrides to clear.');
      return;
    }

    setIsClearing(true);
    try {
      await Promise.all(keysWithOverrides.map((key) => DataService.deleteRuntimeConfig(key, SCOPE)));
      toast.success('Gold materialization overrides cleared.');
      setIsDirty(false);
      await refresh();
    } catch (error) {
      toast.error(`Failed to clear overrides: ${formatSystemStatusText(error)}`);
    } finally {
      setIsClearing(false);
    }
  };

  const isLoading = runtimeConfigQuery.isLoading || catalogQuery.isLoading;
  const hasError = Boolean(runtimeConfigQuery.error || catalogQuery.error);

  if (isLoading) {
    return <PageLoader text="Loading Gold Materialization Controls..." />;
  }

  if (hasError) {
    const message =
      formatSystemStatusText(runtimeConfigQuery.error) ||
      formatSystemStatusText(catalogQuery.error) ||
      'Gold materialization controls are unavailable.';

    return (
      <div className="mcm-panel rounded-lg border border-destructive/30 bg-destructive/10 p-6 text-destructive">
        <p className="font-mono text-sm uppercase tracking-wide">Gold Materialization Unavailable</p>
        <p className="mt-3 text-sm">{message}</p>
      </div>
    );
  }

  return (
    <div className="page-shell">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Live Operations</p>
          <h1 className="page-title">Gold Materialization</h1>
          <p className="page-subtitle">
            Configure by-date Gold market materialization (`market_by_date`) and trigger a run through
            `{` ${GOLD_MARKET_JOB} `}`.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => void refresh()}
            disabled={runtimeConfigQuery.isFetching || catalogQuery.isFetching}
          >
            <RefreshCw
              className={`h-4 w-4 ${runtimeConfigQuery.isFetching || catalogQuery.isFetching ? 'animate-spin' : ''}`}
            />
            Refresh
          </Button>
          <Button
            size="sm"
            className="gap-2"
            onClick={() => void triggerJob(GOLD_MARKET_JOB, ['systemHealth'])}
            disabled={Boolean(triggeringJob)}
          >
            <Play className="h-4 w-4" />
            {triggeringJob === GOLD_MARKET_JOB ? 'Running...' : 'Run Gold Job'}
          </Button>
        </div>
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
        <Card className="mcm-panel">
          <CardHeader>
            <CardTitle>By-Date Controls</CardTitle>
          </CardHeader>
          <CardContent className="space-y-5">
            <div className="flex items-center justify-between rounded-lg border border-border/60 bg-muted/20 px-4 py-3">
              <div>
                <div className="text-xs uppercase text-muted-foreground">Materializer Enabled</div>
                <div className="text-sm">
                  {form.enabled
                    ? 'Runs after gold-market-job feature generation.'
                    : 'Gold market job skips by-date materialization.'}
                </div>
              </div>
              <Switch
                checked={form.enabled}
                onCheckedChange={(checked) => setField('enabled', Boolean(checked))}
                aria-label="Enable by-date materialization"
              />
            </div>

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <label htmlFor="gold-materialization-target-path" className="text-xs uppercase text-muted-foreground">
                  Target Path
                </label>
                <Input
                  id="gold-materialization-target-path"
                  value={form.targetPath}
                  onChange={(event) => setField('targetPath', event.target.value)}
                  placeholder="market_by_date"
                  className="font-mono text-sm"
                />
              </div>
              <div className="space-y-2">
                <label htmlFor="gold-materialization-source-prefix" className="text-xs uppercase text-muted-foreground">
                  Source Prefix
                </label>
                <Input
                  id="gold-materialization-source-prefix"
                  value={form.sourcePrefix}
                  onChange={(event) => setField('sourcePrefix', event.target.value)}
                  placeholder="market"
                  className="font-mono text-sm"
                />
              </div>
            </div>

            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <label htmlFor="gold-materialization-max-tables" className="text-xs uppercase text-muted-foreground">
                  Max Source Tables
                </label>
                <Input
                  id="gold-materialization-max-tables"
                  value={form.maxTables}
                  onChange={(event) => setField('maxTables', event.target.value)}
                  placeholder="250"
                  className="font-mono text-sm"
                />
                {!isMaxTablesValid ? (
                  <p className="text-xs text-destructive">Must be a positive integer.</p>
                ) : null}
              </div>
              <div className="space-y-2">
                <label htmlFor="gold-materialization-year-month" className="text-xs uppercase text-muted-foreground">
                  Year-Month Partition
                </label>
                <Input
                  id="gold-materialization-year-month"
                  value={form.yearMonth}
                  onChange={(event) => setField('yearMonth', event.target.value)}
                  placeholder="YYYY-MM"
                  className="font-mono text-sm"
                />
                {!isYearMonthValid ? (
                  <p className="text-xs text-destructive">Use YYYY-MM format (example: 2026-02).</p>
                ) : null}
              </div>
            </div>

            <div className="space-y-2">
              <label htmlFor="gold-materialization-columns" className="text-xs uppercase text-muted-foreground">
                Included Columns
              </label>
              <Textarea
                id="gold-materialization-columns"
                value={form.columns}
                onChange={(event) => setField('columns', event.target.value)}
                placeholder="close,volume,return_1d,vol_20d"
                className="min-h-[132px] font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">
                Leave blank to include all columns from per-symbol Gold tables. `date` and `symbol`
                are always included.
              </p>
              {columnsPreview.length ? (
                <div className="flex flex-wrap gap-2">
                  {columnsPreview.slice(0, 20).map((column) => (
                    <Badge key={column} variant="secondary" className="font-mono text-[11px]">
                      {column}
                    </Badge>
                  ))}
                  {columnsPreview.length > 20 ? (
                    <Badge variant="outline" className="font-mono text-[11px]">
                      +{columnsPreview.length - 20}
                    </Badge>
                  ) : null}
                </div>
              ) : null}
            </div>

            <div className="flex flex-wrap items-center gap-3">
              <Button
                className="gap-2"
                onClick={() => void saveOverrides()}
                disabled={isSaving || hasValidationError}
              >
                {isSaving ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                Save DB Overrides
              </Button>
              <Button
                variant="outline"
                className="gap-2"
                onClick={() => void clearOverrides()}
                disabled={isClearing}
              >
                {isClearing ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                Reset To Env Defaults
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card className="mcm-panel">
          <CardHeader>
            <CardTitle>Runtime Source Map</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {KEY_DEFS.map((item) => {
              const current = byKey.get(item.key);
              return (
                <div key={item.key} className="rounded-lg border border-border/60 bg-muted/20 p-3">
                  <div className="flex items-center justify-between gap-2">
                    <div className="font-mono text-xs">{item.key}</div>
                    {sourceBadge(current)}
                  </div>
                  <p className="mt-2 text-xs text-muted-foreground">{item.helper}</p>
                  <div className="mt-2 text-[11px] text-muted-foreground">
                    Value: <span className="font-mono">{current?.value || item.placeholder || '—'}</span>
                  </div>
                  <div className="mt-1 text-[11px] text-muted-foreground">
                    Updated: <span className="font-mono">{formatTimeAgo(current?.updatedAt || null)}</span>
                  </div>
                </div>
              );
            })}
            <div className="rounded-lg border border-dashed border-border/70 bg-muted/30 p-3 text-xs text-muted-foreground">
              Triggering `{GOLD_MARKET_JOB}` uses the latest runtime-config values at job startup.
              The by-date materializer executes only when `GOLD_MARKET_BY_DATE_ENABLED=true`.
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
