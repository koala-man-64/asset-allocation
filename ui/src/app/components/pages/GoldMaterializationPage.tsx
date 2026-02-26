import { useEffect, useMemo, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
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
import { Checkbox } from '@/app/components/ui/checkbox';
import { Input } from '@/app/components/ui/input';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/app/components/ui/select';
import { Switch } from '@/app/components/ui/switch';
import { Textarea } from '@/app/components/ui/textarea';

type GoldDomain = 'market' | 'finance' | 'earnings' | 'price-target';

type DomainOption = {
  value: GoldDomain;
  label: string;
  defaultTargetPath: string;
};

type MaterializationFormState = {
  domain: GoldDomain;
  enabled: boolean;
  targetPath: string;
  columns: string;
  yearMonthStart: string;
  yearMonthEnd: string;
};

type ConfigKeyDef = {
  key: string;
  label: string;
  helper: string;
  placeholder?: string;
};

const SCOPE = 'global';
const GOLD_MARKET_JOB = 'gold-market-job';
const GOLD_LAYER = 'gold';

const DOMAIN_OPTIONS: DomainOption[] = [
  { value: 'market', label: 'Market', defaultTargetPath: 'market_by_date' },
  { value: 'finance', label: 'Finance', defaultTargetPath: 'finance_by_date' },
  { value: 'earnings', label: 'Earnings', defaultTargetPath: 'earnings_by_date' },
  { value: 'price-target', label: 'Price Target', defaultTargetPath: 'price_target_by_date' }
];

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
    key: 'GOLD_BY_DATE_DOMAIN',
    label: 'Domain',
    helper: 'Gold domain to materialize by-date (market|finance|earnings|price-target).',
    placeholder: 'market'
  },
  {
    key: 'GOLD_MARKET_BY_DATE_COLUMNS',
    label: 'Included columns',
    helper: 'Comma-separated projection. date/symbol are always included by the materializer.',
    placeholder: 'close,volume,return_1d,vol_20d'
  },
  {
    key: 'MATERIALIZE_YEAR_MONTH',
    label: 'Year-month range (optional)',
    helper: 'Limit materialization to YYYY-MM or YYYY-MM..YYYY-MM for partial rebuilds.',
    placeholder: '2026-01..2026-03'
  }
];

const YEAR_MONTH_RE = /^\d{4}-\d{2}$/;
const YEAR_MONTH_RANGE_RE = /^(\d{4}-\d{2})(?:\s*(?:\.\.|to)\s*(\d{4}-\d{2}))?$/i;

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

function findDomainOption(value: GoldDomain): DomainOption {
  return DOMAIN_OPTIONS.find((option) => option.value === value) || DOMAIN_OPTIONS[0];
}

function parseDomain(value: string | undefined): GoldDomain {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/_/g, "-");
  if (normalized === "targets") return "price-target";
  return (
    DOMAIN_OPTIONS.find((option) => option.value === normalized)?.value || "market"
  );
}

function normalizeColumnList(columns: string[]): string[] {
  const seen = new Set<string>();
  const ordered: string[] = [];
  for (const value of columns || []) {
    const normalized = String(value || '').trim().toLowerCase();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    ordered.push(normalized);
  }
  return ordered;
}

function parseYearMonthRange(raw: string): { start: string; end: string } {
  const value = String(raw || '').trim();
  if (!value) return { start: '', end: '' };

  const match = YEAR_MONTH_RANGE_RE.exec(value);
  if (!match) {
    return { start: value, end: '' };
  }
  const start = String(match[1] || '').trim();
  const end = String(match[2] || '').trim() || start;
  return { start, end };
}

function serializeYearMonthRange(startRaw: string, endRaw: string): string {
  const start = String(startRaw || '').trim();
  const end = String(endRaw || '').trim();
  if (!start) return '';
  if (!end || end === start) return start;
  return `${start}..${end}`;
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
    domain: 'market',
    enabled: false,
    targetPath: 'market_by_date',
    columns: '',
    yearMonthStart: '',
    yearMonthEnd: ''
  });
  const [isDirty, setIsDirty] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isClearing, setIsClearing] = useState(false);

  const domainColumnsQueryKey = useMemo(() => ['domainColumns', GOLD_LAYER, form.domain] as const, [form.domain]);
  const domainColumnsQuery = useQuery({
    queryKey: domainColumnsQueryKey,
    queryFn: async () => {
      let cached: Awaited<ReturnType<typeof DataService.getDomainColumns>> | null = null;
      try {
        cached = await DataService.getDomainColumns(GOLD_LAYER, form.domain);
        if (cached.found && cached.columns.length > 0) {
          return { ...cached, columns: normalizeColumnList(cached.columns) };
        }
      } catch {
        // Ignore cache read failures here and fallback to an explicit refresh call.
      }

      try {
        const refreshed = await DataService.refreshDomainColumns({ layer: GOLD_LAYER, domain: form.domain });
        return { ...refreshed, columns: normalizeColumnList(refreshed.columns) };
      } catch (error) {
        if (cached) {
          return { ...cached, columns: normalizeColumnList(cached.columns) };
        }
        throw error;
      }
    },
    enabled: Boolean(form.domain),
    staleTime: 5 * 60 * 1000,
    retry: false,
    refetchOnWindowFocus: false
  });

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
    const configuredDomain = parseDomain(byKey.get('GOLD_BY_DATE_DOMAIN')?.value || 'market');
    const inferredDomainOption = findDomainOption(configuredDomain);
    const parsedRange = parseYearMonthRange(byKey.get('MATERIALIZE_YEAR_MONTH')?.value || '');
    setForm({
      domain: configuredDomain,
      enabled: parseBool(byKey.get('GOLD_MARKET_BY_DATE_ENABLED')?.value || 'false', false),
      targetPath: byKey.get('GOLD_MARKET_BY_DATE_PATH')?.value || inferredDomainOption.defaultTargetPath,
      columns: byKey.get('GOLD_MARKET_BY_DATE_COLUMNS')?.value || '',
      yearMonthStart: parsedRange.start,
      yearMonthEnd: parsedRange.end
    });
  }, [byKey, isDirty]);

  const yearMonthStartTrimmed = form.yearMonthStart.trim();
  const yearMonthEndTrimmed = form.yearMonthEnd.trim();
  const yearMonthSerialized = serializeYearMonthRange(yearMonthStartTrimmed, yearMonthEndTrimmed);
  const columnsPreview = useMemo(() => parseColumns(form.columns), [form.columns]);
  const selectedColumns = useMemo(() => normalizeColumnList(parseColumns(form.columns)), [form.columns]);
  const selectedColumnSet = useMemo(() => new Set<string>(selectedColumns), [selectedColumns]);
  const selectedDomainOption = useMemo(() => findDomainOption(form.domain), [form.domain]);
  const selectableColumns = useMemo(
    () =>
      (domainColumnsQuery.data?.columns || []).filter(
        (column) => !['date', 'symbol', 'year_month'].includes(String(column || '').toLowerCase())
      ),
    [domainColumnsQuery.data?.columns]
  );

  const isYearMonthStartValid = !yearMonthStartTrimmed || YEAR_MONTH_RE.test(yearMonthStartTrimmed);
  const isYearMonthEndValid = !yearMonthEndTrimmed || YEAR_MONTH_RE.test(yearMonthEndTrimmed);
  const isYearMonthRangeShapeValid = !yearMonthEndTrimmed || Boolean(yearMonthStartTrimmed);
  const isYearMonthRangeOrdered = !yearMonthStartTrimmed || !yearMonthEndTrimmed || yearMonthEndTrimmed >= yearMonthStartTrimmed;
  const hasValidationError =
    !isYearMonthStartValid ||
    !isYearMonthEndValid ||
    !isYearMonthRangeShapeValid ||
    !isYearMonthRangeOrdered;

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

  const setDomain = (domain: GoldDomain) => {
    const next = findDomainOption(domain);
    setForm((prev) => {
      const previousDomain = findDomainOption(prev.domain);
      const currentTarget = prev.targetPath.trim();
      const preserveCustomTarget =
        currentTarget.length > 0 && currentTarget !== previousDomain.defaultTargetPath;
      return {
        ...prev,
        domain,
        targetPath: preserveCustomTarget ? prev.targetPath : next.defaultTargetPath,
        columns: ''
      };
    });
    setIsDirty(true);
  };

  const setSelectedColumns = (columns: string[]) => {
    setField('columns', normalizeColumnList(columns).join(','));
  };

  const toggleColumn = (column: string, checked: boolean) => {
    const normalized = String(column || '').trim().toLowerCase();
    const next = selectedColumns.filter((item) => item !== normalized);
    if (checked && normalized) {
      next.push(normalized);
    }
    setSelectedColumns(next);
  };

  const refreshDomainColumns = async () => {
    try {
      const refreshed = await DataService.refreshDomainColumns({ layer: GOLD_LAYER, domain: form.domain });
      queryClient.setQueryData(domainColumnsQueryKey, {
        ...refreshed,
        columns: normalizeColumnList(refreshed.columns)
      });
      toast.success(`Loaded ${refreshed.columns.length} selectable columns for ${findDomainOption(form.domain).label}.`);
    } catch (error) {
      toast.error(`Failed to load columns: ${formatSystemStatusText(error)}`);
    }
  };

  const saveOverrides = async () => {
    if (hasValidationError) return;

    const payload: Array<{ key: string; value: string }> = [
      { key: 'GOLD_MARKET_BY_DATE_ENABLED', value: form.enabled ? 'true' : 'false' },
      { key: 'GOLD_BY_DATE_DOMAIN', value: form.domain },
      { key: 'GOLD_MARKET_BY_DATE_PATH', value: form.targetPath.trim() },
      { key: 'GOLD_MARKET_BY_DATE_COLUMNS', value: form.columns.trim() },
      { key: 'MATERIALIZE_YEAR_MONTH', value: yearMonthSerialized }
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
            Configure by-date Gold materialization per domain and trigger a run through `{` ${GOLD_MARKET_JOB} `}`.
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
                    : 'Gold job skips by-date materialization.'}
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
                <label htmlFor="gold-materialization-domain" className="text-xs uppercase text-muted-foreground">
                  Domain
                </label>
                <Select value={form.domain} onValueChange={(value) => setDomain(value as GoldDomain)}>
                  <SelectTrigger id="gold-materialization-domain">
                    <SelectValue placeholder="Select domain" />
                  </SelectTrigger>
                  <SelectContent>
                    {DOMAIN_OPTIONS.map((option) => (
                      <SelectItem key={option.value} value={option.value}>
                        {option.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
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
            </div>

            <div className="space-y-2">
              <label className="text-xs uppercase text-muted-foreground">
                Year-Month Range
              </label>
              <div className="grid gap-2 sm:max-w-md sm:grid-cols-2">
                <Input
                  id="gold-materialization-year-month-start"
                  value={form.yearMonthStart}
                  onChange={(event) => setField('yearMonthStart', event.target.value)}
                  placeholder="From YYYY-MM"
                  className="font-mono text-sm"
                />
                <Input
                  id="gold-materialization-year-month-end"
                  value={form.yearMonthEnd}
                  onChange={(event) => setField('yearMonthEnd', event.target.value)}
                  placeholder="To YYYY-MM"
                  className="font-mono text-sm"
                />
              </div>
              {!isYearMonthStartValid || !isYearMonthEndValid ? (
                <p className="text-xs text-destructive">Use YYYY-MM format (example: 2026-02).</p>
              ) : null}
              {!isYearMonthRangeShapeValid ? (
                <p className="text-xs text-destructive">Set a start month before setting an end month.</p>
              ) : null}
              {!isYearMonthRangeOrdered ? (
                <p className="text-xs text-destructive">Range end must be the same month or later than range start.</p>
              ) : null}
            </div>

            <div className="space-y-2">
              <label htmlFor="gold-materialization-columns" className="text-xs uppercase text-muted-foreground">
                Included Columns
              </label>
              <div className="rounded-lg border border-border/60 bg-muted/20 p-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <p className="text-xs text-muted-foreground">
                    Select columns for <span className="font-mono">{selectedDomainOption.label}</span>.
                    `date` and `symbol` are always included.
                  </p>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => setSelectedColumns(selectableColumns)}
                      disabled={!selectableColumns.length}
                    >
                      Select All
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => setSelectedColumns([])}
                      disabled={!selectedColumns.length}
                    >
                      Clear
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => void refreshDomainColumns()}
                      disabled={domainColumnsQuery.isFetching}
                    >
                      <RefreshCw className={`h-4 w-4 ${domainColumnsQuery.isFetching ? 'animate-spin' : ''}`} />
                      Refresh Columns
                    </Button>
                  </div>
                </div>

                {domainColumnsQuery.isLoading ? (
                  <p className="mt-3 text-xs text-muted-foreground">Loading selectable columns...</p>
                ) : selectableColumns.length ? (
                  <div className="mt-3 grid max-h-56 gap-2 overflow-auto pr-2 sm:grid-cols-2 lg:grid-cols-3">
                    {selectableColumns.map((column) => (
                      <label
                        key={column}
                        className="flex items-center gap-2 rounded-md border border-border/40 bg-background/70 px-2 py-1"
                      >
                        <Checkbox
                          checked={selectedColumnSet.has(column)}
                          onCheckedChange={(next) => toggleColumn(column, Boolean(next))}
                        />
                        <span className="font-mono text-xs">{column}</span>
                      </label>
                    ))}
                  </div>
                ) : (
                  <p className="mt-3 text-xs text-muted-foreground">
                    No cached columns found for this domain yet. Click <span className="font-mono">Refresh Columns</span>.
                  </p>
                )}
              </div>
              <Textarea
                id="gold-materialization-columns"
                value={form.columns}
                onChange={(event) => setField('columns', event.target.value)}
                placeholder="close,volume,return_1d,vol_20d"
                className="min-h-[96px] font-mono text-sm"
              />
              <p className="text-xs text-muted-foreground">
                This field stays editable for manual tweaks. Leave blank to include all columns from per-symbol Gold
                tables.
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
