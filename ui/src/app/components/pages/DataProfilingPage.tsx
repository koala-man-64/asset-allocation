import { useCallback, useEffect, useMemo, useState } from 'react';
import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { BarChart3, Database, RefreshCw } from 'lucide-react';

import { Button } from '@/app/components/ui/button';
import { DataService } from '@/services/DataService';
import type { DataProfilingResponse } from '@/services/apiService';

type ContainerLayer = 'bronze' | 'silver' | 'gold';

interface DomainOption {
  value: string;
  label: string;
}

const containerOptions: ContainerLayer[] = ['bronze', 'silver', 'gold'];
const domainOptions: DomainOption[] = [
  { value: 'market', label: 'Market' },
  { value: 'finance', label: 'Finance' },
  { value: 'earnings', label: 'Earnings' },
  { value: 'price-target', label: 'Price Target' }
];

const controlClass =
  'h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono outline-none transition-shadow focus-visible:ring-2 focus-visible:ring-ring/40';

function formatNumber(value: number): string {
  return value.toLocaleString();
}

function toInputSafeNumber(raw: string): number {
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function DataProfilingPage() {
  const [layer, setLayer] = useState<ContainerLayer>('gold');
  const [domain, setDomain] = useState<string>('market');
  const [column, setColumn] = useState<string>('');
  const [bins, setBins] = useState<number>(20);
  const [sampleRows, setSampleRows] = useState<number>(12000);
  const [topValues, setTopValues] = useState<number>(20);

  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [columnsLoading, setColumnsLoading] = useState<boolean>(false);
  const [columnsError, setColumnsError] = useState<string | null>(null);

  const [profile, setProfile] = useState<DataProfilingResponse | null>(null);
  const [profileLoading, setProfileLoading] = useState<boolean>(false);
  const [profileError, setProfileError] = useState<string | null>(null);

  const loadColumns = useCallback(async () => {
    setColumnsLoading(true);
    setColumnsError(null);
    setProfile(null);
    setProfileError(null);
    try {
      const data = await DataService.getGenericData(layer, domain, undefined, 500);
      if (!data.length) {
        setAvailableColumns([]);
        setColumn('');
        setColumnsError(`No data available for ${layer}/${domain}.`);
        return;
      }

      const keys = Object.keys(data[0] ?? {});
      setAvailableColumns(keys);

      if (!column || !keys.includes(column)) {
        setColumn(keys[0] ?? '');
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setColumnsError(message || 'Unable to load columns.');
      setAvailableColumns([]);
      setColumn('');
    } finally {
      setColumnsLoading(false);
    }
  }, [column, domain, layer]);

  useEffect(() => {
    void loadColumns();
  }, [loadColumns]);

  const analyze = useCallback(async () => {
    if (!column) {
      setProfileError('Select a column first.');
      return;
    }

    setProfileLoading(true);
    setProfileError(null);
    try {
      const response = await DataService.getDataProfile(
        layer,
        domain,
        column,
        {
          bins,
          sampleRows,
          topValues
        }
      );
      setProfile(response);
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      setProfileError(message || 'Failed to compute profile.');
      setProfile(null);
    } finally {
      setProfileLoading(false);
    }
  }, [bins, column, domain, layer, sampleRows, topValues]);

  const chartData = profile?.bins ?? [];
  const topBuckets = profile?.topValues ?? [];
  const showChart = Boolean(profile && chartData.length > 0 && profile.kind !== 'string');
  const isStringProfile = profile?.kind === 'string';

  const maxTopCount = useMemo(() => {
    if (!topBuckets.length) return 1;
    return Math.max(...topBuckets.map((item) => item.count), 1);
  }, [topBuckets]);

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Data Exploration</p>
        <h1 className="page-title flex items-center gap-2">
          <BarChart3 className="h-5 w-5 text-mcm-olive" />
          Data Profiling
        </h1>
        <p className="page-subtitle">
          Choose container + domain, then inspect a column distribution. Numeric and date columns render
          as bucketed histograms; string columns show cardinality and top frequencies.
        </p>
      </div>

      <div className="mcm-panel p-4 sm:p-5">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-6 lg:items-end">
          <div className="space-y-2">
            <label htmlFor="dp-layer">Container</label>
            <select
              id="dp-layer"
              value={layer}
              onChange={(e) => setLayer(e.target.value as ContainerLayer)}
              className={controlClass}
            >
              {containerOptions.map((option) => (
                <option key={option} value={option}>
                  {option.toUpperCase()}
                </option>
              ))}
            </select>
          </div>

          <div className="space-y-2">
            <label htmlFor="dp-domain">Domain</label>
            <select
              id="dp-domain"
              value={domain}
              onChange={(e) => setDomain(e.target.value)}
              className={controlClass}
            >
              {domainOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          <div className="space-y-2">
            <label htmlFor="dp-column">Column</label>
            <select
              id="dp-column"
              value={column}
              onChange={(e) => setColumn(e.target.value)}
              disabled={columnsLoading || !availableColumns.length}
              className={controlClass}
            >
              <option value="" disabled>
                {columnsLoading ? 'Loading columns…' : 'Select a column'}
              </option>
              {availableColumns.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          </div>

          <div className="space-y-2">
            <label htmlFor="dp-bins">Bins (numeric/date)</label>
            <input
              id="dp-bins"
              type="number"
              min={3}
              max={200}
              value={bins}
              onChange={(e) => setBins(toInputSafeNumber(e.target.value) || 3)}
              className={controlClass}
            />
          </div>

          <div className="space-y-2">
            <label htmlFor="dp-sample">Sample rows</label>
            <input
              id="dp-sample"
              type="number"
              min={10}
              max={100000}
              value={sampleRows}
              onChange={(e) => setSampleRows(toInputSafeNumber(e.target.value) || 10)}
              className={controlClass}
            />
          </div>

          <div className="space-y-2">
            <label htmlFor="dp-top">Top string values</label>
            <input
              id="dp-top"
              type="number"
              min={1}
              max={200}
              value={topValues}
              onChange={(e) => setTopValues(toInputSafeNumber(e.target.value) || 1)}
              className={controlClass}
            />
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <Button
            onClick={() => void analyze()}
            className="h-10 gap-2"
            disabled={profileLoading || columnsLoading || !column}
          >
            {profileLoading ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            {profileLoading ? 'Profiling…' : 'Run Profile'}
          </Button>

          <Button
            variant="outline"
            className="h-10"
            onClick={() => void loadColumns()}
            disabled={columnsLoading}
          >
            {columnsLoading ? 'Refreshing columns…' : 'Refresh Columns'}
          </Button>

          <p className="text-xs font-mono text-muted-foreground">
            Domain columns fetched via {layer}/{domain} (sample 500 rows).
          </p>
        </div>

        {columnsError ? (
          <p className="mt-3 rounded-md border border-rose-300/40 bg-rose-50 px-3 py-2 text-xs text-rose-700 dark:bg-rose-950/40 dark:text-rose-300">
            {columnsError}
          </p>
        ) : null}
      </div>

      {(profileError || profile) && (
        <section className="mcm-panel p-4 sm:p-5">
          {profileError ? (
            <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              <div className="font-semibold">Profile failed</div>
              <div className="mt-1 text-xs font-mono">{profileError}</div>
            </div>
          ) : null}

          {profile ? (
            <>
              <div className="page-kicker mb-1">Profile result</div>
              <div className="grid gap-1 sm:grid-cols-2 sm:items-end sm:justify-between">
                <h2 className="text-xl font-black uppercase tracking-tight">{profile.column}</h2>
                <div className="text-xs font-mono text-muted-foreground">
                  {profile.kind.toUpperCase()} · {layer.toUpperCase()} · {domain}
                </div>
              </div>

              <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
                <div className="rounded-xl border border-border/50 bg-background/80 p-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">TOTAL ROWS</div>
                  <div className="text-xl font-black font-mono mt-1">{formatNumber(profile.totalRows)}</div>
                  <div className="text-xs text-muted-foreground mt-1">from sample</div>
                </div>

                <div className="rounded-xl border border-border/50 bg-background/80 p-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">NON-NULL</div>
                  <div className="text-xl font-black font-mono mt-1">{formatNumber(profile.nonNullCount)}</div>
                  <div className="text-xs text-muted-foreground mt-1">
                    Nulls: {formatNumber(profile.nullCount)}
                  </div>
                </div>

                <div className="rounded-xl border border-border/50 bg-background/80 p-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">UNIQUE</div>
                  <div className="text-xl font-black font-mono mt-1">
                    {formatNumber(profile.uniqueCount ?? 0)}
                  </div>
                  <div className="text-xs text-muted-foreground mt-1">Distinct values</div>
                </div>

                <div className="rounded-xl border border-border/50 bg-background/80 p-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">DUPLICATES</div>
                  <div className="text-xl font-black font-mono mt-1">
                    {formatNumber(profile.duplicateCount ?? 0)}
                  </div>
                  <div className="text-xs text-muted-foreground mt-1">Repeated values</div>
                </div>

                <div className="rounded-xl border border-border/50 bg-background/80 p-3">
                  <div className="text-[10px] uppercase tracking-[0.22em] text-muted-foreground">SAMPLE ROWS</div>
                  <div className="text-xl font-black font-mono mt-1">
                    {formatNumber(profile.sampleRows)}
                  </div>
                  <div className="text-xs text-muted-foreground mt-1">Rows fetched</div>
                </div>
              </div>

              {showChart ? (
                <div className="mt-5 rounded-xl border border-border/40 bg-background/80 p-3">
                  <div className="mb-2 flex items-center gap-2 text-sm font-mono">
                    <Database className="h-4 w-4" />
                    {profile.kind === 'date' ? 'Monthly buckets' : 'Numeric histogram'}
                  </div>
                  <div className="h-[340px]">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={chartData} margin={{ left: 8, right: 8, bottom: 54 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis
                          dataKey="label"
                          tickLine={false}
                          axisLine={false}
                          angle={-30}
                          textAnchor="end"
                          height={60}
                          interval={0}
                        />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="count" fill="var(--mcm-olive)" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              ) : null}

              {isStringProfile ? (
                <div className="mt-5 rounded-xl border border-border/40 bg-background/80 p-3">
                  <div className="mb-2 text-sm font-mono">Top string values</div>
                  {topBuckets.length === 0 ? (
                    <p className="text-xs text-muted-foreground">No string values to rank in this sample.</p>
                  ) : (
                    <div className="space-y-2">
                      {topBuckets.map((entry) => {
                        const widthPercent = Math.max(12, Math.round((entry.count / maxTopCount) * 100));
                        return (
                          <div key={`${entry.value}-${entry.count}`} className="rounded-lg border border-border/30 p-2">
                            <div className="flex items-center justify-between text-xs font-mono">
                              <span className="truncate max-w-[65%]">{entry.value || '(blank)'}</span>
                              <span className="text-muted-foreground">{formatNumber(entry.count)}</span>
                            </div>
                            <div className="mt-1 h-1.5 w-full bg-muted/60 rounded-full overflow-hidden">
                              <div
                                className="h-full bg-mcm-olive"
                                style={{ width: `${widthPercent}%` }}
                              />
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              ) : null}
            </>
          ) : null}
        </section>
      )}
    </div>
  );
}
