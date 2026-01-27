import { useCallback, useMemo, useState } from 'react';
import { useLineageQuery, useSystemHealthQuery } from '@/hooks/useDataQueries';
import type { DataDomain, DataLayer } from '@/types/strategy';
import { apiClient } from '@/services/apiClient';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Badge } from '@/app/components/ui/badge';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { cn } from '@/app/components/ui/utils';
import { formatTimeAgo, getStatusConfig } from './system-status/SystemStatusHelpers';
import {
  ArrowUpRight,
  CheckCircle2,
  CircleSlash2,
  ExternalLink,
  FlaskConical,
  RefreshCw,
  ScanSearch,
  ShieldAlert,
  Timer,
  TriangleAlert,
} from 'lucide-react';

type ProbeStatus = 'idle' | 'running' | 'pass' | 'warn' | 'fail';

type ProbeResult = {
  status: ProbeStatus;
  at: string;
  ms?: number;
  title: string;
  detail?: string;
  meta?: Record<string, unknown>;
};

type DomainRow = {
  layerName: string;
  layerStatus: string;
  layerPortalUrl?: string;
  domain: DataDomain;
};

const DEFAULT_TICKER = 'SPY';
const DEFAULT_FINANCE_SUBDOMAIN = 'balance_sheet';

const FINANCE_SUBDOMAINS: Array<{ value: string; label: string }> = [
  { value: 'balance_sheet', label: 'Balance Sheet' },
  { value: 'income_statement', label: 'Income Statement' },
  { value: 'cash_flow', label: 'Cash Flow' },
  { value: 'valuation', label: 'Valuation' },
];

function nowIso(): string {
  return new Date().toISOString();
}

function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.trunc(value)));
}

function scoreFromStatus(status: string): number {
  const s = String(status || '').toLowerCase();
  if (['idle', 'pass'].includes(s)) return 0;
  if (['warn'].includes(s)) return 6;
  if (['fail'].includes(s)) return 14;
  if (['healthy', 'success'].includes(s)) return 0;
  if (['warning', 'degraded', 'stale', 'pending', 'running'].includes(s)) return 6;
  if (['critical', 'error', 'failed'].includes(s)) return 14;
  return 8;
}

function ProbePill({ status }: { status: ProbeStatus }) {
  const text =
    status === 'pass'
      ? 'PASS'
      : status === 'warn'
        ? 'WARN'
        : status === 'fail'
          ? 'FAIL'
          : status === 'running'
            ? 'SCAN'
            : '—';

  const styles =
    status === 'pass'
      ? 'bg-emerald-500/15 text-emerald-700 border-emerald-500/30 dark:text-emerald-300'
      : status === 'warn'
        ? 'bg-amber-500/15 text-amber-700 border-amber-500/30 dark:text-amber-300'
        : status === 'fail'
          ? 'bg-rose-500/15 text-rose-700 border-rose-500/30 dark:text-rose-300'
          : status === 'running'
            ? 'bg-sky-500/15 text-sky-700 border-sky-500/30 dark:text-sky-300'
            : 'bg-muted/40 text-muted-foreground border-border/60';

  return (
    <Badge
      variant="outline"
      className={cn(
        'rounded-none px-2 py-0.5 font-mono text-[11px] tracking-[0.22em] uppercase',
        styles,
      )}
    >
      {text}
    </Badge>
  );
}

function formatDurationMs(ms?: number): string {
  if (!ms || !Number.isFinite(ms)) return '';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(2)}s`;
  const minutes = seconds / 60;
  return `${minutes.toFixed(1)}m`;
}

function computeLayerDrift(layers: DataLayer[]): Array<{ domain: string; lagSeconds: number; from: string; to: string }> {
  const byDomain = new Map<string, Array<{ layer: string; ts: number }>>();
  for (const layer of layers || []) {
    for (const domain of layer.domains || []) {
      const name = String(domain?.name || '').trim();
      const lastUpdated = domain?.lastUpdated ? Date.parse(domain.lastUpdated) : NaN;
      if (!name || !Number.isFinite(lastUpdated)) continue;
      const bucket = byDomain.get(name) || [];
      bucket.push({ layer: layer.name, ts: lastUpdated });
      byDomain.set(name, bucket);
    }
  }

  const drift: Array<{ domain: string; lagSeconds: number; from: string; to: string }> = [];
  for (const [domain, points] of byDomain.entries()) {
    if (points.length < 2) continue;
    const sorted = [...points].sort((a, b) => a.ts - b.ts);
    const lagMs = sorted[sorted.length - 1].ts - sorted[0].ts;
    drift.push({
      domain,
      lagSeconds: Math.max(0, Math.round(lagMs / 1000)),
      from: sorted[0].layer,
      to: sorted[sorted.length - 1].layer,
    });
  }

  return drift.sort((a, b) => b.lagSeconds - a.lagSeconds);
}

function normalizeLayerName(layerName: string): 'silver' | 'gold' | 'platinum' | 'bronze' | null {
  const key = String(layerName || '').trim().toLowerCase();
  if (key === 'silver') return 'silver';
  if (key === 'gold') return 'gold';
  if (key === 'platinum') return 'platinum';
  if (key === 'bronze') return 'bronze';
  return null;
}

function normalizeDomainName(domainName: string): 'market' | 'finance' | 'earnings' | 'price-target' | 'signals' | 'rankings' | string {
  const key = String(domainName || '').trim().toLowerCase();
  if (key === 'price-target' || key === 'price_target') return 'price-target';
  if (key === 'signals') return 'signals';
  if (key === 'rankings') return 'rankings';
  return key;
}

function domainKey(row: DomainRow): string {
  return `${row.layerName}:${row.domain.name}:${row.domain.type}:${row.domain.path}`;
}

export function DataQualityPage() {
  const health = useSystemHealthQuery();
  const lineage = useLineageQuery();

  const [ticker, setTicker] = useState(DEFAULT_TICKER);
  const [financeSubDomain, setFinanceSubDomain] = useState(DEFAULT_FINANCE_SUBDOMAIN);
  const [onlyIssues, setOnlyIssues] = useState(false);
  const [probeResults, setProbeResults] = useState<Record<string, ProbeResult>>({});

  const rows: DomainRow[] = useMemo(() => {
    const payload = health.data;
    if (!payload?.dataLayers) return [];
    const out: DomainRow[] = [];
    for (const layer of payload.dataLayers) {
      for (const domain of layer.domains || []) {
        out.push({
          layerName: layer.name,
          layerStatus: layer.status,
          layerPortalUrl: layer.portalUrl,
          domain,
        });
      }
    }
    return out;
  }, [health.data]);

  const drift = useMemo(() => computeLayerDrift(health.data?.dataLayers || []), [health.data?.dataLayers]);

  const impactsByDomain = useMemo(() => {
    const raw = lineage.data && typeof lineage.data === 'object' ? (lineage.data as { impactsByDomain?: unknown }).impactsByDomain : null;
    if (!raw || typeof raw !== 'object') return {};
    return raw as Record<string, string[]>;
  }, [lineage.data]);

  const summary = useMemo(() => {
    const payload = health.data;
    const overall = payload?.overall || 'unknown';
    const layerPenalty = (payload?.dataLayers || []).reduce((acc, layer) => acc + scoreFromStatus(layer.status), 0);
    const domainPenalty = rows.reduce((acc, row) => acc + scoreFromStatus(row.domain.status), 0);
    const probePenalty = Object.values(probeResults).reduce((acc, probe) => acc + scoreFromStatus(probe.status), 0);

    const penalty = layerPenalty + Math.round(domainPenalty * 0.6) + Math.round(probePenalty * 1.25);
    const score = clampInt(100 - penalty, 0, 100);

    const failures = rows.filter((r) => ['error', 'critical', 'failed'].includes(String(r.domain.status).toLowerCase())).length;
    const stales = rows.filter((r) => ['stale', 'warning', 'degraded'].includes(String(r.domain.status).toLowerCase())).length;
    const probesFailing = Object.values(probeResults).filter((p) => p.status === 'fail').length;

    return {
      overall,
      score,
      failures,
      stales,
      probesFailing,
    };
  }, [health.data, rows, probeResults]);

  const runProbe = useCallback(
    async (id: string, title: string, fn: () => Promise<{ ok: boolean; detail?: string; meta?: Record<string, unknown> }>) => {
      const started = performance.now();
      setProbeResults((prev) => ({
        ...prev,
        [id]: {
          status: 'running',
          title,
          at: nowIso(),
        },
      }));

      try {
        const result = await fn();
        const ms = performance.now() - started;
        const status: ProbeStatus = result.ok ? 'pass' : 'fail';
        setProbeResults((prev) => ({
          ...prev,
          [id]: {
            status,
            title,
            at: nowIso(),
            ms,
            detail: result.detail,
            meta: result.meta,
          },
        }));
      } catch (err: unknown) {
        const ms = performance.now() - started;
        const message = err instanceof Error ? err.message : String(err);
        setProbeResults((prev) => ({
          ...prev,
          [id]: {
            status: 'fail',
            title,
            at: nowIso(),
            ms,
            detail: message,
          },
        }));
      }
    },
    [],
  );

  const probeForRow = useCallback(
    async (row: DomainRow) => {
      const layer = normalizeLayerName(row.layerName);
      const domain = normalizeDomainName(row.domain.name);
      const resolvedTicker = ticker.trim().toUpperCase();

      if (!resolvedTicker) {
        setProbeResults((prev) => ({
          ...prev,
          [`row:${domainKey(row)}`]: { status: 'fail', title: 'Probe', at: nowIso(), detail: 'Ticker is required.' },
        }));
        return;
      }

      if ((layer === 'silver' || layer === 'gold') && domain === 'market') {
        const id = `probe:${layer}:market`;
        await runProbe(id, `Market (${layer})`, async () => {
          const data = await apiClient.getMarketData(resolvedTicker, layer);
          const count = Array.isArray(data) ? data.length : 0;
          return {
            ok: count > 0,
            detail: count > 0 ? `Rows: ${count.toLocaleString()}` : 'No rows returned.',
            meta: { count },
          };
        });
        return;
      }

      if ((layer === 'silver' || layer === 'gold') && domain === 'finance') {
        const id = `probe:${layer}:finance:${financeSubDomain}`;
        await runProbe(id, `Finance (${layer})`, async () => {
          const data = await apiClient.getFinanceData(resolvedTicker, financeSubDomain, layer);
          const count = Array.isArray(data) ? data.length : 0;
          return {
            ok: count > 0,
            detail: count > 0 ? `Rows: ${count.toLocaleString()}` : 'No rows returned.',
            meta: { count, subDomain: financeSubDomain },
          };
        });
        return;
      }

      if ((layer === 'silver' || layer === 'gold') && (domain === 'earnings' || domain === 'price-target')) {
        const id = `probe:${layer}:${domain}`;
        await runProbe(id, `${domain} (${layer})`, async () => {
          const data = await apiClient.getDomainData(resolvedTicker, domain, layer);
          const count = Array.isArray(data) ? data.length : 0;
          const sampleKeys =
            count > 0 && data && typeof data[0] === 'object' && data[0] !== null ? Object.keys(data[0] as object).slice(0, 8) : [];
          return {
            ok: count > 0,
            detail: count > 0 ? `Rows: ${count.toLocaleString()} • Keys: ${sampleKeys.join(', ') || '—'}` : 'No rows returned.',
            meta: { count, sampleKeys },
          };
        });
        return;
      }

      if (layer === 'platinum' && domain === 'signals') {
        const id = `probe:platinum:signals`;
        await runProbe(id, 'Signals (platinum)', async () => {
          const data = await apiClient.getSignals({ limit: 1 });
          const count = Array.isArray(data) ? data.length : 0;
          return {
            ok: true,
            detail: `Signals returned: ${count}`,
            meta: { count },
          };
        });
        return;
      }

      setProbeResults((prev) => ({
        ...prev,
        [`row:${domainKey(row)}`]: {
          status: 'warn',
          title: 'Probe',
          at: nowIso(),
          detail: 'No active probe is defined for this container/folder.',
        },
      }));
    },
    [financeSubDomain, runProbe, ticker],
  );

  const runAll = useCallback(async () => {
    const supported = rows.filter((row) => {
      const layer = normalizeLayerName(row.layerName);
      const domain = normalizeDomainName(row.domain.name);
      if (layer === 'platinum' && domain === 'signals') return true;
      if (layer === 'silver' || layer === 'gold') {
        return ['market', 'finance', 'earnings', 'price-target'].includes(domain);
      }
      return false;
    });

    for (const row of supported) {
      await probeForRow(row);
    }
  }, [probeForRow, rows]);

  const filteredRows = useMemo(() => {
    if (!onlyIssues) return rows;
    return rows.filter((row) => {
      const status = String(row.domain.status || '').toLowerCase();
      return ['warning', 'degraded', 'stale', 'critical', 'error', 'failed'].includes(status);
    });
  }, [onlyIssues, rows]);

  const headerStatus = getStatusConfig(summary.overall);

  if (health.isLoading) {
    return (
      <div className="dq min-h-[calc(100vh-6rem)] flex items-center justify-center">
        <div className="dq-panel px-6 py-5 flex items-center gap-3">
          <RefreshCw className="h-5 w-5 animate-spin text-[color:var(--dq-accent)]" />
          <div>
            <div className="dq-kicker">DATA QUALITY</div>
            <div className="dq-title text-base">Loading validation ledger…</div>
          </div>
        </div>
      </div>
    );
  }

  if (health.error || !health.data) {
    const message = health.error instanceof Error ? health.error.message : String(health.error || 'Unknown error');
    return (
      <div className="dq min-h-[calc(100vh-6rem)]">
        <div className="dq-panel p-6">
          <div className="flex items-center gap-3">
            <ShieldAlert className="h-5 w-5 text-rose-500" />
            <div>
              <div className="dq-kicker">DATA QUALITY</div>
              <div className="dq-title">System health is unavailable</div>
            </div>
          </div>
          <div className="mt-4 dq-mono text-sm text-rose-300/90">{message}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="dq min-h-[calc(100vh-6rem)]">
      <div className="dq-shell">
        <header className="dq-hero">
          <div className="dq-hero-left">
            <div className="dq-kicker flex items-center gap-2">
              <FlaskConical className="h-4 w-4" />
              VALIDATION HARNESS
            </div>
            <div className="flex items-end gap-4">
              <h1 className="dq-title">Data Quality</h1>
              <div className="dq-stamp" data-status={summary.score >= 85 ? 'pass' : summary.score >= 65 ? 'warn' : 'fail'}>
                SCORE {summary.score}
              </div>
            </div>
            <p className="dq-subtitle">
              Cross-check freshness, structure, and API reachability across the container/folder topology.
            </p>
          </div>

          <div className="dq-hero-right">
            <div className="dq-scorecard">
              <div className="dq-scorecard-top">
                <headerStatus.icon className="h-5 w-5" style={{ color: headerStatus.text }} />
                <div className="dq-mono text-xs uppercase tracking-[0.22em] text-muted-foreground">
                  SYSTEM
                </div>
              </div>
              <div className="dq-scorecard-main">
                <div className="dq-scorecard-label">Overall</div>
                <div className="dq-scorecard-value" style={{ color: headerStatus.text }}>
                  {String(summary.overall).toUpperCase()}
                </div>
              </div>
              <div className="dq-scorecard-grid">
                <div className="dq-metric">
                  <div className="dq-metric-label">Failures</div>
                  <div className="dq-metric-value">{summary.failures}</div>
                </div>
                <div className="dq-metric">
                  <div className="dq-metric-label">Stale/Warn</div>
                  <div className="dq-metric-value">{summary.stales}</div>
                </div>
                <div className="dq-metric">
                  <div className="dq-metric-label">Probes Failed</div>
                  <div className="dq-metric-value">{summary.probesFailing}</div>
                </div>
              </div>
            </div>

            <div className="dq-actions">
              <Button
                variant="outline"
                className="dq-btn"
                onClick={() => void health.refetch()}
                disabled={health.isFetching}
              >
                <RefreshCw className={cn('h-4 w-4', health.isFetching && 'animate-spin')} />
                Refresh
              </Button>
              <Button className="dq-btn-primary" onClick={() => void runAll()} disabled={health.isFetching}>
                <ScanSearch className="h-4 w-4" />
                Run Probes
              </Button>
            </div>
          </div>
        </header>

        <div className="dq-grid">
          <section className="dq-panel dq-panel-pad">
            <div className="flex items-start justify-between gap-4">
              <div>
                <div className="dq-kicker">TARGET</div>
                <div className="dq-title text-lg">Probe Config</div>
                <p className="dq-subtitle mt-1 text-sm">
                  Pick one symbol; we’ll use it to validate per-ticker partitions in Silver/Gold.
                </p>
              </div>
              <div className="dq-toggle">
                <button
                  type="button"
                  onClick={() => setOnlyIssues((v) => !v)}
                  className={cn('dq-toggle-btn', onlyIssues && 'dq-toggle-btn-on')}
                >
                  {onlyIssues ? <TriangleAlert className="h-4 w-4" /> : <CircleSlash2 className="h-4 w-4" />}
                  {onlyIssues ? 'Issues only' : 'All rows'}
                </button>
              </div>
            </div>

            <div className="mt-6 grid gap-4 sm:grid-cols-2">
              <div>
                <label className="dq-field-label" htmlFor="dq-ticker">
                  Probe Ticker
                </label>
                <div className="mt-1 flex items-center gap-2">
                  <Input
                    id="dq-ticker"
                    value={ticker}
                    onChange={(e) => setTicker(e.target.value)}
                    placeholder="SPY"
                    className="dq-input font-mono uppercase"
                    spellCheck={false}
                  />
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Button
                        variant="outline"
                        className="dq-btn-icon"
                        onClick={() => setTicker(DEFAULT_TICKER)}
                        aria-label="Reset ticker"
                      >
                        <Timer className="h-4 w-4" />
                      </Button>
                    </TooltipTrigger>
                    <TooltipContent>Reset to {DEFAULT_TICKER}</TooltipContent>
                  </Tooltip>
                </div>
              </div>

              <div>
                <label className="dq-field-label" htmlFor="dq-finance">
                  Finance View
                </label>
                <div className="mt-1 grid grid-cols-2 gap-2">
                  {FINANCE_SUBDOMAINS.map((opt) => (
                    <button
                      key={opt.value}
                      type="button"
                      className={cn('dq-pill-btn', financeSubDomain === opt.value && 'dq-pill-btn-on')}
                      onClick={() => setFinanceSubDomain(opt.value)}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </section>

          <section className="dq-panel dq-panel-pad">
            <div className="dq-kicker">DRIFT</div>
            <div className="dq-title text-lg">Cross-Layer Lag</div>
            <p className="dq-subtitle mt-1 text-sm">
              Largest observed timestamp spread across layers (best-effort, based on folder/table last modified).
            </p>

            <div className="mt-4 space-y-2">
              {(drift || []).slice(0, 6).map((item) => {
                const minutes = Math.round(item.lagSeconds / 60);
                const label = minutes >= 120 ? `${Math.round(minutes / 60)}h` : `${minutes}m`;
                const severity = minutes >= 24 * 60 ? 'fail' : minutes >= 6 * 60 ? 'warn' : 'pass';
                return (
                  <div key={item.domain} className="dq-drift-row">
                    <div className="dq-drift-left">
                      <div className="dq-mono text-xs uppercase tracking-[0.22em] text-muted-foreground">
                        {item.domain}
                      </div>
                      <div className="dq-drift-path">
                        {item.from} → {item.to}
                      </div>
                    </div>
                    <div className="dq-drift-right">
                      <ProbePill status={severity as ProbeStatus} />
                      <div className="dq-mono text-xs text-muted-foreground">{label}</div>
                    </div>
                  </div>
                );
              })}
              {drift.length === 0 && (
                <div className="dq-empty">
                  <CheckCircle2 className="h-4 w-4 text-emerald-500" />
                  <div className="dq-mono text-sm">No cross-layer lag detected from available timestamps.</div>
                </div>
              )}
            </div>
          </section>
        </div>

        <section className="dq-panel dq-panel-pad mt-6">
          <div className="flex items-center justify-between gap-4">
            <div>
              <div className="dq-kicker">CONTAINERS & FOLDERS</div>
              <div className="dq-title text-lg">Validation Ledger</div>
              <p className="dq-subtitle mt-1 text-sm">
                Each row is a folder/table probe target emitted by `/api/system/health` with optional active checks.
              </p>
            </div>
            <div className="dq-ledger-meta">
              <div className="dq-mono text-xs text-muted-foreground">
                Rows: {filteredRows.length.toLocaleString()}
              </div>
            </div>
          </div>

          <div className="mt-4 dq-ledger-table">
            <Table className="dq-table">
              <TableHeader>
                <TableRow className="dq-table-head">
                  <TableHead className="dq-th">Layer</TableHead>
                  <TableHead className="dq-th">Domain</TableHead>
                  <TableHead className="dq-th">Type</TableHead>
                  <TableHead className="dq-th">Path</TableHead>
                  <TableHead className="dq-th text-center">Freshness</TableHead>
                  <TableHead className="dq-th text-center">Last Updated</TableHead>
                  <TableHead className="dq-th text-center">Probe</TableHead>
                  <TableHead className="dq-th text-right">Links</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredRows.map((row) => {
                  const layerKey = normalizeLayerName(row.layerName) || row.layerName;
                  const domainName = normalizeDomainName(row.domain.name);
                  const status = getStatusConfig(row.domain.status);

                  const probeId = (() => {
                    if (layerKey === 'silver' && domainName === 'market') return `probe:silver:market`;
                    if (layerKey === 'gold' && domainName === 'market') return `probe:gold:market`;
                    if (layerKey === 'silver' && domainName === 'finance') return `probe:silver:finance:${financeSubDomain}`;
                    if (layerKey === 'gold' && domainName === 'finance') return `probe:gold:finance:${financeSubDomain}`;
                    if (layerKey === 'silver' && domainName === 'earnings') return `probe:silver:earnings`;
                    if (layerKey === 'gold' && domainName === 'earnings') return `probe:gold:earnings`;
                    if (layerKey === 'silver' && domainName === 'price-target') return `probe:silver:price-target`;
                    if (layerKey === 'gold' && domainName === 'price-target') return `probe:gold:price-target`;
                    if (layerKey === 'platinum' && domainName === 'signals') return `probe:platinum:signals`;
                    return null;
                  })();

                  const probe = probeId ? probeResults[probeId] : undefined;
                  const probeStatus = probe?.status || 'idle';
                  const impactedStrategies = impactsByDomain[domainName] || [];

                  return (
                    <TableRow key={domainKey(row)} className="dq-tr">
                      <TableCell className="dq-td">
                        <div className="dq-layer-cell">
                          <div className="dq-layer-tag">{String(layerKey).toUpperCase()}</div>
                          <div className="dq-layer-dot" style={{ background: status.text }} />
                        </div>
                      </TableCell>

                      <TableCell className="dq-td">
                        <div className="dq-domain-cell">
                          <div className="dq-domain-name">{row.domain.name}</div>
                          {impactedStrategies.length > 0 && (
                            <div className="dq-domain-meta">
                              <span className="dq-mono text-[11px] text-muted-foreground">
                                Impacts: {impactedStrategies.slice(0, 2).join(', ')}
                                {impactedStrategies.length > 2 ? ` +${impactedStrategies.length - 2}` : ''}
                              </span>
                            </div>
                          )}
                        </div>
                      </TableCell>

                      <TableCell className="dq-td">
                        <Badge variant="outline" className="dq-badge">
                          {row.domain.type}
                        </Badge>
                      </TableCell>

                      <TableCell className="dq-td">
                        <div className="dq-path">
                          <span className="dq-mono">{row.domain.path}</span>
                          {row.domain.version !== undefined && row.domain.version !== null && (
                            <span className="dq-path-meta">v{row.domain.version}</span>
                          )}
                        </div>
                      </TableCell>

                      <TableCell className="dq-td text-center">
                        <Badge
                          variant="outline"
                          className="dq-badge"
                          style={{
                            borderColor: status.border,
                            color: status.text,
                            backgroundColor: status.bg,
                          }}
                        >
                          {String(row.domain.status).toUpperCase()}
                        </Badge>
                      </TableCell>

                      <TableCell className="dq-td text-center">
                        <div className="dq-mono text-[11px] text-muted-foreground">
                          {row.domain.lastUpdated ? `${formatTimeAgo(row.domain.lastUpdated)} ago` : '—'}
                        </div>
                      </TableCell>

                      <TableCell className="dq-td text-center">
                        <div className="dq-probe-cell">
                          <ProbePill status={probeStatus} />
                          {probe?.ms !== undefined && (
                            <div className="dq-mono text-[10px] text-muted-foreground">{formatDurationMs(probe.ms)}</div>
                          )}
                          {probeId && (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <Button
                                  variant="outline"
                                  className="dq-btn-icon"
                                  onClick={() => void probeForRow(row)}
                                  disabled={probeStatus === 'running'}
                                  aria-label={`Probe ${row.layerName} ${row.domain.name}`}
                                >
                                  <ScanSearch className={cn('h-4 w-4', probeStatus === 'running' && 'animate-spin')} />
                                </Button>
                              </TooltipTrigger>
                              <TooltipContent>
                                {probe?.detail ? `${probe.title}: ${probe.detail}` : 'Run probe'}
                              </TooltipContent>
                            </Tooltip>
                          )}
                        </div>
                      </TableCell>

                      <TableCell className="dq-td text-right">
                        <div className="dq-links">
                          {row.domain.portalUrl ? (
                            <a className="dq-link" href={row.domain.portalUrl} target="_blank" rel="noreferrer">
                              <ExternalLink className="h-4 w-4" />
                              <span className="sr-only">Open portal</span>
                            </a>
                          ) : (
                            <span className="dq-link dq-link-muted" aria-hidden="true">
                              <ExternalLink className="h-4 w-4" />
                            </span>
                          )}

                          {row.domain.jobUrl ? (
                            <a className="dq-link" href={row.domain.jobUrl} target="_blank" rel="noreferrer">
                              <ArrowUpRight className="h-4 w-4" />
                              <span className="sr-only">Open job</span>
                            </a>
                          ) : (
                            <span className="dq-link dq-link-muted" aria-hidden="true">
                              <ArrowUpRight className="h-4 w-4" />
                            </span>
                          )}

                          {row.domain.triggerUrl ? (
                            <a className="dq-link" href={row.domain.triggerUrl} target="_blank" rel="noreferrer">
                              <CheckCircle2 className="h-4 w-4" />
                              <span className="sr-only">Trigger</span>
                            </a>
                          ) : (
                            <span className="dq-link dq-link-muted" aria-hidden="true">
                              <CheckCircle2 className="h-4 w-4" />
                            </span>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>

          <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
            <div className="dq-mono text-xs text-muted-foreground">
              Last refresh: {health.data ? nowIso().slice(0, 19).replace('T', ' ') : '—'} UTC
            </div>
            <div className="flex items-center gap-2">
              <Button variant="outline" className="dq-btn" onClick={() => setProbeResults({})}>
                <ShieldAlert className="h-4 w-4" />
                Clear Probes
              </Button>
              <Button className="dq-btn-primary" onClick={() => void runAll()}>
                <ScanSearch className="h-4 w-4" />
                Run All Supported
              </Button>
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
