import type { DataDomain, DataLayer } from '@/types/strategy';

export type DomainRow = {
  layerName: string;
  domain: DataDomain;
};

export type DriftRow = {
  domain: string;
  lagSeconds: number;
  from: string;
  to: string;
  slaSeconds?: number;
};

const TICKER_RE = /^[A-Z][A-Z0-9.-]{0,9}$/;

export function clampInt(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, Math.trunc(value)));
}

export function scoreFromStatus(status: string): number {
  const s = String(status || '').toLowerCase();
  if (['idle', 'pass'].includes(s)) return 0;
  if (['warn'].includes(s)) return 6;
  if (['fail'].includes(s)) return 14;
  if (['healthy', 'success'].includes(s)) return 0;
  if (['warning', 'degraded', 'stale', 'pending', 'running'].includes(s)) return 6;
  if (['critical', 'error', 'failed'].includes(s)) return 14;
  return 8;
}

export function formatDurationMs(ms?: number): string {
  if (!ms || !Number.isFinite(ms)) return '';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const seconds = ms / 1000;
  if (seconds < 60) return `${seconds.toFixed(2)}s`;
  const minutes = seconds / 60;
  return `${minutes.toFixed(1)}m`;
}

export function computeLayerDrift(layers: DataLayer[]): DriftRow[] {
  const byDomain = new Map<string, Array<{ layer: string; ts: number; maxAgeSeconds?: number }>>();

  for (const layer of layers || []) {
    for (const domain of layer.domains || []) {
      const name = String(domain?.name || '').trim();
      const lastUpdated = domain?.lastUpdated ? Date.parse(domain.lastUpdated) : NaN;
      if (!name || !Number.isFinite(lastUpdated)) continue;
      const bucket = byDomain.get(name) || [];
      bucket.push({
        layer: layer.name,
        ts: lastUpdated,
        maxAgeSeconds:
          typeof domain.maxAgeSeconds === 'number' && Number.isFinite(domain.maxAgeSeconds)
            ? Math.max(1, Math.trunc(domain.maxAgeSeconds))
            : undefined
      });
      byDomain.set(name, bucket);
    }
  }

  const drift: DriftRow[] = [];
  for (const [domain, points] of byDomain.entries()) {
    if (points.length < 2) continue;
    const sorted = [...points].sort((a, b) => a.ts - b.ts);
    const lagMs = sorted[sorted.length - 1].ts - sorted[0].ts;
    const slaCandidates = sorted
      .map((item) => item.maxAgeSeconds)
      .filter((item): item is number => typeof item === 'number' && Number.isFinite(item));
    drift.push({
      domain,
      lagSeconds: Math.max(0, Math.round(lagMs / 1000)),
      from: sorted[0].layer,
      to: sorted[sorted.length - 1].layer,
      slaSeconds: slaCandidates.length > 0 ? Math.min(...slaCandidates) : undefined
    });
  }

  return drift.sort((a, b) => b.lagSeconds - a.lagSeconds);
}

export function normalizeLayerName(
  layerName: string
): 'silver' | 'gold' | 'platinum' | 'bronze' | null {
  const key = String(layerName || '')
    .trim()
    .toLowerCase();
  if (key === 'silver') return 'silver';
  if (key === 'gold') return 'gold';
  if (key === 'platinum') return 'platinum';
  if (key === 'bronze') return 'bronze';
  return null;
}

export function normalizeDomainName(
  domainName: string
): 'market' | 'finance' | 'earnings' | 'price-target' | string {
  const key = String(domainName || '')
    .trim()
    .toLowerCase();
  if (key === 'price-target' || key === 'price_target') return 'price-target';
  return key;
}

export function domainKey(row: DomainRow): string {
  return `${row.layerName}:${row.domain.name}:${row.domain.type}:${row.domain.path}`;
}

export function parseImpactsByDomain(raw: unknown): Record<string, string[]> {
  if (!raw || typeof raw !== 'object') return {};
  const out: Record<string, string[]> = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (!Array.isArray(value)) continue;
    const cleaned = value.map((item) => String(item).trim()).filter((item) => item.length > 0);
    out[String(key).trim().toLowerCase()] = cleaned;
  }
  return out;
}

export function isValidTickerSymbol(value: string): boolean {
  const normalized = String(value || '')
    .trim()
    .toUpperCase();
  return TICKER_RE.test(normalized);
}

export function getProbeIdForRow(
  layerName: string,
  domainName: string,
  financeSubDomain: string
): string | null {
  const layerKey = normalizeLayerName(layerName) || layerName;
  const normalizedDomain = normalizeDomainName(domainName);

  if (layerKey === 'silver' && normalizedDomain === 'market') return 'probe:silver:market';
  if (layerKey === 'gold' && normalizedDomain === 'market') return 'probe:gold:market';
  if (layerKey === 'silver' && normalizedDomain === 'finance') {
    return `probe:silver:finance:${financeSubDomain}`;
  }
  if (layerKey === 'gold' && normalizedDomain === 'finance') {
    return `probe:gold:finance:${financeSubDomain}`;
  }
  if (layerKey === 'silver' && normalizedDomain === 'earnings') return 'probe:silver:earnings';
  if (layerKey === 'gold' && normalizedDomain === 'earnings') return 'probe:gold:earnings';
  if (layerKey === 'silver' && normalizedDomain === 'price-target')
    return 'probe:silver:price-target';
  if (layerKey === 'gold' && normalizedDomain === 'price-target') return 'probe:gold:price-target';
  return null;
}
