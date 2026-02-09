import { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import { ValidationReport } from '@/services/apiService';
import { cn } from '@/app/components/ui/utils';
import {
  normalizeDomainName,
  normalizeLayerName,
  type DomainRow
} from '@/app/components/pages/data-quality/dataQualityUtils';
import {
  AlertCircle,
  CheckCircle,
  ChevronDown,
  FileBox,
  LayoutTemplate,
  Loader2,
  Table as TableIcon
} from 'lucide-react';
import './DataPipelinePanel.css';

// --- Types ---

export interface DriftItem {
  domain: string;
  from: string;
  to: string;
  lagSeconds: number;
  slaSeconds?: number;
}

interface PipelineNodeProps {
  layer: 'bronze' | 'silver' | 'gold';
  domain: string;
  label: string;
  lastUpdated?: string;
}

interface DataPipelinePanelProps {
  drift: DriftItem[];
  rows: DomainRow[];
}

function formatLastUpdated(lastUpdated?: string): string {
  if (!lastUpdated) return '—';
  const parsed = Date.parse(lastUpdated);
  if (!Number.isFinite(parsed)) return lastUpdated;
  return new Date(parsed).toISOString().slice(0, 19).replace('T', ' ') + ' UTC';
}

// --- Components ---

function LagIndicator({ drift }: { drift?: DriftItem }) {
  if (!drift) {
    return <div className="dq-connector" />;
  }

  const minutes = Math.round(drift.lagSeconds / 60);
  // If lag is essentially zero (under 2 mins), just show a check or "0m"
  const isFresh = minutes < 2;

  const label = isFresh
    ? '< 2m'
    : minutes >= 120
      ? `${Math.round(minutes / 60)}h`
      : `${minutes}m`;

  const severity =
    typeof drift.slaSeconds === 'number' && Number.isFinite(drift.slaSeconds)
      ? drift.lagSeconds > drift.slaSeconds
        ? 'fail'
        : drift.lagSeconds > Math.round(drift.slaSeconds * 0.5)
          ? 'warn'
          : 'pass'
      : minutes >= 24 * 60
        ? 'fail'
        : minutes >= 6 * 60
          ? 'warn'
          : 'pass';

  const colorClass =
    severity === 'fail'
      ? 'text-rose-600 border-rose-200 bg-rose-50 dark:border-rose-900/50 dark:bg-rose-900/20'
      : severity === 'warn'
        ? 'text-amber-600 border-amber-200 bg-amber-50 dark:border-amber-900/50 dark:bg-amber-900/20'
        : 'text-muted-foreground border-transparent bg-transparent'; // Neutral if good

  return (
    <div className="dq-connector-wrapper">
      <div className="dq-connector-line" />
      {!isFresh && severity !== 'pass' && (
        <div className={cn("dq-lag-badge", colorClass)}>
          {label} lag
        </div>
      )}
    </div>
  );
}

function StatusIcon({ status }: { status: string }) {
  if (status === 'error') return <AlertCircle className="h-4 w-4 text-rose-500" />;
  if (status === 'warning') return <AlertCircle className="h-4 w-4 text-amber-500" />;
  if (status === 'healthy') return <CheckCircle className="h-4 w-4 text-emerald-500" />;
  return <div className="h-2 w-2 rounded-full bg-slate-300" />;
}

function PipelineNode({ layer, domain, label, lastUpdated }: PipelineNodeProps) {
  const [isColumnsOpen, setIsColumnsOpen] = useState(false);
  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['data-quality', 'validation', layer, domain],
    queryFn: ({ signal }) => DataService.getDataQualityValidation(layer, domain, signal),
    staleTime: 1000 * 60 * 5, // 5 minutes
    retry: 1
  });

  if (isLoading) {
    return (
      <div className="dq-node-card dq-node-loading">
        <div className="dq-node-header">
          <div className="dq-node-title text-sm">{label}</div>
          <Loader2 className="h-3 w-3 animate-spin text-muted-foreground" />
        </div>
        <div className="h-10 w-full rounded bg-muted/20 dq-shimmer" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="dq-node-card border-rose-200 bg-rose-50/50 dark:border-rose-900/50 dark:bg-rose-900/10">
        <div className="dq-node-header">
          <div className="dq-node-title text-sm text-rose-700 dark:text-rose-400">{label}</div>
          <AlertCircle className="h-3 w-3 text-rose-500" />
        </div>
        <div className="text-xs text-rose-600/80">Validation failed</div>
        <button
          onClick={() => refetch()}
          className="mt-2 text-[10px] underline hover:text-rose-800"
        >
          Retry
        </button>
      </div>
    );
  }

  const report = data as ValidationReport;
  const columnsPanelId = `dq-columns-${layer}-${domain}`;
  const statusColor =
    report.status === 'error'
      ? 'text-rose-600'
      : report.status === 'empty'
        ? 'text-amber-600'
        : 'text-emerald-600';

  return (
    <div className="dq-node-card group">
      <div className="dq-node-header">
        <div className="flex items-center gap-2">
          {layer === 'bronze' ? (
            <FileBox className="h-3 w-3 text-muted-foreground" />
          ) : layer === 'silver' ? (
            <TableIcon className="h-3 w-3 text-muted-foreground" />
          ) : (
            <LayoutTemplate className="h-3 w-3 text-muted-foreground" />
          )}
          <span className="dq-node-title">{label}</span>
        </div>
        <div className="dq-node-header-right">
          <span className="dq-node-header-time-label">Updated</span>
          <span className="dq-node-header-time">{formatLastUpdated(lastUpdated)}</span>
          <StatusIcon status={report.status} />
        </div>
      </div>

      <div className="dq-node-kpis">
        <div className="dq-node-kpi">
          <span className="dq-node-meta-label">Rows</span>
          <span className={cn('dq-node-kpi-value font-mono font-medium', statusColor)}>
            {report.rowCount?.toLocaleString() ?? 0}
          </span>
        </div>
        {report.columns && report.columns.length > 0 && (
          <button
            type="button"
            className="dq-node-columns-toggle dq-node-columns-toggle-inline"
            aria-expanded={isColumnsOpen}
            aria-controls={columnsPanelId}
            onClick={() => setIsColumnsOpen((open) => !open)}
          >
            <span>Column Stats</span>
            <ChevronDown className={cn('h-3.5 w-3.5', isColumnsOpen && 'dq-chevron-open')} />
          </button>
        )}
      </div>

      {report.columns && report.columns.length > 0 && isColumnsOpen && (
        <div className="dq-node-columns">
          <div id={columnsPanelId} className="dq-node-columns-panel" role="region">
            <div className="dq-node-columns-scroll">
              <table className="dq-node-columns-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Not Null</th>
                    <th>Null %</th>
                  </tr>
                </thead>
                <tbody>
                  {report.columns.map((col) => (
                    <tr key={col.name}>
                      <td title={col.name}>{col.name}</td>
                      <td>{col.notNull.toLocaleString()}</td>
                      <td>{Number.isFinite(col.nullPct) ? col.nullPct.toFixed(1) : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export function DataPipelinePanel({ drift, rows }: DataPipelinePanelProps) {
  const domains = [
    { id: 'market', label: 'Market Data' },
    { id: 'finance', label: 'Financials' },
    { id: 'earnings', label: 'Earnings' },
    { id: 'price-target', label: 'Price Targets' }
  ];
  const lastUpdatedByLayerDomain = useMemo(() => {
    const result = new Map<string, string>();
    const rowList = Array.isArray(rows) ? rows : [];
    for (const row of rowList) {
      if (!row || !row.domain) continue;
      const layer = normalizeLayerName(row.layerName);
      const domain = normalizeDomainName(row.domain.name);
      const lastUpdated = row.domain.lastUpdated;
      if (!layer || !domain || !lastUpdated) continue;
      result.set(`${layer}:${domain}`, lastUpdated);
    }
    return result;
  }, [rows]);

  return (
    <div className="dq-pipeline-wrapper">
      <div className="dq-pipeline-container">
        <div className="dq-pipeline-header-row">
          <div className="dq-stage-header dq-stage-bronze">
            <div className="dq-stage-dot" />
            <div className="dq-stage-title">Bronze (Raw)</div>
          </div>
          <div className="dq-stage-header dq-stage-silver">
            <div className="dq-stage-dot" />
            <div className="dq-stage-title">Silver (Cleaned)</div>
          </div>
          <div className="dq-stage-header dq-stage-gold">
            <div className="dq-stage-dot" />
            <div className="dq-stage-title">Gold (Features)</div>
          </div>
        </div>
        {domains.map((d) => {
          const bronzeLag =
            drift.find(
              (item) => item.from === 'bronze' && item.to === 'silver' && item.domain === d.label
            ) ||
            drift.find(
              (item) =>
                item.domain.toLowerCase() === d.id.toLowerCase() &&
                item.from === 'bronze' &&
                item.to === 'silver'
            );
          const silverLag = drift.find(
            (item) =>
              item.domain.toLowerCase() === d.id.toLowerCase() &&
              item.from === 'silver' &&
              item.to === 'gold'
          );

          return (
            <div key={d.id} className="dq-pipeline-domain-row">
              <div className="dq-pipeline-cell dq-stage-bronze relative">
                <PipelineNode
                  layer="bronze"
                  domain={d.id}
                  label={d.label}
                  lastUpdated={lastUpdatedByLayerDomain.get(`bronze:${d.id}`)}
                />
                <LagIndicator drift={bronzeLag} />
              </div>
              <div className="dq-pipeline-cell dq-stage-silver relative">
                <PipelineNode
                  layer="silver"
                  domain={d.id}
                  label={d.label}
                  lastUpdated={lastUpdatedByLayerDomain.get(`silver:${d.id}`)}
                />
                <LagIndicator drift={silverLag} />
              </div>
              <div className="dq-pipeline-cell dq-stage-gold">
                <PipelineNode
                  layer="gold"
                  domain={d.id}
                  label={d.label}
                  lastUpdated={lastUpdatedByLayerDomain.get(`gold:${d.id}`)}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
