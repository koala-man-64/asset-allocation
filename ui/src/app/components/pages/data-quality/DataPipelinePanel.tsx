import { useQuery } from '@tanstack/react-query';
import { DataService } from '@/services/DataService';
import { ValidationReport } from '@/services/apiService';
import { cn } from '@/app/components/ui/utils';
import { AlertCircle, CheckCircle, Loader2, Table as TableIcon } from 'lucide-react';
import './DataPipelinePanel.css';

// --- Types ---

interface PipelineNodeProps {
  layer: 'bronze' | 'silver' | 'gold';
  domain: string;
  label: string;
}

// --- Components ---

function StatusIcon({ status }: { status: string }) {
  if (status === 'error') return <AlertCircle className="h-4 w-4 text-rose-500" />;
  if (status === 'warning') return <AlertCircle className="h-4 w-4 text-amber-500" />;
  if (status === 'healthy') return <CheckCircle className="h-4 w-4 text-emerald-500" />;
  return <div className="h-2 w-2 rounded-full bg-slate-300" />;
}

function PipelineNode({ layer, domain, label }: PipelineNodeProps) {
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
        <StatusIcon status={report.status} />
      </div>

      <div className="flex justify-between items-baseline mb-2">
        <span className="text-xs text-muted-foreground uppercase tracking-wider">Rows</span>
        <span className={cn('font-mono font-medium', statusColor)}>
          {report.rowCount?.toLocaleString() ?? 0}
        </span>
      </div>

      {report.columns && report.columns.length > 0 && (
        <div className="dq-detail-overlay hidden group-hover:block absolute top-[100%] left-0 right-0 z-10 shadow-lg ring-1 ring-border">
          <div className="text-[10px] font-semibold uppercase text-muted-foreground mb-1">
            Column Stats
          </div>
          {report.columns.map((col) => (
            <div key={col.name} className="dq-col-row">
              <span className="text-xs">{col.name}</span>
              <span className="font-mono text-[10px] text-muted-foreground">
                {col.unique} unique / {col.null} null
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export function DataPipelinePanel() {
  const domains = [
    { id: 'market', label: 'Market Data' },
    { id: 'finance', label: 'Financials' },
    { id: 'earnings', label: 'Earnings' },
    { id: 'price-target', label: 'Price Targets' }
  ];

  return (
    <div className="dq-pipeline-wrapper">
      <div className="dq-pipeline-container">
        {/* Bronze Stage */}
        <div className="dq-pipeline-stage dq-stage-bronze">
          <div className="dq-stage-header">
            <div className="dq-stage-dot" />
            <div className="dq-stage-title">Bronze (Raw)</div>
          </div>
          {domains.map((d) => (
            <div key={d.id} className="relative">
              <PipelineNode layer="bronze" domain={d.id} label={d.label} />
              <div className="dq-connector" />
            </div>
          ))}
        </div>

        {/* Silver Stage */}
        <div className="dq-pipeline-stage dq-stage-silver">
          <div className="dq-stage-header">
            <div className="dq-stage-dot" />
            <div className="dq-stage-title">Silver (Cleaned)</div>
          </div>
          {domains.map((d) => (
            <div key={d.id} className="relative">
              <PipelineNode layer="silver" domain={d.id} label={d.label} />
              <div className="dq-connector" />
            </div>
          ))}
        </div>

        {/* Gold Stage */}
        <div className="dq-pipeline-stage dq-stage-gold">
          <div className="dq-stage-header">
            <div className="dq-stage-dot" />
            <div className="dq-stage-title">Gold (Features)</div>
          </div>
          {domains.map((d) => (
            <PipelineNode key={d.id} layer="gold" domain={d.id} label={d.label} />
          ))}
        </div>
      </div>
    </div>
  );
}
