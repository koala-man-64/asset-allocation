import type {
  PurgeCandidatesResponse,
  PurgeOperationResponse,
  PurgeSymbolResultItem
} from '@/services/apiService';

export type MedallionLayer = 'bronze' | 'silver' | 'gold';
export type DomainKey = 'market' | 'finance' | 'earnings' | 'price-target';
export type OperatorKey = 'gt' | 'gte' | 'lt' | 'lte' | 'top_percent' | 'bottom_percent';
export type AggregationKey = 'min' | 'max' | 'avg' | 'stddev';
export type SortDirection = 'asc' | 'desc';
export type PurgeOperationStatus = 'running' | 'succeeded' | 'failed' | null;

export type PurgeCompletionSummary = {
  requested: number;
  completed: number;
  pending: number;
  inProgress: number;
  progressPct: number;
  succeeded: number;
  failed: number;
  skipped: number;
  totalDeleted: number;
};

export interface OperatorOption {
  value: OperatorKey;
  label: string;
}

export interface AggregationOption {
  value: AggregationKey;
  label: string;
}

export const layerOptions: MedallionLayer[] = ['bronze', 'silver', 'gold'];

export const domainOptions: Array<{ value: DomainKey; label: string }> = [
  { value: 'market', label: 'Market' },
  { value: 'finance', label: 'Finance' },
  { value: 'earnings', label: 'Earnings' },
  { value: 'price-target', label: 'Price Target' }
];

export const operatorOptions: OperatorOption[] = [
  { value: 'gt', label: 'Numeric >' },
  { value: 'gte', label: 'Numeric >=' },
  { value: 'lt', label: 'Numeric <' },
  { value: 'lte', label: 'Numeric <=' },
  { value: 'top_percent', label: 'Top N%' },
  { value: 'bottom_percent', label: 'Bottom N%' }
];

export const aggregationOptions: AggregationOption[] = [
  { value: 'avg', label: 'Average' },
  { value: 'min', label: 'Min' },
  { value: 'max', label: 'Max' },
  { value: 'stddev', label: 'Std Dev' }
];

export const formFieldClass = 'space-y-1.5';
export const formLabelClass =
  'text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground';
export const formInputClass = 'h-10 bg-input-background';
export const formSelectClass =
  'h-10 w-full rounded-xl border-2 border-mcm-walnut bg-input-background px-3 text-sm font-semibold text-foreground outline-none transition-[color,box-shadow] focus-visible:border-mcm-teal focus-visible:ring-mcm-teal/40 focus-visible:ring-[3px] disabled:cursor-not-allowed disabled:opacity-50';

export const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const formatNumber = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  if (Number.isInteger(value)) {
    return value.toLocaleString();
  }
  return value.toFixed(4);
};

export const formatDate = (value: string | null | undefined): string => {
  if (!value) {
    return '-';
  }
  return new Date(value).toLocaleString();
};

export function buildPurgeExpression(
  operator: OperatorKey,
  column: string,
  value: number,
  aggregation: AggregationKey,
  recentRows: number
): string {
  const display = Number.isInteger(value)
    ? `${value}`
    : `${value}`.replace(/0+$/, '').replace(/\.$/, '');
  const metric =
    recentRows === 1 && aggregation === 'avg'
      ? column
      : `${aggregation}(${column}) over last ${recentRows} rows`;

  switch (operator) {
    case 'gt':
      return `${metric} > ${display}`;
    case 'gte':
      return `${metric} >= ${display}`;
    case 'lt':
      return `${metric} < ${display}`;
    case 'lte':
      return `${metric} <= ${display}`;
    case 'top_percent':
      return `top ${display}% by ${metric}`;
    case 'bottom_percent':
      return `bottom ${display}% by ${metric}`;
    default:
      return `${metric} ${operator} ${display}`;
  }
}

export function extractBatchResult(operation: PurgeOperationResponse): {
  symbolResults: PurgeSymbolResultItem[];
  requestedSymbolCount: number;
  completed: number;
  pending: number;
  inProgress: number;
  progressPct: number;
  succeeded: number;
  failed: number;
  skipped: number;
  totalDeleted: number;
} | null {
  const result = operation.result as {
    scope?: string;
    symbolResults?: PurgeSymbolResultItem[];
    requestedSymbolCount?: number;
    completed?: number;
    pending?: number;
    inProgress?: number;
    progressPct?: number;
    succeeded?: number;
    failed?: number;
    skipped?: number;
    totalDeleted?: number;
  };

  if (!result || result.scope !== 'symbols') {
    return null;
  }

  const requestedSymbolCount = result.requestedSymbolCount || 0;
  const succeeded = result.succeeded || 0;
  const failed = result.failed || 0;
  const skipped = result.skipped || 0;
  const completed = result.completed ?? succeeded + failed + skipped;
  const pending =
    result.pending ?? Math.max(requestedSymbolCount - completed - (result.inProgress || 0), 0);
  const inProgress = result.inProgress || 0;
  const progressPct =
    result.progressPct ??
    (requestedSymbolCount > 0
      ? Number(((completed / requestedSymbolCount) * 100).toFixed(2))
      : 100);

  return {
    symbolResults: result.symbolResults || [],
    requestedSymbolCount,
    completed,
    pending,
    inProgress,
    progressPct,
    succeeded,
    failed,
    skipped,
    totalDeleted: result.totalDeleted || 0
  };
}

export function extractCandidatePreviewResult(
  operation: PurgeOperationResponse
): PurgeCandidatesResponse | null {
  const result = operation.result as Partial<PurgeCandidatesResponse> | undefined;
  if (!result || typeof result !== 'object') {
    return null;
  }
  if (!result.criteria || !result.summary || !Array.isArray(result.symbols)) {
    return null;
  }
  if (typeof result.expression !== 'string') {
    return null;
  }
  return result as PurgeCandidatesResponse;
}

export function statusClass(symbolRow: PurgeSymbolResultItem): string {
  if (symbolRow.status === 'succeeded') {
    return 'text-emerald-600';
  }
  if (symbolRow.status === 'failed') {
    return 'text-destructive';
  }
  return 'text-muted-foreground';
}
