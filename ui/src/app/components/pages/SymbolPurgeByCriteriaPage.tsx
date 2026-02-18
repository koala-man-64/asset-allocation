import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import {
  AlertTriangle,
  ArrowUpDown,
  ClipboardCopy,
  Loader2,
  Search,
  Trash2
} from 'lucide-react';

import { Button } from '@/app/components/ui/button';
import { Checkbox } from '@/app/components/ui/checkbox';
import { Input } from '@/app/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/app/components/ui/table';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { DataService } from '@/services/DataService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { queryKeys } from '@/hooks/useDataQueries';

import type {
  PurgeCandidateRow,
  PurgeCandidatesResponse,
  PurgeOperationResponse,
  PurgeSymbolResultItem
} from '@/services/apiService';

type MedallionLayer = 'bronze' | 'silver' | 'gold';
type DomainKey = 'market' | 'finance' | 'earnings' | 'price-target';
type OperatorKey = 'gt' | 'gte' | 'lt' | 'lte' | 'top_percent' | 'bottom_percent';
type AggregationKey = 'min' | 'max' | 'avg' | 'stddev';
type SortDirection = 'asc' | 'desc';

interface OperatorOption {
  value: OperatorKey;
  label: string;
}

interface AggregationOption {
  value: AggregationKey;
  label: string;
}

const layerOptions: MedallionLayer[] = ['bronze', 'silver', 'gold'];
const domainOptions: Array<{ value: DomainKey; label: string }> = [
  { value: 'market', label: 'Market' },
  { value: 'finance', label: 'Finance' },
  { value: 'earnings', label: 'Earnings' },
  { value: 'price-target', label: 'Price Target' }
];

const operatorOptions: OperatorOption[] = [
  { value: 'gt', label: 'Numeric >' },
  { value: 'gte', label: 'Numeric >=' },
  { value: 'lt', label: 'Numeric <' },
  { value: 'lte', label: 'Numeric <=' },
  { value: 'top_percent', label: 'Top N%' },
  { value: 'bottom_percent', label: 'Bottom N%' }
];

const aggregationOptions: AggregationOption[] = [
  { value: 'avg', label: 'Average' },
  { value: 'min', label: 'Min' },
  { value: 'max', label: 'Max' },
  { value: 'stddev', label: 'Std Dev' }
];

const formFieldClass = 'space-y-1.5';
const formLabelClass = 'text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground';
const formInputClass = 'h-10 bg-input-background';
const formSelectClass =
  'h-10 w-full rounded-xl border-2 border-mcm-walnut bg-input-background px-3 text-sm font-semibold text-foreground outline-none transition-[color,box-shadow] focus-visible:border-mcm-teal focus-visible:ring-mcm-teal/40 focus-visible:ring-[3px] disabled:cursor-not-allowed disabled:opacity-50';

const formatNumber = (value: number | null | undefined): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '—';
  }
  if (Number.isInteger(value)) return value.toLocaleString();
  return value.toFixed(4);
};

const formatDate = (value: string | null | undefined): string => {
  if (!value) return '—';
  return new Date(value).toLocaleString();
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function buildPurgeExpression(
  operator: OperatorKey,
  column: string,
  value: number,
  aggregation: AggregationKey,
  recentRows: number
): string {
  const display = Number.isInteger(value) ? `${value}` : `${value}`.replace(/0+$/, '').replace(/\.$/, '');
  const metric =
    recentRows === 1 && aggregation === 'avg' ? column : `${aggregation}(${column}) over last ${recentRows} rows`;
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

function extractBatchResult(
  operation: PurgeOperationResponse
): {
  symbolResults: PurgeSymbolResultItem[];
  requestedSymbolCount: number;
  succeeded: number;
  failed: number;
  skipped: number;
  totalDeleted: number;
} | null {
  const result = operation.result as {
    scope?: string;
    symbolResults?: PurgeSymbolResultItem[];
    requestedSymbolCount?: number;
    succeeded?: number;
    failed?: number;
    skipped?: number;
    totalDeleted?: number;
  };
  if (!result || result.scope !== 'symbols') return null;
  return {
    symbolResults: result.symbolResults || [],
    requestedSymbolCount: result.requestedSymbolCount || 0,
    succeeded: result.succeeded || 0,
    failed: result.failed || 0,
    skipped: result.skipped || 0,
    totalDeleted: result.totalDeleted || 0
  };
}

export function SymbolPurgeByCriteriaPage() {
  const queryClient = useQueryClient();

  const [layer, setLayer] = useState<MedallionLayer>('silver');
  const [domain, setDomain] = useState<DomainKey>('market');
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [column, setColumn] = useState<string>('volume');
  const [columnsLoading, setColumnsLoading] = useState<boolean>(false);
  const [columnsError, setColumnsError] = useState<string | null>(null);

  const [operator, setOperator] = useState<OperatorKey>('gt');
  const [aggregation, setAggregation] = useState<AggregationKey>('avg');
  const [value, setValue] = useState<string>('90');
  const [recentRows, setRecentRows] = useState<number>(1);

  const [candidateResponse, setCandidateResponse] = useState<PurgeCandidatesResponse | null>(null);
  const [candidateRows, setCandidateRows] = useState<PurgeCandidateRow[]>([]);
  const [candidateLoading, setCandidateLoading] = useState<boolean>(false);
  const [candidateError, setCandidateError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [selectedSymbols, setSelectedSymbols] = useState<Set<string>>(new Set());
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  const [confirmChecked, setConfirmChecked] = useState<boolean>(false);
  const [confirmText, setConfirmText] = useState<string>('');
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [operationId, setOperationId] = useState<string | null>(null);
  const [operationStatus, setOperationStatus] = useState<'running' | 'succeeded' | 'failed' | null>(null);
  const [operationError, setOperationError] = useState<string | null>(null);
  const [symbolExecutionResults, setSymbolExecutionResults] = useState<PurgeSymbolResultItem[]>([]);
  const [completionSummary, setCompletionSummary] = useState<{
    requested: number;
    succeeded: number;
    failed: number;
    skipped: number;
    totalDeleted: number;
  } | null>(null);

  const isPercentMode = operator === 'top_percent' || operator === 'bottom_percent';
  const showBronzeWarning = layer === 'bronze';

  const parsedValue = useMemo(() => Number(value), [value]);
  const isValueValid = Number.isFinite(parsedValue);
  const isPercentValid = isPercentMode ? parsedValue >= 1 && parsedValue <= 100 : true;
  const hasColumnSelection = Boolean(column);

  const previewExpression = useMemo(
    () =>
      isValueValid ? buildPurgeExpression(operator, column || 'column', parsedValue, aggregation, recentRows) : '',
    [operator, column, isValueValid, parsedValue, aggregation, recentRows]
  );

  const loadColumns = useCallback(async () => {
    setColumnsLoading(true);
    setColumnsError(null);

    try {
      const data = await DataService.getGenericData(layer, domain, undefined, 500);
      const keys = data.length ? Object.keys(data[0] ?? {}) : [];

      if (!keys.length) {
        setAvailableColumns([]);
        setColumn('');
        setColumnsError(`No data available for ${layer}/${domain} preview sample.`);
        return;
      }

      setAvailableColumns(keys);
      if (!column || !keys.includes(column)) {
        setColumn(keys[0] ?? '');
      }
    } catch (error: unknown) {
      setAvailableColumns([]);
      setColumn('');
      setColumnsError(formatSystemStatusText(error) || 'Unable to load sample columns.');
    } finally {
      setColumnsLoading(false);
    }
  }, [column, domain, layer]);

  useEffect(() => {
    void loadColumns();
  }, [loadColumns]);

  useEffect(() => {
    setCandidateResponse(null);
    setCandidateRows([]);
    setCandidateError(null);
    setValidationError(null);
    setSelectedSymbols(new Set());
    setOperationId(null);
    setOperationStatus(null);
    setOperationError(null);
    setSymbolExecutionResults([]);
    setCompletionSummary(null);
    setConfirmChecked(false);
    setConfirmText('');
  }, [layer, domain, operator, aggregation, column, recentRows]);

  const sortedCandidates = useMemo(() => {
    const rows = [...candidateRows];
    rows.sort((a, b) => {
      const delta = a.matchedValue - b.matchedValue;
      if (delta === 0) {
        return a.symbol.localeCompare(b.symbol);
      }
      return sortDirection === 'asc' ? delta : -delta;
    });
    return rows;
  }, [candidateRows, sortDirection]);

  const selectedCount = selectedSymbols.size;
  const canPreview = Boolean(hasColumnSelection && isValueValid && isPercentValid);
  const isConfirmPhraseValid = confirmText.trim().toUpperCase() === 'PURGE';
  const canSubmit =
    candidateRows.length > 0 &&
    selectedCount > 0 &&
    confirmChecked &&
    isConfirmPhraseValid &&
    !isSubmitting;

  const pollOperation = async (targetOperationId: string): Promise<PurgeOperationResponse> => {
    const startedAt = Date.now();
    const timeoutMs = 5 * 60_000;
    let attempt = 0;

    while (true) {
      let polledOperation: unknown;
      try {
        polledOperation = await DataService.getPurgeOperation(targetOperationId);
      } catch (error) {
        const message = formatSystemStatusText(error) || 'Unable to poll purge status.';
        if (Date.now() - startedAt > timeoutMs) {
          throw new Error(message || 'Purge did not complete before timeout.');
        }

        const delay = 1000 + Math.min(attempt * 200, 1600);
        attempt += 1;
        await sleep(delay);
        continue;
      }

      const operation = polledOperation as PurgeOperationResponse;
      if (operation.status === 'succeeded') {
        if (!operation.result) {
          throw new Error('Purge completed without a result payload.');
        }
        return operation;
      }

      if (operation.status === 'failed') {
        if (operation.result) {
          return operation;
        }
        const message = operation.error || 'Purge failed.';
        throw new Error(message);
      }

      if (Date.now() - startedAt > timeoutMs) {
        throw new Error(
          `Purge is still running. Check system state for progress. operationId=${targetOperationId}`
        );
      }

      const delay = 1000 + Math.min(attempt * 200, 1600);
      attempt += 1;
      await sleep(delay);
    }
  };

  const runPreview = async () => {
    if (!canPreview) {
      setValidationError('Please fix the rule validation errors before previewing.');
      return;
    }

    if (!column) {
      setValidationError('A column must be selected.');
      return;
    }

    setCandidateLoading(true);
    setValidationError(null);
    setCandidateError(null);
    setSymbolExecutionResults([]);
    setCompletionSummary(null);
    setOperationStatus(null);

    try {
      const payload = {
        layer,
        domain,
        column,
        operator,
        aggregation,
        value: isPercentMode ? undefined : parsedValue,
        percentile: isPercentMode ? parsedValue : undefined,
        recent_rows: recentRows,
        offset: 0
      };

      const response = await DataService.getPurgeCandidates(payload);
      setCandidateResponse(response);
      setCandidateRows(response.symbols || []);
      setSelectedSymbols(new Set((response.symbols || []).map((row) => row.symbol)));
      setOperationId(null);
      setOperationError(null);
      toast.success(`Preview returned ${response.summary.symbolsMatched} symbols.`);
    } catch (error: unknown) {
      setCandidateRows([]);
      setCandidateResponse(null);
      const message = formatSystemStatusText(error) || 'Candidate preview failed.';
      setCandidateError(message);
      toast.error(message);
    } finally {
      setCandidateLoading(false);
    }
  };

  const handleSelectAll = () => {
    setSelectedSymbols(new Set(candidateRows.map((row) => row.symbol)));
  };

  const handleClearAll = () => {
    setSelectedSymbols(new Set());
  };

  const handleInvert = () => {
    const next = new Set<string>();
    for (const row of candidateRows) {
      if (!selectedSymbols.has(row.symbol)) {
        next.add(row.symbol);
      }
    }
    setSelectedSymbols(next);
  };

  const handleCopySelected = async () => {
    if (!selectedCount) {
      toast.warning('Select at least one symbol to copy.');
      return;
    }

    const selected = Array.from(selectedSymbols).sort();
    try {
      await navigator.clipboard.writeText(selected.join(', '));
      toast.success(`${selected.length} symbol${selected.length === 1 ? '' : 's'} copied to clipboard.`);
    } catch {
      toast.error('Clipboard access is unavailable in this browser context.');
    }
  };

  const handleRunPurge = async () => {
    if (!canSubmit) {
      setOperationError('Complete all confirmation steps before running.');
      return;
    }

    setIsSubmitting(true);
    setOperationStatus('running');
    setOperationError(null);
    setCompletionSummary(null);
    setSymbolExecutionResults([]);

    try {
      const response = await DataService.purgeSymbolsBatch({
        symbols: Array.from(selectedSymbols),
        confirm: true,
        scope_note: `${previewExpression} / ${candidateRows.length} matched / selected ${selectedCount}`,
        dry_run: false,
        audit_rule: {
          layer,
          domain,
          column_name: column,
          operator,
          threshold: parsedValue,
          aggregation,
          recent_rows: recentRows,
          expression: previewExpression,
          selected_symbol_count: selectedCount,
          matched_symbol_count: candidateRows.length
        }
      });

      setOperationId(response.operationId);
      const finished = response.status === 'succeeded' ? response : await pollOperation(response.operationId);
      setOperationStatus(finished.status);
      setOperationError(finished.status === 'failed' ? (finished.error || null) : null);

      const result = extractBatchResult(finished);
      if (!result) {
        throw new Error('Purge completed without batch result payload.');
      }

      setSymbolExecutionResults(result.symbolResults);
      setCompletionSummary({
        requested: result.requestedSymbolCount,
        succeeded: result.succeeded,
        failed: result.failed,
        skipped: result.skipped,
        totalDeleted: result.totalDeleted
      });

      if (result.failed > 0 || finished.status === 'failed') {
        toast.error(`Purge completed with ${result.failed} failed symbol(s).`);
      } else {
        const successMessage = `Purge completed. Total deleted blobs: ${result.totalDeleted}.`;
        toast.success(successMessage);
      }
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (error: unknown) {
      const message = formatSystemStatusText(error) || 'Symbol purge failed.';
      setOperationStatus('failed');
      setOperationError(message);
      toast.error(`Purge failed: ${message}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const statusClass = (symbolRow: PurgeSymbolResultItem): string => {
    if (symbolRow.status === 'succeeded') return 'text-emerald-600';
    if (symbolRow.status === 'failed') return 'text-destructive';
    return 'text-muted-foreground';
  };

  return (
    <div className="grid gap-4 lg:grid-cols-[390px_1fr]">
      <section className="mcm-panel p-4 sm:p-5">
        <div className="mb-3 flex items-start justify-between gap-3">
          <div>
            <p className="page-kicker">Live Operations</p>
            <h1 className="page-title leading-[1.05]">Symbol Purge Console</h1>
            <p className="page-subtitle mt-1 max-w-[30ch] leading-relaxed">
              Build a rule, review candidate symbols, then execute a destructive bulk purge.
            </p>
          </div>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-9 px-4"
                onClick={() => void runPreview()}
              >
                Preview
              </Button>
            </TooltipTrigger>
            <TooltipContent>Run candidate preview</TooltipContent>
          </Tooltip>
        </div>

        <div className="space-y-3">
          <div className={formFieldClass}>
            <label className={formLabelClass}>Medallion layer</label>
            <select
              value={layer}
              className={formSelectClass}
              onChange={(event) => setLayer(event.target.value as MedallionLayer)}
            >
              {layerOptions.map((layerKey) => (
                <option key={layerKey} value={layerKey}>
                  {layerKey.toUpperCase()}
                </option>
              ))}
            </select>
            {showBronzeWarning ? (
              <p className="text-[11px] leading-relaxed text-amber-600">
                Bronze-wide criteria are approximated from the silver preview layer. Silver/gold is recommended.
              </p>
            ) : null}
          </div>

          <div className={formFieldClass}>
            <label className={formLabelClass}>Domain</label>
            <select
              value={domain}
              className={formSelectClass}
              onChange={(event) => setDomain(event.target.value as DomainKey)}
            >
              {domainOptions.map((entry) => (
                <option key={entry.value} value={entry.value}>
                  {entry.label}
                </option>
              ))}
            </select>
          </div>

          <div className={formFieldClass}>
            <label className={formLabelClass}>Column</label>
            <select
              value={column}
              className={formSelectClass}
              disabled={columnsLoading}
              onChange={(event) => setColumn(event.target.value)}
            >
              <option value="" disabled>
                {columnsLoading ? 'Loading columns…' : 'Select a column'}
              </option>
              {availableColumns.map((col) => (
                <option key={col} value={col}>
                  {col}
                </option>
              ))}
            </select>
            {columnsError ? <p className="text-[11px] text-destructive">{columnsError}</p> : null}
          </div>

          <div className="grid grid-cols-2 gap-2.5">
            <div className={formFieldClass}>
              <label className={formLabelClass}>Rule type</label>
              <select
                value={operator}
                className={formSelectClass}
                onChange={(event) => setOperator(event.target.value as OperatorKey)}
              >
                {operatorOptions.map((entry) => (
                  <option key={entry.value} value={entry.value}>
                    {entry.label}
                  </option>
                ))}
              </select>
            </div>
            <div className={formFieldClass}>
              <label className={formLabelClass}>
                {isPercentMode ? 'Percent (1-100)' : 'Numeric value'}
              </label>
              <Input
                type="text"
                value={value}
                onChange={(event) => setValue(event.target.value)}
                className={formInputClass}
                placeholder={isPercentMode ? 'e.g. 90' : 'e.g. 100'}
              />
              {!isValueValid || !isPercentValid ? (
                <p className="text-[11px] text-destructive">
                  {isValueValid ? 'Percentile must be between 1 and 100.' : 'Numeric value must be finite.'}
                </p>
              ) : null}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-2.5">
            <div className={formFieldClass}>
              <label className={formLabelClass}>Aggregation</label>
              <select
                value={aggregation}
                className={formSelectClass}
                onChange={(event) => setAggregation(event.target.value as AggregationKey)}
              >
                {aggregationOptions.map((entry) => (
                  <option key={entry.value} value={entry.value}>
                    {entry.label}
                  </option>
                ))}
              </select>
            </div>

            <div className={formFieldClass}>
              <label className={formLabelClass}>Recent Row Count</label>
              <Input
                type="number"
                min={1}
                value={recentRows}
                onChange={(event) => setRecentRows(Number(event.target.value) || 1)}
                className={formInputClass}
              />
            </div>
          </div>

          <Button
            onClick={() => void runPreview()}
            disabled={!canPreview || candidateLoading}
            className="h-10 w-full gap-2"
          >
            {candidateLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            {candidateLoading ? 'Previewing…' : 'Preview symbols'}
          </Button>

          {validationError ? <p className="text-[11px] text-destructive">{validationError}</p> : null}

          <div className="rounded-xl border border-border/70 bg-muted/30 p-2.5 text-xs text-muted-foreground">
            <p className="font-semibold text-foreground">Rule summary</p>
            <p className="font-mono break-words mt-1">{previewExpression || 'No valid rule yet.'}</p>
          </div>
        </div>
      </section>

      <section className="space-y-4">
        <div className="mcm-panel h-[620px] p-4 sm:p-5 flex flex-col">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <h2 className="text-lg font-black uppercase">Candidate review</h2>
              <p className="text-xs text-muted-foreground">
                {candidateResponse
                  ? `Rows scanned: ${formatNumber(candidateResponse.summary.totalRowsScanned)} · Matches: ${formatNumber(candidateResponse.summary.symbolsMatched)}`
                  : 'Run preview to load candidates.'}
              </p>
            </div>
            <div className="flex flex-wrap justify-end gap-2">
              <Button variant="outline" size="sm" onClick={() => void handleCopySelected()} disabled={selectedCount === 0}>
                <ClipboardCopy className="h-4 w-4" />
                Copy selected
              </Button>
              <Button variant="outline" size="sm" onClick={handleSelectAll} disabled={!candidateRows.length}>
                Select all
              </Button>
              <Button variant="outline" size="sm" onClick={handleClearAll} disabled={!candidateRows.length}>
                Clear all
              </Button>
              <Button variant="outline" size="sm" onClick={handleInvert} disabled={!candidateRows.length}>
                <ArrowUpDown className="h-4 w-4" />
                Invert
              </Button>
            </div>
          </div>

          <p className="mt-2 text-xs text-muted-foreground">
            {candidateError ? `Failed: ${candidateError}` : ''}
          </p>

          <div className="mt-3 flex-1 min-h-0 overflow-y-auto rounded-md border border-border/80">
            <Table>
              <TableHeader className="sticky top-0 z-10">
                <TableRow>
                  <TableHead className="w-12">Select</TableHead>
                  <TableHead>Symbol</TableHead>
                  <TableHead className="w-[140px] cursor-pointer" onClick={() => setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc')}>
                    Matched value
                  </TableHead>
                  <TableHead className="w-[180px]">Rows contributing</TableHead>
                  <TableHead>Latest as-of</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {sortedCandidates.length === 0 && (
                  <TableRow>
                    <td colSpan={5} className="py-8 text-center text-sm text-muted-foreground">
                      {candidateLoading ? 'Loading candidates…' : 'No candidates yet. Select rule criteria and preview.'}
                    </td>
                  </TableRow>
                )}
                {sortedCandidates.map((row) => {
                  const checked = selectedSymbols.has(row.symbol);
                  return (
                    <TableRow key={row.symbol}>
                      <TableCell>
                        <Checkbox
                          checked={checked}
                          onCheckedChange={(checkedValue) => {
                            const next = new Set(selectedSymbols);
                            if (checkedValue) {
                              next.add(row.symbol);
                            } else {
                              next.delete(row.symbol);
                            }
                            setSelectedSymbols(next);
                          }}
                        />
                      </TableCell>
                      <TableCell className="font-mono">{row.symbol}</TableCell>
                      <TableCell>{formatNumber(row.matchedValue)}</TableCell>
                      <TableCell>{formatNumber(row.rowsContributing)}</TableCell>
                      <TableCell>{formatDate(row.latestAsOf)}</TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </div>

        <div className="mcm-panel p-4 sm:p-5">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h2 className="text-lg font-black uppercase">Execution panel</h2>
            {candidateResponse ? (
              <p className="text-xs text-muted-foreground">
                Filter: <span className="font-mono">{candidateResponse.expression}</span>
              </p>
            ) : null}
          </div>
          <div className="mt-3 grid gap-3 sm:grid-cols-2">
            <div className="rounded-md border border-border/70 bg-muted/30 p-3">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Selected symbols</p>
              <p className="text-2xl font-black font-mono">{selectedCount}</p>
            </div>
            <div className="rounded-md border border-border/70 bg-muted/30 p-3">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Estimated purge target</p>
              <p className="text-2xl font-black font-mono">
                {candidateResponse ? formatNumber(candidateResponse.summary.estimatedDeletionTargets) : '—'}
              </p>
            </div>
          </div>

          {candidateResponse?.note ? (
            <p className="mt-3 rounded-md border border-amber-300/40 bg-amber-50 px-3 py-2 text-xs text-amber-700">
              {candidateResponse.note}
            </p>
          ) : null}

          <div className="mt-4 flex items-center gap-3 overflow-x-auto pb-1">
            <label className="inline-flex shrink-0 items-center gap-3 text-xs font-semibold uppercase tracking-wide">
              <Checkbox checked={confirmChecked} onCheckedChange={(next) => setConfirmChecked(Boolean(next))} />
              I understand this is destructive and cannot be undone.
            </label>

            <label className="inline-flex items-center gap-2 shrink-0">
              <span className="text-xs font-semibold uppercase tracking-wide whitespace-nowrap">Type PURGE to confirm</span>
              <Input
                value={confirmText}
                onChange={(event) => setConfirmText(event.target.value)}
                placeholder="PURGE"
                className={`${formInputClass} h-9 w-[180px]`}
              />
            </label>
            <Button
              onClick={() => void handleRunPurge()}
              className="h-9 shrink-0 gap-2"
              disabled={!canSubmit}
              variant="destructive"
            >
              {isSubmitting || operationStatus === 'running' ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Trash2 className="h-4 w-4" />
              )}
              {isSubmitting || operationStatus === 'running' ? 'Running purge…' : 'Run purge for selected symbols'}
            </Button>
          </div>

          {operationId ? (
            <p className="mt-2 text-xs text-muted-foreground">Operation: {operationId}</p>
          ) : null}

          {operationStatus && (
            <p
              className={`mt-3 text-xs ${operationStatus === 'failed' ? 'text-destructive' : 'text-muted-foreground'}`}
            >
              {operationStatus === 'running'
                ? 'Purge is running. Polling status updates from /system/purge/{operationId}.'
                : operationStatus === 'succeeded'
                  ? `Purge completed successfully.${completionSummary ? ` Deleted ${formatNumber(completionSummary.totalDeleted)}` : ''}`
                  : operationError || 'Purge failed.'}
            </p>
          )}

          {completionSummary ? (
            <div className="mt-3 grid gap-3 sm:grid-cols-4">
              <div className="rounded-md border border-border/70 bg-muted/30 p-2 text-sm">
                <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Requested</div>
                <div className="font-black font-mono">{completionSummary.requested}</div>
              </div>
              <div className="rounded-md border border-border/70 bg-muted/30 p-2 text-sm">
                <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Succeeded</div>
                <div className="font-black font-mono text-emerald-600">{completionSummary.succeeded}</div>
              </div>
              <div className="rounded-md border border-border/70 bg-muted/30 p-2 text-sm">
                <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Failed</div>
                <div className="font-black font-mono text-destructive">{completionSummary.failed}</div>
              </div>
              <div className="rounded-md border border-border/70 bg-muted/30 p-2 text-sm">
                <div className="text-[10px] uppercase tracking-[0.18em] text-muted-foreground">Skipped</div>
                <div className="font-black font-mono">{completionSummary.skipped}</div>
              </div>
            </div>
          ) : null}
        </div>

        {symbolExecutionResults.length > 0 ? (
          <div className="mcm-panel p-4 sm:p-5">
            <h3 className="mb-2 flex items-center gap-2 font-semibold">
              <AlertTriangle className="h-4 w-4" />
              Symbol execution status
            </h3>
            <div className="overflow-x-auto rounded-md border border-border/80">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Symbol</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Deleted rows</TableHead>
                    <TableHead>Error</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {symbolExecutionResults.map((row) => (
                    <TableRow key={row.symbol}>
                      <TableCell className="font-mono">{row.symbol}</TableCell>
                      <TableCell className={`font-semibold ${statusClass(row)}`}>{row.status.toUpperCase()}</TableCell>
                      <TableCell>{formatNumber(row.deleted || 0)}</TableCell>
                      <TableCell>{row.error || '—'}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          </div>
        ) : null}
      </section>
    </div>
  );
}
