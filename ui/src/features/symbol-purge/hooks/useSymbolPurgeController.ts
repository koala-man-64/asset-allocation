import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';

import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import type {
  PurgeCandidateRow,
  PurgeCandidatesRequest,
  PurgeCandidatesResponse,
  PurgeOperationResponse,
  PurgeSymbolResultItem
} from '@/services/apiService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

import {
  type AggregationKey,
  type DomainKey,
  type MedallionLayer,
  type OperatorKey,
  type PurgeCompletionSummary,
  type PurgeOperationStatus,
  type SortDirection,
  buildPurgeExpression,
  extractBatchResult,
  extractCandidatePreviewResult,
  sleep
} from '../lib/symbolPurge';

export function useSymbolPurgeController() {
  const queryClient = useQueryClient();

  const [layer, setLayer] = useState<MedallionLayer>('silver');
  const [domain, setDomain] = useState<DomainKey>('market');
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [column, setColumn] = useState<string>('volume');
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [columnsError, setColumnsError] = useState<string | null>(null);
  const [columnsRequireRetrieve, setColumnsRequireRetrieve] = useState(false);

  const [operator, setOperator] = useState<OperatorKey>('gt');
  const [aggregation, setAggregation] = useState<AggregationKey>('avg');
  const [value, setValue] = useState<string>('90');
  const [recentRows, setRecentRows] = useState<number>(1);

  const [candidateResponse, setCandidateResponse] = useState<PurgeCandidatesResponse | null>(null);
  const [candidateRows, setCandidateRows] = useState<PurgeCandidateRow[]>([]);
  const [candidateLoading, setCandidateLoading] = useState(false);
  const [candidateError, setCandidateError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [selectedSymbols, setSelectedSymbols] = useState<Set<string>>(new Set());
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');

  const [confirmChecked, setConfirmChecked] = useState(false);
  const [confirmText, setConfirmText] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [operationId, setOperationId] = useState<string | null>(null);
  const [operationStatus, setOperationStatus] = useState<PurgeOperationStatus>(null);
  const [operationError, setOperationError] = useState<string | null>(null);
  const [symbolExecutionResults, setSymbolExecutionResults] = useState<PurgeSymbolResultItem[]>([]);
  const [completionSummary, setCompletionSummary] = useState<PurgeCompletionSummary | null>(null);

  const isPercentMode = operator === 'top_percent' || operator === 'bottom_percent';
  const showBronzeWarning = layer === 'bronze';
  const parsedValue = useMemo(() => Number(value), [value]);
  const isValueValid = Number.isFinite(parsedValue);
  const isPercentValid = isPercentMode ? parsedValue >= 1 && parsedValue <= 100 : true;
  const hasColumnSelection = Boolean(column);

  const previewExpression = useMemo(
    () =>
      isValueValid
        ? buildPurgeExpression(operator, column || 'column', parsedValue, aggregation, recentRows)
        : '',
    [aggregation, column, isValueValid, operator, parsedValue, recentRows]
  );

  const loadColumns = useCallback(async () => {
    setColumnsLoading(true);
    setColumnsError(null);
    setColumnsRequireRetrieve(false);

    try {
      const payload = await DataService.getDomainColumns(layer, domain);
      const keys = payload.columns || [];

      if (!keys.length) {
        setAvailableColumns([]);
        setColumn('');
        setColumnsRequireRetrieve(true);
        setColumnsError(
          `No cached columns found for ${layer}/${domain}. Click "Retrieve Columns" to fetch and cache them.`
        );
        return;
      }

      setColumnsRequireRetrieve(false);
      setAvailableColumns(keys);
      setColumn((previous) => (!previous || !keys.includes(previous) ? (keys[0] ?? '') : previous));
    } catch (error: unknown) {
      setAvailableColumns([]);
      setColumn('');
      setColumnsRequireRetrieve(false);
      setColumnsError(formatSystemStatusText(error) || 'Unable to load cached columns.');
    } finally {
      setColumnsLoading(false);
    }
  }, [domain, layer]);

  const refreshColumns = useCallback(async () => {
    setColumnsLoading(true);
    setColumnsError(null);

    try {
      const payload = await DataService.refreshDomainColumns({
        layer,
        domain,
        sample_limit: 500
      });
      const keys = payload.columns || [];

      if (!keys.length) {
        setAvailableColumns([]);
        setColumn('');
        setColumnsRequireRetrieve(true);
        setColumnsError(
          `No columns discovered for ${layer}/${domain}. Verify the dataset has rows, then retry.`
        );
        return;
      }

      setAvailableColumns(keys);
      setColumnsRequireRetrieve(false);
      setColumn((previous) => (!previous || !keys.includes(previous) ? (keys[0] ?? '') : previous));
      toast.success(
        `Retrieved ${keys.length} cached column${keys.length === 1 ? '' : 's'} for ${layer}/${domain}.`
      );
    } catch (error: unknown) {
      const message = formatSystemStatusText(error) || 'Unable to refresh columns.';
      setColumnsError(message);
      toast.error(message);
    } finally {
      setColumnsLoading(false);
    }
  }, [domain, layer]);

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
  }, [aggregation, column, domain, layer, operator, recentRows]);

  const sortedCandidates = useMemo(() => {
    const rows = [...candidateRows];
    rows.sort((left, right) => {
      const delta = left.matchedValue - right.matchedValue;
      if (delta === 0) {
        return left.symbol.localeCompare(right.symbol);
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
  const canSubmitBlacklist = confirmChecked && isConfirmPhraseValid && !isSubmitting;

  const applyOperationProgress = useCallback((operation: PurgeOperationResponse) => {
    setOperationStatus(operation.status);
    setOperationError(operation.status === 'failed' ? operation.error || null : null);

    const result = extractBatchResult(operation);
    if (!result) {
      return;
    }

    setSymbolExecutionResults(result.symbolResults);
    setCompletionSummary({
      requested: result.requestedSymbolCount,
      completed: result.completed,
      pending: result.pending,
      inProgress: result.inProgress,
      progressPct: result.progressPct,
      succeeded: result.succeeded,
      failed: result.failed,
      skipped: result.skipped,
      totalDeleted: result.totalDeleted
    });
  }, []);

  const pollOperation = useCallback(
    async (targetOperationId: string): Promise<PurgeOperationResponse> => {
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
          const delay = 700 + Math.min(attempt * 150, 900);
          attempt += 1;
          await sleep(delay);
          continue;
        }

        const operation = polledOperation as PurgeOperationResponse;
        applyOperationProgress(operation);
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
          throw new Error(operation.error || 'Purge failed.');
        }
        if (Date.now() - startedAt > timeoutMs) {
          throw new Error(
            `Purge is still running. Check system state for progress. operationId=${targetOperationId}`
          );
        }
        const delay = 700 + Math.min(attempt * 150, 900);
        attempt += 1;
        await sleep(delay);
      }
    },
    [applyOperationProgress]
  );

  const pollPreviewOperation = useCallback(async (targetOperationId: string) => {
    const startedAt = Date.now();
    const timeoutMs = 2 * 60_000;
    let attempt = 0;

    while (true) {
      let polledOperation: unknown;
      try {
        polledOperation = await DataService.getPurgeOperation(targetOperationId);
      } catch (error) {
        const message = formatSystemStatusText(error) || 'Unable to poll preview status.';
        if (Date.now() - startedAt > timeoutMs) {
          throw new Error(message || 'Preview did not complete before timeout.');
        }
        const delay = 700 + Math.min(attempt * 150, 900);
        attempt += 1;
        await sleep(delay);
        continue;
      }

      const operation = polledOperation as PurgeOperationResponse;
      if (operation.status === 'succeeded') {
        return operation;
      }
      if (operation.status === 'failed') {
        throw new Error(operation.error || 'Candidate preview failed.');
      }
      if (Date.now() - startedAt > timeoutMs) {
        throw new Error(`Candidate preview is still running. operationId=${targetOperationId}`);
      }
      const delay = 700 + Math.min(attempt * 150, 900);
      attempt += 1;
      await sleep(delay);
    }
  }, []);

  const runPreview = useCallback(async () => {
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
      const payload: PurgeCandidatesRequest = {
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

      const operation = await DataService.createPurgeCandidatesOperation(payload);
      const finishedOperation =
        operation.status === 'succeeded'
          ? operation
          : await pollPreviewOperation(operation.operationId);
      const response = extractCandidatePreviewResult(finishedOperation);
      if (!response) {
        throw new Error('Candidate preview completed without a valid result payload.');
      }
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
  }, [
    aggregation,
    canPreview,
    column,
    domain,
    isPercentMode,
    layer,
    operator,
    parsedValue,
    pollPreviewOperation,
    recentRows
  ]);

  const selectAll = useCallback(() => {
    setSelectedSymbols(new Set(candidateRows.map((row) => row.symbol)));
  }, [candidateRows]);

  const clearAll = useCallback(() => {
    setSelectedSymbols(new Set());
  }, []);

  const invertSelection = useCallback(() => {
    const next = new Set<string>();
    for (const row of candidateRows) {
      if (!selectedSymbols.has(row.symbol)) {
        next.add(row.symbol);
      }
    }
    setSelectedSymbols(next);
  }, [candidateRows, selectedSymbols]);

  const toggleCandidateSelection = useCallback((symbol: string, checked: boolean) => {
    setSelectedSymbols((previous) => {
      const next = new Set(previous);
      if (checked) {
        next.add(symbol);
      } else {
        next.delete(symbol);
      }
      return next;
    });
  }, []);

  const toggleSortDirection = useCallback(() => {
    setSortDirection((previous) => (previous === 'asc' ? 'desc' : 'asc'));
  }, []);

  const copySelected = useCallback(async () => {
    if (!selectedCount) {
      toast.warning('Select at least one symbol to copy.');
      return;
    }

    const selected = Array.from(selectedSymbols).sort();
    try {
      await navigator.clipboard.writeText(selected.join(', '));
      toast.success(
        `${selected.length} symbol${selected.length === 1 ? '' : 's'} copied to clipboard.`
      );
    } catch {
      toast.error('Clipboard access is unavailable in this browser context.');
    }
  }, [selectedCount, selectedSymbols]);

  const runPurge = useCallback(async () => {
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
      applyOperationProgress(response);
      const finished =
        response.status === 'succeeded' ? response : await pollOperation(response.operationId);
      const result = extractBatchResult(finished);
      if (!result) {
        throw new Error('Purge completed without batch result payload.');
      }
      if (result.failed > 0 || finished.status === 'failed') {
        toast.error(`Purge completed with ${result.failed} failed symbol(s).`);
      } else {
        toast.success(`Purge completed. Total deleted blobs: ${result.totalDeleted}.`);
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
  }, [
    aggregation,
    applyOperationProgress,
    candidateRows.length,
    canSubmit,
    column,
    domain,
    layer,
    operator,
    parsedValue,
    pollOperation,
    previewExpression,
    queryClient,
    recentRows,
    selectedCount,
    selectedSymbols
  ]);

  const resetPurgeList = useCallback(() => {
    setSelectedSymbols(new Set());
    setOperationId(null);
    setOperationStatus(null);
    setOperationError(null);
    setSymbolExecutionResults([]);
    setCompletionSummary(null);
    setConfirmChecked(false);
    setConfirmText('');
    toast.success('Purge list reset.');
  }, []);

  const runBlacklistPurge = useCallback(async () => {
    if (!canSubmitBlacklist) {
      setOperationError('Complete all confirmation steps before running.');
      return;
    }

    setIsSubmitting(true);
    setOperationStatus('running');
    setOperationError(null);
    setCompletionSummary(null);
    setSymbolExecutionResults([]);

    try {
      const blacklist = await DataService.getPurgeBlacklistSymbols();
      const symbols = (blacklist.symbols || [])
        .map((item) => String(item || '').trim())
        .filter((item) => item.length > 0);
      const uniqueSymbols = Array.from(new Set(symbols));
      if (!uniqueSymbols.length) {
        setOperationStatus(null);
        toast.warning('No symbols found in bronze blacklists. Nothing to purge.');
        return;
      }

      setSelectedSymbols(new Set(uniqueSymbols));

      const response = await DataService.purgeSymbolsBatch({
        symbols: uniqueSymbols,
        confirm: true,
        scope_note: `bronze blacklist union / selected ${uniqueSymbols.length} / sources ${(blacklist.sources || []).length}`,
        dry_run: false
      });

      setOperationId(response.operationId);
      applyOperationProgress(response);
      const finished =
        response.status === 'succeeded' ? response : await pollOperation(response.operationId);
      const result = extractBatchResult(finished);
      if (!result) {
        throw new Error('Purge completed without batch result payload.');
      }
      if (result.failed > 0 || finished.status === 'failed') {
        toast.error(`Blacklist purge completed with ${result.failed} failed symbol(s).`);
      } else {
        toast.success(`Blacklist purge completed. Total deleted blobs: ${result.totalDeleted}.`);
      }
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (error: unknown) {
      const message = formatSystemStatusText(error) || 'Blacklist symbol purge failed.';
      setOperationStatus('failed');
      setOperationError(message);
      toast.error(`Purge failed: ${message}`);
    } finally {
      setIsSubmitting(false);
    }
  }, [applyOperationProgress, canSubmitBlacklist, pollOperation, queryClient]);

  const hasPurgeListState =
    selectedCount > 0 ||
    Boolean(operationId) ||
    Boolean(operationStatus) ||
    Boolean(operationError) ||
    symbolExecutionResults.length > 0 ||
    Boolean(completionSummary) ||
    confirmChecked ||
    confirmText.trim().length > 0;

  return {
    criteria: {
      layer,
      domain,
      column,
      operator,
      aggregation,
      value,
      recentRows
    },
    columns: {
      availableColumns,
      columnsLoading,
      columnsError,
      columnsRequireRetrieve
    },
    candidate: {
      response: candidateResponse,
      rows: candidateRows,
      sortedRows: sortedCandidates,
      loading: candidateLoading,
      error: candidateError,
      validationError,
      selectedSymbols,
      selectedCount,
      sortDirection
    },
    execution: {
      confirmChecked,
      confirmText,
      isSubmitting,
      operationId,
      operationStatus,
      operationError,
      symbolExecutionResults,
      completionSummary,
      hasPurgeListState
    },
    derived: {
      isPercentMode,
      showBronzeWarning,
      isValueValid,
      isPercentValid,
      canPreview,
      previewExpression,
      canSubmit,
      canSubmitBlacklist
    },
    actions: {
      setLayer,
      setDomain,
      setColumn,
      setOperator,
      setAggregation,
      setValue,
      setRecentRows,
      setConfirmChecked,
      setConfirmText,
      loadColumns,
      refreshColumns,
      runPreview,
      selectAll,
      clearAll,
      invertSelection,
      toggleCandidateSelection,
      toggleSortDirection,
      copySelected,
      runPurge,
      resetPurgeList,
      runBlacklistPurge
    }
  };
}

export type SymbolPurgeController = ReturnType<typeof useSymbolPurgeController>;
