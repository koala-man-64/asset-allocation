import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';

import { SymbolPurgeByCriteriaPage } from '@/app/components/pages/SymbolPurgeByCriteriaPage';
import { DataService } from '@/services/DataService';
import type {
  PurgeCandidateRow,
  PurgeCandidatesResponse,
  PurgeOperationResponse
} from '@/services/apiService';
import { renderWithProviders } from '@/test/utils';

const { mockToastSuccess, mockToastError, mockToastWarning } = vi.hoisted(() => ({
  mockToastSuccess: vi.fn(),
  mockToastError: vi.fn(),
  mockToastWarning: vi.fn()
}));

vi.mock('sonner', () => ({
  toast: {
    success: mockToastSuccess,
    error: mockToastError,
    warning: mockToastWarning
  }
}));

vi.mock('@/services/DataService', () => ({
  DataService: {
    getGenericData: vi.fn(),
    getPurgeCandidates: vi.fn(),
    purgeSymbolsBatch: vi.fn(),
    getPurgeOperation: vi.fn()
  }
}));

const TIMESTAMP = '2026-02-18T00:00:00Z';
const BRONZE_NOTE =
  'Bronze preview uses silver dataset for ranking; bronze-wide criteria are supported for runtime purge targets only.';

function makeCandidateRows(): PurgeCandidateRow[] {
  return [
    { symbol: 'AAA', matchedValue: 0.99, rowsContributing: 1, latestAsOf: '2026-02-12T18:00:00Z' },
    { symbol: 'BBB', matchedValue: 0.98, rowsContributing: 1, latestAsOf: '2026-02-12T18:00:00Z' }
  ];
}

function makeCandidateResponse(
  overrides: Partial<PurgeCandidatesResponse> = {},
  symbols: PurgeCandidateRow[] = makeCandidateRows()
): PurgeCandidatesResponse {
  return {
    criteria: {
      requestedLayer: 'silver',
      resolvedLayer: 'silver',
      domain: 'market',
      column: 'Close',
      operator: 'lt',
      value: 1,
      asOf: null,
      minRows: 1,
      recentRows: 1,
      aggregation: 'avg'
    },
    expression: 'Close < 1',
    summary: {
      totalRowsScanned: 10008,
      symbolsMatched: symbols.length,
      rowsContributing: symbols.length,
      estimatedDeletionTargets: symbols.length
    },
    symbols,
    offset: 0,
    limit: 200,
    total: symbols.length,
    hasMore: false,
    note: null,
    ...overrides
  };
}

function makeBatchRunningOperation(operationId: string): PurgeOperationResponse {
  return {
    operationId,
    status: 'running',
    scope: 'symbols',
    createdAt: TIMESTAMP,
    updatedAt: TIMESTAMP,
    startedAt: TIMESTAMP,
    completedAt: null,
    result: undefined,
    error: null
  };
}

function makeBatchSucceededOperation(
  operationId: string,
  symbols: PurgeCandidateRow[] = makeCandidateRows()
): PurgeOperationResponse {
  return {
    operationId,
    status: 'succeeded',
    scope: 'symbols',
    createdAt: TIMESTAMP,
    updatedAt: TIMESTAMP,
    startedAt: TIMESTAMP,
    completedAt: TIMESTAMP,
    result: {
      scope: 'symbols',
      dryRun: false,
      scopeNote: 'Close < 1 / 2 matched / selected 2',
      requestedSymbols: symbols.map((row) => row.symbol),
      requestedSymbolCount: symbols.length,
      succeeded: symbols.length,
      failed: 0,
      skipped: 0,
      totalDeleted: 5,
      symbolResults: symbols.map((row) => ({
        symbol: row.symbol,
        status: 'succeeded' as const,
        deleted: row.symbol === 'AAA' ? 3 : 2
      }))
    },
    error: null
  };
}

async function waitForColumns(): Promise<void> {
  await waitFor(() => {
    expect(DataService.getGenericData).toHaveBeenCalled();
  });
  await waitFor(() => {
    expect(screen.getByDisplayValue('Close')).toBeInTheDocument();
  });
}

async function previewCandidates(): Promise<void> {
  fireEvent.click(screen.getByRole('button', { name: /preview symbols/i }));
  await waitFor(() => {
    expect(DataService.getPurgeCandidates).toHaveBeenCalled();
  });
}

describe('SymbolPurgeByCriteriaPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(DataService.getGenericData).mockResolvedValue([{ Close: 0.99, Volume: 100, Symbol: 'AAA' }]);
    vi.mocked(DataService.getPurgeCandidates).mockResolvedValue(makeCandidateResponse());
    vi.mocked(DataService.purgeSymbolsBatch).mockResolvedValue(makeBatchSucceededOperation('op-default'));
    vi.mocked(DataService.getPurgeOperation).mockResolvedValue(makeBatchSucceededOperation('op-default'));

    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: {
        writeText: vi.fn().mockResolvedValue(undefined)
      }
    });
  });

  it('previews numeric < rule and sends expected payload', async () => {
    renderWithProviders(<SymbolPurgeByCriteriaPage />);
    await waitForColumns();

    fireEvent.change(screen.getByDisplayValue('Numeric >'), { target: { value: 'lt' } });
    fireEvent.change(screen.getByDisplayValue('90'), { target: { value: '1' } });

    await previewCandidates();

    expect(DataService.getPurgeCandidates).toHaveBeenCalledWith({
      layer: 'silver',
      domain: 'market',
      column: 'Close',
      operator: 'lt',
      aggregation: 'avg',
      value: 1,
      percentile: undefined,
      recent_rows: 1,
      offset: 0
    });

    expect(screen.getByText('AAA')).toBeInTheDocument();
    expect(screen.getByText('0.9900')).toBeInTheDocument();
  });

  it('blocks preview in percent mode when value is outside 1-100', async () => {
    renderWithProviders(<SymbolPurgeByCriteriaPage />);
    await waitForColumns();

    fireEvent.change(screen.getByDisplayValue('Numeric >'), { target: { value: 'top_percent' } });
    fireEvent.change(screen.getByDisplayValue('90'), { target: { value: '101' } });
    const previewButton = screen.getByRole('button', { name: /preview symbols/i });
    expect(previewButton).toBeDisabled();
    fireEvent.click(previewButton);

    expect(screen.getByText('Percentile must be between 1 and 100.')).toBeInTheDocument();
    expect(DataService.getPurgeCandidates).not.toHaveBeenCalled();
  });

  it('shows bronze warning + bronze preview note from backend', async () => {
    vi.mocked(DataService.getPurgeCandidates).mockResolvedValue(makeCandidateResponse({ note: BRONZE_NOTE }));

    renderWithProviders(<SymbolPurgeByCriteriaPage />);
    await waitForColumns();

    fireEvent.change(screen.getByDisplayValue('SILVER'), { target: { value: 'bronze' } });
    await waitFor(() => {
      expect(vi.mocked(DataService.getGenericData).mock.calls.length).toBeGreaterThanOrEqual(2);
    });

    expect(
      screen.getByText('Bronze-wide criteria are approximated from the silver preview layer. Silver/gold is recommended.')
    ).toBeInTheDocument();

    await previewCandidates();

    expect(screen.getByText(BRONZE_NOTE)).toBeInTheDocument();
  });

  it('supports clear/invert/copy selected controls', async () => {
    const rows: PurgeCandidateRow[] = [
      { symbol: 'BBB', matchedValue: 0.98, rowsContributing: 1, latestAsOf: '2026-02-12T18:00:00Z' },
      { symbol: 'AAA', matchedValue: 0.99, rowsContributing: 1, latestAsOf: '2026-02-12T18:00:00Z' }
    ];
    vi.mocked(DataService.getPurgeCandidates).mockResolvedValue(makeCandidateResponse({}, rows));

    renderWithProviders(<SymbolPurgeByCriteriaPage />);
    await waitForColumns();
    await previewCandidates();

    const copyButton = screen.getByRole('button', { name: /copy selected/i });
    expect(copyButton).toBeEnabled();

    fireEvent.click(screen.getByRole('button', { name: /clear all/i }));
    expect(copyButton).toBeDisabled();

    fireEvent.click(screen.getByRole('button', { name: /invert/i }));
    expect(copyButton).toBeEnabled();

    fireEvent.click(copyButton);
    const writeText = navigator.clipboard.writeText as ReturnType<typeof vi.fn>;
    expect(writeText).toHaveBeenCalledWith('AAA, BBB');
  });

  it('requires destructive confirmations before enabling purge', async () => {
    renderWithProviders(<SymbolPurgeByCriteriaPage />);
    await waitForColumns();
    await previewCandidates();

    const runButton = screen.getByRole('button', { name: /run purge for selected symbols/i });
    expect(runButton).toBeDisabled();

    fireEvent.click(screen.getByRole('checkbox', { name: /i understand this is destructive/i }));
    fireEvent.change(screen.getByPlaceholderText('PURGE'), { target: { value: 'PURGE' } });

    expect(runButton).toBeEnabled();
  });

  it('runs purge, polls operation status, and renders completion details', async () => {
    const rows = makeCandidateRows();
    vi.mocked(DataService.getPurgeCandidates).mockResolvedValue(makeCandidateResponse({}, rows));
    vi.mocked(DataService.purgeSymbolsBatch).mockResolvedValue(makeBatchRunningOperation('op-123'));
    vi.mocked(DataService.getPurgeOperation).mockResolvedValue(makeBatchSucceededOperation('op-123', rows));

    renderWithProviders(<SymbolPurgeByCriteriaPage />);
    await waitForColumns();
    await previewCandidates();

    fireEvent.click(screen.getByRole('checkbox', { name: /i understand this is destructive/i }));
    fireEvent.change(screen.getByPlaceholderText('PURGE'), { target: { value: 'PURGE' } });
    fireEvent.click(screen.getByRole('button', { name: /run purge for selected symbols/i }));

    await waitFor(() => {
      expect(DataService.purgeSymbolsBatch).toHaveBeenCalledWith({
        symbols: ['AAA', 'BBB'],
        confirm: true,
        scope_note: 'Close > 90 / 2 matched / selected 2',
        dry_run: false,
        audit_rule: {
          layer: 'silver',
          domain: 'market',
          column_name: 'Close',
          operator: 'gt',
          threshold: 90,
          aggregation: 'avg',
          recent_rows: 1,
          expression: 'Close > 90',
          selected_symbol_count: 2,
          matched_symbol_count: 2
        }
      });
    });

    await waitFor(() => {
      expect(DataService.getPurgeOperation).toHaveBeenCalledWith('op-123');
    });

    expect(await screen.findByText('Operation: op-123')).toBeInTheDocument();
    expect(await screen.findByText('Purge completed successfully. Deleted 5')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: /symbol execution status/i })).toBeInTheDocument();
    expect(screen.getAllByText('SUCCEEDED').length).toBeGreaterThan(0);
  });
});
