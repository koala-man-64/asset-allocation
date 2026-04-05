import { ArrowUpDown, ClipboardCopy } from 'lucide-react';

import { Button } from '@/app/components/ui/button';
import { Checkbox } from '@/app/components/ui/checkbox';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';

import type { SymbolPurgeController } from '../hooks/useSymbolPurgeController';
import { formatDate, formatNumber } from '../lib/symbolPurge';

type Props = {
  controller: SymbolPurgeController;
};

export function SymbolPurgeCandidateReview({ controller }: Props) {
  const { candidate, actions } = controller;

  return (
    <div className="mcm-panel flex h-[620px] flex-col p-4 sm:p-5">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h2 className="text-lg font-black uppercase">Candidate review</h2>
          <p className="text-xs text-muted-foreground">
            {candidate.response
              ? `Rows scanned: ${formatNumber(candidate.response.summary.totalRowsScanned)} - Matches: ${formatNumber(candidate.response.summary.symbolsMatched)}`
              : 'Run preview to load candidates.'}
          </p>
        </div>
        <div className="flex flex-wrap justify-end gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => void actions.copySelected()}
            disabled={candidate.selectedCount === 0}
          >
            <ClipboardCopy className="h-4 w-4" />
            Copy selected
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={actions.selectAll}
            disabled={!candidate.rows.length}
          >
            Select all
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={actions.clearAll}
            disabled={!candidate.rows.length}
          >
            Clear all
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={actions.invertSelection}
            disabled={!candidate.rows.length}
          >
            <ArrowUpDown className="h-4 w-4" />
            Invert
          </Button>
        </div>
      </div>

      <p className="mt-2 text-xs text-muted-foreground">
        {candidate.error ? `Failed: ${candidate.error}` : ''}
      </p>

      <div className="mt-3 min-h-0 flex-1 overflow-y-auto rounded-md border border-border/80">
        <Table>
          <TableHeader className="sticky top-0 z-10">
            <TableRow>
              <TableHead className="w-12">Select</TableHead>
              <TableHead>Symbol</TableHead>
              <TableHead className="w-[140px] cursor-pointer" onClick={actions.toggleSortDirection}>
                Matched value
              </TableHead>
              <TableHead className="w-[180px]">Rows contributing</TableHead>
              <TableHead>Latest as-of</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {candidate.sortedRows.length === 0 ? (
              <TableRow>
                <td colSpan={5} className="py-8 text-center text-sm text-muted-foreground">
                  {candidate.loading
                    ? 'Loading candidates...'
                    : 'No candidates yet. Select rule criteria and preview.'}
                </td>
              </TableRow>
            ) : (
              candidate.sortedRows.map((row) => (
                <TableRow key={row.symbol}>
                  <TableCell>
                    <Checkbox
                      checked={candidate.selectedSymbols.has(row.symbol)}
                      onCheckedChange={(checked) =>
                        actions.toggleCandidateSelection(row.symbol, Boolean(checked))
                      }
                    />
                  </TableCell>
                  <TableCell className="font-mono">{row.symbol}</TableCell>
                  <TableCell>{formatNumber(row.matchedValue)}</TableCell>
                  <TableCell>{formatNumber(row.rowsContributing)}</TableCell>
                  <TableCell>{formatDate(row.latestAsOf)}</TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
