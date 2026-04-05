import { AlertTriangle, Database, Layers3, ScanSearch, Search, Table2 } from 'lucide-react';

import { Alert, AlertDescription, AlertTitle } from '@/app/components/ui/alert';
import { Badge } from '@/app/components/ui/badge';
import { Input } from '@/app/components/ui/input';
import { Skeleton } from '@/app/components/ui/skeleton';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';

import type { StrategyDataCatalogController } from '../hooks/useStrategyDataCatalog';
import { formatBytes, formatDateRangeLabel, formatInt } from '../lib/strategyDataCatalog';

type Props = {
  controller: StrategyDataCatalogController;
};

export function StrategyDataCatalogDetailPanel({ controller }: Props) {
  const { detail, actions } = controller;
  const { selectedTable, selectedTableState, selectedColumns, selectedTableDocumentedCount } =
    detail;

  if (!selectedTable) {
    return (
      <div className="mcm-panel p-6">
        <div className="flex items-center gap-3 text-mcm-walnut/70">
          <Database className="h-5 w-5" />
          <div>
            <div className="font-display text-lg font-black uppercase tracking-[0.08em] text-foreground">
              Select a table contract
            </div>
            <div className="text-sm text-muted-foreground">
              The detail panel will show columns, types, descriptions, and key constraints.
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <>
      <section className="relative overflow-hidden rounded-[2rem] border-2 border-mcm-walnut bg-mcm-paper px-6 py-6 shadow-[12px_12px_0px_0px_rgba(119,63,26,0.1)]">
        <div className="absolute inset-y-0 left-0 w-2 bg-gradient-to-b from-mcm-teal via-mcm-mustard to-mcm-walnut" />
        <div className="absolute right-6 top-4 h-24 w-24 rounded-full bg-mcm-teal/12 blur-2xl" />
        <div className="relative space-y-5">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div className="space-y-3">
              <div className="flex flex-wrap gap-2">
                <Badge variant="outline">{selectedTable.layerLabel}</Badge>
                <Badge variant="secondary">{selectedTable.schemaName}</Badge>
                {selectedTable.domainLabel ? (
                  <Badge variant="secondary">{selectedTable.domainLabel}</Badge>
                ) : null}
                {selectedTable.domainMetadata?.type ? (
                  <Badge variant="secondary">{selectedTable.domainMetadata.type}</Badge>
                ) : null}
              </div>

              <div>
                <h2 className="font-display text-[clamp(1.8rem,3vw,3rem)] font-black uppercase leading-none tracking-[0.06em] text-foreground">
                  {selectedTable.tableName}
                </h2>
                <p className="mt-3 max-w-[72ch] text-sm text-mcm-walnut/70">
                  {(
                    selectedTable.domainDescription ||
                    'Postgres contract for this medallion slice. Descriptions come from published column comments and gold lookup annotations when present.'
                  ).trim()}
                </p>
              </div>
            </div>

            <div className="rounded-[1.3rem] border border-mcm-walnut/15 bg-mcm-paper/80 px-4 py-4 text-sm text-mcm-walnut/70 shadow-[0_12px_28px_rgba(119,63,26,0.08)]">
              <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                Domain telemetry
              </div>
              <div className="mt-2 space-y-1">
                <div>
                  Columns in snapshot: {formatInt(selectedTable.domainMetadata?.columnCount)}
                </div>
                <div>Symbols: {formatInt(selectedTable.domainMetadata?.symbolCount)}</div>
                <div>Storage: {formatBytes(selectedTable.domainMetadata?.totalBytes)}</div>
                <div>Range: {formatDateRangeLabel(selectedTable.domainMetadata)}</div>
              </div>
            </div>
          </div>

          {selectedTableState?.error ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Table metadata could not be loaded</AlertTitle>
              <AlertDescription>{selectedTableState.error}</AlertDescription>
            </Alert>
          ) : selectedTableState?.isLoading || !selectedTableState?.data ? (
            <ColumnDetailSkeleton />
          ) : (
            <div className="space-y-4">
              <div className="grid gap-3 md:grid-cols-4">
                <SummaryTile
                  label="Columns"
                  value={formatInt(selectedTableState.data.columns.length)}
                  note="Fields in the Postgres contract."
                />
                <SummaryTile
                  label="Documented"
                  value={formatInt(selectedTableDocumentedCount)}
                  note="Columns with a published description."
                />
                <SummaryTile
                  label="Primary Key"
                  value={
                    selectedTableState.data.primary_key.length
                      ? selectedTableState.data.primary_key.join(', ')
                      : 'None'
                  }
                  note="Key columns declared by the table."
                />
                <SummaryTile
                  label="Editing"
                  value={selectedTableState.data.can_edit ? 'Enabled' : 'Read only'}
                  note={
                    selectedTableState.data.can_edit
                      ? 'Rows can be edited from the explorer.'
                      : (
                          selectedTableState.data.edit_reason ||
                          'Editing is disabled for this contract.'
                        ).trim()
                  }
                />
              </div>

              <div className="rounded-[1.5rem] border border-mcm-walnut/15 bg-mcm-paper/80 p-4 shadow-[0_14px_30px_rgba(119,63,26,0.08)]">
                <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                  <div className="space-y-1">
                    <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                      Column Contract
                    </div>
                    <div className="flex items-center gap-2 font-display text-xl font-black uppercase tracking-[0.08em] text-foreground">
                      <Table2 className="h-5 w-5 text-mcm-teal" />
                      Name, type, and description
                    </div>
                  </div>

                  <div className="relative min-w-[280px]">
                    <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      aria-label="Search selected table columns"
                      value={detail.columnSearch}
                      onChange={(event) => actions.setColumnSearch(event.target.value)}
                      placeholder="Filter the selected column contract"
                      className="pl-10"
                    />
                  </div>
                </div>

                <div className="mt-4 overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-[280px]">Column</TableHead>
                        <TableHead className="w-[180px]">Type</TableHead>
                        <TableHead className="w-[140px]">Flags</TableHead>
                        <TableHead>Description</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {selectedColumns.length === 0 ? (
                        <TableRow>
                          <TableCell
                            colSpan={4}
                            className="py-10 text-center text-sm text-muted-foreground"
                          >
                            No columns matched the current column filter.
                          </TableCell>
                        </TableRow>
                      ) : (
                        selectedColumns.map((column) => (
                          <TableRow key={column.name}>
                            <TableCell className="align-top">
                              <div className="space-y-2">
                                <div className="font-mono text-xs font-bold uppercase tracking-[0.12em] text-foreground">
                                  {column.name}
                                </div>
                                <div className="flex flex-wrap gap-2">
                                  {column.primary_key ? <Badge variant="default">PK</Badge> : null}
                                  {column.nullable ? (
                                    <Badge variant="outline">Nullable</Badge>
                                  ) : (
                                    <Badge variant="outline">Required</Badge>
                                  )}
                                  {column.status ? (
                                    <Badge variant="secondary">{column.status}</Badge>
                                  ) : null}
                                </div>
                              </div>
                            </TableCell>
                            <TableCell className="align-top font-mono text-xs text-foreground">
                              {column.data_type}
                              {column.calculationType ? (
                                <div className="mt-2 text-[11px] uppercase tracking-[0.14em] text-mcm-walnut/55">
                                  {column.calculationType}
                                </div>
                              ) : null}
                            </TableCell>
                            <TableCell className="align-top text-xs text-mcm-walnut/70">
                              <div>{column.editable ? 'Editable' : 'Read only'}</div>
                              <div className="mt-1 uppercase tracking-[0.14em] text-mcm-walnut/55">
                                {column.descriptionSource === 'postgres'
                                  ? 'postgres comment'
                                  : column.descriptionSource === 'gold-lookup'
                                    ? 'gold lookup'
                                    : 'undocumented'}
                              </div>
                            </TableCell>
                            <TableCell className="align-top whitespace-normal text-sm text-foreground">
                              {column.description ? (
                                <div className="space-y-2">
                                  <div>{column.description}</div>
                                  {column.calculationNotes ? (
                                    <div className="rounded-[1rem] bg-mcm-cream/70 px-3 py-2 text-xs text-mcm-walnut/70">
                                      {column.calculationNotes}
                                    </div>
                                  ) : null}
                                </div>
                              ) : (
                                <span className="text-muted-foreground">
                                  Description not published for this column.
                                </span>
                              )}
                            </TableCell>
                          </TableRow>
                        ))
                      )}
                    </TableBody>
                  </Table>
                </div>
              </div>
            </div>
          )}
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
        <div className="mcm-panel p-5">
          <div className="flex items-center gap-3">
            <Layers3 className="h-5 w-5 text-mcm-teal" />
            <div>
              <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                Why this table
              </div>
              <div className="font-display text-lg font-black uppercase tracking-[0.08em] text-foreground">
                Current role in the atlas
              </div>
            </div>
          </div>
          <p className="mt-4 text-sm text-mcm-walnut/70">
            {selectedTable.domainLabel
              ? `${selectedTable.tableName} is linked to the ${selectedTable.domainLabel} domain inside the ${selectedTable.layerLabel} medallion. Use the domain strips above to compare its telemetry against peer domains.`
              : `${selectedTable.tableName} is visible in the ${selectedTable.layerLabel} schema, but the current system metadata did not publish a direct domain match for it.`}
          </p>
        </div>

        <div className="mcm-panel p-5">
          <div className="flex items-center gap-3">
            <ScanSearch className="h-5 w-5 text-mcm-teal" />
            <div>
              <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                Contract Source
              </div>
              <div className="font-display text-lg font-black uppercase tracking-[0.08em] text-foreground">
                Metadata lineage
              </div>
            </div>
          </div>
          <ul className="mt-4 space-y-2 text-sm text-mcm-walnut/70">
            <li>
              System-status snapshot provides domain health, column counts, symbols, and storage
              rollups.
            </li>
            <li>
              Postgres table metadata provides authoritative table columns, types, keys, and
              editability.
            </li>
            <li>
              Gold lookup annotations backfill authored descriptions when the Postgres comment is
              empty.
            </li>
          </ul>
        </div>
      </section>
    </>
  );
}

function SummaryTile({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="rounded-[1.4rem] border border-mcm-walnut/15 bg-mcm-paper/75 px-4 py-4 shadow-[0_12px_32px_rgba(119,63,26,0.08)]">
      <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
        {label}
      </div>
      <div className="mt-2 font-display text-3xl font-black tracking-[0.04em] text-foreground">
        {value}
      </div>
      <div className="mt-1 text-sm text-muted-foreground">{note}</div>
    </div>
  );
}

function ColumnDetailSkeleton() {
  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <Skeleton key={index} className="h-20 rounded-[1.25rem]" />
        ))}
      </div>
      <Skeleton className="h-12 w-full rounded-[1rem]" />
      <Skeleton className="h-[420px] w-full rounded-[1.5rem]" />
    </div>
  );
}
