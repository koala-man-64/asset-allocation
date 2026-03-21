import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Checkbox } from '@/app/components/ui/checkbox';
import { Input } from '@/app/components/ui/input';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import { PostgresService, type GoldColumnLookupRow } from '@/services/PostgresService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { toast } from 'sonner';
import { CirclePlus, Copy, Database, ListChecks, RefreshCw, Search, Trash2 } from 'lucide-react';

const GOLD_SCHEMA = 'gold';

type ExportRow = {
  schema: string;
  table: string;
  column: string;
  description: string;
};

function makeRowKey(row: Pick<ExportRow, 'schema' | 'table' | 'column'>): string {
  return `${row.schema}.${row.table}.${row.column}`;
}

function escapeMarkdownCell(value: string): string {
  return value.replace(/\|/g, '\\|').replace(/\r?\n/g, ' ').trim();
}

function toMarkdown(rows: ExportRow[]): string {
  const header = '| schema | table | column | description |';
  const divider = '|---|---|---|---|';
  const lines = rows.map((row) => {
    const schema = escapeMarkdownCell(row.schema);
    const table = escapeMarkdownCell(row.table);
    const column = escapeMarkdownCell(row.column);
    const description = escapeMarkdownCell(row.description);
    return `| ${schema} | ${table} | ${column} | ${description} |`;
  });
  return [header, divider, ...lines].join('\n');
}

function escapeCsvCell(value: string): string {
  const escaped = value.replace(/"/g, '""');
  return `"${escaped}"`;
}

function toCsv(rows: ExportRow[]): string {
  const header = '"schema","table","column","description"';
  const lines = rows.map((row) =>
    [
      escapeCsvCell(row.schema),
      escapeCsvCell(row.table),
      escapeCsvCell(row.column),
      escapeCsvCell(row.description)
    ].join(',')
  );
  return [header, ...lines].join('\n');
}

async function copyText(value: string): Promise<void> {
  if (navigator?.clipboard?.writeText) {
    await navigator.clipboard.writeText(value);
    return;
  }

  const textarea = document.createElement('textarea');
  textarea.value = value;
  textarea.style.position = 'fixed';
  textarea.style.left = '-9999px';
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  document.execCommand('copy');
  document.body.removeChild(textarea);
}

export const StrategyDataCatalogPage: React.FC = () => {
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [catalogRows, setCatalogRows] = useState<GoldColumnLookupRow[]>([]);
  const [tablesLoading, setTablesLoading] = useState<boolean>(false);
  const [columnsLoading, setColumnsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [tableSearch, setTableSearch] = useState<string>('');
  const [columnSearch, setColumnSearch] = useState<string>('');
  const [checkedColumns, setCheckedColumns] = useState<string[]>([]);
  const [exportRows, setExportRows] = useState<ExportRow[]>([]);

  const loadGoldTables = useCallback(async () => {
    setTablesLoading(true);
    setError(null);
    try {
      const loadedTables = await PostgresService.listGoldLookupTables();
      setTables(loadedTables);
      setSelectedTable((current) =>
        current && loadedTables.includes(current) ? current : loadedTables[0] || ''
      );
      if (loadedTables.length === 0) {
        setCatalogRows([]);
      }
    } catch (err) {
      setError(formatSystemStatusText(err));
    } finally {
      setTablesLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadGoldTables();
  }, [loadGoldTables]);

  useEffect(() => {
    let active = true;
    const loadColumns = async () => {
      if (!selectedTable) {
        setCatalogRows([]);
        return;
      }

      setColumnsLoading(true);
      setError(null);
      setCheckedColumns([]);
      try {
        const response = await PostgresService.listGoldColumnLookup({
          table: selectedTable,
          q: columnSearch.trim() || undefined,
          limit: 5000
        });
        if (!active) {
          return;
        }
        setCatalogRows(response.rows);
      } catch (err) {
        if (!active) {
          return;
        }
        setError(formatSystemStatusText(err));
        setCatalogRows([]);
      } finally {
        if (active) {
          setColumnsLoading(false);
        }
      }
    };

    void loadColumns();
    return () => {
      active = false;
    };
  }, [columnSearch, selectedTable]);

  const visibleTables = useMemo(() => {
    const query = tableSearch.trim().toLowerCase();
    if (!query) {
      return tables;
    }
    return tables.filter((tableName) => tableName.toLowerCase().includes(query));
  }, [tableSearch, tables]);

  const visibleColumns = useMemo(() => {
    return catalogRows;
  }, [catalogRows]);

  const checkedSet = useMemo(() => new Set(checkedColumns), [checkedColumns]);
  const selectedVisibleCount = visibleColumns.filter((column) =>
    checkedSet.has(column.column)
  ).length;
  const allVisibleChecked =
    visibleColumns.length > 0 && selectedVisibleCount === visibleColumns.length;

  const toggleColumn = useCallback((columnName: string, nextChecked: boolean) => {
    setCheckedColumns((current) => {
      const currentSet = new Set(current);
      if (nextChecked) {
        currentSet.add(columnName);
      } else {
        currentSet.delete(columnName);
      }
      return Array.from(currentSet);
    });
  }, []);

  const toggleVisibleColumns = useCallback(
    (nextChecked: boolean) => {
      setCheckedColumns((current) => {
        const currentSet = new Set(current);
        for (const column of visibleColumns) {
          if (nextChecked) {
            currentSet.add(column.column);
          } else {
            currentSet.delete(column.column);
          }
        }
        return Array.from(currentSet);
      });
    },
    [visibleColumns]
  );

  const addSelectedColumns = useCallback(() => {
    if (!selectedTable) {
      return;
    }
    if (checkedColumns.length === 0) {
      toast.error('Select at least one column before adding to the export list.');
      return;
    }

    const selectedByName = new Set(checkedColumns);
    const candidateRows = visibleColumns
      .filter((column) => selectedByName.has(column.column))
      .map<ExportRow>((column) => ({
        schema: column.schema || GOLD_SCHEMA,
        table: selectedTable,
        column: column.column,
        description: (column.description || '').trim() || 'No description provided.'
      }));

    let added = 0;
    setExportRows((current) => {
      const next = [...current];
      const keys = new Set(current.map((row) => makeRowKey(row)));
      for (const row of candidateRows) {
        const key = makeRowKey(row);
        if (!keys.has(key)) {
          next.push(row);
          keys.add(key);
          added += 1;
        }
      }
      return next;
    });

    if (added === 0) {
      toast.message('All selected columns are already in the export list.');
      return;
    }
    toast.success(`Added ${added} column${added === 1 ? '' : 's'} to the export list.`);
  }, [checkedColumns, selectedTable, visibleColumns]);

  const removeExportRow = useCallback((row: ExportRow) => {
    const keyToRemove = makeRowKey(row);
    setExportRows((current) => current.filter((item) => makeRowKey(item) !== keyToRemove));
  }, []);

  const clearExportRows = useCallback(() => {
    setExportRows([]);
  }, []);

  const copyMarkdown = useCallback(async () => {
    if (!exportRows.length) {
      toast.error('Add at least one row before copying.');
      return;
    }
    try {
      await copyText(toMarkdown(exportRows));
      toast.success('Copied export list as Markdown table.');
    } catch {
      toast.error('Failed to copy Markdown to clipboard.');
    }
  }, [exportRows]);

  const copyCsv = useCallback(async () => {
    if (!exportRows.length) {
      toast.error('Add at least one row before copying.');
      return;
    }
    try {
      await copyText(toCsv(exportRows));
      toast.success('Copied export list as CSV.');
    } catch {
      toast.error('Failed to copy CSV to clipboard.');
    }
  }, [exportRows]);

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Strategy Exploration</p>
        <h1 className="page-title flex items-center gap-2">
          <ListChecks className="h-5 w-5 text-mcm-teal" />
          Gold Data Catalog
        </h1>
        <p className="page-subtitle">
          Browse gold-layer tables, pick feature columns, and build a reusable export list for
          strategy ideation agents.
        </p>
      </div>

      {error ? (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-4 font-mono text-sm text-destructive">
          <strong>Error:</strong> {error}
        </div>
      ) : null}

      <div className="grid gap-6 xl:grid-cols-[minmax(260px,300px)_minmax(0,1fr)]">
        <section className="mcm-panel p-4 sm:p-5">
          <div className="mb-3 flex items-center justify-between gap-3">
            <div className="space-y-1">
              <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">
                Gold Tables
              </div>
              <div className="font-display text-lg text-foreground">{tables.length} available</div>
            </div>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => void loadGoldTables()}
              disabled={tablesLoading}
              className="gap-2"
            >
              <RefreshCw className={`h-3.5 w-3.5 ${tablesLoading ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </div>

          <div className="relative mb-3">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={tableSearch}
              onChange={(event) => setTableSearch(event.target.value)}
              placeholder="Filter tables..."
              className="pl-9"
              aria-label="Filter tables"
            />
          </div>

          <div className="max-h-[520px] space-y-2 overflow-y-auto pr-1">
            {tablesLoading ? (
              <div className="rounded-xl border border-mcm-walnut/25 bg-mcm-cream/60 p-4 text-sm text-muted-foreground">
                Loading table catalog...
              </div>
            ) : visibleTables.length === 0 ? (
              <div className="rounded-xl border border-dashed border-mcm-walnut/30 bg-mcm-cream/55 p-4 text-sm text-muted-foreground">
                No tables match your filter.
              </div>
            ) : (
              visibleTables.map((tableName) => {
                const isActive = selectedTable === tableName;
                return (
                  <button
                    key={tableName}
                    type="button"
                    className={`w-full rounded-2xl border-2 px-3 py-3 text-left transition ${
                      isActive
                        ? 'border-mcm-teal bg-mcm-teal/10'
                        : 'border-mcm-walnut/25 bg-mcm-paper hover:bg-mcm-cream/80'
                    }`}
                    onClick={() => setSelectedTable(tableName)}
                    aria-pressed={isActive}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <span className="font-display text-sm text-foreground">{tableName}</span>
                      {isActive ? <Badge variant="default">Active</Badge> : null}
                    </div>
                    <div className="mt-1 text-xs text-muted-foreground">schema: {GOLD_SCHEMA}</div>
                  </button>
                );
              })
            )}
          </div>
        </section>

        <div className="space-y-6">
          <section className="mcm-panel overflow-hidden p-4 sm:p-5">
            <div className="mb-4 flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div className="space-y-1">
                <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">
                  Column Browser
                </div>
                <h2 className="font-display text-xl text-foreground">
                  {selectedTable ? `${GOLD_SCHEMA}.${selectedTable}` : 'Select a table'}
                </h2>
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{visibleColumns.length} visible</Badge>
                <Badge variant={checkedColumns.length ? 'default' : 'secondary'}>
                  {checkedColumns.length} selected
                </Badge>
              </div>
            </div>

            <div className="mb-4 grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto_auto]">
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
                <Input
                  value={columnSearch}
                  onChange={(event) => setColumnSearch(event.target.value)}
                  placeholder="Filter columns or descriptions..."
                  className="pl-9"
                  aria-label="Filter columns or descriptions"
                />
              </div>
              <Button
                type="button"
                variant="outline"
                onClick={() => toggleVisibleColumns(!allVisibleChecked)}
                disabled={!visibleColumns.length}
              >
                {allVisibleChecked ? 'Clear Visible' : 'Select Visible'}
              </Button>
              <Button
                type="button"
                onClick={addSelectedColumns}
                disabled={!checkedColumns.length || !selectedTable || columnsLoading}
                className="gap-2"
              >
                <CirclePlus className="h-4 w-4" />
                Add to Export List
              </Button>
            </div>

            {columnsLoading ? (
              <div className="rounded-xl border border-mcm-walnut/25 bg-mcm-cream/60 p-4 text-sm text-muted-foreground">
                Loading column metadata...
              </div>
            ) : !selectedTable ? (
              <div className="rounded-xl border border-dashed border-mcm-walnut/30 bg-mcm-cream/60 p-4 text-sm text-muted-foreground">
                Select a gold table to browse its available columns.
              </div>
            ) : visibleColumns.length === 0 ? (
              <div className="rounded-xl border border-dashed border-mcm-walnut/30 bg-mcm-cream/60 p-4 text-sm text-muted-foreground">
                No columns match your filter.
              </div>
            ) : (
              <div className="max-h-[420px] overflow-y-auto pr-1">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-14 text-center">Pick</TableHead>
                      <TableHead>Column</TableHead>
                      <TableHead className="w-[200px]">Type</TableHead>
                      <TableHead>Description</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {visibleColumns.map((column) => {
                      const checked = checkedSet.has(column.column);
                      return (
                        <TableRow key={column.column} data-state={checked ? 'selected' : undefined}>
                          <TableCell className="text-center">
                            <Checkbox
                              checked={checked}
                              onCheckedChange={(next) => toggleColumn(column.column, Boolean(next))}
                              aria-label={`Select column ${column.column}`}
                            />
                          </TableCell>
                          <TableCell className="font-mono text-xs uppercase tracking-wide">
                            {column.column}
                            <div className="mt-1 text-[11px] text-muted-foreground">
                              {column.calculation_type} · {column.status}
                            </div>
                          </TableCell>
                          <TableCell className="font-mono text-xs">{column.data_type}</TableCell>
                          <TableCell className="whitespace-normal text-sm">
                            {(column.description || '').trim() || (
                              <span className="text-muted-foreground">
                                No description provided.
                              </span>
                            )}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </section>

          <section className="mcm-panel overflow-hidden p-4 sm:p-5">
            <div className="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div className="space-y-1">
                <div className="text-[10px] font-black uppercase tracking-[0.18em] text-muted-foreground">
                  Export List
                </div>
                <h2 className="font-display text-xl text-foreground flex items-center gap-2">
                  <Database className="h-5 w-5 text-mcm-teal" />
                  Strategy Feature Catalog
                </h2>
                <p className="text-sm text-muted-foreground">
                  Export rows include exactly: schema, table, column, description.
                </p>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => void copyMarkdown()}
                  disabled={!exportRows.length}
                  className="gap-2"
                >
                  <Copy className="h-4 w-4" />
                  Copy Markdown
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  onClick={() => void copyCsv()}
                  disabled={!exportRows.length}
                  className="gap-2"
                >
                  <Copy className="h-4 w-4" />
                  Copy CSV
                </Button>
                <Button
                  type="button"
                  variant="destructive"
                  onClick={clearExportRows}
                  disabled={!exportRows.length}
                  className="gap-2"
                >
                  <Trash2 className="h-4 w-4" />
                  Clear List
                </Button>
              </div>
            </div>

            {exportRows.length === 0 ? (
              <div className="rounded-xl border-2 border-dashed border-mcm-walnut/35 bg-mcm-cream/55 p-5 text-sm text-muted-foreground">
                Add columns from the browser above to build your strategy exploration catalog.
              </div>
            ) : (
              <div className="max-h-[420px] overflow-y-auto pr-1">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-[130px]">Schema</TableHead>
                      <TableHead className="w-[200px]">Table</TableHead>
                      <TableHead className="w-[220px]">Column</TableHead>
                      <TableHead>Description</TableHead>
                      <TableHead className="w-20 text-center">Remove</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {exportRows.map((row) => (
                      <TableRow key={makeRowKey(row)}>
                        <TableCell className="font-mono text-xs uppercase tracking-wide">
                          {row.schema}
                        </TableCell>
                        <TableCell className="font-mono text-xs">{row.table}</TableCell>
                        <TableCell className="font-mono text-xs">{row.column}</TableCell>
                        <TableCell className="whitespace-normal text-sm">
                          {row.description}
                        </TableCell>
                        <TableCell className="text-center">
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon"
                            onClick={() => removeExportRow(row)}
                            aria-label={`Remove ${row.column}`}
                          >
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
};
