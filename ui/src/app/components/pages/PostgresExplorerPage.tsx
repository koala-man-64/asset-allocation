import React, { useState, useEffect, useCallback } from 'react';
import { PostgresService } from '@/services/PostgresService';
import { DataTable } from '@/app/components/common/DataTable';
import { Database, Table as TableIcon, RefreshCw } from 'lucide-react';
import { Button } from '@/app/components/ui/button';

export const PostgresExplorerPage: React.FC = () => {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string>('public');
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [limit, setLimit] = useState<number>(100);
  const [data, setData] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [metadataLoading, setMetadataLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch Data
  const fetchData = useCallback(async () => {
    if (!selectedSchema || !selectedTable) return;

    setLoading(true);
    setError(null);
    try {
      const result = await PostgresService.queryTable({
        schema_name: selectedSchema,
        table_name: selectedTable,
        limit
      });
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [selectedSchema, selectedTable, limit]);

  // Initial Load: Fetch Schemas
  useEffect(() => {
    const loadSchemas = async () => {
      try {
        const s = await PostgresService.listSchemas();
        setSchemas(s);
      } catch (err) {
        console.error('Failed to load schemas', err);
        setError('Failed to load schemas.');
      }
    };
    loadSchemas();
  }, []);

  // Fetch Tables when Schema Changes
  useEffect(() => {
    const loadTables = async () => {
      if (!selectedSchema) return;
      setMetadataLoading(true);
      setTables([]); // Clear previous
      setSelectedTable(''); // Clear selection
      setData([]); // Clear data
      try {
        const t = await PostgresService.listTables(selectedSchema);
        setTables(t);
        if (t.length > 0) {
          setSelectedTable(t[0]); // Auto-select first table
        }
      } catch (err) {
        console.error('Failed to load tables', err);
        setError(`Failed to load tables for schema ${selectedSchema}`);
      } finally {
        setMetadataLoading(false);
      }
    };
    loadTables();
  }, [selectedSchema]);

  // Cleanup keys function to infer columns if empty
  // DataTable usually handles this, so passing data directly is fine.
  const controlClass =
    'h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono outline-none transition-shadow focus-visible:ring-2 focus-visible:ring-ring/40';

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title flex items-center gap-2">
          <Database className="h-5 w-5 text-mcm-teal" />
          Postgres Explorer
        </h1>
        <p className="page-subtitle">
          Introspect database schemas and query tables directly.
        </p>
      </div>

      <div className="mcm-panel p-4 sm:p-5">
        {/* Schema Selector */}
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-[220px_300px_160px_1fr_auto] lg:items-end">
          <div className="space-y-2">
            <label htmlFor="postgres-schema">Schema</label>
            <select
              id="postgres-schema"
              value={selectedSchema}
              onChange={(e) => setSelectedSchema(e.target.value)}
              className={controlClass}
            >
              {schemas.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>

          {/* Table Selector */}
          <div className="space-y-2">
            <label htmlFor="postgres-table">Table</label>
            <select
              id="postgres-table"
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              disabled={metadataLoading || tables.length === 0}
              className={`${controlClass} disabled:opacity-50`}
            >
              {tables.length === 0 ? (
                <option value="">(No tables found)</option>
              ) : (
                tables.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))
              )}
            </select>
          </div>

          {/* Limit Input */}
          <div className="space-y-2">
            <label htmlFor="postgres-limit">Limit</label>
            <input
              id="postgres-limit"
              type="number"
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              min={1}
              max={1000}
              className={controlClass}
            />
          </div>

          <div />

          {/* Actions */}
          <Button
            onClick={fetchData}
            disabled={loading || !selectedTable || metadataLoading}
            className="h-10 gap-2 px-6"
          >
            {loading ? <RefreshCw className="h-4 w-4 animate-spin" /> : <TableIcon className="h-4 w-4" />}
            {loading ? 'Querying…' : 'Query Table'}
          </Button>
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-4 font-mono text-sm text-destructive">
          <strong>Error:</strong> {error}
        </div>
      )}

      <div className="flex-1 overflow-hidden flex flex-col min-h-[400px]">
        <DataTable
          data={data}
          className="flex-1"
          emptyMessage="Select a table and run query to view data."
        />
        <div className="mt-2 text-right font-mono text-xs text-muted-foreground">
          {data.length > 0 ? `Showing ${data.length} rows.` : 'Ready.'}
        </div>
      </div>
    </div>
  );
};
