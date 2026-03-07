import React, { useState, useEffect, useCallback } from 'react';
import { PostgresService } from '@/services/PostgresService';
import { DataTable } from '@/app/components/common/DataTable';
import { Database, Table as TableIcon, RefreshCw, Trash2 } from 'lucide-react';
import { Button } from '@/app/components/ui/button';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

const HIDDEN_SCHEMAS = new Set(['public', 'information_schema']);

function isVisibleSchema(schema: string): boolean {
  return !HIDDEN_SCHEMAS.has(String(schema || '').trim().toLowerCase());
}

export const PostgresExplorerPage: React.FC = () => {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string>('');
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [limit, setLimit] = useState<number>(100);
  const [data, setData] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [purging, setPurging] = useState<boolean>(false);
  const [metadataLoading, setMetadataLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    if (!selectedSchema || !selectedTable) return;

    setLoading(true);
    setError(null);
    setStatusMessage(null);
    try {
      const result = await PostgresService.queryTable({
        schema_name: selectedSchema,
        table_name: selectedTable,
        limit
      });
      setData(result);
    } catch (err) {
      setError(formatSystemStatusText(err));
    } finally {
      setLoading(false);
    }
  }, [selectedSchema, selectedTable, limit]);

  const purgeData = useCallback(async () => {
    if (!selectedSchema || !selectedTable) return;

    const confirmed = window.confirm(
      `Purge all rows from ${selectedSchema}.${selectedTable}? This action cannot be undone.`
    );
    if (!confirmed) return;

    setPurging(true);
    setError(null);
    setStatusMessage(null);
    try {
      const result = await PostgresService.purgeTable({
        schema_name: selectedSchema,
        table_name: selectedTable
      });
      setData([]);
      setStatusMessage(
        `Purged ${result.row_count} rows from ${result.schema_name}.${result.table_name}.`
      );
    } catch (err) {
      setError(formatSystemStatusText(err));
    } finally {
      setPurging(false);
    }
  }, [selectedSchema, selectedTable]);

  useEffect(() => {
    const loadSchemas = async () => {
      try {
        const loadedSchemas = (await PostgresService.listSchemas()).filter(isVisibleSchema);
        setSchemas(loadedSchemas);
        setSelectedSchema((current) => {
          if (current && loadedSchemas.includes(current)) {
            return current;
          }
          return loadedSchemas[0] ?? '';
        });
      } catch (err) {
        console.error('Failed to load schemas', err);
        const message = formatSystemStatusText(err);
        setError(message ? `Failed to load schemas: ${message}` : 'Failed to load schemas.');
      }
    };
    void loadSchemas();
  }, []);

  useEffect(() => {
    const loadTables = async () => {
      if (!selectedSchema) return;
      setMetadataLoading(true);
      setError(null);
      setStatusMessage(null);
      setTables([]);
      setSelectedTable('');
      setData([]);
      try {
        const loadedTables = await PostgresService.listTables(selectedSchema);
        setTables(loadedTables);
        if (loadedTables.length > 0) {
          setSelectedTable(loadedTables[0]);
        }
      } catch (err) {
        console.error('Failed to load tables', err);
        const message = formatSystemStatusText(err);
        setError(
          message
            ? `Failed to load tables for schema ${selectedSchema}: ${message}`
            : `Failed to load tables for schema ${selectedSchema}`
        );
      } finally {
        setMetadataLoading(false);
      }
    };
    void loadTables();
  }, [selectedSchema]);

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
        <p className="page-subtitle">Introspect database schemas and query tables directly.</p>
      </div>

      <div className="mcm-panel p-4 sm:p-5">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-[220px_300px_160px_1fr_auto_auto] lg:items-end">
          <div className="space-y-2">
            <label htmlFor="postgres-schema">Schema</label>
            <select
              id="postgres-schema"
              value={selectedSchema}
              onChange={(e) => setSelectedSchema(e.target.value)}
              disabled={schemas.length === 0}
              className={controlClass}
            >
              {schemas.length === 0 ? (
                <option value="">(No visible schemas)</option>
              ) : (
                schemas.map((schema) => (
                  <option key={schema} value={schema}>
                    {schema}
                  </option>
                ))
              )}
            </select>
          </div>

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
                tables.map((table) => (
                  <option key={table} value={table}>
                    {table}
                  </option>
                ))
              )}
            </select>
          </div>

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

          <Button
            onClick={() => void fetchData()}
            disabled={loading || purging || !selectedTable || metadataLoading}
            className="h-10 gap-2 px-6"
          >
            {loading ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <TableIcon className="h-4 w-4" />
            )}
            {loading ? 'Querying…' : 'Query Table'}
          </Button>

          <Button
            onClick={() => void purgeData()}
            disabled={loading || purging || !selectedTable || metadataLoading}
            variant="destructive"
            className="h-10 gap-2 px-6"
          >
            {purging ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Trash2 className="h-4 w-4" />
            )}
            {purging ? 'Purging…' : 'Purge Table'}
          </Button>
        </div>
      </div>

      {statusMessage && (
        <div className="rounded-lg border border-mcm-teal/30 bg-mcm-teal/10 p-4 font-mono text-sm text-mcm-walnut">
          <strong>Status:</strong> {statusMessage}
        </div>
      )}

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
