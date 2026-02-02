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

  // Cleanup keys function to infer columns if empty
  // DataTable usually handles this, so passing data directly is fine.

  return (
    <div className="p-6 min-h-screen bg-gray-50 flex flex-col gap-6">
      <div className="flex flex-col gap-2">
        <h1 className="text-2xl font-bold text-gray-800 font-mono tracking-tight uppercase flex items-center gap-2">
          <Database className="h-6 w-6 text-blue-600" />
          Postgres Explorer
        </h1>
        <p className="text-sm text-gray-500 font-mono">
          Introspect database schemas and query tables directly.
        </p>
      </div>

      <div className="bg-white p-4 border border-gray-300 shadow-sm flex flex-wrap gap-4 items-end">
        {/* Schema Selector */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold text-gray-600 font-mono uppercase">Schema</label>
          <div className="relative">
            <select
              value={selectedSchema}
              onChange={(e) => setSelectedSchema(e.target.value)}
              className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-48 bg-gray-50 h-10"
            >
              {schemas.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Table Selector */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold text-gray-600 font-mono uppercase">Table</label>
          <div className="relative">
            <select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              disabled={metadataLoading || tables.length === 0}
              className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-64 bg-gray-50 h-10 disabled:opacity-50"
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
        </div>

        {/* Limit Input */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold text-gray-600 font-mono uppercase">Limit</label>
          <input
            type="number"
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
            min={1}
            max={1000}
            className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-24 bg-gray-50 h-10"
          />
        </div>

        {/* Actions */}
        <Button
          onClick={fetchData}
          disabled={loading || !selectedTable || metadataLoading}
          className="bg-gray-800 text-white hover:bg-black h-10 px-6 font-mono"
        >
          {loading ? (
            <RefreshCw className="h-4 w-4 animate-spin mr-2" />
          ) : (
            <TableIcon className="h-4 w-4 mr-2" />
          )}
          {loading ? 'QUERYING...' : 'QUERY TABLE'}
        </Button>
      </div>

      {error && (
        <div className="p-4 border-l-4 border-red-500 bg-red-50 text-red-700 font-mono text-sm">
          <strong>ERROR:</strong> {error}
        </div>
      )}

      <div className="flex-1 overflow-hidden flex flex-col min-h-[400px]">
        <DataTable
          data={data}
          className="flex-1 shadow-sm"
          emptyMessage="Select a table and run query to view data."
        />
        <div className="mt-2 text-xs text-gray-400 font-mono text-right">
          {data.length > 0 ? `Showing ${data.length} rows.` : 'Ready.'}
        </div>
      </div>
    </div>
  );
};
