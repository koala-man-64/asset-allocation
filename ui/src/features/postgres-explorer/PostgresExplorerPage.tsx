import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  PostgresService,
  type PostgresColumnMetadata,
  type QueryFilter as PostgresQueryFilter,
  type QueryFilterOperator,
  type PostgresTableMetadata
} from '@/services/PostgresService';
import { DataTable } from '@/app/components/common/DataTable';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from '@/app/components/ui/dialog';
import { Input } from '@/app/components/ui/input';
import { Switch } from '@/app/components/ui/switch';
import { Textarea } from '@/app/components/ui/textarea';
import { toast } from 'sonner';
import {
  Database,
  Filter as FilterIcon,
  Pencil,
  Plus,
  RefreshCw,
  Save,
  Table as TableIcon,
  Trash2,
  X
} from 'lucide-react';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

const HIDDEN_SCHEMAS = new Set(['public', 'information_schema']);
const BOOLEAN_TRUE_VALUES = new Set(['1', 'true', 't', 'yes', 'y', 'on']);
const BOOLEAN_FALSE_VALUES = new Set(['0', 'false', 'f', 'no', 'n', 'off']);

type RowData = Record<string, unknown>;

type EditableFieldState = {
  raw: string;
  isNull: boolean;
};

type EditState = {
  match: Record<string, unknown>;
  fields: Record<string, EditableFieldState>;
};

type QueryFilterDraft = {
  id: string;
  columnName: string;
  operator: QueryFilterOperator;
  value: string;
};

const TEXT_FILTER_OPERATORS: Array<{ value: QueryFilterOperator; label: string }> = [
  { value: 'contains', label: 'contains' },
  { value: 'eq', label: 'equals' },
  { value: 'neq', label: 'does not equal' },
  { value: 'starts_with', label: 'starts with' },
  { value: 'ends_with', label: 'ends with' },
  { value: 'is_null', label: 'is null' },
  { value: 'is_not_null', label: 'is not null' }
];

const COMPARISON_FILTER_OPERATORS: Array<{ value: QueryFilterOperator; label: string }> = [
  { value: 'eq', label: 'equals' },
  { value: 'neq', label: 'does not equal' },
  { value: 'gt', label: 'greater than' },
  { value: 'gte', label: 'greater than or equal' },
  { value: 'lt', label: 'less than' },
  { value: 'lte', label: 'less than or equal' },
  { value: 'is_null', label: 'is null' },
  { value: 'is_not_null', label: 'is not null' }
];

const BOOLEAN_FILTER_OPERATORS: Array<{ value: QueryFilterOperator; label: string }> = [
  { value: 'eq', label: 'equals' },
  { value: 'neq', label: 'does not equal' },
  { value: 'is_null', label: 'is null' },
  { value: 'is_not_null', label: 'is not null' }
];

function isVisibleSchema(schema: string): boolean {
  return !HIDDEN_SCHEMAS.has(
    String(schema || '')
      .trim()
      .toLowerCase()
  );
}

function normalizeFieldValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }
  if (typeof value === 'boolean') {
    return value ? 'true' : 'false';
  }
  return String(value);
}

function buildEditState(row: RowData, metadata: PostgresTableMetadata): EditState {
  const match = metadata.primary_key.reduce<Record<string, unknown>>((acc, columnName) => {
    acc[columnName] = row[columnName];
    return acc;
  }, {});

  const fields = metadata.columns.reduce<Record<string, EditableFieldState>>((acc, column) => {
    const value = row[column.name];
    acc[column.name] = {
      raw: normalizeFieldValue(value),
      isNull: value === null || value === undefined
    };
    return acc;
  }, {});

  return { match, fields };
}

function isJsonType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return normalized.includes('json');
}

function isArrayType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return normalized.includes('array') || normalized.endsWith('[]');
}

function isBooleanType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return normalized.includes('bool');
}

function isNumericType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return ['int', 'numeric', 'decimal', 'real', 'double', 'float', 'serial', 'money'].some((token) =>
    normalized.includes(token)
  );
}

function isDateType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return (
    normalized.includes('date') && !normalized.includes('time') && !normalized.includes('stamp')
  );
}

function isDateTimeType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return normalized.includes('timestamp') || normalized.includes('datetime');
}

function isTimeType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return normalized.includes('time') && !normalized.includes('stamp');
}

function isTextType(dataType: string): boolean {
  const normalized = String(dataType || '').toLowerCase();
  return ['char', 'text', 'uuid', 'citext'].some((token) => normalized.includes(token));
}

function getFilterOperatorOptions(
  dataType: string
): Array<{ value: QueryFilterOperator; label: string }> {
  if (isBooleanType(dataType)) {
    return BOOLEAN_FILTER_OPERATORS;
  }
  if (
    isNumericType(dataType) ||
    isDateType(dataType) ||
    isDateTimeType(dataType) ||
    isTimeType(dataType)
  ) {
    return COMPARISON_FILTER_OPERATORS;
  }
  if (isTextType(dataType)) {
    return TEXT_FILTER_OPERATORS;
  }
  return BOOLEAN_FILTER_OPERATORS;
}

function getDefaultFilterOperator(dataType: string): QueryFilterOperator {
  return getFilterOperatorOptions(dataType)[0]?.value ?? 'eq';
}

function queryFilterOperatorNeedsValue(operator: QueryFilterOperator): boolean {
  return operator !== 'is_null' && operator !== 'is_not_null';
}

function createQueryFilterId(): string {
  return `filter-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function createQueryFilterDraft(metadata: PostgresTableMetadata | null): QueryFilterDraft | null {
  const firstColumn = metadata?.columns?.[0];
  if (!firstColumn) {
    return null;
  }

  return {
    id: createQueryFilterId(),
    columnName: firstColumn.name,
    operator: getDefaultFilterOperator(firstColumn.data_type),
    value: ''
  };
}

function shouldUseTextarea(column: PostgresColumnMetadata, rawValue: string): boolean {
  return (
    isJsonType(column.data_type) ||
    isArrayType(column.data_type) ||
    rawValue.includes('\n') ||
    rawValue.length > 72
  );
}

function coerceFieldValue(column: PostgresColumnMetadata, field: EditableFieldState): unknown {
  if (field.isNull) {
    return null;
  }

  const dataType = String(column.data_type || '').toLowerCase();
  const raw = field.raw;
  const trimmed = raw.trim();

  if (isJsonType(dataType) || isArrayType(dataType)) {
    if (!trimmed) {
      throw new Error(`Field "${column.name}" requires valid JSON.`);
    }
    try {
      return JSON.parse(raw);
    } catch {
      throw new Error(`Field "${column.name}" must contain valid JSON.`);
    }
  }

  if (isBooleanType(dataType)) {
    const normalized = trimmed.toLowerCase();
    if (BOOLEAN_TRUE_VALUES.has(normalized)) {
      return true;
    }
    if (BOOLEAN_FALSE_VALUES.has(normalized)) {
      return false;
    }
    throw new Error(`Field "${column.name}" must be a boolean value.`);
  }

  if (isNumericType(dataType)) {
    if (!trimmed) {
      throw new Error(`Field "${column.name}" must be a numeric value.`);
    }
    const numeric = Number(trimmed);
    if (Number.isNaN(numeric)) {
      throw new Error(`Field "${column.name}" must be a numeric value.`);
    }
    return numeric;
  }

  return raw;
}

export const PostgresExplorerPage: React.FC = () => {
  const [schemas, setSchemas] = useState<string[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<string>('');
  const [tables, setTables] = useState<string[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [limit, setLimit] = useState<number>(100);
  const [data, setData] = useState<RowData[]>([]);
  const [tableMetadata, setTableMetadata] = useState<PostgresTableMetadata | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [purging, setPurging] = useState<boolean>(false);
  const [tablesLoading, setTablesLoading] = useState<boolean>(false);
  const [tableMetadataLoading, setTableMetadataLoading] = useState<boolean>(false);
  const [rowSaving, setRowSaving] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [editState, setEditState] = useState<EditState | null>(null);
  const [queryFilters, setQueryFilters] = useState<QueryFilterDraft[]>([]);

  const resetSelectionState = useCallback(() => {
    setError(null);
    setStatusMessage(null);
    setData([]);
    setEditState(null);
    setQueryFilters([]);
  }, []);

  const handleSchemaChange = useCallback(
    (schema: string) => {
      resetSelectionState();
      setTables([]);
      setSelectedTable('');
      setTableMetadata(null);
      setSelectedSchema(schema);
    },
    [resetSelectionState]
  );

  const handleTableChange = useCallback(
    (table: string) => {
      resetSelectionState();
      setTableMetadata(null);
      setSelectedTable(table);
    },
    [resetSelectionState]
  );

  const fetchData = useCallback(async () => {
    if (!selectedSchema || !selectedTable) return;

    setLoading(true);
    setError(null);
    setStatusMessage(null);
    try {
      const filters: PostgresQueryFilter[] = queryFilters.map((filter) => {
        const column = tableMetadata?.columns.find((item) => item.name === filter.columnName);
        if (!column) {
          throw new Error(`Unknown filter column "${filter.columnName}".`);
        }
        if (queryFilterOperatorNeedsValue(filter.operator) && !filter.value.trim()) {
          throw new Error(`Filter "${filter.columnName}" requires a value.`);
        }

        return queryFilterOperatorNeedsValue(filter.operator)
          ? {
              column_name: filter.columnName,
              operator: filter.operator,
              value: filter.value.trim()
            }
          : {
              column_name: filter.columnName,
              operator: filter.operator
            };
      });

      const result = await PostgresService.queryTable({
        schema_name: selectedSchema,
        table_name: selectedTable,
        limit,
        filters
      });
      setData(result);
      setEditState(null);
    } catch (err) {
      setError(formatSystemStatusText(err));
    } finally {
      setLoading(false);
    }
  }, [limit, queryFilters, selectedSchema, selectedTable, tableMetadata]);

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
      setEditState(null);
      setStatusMessage(
        `Purged ${result.row_count} rows from ${result.schema_name}.${result.table_name}.`
      );
    } catch (err) {
      setError(formatSystemStatusText(err));
    } finally {
      setPurging(false);
    }
  }, [selectedSchema, selectedTable]);

  const openEditor = useCallback(
    (row: RowData) => {
      if (!tableMetadata?.can_edit) {
        const message = tableMetadata?.edit_reason || 'Row editing is unavailable for this table.';
        setError(message);
        return;
      }
      setError(null);
      setStatusMessage(null);
      setEditState(buildEditState(row, tableMetadata));
    },
    [tableMetadata]
  );

  const closeEditor = useCallback(() => {
    setEditState(null);
  }, []);

  const addQueryFilter = useCallback(() => {
    const nextFilter = createQueryFilterDraft(tableMetadata);
    if (!nextFilter) {
      return;
    }
    setQueryFilters((current) => [...current, nextFilter]);
  }, [tableMetadata]);

  const removeQueryFilter = useCallback((filterId: string) => {
    setQueryFilters((current) => current.filter((filter) => filter.id !== filterId));
  }, []);

  const clearQueryFilters = useCallback(() => {
    setQueryFilters([]);
  }, []);

  const updateQueryFilterColumn = useCallback(
    (filterId: string, columnName: string) => {
      const column = tableMetadata?.columns.find((item) => item.name === columnName);
      if (!column) {
        return;
      }

      setQueryFilters((current) =>
        current.map((filter) => {
          if (filter.id !== filterId) {
            return filter;
          }

          const allowedOperators = getFilterOperatorOptions(column.data_type).map(
            (item) => item.value
          );
          return {
            ...filter,
            columnName,
            operator: allowedOperators.includes(filter.operator)
              ? filter.operator
              : getDefaultFilterOperator(column.data_type)
          };
        })
      );
    },
    [tableMetadata]
  );

  const updateQueryFilterOperator = useCallback(
    (filterId: string, operator: QueryFilterOperator) => {
      setQueryFilters((current) =>
        current.map((filter) =>
          filter.id === filterId
            ? {
                ...filter,
                operator
              }
            : filter
        )
      );
    },
    []
  );

  const updateQueryFilterValue = useCallback((filterId: string, value: string) => {
    setQueryFilters((current) =>
      current.map((filter) =>
        filter.id === filterId
          ? {
              ...filter,
              value
            }
          : filter
      )
    );
  }, []);

  const updateFieldRaw = useCallback((columnName: string, raw: string) => {
    setEditState((current) => {
      if (!current) return current;
      return {
        ...current,
        fields: {
          ...current.fields,
          [columnName]: {
            ...current.fields[columnName],
            raw
          }
        }
      };
    });
  }, []);

  const updateFieldNull = useCallback((columnName: string, isNull: boolean) => {
    setEditState((current) => {
      if (!current) return current;
      return {
        ...current,
        fields: {
          ...current.fields,
          [columnName]: {
            ...current.fields[columnName],
            isNull
          }
        }
      };
    });
  }, []);

  const saveRow = useCallback(async () => {
    if (!selectedSchema || !selectedTable || !tableMetadata || !editState) {
      return;
    }

    const values: Record<string, unknown> = {};
    try {
      for (const column of tableMetadata.columns) {
        if (!column.editable) {
          continue;
        }
        const field = editState.fields[column.name];
        if (!field) {
          continue;
        }
        values[column.name] = coerceFieldValue(column, field);
      }
    } catch (err) {
      const message =
        err instanceof Error ? err.message : 'Failed to validate row values before saving.';
      toast.error(message);
      return;
    }

    if (Object.keys(values).length === 0) {
      toast.error('This table has no editable columns.');
      return;
    }

    setRowSaving(true);
    setError(null);
    setStatusMessage(null);
    try {
      const result = await PostgresService.updateRow({
        schema_name: selectedSchema,
        table_name: selectedTable,
        match: editState.match,
        values
      });
      closeEditor();
      await fetchData();
      setStatusMessage(
        `Updated ${result.row_count} row in ${result.schema_name}.${result.table_name}.`
      );
      toast.success(`Updated ${result.updated_columns.length} field(s) on the selected row.`);
    } catch (err) {
      const message = formatSystemStatusText(err);
      setError(message);
      toast.error(`Failed to update row: ${message}`);
    } finally {
      setRowSaving(false);
    }
  }, [closeEditor, editState, fetchData, selectedSchema, selectedTable, tableMetadata]);

  useEffect(() => {
    let isActive = true;

    const loadSchemas = async () => {
      try {
        const loadedSchemas = (await PostgresService.listSchemas()).filter(isVisibleSchema);
        if (!isActive) {
          return;
        }
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
    return () => {
      isActive = false;
    };
  }, []);

  useEffect(() => {
    let isActive = true;

    const loadTables = async () => {
      if (!selectedSchema) {
        setTables([]);
        setSelectedTable('');
        setTableMetadata(null);
        return;
      }

      setTablesLoading(true);
      setError(null);
      setStatusMessage(null);
      setTables([]);
      setSelectedTable('');
      setTableMetadata(null);
      setData([]);
      setEditState(null);
      try {
        const loadedTables = await PostgresService.listTables(selectedSchema);
        if (!isActive) {
          return;
        }
        setTables(loadedTables);
        if (loadedTables.length > 0) {
          setSelectedTable(loadedTables[0]);
        }
      } catch (err) {
        if (!isActive) {
          return;
        }
        console.error('Failed to load tables', err);
        const message = formatSystemStatusText(err);
        setError(
          message
            ? `Failed to load tables for schema ${selectedSchema}: ${message}`
            : `Failed to load tables for schema ${selectedSchema}`
        );
      } finally {
        if (isActive) {
          setTablesLoading(false);
        }
      }
    };
    void loadTables();
    return () => {
      isActive = false;
    };
  }, [selectedSchema]);

  useEffect(() => {
    let isActive = true;

    const loadTableMetadata = async () => {
      if (!selectedSchema || !selectedTable) {
        setTableMetadata(null);
        return;
      }

      setTableMetadataLoading(true);
      setError(null);
      setStatusMessage(null);
      setTableMetadata(null);
      setEditState(null);
      try {
        const metadata = await PostgresService.getTableMetadata(selectedSchema, selectedTable);
        if (!isActive) {
          return;
        }
        setTableMetadata(metadata);
      } catch (err) {
        if (!isActive) {
          return;
        }
        console.error('Failed to load table metadata', err);
        const message = formatSystemStatusText(err);
        setError(
          message
            ? `Failed to load table metadata for ${selectedSchema}.${selectedTable}: ${message}`
            : `Failed to load table metadata for ${selectedSchema}.${selectedTable}`
        );
      } finally {
        if (isActive) {
          setTableMetadataLoading(false);
        }
      }
    };
    void loadTableMetadata();
    return () => {
      isActive = false;
    };
  }, [selectedSchema, selectedTable]);

  const columnsByName = useMemo(() => {
    return new Map((tableMetadata?.columns || []).map((column) => [column.name, column]));
  }, [tableMetadata?.columns]);

  const controlClass =
    'h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono outline-none transition-shadow focus-visible:ring-2 focus-visible:ring-ring/40';
  const editingEnabled = Boolean(tableMetadata?.can_edit);
  const editCapabilityLabel = tableMetadataLoading
    ? 'Loading table metadata...'
    : editingEnabled
      ? 'Row editing enabled. Click a row to edit its fields.'
      : tableMetadata?.edit_reason || 'Row editing unavailable.';

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title flex items-center gap-2">
          <Database className="h-5 w-5 text-mcm-teal" />
          Postgres Explorer
        </h1>
        <p className="page-subtitle">
          Introspect database schemas, query tables directly, and edit primary-key-backed rows.
        </p>
      </div>

      <div className="mcm-panel p-4 sm:p-5">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-[220px_300px_160px_1fr_auto_auto] lg:items-end">
          <div className="space-y-2">
            <label htmlFor="postgres-schema">Schema</label>
            <select
              id="postgres-schema"
              value={selectedSchema}
              onChange={(e) => handleSchemaChange(e.target.value)}
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
              onChange={(e) => handleTableChange(e.target.value)}
              disabled={tablesLoading || tables.length === 0}
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

          <div className="flex min-h-10 items-center gap-3 text-xs text-muted-foreground">
            <Badge variant={editingEnabled ? 'default' : 'secondary'} className="font-mono">
              {editingEnabled ? 'Editable' : 'Read Only'}
            </Badge>
            {queryFilters.length ? (
              <Badge variant="outline" className="font-mono">
                {queryFilters.length} filter{queryFilters.length === 1 ? '' : 's'}
              </Badge>
            ) : null}
            <span className="font-mono">{editCapabilityLabel}</span>
          </div>

          <Button
            onClick={() => void fetchData()}
            disabled={loading || purging || !selectedTable || tablesLoading || tableMetadataLoading}
            className="h-10 gap-2 px-6"
          >
            {loading ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <TableIcon className="h-4 w-4" />
            )}
            {loading ? 'Querying...' : 'Query Table'}
          </Button>

          <Button
            onClick={() => void purgeData()}
            disabled={loading || purging || !selectedTable || tablesLoading || tableMetadataLoading}
            variant="destructive"
            className="h-10 gap-2 px-6"
          >
            {purging ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Trash2 className="h-4 w-4" />
            )}
            {purging ? 'Purging...' : 'Purge Table'}
          </Button>
        </div>

        <div className="mt-4 rounded-lg border border-border/60 bg-muted/20 p-4">
          <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
            <div className="space-y-1">
              <div className="flex items-center gap-2 font-mono text-xs uppercase tracking-[0.2em] text-muted-foreground">
                <FilterIcon className="h-3.5 w-3.5" />
                Query Filters
              </div>
              <p className="font-mono text-xs text-muted-foreground">
                Filters are combined with AND on the database before the row limit is applied.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button
                type="button"
                variant="outline"
                onClick={addQueryFilter}
                disabled={tableMetadataLoading || !tableMetadata?.columns.length}
                className="h-9 gap-2 px-3"
              >
                <Plus className="h-4 w-4" />
                Add Filter
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={clearQueryFilters}
                disabled={queryFilters.length === 0}
                className="h-9 px-3"
              >
                Clear Filters
              </Button>
            </div>
          </div>

          {queryFilters.length === 0 ? (
            <p className="mt-4 font-mono text-xs text-muted-foreground">
              No filters applied. Querying returns the first {limit} rows from the selected table.
            </p>
          ) : (
            <div className="mt-4 space-y-3">
              {queryFilters.map((filter, index) => {
                const column = columnsByName.get(filter.columnName) || tableMetadata?.columns[0];
                const operatorOptions = getFilterOperatorOptions(column?.data_type || '');
                const valueRequired = queryFilterOperatorNeedsValue(filter.operator);
                const columnId = `postgres-filter-column-${filter.id}`;
                const operatorId = `postgres-filter-operator-${filter.id}`;
                const valueId = `postgres-filter-value-${filter.id}`;

                return (
                  <div
                    key={filter.id}
                    className="grid gap-3 rounded-lg border border-border/60 bg-background/80 p-3 md:grid-cols-[minmax(0,1fr)_220px_minmax(0,1fr)_auto]"
                  >
                    <div className="space-y-1">
                      <label
                        htmlFor={columnId}
                        className="font-mono text-[11px] uppercase tracking-wide text-muted-foreground"
                      >
                        Column {index + 1}
                      </label>
                      <select
                        id={columnId}
                        value={filter.columnName}
                        onChange={(event) => updateQueryFilterColumn(filter.id, event.target.value)}
                        className={controlClass}
                      >
                        {(tableMetadata?.columns || []).map((item) => (
                          <option key={item.name} value={item.name}>
                            {item.name}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div className="space-y-1">
                      <label
                        htmlFor={operatorId}
                        className="font-mono text-[11px] uppercase tracking-wide text-muted-foreground"
                      >
                        Operator
                      </label>
                      <select
                        id={operatorId}
                        value={filter.operator}
                        onChange={(event) =>
                          updateQueryFilterOperator(
                            filter.id,
                            event.target.value as QueryFilterOperator
                          )
                        }
                        className={controlClass}
                      >
                        {operatorOptions.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div className="space-y-1">
                      <label
                        htmlFor={valueId}
                        className="font-mono text-[11px] uppercase tracking-wide text-muted-foreground"
                      >
                        Value
                      </label>
                      {valueRequired ? (
                        <Input
                          id={valueId}
                          value={filter.value}
                          onChange={(event) =>
                            updateQueryFilterValue(filter.id, event.target.value)
                          }
                          className="font-mono text-sm"
                          placeholder={column?.data_type ? `${column.data_type}` : 'Value'}
                        />
                      ) : (
                        <div className={`${controlClass} flex items-center text-muted-foreground`}>
                          No value required
                        </div>
                      )}
                    </div>

                    <div className="flex items-end">
                      <Button
                        type="button"
                        variant="outline"
                        onClick={() => removeQueryFilter(filter.id)}
                        className="h-10 px-3"
                        aria-label={`Remove filter ${index + 1}`}
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
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
          onRowClick={editingEnabled ? (row) => openEditor(row) : undefined}
          enableColumnSorting
        />
        <div className="mt-2 flex items-center justify-between gap-3 text-right font-mono text-xs text-muted-foreground">
          <span>
            {data.length > 0 && editingEnabled ? (
              <span className="inline-flex items-center gap-1">
                <Pencil className="h-3.5 w-3.5" />
                Click a row to edit it.
              </span>
            ) : (
              editCapabilityLabel
            )}
          </span>
          <span>{data.length > 0 ? `Showing ${data.length} rows.` : 'Ready.'}</span>
        </div>
      </div>

      <Dialog
        open={Boolean(editState)}
        onOpenChange={(open) => (!open ? closeEditor() : undefined)}
      >
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <DialogTitle>Edit Row</DialogTitle>
            <DialogDescription>
              Editing {selectedSchema}.{selectedTable}. Primary-key columns identify the row being
              updated.
            </DialogDescription>
          </DialogHeader>

          <div className="max-h-[70vh] overflow-y-auto pr-2">
            <div className="mb-4 rounded-lg border border-border/60 bg-muted/30 p-3 text-xs">
              <div className="font-mono uppercase tracking-wide text-muted-foreground">
                Row Match
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {(tableMetadata?.primary_key || []).map((columnName) => (
                  <Badge key={columnName} variant="outline" className="font-mono">
                    {columnName}={normalizeFieldValue(editState?.match[columnName]) || 'null'}
                  </Badge>
                ))}
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              {(tableMetadata?.columns || []).map((column) => {
                const field = editState?.fields[column.name] || { raw: '', isNull: false };
                const disabled = rowSaving || !column.editable;
                const useTextarea = shouldUseTextarea(column, field.raw);
                const fieldId = `postgres-edit-${column.name}`;

                return (
                  <div
                    key={column.name}
                    className="space-y-2 rounded-lg border border-border/60 p-3"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <label htmlFor={fieldId} className="space-y-1">
                        <div className="font-mono text-xs uppercase tracking-wide text-foreground">
                          {column.name}
                        </div>
                        <div className="font-mono text-[11px] text-muted-foreground">
                          {column.data_type}
                        </div>
                      </label>
                      <div className="flex flex-wrap justify-end gap-2">
                        {column.primary_key ? (
                          <Badge variant="default" className="font-mono text-[10px]">
                            PK
                          </Badge>
                        ) : null}
                        {!column.editable ? (
                          <Badge variant="secondary" className="font-mono text-[10px]">
                            Read Only
                          </Badge>
                        ) : null}
                        {column.nullable ? (
                          <Badge variant="outline" className="font-mono text-[10px]">
                            Nullable
                          </Badge>
                        ) : (
                          <Badge variant="outline" className="font-mono text-[10px]">
                            Required
                          </Badge>
                        )}
                      </div>
                    </div>

                    {useTextarea ? (
                      <Textarea
                        id={fieldId}
                        value={field.raw}
                        onChange={(event) => updateFieldRaw(column.name, event.target.value)}
                        disabled={disabled || field.isNull}
                        rows={6}
                        className="font-mono text-xs"
                      />
                    ) : (
                      <Input
                        id={fieldId}
                        value={field.raw}
                        onChange={(event) => updateFieldRaw(column.name, event.target.value)}
                        disabled={disabled || field.isNull}
                        className="font-mono text-xs"
                      />
                    )}

                    <div className="flex items-center justify-between gap-3">
                      <span className="text-xs text-muted-foreground">
                        {column.edit_reason || 'Editable field'}
                      </span>
                      <label className="inline-flex items-center gap-2 text-xs text-muted-foreground">
                        <Switch
                          checked={field.isNull}
                          onCheckedChange={(checked) =>
                            updateFieldNull(column.name, Boolean(checked))
                          }
                          disabled={disabled || !column.nullable}
                        />
                        NULL
                      </label>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={closeEditor} disabled={rowSaving}>
              Cancel
            </Button>
            <Button
              onClick={() => void saveRow()}
              disabled={rowSaving || !editState}
              className="gap-2"
            >
              {rowSaving ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Save className="h-4 w-4" />
              )}
              {rowSaving ? 'Saving...' : 'Save Row'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
