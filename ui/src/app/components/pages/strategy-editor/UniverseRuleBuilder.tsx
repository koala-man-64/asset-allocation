import { useEffect, useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Eye, Plus, Trash2 } from 'lucide-react';

import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Label } from '@/app/components/ui/label';
import { Textarea } from '@/app/components/ui/textarea';
import { cn } from '@/app/components/ui/utils';
import { strategyApi } from '@/services/strategyApi';
import type {
  UniverseCatalogColumn,
  UniverseCatalogResponse,
  UniverseCondition,
  UniverseConditionOperator,
  UniverseDefinition,
  UniverseGroup,
  UniverseNode,
  UniversePreviewResponse,
  UniverseValueKind
} from '@/types/strategy';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

import {
  buildEmptyUniverseCondition,
  buildEmptyUniverseGroup,
  cloneUniverse,
  collectUniverseTables,
  coerceDraftValue,
  countUniverseConditions,
  formatUniverseOperator,
  isMultiValueOperator,
  isNullOperator,
  isUniverseGroup
} from './universeUtils';

type NodePath = number[];

const selectClassName =
  'h-10 w-full rounded-lg border border-border bg-background px-3 text-sm text-foreground shadow-sm outline-none transition-[color,box-shadow] focus-visible:border-ring focus-visible:ring-2 focus-visible:ring-ring/20 disabled:cursor-not-allowed disabled:opacity-50';

const sectionClassName = 'rounded-xl border border-border/60 bg-card shadow-sm';
const surfaceClassName = 'rounded-lg border border-border/60 bg-background p-4';
const mutedSurfaceClassName = 'rounded-lg border border-border/60 bg-muted/20 p-4';

interface UniverseRuleBuilderProps {
  value: UniverseDefinition;
  onChange: (nextValue: UniverseDefinition) => void;
}

function getNodeAtPath(root: UniverseGroup, path: NodePath): UniverseNode {
  let current: UniverseNode = root;
  for (const index of path) {
    if (!isUniverseGroup(current) || !current.clauses[index]) {
      throw new Error(`Invalid universe path: ${path.join('.')}`);
    }
    current = current.clauses[index];
  }
  return current;
}

function updateNodeAtPath(
  universe: UniverseDefinition,
  path: NodePath,
  updater: (node: UniverseNode) => UniverseNode
): UniverseDefinition {
  const nextUniverse = cloneUniverse(universe);
  if (path.length === 0) {
    nextUniverse.root = updater(nextUniverse.root) as UniverseGroup;
    return nextUniverse;
  }

  const parentPath = path.slice(0, -1);
  const nodeIndex = path[path.length - 1];
  const parentNode = getNodeAtPath(nextUniverse.root, parentPath);
  if (!isUniverseGroup(parentNode)) {
    throw new Error(`Universe path does not resolve to a group: ${parentPath.join('.')}`);
  }
  parentNode.clauses[nodeIndex] = updater(parentNode.clauses[nodeIndex]);
  return nextUniverse;
}

function addClauseAtPath(
  universe: UniverseDefinition,
  path: NodePath,
  clause: UniverseNode
): UniverseDefinition {
  return updateNodeAtPath(universe, path, (node) => {
    if (!isUniverseGroup(node)) {
      throw new Error(`Cannot add a clause to a condition node: ${path.join('.')}`);
    }
    return {
      ...node,
      clauses: [...node.clauses, clause]
    };
  });
}

function removeNodeAtPath(universe: UniverseDefinition, path: NodePath): UniverseDefinition {
  if (path.length === 0) {
    return universe;
  }
  const nextUniverse = cloneUniverse(universe);
  const parentPath = path.slice(0, -1);
  const nodeIndex = path[path.length - 1];
  const parentNode = getNodeAtPath(nextUniverse.root, parentPath);
  if (!isUniverseGroup(parentNode)) {
    throw new Error(`Universe path does not resolve to a group: ${parentPath.join('.')}`);
  }
  parentNode.clauses.splice(nodeIndex, 1);
  return nextUniverse;
}

function getCatalogTable(catalog: UniverseCatalogResponse | undefined, tableName: string) {
  return catalog?.tables.find((table) => table.name === tableName) || null;
}

function getCatalogColumn(
  catalog: UniverseCatalogResponse | undefined,
  tableName: string,
  columnName: string
): UniverseCatalogColumn | null {
  const table = getCatalogTable(catalog, tableName);
  return table?.columns.find((column) => column.name === columnName) || null;
}

function toMultiValueText(values: UniverseCondition['values']): string {
  if (!values?.length) return '';
  return values.map((value) => String(value)).join('\n');
}

function parseMultiValueText(raw: string, kind: UniverseValueKind) {
  return raw
    .split(/[\n,]/)
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => coerceDraftValue(item, kind));
}

export function UniverseRuleBuilder({ value, onChange }: UniverseRuleBuilderProps) {
  const [preview, setPreview] = useState<UniversePreviewResponse | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const conditionCount = countUniverseConditions(value.root);
  const tableCount = collectUniverseTables(value.root).length;

  const catalogQuery = useQuery({
    queryKey: ['strategies', 'universe-catalog'],
    queryFn: () => strategyApi.getUniverseCatalog()
  });

  const previewMutation = useMutation({
    mutationFn: (universe: UniverseDefinition) =>
      strategyApi.previewUniverse({
        universe,
        sampleLimit: 12
      }),
    onSuccess: (response) => {
      setPreview(response);
      setPreviewError(null);
    },
    onError: (error) => {
      setPreview(null);
      setPreviewError(formatSystemStatusText(error) || 'Unable to preview the current universe.');
    }
  });

  useEffect(() => {
    setPreview(null);
    setPreviewError(null);
  }, [value]);

  useEffect(() => {
    const catalog = catalogQuery.data;
    if (!catalog?.tables.length) return;
    if (value.root.clauses.length !== 1) return;
    const firstClause = value.root.clauses[0];
    if (isUniverseGroup(firstClause)) return;
    if (
      firstClause.table ||
      firstClause.column ||
      firstClause.value !== undefined ||
      firstClause.values?.length
    ) {
      return;
    }

    const firstTable = catalog.tables[0];
    const firstColumn = firstTable.columns[0];
    if (!firstColumn) return;

    onChange({
      ...value,
      root: {
        ...value.root,
        clauses: [
          {
            ...firstClause,
            table: firstTable.name,
            column: firstColumn.name,
            operator: firstColumn.operators[0] || 'eq'
          }
        ]
      }
    });
  }, [catalogQuery.data, onChange, value]);

  const updateUniverse = (nextUniverse: UniverseDefinition) => {
    onChange({
      source: 'postgres_gold',
      root: nextUniverse.root
    });
  };

  const setGroupOperator = (path: NodePath, operator: 'and' | 'or') => {
    updateUniverse(
      updateNodeAtPath(value, path, (node) => {
        if (!isUniverseGroup(node)) return node;
        return { ...node, operator };
      })
    );
  };

  const addCondition = (path: NodePath) => {
    updateUniverse(addClauseAtPath(value, path, buildEmptyUniverseCondition()));
  };

  const addGroup = (path: NodePath) => {
    updateUniverse(addClauseAtPath(value, path, buildEmptyUniverseGroup()));
  };

  const removeNode = (path: NodePath) => {
    updateUniverse(removeNodeAtPath(value, path));
  };

  const updateCondition = (
    path: NodePath,
    updater: (condition: UniverseCondition) => UniverseCondition
  ) => {
    updateUniverse(
      updateNodeAtPath(value, path, (node) => {
        if (isUniverseGroup(node)) return node;
        return updater(node);
      })
    );
  };

  const handleTableChange = (path: NodePath, tableName: string) => {
    const table = getCatalogTable(catalogQuery.data, tableName);
    const firstColumn = table?.columns[0] || null;
    updateCondition(path, (condition) => ({
      ...condition,
      table: tableName,
      column: firstColumn?.name || '',
      operator: (firstColumn?.operators[0] || 'eq') as UniverseConditionOperator,
      value: undefined,
      values: undefined
    }));
  };

  const handleColumnChange = (path: NodePath, columnName: string) => {
    const current = getNodeAtPath(value.root, path);
    if (isUniverseGroup(current)) return;
    const column = getCatalogColumn(catalogQuery.data, current.table, columnName);
    updateCondition(path, (condition) => ({
      ...condition,
      column: columnName,
      operator:
        (column?.operators.includes(condition.operator)
          ? condition.operator
          : column?.operators[0]) || 'eq',
      value: undefined,
      values: undefined
    }));
  };

  const handleOperatorChange = (path: NodePath, operator: UniverseConditionOperator) => {
    updateCondition(path, (condition) => ({
      ...condition,
      operator,
      value:
        isNullOperator(operator) || isMultiValueOperator(operator) ? undefined : condition.value,
      values: isMultiValueOperator(operator) ? condition.values : undefined
    }));
  };

  const handleSingleValueChange = (path: NodePath, rawValue: string, kind: UniverseValueKind) => {
    updateCondition(path, (condition) => ({
      ...condition,
      value: rawValue === '' ? undefined : coerceDraftValue(rawValue, kind),
      values: undefined
    }));
  };

  const handleBooleanValueChange = (path: NodePath, rawValue: string) => {
    updateCondition(path, (condition) => ({
      ...condition,
      value: rawValue === 'true',
      values: undefined
    }));
  };

  const handleMultiValueChange = (path: NodePath, rawValue: string, kind: UniverseValueKind) => {
    updateCondition(path, (condition) => ({
      ...condition,
      value: undefined,
      values: parseMultiValueText(rawValue, kind)
    }));
  };

  const previewUniverse = () => {
    previewMutation.mutate(value);
  };

  const renderConditionValueEditor = (condition: UniverseCondition, path: NodePath) => {
    const column = getCatalogColumn(catalogQuery.data, condition.table, condition.column);
    const valueKind = column?.valueKind || 'string';

    if (isNullOperator(condition.operator)) {
      return (
        <div className="rounded-lg border border-dashed border-border/70 bg-muted/15 px-4 py-3 text-xs text-muted-foreground">
          This operator does not require a value.
        </div>
      );
    }

    if (isMultiValueOperator(condition.operator)) {
      return (
        <div className="grid gap-2">
          <Label htmlFor={`universe-values-${path.join('-')}`}>Values</Label>
          <Textarea
            id={`universe-values-${path.join('-')}`}
            value={toMultiValueText(condition.values)}
            onChange={(event) => handleMultiValueChange(path, event.target.value, valueKind)}
            placeholder="Enter one value per line or use commas."
          />
        </div>
      );
    }

    if (valueKind === 'boolean') {
      return (
        <div className="grid gap-2">
          <Label htmlFor={`universe-value-${path.join('-')}`}>Value</Label>
          <select
            id={`universe-value-${path.join('-')}`}
            className={selectClassName}
            value={String(Boolean(condition.value))}
            onChange={(event) => handleBooleanValueChange(path, event.target.value)}
          >
            <option value="true">True</option>
            <option value="false">False</option>
          </select>
        </div>
      );
    }

    const type =
      valueKind === 'number'
        ? 'number'
        : valueKind === 'date'
          ? 'date'
          : valueKind === 'datetime'
            ? 'datetime-local'
            : 'text';

    return (
      <div className="grid gap-2">
        <Label htmlFor={`universe-value-${path.join('-')}`}>Value</Label>
        <Input
          id={`universe-value-${path.join('-')}`}
          type={type}
          value={condition.value === undefined ? '' : String(condition.value)}
          onChange={(event) => handleSingleValueChange(path, event.target.value, valueKind)}
          step={valueKind === 'number' ? 'any' : undefined}
        />
      </div>
    );
  };

  const renderNode = (node: UniverseNode, path: NodePath = [], depth: number = 0) => {
    if (!isUniverseGroup(node)) {
      const table = getCatalogTable(catalogQuery.data, node.table);
      const selectedColumn = getCatalogColumn(catalogQuery.data, node.table, node.column);
      const availableOperators = selectedColumn?.operators || [];

      return (
        <div key={path.join('-') || 'condition-root'} className={surfaceClassName}>
          <div className="flex items-start justify-between gap-3">
            <div className="space-y-1">
              <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                Condition
              </div>
              <div className="text-sm text-muted-foreground">
                {node.table && node.column
                  ? `${node.table}.${node.column}`
                  : 'Select a table, column, and operator to define this rule.'}
              </div>
              {selectedColumn ? (
                <div className="text-xs text-muted-foreground">
                  Data type:{' '}
                  <span className="font-medium text-foreground">{selectedColumn.dataType}</span>
                </div>
              ) : null}
            </div>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              onClick={() => removeNode(path)}
              aria-label={`Remove universe condition ${path.join('.') || 'root'}`}
            >
              <Trash2 className="h-4 w-4" />
              Remove
            </Button>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <div className="grid gap-2">
              <Label htmlFor={`universe-table-${path.join('-')}`}>Gold Table</Label>
              <select
                id={`universe-table-${path.join('-')}`}
                className={selectClassName}
                value={node.table}
                onChange={(event) => handleTableChange(path, event.target.value)}
                disabled={catalogQuery.isLoading || !catalogQuery.data?.tables.length}
              >
                <option value="">Select a table</option>
                {(catalogQuery.data?.tables || []).map((item) => (
                  <option key={item.name} value={item.name}>
                    {item.name}
                  </option>
                ))}
              </select>
            </div>

            <div className="grid gap-2">
              <Label htmlFor={`universe-column-${path.join('-')}`}>Column</Label>
              <select
                id={`universe-column-${path.join('-')}`}
                className={selectClassName}
                value={node.column}
                onChange={(event) => handleColumnChange(path, event.target.value)}
                disabled={!table}
              >
                <option value="">Select a column</option>
                {(table?.columns || []).map((column) => (
                  <option key={column.name} value={column.name}>
                    {column.name}
                  </option>
                ))}
              </select>
            </div>

            <div className="grid gap-2">
              <Label htmlFor={`universe-operator-${path.join('-')}`}>Operator</Label>
              <select
                id={`universe-operator-${path.join('-')}`}
                className={selectClassName}
                value={node.operator}
                onChange={(event) =>
                  handleOperatorChange(path, event.target.value as UniverseConditionOperator)
                }
                disabled={!selectedColumn}
              >
                <option value="">Select an operator</option>
                {availableOperators.map((operator) => (
                  <option key={operator} value={operator}>
                    {operator}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {renderConditionValueEditor(node, path)}
        </div>
      );
    }

    return (
      <div
        key={path.join('-') || 'group-root'}
        className={cn(
          'space-y-4 rounded-xl border border-border/60 p-4',
          depth === 0 ? 'bg-muted/20' : 'bg-background'
        )}
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="space-y-1">
            <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
              {path.length === 0 ? 'Root Group' : 'Nested Group'}
            </div>
            <div className="text-sm font-medium text-foreground">
              {formatUniverseOperator(node.operator)}
            </div>
            <div className="text-xs text-muted-foreground">{node.clauses.length} clauses</div>
          </div>
          <div className="flex flex-wrap gap-2">
            <select
              aria-label={`Universe group operator ${path.join('.') || 'root'}`}
              className={cn(selectClassName, 'h-9 w-auto min-w-[140px]')}
              value={node.operator}
              onChange={(event) => setGroupOperator(path, event.target.value as 'and' | 'or')}
            >
              <option value="and">Match all</option>
              <option value="or">Match any</option>
            </select>
            <Button
              type="button"
              variant="secondary"
              size="sm"
              onClick={() => addCondition(path)}
              aria-label={`Add condition to universe group ${path.join('.') || 'root'}`}
            >
              <Plus className="h-4 w-4" />
              Add Condition
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => addGroup(path)}
              aria-label={`Add group to universe group ${path.join('.') || 'root'}`}
            >
              <Plus className="h-4 w-4" />
              Add Group
            </Button>
            {path.length > 0 ? (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => removeNode(path)}
                aria-label={`Remove universe group ${path.join('.')}`}
              >
                <Trash2 className="h-4 w-4" />
                Remove Group
              </Button>
            ) : null}
          </div>
        </div>

        <div className="space-y-3">
          {node.clauses.length === 0 ? (
            <div className="rounded-lg border border-dashed border-border/70 bg-muted/15 px-4 py-3 text-sm text-muted-foreground">
              This group has no clauses yet.
            </div>
          ) : (
            node.clauses.map((clause, index) => renderNode(clause, [...path, index], depth + 1))
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-5">
      <div className={sectionClassName}>
        <div className="flex flex-col gap-4 border-b border-border/60 px-5 py-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h4 className="text-base font-semibold text-foreground">Universe Definition</h4>
            <p className="text-sm text-muted-foreground">
              Define the rule tree that determines symbol eligibility from Postgres gold data.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Badge variant="outline">Source: Postgres Gold</Badge>
            <Badge variant="outline">{conditionCount} conditions</Badge>
            <Badge variant="outline">{tableCount} tables</Badge>
          </div>
        </div>

        {catalogQuery.isLoading ? (
          <p className="px-5 pt-5 text-sm text-muted-foreground">
            Loading gold tables and columns...
          </p>
        ) : null}
        {catalogQuery.error ? (
          <div className="mx-5 mt-5 rounded-lg border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
            {formatSystemStatusText(catalogQuery.error) || 'Failed to load universe catalog.'}
          </div>
        ) : null}

        <div className="px-5 py-5">{renderNode(value.root)}</div>
      </div>

      <div className={sectionClassName}>
        <div className="flex flex-col gap-4 border-b border-border/60 px-5 py-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h4 className="text-base font-semibold text-foreground">Universe Preview</h4>
            <p className="text-sm text-muted-foreground">
              Preview the symbols that match the current rule set using the latest available gold
              rows.
            </p>
          </div>
          <Button
            type="button"
            variant="secondary"
            onClick={previewUniverse}
            disabled={catalogQuery.isLoading || previewMutation.isPending}
          >
            <Eye className="h-4 w-4" />
            {previewMutation.isPending ? 'Previewing...' : 'Preview Universe'}
          </Button>
        </div>

        <div className="space-y-4 px-5 py-5">
          {previewError ? (
            <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
              {previewError}
            </div>
          ) : null}

          {preview ? (
            <div className="space-y-4">
              <div className="grid gap-3 md:grid-cols-3">
                <div className={mutedSurfaceClassName}>
                  <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                    Matching universe
                  </div>
                  <div className="mt-2 font-mono text-lg font-semibold text-foreground">
                    {preview.symbolCount}
                  </div>
                </div>
                <div className={mutedSurfaceClassName}>
                  <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                    Tables used
                  </div>
                  <div className="mt-2 font-mono text-lg font-semibold text-foreground">
                    {preview.tablesUsed.length}
                  </div>
                </div>
                <div className={mutedSurfaceClassName}>
                  <div className="text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                    Sample returned
                  </div>
                  <div className="mt-2 font-mono text-lg font-semibold text-foreground">
                    {preview.sampleSymbols.length}
                  </div>
                </div>
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <Badge variant="outline">{preview.symbolCount} symbols</Badge>
                {preview.tablesUsed.map((tableName) => (
                  <Badge key={tableName} variant="outline">
                    {tableName}
                  </Badge>
                ))}
              </div>

              {preview.sampleSymbols.length ? (
                <div className={mutedSurfaceClassName}>
                  <div className="mb-3 text-xs font-medium uppercase tracking-[0.08em] text-muted-foreground">
                    Sample symbols
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {preview.sampleSymbols.map((symbol) => (
                      <Badge key={symbol} variant="outline" className="font-mono">
                        {symbol}
                      </Badge>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="text-sm text-muted-foreground">
                  No symbols matched the current universe.
                </div>
              )}

              {preview.warnings.length ? (
                <div className="rounded-lg border border-border/70 bg-muted/20 p-3 text-sm text-muted-foreground">
                  {preview.warnings.join(' ')}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="rounded-lg border border-dashed border-border/70 bg-muted/15 p-4 text-sm text-muted-foreground">
              Run a preview to inspect the current symbol count and table coverage before saving.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
