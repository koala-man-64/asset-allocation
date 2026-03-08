import type {
  UniverseCondition,
  UniverseDefinition,
  UniverseGroup,
  UniverseNode,
  UniverseValue,
  UniverseValueKind
} from '@/types/strategy';

export function buildEmptyUniverseCondition(): UniverseCondition {
  return {
    kind: 'condition',
    table: '',
    column: '',
    operator: 'eq'
  };
}

export function buildEmptyUniverseGroup(operator: 'and' | 'or' = 'and'): UniverseGroup {
  return {
    kind: 'group',
    operator,
    clauses: [buildEmptyUniverseCondition()]
  };
}

export function buildEmptyUniverse(): UniverseDefinition {
  return {
    source: 'postgres_gold',
    root: buildEmptyUniverseGroup()
  };
}

export function cloneUniverse(universe: UniverseDefinition): UniverseDefinition {
  return JSON.parse(JSON.stringify(universe)) as UniverseDefinition;
}

export function isUniverseGroup(node: UniverseNode): node is UniverseGroup {
  return node.kind === 'group';
}

export function isMultiValueOperator(operator: string): boolean {
  return operator === 'in' || operator === 'not_in';
}

export function isNullOperator(operator: string): boolean {
  return operator === 'is_null' || operator === 'is_not_null';
}

export function countUniverseConditions(node: UniverseNode): number {
  if (!isUniverseGroup(node)) return 1;
  return node.clauses.reduce((sum, clause) => sum + countUniverseConditions(clause), 0);
}

export function collectUniverseTables(node: UniverseNode): string[] {
  const tables = new Set<string>();
  collectUniverseTablesInto(node, tables);
  return Array.from(tables).sort();
}

function collectUniverseTablesInto(node: UniverseNode, tables: Set<string>): void {
  if (!isUniverseGroup(node)) {
    if (node.table) tables.add(node.table);
    return;
  }
  node.clauses.forEach((clause) => collectUniverseTablesInto(clause, tables));
}

export function summarizeUniverse(universe: UniverseDefinition): string {
  const conditionCount = countUniverseConditions(universe.root);
  const tableCount = collectUniverseTables(universe.root).length;
  const conditionLabel = conditionCount === 1 ? 'condition' : 'conditions';
  const tableLabel = tableCount === 1 ? 'table' : 'tables';
  return `${conditionCount} ${conditionLabel} across ${tableCount} ${tableLabel}`;
}

export function formatUniverseOperator(operator: 'and' | 'or'): string {
  return operator === 'and' ? 'Match all' : 'Match any';
}

export function coerceDraftValue(raw: string, kind: UniverseValueKind): UniverseValue | string {
  const trimmed = raw.trim();
  if (kind === 'number') {
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : raw;
  }
  if (kind === 'boolean') {
    const normalized = trimmed.toLowerCase();
    if (['true', '1', 'yes', 'y', 'on', 't'].includes(normalized)) return true;
    if (['false', '0', 'no', 'n', 'off', 'f'].includes(normalized)) return false;
  }
  return raw;
}
