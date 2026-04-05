import {
  PostgresService,
  type GoldColumnLookupRow,
  type PostgresTableMetadata
} from '@/services/PostgresService';
import type { DomainMetadata } from '@/types/strategy';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

export const MEDALLION_ORDER = ['bronze', 'silver', 'gold', 'platinum'] as const;

export type MedallionKey = (typeof MEDALLION_ORDER)[number];
export type LayerFilter = MedallionKey | 'all';

export type LayerVisual = {
  shellClassName: string;
  chipClassName: string;
  activeClassName: string;
  glowClassName: string;
};

export type TableCatalogSection = {
  layerKey: MedallionKey;
  schemaName: string;
  label: string;
  tables: string[];
};

export type TableCatalogResponse = {
  sections: TableCatalogSection[];
  warnings: string[];
};

export type DomainDescriptor = {
  key: string;
  label: string;
  description?: string;
  status?: string;
  metadata?: DomainMetadata;
  tokens: string[];
};

export type TableCatalogItem = {
  key: string;
  layerKey: MedallionKey;
  layerLabel: string;
  schemaName: string;
  tableName: string;
  domainKey: string | null;
  domainLabel: string | null;
  domainDescription?: string;
  domainStatus?: string;
  domainMetadata?: DomainMetadata;
};

export type LayerAtlasDomain = {
  key: string;
  label: string;
  description?: string;
  status?: string;
  metadata?: DomainMetadata;
  tableCount: number;
};

export type LayerAtlas = {
  key: MedallionKey;
  label: string;
  description: string;
  domains: LayerAtlasDomain[];
};

export type TableDetailState = {
  isLoading: boolean;
  data?: PostgresTableMetadata;
  goldLookupByColumn?: Record<string, GoldColumnLookupRow>;
  error?: string;
};

export type CatalogColumn = {
  name: string;
  data_type: string;
  description: string | null;
  nullable: boolean;
  primary_key: boolean;
  editable: boolean;
  status?: GoldColumnLookupRow['status'];
  calculationType?: string;
  calculationNotes?: string | null;
  descriptionSource: 'postgres' | 'gold-lookup' | 'none';
};

const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
const tableNameCollator = new Intl.Collator(undefined, {
  sensitivity: 'base',
  numeric: true
});

export const LAYER_VISUALS: Record<MedallionKey, LayerVisual> = {
  bronze: {
    shellClassName: 'border-mcm-walnut/25 bg-mcm-paper/80',
    chipClassName: 'border-mcm-walnut/40 bg-mcm-paper text-mcm-walnut',
    activeClassName: 'border-mcm-walnut bg-mcm-paper shadow-[0_0_0_2px_rgba(119,63,26,0.14)]',
    glowClassName: 'bg-mcm-walnut/10'
  },
  silver: {
    shellClassName: 'border-slate-400/40 bg-slate-100/70',
    chipClassName: 'border-slate-500/40 bg-slate-100 text-slate-700',
    activeClassName: 'border-slate-500 bg-slate-100 shadow-[0_0_0_2px_rgba(71,85,105,0.12)]',
    glowClassName: 'bg-slate-400/12'
  },
  gold: {
    shellClassName: 'border-mcm-mustard/40 bg-mcm-mustard/10',
    chipClassName: 'border-mcm-mustard/50 bg-mcm-mustard/15 text-mcm-walnut',
    activeClassName: 'border-mcm-mustard bg-mcm-mustard/15 shadow-[0_0_0_2px_rgba(225,173,1,0.16)]',
    glowClassName: 'bg-mcm-mustard/14'
  },
  platinum: {
    shellClassName: 'border-mcm-teal/40 bg-mcm-teal/10',
    chipClassName: 'border-mcm-teal/45 bg-mcm-teal/12 text-mcm-teal',
    activeClassName: 'border-mcm-teal bg-mcm-teal/14 shadow-[0_0_0_2px_rgba(0,128,128,0.14)]',
    glowClassName: 'bg-mcm-teal/14'
  }
};

export function normalizeKey(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[_\s/]+/g, '-')
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

export function normalizeDomainKey(value: string): string {
  const normalized = normalizeKey(value);
  return normalized === 'targets' ? 'price-target' : normalized;
}

export function toMedallionKey(value: string): MedallionKey | null {
  const normalized = normalizeKey(value);
  return MEDALLION_ORDER.includes(normalized as MedallionKey) ? (normalized as MedallionKey) : null;
}

export function titleCase(value: string): string {
  return value
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

export function formatInt(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return 'N/A';
  }
  return numberFormatter.format(value);
}

export function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return 'N/A';
  }
  if (value < 1024) {
    return `${value} B`;
  }
  const units = ['KB', 'MB', 'GB', 'TB'] as const;
  let size = value / 1024;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

export function formatDateLabel(value: string | null | undefined): string {
  if (!value) {
    return 'N/A';
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return value;
  }
  return dt.toISOString().slice(0, 10);
}

export function formatDateRangeLabel(metadata?: DomainMetadata): string {
  const min = metadata?.dateRange?.min;
  const max = metadata?.dateRange?.max;
  if (!min && !max) {
    return 'N/A';
  }
  const from = formatDateLabel(min);
  const to = formatDateLabel(max);
  if (min && max && from === to) {
    return from;
  }
  return `${from} to ${to}`;
}

export function buildDomainTokens(domainKey: string, label: string): string[] {
  const values = new Set<string>();
  const addValue = (value: string) => {
    const normalized = normalizeDomainKey(value);
    if (!normalized) {
      return;
    }
    values.add(normalized);
    values.add(normalized.replace(/-/g, ''));
  };

  addValue(domainKey);
  addValue(label);

  if (domainKey === 'price-target') {
    addValue('price target');
    addValue('price_target');
  }

  return Array.from(values).sort((left, right) => right.length - left.length);
}

export function inferDomainForTableName(
  tableName: string,
  layerDomains: DomainDescriptor[]
): DomainDescriptor | null {
  const normalizedTable = normalizeKey(tableName);
  const compactTable = normalizedTable.replace(/-/g, '');

  for (const domain of layerDomains) {
    const matched = domain.tokens.some((token) => {
      const compactToken = token.replace(/-/g, '');
      return normalizedTable.includes(token) || compactTable.includes(compactToken);
    });
    if (matched) {
      return domain;
    }
  }

  return null;
}

export function countDocumentedColumns(
  metadata: PostgresTableMetadata | undefined,
  goldLookupByColumn: Record<string, GoldColumnLookupRow> | undefined
): number {
  if (!metadata) {
    return 0;
  }

  return metadata.columns.reduce((count, column) => {
    const fallbackDescription = goldLookupByColumn?.[column.name]?.description;
    const description = (column.description || fallbackDescription || '').trim();
    return description ? count + 1 : count;
  }, 0);
}

export async function loadMedallionTableCatalog(): Promise<TableCatalogResponse> {
  const schemas = await PostgresService.listSchemas();
  const medallionSchemas = schemas
    .map((schemaName) => ({
      schemaName,
      layerKey: toMedallionKey(schemaName)
    }))
    .filter((entry): entry is { schemaName: string; layerKey: MedallionKey } =>
      Boolean(entry.layerKey)
    );

  if (medallionSchemas.length === 0) {
    return {
      sections: [],
      warnings: ['Postgres did not expose bronze, silver, gold, or platinum schemas.']
    };
  }

  const settled = await Promise.allSettled(
    medallionSchemas.map(async ({ schemaName, layerKey }) => ({
      schemaName,
      layerKey,
      tables: await PostgresService.listTables(schemaName)
    }))
  );

  const sections: TableCatalogSection[] = [];
  const warnings: string[] = [];

  settled.forEach((result, index) => {
    const target = medallionSchemas[index];
    if (result.status === 'fulfilled') {
      sections.push({
        layerKey: target.layerKey,
        schemaName: target.schemaName,
        label: titleCase(target.layerKey),
        tables: [...result.value.tables].sort(tableNameCollator.compare)
      });
      return;
    }

    warnings.push(
      `${titleCase(target.layerKey)} table catalog could not be loaded: ${formatSystemStatusText(result.reason)}`
    );
  });

  sections.sort(
    (left, right) =>
      MEDALLION_ORDER.indexOf(left.layerKey) - MEDALLION_ORDER.indexOf(right.layerKey)
  );

  if (sections.length === 0) {
    throw new Error(warnings[0] || 'Postgres table catalog is unavailable.');
  }

  return { sections, warnings };
}
