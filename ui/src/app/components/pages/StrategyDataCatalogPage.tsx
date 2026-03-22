import React, {
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  AlertTriangle,
  Database,
  Layers3,
  ListChecks,
  Loader2,
  ScanSearch,
  Search,
  Table2
} from 'lucide-react';

import { Alert, AlertDescription, AlertTitle } from '@/app/components/ui/alert';
import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
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
import { useSystemStatusViewQuery } from '@/hooks/useSystemStatusView';
import {
  PostgresService,
  type GoldColumnLookupRow,
  type PostgresColumnMetadata,
  type PostgresTableMetadata
} from '@/services/PostgresService';
import type { DomainMetadata } from '@/types/strategy';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

const MEDALLION_ORDER = ['bronze', 'silver', 'gold', 'platinum'] as const;

type MedallionKey = (typeof MEDALLION_ORDER)[number];
type LayerFilter = MedallionKey | 'all';

type LayerVisual = {
  shellClassName: string;
  chipClassName: string;
  activeClassName: string;
  glowClassName: string;
};

type TableCatalogSection = {
  layerKey: MedallionKey;
  schemaName: string;
  label: string;
  tables: string[];
};

type TableCatalogResponse = {
  sections: TableCatalogSection[];
  warnings: string[];
};

type DomainDescriptor = {
  key: string;
  label: string;
  description?: string;
  status?: string;
  metadata?: DomainMetadata;
  tokens: string[];
};

type TableCatalogItem = {
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

type LayerAtlasDomain = {
  key: string;
  label: string;
  description?: string;
  status?: string;
  metadata?: DomainMetadata;
  tableCount: number;
};

type LayerAtlas = {
  key: MedallionKey;
  label: string;
  description: string;
  domains: LayerAtlasDomain[];
};

type TableDetailState = {
  isLoading: boolean;
  data?: PostgresTableMetadata;
  goldLookupByColumn?: Record<string, GoldColumnLookupRow>;
  error?: string;
};

type CatalogColumn = PostgresColumnMetadata & {
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

const LAYER_VISUALS: Record<MedallionKey, LayerVisual> = {
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
    activeClassName:
      'border-mcm-mustard bg-mcm-mustard/15 shadow-[0_0_0_2px_rgba(225,173,1,0.16)]',
    glowClassName: 'bg-mcm-mustard/14'
  },
  platinum: {
    shellClassName: 'border-mcm-teal/40 bg-mcm-teal/10',
    chipClassName: 'border-mcm-teal/45 bg-mcm-teal/12 text-mcm-teal',
    activeClassName: 'border-mcm-teal bg-mcm-teal/14 shadow-[0_0_0_2px_rgba(0,128,128,0.14)]',
    glowClassName: 'bg-mcm-teal/14'
  }
};

function normalizeKey(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[_\s/]+/g, '-')
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function normalizeDomainKey(value: string): string {
  const normalized = normalizeKey(value);
  return normalized === 'targets' ? 'price-target' : normalized;
}

function toMedallionKey(value: string): MedallionKey | null {
  const normalized = normalizeKey(value);
  return MEDALLION_ORDER.includes(normalized as MedallionKey)
    ? (normalized as MedallionKey)
    : null;
}

function titleCase(value: string): string {
  return value
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function formatInt(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return 'N/A';
  }
  return numberFormatter.format(value);
}

function formatBytes(value: number | null | undefined): string {
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

function formatDateLabel(value: string | null | undefined): string {
  if (!value) {
    return 'N/A';
  }
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) {
    return value;
  }
  return dt.toISOString().slice(0, 10);
}

function formatDateRangeLabel(metadata?: DomainMetadata): string {
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

function buildDomainTokens(domainKey: string, label: string): string[] {
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

function inferDomainForTableName(
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

function countDocumentedColumns(
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

async function loadMedallionTableCatalog(): Promise<TableCatalogResponse> {
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
    (left, right) => MEDALLION_ORDER.indexOf(left.layerKey) - MEDALLION_ORDER.indexOf(right.layerKey)
  );

  if (sections.length === 0) {
    throw new Error(warnings[0] || 'Postgres table catalog is unavailable.');
  }

  return { sections, warnings };
}

function SummaryTile({
  label,
  value,
  note
}: {
  label: string;
  value: string;
  note: string;
}) {
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

export const StrategyDataCatalogPage: React.FC = () => {
  const {
    data: systemStatusView,
    isLoading: isStatusLoading,
    isFetching: isStatusFetching,
    error: statusError
  } = useSystemStatusViewQuery();

  const tableCatalogQuery = useQuery<TableCatalogResponse>({
    queryKey: ['strategyDataCatalog', 'medallionTableCatalog'],
    queryFn: loadMedallionTableCatalog,
    staleTime: 5 * 60 * 1000
  });

  const [selectedLayer, setSelectedLayer] = useState<LayerFilter>('all');
  const [selectedDomain, setSelectedDomain] = useState<string | null>(null);
  const [navigatorSearch, setNavigatorSearch] = useState('');
  const [columnSearch, setColumnSearch] = useState('');
  const [selectedTableKey, setSelectedTableKey] = useState<string | null>(null);
  const [tableDetailsByKey, setTableDetailsByKey] = useState<Record<string, TableDetailState>>({});
  const tableDetailsRef = useRef<Record<string, TableDetailState>>({});

  const deferredNavigatorSearch = useDeferredValue(navigatorSearch);
  const deferredColumnSearch = useDeferredValue(columnSearch);

  const layerDomainIndex = useMemo(() => {
    const snapshotEntries = systemStatusView?.metadataSnapshot.entries || {};
    const index = new Map<MedallionKey, DomainDescriptor[]>();

    for (const layer of systemStatusView?.systemHealth.dataLayers || []) {
      const layerKey = toMedallionKey(String(layer?.name || ''));
      if (!layerKey) {
        continue;
      }

      const descriptors: DomainDescriptor[] = [];
      for (const domain of layer.domains || []) {
        const label = String(domain?.name || '').trim();
        const key = normalizeDomainKey(label);
        if (!key) {
          continue;
        }

        descriptors.push({
          key,
          label,
          description: domain.description,
          status: domain.status,
          metadata: snapshotEntries[`${layerKey}/${key}`],
          tokens: buildDomainTokens(key, label)
        });
      }

      index.set(
        layerKey,
        descriptors.sort((left, right) => tableNameCollator.compare(left.label, right.label))
      );
    }

    return index;
  }, [systemStatusView?.metadataSnapshot.entries, systemStatusView?.systemHealth.dataLayers]);

  const tableCatalogItems = useMemo<TableCatalogItem[]>(() => {
    const items: TableCatalogItem[] = [];

    for (const section of tableCatalogQuery.data?.sections || []) {
      const layerDomains = layerDomainIndex.get(section.layerKey) || [];

      for (const tableName of section.tables) {
        const inferredDomain = inferDomainForTableName(tableName, layerDomains);
        items.push({
          key: `${section.schemaName}.${tableName}`,
          layerKey: section.layerKey,
          layerLabel: section.label,
          schemaName: section.schemaName,
          tableName,
          domainKey: inferredDomain?.key ?? null,
          domainLabel: inferredDomain?.label ?? null,
          domainDescription: inferredDomain?.description,
          domainStatus: inferredDomain?.status,
          domainMetadata: inferredDomain?.metadata
        });
      }
    }

    return items;
  }, [layerDomainIndex, tableCatalogQuery.data?.sections]);

  const atlasLayers = useMemo<LayerAtlas[]>(() => {
    const layersByKey = new Map<MedallionKey, LayerAtlas>();
    const dataLayers = systemStatusView?.systemHealth.dataLayers || [];

    for (const layer of dataLayers) {
      const layerKey = toMedallionKey(String(layer?.name || ''));
      if (!layerKey) {
        continue;
      }

      const domains: LayerAtlasDomain[] = [];
      for (const domain of layer.domains || []) {
        const key = normalizeDomainKey(String(domain?.name || ''));
        if (!key) {
          continue;
        }

        const metadata = systemStatusView?.metadataSnapshot.entries?.[`${layerKey}/${key}`];
        const tableCount = tableCatalogItems.filter(
          (item) => item.layerKey === layerKey && item.domainKey === key
        ).length;

        domains.push({
          key,
          label: String(domain?.name || '').trim(),
          description: domain.description,
          status: domain.status,
          metadata,
          tableCount
        });
      }

      domains.sort((left, right) => tableNameCollator.compare(left.label, right.label));

      layersByKey.set(layerKey, {
        key: layerKey,
        label: String(layer.name || '').trim() || titleCase(layerKey),
        description: String(layer.description || '').trim() || 'Layer metadata is available.',
        domains
      });
    }

    for (const section of tableCatalogQuery.data?.sections || []) {
      if (layersByKey.has(section.layerKey)) {
        continue;
      }
      layersByKey.set(section.layerKey, {
        key: section.layerKey,
        label: section.label,
        description: `${section.label} tables are available in Postgres, but the system-status layer feed did not publish domain details.`,
        domains: []
      });
    }

    return MEDALLION_ORDER.map((layerKey) => layersByKey.get(layerKey)).filter(
      (value): value is LayerAtlas => Boolean(value)
    );
  }, [
    systemStatusView?.metadataSnapshot.entries,
    systemStatusView?.systemHealth.dataLayers,
    tableCatalogItems,
    tableCatalogQuery.data?.sections
  ]);

  const filteredTables = useMemo(() => {
    const query = normalizeKey(deferredNavigatorSearch);

    return tableCatalogItems.filter((item) => {
      if (selectedLayer !== 'all' && item.layerKey !== selectedLayer) {
        return false;
      }
      if (selectedDomain && item.domainKey !== selectedDomain) {
        return false;
      }
      if (!query) {
        return true;
      }

      const haystack = [
        item.tableName,
        item.schemaName,
        item.layerLabel,
        item.domainLabel || '',
        item.domainDescription || '',
        tableDetailsByKey[item.key]?.data?.columns.map((column) => column.name).join(' ') || ''
      ]
        .join(' ')
        .toLowerCase();

      return haystack.includes(query.replace(/-/g, ' ')) || haystack.includes(query);
    });
  }, [deferredNavigatorSearch, selectedDomain, selectedLayer, tableCatalogItems, tableDetailsByKey]);

  const selectedTable = useMemo(
    () => filteredTables.find((item) => item.key === selectedTableKey) ?? filteredTables[0] ?? null,
    [filteredTables, selectedTableKey]
  );

  useEffect(() => {
    tableDetailsRef.current = tableDetailsByKey;
  }, [tableDetailsByKey]);

  const ensureTableDetails = useCallback(async (table: TableCatalogItem) => {
    const existing = tableDetailsRef.current[table.key];
    if (existing?.isLoading || existing?.data) {
      return;
    }

    setTableDetailsByKey((current) => {
      const currentEntry = current[table.key];
      if (currentEntry?.isLoading || currentEntry?.data) {
        return current;
      }

      return {
        ...current,
        [table.key]: {
          isLoading: true
        }
      };
    });

    try {
      const metadata = await PostgresService.getTableMetadata(table.schemaName, table.tableName);
      let goldLookupByColumn: Record<string, GoldColumnLookupRow> | undefined;

      if (table.layerKey === 'gold') {
        try {
          const lookupResponse = await PostgresService.listGoldColumnLookup({
            table: table.tableName,
            limit: 5000
          });
          goldLookupByColumn = Object.fromEntries(
            lookupResponse.rows.map((row) => [row.column, row] as const)
          );
        } catch {
          goldLookupByColumn = {};
        }
      }

      setTableDetailsByKey((current) => ({
        ...current,
        [table.key]: {
          isLoading: false,
          data: metadata,
          goldLookupByColumn
        }
      }));
    } catch (error) {
      setTableDetailsByKey((current) => ({
        ...current,
        [table.key]: {
          isLoading: false,
          error: formatSystemStatusText(error)
        }
      }));
    }
  }, []);

  useEffect(() => {
    if (!filteredTables.length) {
      setSelectedTableKey(null);
      return;
    }

    const stillVisible = filteredTables.some((item) => item.key === selectedTableKey);
    if (stillVisible) {
      return;
    }

    startTransition(() => {
      setSelectedTableKey(filteredTables[0].key);
    });
  }, [filteredTables, selectedTableKey]);

  useEffect(() => {
    if (!selectedTable) {
      return;
    }
    void ensureTableDetails(selectedTable);
  }, [ensureTableDetails, selectedTable]);

  useEffect(() => {
    setColumnSearch('');
  }, [selectedTableKey]);

  const selectedTableState = selectedTable ? tableDetailsByKey[selectedTable.key] : undefined;

  const selectedColumns = useMemo<CatalogColumn[]>(() => {
    if (!selectedTableState?.data) {
      return [];
    }

    const searchQuery = normalizeKey(deferredColumnSearch);
    const lookupByColumn = selectedTableState.goldLookupByColumn || {};

    return selectedTableState.data.columns
      .map((column, index) => {
        const lookup = lookupByColumn[column.name];
        const postgresDescription = (column.description || '').trim();
        const lookupDescription = (lookup?.description || '').trim();
        const descriptionSource: CatalogColumn['descriptionSource'] = postgresDescription
          ? 'postgres'
          : lookupDescription
            ? 'gold-lookup'
            : 'none';

        return {
          ...column,
          description: postgresDescription || lookupDescription || null,
          descriptionSource,
          status: lookup?.status,
          calculationType: lookup?.calculation_type,
          calculationNotes: lookup?.calculation_notes ?? null,
          _index: index
        };
      })
      .filter((column) => {
        if (!searchQuery) {
          return true;
        }

        const haystack = [
          column.name,
          column.data_type,
          column.description || '',
          column.calculationType || '',
          column.calculationNotes || ''
        ]
          .join(' ')
          .toLowerCase();

        return haystack.includes(searchQuery.replace(/-/g, ' ')) || haystack.includes(searchQuery);
      })
      .sort((left, right) => {
        if (left.primary_key !== right.primary_key) {
          return left.primary_key ? -1 : 1;
        }
        return left._index - right._index;
      })
      .map(({ _index, ...column }) => column);
  }, [deferredColumnSearch, selectedTableState]);

  const selectedTableDocumentedCount = countDocumentedColumns(
    selectedTableState?.data,
    selectedTableState?.goldLookupByColumn
  );

  const totalDomainCount = atlasLayers.reduce((count, layer) => count + layer.domains.length, 0);
  const totalTableCount = tableCatalogItems.length;
  const medallionCount = atlasLayers.length || tableCatalogQuery.data?.sections.length || 0;

  const statusErrorMessage = statusError ? formatSystemStatusText(statusError) : '';
  const tableCatalogErrorMessage = tableCatalogQuery.error
    ? formatSystemStatusText(tableCatalogQuery.error)
    : '';
  const tableCatalogWarnings = tableCatalogQuery.data?.warnings || [];

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Data Platform</p>
        <h1 className="page-title">Domain Atlas</h1>
        <p className="page-subtitle">
          A single editorial surface for the medallion stack: live domain coverage, inferred
          domain-to-table links, and per-column contracts with names, descriptions, and data types.
        </p>
      </div>

      <section className="relative overflow-hidden rounded-[2rem] border-2 border-mcm-walnut bg-mcm-paper px-6 py-6 shadow-[12px_12px_0px_0px_rgba(119,63,26,0.12)]">
        <div className="absolute inset-0 bg-[linear-gradient(145deg,rgba(119,63,26,0.08),transparent_36%),linear-gradient(315deg,rgba(0,128,128,0.08),transparent_42%),linear-gradient(0deg,rgba(225,173,1,0.08),transparent_55%)]" />
        <div className="absolute left-[12%] top-8 h-32 w-32 rounded-full bg-mcm-mustard/20 blur-3xl" />
        <div className="absolute bottom-2 right-[18%] h-36 w-36 rounded-full bg-mcm-teal/16 blur-3xl" />

        <div className="relative grid gap-6 xl:grid-cols-[minmax(0,1.3fr)_360px]">
          <div className="space-y-5">
            <Badge variant="outline" className="border-mcm-walnut/30 bg-mcm-paper/70">
              Data Dictionary Surface
            </Badge>

            <div className="max-w-3xl space-y-3">
              <h2 className="font-display text-[clamp(2rem,4vw,3.8rem)] font-black uppercase leading-[0.92] tracking-[0.04em] text-foreground">
                Medallions first. Domains second. Column contracts on demand.
              </h2>
              <p className="max-w-[64ch] text-base text-mcm-walnut/70">
                Domain cards are sourced from the live system-status snapshot. Table contracts come
                from Postgres metadata, with gold lookup annotations filling in richer column
                descriptions where they exist.
              </p>
            </div>

            <div className="grid gap-3 md:grid-cols-3">
              <SummaryTile
                label="Medallions"
                value={formatInt(medallionCount)}
                note="Live layers represented across the atlas."
              />
              <SummaryTile
                label="Domains"
                value={formatInt(totalDomainCount)}
                note="System-status domains with metadata and health context."
              />
              <SummaryTile
                label="Postgres Tables"
                value={formatInt(totalTableCount)}
                note="Queryable table contracts grouped by medallion schema."
              />
            </div>
          </div>

          <div className="rounded-[1.6rem] border border-mcm-walnut/15 bg-mcm-paper/80 p-5 shadow-[0_14px_32px_rgba(119,63,26,0.08)]">
            <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/60">
              <ListChecks className="h-4 w-4 text-mcm-teal" />
              Coverage Notes
            </div>
            <div className="mt-4 space-y-3 text-sm text-mcm-walnut/70">
              <p>
                Use the medallion strips to understand what the platform publishes. Use the table
                navigator to inspect the actual serving contracts that analysts and agents consume.
              </p>
              <p>
                When a column description is unavailable, the page shows the gap directly instead of
                inventing one. That makes missing metadata visible instead of burying it.
              </p>
              <div className="rounded-[1.2rem] border border-mcm-walnut/10 bg-mcm-cream/70 p-4">
                <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/60">
                  Current Feeds
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  <Badge variant="secondary">
                    {isStatusLoading || isStatusFetching ? 'Refreshing system snapshot' : 'System snapshot loaded'}
                  </Badge>
                  <Badge variant="secondary">
                    {tableCatalogQuery.isLoading ? 'Loading Postgres catalog' : 'Postgres catalog loaded'}
                  </Badge>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {statusErrorMessage ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>System metadata is unavailable</AlertTitle>
          <AlertDescription>{statusErrorMessage}</AlertDescription>
        </Alert>
      ) : null}

      {tableCatalogErrorMessage ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Postgres catalog is unavailable</AlertTitle>
          <AlertDescription>{tableCatalogErrorMessage}</AlertDescription>
        </Alert>
      ) : null}

      {tableCatalogWarnings.length > 0 ? (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Partial catalog coverage</AlertTitle>
          <AlertDescription>{tableCatalogWarnings.join(' ')}</AlertDescription>
        </Alert>
      ) : null}

      <section className="space-y-4">
        <div className="page-header-row">
          <div className="page-header">
            <p className="page-kicker">Domain Coverage</p>
            <h2 className="page-title">Medallion Strips</h2>
            <p className="page-subtitle">
              Click any domain tile to focus the atlas on that medallion/domain slice.
            </p>
          </div>
          {selectedDomain ? (
            <Button
              type="button"
              variant="outline"
              onClick={() => {
                startTransition(() => {
                  setSelectedDomain(null);
                  setSelectedLayer('all');
                });
              }}
            >
              Clear Domain Focus
            </Button>
          ) : null}
        </div>

        {isStatusLoading && !atlasLayers.length ? (
          <div className="grid gap-4 xl:grid-cols-4">
            {Array.from({ length: 4 }).map((_, index) => (
              <Skeleton key={index} className="h-[280px] rounded-[1.75rem]" />
            ))}
          </div>
        ) : atlasLayers.length === 0 ? (
          <div className="rounded-[1.6rem] border-2 border-dashed border-mcm-walnut/25 bg-mcm-paper/65 p-6 text-sm text-muted-foreground">
            System status did not publish any medallion domain metadata for this deployment.
          </div>
        ) : (
          <div className="grid gap-4 xl:grid-cols-4">
            {atlasLayers.map((layer) => {
              const visual = LAYER_VISUALS[layer.key];
              return (
                <section
                  key={layer.key}
                  className={`relative overflow-hidden rounded-[1.8rem] border px-4 py-4 shadow-[0_18px_38px_rgba(119,63,26,0.08)] ${visual.shellClassName}`}
                >
                  <div className={`absolute right-4 top-4 h-16 w-16 rounded-full blur-2xl ${visual.glowClassName}`} />
                  <div className="relative space-y-4">
                    <div className="space-y-2">
                      <Badge variant="outline" className={visual.chipClassName}>
                        {layer.label}
                      </Badge>
                      <div>
                        <div className="font-display text-xl font-black uppercase tracking-[0.08em] text-foreground">
                          {formatInt(layer.domains.length)} domains
                        </div>
                        <p className="mt-1 text-sm text-mcm-walnut/65">{layer.description}</p>
                      </div>
                    </div>

                    <div className="space-y-3">
                      {layer.domains.length === 0 ? (
                        <div className="rounded-[1.2rem] border border-dashed border-mcm-walnut/20 bg-mcm-paper/60 p-4 text-sm text-muted-foreground">
                          No domain tiles were published for this medallion.
                        </div>
                      ) : (
                        layer.domains.map((domain) => {
                          const isActive =
                            selectedDomain === domain.key && selectedLayer === layer.key;
                          return (
                            <button
                              key={`${layer.key}-${domain.key}`}
                              type="button"
                              aria-pressed={isActive}
                              aria-label={`Focus ${layer.label} ${domain.label} domain`}
                              className={`w-full rounded-[1.25rem] border px-4 py-4 text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-mcm-teal ${
                                isActive
                                  ? visual.activeClassName
                                  : 'border-mcm-walnut/12 bg-mcm-paper/70 hover:bg-mcm-paper'
                              }`}
                              onClick={() => {
                                startTransition(() => {
                                  setSelectedLayer(layer.key);
                                  setSelectedDomain(domain.key);
                                });
                              }}
                            >
                              <div className="flex items-start justify-between gap-3">
                                <div>
                                  <div className="font-display text-lg font-black uppercase tracking-[0.08em] text-foreground">
                                    {domain.label}
                                  </div>
                                  <div className="mt-1 text-xs text-mcm-walnut/60">
                                    {(domain.description || 'No domain description published.').trim()}
                                  </div>
                                </div>
                                {domain.metadata?.type ? (
                                  <Badge variant="secondary">{domain.metadata.type}</Badge>
                                ) : null}
                              </div>

                              <div className="mt-4 grid grid-cols-2 gap-2 text-xs text-mcm-walnut/70">
                                <div className="rounded-[1rem] bg-mcm-cream/65 px-3 py-2">
                                  <div className="text-[10px] uppercase tracking-[0.18em] text-mcm-walnut/55">
                                    Symbols
                                  </div>
                                  <div className="mt-1 font-mono font-bold text-foreground">
                                    {formatInt(domain.metadata?.symbolCount)}
                                  </div>
                                </div>
                                <div className="rounded-[1rem] bg-mcm-cream/65 px-3 py-2">
                                  <div className="text-[10px] uppercase tracking-[0.18em] text-mcm-walnut/55">
                                    Columns
                                  </div>
                                  <div className="mt-1 font-mono font-bold text-foreground">
                                    {formatInt(domain.metadata?.columnCount ?? domain.metadata?.columns?.length)}
                                  </div>
                                </div>
                                <div className="rounded-[1rem] bg-mcm-cream/65 px-3 py-2">
                                  <div className="text-[10px] uppercase tracking-[0.18em] text-mcm-walnut/55">
                                    Tables
                                  </div>
                                  <div className="mt-1 font-mono font-bold text-foreground">
                                    {formatInt(domain.tableCount)}
                                  </div>
                                </div>
                                <div className="rounded-[1rem] bg-mcm-cream/65 px-3 py-2">
                                  <div className="text-[10px] uppercase tracking-[0.18em] text-mcm-walnut/55">
                                    Range
                                  </div>
                                  <div className="mt-1 font-mono text-[11px] font-bold text-foreground">
                                    {formatDateRangeLabel(domain.metadata)}
                                  </div>
                                </div>
                              </div>

                              <div className="mt-3 flex items-center justify-between text-[11px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                                <span>{domain.status || 'status n/a'}</span>
                                <span>{formatBytes(domain.metadata?.totalBytes)}</span>
                              </div>
                            </button>
                          );
                        })
                      )}
                    </div>
                  </div>
                </section>
              );
            })}
          </div>
        )}
      </section>

      <section className="grid gap-6 xl:grid-cols-[340px_minmax(0,1fr)]">
        <aside className="mcm-panel h-fit overflow-hidden p-4 sm:p-5 xl:sticky xl:top-6">
          <div className="space-y-5">
            <div className="space-y-1">
              <p className="page-kicker">Atlas Navigator</p>
              <h2 className="font-display text-xl font-black uppercase tracking-[0.08em] text-foreground">
                Table Catalog
              </h2>
              <p className="text-sm text-muted-foreground">
                Filter the contract list, then inspect one table at a time.
              </p>
            </div>

            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                aria-label="Search table catalog"
                value={navigatorSearch}
                onChange={(event) => setNavigatorSearch(event.target.value)}
                placeholder="Search tables, domains, or loaded columns"
                className="pl-10"
              />
            </div>

            <div className="space-y-2">
              <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                Medallion Filter
              </div>
              <div className="grid grid-cols-2 gap-2">
                <Button
                  type="button"
                  variant={selectedLayer === 'all' ? 'default' : 'outline'}
                  className="justify-center"
                  onClick={() => {
                    startTransition(() => {
                      setSelectedLayer('all');
                    });
                  }}
                >
                  All Layers
                </Button>
                {atlasLayers.map((layer) => (
                  <Button
                    key={layer.key}
                    type="button"
                    variant={selectedLayer === layer.key ? 'default' : 'outline'}
                    className="justify-center"
                    onClick={() => {
                      startTransition(() => {
                        setSelectedLayer(layer.key);
                        if (
                          selectedDomain &&
                          !layer.domains.some((domain) => domain.key === selectedDomain)
                        ) {
                          setSelectedDomain(null);
                        }
                      });
                    }}
                  >
                    {layer.label}
                  </Button>
                ))}
              </div>
            </div>

            <div className="rounded-[1.35rem] border border-mcm-walnut/15 bg-mcm-cream/55 px-4 py-3">
              <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                Active Focus
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                <Badge variant="secondary">{selectedLayer === 'all' ? 'All medallions' : titleCase(selectedLayer)}</Badge>
                <Badge variant="secondary">
                  {selectedDomain
                    ? atlasLayers
                        .flatMap((layer) => layer.domains)
                        .find((domain) => domain.key === selectedDomain)?.label || titleCase(selectedDomain)
                    : 'All domains'}
                </Badge>
                <Badge variant="secondary">{formatInt(filteredTables.length)} visible tables</Badge>
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                  Contract List
                </div>
                {tableCatalogQuery.isLoading ? (
                  <span className="inline-flex items-center gap-1 text-[10px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    loading
                  </span>
                ) : null}
              </div>

              <div className="max-h-[760px] space-y-2 overflow-y-auto pr-1">
                {tableCatalogQuery.isLoading && !filteredTables.length ? (
                  Array.from({ length: 6 }).map((_, index) => (
                    <Skeleton key={index} className="h-24 rounded-[1.2rem]" />
                  ))
                ) : filteredTables.length === 0 ? (
                  <div className="rounded-[1.2rem] border border-dashed border-mcm-walnut/20 bg-mcm-paper/70 p-5 text-sm text-muted-foreground">
                    No tables matched the current medallion, domain, and search filters.
                  </div>
                ) : (
                  filteredTables.map((table) => {
                    const isSelected = selectedTable?.key === table.key;
                    const detailState = tableDetailsByKey[table.key];
                    const columnCount = detailState?.data?.columns.length;

                    return (
                      <button
                        key={table.key}
                        type="button"
                        aria-pressed={isSelected}
                        className={`w-full rounded-[1.25rem] border px-4 py-4 text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-mcm-teal ${
                          isSelected
                            ? 'border-mcm-teal bg-mcm-teal/10 shadow-[0_0_0_2px_rgba(0,128,128,0.14)]'
                            : 'border-mcm-walnut/12 bg-mcm-paper hover:bg-mcm-cream/85'
                        }`}
                        onClick={() => {
                          startTransition(() => {
                            setSelectedTableKey(table.key);
                          });
                        }}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div className="space-y-2">
                            <div className="font-display text-base font-black uppercase tracking-[0.08em] text-foreground">
                              {table.tableName}
                            </div>
                            <div className="flex flex-wrap gap-2">
                              <Badge variant="outline">{table.layerLabel}</Badge>
                              {table.domainLabel ? <Badge variant="secondary">{table.domainLabel}</Badge> : null}
                            </div>
                          </div>
                          {detailState?.isLoading ? (
                            <Loader2 className="h-4 w-4 animate-spin text-mcm-teal" />
                          ) : null}
                        </div>

                        <div className="mt-3 text-xs text-mcm-walnut/65">
                          {(table.domainDescription || 'Serving-table contract for this medallion slice.').trim()}
                        </div>

                        <div className="mt-4 grid grid-cols-2 gap-2 text-xs">
                          <div className="rounded-[0.9rem] bg-mcm-cream/70 px-3 py-2">
                            <div className="text-[10px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                              Schema
                            </div>
                            <div className="mt-1 font-mono font-bold text-foreground">
                              {table.schemaName}
                            </div>
                          </div>
                          <div className="rounded-[0.9rem] bg-mcm-cream/70 px-3 py-2">
                            <div className="text-[10px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                              Columns
                            </div>
                            <div className="mt-1 font-mono font-bold text-foreground">
                              {columnCount ? formatInt(columnCount) : detailState?.isLoading ? '...' : 'Open'}
                            </div>
                          </div>
                        </div>
                      </button>
                    );
                  })
                )}
              </div>
            </div>
          </div>
        </aside>

        <div className="space-y-6">
          {!selectedTable ? (
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
          ) : (
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
                          {(selectedTable.domainDescription ||
                            'Postgres contract for this medallion slice. Descriptions come from published column comments and gold lookup annotations when present.').trim()}
                        </p>
                      </div>
                    </div>

                    <div className="rounded-[1.3rem] border border-mcm-walnut/15 bg-mcm-paper/80 px-4 py-4 text-sm text-mcm-walnut/70 shadow-[0_12px_28px_rgba(119,63,26,0.08)]">
                      <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
                        Domain telemetry
                      </div>
                      <div className="mt-2 space-y-1">
                        <div>Columns in snapshot: {formatInt(selectedTable.domainMetadata?.columnCount)}</div>
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
                              : (selectedTableState.data.edit_reason || 'Editing is disabled for this contract.').trim()
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
                              value={columnSearch}
                              onChange={(event) => setColumnSearch(event.target.value)}
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
                                  <TableCell colSpan={4} className="py-10 text-center text-sm text-muted-foreground">
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
                    <li>System-status snapshot provides domain health, column counts, symbols, and storage rollups.</li>
                    <li>Postgres table metadata provides authoritative table columns, types, keys, and editability.</li>
                    <li>Gold lookup annotations backfill authored descriptions when the Postgres comment is empty.</li>
                  </ul>
                </div>
              </section>
            </>
          )}
        </div>
      </section>
    </div>
  );
};
