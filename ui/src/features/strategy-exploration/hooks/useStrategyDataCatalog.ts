import {
  startTransition,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState
} from 'react';
import { useQuery } from '@tanstack/react-query';

import { useSystemStatusViewQuery } from '@/hooks/useSystemStatusView';
import { PostgresService, type GoldColumnLookupRow } from '@/services/PostgresService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

import {
  type CatalogColumn,
  type DomainDescriptor,
  type LayerAtlas,
  type LayerFilter,
  type MedallionKey,
  type TableCatalogItem,
  type TableCatalogResponse,
  type TableDetailState,
  buildDomainTokens,
  countDocumentedColumns,
  inferDomainForTableName,
  loadMedallionTableCatalog,
  normalizeDomainKey,
  normalizeKey,
  titleCase,
  toMedallionKey,
  MEDALLION_ORDER
} from '../lib/strategyDataCatalog';

export function useStrategyDataCatalog() {
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

      descriptors.sort((left, right) => left.label.localeCompare(right.label));
      index.set(layerKey, descriptors);
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

      const domains = (layer.domains || [])
        .map((domain) => {
          const key = normalizeDomainKey(String(domain?.name || ''));
          if (!key) {
            return null;
          }

          const metadata = systemStatusView?.metadataSnapshot.entries?.[`${layerKey}/${key}`];
          const tableCount = tableCatalogItems.filter(
            (item) => item.layerKey === layerKey && item.domainKey === key
          ).length;

          return {
            key,
            label: String(domain?.name || '').trim(),
            description: domain.description,
            status: domain.status,
            metadata,
            tableCount
          };
        })
        .filter((value): value is LayerAtlas['domains'][number] => Boolean(value))
        .sort((left, right) => left.label.localeCompare(right.label));

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
  }, [
    deferredNavigatorSearch,
    selectedDomain,
    selectedLayer,
    tableCatalogItems,
    tableDetailsByKey
  ]);

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

  const focusDomain = useCallback((layerKey: MedallionKey, domainKey: string) => {
    startTransition(() => {
      setSelectedLayer(layerKey);
      setSelectedDomain(domainKey);
    });
  }, []);

  const clearDomainFocus = useCallback(() => {
    startTransition(() => {
      setSelectedDomain(null);
      setSelectedLayer('all');
    });
  }, []);

  const selectLayer = useCallback(
    (layerKey: LayerFilter) => {
      startTransition(() => {
        setSelectedLayer(layerKey);
        if (
          layerKey !== 'all' &&
          selectedDomain &&
          !atlasLayers
            .find((layer) => layer.key === layerKey)
            ?.domains.some((domain) => domain.key === selectedDomain)
        ) {
          setSelectedDomain(null);
        }
      });
    },
    [atlasLayers, selectedDomain]
  );

  const selectTable = useCallback((tableKey: string) => {
    startTransition(() => {
      setSelectedTableKey(tableKey);
    });
  }, []);

  return {
    hero: {
      medallionCount,
      totalDomainCount,
      totalTableCount,
      isStatusLoading,
      isStatusFetching,
      isTableCatalogLoading: tableCatalogQuery.isLoading
    },
    alerts: {
      statusErrorMessage,
      tableCatalogErrorMessage,
      tableCatalogWarnings
    },
    atlas: {
      layers: atlasLayers,
      selectedLayer,
      selectedDomain
    },
    navigator: {
      search: navigatorSearch,
      filteredTables,
      selectedLayer,
      selectedDomain,
      selectedTable,
      tableDetailsByKey,
      isLoading: tableCatalogQuery.isLoading
    },
    detail: {
      selectedTable,
      selectedTableState,
      selectedColumns,
      columnSearch,
      selectedTableDocumentedCount
    },
    actions: {
      setNavigatorSearch,
      setColumnSearch,
      focusDomain,
      clearDomainFocus,
      selectLayer,
      selectTable
    }
  };
}

export type StrategyDataCatalogController = ReturnType<typeof useStrategyDataCatalog>;
