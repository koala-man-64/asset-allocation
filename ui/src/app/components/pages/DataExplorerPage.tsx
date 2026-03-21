import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { Button } from '@/app/components/ui/button';
import { Label } from '@/app/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/app/components/ui/select';
import { DataService } from '@/services/DataService';
import type { AdlsFilePreviewResponse, AdlsHierarchyEntry } from '@/services/apiService';
import {
  ChevronDown,
  ChevronRight,
  Database,
  File,
  FileText,
  Folder,
  RefreshCw
} from 'lucide-react';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { formatPreviewContent } from '@/utils/formatPreviewContent';

const TEXT_FILE_EXTENSIONS = new Set([
  'txt',
  'csv',
  'json',
  'jsonl',
  'log',
  'yaml',
  'yml',
  'xml',
  'md',
  'py',
  'sql',
  'ts',
  'tsx',
  'js',
  'jsx',
  'css',
  'html',
  'htm',
  'env'
]);

const normalizeFolderPath = (value: string): string => {
  const cleaned = String(value || '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/^\/+/g, '')
    .replace(/\/+$/g, '');
  return cleaned ? `${cleaned}/` : '';
};

const normalizeFilePath = (value: string): string => {
  return String(value || '')
    .trim()
    .replace(/\\/g, '/')
    .replace(/^\/+/g, '')
    .replace(/\/+$/g, '');
};

const formatBytes = (value?: number | null): string => {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return '-';
  }
  if (value < 1024) {
    return `${value} B`;
  }
  const units = ['KB', 'MB', 'GB', 'TB'];
  let size = value / 1024;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  return `${size.toFixed(size >= 100 ? 0 : size >= 10 ? 1 : 2)} ${units[unitIndex]}`;
};

const isLikelyTextFile = (name: string): boolean => {
  const idx = name.lastIndexOf('.');
  if (idx < 0) {
    return false;
  }
  const ext = name.slice(idx + 1).toLowerCase();
  return TEXT_FILE_EXTENSIONS.has(ext);
};

type LayerKey = 'bronze' | 'silver' | 'gold' | 'platinum';
type DomainKey = 'market' | 'finance' | 'earnings' | 'price-target' | 'regime';

const CONTAINER_OPTIONS: Array<{ value: LayerKey; label: string }> = [
  { value: 'bronze', label: 'Bronze' },
  { value: 'silver', label: 'Silver' },
  { value: 'gold', label: 'Gold' },
  { value: 'platinum', label: 'Platinum' }
];

const DOMAIN_OPTIONS_BY_LAYER: Record<LayerKey, Array<{ value: DomainKey; label: string }>> = {
  bronze: [
    { value: 'market', label: 'Market' },
    { value: 'finance', label: 'Finance' },
    { value: 'earnings', label: 'Earnings' },
    { value: 'price-target', label: 'Targets' }
  ],
  silver: [
    { value: 'market', label: 'Market' },
    { value: 'finance', label: 'Finance' },
    { value: 'earnings', label: 'Earnings' },
    { value: 'price-target', label: 'Targets' }
  ],
  gold: [
    { value: 'market', label: 'Market' },
    { value: 'finance', label: 'Finance' },
    { value: 'earnings', label: 'Earnings' },
    { value: 'price-target', label: 'Targets' },
    { value: 'regime', label: 'Regime' }
  ],
  platinum: [
    { value: 'market', label: 'Market' },
    { value: 'finance', label: 'Finance' },
    { value: 'earnings', label: 'Earnings' },
    { value: 'price-target', label: 'Targets' }
  ]
};

const EXPLORER_ROOT_PATHS: Record<LayerKey, Record<DomainKey, string>> = {
  bronze: {
    market: 'market-data/',
    finance: 'finance-data/',
    earnings: 'earnings-data/',
    'price-target': 'price-target-data/',
    regime: 'regime/'
  },
  silver: {
    market: 'market-data/buckets/',
    finance: 'finance-data/',
    earnings: 'earnings-data/buckets/',
    'price-target': 'price-target-data/buckets/',
    regime: 'regime/'
  },
  gold: {
    market: 'market/buckets/',
    finance: 'finance/',
    earnings: 'earnings/buckets/',
    'price-target': 'targets/buckets/',
    regime: 'regime/'
  },
  platinum: {
    market: 'market/buckets/',
    finance: 'finance/',
    earnings: 'earnings/buckets/',
    'price-target': 'targets/buckets/',
    regime: 'regime/'
  }
};

const DELTA_PREVIEW_FILE_OPTIONS = [
  { value: '0', label: '0' },
  ...Array.from({ length: 25 }, (_, index) => {
    const value = String(index + 1).padStart(2, '0');
    return { value, label: value };
  })
];

const formatTwoDigitCount = (value?: number | null): string => {
  const normalized = Number.isFinite(Number(value)) ? Number(value) : 0;
  return String(Math.max(0, Math.floor(normalized))).padStart(2, '0');
};

const formatPreviewTableCell = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '-';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
};

type TreeMeta = {
  container: string;
  scanLimit: number;
  truncated: boolean;
};

export const DataExplorerPage: React.FC = () => {
  const [layer, setLayer] = useState<LayerKey>('gold');
  const [domain, setDomain] = useState<DomainKey>('market');
  const domainOptions = useMemo(() => DOMAIN_OPTIONS_BY_LAYER[layer], [layer]);
  const [maxDeltaFiles, setMaxDeltaFiles] = useState<string>('0');
  const rootPath = useMemo(
    () => normalizeFolderPath(EXPLORER_ROOT_PATHS[layer][domain]),
    [domain, layer]
  );
  const scanLimit = 5000;
  const previewMaxBytes = 262144;

  const [treeByPath, setTreeByPath] = useState<Record<string, AdlsHierarchyEntry[]>>({});
  const [treeMetaByPath, setTreeMetaByPath] = useState<Record<string, TreeMeta>>({});
  const [expandedFolders, setExpandedFolders] = useState<Record<string, boolean>>({});
  const [loadingPaths, setLoadingPaths] = useState<Record<string, boolean>>({});

  const [selectedFilePath, setSelectedFilePath] = useState<string | null>(null);
  const [preview, setPreview] = useState<AdlsFilePreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState<boolean>(false);
  const [refreshing, setRefreshing] = useState<boolean>(false);

  const [error, setError] = useState<string | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const loadFolder = useCallback(
    async (folderPath: string) => {
      const normalizedPath = normalizeFolderPath(folderPath);

      setLoadingPaths((prev) => ({ ...prev, [normalizedPath]: true }));
      setError(null);
      try {
        const result = await DataService.getAdlsTree({
          layer,
          path: normalizedPath || undefined,
          maxEntries: scanLimit
        });
        setTreeByPath((prev) => ({ ...prev, [normalizedPath]: result.entries }));
        setTreeMetaByPath((prev) => ({
          ...prev,
          [normalizedPath]: {
            container: result.container,
            scanLimit: result.scanLimit,
            truncated: result.truncated
          }
        }));
      } catch (err) {
        setError(formatSystemStatusText(err));
      } finally {
        setLoadingPaths((prev) => ({ ...prev, [normalizedPath]: false }));
      }
    },
    [layer, scanLimit]
  );

  const loadPreview = useCallback(
    async (filePath: string, options?: { maxDeltaFiles?: number }) => {
      const normalized = normalizeFilePath(filePath);
      if (!normalized) {
        return;
      }

      setSelectedFilePath(normalized);
      setPreviewLoading(true);
      setPreviewError(null);
      setPreview(null);

      try {
        const result = await DataService.getAdlsFilePreview({
          layer,
          path: normalized,
          maxBytes: previewMaxBytes,
          maxDeltaFiles: options?.maxDeltaFiles ?? Number(maxDeltaFiles)
        });
        setPreview(result);
      } catch (err) {
        setPreviewError(formatSystemStatusText(err));
      } finally {
        setPreviewLoading(false);
      }
    },
    [layer, maxDeltaFiles, previewMaxBytes]
  );

  useEffect(() => {
    if (domainOptions.some((option) => option.value === domain)) {
      return;
    }
    setDomain(domainOptions[0]?.value ?? 'market');
  }, [domain, domainOptions]);

  useEffect(() => {
    setTreeByPath({});
    setTreeMetaByPath({});
    setExpandedFolders({});
    setSelectedFilePath(null);
    setPreview(null);
    setPreviewError(null);
    void loadFolder(rootPath);
  }, [layer, rootPath, scanLimit, loadFolder]);

  const rootEntries = treeByPath[rootPath] || [];
  const rootMeta = treeMetaByPath[rootPath];
  const rootLoading = Boolean(loadingPaths[rootPath]);

  const handleToggleFolder = useCallback(
    (folderPath: string) => {
      const normalized = normalizeFolderPath(folderPath);
      const shouldExpand = !expandedFolders[normalized];
      setExpandedFolders((prev) => ({ ...prev, [normalized]: shouldExpand }));
      if (shouldExpand && !treeByPath[normalized]) {
        void loadFolder(normalized);
      }
    },
    [expandedFolders, loadFolder, treeByPath]
  );

  const refreshExplorer = useCallback(async () => {
    if (refreshing) {
      return;
    }

    setRefreshing(true);

    const expandedPaths = Object.entries(expandedFolders)
      .filter(([path, isExpanded]) => isExpanded && path !== rootPath && path.startsWith(rootPath))
      .map(([path]) => normalizeFolderPath(path))
      .sort((left, right) => left.length - right.length);

    try {
      await loadFolder(rootPath);
      if (expandedPaths.length) {
        await Promise.allSettled(expandedPaths.map((folderPath) => loadFolder(folderPath)));
      }
      if (selectedFilePath) {
        await loadPreview(selectedFilePath, { maxDeltaFiles: Number(maxDeltaFiles) });
      }
    } finally {
      setRefreshing(false);
    }
  }, [
    expandedFolders,
    loadFolder,
    loadPreview,
    maxDeltaFiles,
    refreshing,
    rootPath,
    selectedFilePath
  ]);

  const renderEntries = useCallback(
    (entries: AdlsHierarchyEntry[], depth: number): React.ReactNode => {
      return entries.map((entry) => {
        if (entry.type === 'folder') {
          const folderPath = normalizeFolderPath(entry.path);
          const isExpanded = Boolean(expandedFolders[folderPath]);
          const isLoading = Boolean(loadingPaths[folderPath]);
          const children = treeByPath[folderPath] || [];

          return (
            <div key={folderPath}>
              <button
                type="button"
                onClick={() => handleToggleFolder(folderPath)}
                className="flex w-full items-center gap-2 rounded px-2 py-1 text-left font-mono text-sm transition-colors hover:bg-accent"
                style={{ paddingLeft: `${depth * 14 + 8}px` }}
              >
                {isExpanded ? (
                  <ChevronDown className="h-4 w-4 shrink-0 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                )}
                <Folder className="h-4 w-4 shrink-0 text-mcm-teal" />
                <span className="truncate">{entry.name}</span>
              </button>

              {isExpanded && (
                <div>
                  {isLoading ? (
                    <div
                      className="py-1 font-mono text-xs text-muted-foreground"
                      style={{ paddingLeft: `${depth * 14 + 34}px` }}
                    >
                      Loading...
                    </div>
                  ) : children.length ? (
                    renderEntries(children, depth + 1)
                  ) : (
                    <div
                      className="py-1 font-mono text-xs text-muted-foreground"
                      style={{ paddingLeft: `${depth * 14 + 34}px` }}
                    >
                      Empty folder
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        }

        const normalizedFilePath = normalizeFilePath(entry.path);
        const isSelected = selectedFilePath === normalizedFilePath;
        const textLike = isLikelyTextFile(entry.name);

        return (
          <button
            key={normalizedFilePath}
            type="button"
            onClick={() => void loadPreview(normalizedFilePath)}
            className={`flex w-full items-center gap-2 rounded px-2 py-1 text-left font-mono text-sm transition-colors hover:bg-accent ${
              isSelected ? 'bg-accent/80' : ''
            }`}
            style={{ paddingLeft: `${depth * 14 + 26}px` }}
          >
            {textLike ? (
              <FileText className="h-4 w-4 shrink-0 text-mcm-copper" />
            ) : (
              <File className="h-4 w-4 shrink-0 text-muted-foreground" />
            )}
            <span className="min-w-0 flex-1 truncate">{entry.name}</span>
            <span className="shrink-0 text-[11px] text-muted-foreground">
              {formatBytes(entry.size)}
            </span>
          </button>
        );
      });
    },
    [expandedFolders, handleToggleFolder, loadPreview, loadingPaths, selectedFilePath, treeByPath]
  );

  const selectedFileLabel = useMemo(() => {
    if (!selectedFilePath) {
      return null;
    }
    return selectedFilePath.split('/').pop() || selectedFilePath;
  }, [selectedFilePath]);

  const formattedPreviewContent = useMemo(() => {
    return formatPreviewContent(preview?.contentPreview, {
      path: selectedFilePath,
      contentType: preview?.contentType
    });
  }, [preview?.contentPreview, preview?.contentType, selectedFilePath]);

  const isTablePreview =
    preview?.previewMode === 'delta-table' || preview?.previewMode === 'parquet-table';
  const previewTableColumns = preview?.tableColumns ?? [];
  const previewTableRows = preview?.tableRows ?? [];
  const previewRowCount = preview?.tableRowCount ?? previewTableRows.length;
  const previewRowLimit = preview?.tablePreviewLimit ?? previewTableRows.length;
  const displayedPreviewRows = Math.min(previewRowCount, previewRowLimit);

  return (
    <div className="page-shell">
      <div className="page-header-row items-start gap-6">
        <div className="page-header min-w-0 flex-1">
          <p className="page-kicker">Live Operations</p>
          <div className="flex flex-col gap-2 md:flex-row md:flex-wrap md:items-end md:gap-4">
            <h1 className="page-title flex items-center gap-2">
              <Database className="h-5 w-5 text-mcm-teal" />
              Data Explorer
            </h1>
            <p className="page-subtitle max-w-none md:pb-0.5">
              Browse ADLS folders/files and preview plaintext blobs in a dedicated side panel.
            </p>
          </div>
        </div>

        <div className="grid w-full max-w-[52rem] gap-4 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_9rem_auto]">
          <div className="space-y-2">
            <Label htmlFor="data-explorer-container">Container</Label>
            <Select value={layer} onValueChange={(value) => setLayer(value as LayerKey)}>
              <SelectTrigger id="data-explorer-container" className="font-mono uppercase">
                <SelectValue placeholder="Select container" />
              </SelectTrigger>
              <SelectContent>
                {CONTAINER_OPTIONS.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label htmlFor="data-explorer-domain">Domain</Label>
            <Select value={domain} onValueChange={(value) => setDomain(value as DomainKey)}>
              <SelectTrigger id="data-explorer-domain" className="font-mono uppercase">
                <SelectValue placeholder="Select domain" />
              </SelectTrigger>
              <SelectContent>
                {domainOptions.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-2">
            <Label htmlFor="data-explorer-delta-files">Delta Files</Label>
            <Select
              value={maxDeltaFiles}
              onValueChange={(value) => {
                setMaxDeltaFiles(value);
                if (selectedFilePath) {
                  void loadPreview(selectedFilePath, { maxDeltaFiles: Number(value) });
                }
              }}
            >
              <SelectTrigger id="data-explorer-delta-files" className="font-mono">
                <SelectValue placeholder="0" />
              </SelectTrigger>
              <SelectContent>
                {DELTA_PREVIEW_FILE_OPTIONS.map((option) => (
                  <SelectItem key={option.value} value={option.value}>
                    {option.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="flex items-end">
            <Button
              type="button"
              variant="outline"
              onClick={() => void refreshExplorer()}
              disabled={refreshing}
              className="w-full sm:w-auto"
            >
              <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? 'animate-spin' : ''}`} />
              {refreshing ? 'Refreshing...' : 'Refresh Metadata'}
            </Button>
          </div>
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-4 font-mono text-sm text-destructive">
          <strong>Error:</strong> {error}
        </div>
      )}

      <div className="grid flex-1 gap-4 overflow-hidden lg:grid-cols-[minmax(320px,42%)_1fr]">
        <div className="mcm-panel flex min-h-[420px] flex-col overflow-hidden p-0">
          <div className="border-b border-border/60 px-4 py-3">
            <p className="font-mono text-xs text-muted-foreground">
              {rootMeta ? `container=${rootMeta.container}` : 'container=...'} | path=
              {rootPath || '/'}
            </p>
            {rootMeta?.truncated ? (
              <p className="mt-1 font-mono text-xs text-amber-600">
                Listing truncated at {rootMeta.scanLimit.toLocaleString()} scanned blobs.
              </p>
            ) : null}
          </div>

          <div className="flex-1 overflow-auto px-2 py-2">
            {rootLoading ? (
              <div className="p-2 font-mono text-sm text-muted-foreground">Loading tree...</div>
            ) : rootEntries.length ? (
              renderEntries(rootEntries, 0)
            ) : (
              <div className="p-2 font-mono text-sm text-muted-foreground">
                No folders or files found for this path.
              </div>
            )}
          </div>
        </div>

        <div className="mcm-panel flex min-h-[420px] flex-col overflow-hidden p-0">
          <div className="border-b border-border/60 px-4 py-3">
            <p className="font-mono text-xs text-muted-foreground">Preview</p>
            <p className="truncate font-mono text-sm">
              {selectedFilePath ? selectedFilePath : 'Select a file from the hierarchy'}
            </p>
          </div>

          <div className="flex-1 overflow-auto p-4">
            {!selectedFilePath ? (
              <div className="font-mono text-sm text-muted-foreground">
                Choose a file to preview plaintext content.
              </div>
            ) : previewLoading ? (
              <div className="font-mono text-sm text-muted-foreground">
                Loading preview for {selectedFileLabel}...
              </div>
            ) : previewError ? (
              <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 font-mono text-sm text-destructive">
                <strong>Error:</strong> {previewError}
              </div>
            ) : preview && isTablePreview ? (
              <div className="space-y-3">
                <div className="flex flex-wrap gap-x-4 gap-y-1 font-mono text-xs text-muted-foreground">
                  <span>
                    {preview.previewMode === 'delta-table' ? 'delta snapshot' : 'parquet preview'}
                  </span>
                  {preview.resolvedTablePath ? (
                    <span>table={preview.resolvedTablePath}</span>
                  ) : null}
                  {preview.tableVersion !== null && preview.tableVersion !== undefined ? (
                    <span>version={preview.tableVersion}</span>
                  ) : (
                    <span>version=latest</span>
                  )}
                  {preview.processedDeltaFiles !== null &&
                  preview.processedDeltaFiles !== undefined ? (
                    <span>commits={formatTwoDigitCount(preview.processedDeltaFiles)}</span>
                  ) : null}
                  <span>
                    rows={displayedPreviewRows.toLocaleString()}
                    {previewRowCount > 0 ? `/${previewRowCount.toLocaleString()}` : ''}
                  </span>
                  {preview.deltaLogPath ? <span>log={preview.deltaLogPath}</span> : null}
                </div>

                {previewTableColumns.length ? (
                  <div className="max-h-[60vh] overflow-auto rounded-md border border-border/60 bg-background">
                    <table className="min-w-full border-collapse font-mono text-xs">
                      <thead className="sticky top-0 z-10 bg-mcm-paper">
                        <tr className="border-b border-border/60">
                          <th className="w-12 border-r border-border/50 px-3 py-2 text-right text-[10px] uppercase tracking-[0.2em] text-muted-foreground">
                            #
                          </th>
                          {previewTableColumns.map((column) => (
                            <th
                              key={column}
                              className="border-r border-border/50 px-3 py-2 text-left text-[10px] uppercase tracking-[0.2em] text-muted-foreground last:border-r-0"
                            >
                              {column}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {previewTableRows.length ? (
                          previewTableRows.map((row, rowIndex) => (
                            <tr
                              key={`${selectedFilePath}-${rowIndex}`}
                              className="border-b border-border/40 align-top last:border-b-0"
                            >
                              <td className="border-r border-border/40 px-3 py-2 text-right text-muted-foreground">
                                {rowIndex + 1}
                              </td>
                              {previewTableColumns.map((column) => {
                                const displayValue = formatPreviewTableCell(row[column]);
                                return (
                                  <td
                                    key={`${selectedFilePath}-${rowIndex}-${column}`}
                                    className="max-w-[18rem] border-r border-border/40 px-3 py-2 last:border-r-0"
                                    title={displayValue}
                                  >
                                    <div className="truncate">{displayValue}</div>
                                  </td>
                                );
                              })}
                            </tr>
                          ))
                        ) : (
                          <tr>
                            <td
                              colSpan={previewTableColumns.length + 1}
                              className="px-3 py-6 text-center text-muted-foreground"
                            >
                              Delta preview returned no rows.
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="font-mono text-sm text-muted-foreground">
                    Preview returned no table columns.
                  </div>
                )}
              </div>
            ) : preview && !preview.isPlainText ? (
              <div className="space-y-2 font-mono text-sm text-muted-foreground">
                <div>
                  This file does not appear to be plaintext and cannot be rendered as text preview.
                </div>
                {preview.contentType ? <div>contentType={preview.contentType}</div> : null}
                {preview.truncated ? (
                  <div>Preview bytes truncated at {preview.maxBytes.toLocaleString()}.</div>
                ) : null}
              </div>
            ) : preview ? (
              <div className="space-y-2">
                {preview.previewMode === 'delta-log' ? (
                  <div className="font-mono text-xs text-mcm-olive">
                    delta-log preview from {preview.deltaLogPath || '_delta_log/'} | files=
                    {formatTwoDigitCount(preview.processedDeltaFiles ?? Number(maxDeltaFiles))}
                  </div>
                ) : null}
                <div className="font-mono text-xs text-muted-foreground">
                  encoding={preview.encoding || 'unknown'}
                  {preview.contentType ? ` | contentType=${preview.contentType}` : ''}
                  {preview.truncated
                    ? ` | truncated at ${preview.maxBytes.toLocaleString()} bytes`
                    : ''}
                </div>
                <pre className="max-h-[60vh] overflow-auto whitespace-pre-wrap break-words rounded-md border border-border/60 bg-background p-3 font-mono text-xs leading-5">
                  {formattedPreviewContent}
                </pre>
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
};
