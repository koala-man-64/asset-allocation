import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { DataService } from '@/services/DataService';
import type { AdlsFilePreviewResponse, AdlsHierarchyEntry } from '@/services/apiService';
import { Button } from '@/app/components/ui/button';
import { ChevronDown, ChevronRight, Database, File, FileText, Folder, RefreshCw } from 'lucide-react';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

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
  const cleaned = String(value || '').trim().replace(/\\/g, '/').replace(/^\/+/g, '').replace(/\/+$/g, '');
  return cleaned ? `${cleaned}/` : '';
};

const normalizeFilePath = (value: string): string => {
  return String(value || '').trim().replace(/\\/g, '/').replace(/^\/+/g, '').replace(/\/+$/g, '');
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

type TreeMeta = {
  container: string;
  scanLimit: number;
  truncated: boolean;
};

export const DataExplorerPage: React.FC = () => {
  const [layer, setLayer] = useState<LayerKey>('gold');
  const [pathInput, setPathInput] = useState<string>('');
  const [rootPath, setRootPath] = useState<string>('');
  const [scanLimit, setScanLimit] = useState<number>(5000);
  const [previewMaxBytes, setPreviewMaxBytes] = useState<number>(262144);

  const [treeByPath, setTreeByPath] = useState<Record<string, AdlsHierarchyEntry[]>>({});
  const [treeMetaByPath, setTreeMetaByPath] = useState<Record<string, TreeMeta>>({});
  const [expandedFolders, setExpandedFolders] = useState<Record<string, boolean>>({});
  const [loadingPaths, setLoadingPaths] = useState<Record<string, boolean>>({});

  const [selectedFilePath, setSelectedFilePath] = useState<string | null>(null);
  const [preview, setPreview] = useState<AdlsFilePreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState<boolean>(false);

  const [error, setError] = useState<string | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const controlClass =
    'h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono outline-none transition-shadow focus-visible:ring-2 focus-visible:ring-ring/40';

  const loadFolder = useCallback(
    async (folderPath: string, force: boolean = false) => {
      const normalizedPath = normalizeFolderPath(folderPath);
      void force;

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
    async (filePath: string) => {
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
          maxBytes: previewMaxBytes
        });
        setPreview(result);
      } catch (err) {
        setPreviewError(formatSystemStatusText(err));
      } finally {
        setPreviewLoading(false);
      }
    },
    [layer, previewMaxBytes]
  );

  useEffect(() => {
    setTreeByPath({});
    setTreeMetaByPath({});
    setExpandedFolders({});
    setSelectedFilePath(null);
    setPreview(null);
    setPreviewError(null);
    void loadFolder(rootPath, true);
  }, [layer, rootPath, scanLimit, loadFolder]);

  const rootEntries = treeByPath[rootPath] || [];
  const rootMeta = treeMetaByPath[rootPath];
  const rootLoading = Boolean(loadingPaths[rootPath]);

  const handleToggleFolder = (folderPath: string) => {
    const normalized = normalizeFolderPath(folderPath);
    const shouldExpand = !expandedFolders[normalized];
    setExpandedFolders((prev) => ({ ...prev, [normalized]: shouldExpand }));
    if (shouldExpand && !treeByPath[normalized]) {
      void loadFolder(normalized);
    }
  };

  const applyPathFilter = () => {
    setRootPath(normalizeFolderPath(pathInput));
  };

  const refreshTree = () => {
    setTreeByPath({});
    setTreeMetaByPath({});
    setExpandedFolders({});
    setSelectedFilePath(null);
    setPreview(null);
    setPreviewError(null);
    void loadFolder(rootPath, true);
  };

  const handlePathInputKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      applyPathFilter();
    }
  };

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
            <span className="shrink-0 text-[11px] text-muted-foreground">{formatBytes(entry.size)}</span>
          </button>
        );
      });
    },
    [expandedFolders, loadPreview, loadingPaths, selectedFilePath, treeByPath]
  );

  const selectedFileLabel = useMemo(() => {
    if (!selectedFilePath) {
      return null;
    }
    return selectedFilePath.split('/').pop() || selectedFilePath;
  }, [selectedFilePath]);

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title flex items-center gap-2">
          <Database className="h-5 w-5 text-mcm-teal" />
          Data Explorer
        </h1>
        <p className="page-subtitle">
          Browse ADLS folders/files and preview plaintext blobs in a dedicated side panel.
        </p>
      </div>

      <div className="mcm-panel p-4 sm:p-5">
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-[160px_1fr_150px_170px_auto_auto] lg:items-end">
          <div className="space-y-2">
            <label htmlFor="adls-layer">Layer</label>
            <select
              id="adls-layer"
              value={layer}
              onChange={(event) => setLayer(event.target.value as LayerKey)}
              className={controlClass}
            >
              <option value="bronze">BRONZE</option>
              <option value="silver">SILVER</option>
              <option value="gold">GOLD</option>
              <option value="platinum">PLATINUM</option>
            </select>
          </div>

          <div className="space-y-2">
            <label htmlFor="adls-root-path">Root Path (optional)</label>
            <input
              id="adls-root-path"
              type="text"
              value={pathInput}
              onChange={(event) => setPathInput(event.target.value)}
              onKeyDown={handlePathInputKeyDown}
              className={controlClass}
              placeholder="market/buckets/"
            />
          </div>

          <div className="space-y-2">
            <label htmlFor="adls-scan-limit">Scan Limit</label>
            <input
              id="adls-scan-limit"
              type="number"
              min={1}
              max={100000}
              value={scanLimit}
              onChange={(event) => setScanLimit(Math.max(1, Number(event.target.value) || 1))}
              className={controlClass}
            />
          </div>

          <div className="space-y-2">
            <label htmlFor="adls-preview-max-bytes">Preview Bytes</label>
            <input
              id="adls-preview-max-bytes"
              type="number"
              min={1024}
              max={1048576}
              value={previewMaxBytes}
              onChange={(event) => setPreviewMaxBytes(Math.max(1024, Number(event.target.value) || 1024))}
              className={controlClass}
            />
          </div>

          <Button onClick={applyPathFilter} className="h-10 px-6">
            Apply Path
          </Button>

          <Button onClick={refreshTree} className="h-10 gap-2 px-6" variant="outline" disabled={rootLoading}>
            {rootLoading ? <RefreshCw className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            Refresh
          </Button>
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
              {rootMeta ? `container=${rootMeta.container}` : 'container=...'} | path={rootPath || '/'}
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
              <div className="p-2 font-mono text-sm text-muted-foreground">No folders or files found for this path.</div>
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
              <div className="font-mono text-sm text-muted-foreground">Choose a file to preview plaintext content.</div>
            ) : previewLoading ? (
              <div className="font-mono text-sm text-muted-foreground">Loading preview for {selectedFileLabel}...</div>
            ) : previewError ? (
              <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-3 font-mono text-sm text-destructive">
                <strong>Error:</strong> {previewError}
              </div>
            ) : preview && !preview.isPlainText ? (
              <div className="space-y-2 font-mono text-sm text-muted-foreground">
                <div>This file does not appear to be plaintext and cannot be rendered as text preview.</div>
                {preview.contentType ? <div>contentType={preview.contentType}</div> : null}
                {preview.truncated ? <div>Preview bytes truncated at {preview.maxBytes.toLocaleString()}.</div> : null}
              </div>
            ) : preview ? (
              <div className="space-y-2">
                <div className="font-mono text-xs text-muted-foreground">
                  encoding={preview.encoding || 'unknown'}
                  {preview.contentType ? ` | contentType=${preview.contentType}` : ''}
                  {preview.truncated ? ` | truncated at ${preview.maxBytes.toLocaleString()} bytes` : ''}
                </div>
                <pre className="max-h-[60vh] overflow-auto whitespace-pre-wrap break-words rounded-md border border-border/60 bg-background p-3 font-mono text-xs leading-5">
                  {preview.contentPreview || ''}
                </pre>
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
};
