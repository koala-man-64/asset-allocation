import React, { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertCircle, Ban, FileText, Loader2, Search } from 'lucide-react';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle
} from '@/app/components/ui/sheet';
import { Alert, AlertDescription, AlertTitle } from '@/app/components/ui/alert';
import { Badge } from '@/app/components/ui/badge';
import { Input } from '@/app/components/ui/input';
import { ScrollArea } from '@/app/components/ui/scroll-area';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/app/components/ui/tabs';
import { DataService } from '@/services/DataService';
import type { DomainListFilePreview } from '@/services/apiService';
import { StatusTypos } from './StatusTokens';
import { formatSystemStatusText } from './systemStatusText';

type LayerKey = 'bronze' | 'silver' | 'gold' | 'platinum';

type ListType = 'whitelist' | 'blacklist';

export interface DomainListViewerTarget {
  layerKey: LayerKey;
  layerLabel: string;
  domainKey: string;
  domainLabel: string;
}

interface DomainListViewerSheetProps {
  target: DomainListViewerTarget | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const DEFAULT_SYMBOL_LIMIT = 5000;
const numberFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

function formatInt(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return '0';
  return numberFormatter.format(value);
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) return '—';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

export function DomainListViewerSheet({ target, open, onOpenChange }: DomainListViewerSheetProps) {
  const [activeListType, setActiveListType] = useState<ListType>('whitelist');
  const [search, setSearch] = useState('');

  useEffect(() => {
    setActiveListType('whitelist');
    setSearch('');
  }, [open, target?.layerKey, target?.domainKey]);

  const query = useQuery({
    queryKey: ['domainLists', target?.layerKey || '', target?.domainKey || '', DEFAULT_SYMBOL_LIMIT],
    queryFn: async () => {
      if (!target) throw new Error('List viewer target is required.');
      return DataService.getDomainLists(target.layerKey, target.domainKey, {
        limit: DEFAULT_SYMBOL_LIMIT
      });
    },
    enabled: open && Boolean(target),
    staleTime: 60 * 1000,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  const filesByType = useMemo(() => {
    const out = new Map<ListType, DomainListFilePreview>();
    for (const file of query.data?.files || []) {
      if (file.listType === 'whitelist' || file.listType === 'blacklist') {
        out.set(file.listType, file);
      }
    }
    return out;
  }, [query.data?.files]);

  const renderListFile = (file: DomainListFilePreview | undefined) => {
    if (!file) {
      return (
        <div className="rounded-xl border border-mcm-walnut/20 bg-mcm-cream/25 p-3 text-sm text-mcm-walnut/65">
          No file metadata returned for this list type.
        </div>
      );
    }

    const needle = search.trim().toUpperCase();
    const filteredSymbols = !needle
      ? file.symbols
      : file.symbols.filter((symbol) => symbol.toUpperCase().includes(needle));

    return (
      <div className="space-y-3">
        <div className="rounded-xl border border-mcm-walnut/20 bg-mcm-cream/30 p-3">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant={file.exists ? 'secondary' : 'outline'}>
              {file.exists ? 'File exists' : 'File missing'}
            </Badge>
            <Badge variant="outline">{formatInt(file.symbolCount)} symbols</Badge>
            {file.truncated ? (
              <Badge variant="outline" className="text-amber-700">
                Preview truncated
              </Badge>
            ) : null}
          </div>
          <div className={`${StatusTypos.MONO} mt-2 break-all text-[11px] text-mcm-walnut/70`}>
            {file.path}
          </div>
        </div>

        {file.warning ? (
          <Alert variant="destructive">
            <AlertCircle className="h-4 w-4" />
            <AlertTitle>List read warning</AlertTitle>
            <AlertDescription className="break-words">{file.warning}</AlertDescription>
          </Alert>
        ) : null}

        {!file.exists ? (
          <div className="rounded-xl border border-mcm-walnut/20 bg-mcm-cream/25 p-3 text-sm text-mcm-walnut/65">
            This file does not exist yet for this layer/domain.
          </div>
        ) : (
          <>
            <div className="space-y-2">
              <label className="flex items-center gap-2 text-xs font-semibold text-mcm-walnut/75">
                <Search className="h-3.5 w-3.5" />
                Filter symbols
              </label>
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Type symbol prefix or substring"
                className="h-9"
                aria-label="Filter list symbols"
              />
            </div>

            {file.truncated ? (
              <p className="text-[11px] text-mcm-walnut/65">
                Showing first {formatInt(file.symbols.length)} of {formatInt(file.symbolCount)} symbols due
                to preview limit.
              </p>
            ) : null}

            {file.symbols.length === 0 ? (
              <div className="rounded-xl border border-dashed border-mcm-walnut/20 bg-mcm-cream/20 p-3 text-sm text-mcm-walnut/65">
                No symbols are currently stored in this file.
              </div>
            ) : filteredSymbols.length === 0 ? (
              <div className="rounded-xl border border-dashed border-mcm-walnut/20 bg-mcm-cream/20 p-3 text-sm text-mcm-walnut/65">
                No symbols match the current filter.
              </div>
            ) : (
              <ScrollArea className="h-[320px] rounded-xl border border-mcm-walnut/20 bg-mcm-paper/70 p-2">
                <ol className="space-y-1.5 pr-2">
                  {filteredSymbols.map((symbol, index) => (
                    <li
                      key={`${symbol}-${index}`}
                      className="flex items-center justify-between rounded-md border border-mcm-walnut/10 bg-mcm-cream/20 px-2 py-1"
                    >
                      <span className="text-[10px] text-mcm-walnut/45">{formatInt(index + 1)}</span>
                      <span className={`${StatusTypos.MONO} text-[12px] font-semibold text-mcm-walnut`}>
                        {symbol}
                      </span>
                    </li>
                  ))}
                </ol>
              </ScrollArea>
            )}
          </>
        )}
      </div>
    );
  };

  const whitelistFile = filesByType.get('whitelist');
  const blacklistFile = filesByType.get('blacklist');

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="w-[460px] sm:w-[640px] bg-mcm-paper border-l-2 border-mcm-walnut shadow-[10px_10px_0px_0px_rgba(119,63,26,0.1)]"
      >
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            <Ban className="h-4 w-4 text-mcm-walnut/70" />
            <span>
              {target ? `${target.layerLabel} • ${target.domainLabel}` : 'List file viewer'}
            </span>
          </SheetTitle>
          <SheetDescription className="text-xs text-mcm-walnut/60">
            View whitelist and blacklist files for this medallion-domain combination.
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-3 px-4 pb-4">
          {query.isLoading ? (
            <div className="flex items-center gap-2 text-sm text-mcm-walnut/70">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading domain list files…
            </div>
          ) : query.isError ? (
            <Alert variant="destructive">
              <AlertTitle>List files unavailable</AlertTitle>
              <AlertDescription className="break-words">
                {formatSystemStatusText(query.error)}
              </AlertDescription>
            </Alert>
          ) : query.data ? (
            <>
              <div className="flex flex-wrap items-center gap-2 text-[11px] text-mcm-walnut/65">
                <Badge variant="outline" className="text-[10px] uppercase tracking-wide">
                  <FileText className="mr-1 h-3 w-3" />
                  {query.data.container}
                </Badge>
                <span>Loaded {formatDateTime(query.data.loadedAt)}</span>
                <span>Preview limit {formatInt(query.data.limit)} symbols/file</span>
              </div>

              <Tabs
                value={activeListType}
                onValueChange={(value) => setActiveListType(value as ListType)}
                className="space-y-3"
              >
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="whitelist">
                    Whitelist ({formatInt(whitelistFile?.symbolCount)})
                  </TabsTrigger>
                  <TabsTrigger value="blacklist">
                    Blacklist ({formatInt(blacklistFile?.symbolCount)})
                  </TabsTrigger>
                </TabsList>
                <TabsContent value="whitelist">{renderListFile(whitelistFile)}</TabsContent>
                <TabsContent value="blacklist">{renderListFile(blacklistFile)}</TabsContent>
              </Tabs>
            </>
          ) : (
            <div className="text-sm text-mcm-walnut/60">Select a cell to view list files.</div>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
