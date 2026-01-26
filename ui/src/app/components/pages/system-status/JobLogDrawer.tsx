import React, { useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Copy, RefreshCcw, WrapText } from 'lucide-react';

import { Drawer, DrawerClose, DrawerContent, DrawerDescription, DrawerFooter, DrawerHeader, DrawerTitle } from '@/app/components/ui/drawer';
import { Button } from '@/app/components/ui/button';
import { ScrollArea } from '@/app/components/ui/scroll-area';
import { cn } from '@/app/components/ui/utils';
import { DataService } from '@/services/DataService';

type JobLogDrawerProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  jobName: string | null;
  startTime?: string | null;
};

export function JobLogDrawer({ open, onOpenChange, jobName, startTime }: JobLogDrawerProps) {
  const [wrap, setWrap] = useState(false);

  const queryKey = useMemo(() => ['jobLogs', jobName ?? '-', startTime ?? '-'] as const, [jobName, startTime]);
  const { data, isFetching, isError, error, refetch } = useQuery({
    queryKey,
    enabled: open && Boolean(jobName),
    queryFn: ({ signal }) => DataService.getJobLogs(jobName!, { startTime }, signal),
    staleTime: 10_000,
    retry: false,
  });

  const bodyText = data?.text ?? '';
  const truncated = Boolean(data?.truncated);
  const metaLine = [jobName, startTime ? `start=${startTime}` : null, data?.timespan ? `span=${data.timespan}` : null]
    .filter(Boolean)
    .join(' • ');

  const handleCopy = async () => {
    if (!bodyText) return;
    try {
      await navigator.clipboard.writeText(bodyText);
    } catch {
      // no-op (clipboard permission / insecure context)
    }
  };

  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DrawerHeader className="space-y-2">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <DrawerTitle className="font-mono text-sm uppercase tracking-widest">Latest Console Log</DrawerTitle>
              <DrawerDescription className="font-mono text-[11px]">
                <span className="text-muted-foreground">{metaLine || '—'}</span>
              </DrawerDescription>
            </div>
            <div className="flex items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-8 gap-2 font-mono text-[11px]"
                onClick={() => setWrap((v) => !v)}
              >
                <WrapText className="h-4 w-4" />
                {wrap ? 'No Wrap' : 'Wrap'}
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-8 gap-2 font-mono text-[11px]"
                disabled={!bodyText}
                onClick={() => void handleCopy()}
              >
                <Copy className="h-4 w-4" />
                Copy
              </Button>
              <Button
                type="button"
                variant="outline"
                size="sm"
                className="h-8 gap-2 font-mono text-[11px]"
                onClick={() => void refetch()}
              >
                <RefreshCcw className={cn('h-4 w-4', isFetching && 'animate-spin')} />
                Refresh
              </Button>
            </div>
          </div>
        </DrawerHeader>

        <div className="px-4 pb-4">
          <div className="relative overflow-hidden rounded-md border bg-zinc-950 text-zinc-50">
            <div className="flex items-center justify-between border-b border-zinc-800/80 px-3 py-2">
              <div className="flex items-center gap-2 font-mono text-[10px] uppercase tracking-widest text-zinc-400">
                <span className={cn('h-2 w-2 rounded-full', isFetching ? 'bg-cyan-400 animate-pulse' : 'bg-zinc-600')} />
                {isFetching ? 'Streaming…' : isError ? 'Unavailable' : bodyText ? 'Ready' : 'Empty'}
              </div>
              {truncated && (
                <div className="rounded border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 font-mono text-[10px] uppercase tracking-widest text-amber-300">
                  Truncated
                </div>
              )}
            </div>

            <ScrollArea className="h-[55vh]">
              {isError ? (
                <div className="p-4 font-mono text-[11px] text-rose-200">
                  {(error as Error)?.message || 'Failed to load logs.'}
                  <div className="mt-2 text-zinc-400">
                    Configure `SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=true`, `SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID`, and `SYSTEM_HEALTH_JOB_LOG_QUERY`.
                  </div>
                </div>
              ) : bodyText ? (
                <pre
                  className={cn(
                    'p-4 font-mono text-[11px] leading-relaxed',
                    wrap ? 'whitespace-pre-wrap break-words' : 'whitespace-pre',
                  )}
                >
                  {bodyText}
                </pre>
              ) : (
                <div className="p-4 font-mono text-[11px] text-zinc-400">
                  {isFetching ? 'Loading…' : 'No log lines returned for this window.'}
                </div>
              )}
            </ScrollArea>
          </div>
        </div>

        <DrawerFooter className="flex-row justify-end gap-2">
          <DrawerClose asChild>
            <Button type="button" variant="default">
              Close
            </Button>
          </DrawerClose>
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  );
}

