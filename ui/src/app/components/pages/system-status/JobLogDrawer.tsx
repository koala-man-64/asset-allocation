import React, { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Copy, RefreshCcw, WrapText } from 'lucide-react';

import { Drawer, DrawerClose, DrawerContent, DrawerDescription, DrawerFooter, DrawerHeader, DrawerTitle } from '@/app/components/ui/drawer';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Label } from '@/app/components/ui/label';
import { ScrollArea } from '@/app/components/ui/scroll-area';
import { cn } from '@/app/components/ui/utils';
import { DataService } from '@/services/DataService';

type JobLogDrawerProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  jobName: string | null;
};

export function JobLogDrawer({ open, onOpenChange, jobName }: JobLogDrawerProps) {
  const [wrap, setWrap] = useState(false);
  const [runs, setRuns] = useState(1);

  const queryKey = useMemo(() => ['jobLogs', jobName ?? '-', runs] as const, [jobName, runs]);
  const { data, isFetching, isError, error, refetch } = useQuery({
    queryKey,
    enabled: open && Boolean(jobName),
    queryFn: ({ signal }) => DataService.getJobLogs(jobName!, { runs }, signal),
    staleTime: 10_000,
    retry: false,
  });

  const renderedText = useMemo(() => {
    if (!data?.runs?.length) return '';

    const blocks: string[] = [];
    data.runs.forEach((run, idx) => {
      const title = [
        `#${idx + 1}`,
        (run.executionName || run.executionId || 'execution').trim(),
        run.status ? `[${run.status}]` : null,
        run.startTime ? `start=${run.startTime}` : null,
        run.endTime ? `end=${run.endTime}` : null,
      ]
        .filter(Boolean)
        .join(' ');

      blocks.push(title);
      if (run.error) {
        blocks.push(`ERROR: ${run.error}`);
      } else if (run.tail?.length) {
        blocks.push(...run.tail);
      } else {
        blocks.push('(no lines returned)');
      }
      blocks.push('');
    });

    return blocks.join('\n').trimEnd();
  }, [data]);

  useEffect(() => {
    if (!data || !jobName || !open) return;
    if (!data.runs?.length) {
      console.info('[JobLogDrawer] logs empty', { jobName, runsRequested: data.runsRequested });
      return;
    }

    data.runs.forEach((run) => {
      const label = (run.executionName || run.executionId || 'execution').trim();
      console.info('[JobLogDrawer] log tail', { jobName, execution: label, tail: run.tail });
    });
  }, [data, jobName, open]);

  const metaLine = [
    jobName,
    `runs=${runs}`,
    data ? `returned=${data.runsReturned}` : null,
    data ? `tail=${data.tailLines}` : null,
  ]
    .filter(Boolean)
    .join(' • ');

  const handleCopy = async () => {
    if (!renderedText) return;
    try {
      await navigator.clipboard.writeText(renderedText);
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
              <DrawerTitle className="font-mono text-sm uppercase tracking-widest">Job Console Tail</DrawerTitle>
              <DrawerDescription className="font-mono text-[11px]">
                <span className="text-muted-foreground">{metaLine || '—'}</span>
              </DrawerDescription>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-2 rounded-md border bg-background px-2 py-1">
                <Label className="font-mono text-[10px] uppercase tracking-widest text-muted-foreground">Runs</Label>
                <Input
                  type="number"
                  min={1}
                  max={10}
                  value={runs}
                  onChange={(event) => {
                    const raw = event.target.value;
                    const next = raw === '' ? 1 : Number.parseInt(raw, 10);
                    if (!Number.isFinite(next)) return;
                    const clamped = Math.max(1, Math.min(10, next));
                    setRuns(clamped);
                  }}
                  className="h-7 w-[68px] border-0 bg-transparent p-0 text-right font-mono text-[11px] shadow-none focus-visible:ring-0"
                />
              </div>
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
                disabled={!renderedText}
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
                {isFetching ? 'Loading…' : isError ? 'Unavailable' : renderedText ? 'Ready' : 'Empty'}
              </div>
            </div>

            <ScrollArea className="h-[55vh]">
              {isError ? (
                <div className="p-4 font-mono text-[11px] text-rose-200">
                  {(error as Error)?.message || 'Failed to load logs.'}
                  <div className="mt-2 text-zinc-400">
                    Configure `SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID`, `SYSTEM_HEALTH_ARM_RESOURCE_GROUP`, `SYSTEM_HEALTH_ARM_JOBS`, `SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=true`, and `SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID`.
                  </div>
                </div>
              ) : renderedText ? (
                <pre
                  className={cn(
                    'p-4 font-mono text-[11px] leading-relaxed',
                    wrap ? 'whitespace-pre-wrap break-words' : 'whitespace-pre',
                  )}
                >
                  {renderedText}
                </pre>
              ) : (
                <div className="p-4 font-mono text-[11px] text-zinc-400">
                  {isFetching ? 'Loading…' : 'No log tail returned for the selected run(s).'}
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

