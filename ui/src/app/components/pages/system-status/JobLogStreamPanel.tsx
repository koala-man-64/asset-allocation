import { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ExternalLink, Loader2, ScrollText } from 'lucide-react';

import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/app/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import type { JobLogsResponse } from '@/services/apiService';
import { DataService } from '@/services/DataService';
import {
  addConsoleLogStreamListener,
  buildJobLogTopic,
  requestRealtimeSubscription,
  requestRealtimeUnsubscription,
} from '@/services/realtimeBus';
import {
  formatTimeAgo,
  getAzureJobExecutionsUrl,
  getStatusBadge,
  getStatusIcon,
  normalizeJobStatus,
  normalizeAzurePortalUrl,
} from './SystemStatusHelpers';
import { formatSystemStatusText } from './systemStatusText';

const LOG_LINE_LIMIT = 200;

export type JobLogStreamTarget = {
  name: string;
  label: string;
  layerName?: string | null;
  domainName?: string | null;
  jobUrl?: string | null;
  runningState?: string | null;
  recentStatus?: string | null;
  startTime?: string | null;
};

type LogState = {
  lines: string[];
  loading: boolean;
  error: string | null;
};

function mergeLogLines(existing: string[], incoming: string[], limit = LOG_LINE_LIMIT): string[] {
  const next = [...existing];
  const windowed = new Set(existing.slice(-limit));

  incoming.forEach((line) => {
    const text = String(line || '').trim();
    if (!text || windowed.has(text)) {
      return;
    }
    next.push(text);
    windowed.add(text);
    while (next.length > limit) {
      const removed = next.shift();
      if (removed && !next.includes(removed)) {
        windowed.delete(removed);
      }
    }
  });

  return next.slice(-limit);
}

function extractJobLogLines(response: JobLogsResponse): string[] {
  const combined = [
    ...(response?.runs ?? []).flatMap((run) => [...(run?.tail ?? []), ...(run?.consoleLogs ?? [])]),
  ]
    .filter((line) => line !== undefined && line !== null)
    .map((line) => formatSystemStatusText(line))
    .filter((line) => line.length > 0);

  return combined.slice(-LOG_LINE_LIMIT);
}

function pickDefaultJobName(jobs: JobLogStreamTarget[]): string {
  if (!jobs.length) {
    return '';
  }

  const sorted = [...jobs].sort((left, right) => {
    const leftRunning = normalizeJobStatus(left.runningState || left.recentStatus) === 'running' ? 1 : 0;
    const rightRunning = normalizeJobStatus(right.runningState || right.recentStatus) === 'running' ? 1 : 0;
    if (leftRunning !== rightRunning) {
      return rightRunning - leftRunning;
    }

    const leftStart = left.startTime ? Date.parse(left.startTime) : Number.NEGATIVE_INFINITY;
    const rightStart = right.startTime ? Date.parse(right.startTime) : Number.NEGATIVE_INFINITY;
    if (leftStart !== rightStart) {
      return rightStart - leftStart;
    }

    return left.label.localeCompare(right.label);
  });

  return sorted[0]?.name ?? '';
}

export function JobLogStreamPanel({ jobs }: { jobs: JobLogStreamTarget[] }) {
  const [selectedJobName, setSelectedJobName] = useState('');
  const [logState, setLogState] = useState<LogState>({
    lines: [],
    loading: false,
    error: null,
  });
  const requestControllerRef = useRef<AbortController | null>(null);

  const selectedJob = useMemo(
    () => jobs.find((job) => job.name === selectedJobName) ?? null,
    [jobs, selectedJobName]
  );

  useEffect(() => {
    if (!jobs.length) {
      setSelectedJobName('');
      setLogState({ lines: [], loading: false, error: null });
      return;
    }

    const selectionStillExists = jobs.some((job) => job.name === selectedJobName);
    if (selectionStillExists) {
      return;
    }

    setSelectedJobName(pickDefaultJobName(jobs));
  }, [jobs, selectedJobName]);

  useEffect(() => {
    return () => {
      requestControllerRef.current?.abort();
    };
  }, []);

  useEffect(() => {
    if (!selectedJob) {
      setLogState({ lines: [], loading: false, error: null });
      return;
    }

    requestControllerRef.current?.abort();
    const controller = new AbortController();
    requestControllerRef.current = controller;

    setLogState({ lines: [], loading: true, error: null });
    DataService.getJobLogs(selectedJob.name, { runs: 1 }, controller.signal)
      .then((response) => {
        setLogState({
          lines: extractJobLogLines(response),
          loading: false,
          error: null,
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setLogState({
          lines: [],
          loading: false,
          error: formatSystemStatusText(error),
        });
      });

    return () => {
      controller.abort();
    };
  }, [selectedJobName]);

  useEffect(() => {
    if (!selectedJob) {
      return;
    }

    const topic = buildJobLogTopic(selectedJob.name);
    requestRealtimeSubscription([topic]);
    return () => requestRealtimeUnsubscription([topic]);
  }, [selectedJobName]);

  useEffect(() => {
    if (!selectedJob) {
      return;
    }

    const topic = buildJobLogTopic(selectedJob.name);
    return addConsoleLogStreamListener((detail) => {
      if (detail.topic !== topic) {
        return;
      }

      const incoming = detail.lines
        .map((line) => formatSystemStatusText(line.message))
        .filter((line) => line.length > 0);

      if (!incoming.length) {
        return;
      }

      setLogState((current) => ({
        lines: mergeLogLines(current.lines, incoming),
        loading: false,
        error: null,
      }));
    });
  }, [selectedJobName]);

  if (!jobs.length) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="h-5 w-5" />
            Job Console Stream
          </CardTitle>
          <CardDescription>No Azure jobs are available to monitor.</CardDescription>
        </CardHeader>
      </Card>
    );
  }

  const executionUrl = getAzureJobExecutionsUrl(selectedJob?.jobUrl);
  const portalUrl = normalizeAzurePortalUrl(selectedJob?.jobUrl);
  const status = normalizeJobStatus(selectedJob?.recentStatus || selectedJob?.runningState);

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <CardTitle className="flex items-center gap-2">
              <Activity className="h-5 w-5" />
              Job Console Stream
            </CardTitle>
            <CardDescription>
              Select one job to tail live logs. Only one job stream is active at a time.
            </CardDescription>
          </div>
          <div className="flex items-center gap-2">
            {executionUrl ? (
              <Button asChild variant="outline" size="sm">
                <a href={executionUrl} target="_blank" rel="noreferrer">
                  <ScrollText className="h-4 w-4" />
                  Execution History
                </a>
              </Button>
            ) : null}
            {portalUrl ? (
              <Button asChild variant="ghost" size="sm">
                <a href={portalUrl} target="_blank" rel="noreferrer">
                  <ExternalLink className="h-4 w-4" />
                  Azure
                </a>
              </Button>
            ) : null}
          </div>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-3 lg:grid-cols-[minmax(0,340px)_1fr]">
          <div className="space-y-2">
            <label htmlFor="job-log-stream-select" className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Monitored Job
            </label>
            <Select value={selectedJobName} onValueChange={setSelectedJobName}>
              <SelectTrigger id="job-log-stream-select" aria-label="Monitored job">
                <SelectValue placeholder="Select a job" />
              </SelectTrigger>
              <SelectContent>
                {jobs.map((job) => (
                  <SelectItem key={job.name} value={job.name}>
                    {job.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="grid gap-3 sm:grid-cols-3">
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">Status</div>
              <div className="mt-2 flex items-center gap-2 text-sm">
                {getStatusIcon(status)}
                {getStatusBadge(status)}
              </div>
            </div>
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">Layer / Domain</div>
              <div className="mt-2 text-sm">
                {selectedJob?.layerName || '-'} / {selectedJob?.domainName || '-'}
              </div>
            </div>
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">Last Start</div>
              <div className="mt-2 text-sm">{selectedJob?.startTime ? `${formatTimeAgo(selectedJob.startTime)} ago` : '-'}</div>
            </div>
          </div>
        </div>

        <div className="rounded-md border bg-background">
          <div className="flex items-center justify-between gap-3 border-b px-3 py-2 text-xs font-semibold text-muted-foreground">
            <span>Live Console Tail</span>
            <span className="text-[11px] font-normal text-muted-foreground/80">
              {selectedJob ? selectedJob.name : 'No job selected'}
            </span>
          </div>
          <div className="max-h-80 overflow-auto overflow-x-hidden break-words px-3 py-2 text-xs font-mono leading-relaxed">
            {logState.loading ? (
              <div className="flex items-center gap-2 text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading logs…
              </div>
            ) : null}
            {!logState.loading && logState.error ? (
              <div className="break-words text-destructive">Failed to load logs: {logState.error}</div>
            ) : null}
            {!logState.loading && !logState.error && logState.lines.length === 0 ? (
              <div className="text-muted-foreground">No log output available.</div>
            ) : null}
            {!logState.loading && !logState.error && logState.lines.length > 0 ? (
              <div className="space-y-1">
                {logState.lines.slice(-LOG_LINE_LIMIT).map((line, index) => (
                  <div
                    key={`${selectedJobName}-stream-log-${index}`}
                    className={`whitespace-pre-wrap break-words px-2 py-1 text-foreground/90 ${
                      index % 2 === 0 ? 'bg-muted/30' : 'bg-transparent'
                    }`}
                  >
                    {line}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
