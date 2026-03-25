import { useEffect, useMemo, useRef, useState } from 'react';
import { Activity, ExternalLink, Loader2, ScrollText } from 'lucide-react';

import { Button } from '@/app/components/ui/button';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle
} from '@/app/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/app/components/ui/select';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '@/app/components/ui/table';
import type { JobLogRunResponse, JobLogsResponse } from '@/services/apiService';
import { DataService } from '@/services/DataService';
import {
  addConsoleLogStreamListener,
  buildJobLogTopic,
  requestRealtimeSubscription,
  requestRealtimeUnsubscription,
  type ConsoleLogStreamLine
} from '@/services/realtimeBus';
import type { ResourceSignal } from '@/types/strategy';
import {
  effectiveJobStatus,
  formatTimeAgo,
  getAzureJobExecutionsUrl,
  getStatusBadge,
  getStatusIcon,
  normalizeAzureJobName,
  normalizeAzurePortalUrl,
  selectAnchoredJobRun
} from './SystemStatusHelpers';
import { getLogStreamFeedback } from './logStreamFeedback';
import { formatSystemStatusText } from './systemStatusText';

const LOG_LINE_LIMIT = 200;
const LOG_AUTO_SCROLL_BOTTOM_THRESHOLD_PX = 16;
const JOB_USAGE_REFRESH_INTERVAL_MS = 5_000;
const CPU_SIGNAL_NAMES = ['usagenanocores', 'cpupercent', 'cpupercentage', 'cpuusage'];
const MEMORY_SIGNAL_NAMES = [
  'usagebytes',
  'memoryworkingsetbytes',
  'memorybytes',
  'workingsetbytes',
  'memorypercent',
  'memoryusagepercent',
  'memoryusage'
];

export type JobLogStreamTarget = {
  name: string;
  label: string;
  layerName?: string | null;
  domainName?: string | null;
  jobUrl?: string | null;
  runningState?: string | null;
  recentStatus?: string | null;
  startTime?: string | null;
  signals?: ResourceSignal[] | null;
};

type ConsoleTailLine = {
  id: string;
  timestamp?: string | null;
  stream_s?: string | null;
  message: string;
  executionName?: string | null;
};

type LogState = {
  lines: ConsoleTailLine[];
  loading: boolean;
  error: string | null;
};

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function normalizeSignalName(value?: string | null): string {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

function latestSignalTimestamp(signals: ResourceSignal[] | null | undefined): number {
  if (!Array.isArray(signals) || signals.length === 0) {
    return Number.NEGATIVE_INFINITY;
  }

  return signals.reduce((latest, signal) => {
    const timestamp = Date.parse(String(signal?.timestamp || '').trim());
    return Number.isFinite(timestamp) ? Math.max(latest, timestamp) : latest;
  }, Number.NEGATIVE_INFINITY);
}

function preferNewerSignals(
  current: ResourceSignal[] | null | undefined,
  incoming: ResourceSignal[] | null | undefined
): ResourceSignal[] | null {
  const normalizedCurrent = Array.isArray(current) && current.length > 0 ? current : null;
  const normalizedIncoming = Array.isArray(incoming) && incoming.length > 0 ? incoming : null;

  if (!normalizedIncoming) {
    return normalizedCurrent;
  }
  if (!normalizedCurrent) {
    return normalizedIncoming;
  }

  return latestSignalTimestamp(normalizedIncoming) >= latestSignalTimestamp(normalizedCurrent)
    ? normalizedIncoming
    : normalizedCurrent;
}

function findJobResourceSignals(
  jobName: string,
  signals:
    | {
        name?: string | null;
        resourceType?: string | null;
        signals?: ResourceSignal[] | null;
      }[]
    | null
    | undefined
): ResourceSignal[] | null {
  const jobKey = normalizeAzureJobName(jobName);
  if (!jobKey || !Array.isArray(signals) || signals.length === 0) {
    return null;
  }

  const resource = signals.find((item) => {
    if (String(item?.resourceType || '').trim() !== 'Microsoft.App/jobs') {
      return false;
    }
    return normalizeAzureJobName(item?.name) === jobKey;
  });

  return Array.isArray(resource?.signals) && resource.signals.length > 0 ? resource.signals : null;
}

function findUsageSignal(
  signals: ResourceSignal[] | null | undefined,
  preferredNames: string[]
): ResourceSignal | null {
  if (!Array.isArray(signals) || signals.length === 0) {
    return null;
  }

  const normalizedSignals = signals.map((signal) => ({
    signal,
    name: normalizeSignalName(signal?.name)
  }));

  for (const preferredName of preferredNames) {
    const normalizedPreferredName = normalizeSignalName(preferredName);
    const exactMatch = normalizedSignals.find((entry) => entry.name === normalizedPreferredName);
    if (exactMatch) {
      return exactMatch.signal;
    }

    const broadMatch = normalizedSignals.find(
      (entry) =>
        entry.name &&
        (entry.name.includes(normalizedPreferredName) ||
          normalizedPreferredName.includes(entry.name))
    );
    if (broadMatch) {
      return broadMatch.signal;
    }
  }

  return null;
}

function formatMetricNumber(value: number, maximumFractionDigits = 1): string {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits
  }).format(value);
}

function formatPercent(value: number): string {
  return `${formatMetricNumber(value, value >= 10 ? 0 : 1)}%`;
}

function formatBinaryBytes(value: number): string {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const sign = value < 0 ? -1 : 1;
  let scaled = Math.abs(value);
  let unitIndex = 0;

  while (scaled >= 1024 && unitIndex < units.length - 1) {
    scaled /= 1024;
    unitIndex += 1;
  }

  const maximumFractionDigits = scaled >= 100 || unitIndex === 0 ? 0 : scaled >= 10 ? 1 : 2;
  return `${formatMetricNumber(sign * scaled, maximumFractionDigits)} ${units[unitIndex]}`;
}

function formatCpuCoresFromNanocores(value: number): string {
  const cores = value / 1_000_000_000;
  const maximumFractionDigits = cores >= 10 ? 1 : cores >= 1 ? 2 : 3;
  return `${formatMetricNumber(cores, maximumFractionDigits)} cores`;
}

function formatUsageValue(signal: ResourceSignal | null, metric: 'cpu' | 'memory'): string {
  if (!signal || !isFiniteNumber(signal.value)) {
    return '-';
  }

  const unit = normalizeSignalName(signal.unit);
  if (unit.includes('percent')) {
    return formatPercent(signal.value);
  }

  if (metric === 'cpu' && unit.includes('nanocore')) {
    return formatCpuCoresFromNanocores(signal.value);
  }

  if (metric === 'memory' && unit.includes('byte')) {
    return formatBinaryBytes(signal.value);
  }

  const suffix = String(signal.unit || '').trim();
  const value = formatMetricNumber(signal.value);
  return suffix ? `${value} ${suffix}` : value;
}

function sortJobsForDisplay(jobs: JobLogStreamTarget[]): JobLogStreamTarget[] {
  return [...jobs].sort((left, right) => {
    const labelComparison = left.label.localeCompare(right.label, undefined, {
      numeric: true,
      sensitivity: 'base'
    });
    if (labelComparison !== 0) {
      return labelComparison;
    }

    return left.name.localeCompare(right.name, undefined, {
      numeric: true,
      sensitivity: 'base'
    });
  });
}

function normalizeLogLine(
  line: Pick<ConsoleTailLine, 'message' | 'timestamp' | 'stream_s' | 'executionName'> & {
    id?: string | null;
  }
): ConsoleTailLine | null {
  const message = formatSystemStatusText(line.message);
  if (!message) {
    return null;
  }

  const timestamp = typeof line.timestamp === 'string' ? line.timestamp.trim() || null : null;
  const stream_s = typeof line.stream_s === 'string' ? line.stream_s.trim() || null : null;
  const executionName =
    typeof line.executionName === 'string' ? line.executionName.trim() || null : null;
  const id =
    typeof line.id === 'string' && line.id.trim()
      ? line.id.trim()
      : [timestamp || '', executionName || '', stream_s || '', message].join('|');

  return {
    id,
    timestamp,
    stream_s,
    message,
    executionName
  };
}

function mergeLogLines(
  existing: ConsoleTailLine[],
  incoming: ConsoleTailLine[],
  limit = LOG_LINE_LIMIT
): ConsoleTailLine[] {
  const next = [...existing];
  const windowed = new Set(existing.slice(-limit).map((line) => line.id));

  incoming.forEach((line) => {
    if (!line.message || windowed.has(line.id)) {
      return;
    }
    next.push(line);
    windowed.add(line.id);
    while (next.length > limit) {
      const removed = next.shift();
      if (removed && !next.some((candidate) => candidate.id === removed.id)) {
        windowed.delete(removed.id);
      }
    }
  });

  return next.slice(-limit);
}

function extractAnchoredJobLogRun(response: JobLogsResponse): JobLogRunResponse | null {
  return selectAnchoredJobRun(response?.runs ?? []);
}

function extractJobLogLines(response: JobLogsResponse): ConsoleTailLine[] {
  const run = extractAnchoredJobLogRun(response);
  if (!run) {
    return [];
  }

  if (Array.isArray(run.consoleLogs) && run.consoleLogs.length > 0) {
    return run.consoleLogs
      .map((line) => normalizeLogLine(line))
      .filter((line): line is ConsoleTailLine => line !== null)
      .slice(-LOG_LINE_LIMIT);
  }

  return (run.tail ?? [])
    .map((line) =>
      normalizeLogLine({
        message: String(line || ''),
        executionName: run.executionName,
        timestamp: run.startTime ?? null
      })
    )
    .filter((line): line is ConsoleTailLine => line !== null)
    .slice(-LOG_LINE_LIMIT);
}

function extractAnchoredExecutionName(response: JobLogsResponse): string | null {
  const run = extractAnchoredJobLogRun(response);
  const executionName = typeof run?.executionName === 'string' ? run.executionName.trim() : '';
  if (executionName) {
    return executionName;
  }

  if (Array.isArray(run?.consoleLogs)) {
    for (const entry of run.consoleLogs) {
      const lineExecutionName =
        typeof entry?.executionName === 'string' ? entry.executionName.trim() : '';
      if (lineExecutionName) {
        return lineExecutionName;
      }
    }
  }

  return null;
}

function formatConsoleTimestamp(timestamp?: string | null): string | null {
  const raw = String(timestamp || '').trim();
  if (!raw) {
    return null;
  }
  const parsed = Date.parse(raw);
  if (!Number.isFinite(parsed)) {
    return raw;
  }
  return new Intl.DateTimeFormat('en-US', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(new Date(parsed));
}

function isNearBottom(
  element: HTMLElement,
  thresholdPx = LOG_AUTO_SCROLL_BOTTOM_THRESHOLD_PX
): boolean {
  const remaining = element.scrollHeight - element.scrollTop - element.clientHeight;
  return remaining <= thresholdPx;
}

export function JobLogStreamPanel({ jobs }: { jobs: JobLogStreamTarget[] }) {
  const [selectedJobName, setSelectedJobName] = useState('');
  const [selectedExecutionName, setSelectedExecutionName] = useState<string | null>(null);
  const [liveSignals, setLiveSignals] = useState<ResourceSignal[] | null>(null);
  const [logState, setLogState] = useState<LogState>({
    lines: [],
    loading: false,
    error: null
  });
  const requestControllerRef = useRef<AbortController | null>(null);
  const usageRequestControllerRef = useRef<AbortController | null>(null);
  const logViewportRef = useRef<HTMLDivElement | null>(null);
  const shouldAutoScrollRef = useRef(true);
  const sortedJobs = useMemo(() => sortJobsForDisplay(jobs), [jobs]);

  const selectedJob = useMemo(
    () => sortedJobs.find((job) => job.name === selectedJobName) ?? null,
    [sortedJobs, selectedJobName]
  );
  const selectedJobStartTime = selectedJob?.startTime ?? null;
  const selectedJobTopic =
    selectedJobName && selectedExecutionName
      ? buildJobLogTopic(selectedJobName, selectedExecutionName)
      : null;
  const logFeedback = getLogStreamFeedback(logState.error, 'job');

  useEffect(() => {
    if (!sortedJobs.length) {
      setSelectedJobName('');
      setSelectedExecutionName(null);
      setLogState({ lines: [], loading: false, error: null });
      return;
    }

    const selectionStillExists = sortedJobs.some((job) => job.name === selectedJobName);
    if (selectionStillExists) {
      return;
    }

    setSelectedJobName(sortedJobs[0]?.name ?? '');
  }, [sortedJobs, selectedJobName]);

  useEffect(() => {
    return () => {
      requestControllerRef.current?.abort();
      usageRequestControllerRef.current?.abort();
    };
  }, []);

  useEffect(() => {
    shouldAutoScrollRef.current = true;
    setSelectedExecutionName(null);
    setLiveSignals(null);
  }, [selectedJobName, selectedJobStartTime]);

  useEffect(() => {
    setLiveSignals((current) => preferNewerSignals(current, selectedJob?.signals ?? null));
  }, [selectedJob?.signals, selectedJobName]);

  useEffect(() => {
    if (!selectedJobName) {
      setLogState({ lines: [], loading: false, error: null });
      return;
    }

    requestControllerRef.current?.abort();
    const controller = new AbortController();
    requestControllerRef.current = controller;

    setLogState({ lines: [], loading: true, error: null });
    DataService.getJobLogs(selectedJobName, { runs: 1 }, controller.signal)
      .then((response) => {
        setSelectedExecutionName(extractAnchoredExecutionName(response));
        setLogState({
          lines: extractJobLogLines(response),
          loading: false,
          error: null
        });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) {
          return;
        }
        setSelectedExecutionName(null);
        setLogState({
          lines: [],
          loading: false,
          error: formatSystemStatusText(error)
        });
      });

    return () => {
      controller.abort();
    };
  }, [selectedJobName, selectedJobStartTime]);

  useEffect(() => {
    if (!selectedJobTopic) {
      return;
    }

    requestRealtimeSubscription([selectedJobTopic]);
    return () => requestRealtimeUnsubscription([selectedJobTopic]);
  }, [selectedJobTopic]);

  useEffect(() => {
    if (!selectedJobTopic) {
      return;
    }

    return addConsoleLogStreamListener((detail) => {
      if (detail.topic !== selectedJobTopic) {
        return;
      }

      const incoming = detail.lines
        .map((line: ConsoleLogStreamLine) => normalizeLogLine(line))
        .filter((line): line is ConsoleTailLine => line !== null);

      if (!incoming.length) {
        return;
      }

      setLogState((current) => ({
        lines: mergeLogLines(current.lines, incoming),
        loading: false,
        error: null
      }));
    });
  }, [selectedJobTopic]);

  useEffect(() => {
    const viewport = logViewportRef.current;
    if (!viewport || !shouldAutoScrollRef.current) {
      return;
    }
    viewport.scrollTop = viewport.scrollHeight;
  }, [logState.lines, logState.loading, logState.error, selectedJobName]);

  useEffect(() => {
    if (!selectedJobName) {
      setLiveSignals(null);
      return;
    }

    let isDisposed = false;

    const refreshSignals = async () => {
      usageRequestControllerRef.current?.abort();
      const controller = new AbortController();
      usageRequestControllerRef.current = controller;

      try {
        const systemHealth = await DataService.getSystemHealth({ refresh: true });
        if (isDisposed || controller.signal.aborted) {
          return;
        }

        const nextSignals = findJobResourceSignals(selectedJobName, systemHealth.resources);
        setLiveSignals((current) => preferNewerSignals(current, nextSignals));
      } catch (error: unknown) {
        if (!controller.signal.aborted) {
          console.debug('[JobLogStreamPanel] live usage refresh failed', error);
        }
      }
    };

    void refreshSignals();
    const intervalHandle = window.setInterval(() => {
      void refreshSignals();
    }, JOB_USAGE_REFRESH_INTERVAL_MS);

    return () => {
      isDisposed = true;
      window.clearInterval(intervalHandle);
      usageRequestControllerRef.current?.abort();
    };
  }, [selectedJobName]);

  if (!sortedJobs.length) {
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
  const status = effectiveJobStatus(selectedJob?.recentStatus, selectedJob?.runningState);
  const usageSignals = preferNewerSignals(selectedJob?.signals, liveSignals);
  const cpuSignal = findUsageSignal(usageSignals, CPU_SIGNAL_NAMES);
  const memorySignal = findUsageSignal(usageSignals, MEMORY_SIGNAL_NAMES);

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
            <label
              htmlFor="job-log-stream-select"
              className="text-xs font-semibold uppercase tracking-wide text-muted-foreground"
            >
              Monitored Job
            </label>
            <Select value={selectedJobName} onValueChange={setSelectedJobName}>
              <SelectTrigger id="job-log-stream-select" aria-label="Monitored job">
                <SelectValue placeholder="Select a job" />
              </SelectTrigger>
              <SelectContent>
                {sortedJobs.map((job) => (
                  <SelectItem key={job.name} value={job.name}>
                    {job.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                Status
              </div>
              <div className="mt-2 flex items-center gap-2 text-sm">
                {getStatusIcon(status)}
                {getStatusBadge(status)}
              </div>
            </div>
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                Layer / Domain
              </div>
              <div className="mt-2 text-sm">
                {selectedJob?.layerName || '-'} / {selectedJob?.domainName || '-'}
              </div>
            </div>
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                Run Start
              </div>
              <div className="mt-2 text-sm">
                {selectedJob?.startTime ? `${formatTimeAgo(selectedJob.startTime)} ago` : '-'}
              </div>
            </div>
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                CPU Usage
              </div>
              <div className="mt-2 text-sm font-medium">{formatUsageValue(cpuSignal, 'cpu')}</div>
            </div>
            <div className="rounded-md border bg-muted/20 p-3">
              <div className="text-[11px] uppercase tracking-wider text-muted-foreground">
                Memory Usage
              </div>
              <div className="mt-2 text-sm font-medium">
                {formatUsageValue(memorySignal, 'memory')}
              </div>
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
          <div
            ref={logViewportRef}
            className="max-h-80 overflow-auto overflow-x-auto px-3 py-2 text-xs font-mono leading-relaxed"
            data-testid="job-log-stream-tail"
            onScroll={(event) => {
              shouldAutoScrollRef.current = isNearBottom(event.currentTarget);
            }}
          >
            {logState.loading ? (
              <div className="flex items-center gap-2 text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Loading logs…
              </div>
            ) : null}
            {!logState.loading && logFeedback.tone === 'error' && logFeedback.message ? (
              <div className="break-words text-destructive">
                Failed to load logs: {logFeedback.message}
              </div>
            ) : null}
            {!logState.loading && logFeedback.tone === 'info' && logFeedback.message ? (
              <div className="text-muted-foreground">{logFeedback.message}</div>
            ) : null}
            {!logState.loading && logFeedback.tone === 'none' && logState.lines.length === 0 ? (
              <div className="text-muted-foreground">No log output available.</div>
            ) : null}
            {!logState.loading && logFeedback.tone === 'none' && logState.lines.length > 0 ? (
              <Table className="min-w-full text-xs">
                <TableHeader>
                  <TableRow className="hover:[&>td]:bg-transparent">
                    <TableHead>timestamp</TableHead>
                    <TableHead>stream_s</TableHead>
                    <TableHead className="min-w-[28rem]">message</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {logState.lines.slice(-LOG_LINE_LIMIT).map((line, index) => (
                    <TableRow key={`${selectedJobName}-stream-log-${line.id || index}`}>
                      <TableCell className="align-top text-[11px] text-muted-foreground">
                        {formatConsoleTimestamp(line.timestamp) || '-'}
                      </TableCell>
                      <TableCell className="align-top uppercase tracking-wide">
                        {line.stream_s || '-'}
                      </TableCell>
                      <TableCell className="min-w-[28rem] whitespace-pre-wrap break-words align-top text-foreground/90">
                        {line.message}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            ) : null}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
