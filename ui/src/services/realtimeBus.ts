export const REALTIME_SUBSCRIBE_EVENT = 'asset-allocation:realtime-subscribe';
export const REALTIME_UNSUBSCRIBE_EVENT = 'asset-allocation:realtime-unsubscribe';
export const CONSOLE_LOG_STREAM_BROWSER_EVENT = 'asset-allocation:console-log-stream';
export const CONSOLE_LOG_STREAM_EVENT_TYPE = 'CONSOLE_LOG_STREAM';

export type ConsoleLogStreamLine = {
  id: string;
  timestamp?: string;
  stream_s?: string | null;
  message: string;
  executionName?: string | null;
};

export type ConsoleLogStreamDetail = {
  topic: string;
  resourceType: 'job' | 'container-app';
  resourceName: string;
  lines: ConsoleLogStreamLine[];
  polledAt?: string;
};

function dispatchRealtimeEvent(eventName: string, topics: string[]): void {
  if (typeof window === 'undefined') return;
  const filtered = topics
    .map((topic) => String(topic || '').trim())
    .filter((topic) => topic.length > 0);
  if (filtered.length === 0) return;
  window.dispatchEvent(new CustomEvent(eventName, { detail: { topics: filtered } }));
}

export function buildJobLogTopic(jobName: string, executionName?: string | null): string {
  const normalizedJobName = String(jobName || '').trim();
  const normalizedExecutionName = String(executionName || '').trim();
  if (!normalizedExecutionName) {
    return `job-logs:${normalizedJobName}`;
  }
  return `job-logs:${normalizedJobName}/executions/${normalizedExecutionName}`;
}

export function buildContainerAppLogTopic(appName: string): string {
  return `container-app-logs:${String(appName || '').trim()}`;
}

export function requestRealtimeSubscription(topics: string[]): void {
  dispatchRealtimeEvent(REALTIME_SUBSCRIBE_EVENT, topics);
}

export function requestRealtimeUnsubscription(topics: string[]): void {
  dispatchRealtimeEvent(REALTIME_UNSUBSCRIBE_EVENT, topics);
}

export function emitConsoleLogStream(detail: ConsoleLogStreamDetail): void {
  if (typeof window === 'undefined') return;
  window.dispatchEvent(new CustomEvent(CONSOLE_LOG_STREAM_BROWSER_EVENT, { detail }));
}

export function addConsoleLogStreamListener(
  listener: (detail: ConsoleLogStreamDetail) => void
): () => void {
  if (typeof window === 'undefined') {
    return () => undefined;
  }

  const handler = (event: Event) => {
    const customEvent = event as CustomEvent<ConsoleLogStreamDetail>;
    if (!customEvent.detail) return;
    listener(customEvent.detail);
  };

  window.addEventListener(CONSOLE_LOG_STREAM_BROWSER_EVENT, handler);
  return () => window.removeEventListener(CONSOLE_LOG_STREAM_BROWSER_EVENT, handler);
}
