export type LogStreamResourceType = 'job' | 'container-app';

export type LogStreamFeedback = {
  tone: 'info' | 'error' | 'none';
  message: string | null;
};

const JOB_NOT_CONFIGURED_PATTERNS = [
  /azure job log retrieval is not configured/i,
  /log analytics is not configured for job log retrieval/i
];

const CONTAINER_APP_NOT_CONFIGURED_PATTERNS = [
  /container app log retrieval is not configured/i,
  /log analytics is not configured for container app log retrieval/i
];

function getNotConfiguredMessage(resourceType: LogStreamResourceType): string {
  if (resourceType === 'job') {
    return 'Live job logs are not configured for this environment.';
  }
  return 'Live console logs are not configured for this environment.';
}

export function getLogStreamFeedback(
  error: string | null | undefined,
  resourceType: LogStreamResourceType
): LogStreamFeedback {
  const message = String(error || '').trim();
  if (!message) {
    return { tone: 'none', message: null };
  }

  const patterns =
    resourceType === 'job' ? JOB_NOT_CONFIGURED_PATTERNS : CONTAINER_APP_NOT_CONFIGURED_PATTERNS;
  if (patterns.some((pattern) => pattern.test(message))) {
    return {
      tone: 'info',
      message: getNotConfiguredMessage(resourceType)
    };
  }

  return {
    tone: 'error',
    message
  };
}
