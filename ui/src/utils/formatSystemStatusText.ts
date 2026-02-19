const API_ERROR_PREFIX = 'API Error:';
const ESCAPED_NEWLINE_PATTERN = /\\[rn]/g;
const NEWLINE_PATTERN = /\r\n|\r|\n/g;
const MULTISPACE_PATTERN = /\s+/g;
const BULLET_PREFIX_PATTERN = /^\s*[-*]\s+/;
// eslint-disable-next-line no-control-regex
const ANSI_PATTERN = /\u001b\[[0-?]*[ -/]*[@-~]/g;
const ESCAPED_UNICODE_ANSI_PATTERN = /\\u001b\[[0-?]*[ -/]*[@-~]/gi;
const ESCAPED_HEX_ANSI_PATTERN = /\\x1b\[[0-?]*[ -/]*[@-~]/gi;
const ERROR_KEYS = ['detail', 'details', 'message', 'msg', 'error', 'title', 'reason'] as const;

function asText(value: unknown): string {
  if (value instanceof Error) return value.message;
  if (typeof value === 'string') return value;
  if (value === undefined || value === null) return '';
  return String(value);
}

function removeApiPrefix(value: string): string {
  if (!value.startsWith(API_ERROR_PREFIX)) return value;
  const separator = value.indexOf(' - ');
  if (separator < 0) return value;
  return value.slice(separator + 3).trim();
}

function parseJson(value: string): unknown | null {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function extractErrorDetail(payload: unknown): string | null {
  if (payload === undefined || payload === null) return null;
  if (typeof payload === 'string') {
    const trimmed = payload.trim();
    if (!trimmed) return null;
    const nested = parseJson(trimmed);
    if (nested === null) return trimmed;
    return extractErrorDetail(nested) ?? trimmed;
  }
  if (Array.isArray(payload)) {
    const parts = payload.map((item) => extractErrorDetail(item)).filter(Boolean) as string[];
    return parts.length > 0 ? parts.join(' | ') : null;
  }
  if (typeof payload === 'object') {
    const record = payload as Record<string, unknown>;
    for (const key of ERROR_KEYS) {
      const candidate = extractErrorDetail(record[key]);
      if (candidate) return candidate;
    }
    return null;
  }
  return String(payload);
}

function stripAnsi(value: string): string {
  return value
    .replace(ANSI_PATTERN, '')
    .replace(ESCAPED_UNICODE_ANSI_PATTERN, '')
    .replace(ESCAPED_HEX_ANSI_PATTERN, '');
}

export function formatSystemStatusText(value: unknown): string {
  const raw = (extractErrorDetail(value) ?? asText(value)).trim();
  if (!raw) return '';

  const withoutApiPrefix = removeApiPrefix(raw);
  const parseCandidate = withoutApiPrefix.replace(BULLET_PREFIX_PATTERN, '');
  const parsed = parseJson(parseCandidate);
  const extracted = parsed === null ? withoutApiPrefix : extractErrorDetail(parsed) ?? withoutApiPrefix;

  const cleaned = stripAnsi(extracted)
    .replace(ESCAPED_NEWLINE_PATTERN, ' ')
    .replace(NEWLINE_PATTERN, ' ')
    .replace(MULTISPACE_PATTERN, ' ')
    .trim();

  return cleaned || raw;
}
