export interface SanitizeExternalUrlOptions {
  allowedProtocols?: string[];
  allowedHosts?: string[];
  allowRelative?: boolean;
}

function hostMatches(hostname: string, pattern: string): boolean {
  const host = hostname.trim().toLowerCase();
  const rule = pattern.trim().toLowerCase();
  if (!host || !rule) return false;
  if (rule.startsWith('*.')) {
    const suffix = rule.slice(1); // includes leading dot
    return host.endsWith(suffix) && host.length > suffix.length;
  }
  return host === rule;
}

export function sanitizeExternalUrl(
  value: string | null | undefined,
  options: SanitizeExternalUrlOptions = {}
): string {
  const raw = String(value ?? '').trim();
  if (!raw) return '';

  const allowedProtocols = (options.allowedProtocols && options.allowedProtocols.length > 0
    ? options.allowedProtocols
    : ['https:']
  ).map((item) => String(item).trim().toLowerCase());
  const allowedHosts = (options.allowedHosts || [])
    .map((item) => String(item).trim().toLowerCase())
    .filter(Boolean);
  const allowRelative = Boolean(options.allowRelative);

  let parsed: URL;
  try {
    parsed = new URL(raw, window.location.origin);
  } catch {
    return '';
  }

  const isRelative = !/^[a-z][a-z0-9+.-]*:/i.test(raw);
  if (isRelative && !allowRelative) {
    return '';
  }

  const protocol = parsed.protocol.toLowerCase();
  if (!allowedProtocols.includes(protocol)) {
    return '';
  }

  if (allowedHosts.length > 0) {
    const hostname = parsed.hostname.toLowerCase();
    const hostAllowed = allowedHosts.some((pattern) => hostMatches(hostname, pattern));
    if (!hostAllowed) {
      return '';
    }
  }

  return parsed.toString();
}
