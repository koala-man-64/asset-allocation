export function normalizeApiBaseUrl(value: unknown, fallback: string = '/api'): string {
  const raw = typeof value === 'string' ? value : '';
  const trimmed = raw.trim();
  if (!trimmed) return fallback;

  const withoutTrailingSlashes = trimmed.replace(/\/+$/, '');
  if (!withoutTrailingSlashes) return fallback;

  if (withoutTrailingSlashes === 'api') return '/api';

  if (/^https?:\/\//i.test(withoutTrailingSlashes)) {
    try {
      const url = new URL(withoutTrailingSlashes);
      const pathname = url.pathname.replace(/\/+$/, '');
      if (!pathname || pathname === '/') return `${url.origin}/api`;
      return `${url.origin}${pathname}`;
    } catch {
      return withoutTrailingSlashes;
    }
  }

  if (withoutTrailingSlashes === '/') return '/api';
  return withoutTrailingSlashes.startsWith('/')
    ? withoutTrailingSlashes
    : `/${withoutTrailingSlashes}`;
}

export function toWebSocketBaseUrl(apiBaseUrl: string): string {
  return apiBaseUrl.replace(/^https?:\/\//i, (match) =>
    match.toLowerCase().startsWith('https') ? 'wss://' : 'ws://'
  );
}
