import { backtestApi } from '@/services/backtestApi';

export async function openSystemLink(token: string): Promise<void> {
  const trimmed = String(token ?? '').trim();
  if (!trimmed) {
    console.warn('[SystemLink] openSystemLink called with empty token');
    return;
  }

  console.info('[SystemLink] resolve start', {
    tokenLength: trimmed.length,
  });

  try {
    const { url } = await backtestApi.resolveSystemLink(trimmed);
    console.info('[SystemLink] resolve success', { urlHost: (() => { try { return new URL(url).host; } catch { return null; } })() });
    const opened = window.open(url, '_blank', 'noopener,noreferrer');
    if (!opened) {
      window.location.assign(url);
    }
  } catch (error) {
    console.error('[SystemLink] resolve failed', error);
  }
}
