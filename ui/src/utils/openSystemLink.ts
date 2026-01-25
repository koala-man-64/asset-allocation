import { backtestApi } from '@/services/backtestApi';

export async function openSystemLink(token: string): Promise<void> {
  const trimmed = String(token ?? '').trim();
  if (!trimmed) {
    console.warn('[SystemLink] openSystemLink called with empty token');
    return;
  }

  // Open a placeholder tab synchronously (avoids popup blockers) then navigate after resolving.
  const placeholder = window.open('about:blank', '_blank', 'noopener,noreferrer');
  console.info('[SystemLink] resolve start', {
    tokenLength: trimmed.length,
    placeholderOpened: Boolean(placeholder),
  });

  try {
    const { url } = await backtestApi.resolveSystemLink(trimmed);
    console.info('[SystemLink] resolve success', { urlHost: (() => { try { return new URL(url).host; } catch { return null; } })() });
    if (placeholder) {
      placeholder.location.href = url;
      return;
    }
    window.open(url, '_blank', 'noopener,noreferrer');
  } catch (error) {
    if (placeholder) {
      placeholder.close();
    }
    console.error('[SystemLink] resolve failed', error);
  }
}
