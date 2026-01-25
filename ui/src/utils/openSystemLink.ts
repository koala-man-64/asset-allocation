import { backtestApi } from '@/services/backtestApi';

export async function openSystemLink(token: string): Promise<void> {
  const trimmed = String(token ?? '').trim();
  if (!trimmed) {
    console.warn('[SystemLink] openSystemLink called with empty token');
    return;
  }

  // Open a placeholder tab synchronously (avoids popup blockers) then navigate after resolving.
  // Avoid `noopener` here so Safari returns a usable window handle (otherwise it can open a blank tab and return null).
  const placeholder = window.open('about:blank', '_blank');
  if (placeholder) {
    // Mitigate reverse-tabnabbing risk before navigating cross-origin.
    try {
      placeholder.opener = null;
    } catch {
      // Ignore if browser prevents it.
    }
  }
  console.info('[SystemLink] resolve start', {
    tokenLength: trimmed.length,
    placeholderOpened: Boolean(placeholder),
  });

  try {
    const { url } = await backtestApi.resolveSystemLink(trimmed);
    console.info('[SystemLink] resolve success', { urlHost: (() => { try { return new URL(url).host; } catch { return null; } })() });
    if (placeholder) {
      placeholder.location.replace(url);
      return;
    }
    const opened = window.open(url, '_blank', 'noopener,noreferrer');
    if (!opened) {
      window.location.assign(url);
    }
  } catch (error) {
    if (placeholder) {
      placeholder.close();
    }
    console.error('[SystemLink] resolve failed', error);
  }
}
