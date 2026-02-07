

export type AccessTokenProvider = () => Promise<string | null>;

let accessTokenProvider: AccessTokenProvider | null = null;

function isTruthy(value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  const text = String(value ?? '')
    .trim()
    .toLowerCase();
  return ['1', 'true', 'yes', 'y', 'on', 't'].includes(text);
}

function shouldSendApiKey(): boolean {
  const mode = String(import.meta.env.VITE_AUTH_MODE ?? '')
    .trim()
    .toLowerCase();
  if (mode === 'api_key') return true;
  if (isTruthy(import.meta.env.VITE_ALLOW_BROWSER_API_KEY)) return true;
  return Boolean(import.meta.env.DEV);
}

function getApiKey(): string {
  return String(import.meta.env.VITE_BACKTEST_API_KEY ?? '').trim();
}

export function setAccessTokenProvider(provider: AccessTokenProvider | null): void {
  accessTokenProvider = provider;
}

export async function appendAuthHeaders(headersInput?: HeadersInit): Promise<Headers> {
  const headers = new Headers(headersInput);

  if (accessTokenProvider && !headers.has('Authorization')) {
    const token = await accessTokenProvider();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
  }

  const apiKey = getApiKey();
  if (apiKey && shouldSendApiKey() && !headers.has('X-API-Key')) {
    headers.set('X-API-Key', apiKey);
  }

  return headers;
}
