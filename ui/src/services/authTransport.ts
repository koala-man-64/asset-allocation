type HeadersInit = Headers | [string, string][] | Record<string, string>;

export type AccessTokenProvider = () => Promise<string | null>;

let accessTokenProvider: AccessTokenProvider | null = null;

export function setAccessTokenProvider(provider: AccessTokenProvider | null): void {
  accessTokenProvider = provider;
}

export async function appendAuthHeaders(headersInput?: HeadersInit): Promise<Headers> {
  const headers = new Headers(headersInput as HeadersInit);

  if (accessTokenProvider && !headers.has('Authorization')) {
    const token = await accessTokenProvider();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
  }

  return headers;
}
