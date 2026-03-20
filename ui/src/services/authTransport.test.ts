import { afterEach, describe, expect, it } from 'vitest';

import { appendAuthHeaders, setAccessTokenProvider } from './authTransport';

describe('authTransport bearer auth support', () => {
  afterEach(() => {
    setAccessTokenProvider(null);
  });

  it('appends the access token to outgoing headers', async () => {
    setAccessTokenProvider(async () => 'oidc-token');

    const headers = await appendAuthHeaders();

    expect(headers.get('Authorization')).toBe('Bearer oidc-token');
  });

  it('does not overwrite an existing Authorization header', async () => {
    setAccessTokenProvider(async () => 'oidc-token');

    const headers = await appendAuthHeaders({ Authorization: 'Bearer existing-token' });

    expect(headers.get('Authorization')).toBe('Bearer existing-token');
  });

  it('returns the original headers when no access token is available', async () => {
    setAccessTokenProvider(async () => null);

    const headers = await appendAuthHeaders({ 'X-Test': 'value' });

    expect(headers.get('X-Test')).toBe('value');
    expect(headers.has('Authorization')).toBe(false);
  });
});
