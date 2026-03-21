import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { AuthProvider, useAuth } from '../AuthContext';

const POST_LOGIN_PATH_STORAGE_KEY = 'asset-allocation.post-login-path';

const mockMsal = vi.hoisted(() => ({
  initialize: vi.fn(),
  handleRedirectPromise: vi.fn(),
  getActiveAccount: vi.fn(),
  getAllAccounts: vi.fn(),
  setActiveAccount: vi.fn(),
  acquireTokenSilent: vi.fn(),
  loginRedirect: vi.fn(),
  logoutRedirect: vi.fn()
}));

vi.mock('@/config', () => ({
  config: {
    oidcEnabled: true,
    oidcClientId: 'spa-client-id',
    oidcAuthority: 'https://login.microsoftonline.com/tenant-id',
    oidcScopes: ['api://asset-allocation-api/user_impersonation'],
    oidcRedirectUri: 'https://asset-allocation.example.com/auth/callback'
  }
}));

vi.mock('@azure/msal-browser', () => ({
  InteractionRequiredAuthError: class InteractionRequiredAuthError extends Error {},
  PublicClientApplication: vi.fn(function PublicClientApplication() {
    return mockMsal;
  })
}));

function Harness() {
  const auth = useAuth();

  return (
    <div>
      <div data-testid="ready">{String(auth.ready)}</div>
      <div data-testid="error">{auth.error ?? ''}</div>
      <button onClick={() => auth.signIn('/system-status')}>Sign in</button>
    </div>
  );
}

describe('AuthProvider', () => {
  beforeEach(() => {
    window.sessionStorage.clear();
    mockMsal.initialize.mockReset();
    mockMsal.handleRedirectPromise.mockReset();
    mockMsal.getActiveAccount.mockReset();
    mockMsal.getAllAccounts.mockReset();
    mockMsal.setActiveAccount.mockReset();
    mockMsal.acquireTokenSilent.mockReset();
    mockMsal.loginRedirect.mockReset();
    mockMsal.logoutRedirect.mockReset();

    mockMsal.initialize.mockResolvedValue(undefined);
    mockMsal.handleRedirectPromise.mockResolvedValue(null);
    mockMsal.getActiveAccount.mockReturnValue(null);
    mockMsal.getAllAccounts.mockReturnValue([]);
    mockMsal.loginRedirect.mockResolvedValue(undefined);
    mockMsal.logoutRedirect.mockResolvedValue(undefined);
  });

  it('initializes msal before starting the redirect flow', async () => {
    render(
      <AuthProvider>
        <Harness />
      </AuthProvider>
    );

    await waitFor(() => {
      expect(screen.getByTestId('ready')).toHaveTextContent('true');
    });

    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }));

    await waitFor(() => {
      expect(mockMsal.initialize).toHaveBeenCalled();
      expect(mockMsal.loginRedirect).toHaveBeenCalledWith({
        scopes: ['api://asset-allocation-api/user_impersonation']
      });
    });
    expect(window.sessionStorage.getItem(POST_LOGIN_PATH_STORAGE_KEY)).toBe('/system-status');
  });

  it('surfaces redirect startup failures', async () => {
    mockMsal.loginRedirect.mockRejectedValueOnce(new Error('popup blocked'));

    render(
      <AuthProvider>
        <Harness />
      </AuthProvider>
    );

    await waitFor(() => {
      expect(screen.getByTestId('ready')).toHaveTextContent('true');
    });

    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }));

    await waitFor(() => {
      expect(screen.getByTestId('error')).toHaveTextContent(
        'OIDC sign-in could not be started. popup blocked'
      );
    });
  });
});
