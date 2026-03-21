import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import type { AccountInfo, AuthenticationResult } from '@azure/msal-browser';
import { InteractionRequiredAuthError, PublicClientApplication } from '@azure/msal-browser';

import { config } from '@/config';
import { setAccessTokenProvider } from '@/services/authTransport';

const POST_LOGIN_PATH_STORAGE_KEY = 'asset-allocation.post-login-path';
const DEFAULT_POST_LOGIN_PATH = '/system-status';

export interface AuthContextType {
  enabled: boolean;
  ready: boolean;
  authenticated: boolean;
  userLabel: string | null;
  error: string | null;
  signIn: (returnPath?: string) => void;
  signOut: () => void;
}

function isCallbackPath(pathname: string): boolean {
  return pathname === '/auth/callback';
}

function resolveReturnPath(fallback?: string): string {
  const trimmed = String(fallback ?? '').trim();
  if (trimmed) {
    return trimmed;
  }
  if (typeof window === 'undefined') {
    return DEFAULT_POST_LOGIN_PATH;
  }
  const currentPath = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (!currentPath || isCallbackPath(window.location.pathname)) {
    return DEFAULT_POST_LOGIN_PATH;
  }
  return currentPath;
}

function storePostLoginRedirectPath(path: string): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(POST_LOGIN_PATH_STORAGE_KEY, resolveReturnPath(path));
  } catch {
    // Ignore sessionStorage failures and fall back to the default route after login.
  }
}

export function consumePostLoginRedirectPath(): string {
  if (typeof window === 'undefined') {
    return DEFAULT_POST_LOGIN_PATH;
  }
  try {
    const stored = String(window.sessionStorage.getItem(POST_LOGIN_PATH_STORAGE_KEY) ?? '').trim();
    window.sessionStorage.removeItem(POST_LOGIN_PATH_STORAGE_KEY);
    return stored || DEFAULT_POST_LOGIN_PATH;
  } catch {
    return DEFAULT_POST_LOGIN_PATH;
  }
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const oidcClientId = config.oidcClientId;
  const oidcAuthority = config.oidcAuthority;
  const oidcScopes = config.oidcScopes;
  const oidcRedirectUri = config.oidcRedirectUri;

  const enabled = config.oidcEnabled && Boolean(oidcClientId && oidcAuthority && oidcRedirectUri);

  const msal = useMemo(() => {
    if (!enabled) return null;
    return new PublicClientApplication({
      auth: {
        clientId: oidcClientId,
        authority: oidcAuthority,
        redirectUri: oidcRedirectUri,
        postLogoutRedirectUri: oidcRedirectUri
      },
      cache: {
        cacheLocation: 'sessionStorage'
      }
    });
  }, [enabled, oidcAuthority, oidcClientId, oidcRedirectUri]);

  const [account, setAccount] = useState<AccountInfo | null>(null);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!msal) {
      setAccount(null);
      setError(null);
      setReady(true);
      return;
    }

    let cancelled = false;
    setReady(false);
    setError(null);

    msal
      .handleRedirectPromise()
      .then((result: AuthenticationResult | null) => {
        const chosen =
          result?.account ?? msal.getActiveAccount() ?? msal.getAllAccounts()[0] ?? null;
        if (chosen) {
          msal.setActiveAccount(chosen);
        }
        if (!cancelled) {
          setAccount(chosen);
          setReady(true);
        }
      })
      .catch((err) => {
        console.error('OIDC redirect handling failed', err);
        if (!cancelled) {
          setAccount(null);
          setError('OIDC redirect handling failed.');
          setReady(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [msal]);

  useEffect(() => {
    if (!msal) {
      setAccessTokenProvider(null);
      return;
    }

    setAccessTokenProvider(async () => {
      if (!account) return null;
      try {
        const result = await msal.acquireTokenSilent({
          account,
          scopes: oidcScopes
        });
        return result.accessToken || null;
      } catch (err) {
        if (err instanceof InteractionRequiredAuthError) {
          return null;
        }
        console.warn('Failed to acquire access token', err);
        return null;
      }
    });

    return () => {
      setAccessTokenProvider(null);
    };
  }, [msal, account, oidcScopes]);

  const signIn = (returnPath?: string) => {
    if (!msal) return;
    storePostLoginRedirectPath(resolveReturnPath(returnPath));
    void msal.loginRedirect({ scopes: oidcScopes });
  };

  const signOut = () => {
    if (!msal) return;
    void msal.logoutRedirect({ account: account ?? undefined });
  };

  const userLabel = account?.name || account?.username || null;

  return (
    <AuthContext.Provider
      value={{
        enabled,
        ready,
        authenticated: Boolean(account),
        userLabel,
        error,
        signIn,
        signOut
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextType {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
