import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import type { AccountInfo, AuthenticationResult } from '@azure/msal-browser';
import { InteractionRequiredAuthError, PublicClientApplication } from '@azure/msal-browser';

import { config } from '@/config';
import { setAccessTokenProvider } from '@/services/authTransport';

const POST_LOGIN_PATH_STORAGE_KEY = 'asset-allocation.post-login-path';
const DEFAULT_POST_LOGIN_PATH = '/system-status';

function describeAuthError(prefix: string, err: unknown): string {
  const detail = err instanceof Error ? err.message.trim() : String(err ?? '').trim();
  return detail ? `${prefix} ${detail}` : prefix;
}

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

  const enabled =
    config.oidcEnabled &&
    Boolean(oidcClientId && oidcAuthority && oidcRedirectUri && oidcScopes.length > 0);

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

  const ensureMsalInitialized = useMemo(() => {
    if (!msal) return null;
    let initializationPromise: Promise<PublicClientApplication> | null = null;
    return () => {
      if (!initializationPromise) {
        initializationPromise = msal.initialize().then(() => msal);
      }
      return initializationPromise;
    };
  }, [msal]);

  const [account, setAccount] = useState<AccountInfo | null>(null);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!ensureMsalInitialized) {
      setAccount(null);
      setError(null);
      setReady(true);
      return;
    }

    let cancelled = false;
    setReady(false);
    setError(null);

    ensureMsalInitialized()
      .then((instance) =>
        instance
          .handleRedirectPromise()
          .then((result: AuthenticationResult | null) => ({ instance, result }))
      )
      .then(({ instance, result }) => {
        const chosen =
          result?.account ?? instance.getActiveAccount() ?? instance.getAllAccounts()[0] ?? null;
        if (chosen) {
          instance.setActiveAccount(chosen);
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
          setError(describeAuthError('OIDC redirect handling failed.', err));
          setReady(true);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [ensureMsalInitialized]);

  useEffect(() => {
    if (!ensureMsalInitialized) {
      setAccessTokenProvider(null);
      return;
    }

    setAccessTokenProvider(async () => {
      if (!account) return null;
      try {
        const instance = await ensureMsalInitialized();
        const result = await instance.acquireTokenSilent({
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
  }, [account, ensureMsalInitialized, oidcScopes]);

  const signIn = (returnPath?: string) => {
    if (!ensureMsalInitialized) return;
    setError(null);
    storePostLoginRedirectPath(resolveReturnPath(returnPath));
    void ensureMsalInitialized()
      .then((instance) =>
        instance.loginRedirect({
          scopes: oidcScopes
        })
      )
      .catch((err) => {
        console.error('OIDC sign-in failed', err);
        setError(describeAuthError('OIDC sign-in could not be started.', err));
      });
  };

  const signOut = () => {
    if (!ensureMsalInitialized) return;
    setError(null);
    void ensureMsalInitialized()
      .then((instance) => instance.logoutRedirect({ account: account ?? undefined }))
      .catch((err) => {
        console.error('OIDC sign-out failed', err);
        setError(describeAuthError('OIDC sign-out could not be completed.', err));
      });
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
