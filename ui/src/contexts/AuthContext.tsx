import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import type { AccountInfo, AuthenticationResult } from '@azure/msal-browser';
import { InteractionRequiredAuthError, PublicClientApplication } from '@azure/msal-browser';

import { setAccessTokenProvider } from '@/services/authTransport';

type AuthMode = 'none' | 'oidc' | 'api_key';

type RuntimeConfig = {
  authMode?: string;
  oidcClientId?: string;
  oidcAuthority?: string;
  oidcScopes?: string[] | string;
};

function getRuntimeConfig(): RuntimeConfig {
  return (window.__BACKTEST_UI_CONFIG__ as RuntimeConfig | undefined) ?? {};
}

function parseScopes(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw
      .map(String)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  if (typeof raw !== 'string') return [];
  const normalized = raw.replace(/,/g, ' ').trim();
  return normalized ? normalized.split(/\s+/).filter(Boolean) : [];
}

function parseAuthMode(rawValue: unknown): AuthMode {
  const raw = String(rawValue ?? '')
    .trim()
    .toLowerCase();
  if (!raw) return 'none';
  if (raw === 'oidc') return 'oidc';
  if (raw === 'api_key') return 'api_key';
  return 'none';
}

export interface AuthContextType {
  enabled: boolean;
  authenticated: boolean;
  userLabel: string | null;
  signIn: () => void;
  signOut: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const runtime = getRuntimeConfig();
  const mode = parseAuthMode(runtime.authMode ?? import.meta.env.VITE_AUTH_MODE);
  const oidcClientId = String(
    runtime.oidcClientId ?? import.meta.env.VITE_OIDC_CLIENT_ID ?? ''
  ).trim();
  const oidcAuthority = String(
    runtime.oidcAuthority ?? import.meta.env.VITE_OIDC_AUTHORITY ?? ''
  ).trim();
  const oidcScopesRaw = runtime.oidcScopes ?? import.meta.env.VITE_OIDC_SCOPES;
  const oidcScopes = useMemo(() => parseScopes(oidcScopesRaw), [oidcScopesRaw]);

  const enabled = mode === 'oidc' && Boolean(oidcClientId && oidcAuthority);

  const msal = useMemo(() => {
    if (!enabled) return null;
    return new PublicClientApplication({
      auth: {
        clientId: oidcClientId,
        authority: oidcAuthority,
        redirectUri: window.location.origin,
        postLogoutRedirectUri: window.location.origin
      },
      cache: {
        cacheLocation: 'sessionStorage'
      }
    });
  }, [enabled, oidcClientId, oidcAuthority]);

  const [account, setAccount] = useState<AccountInfo | null>(null);

  useEffect(() => {
    if (!msal) {
      setAccount(null);
      return;
    }

    let cancelled = false;

    msal
      .handleRedirectPromise()
      .then((result: AuthenticationResult | null) => {
        const chosen =
          result?.account ?? msal.getActiveAccount() ?? msal.getAllAccounts()[0] ?? null;
        if (chosen) {
          msal.setActiveAccount(chosen);
        }
        if (!cancelled) setAccount(chosen);
      })
      .catch((err) => {
        console.error('OIDC redirect handling failed', err);
        if (!cancelled) setAccount(null);
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

  const signIn = () => {
    if (!msal) return;
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
        authenticated: Boolean(account),
        userLabel,
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
