import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';

type RuntimeUiConfig = {
  apiBaseUrl?: string;
  oidcEnabled?: boolean;
  authRequired?: boolean;
  oidcAuthority?: string;
  oidcClientId?: string;
  oidcScopes?: string[] | string;
  oidcRedirectUri?: string;
};

declare global {
  interface Window {
    __API_UI_CONFIG__?: RuntimeUiConfig;
  }
}

function resolveBoolean(...values: unknown[]): boolean {
  for (const value of values) {
    if (typeof value === 'boolean') {
      return value;
    }
    const normalized = String(value ?? '')
      .trim()
      .toLowerCase();
    if (['1', 'true', 'yes', 'y', 'on', 't'].includes(normalized)) {
      return true;
    }
    if (['0', 'false', 'no', 'n', 'off', 'f'].includes(normalized)) {
      return false;
    }
  }
  return false;
}

function resolveString(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value !== 'string') {
      continue;
    }
    const normalized = value.trim();
    if (normalized) {
      return normalized;
    }
  }
  return '';
}

function resolveScopes(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw
      .map(String)
      .map((scope) => scope.trim())
      .filter(Boolean);
  }

  if (typeof raw !== 'string') {
    return [];
  }

  const normalized = raw.replace(/,/g, ' ').trim();
  return normalized ? normalized.split(/\s+/).filter(Boolean) : [];
}

const runtimeConfig = typeof window === 'undefined' ? {} : window.__API_UI_CONFIG__ || {};

const apiBaseUrl = normalizeApiBaseUrl(
  runtimeConfig.apiBaseUrl || import.meta.env.VITE_API_BASE_URL,
  '/api'
);
const oidcAuthority = resolveString(runtimeConfig.oidcAuthority, import.meta.env.VITE_OIDC_AUTHORITY);
const oidcClientId = resolveString(runtimeConfig.oidcClientId, import.meta.env.VITE_OIDC_CLIENT_ID);
const oidcRedirectUri = resolveString(runtimeConfig.oidcRedirectUri);
const oidcScopes = resolveScopes(runtimeConfig.oidcScopes ?? import.meta.env.VITE_OIDC_SCOPES);
const oidcEnabled = resolveBoolean(
  runtimeConfig.oidcEnabled,
  Boolean(oidcAuthority && oidcClientId && oidcRedirectUri)
);
const authRequired = resolveBoolean(runtimeConfig.authRequired);

if (typeof window !== 'undefined') {
  const nextRuntimeConfig: RuntimeUiConfig = {
    ...(window.__API_UI_CONFIG__ || {}),
    apiBaseUrl,
    oidcScopes,
    oidcEnabled,
    authRequired
  };
  if (oidcAuthority) {
    nextRuntimeConfig.oidcAuthority = oidcAuthority;
  }
  if (oidcClientId) {
    nextRuntimeConfig.oidcClientId = oidcClientId;
  }
  if (oidcRedirectUri) {
    nextRuntimeConfig.oidcRedirectUri = oidcRedirectUri;
  }
  window.__API_UI_CONFIG__ = nextRuntimeConfig;
}

export const config = {
  apiBaseUrl,
  oidcEnabled,
  authRequired,
  oidcAuthority,
  oidcClientId,
  oidcScopes,
  oidcRedirectUri
};
