/// <reference types="vite/client" />

interface Window {
  __BACKTEST_UI_CONFIG__?: {
    backtestApiBaseUrl?: string;
    debugApi?: string;
    oidcClientId?: string;
    oidcAuthority?: string;
    oidcScopes?: string[] | string;
    oidcRedirectUri?: string;
    oidcEnabled?: boolean;
    apiKeyAuthConfigured?: boolean;
    authRequired?: boolean;
  };
  __API_UI_CONFIG__?: {
    apiBaseUrl?: string;
    oidcClientId?: string;
    oidcAuthority?: string;
    oidcScopes?: string[] | string;
    oidcRedirectUri?: string;
    oidcEnabled?: boolean;
    apiKeyAuthConfigured?: boolean;
    authRequired?: boolean;
  };
}
