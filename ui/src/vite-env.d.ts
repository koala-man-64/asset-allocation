/// <reference types="vite/client" />

interface Window {
  __BACKTEST_UI_CONFIG__?: {
    backtestApiBaseUrl?: string;
    debugApi?: string;
    authMode?: string;
    oidcClientId?: string;
    oidcAuthority?: string;
    oidcScopes?: string[] | string;
  };
  __API_UI_CONFIG__?: {
    apiBaseUrl?: string;
  };
}
