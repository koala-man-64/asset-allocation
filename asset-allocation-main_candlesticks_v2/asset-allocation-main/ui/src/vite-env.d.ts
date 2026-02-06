/// <reference types="vite/client" />

interface Window {
  __BACKTEST_UI_CONFIG__?: {
    backtestApiBaseUrl?: string;
    authMode?: string;
    oidcClientId?: string;
    oidcAuthority?: string;
    oidcScopes?: string[] | string;
  };
}
