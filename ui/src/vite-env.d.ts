/// <reference types="vite/client" />

interface Window {
  __API_UI_CONFIG__?: {
    apiBaseUrl?: string;
    oidcClientId?: string;
    oidcAuthority?: string;
    oidcScopes?: string[] | string;
    oidcRedirectUri?: string;
    oidcEnabled?: boolean;
    authRequired?: boolean;
  };
}
