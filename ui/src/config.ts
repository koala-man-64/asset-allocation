import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';

type RuntimeUiConfig = {
  apiBaseUrl?: string;
  backtestApiBaseUrl?: string;
  oidcEnabled?: boolean;
  authRequired?: boolean;
};

interface WindowWithConfig extends Window {
  __BACKTEST_UI_CONFIG__?: RuntimeUiConfig;
  __API_UI_CONFIG__?: RuntimeUiConfig;
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

const runtime = window as WindowWithConfig;
const runtimeBacktestConfig = runtime.__BACKTEST_UI_CONFIG__ || {};
const runtimeApiConfig = runtime.__API_UI_CONFIG__ || {};

const resolvedApiBaseUrl = normalizeApiBaseUrl(
  runtimeApiConfig.apiBaseUrl ||
    runtimeBacktestConfig.backtestApiBaseUrl ||
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_BACKTEST_API_BASE_URL,
  '/api'
);

runtime.__BACKTEST_UI_CONFIG__ = {
  ...(runtime.__BACKTEST_UI_CONFIG__ || {}),
  backtestApiBaseUrl: resolvedApiBaseUrl
};
runtime.__API_UI_CONFIG__ = {
  ...(runtime.__API_UI_CONFIG__ || {}),
  apiBaseUrl: resolvedApiBaseUrl
};

export const config = {
  apiBaseUrl: resolvedApiBaseUrl,
  oidcEnabled: resolveBoolean(runtimeApiConfig.oidcEnabled, runtimeBacktestConfig.oidcEnabled),
  authRequired: resolveBoolean(runtimeApiConfig.authRequired, runtimeBacktestConfig.authRequired)
};
