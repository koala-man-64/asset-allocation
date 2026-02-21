import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';

interface WindowWithConfig extends Window {
  __BACKTEST_UI_CONFIG__?: { backtestApiBaseUrl?: string };
  __API_UI_CONFIG__?: { apiBaseUrl?: string };
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
  apiBaseUrl: resolvedApiBaseUrl
};
