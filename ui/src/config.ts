import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';

interface WindowWithConfig extends Window {
    __BACKTEST_UI_CONFIG__?: { backtestApiBaseUrl?: string };
}
const runtime = (window as WindowWithConfig).__BACKTEST_UI_CONFIG__ || {};

const resolvedApiBaseUrl = normalizeApiBaseUrl(
    runtime.backtestApiBaseUrl ||
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_BACKTEST_API_BASE_URL,
);

console.info('[UI Config] apiBaseUrl resolved', {
    apiBaseUrl: resolvedApiBaseUrl,
    runtimeApiBaseUrl: runtime.backtestApiBaseUrl,
    envApiBaseUrl: import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_BACKTEST_API_BASE_URL,
});

export const config = {
    apiBaseUrl: resolvedApiBaseUrl,
};
