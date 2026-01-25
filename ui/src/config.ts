import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';

const runtime = (window as any).__BACKTEST_UI_CONFIG__ || {};

export const config = {
    apiBaseUrl: normalizeApiBaseUrl(
        runtime.backtestApiBaseUrl ||
        import.meta.env.VITE_API_BASE_URL ||
        import.meta.env.VITE_BACKTEST_API_BASE_URL ||
        '/api'
    ).replace(/\/+$/, ''),
};
