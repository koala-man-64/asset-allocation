const runtime = (window as any).__BACKTEST_UI_CONFIG__ || {};

export const config = {
    apiBaseUrl: (
        runtime.backtestApiBaseUrl ||
        import.meta.env.VITE_API_BASE_URL ||
        import.meta.env.VITE_BACKTEST_API_BASE_URL ||
        'http://localhost:8000'
    ).replace(/\/+$/, ''),
};
