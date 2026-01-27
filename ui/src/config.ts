import { normalizeApiBaseUrl } from '@/utils/apiBaseUrl';

interface WindowWithConfig extends Window {
    __API_UI_CONFIG__?: { apiBaseUrl?: string };
}
const runtime = (window as WindowWithConfig).__API_UI_CONFIG__ || {};

const resolvedApiBaseUrl = normalizeApiBaseUrl(
    runtime.apiBaseUrl ||
    import.meta.env.VITE_API_BASE_URL,
);

console.info('[UI Config] apiBaseUrl resolved', {
    apiBaseUrl: resolvedApiBaseUrl,
    runtimeApiBaseUrl: runtime.apiBaseUrl,
    envApiBaseUrl: import.meta.env.VITE_API_BASE_URL,
});

export const config = {
    apiBaseUrl: resolvedApiBaseUrl,
};
