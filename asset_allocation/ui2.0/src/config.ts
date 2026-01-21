export const config = {
    useMockData: import.meta.env.VITE_USE_MOCK_DATA === 'true',
    apiBaseUrl: import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000',
};
