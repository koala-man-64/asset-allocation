import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
// Mocking AppContent since App.tsx usually has providers which we might duplicate or want to skip
// Actually App.tsx has AuthProvider and QueryProvider. 
// renderWithProviders ADDS QueryProvider and Router.
// App.tsx ADDS AuthProvider, QueryProvider, AppProvider.
// If we render <App />, we get double providers.
// Better to smoke test a smaller part or mock the providers in App?
// For a true smoke test of "App works", we usually want to see if it renders without crashing.
// Let's import App. If it crashes due to double providers, we'll know.
// Actually, App.tsx wraps AppContent. AppContent contains the Routes.
// Our renderWithProviders already contains Router. 
// If AppContent uses `useNavigate` immediately, it needs Router context.
// App composes providers -> AppContent.
// If I test `App`, I am wrapping providers in providers.
// Let's test `App` directly first. If it fails, I'll specificy components.
// Wait, `App` has `BrowserRouter`? No, `App` only has Context Providers. `AppContent` has `useNavigate` but where is `BrowserRouter`?
// Let's check App.tsx again.

// Checking App.tsx imports...
// import { Routes, Route ... } from 'react-router-dom';
// AppContent uses `useNavigate()`.
// App returns AuthProvider > QueryProvider > AppProvider > AppContent.
// MISSING: BrowserRouter! 
// Wait, where is BrowserRouter in the main code?
// `main.tsx` usually has it. 
// If App.tsx doesn't have BrowserRouter, then `AppContent` will fail if rendered inside `App` without Router context from outside.
// So `renderWithProviders(<App />)` is actually correct because `renderWithProviders` adds the Router!

import App from '../App';

vi.mock('@/hooks/useRealtime', () => ({
    useRealtime: () => undefined,
}));

vi.mock('@/hooks/useDataQueries', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@/hooks/useDataQueries')>();

    return {
        ...actual,
        useSystemHealthQuery: () => ({
            data: {
                overall: 'healthy',
                dataLayers: [],
                recentJobs: [],
                alerts: [],
            },
            isLoading: false,
            error: null,
        }),
        useLineageQuery: () => ({
            data: { impactsByDomain: {} },
            isLoading: false,
            error: null,
        }),
    };
});

describe('App Smoke Test', () => {
    it('renders without crashing', async () => {
        renderWithProviders(<App />);
        expect(await screen.findByRole('heading', { name: /system status/i })).toBeInTheDocument();
    });
});
