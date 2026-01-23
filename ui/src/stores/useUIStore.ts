import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface UIState {
    // Appearance
    isDarkMode: boolean;
    toggleDarkMode: () => void;
    setIsDarkMode: (dark: boolean) => void;

    // Layout
    sidebarOpen: boolean;
    setSidebarOpen: (open: boolean) => void;
    cartOpen: boolean;
    setCartOpen: (open: boolean) => void;

    // Global Filters
    dateRange: { start: string; end: string };
    setDateRange: (range: { start: string; end: string }) => void;
    benchmark: string;
    setBenchmark: (benchmark: string) => void;
    costModel: string;
    setCostModel: (model: string) => void;

    // Domain Config
    dataSource: 'mock' | 'live';
    setDataSource: (source: 'mock' | 'live') => void;
    environment: 'DEV' | 'PROD';
    setEnvironment: (env: 'DEV' | 'PROD') => void;

    // Run Selection (Cart)
    selectedRuns: string[]; // Set is not serializable for persist, use array
    addToCart: (runId: string) => void;
    removeFromCart: (runId: string) => void;
    clearCart: () => void;
}

export const useUIStore = create<UIState>()(
    persist(
        (set) => ({
            // Defaults
            isDarkMode: false,
            sidebarOpen: true,
            cartOpen: false,
            dateRange: { start: '2020-01-01', end: '2025-01-01' },
            benchmark: 'SPY',
            costModel: 'Passive bps',
            dataSource: 'mock',
            environment: 'DEV',
            selectedRuns: [],

            // Actions
            toggleDarkMode: () => set((state) => ({ isDarkMode: !state.isDarkMode })),
            setIsDarkMode: (dark) => set({ isDarkMode: dark }),
            setSidebarOpen: (open) => set({ sidebarOpen: open }),
            setCartOpen: (open) => set({ cartOpen: open }),
            setDateRange: (range) => set({ dateRange: range }),
            setBenchmark: (benchmark) => set({ benchmark }),
            setCostModel: (model) => set({ costModel: model }),
            setDataSource: (source) => set({ dataSource: source }),
            setEnvironment: (env) => set({ environment: env }),

            addToCart: (runId) =>
                set((state) => ({
                    selectedRuns: [...new Set([...state.selectedRuns, runId])],
                })),
            removeFromCart: (runId) =>
                set((state) => ({
                    selectedRuns: state.selectedRuns.filter((id) => id !== runId),
                })),
            clearCart: () => set({ selectedRuns: [] }),
        }),
        {
            name: 'ui-storage',
            partialize: (state) => ({
                // Only persist settings the user would expect to stick
                isDarkMode: state.isDarkMode,
                dataSource: state.dataSource,
                benchmark: state.benchmark,
                costModel: state.costModel,
                dateRange: state.dateRange,
            }),
        }
    )
);
