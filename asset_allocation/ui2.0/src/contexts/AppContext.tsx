// Global application context for managing state across the dashboard

import React, { createContext, useContext, ReactNode, useEffect, useMemo } from 'react';
import { useUIStore } from '@/stores/useUIStore';

interface AppContextType {
  // Selected runs for comparison (the "cart")
  selectedRuns: Set<string>;
  addToCart: (runId: string) => void;
  removeFromCart: (runId: string) => void;
  clearCart: () => void;

  // Global filters
  dateRange: { start: string; end: string };
  setDateRange: (range: { start: string; end: string }) => void;

  benchmark: string;
  setBenchmark: (benchmark: string) => void;

  costModel: string;
  setCostModel: (model: string) => void;

  // Data Source
  dataSource: 'mock' | 'live';
  setDataSource: (source: 'mock' | 'live') => void;

  // UI state
  isDarkMode: boolean;
  setIsDarkMode: (dark: boolean) => void;

  environment: 'DEV' | 'PROD';
  setEnvironment: (env: 'DEV' | 'PROD') => void;

  cartOpen: boolean;
  setCartOpen: (open: boolean) => void;
}

const AppContext = createContext<AppContextType | undefined>(undefined);

export function AppProvider({ children }: { children: ReactNode }) {
  const store = useUIStore();

  // Compatibility layer for Set<string>
  const selectedRunsSet = useMemo(() => new Set(store.selectedRuns), [store.selectedRuns]);

  // Apply dark mode to document (moving logic here if not elsewhere)
  useEffect(() => {
    if (store.isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [store.isDarkMode]);

  return (
    <AppContext.Provider value={{
      selectedRuns: selectedRunsSet,
      addToCart: store.addToCart,
      removeFromCart: store.removeFromCart,
      clearCart: store.clearCart,
      dateRange: store.dateRange,
      setDateRange: store.setDateRange,
      benchmark: store.benchmark,
      setBenchmark: store.setBenchmark,
      costModel: store.costModel,
      setCostModel: store.setCostModel,
      dataSource: store.dataSource,
      setDataSource: store.setDataSource,
      isDarkMode: store.isDarkMode,
      setIsDarkMode: store.setIsDarkMode,
      environment: store.environment,
      setEnvironment: store.setEnvironment,
      cartOpen: store.cartOpen,
      setCartOpen: store.setCartOpen
    }}>
      {children}
    </AppContext.Provider>
  );
}

export function useApp() {
  const context = useContext(AppContext);
  if (!context) {
    throw new Error('useApp must be used within AppProvider');
  }
  return context;
}