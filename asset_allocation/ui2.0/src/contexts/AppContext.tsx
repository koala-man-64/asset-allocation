// Global application context for managing state across the dashboard

import React, { createContext, useContext, useState, ReactNode, useEffect } from 'react';
import { DataService } from '@/services/DataService';
// StrategyRun type is used in DataService but maybe not here directly now, but let's keep it safe. 
// Actually, I don't see StrategyRun usage in AppContext explicitly besides imports in previous versions.
// checking Step 125, it imported StrategyRun but didn't seem to use it in types.
// I'll keep the import if it was there to avoid breaking other things.
import { StrategyRun } from '@/types/strategy';

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
  const [selectedRuns, setSelectedRuns] = useState<Set<string>>(new Set());
  const [dateRange, setDateRange] = useState({ start: '2020-01-01', end: '2025-01-01' });
  const [benchmark, setBenchmark] = useState('SPY');
  const [costModel, setCostModel] = useState('Passive bps');
  const [dataSource, setDataSource] = useState<'mock' | 'live'>('mock');
  const [isDarkMode, setIsDarkMode] = useState(false);
  const [environment, setEnvironment] = useState<'DEV' | 'PROD'>('DEV');
  const [cartOpen, setCartOpen] = useState(false);

  // Sync DataService mode
  useEffect(() => {
    DataService.setMode(dataSource);
  }, [dataSource]);

  // Apply dark mode to document
  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDarkMode]);

  const addToCart = (runId: string) => {
    setSelectedRuns(prev => new Set(prev).add(runId));
  };

  const removeFromCart = (runId: string) => {
    setSelectedRuns(prev => {
      const next = new Set(prev);
      next.delete(runId);
      return next;
    });
  };

  const clearCart = () => {
    setSelectedRuns(new Set());
  };

  return (
    <AppContext.Provider value={{
      selectedRuns,
      addToCart,
      removeFromCart,
      clearCart,
      dateRange,
      setDateRange,
      benchmark,
      setBenchmark,
      costModel,
      setCostModel,
      dataSource,
      setDataSource,
      isDarkMode,
      setIsDarkMode,
      environment,
      setEnvironment,
      cartOpen,
      setCartOpen
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