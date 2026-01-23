// Global application context shim
// DEPRECATED: Use useUIStore directly for better performance

import React, { ReactNode, useMemo, useEffect } from 'react';
import { useUIStore } from '@/stores/useUIStore';

// Shim AppProvider to be a simple pass-through to avoid breaking imports
export function AppProvider({ children }: { children: ReactNode }) {
  const isDarkMode = useUIStore((s) => s.isDarkMode);

  // Maintain the dark mode side-effect here for now
  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDarkMode]);

  return <>{children}</>;
}

// Shim useApp to return the implementation from Zustand
// Note: This still mimics the broad re-render behavior of the original Context
// Future Refactor: Update components to import useUIStore and select only what they need
export function useApp() {
  const store = useUIStore();

  // Compatibility layer for Set<string> - conversion happens on every render
  // This is acceptable for the shim phase
  const selectedRuns = store.selectedRuns instanceof Set
    ? store.selectedRuns
    : new Set(store.selectedRuns);

  return {
    ...store,
    selectedRuns, // Override array with Set for backward compat
  };
}