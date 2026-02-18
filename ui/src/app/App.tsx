import { useEffect, Suspense, lazy, useRef, useState } from 'react';
import { Routes, Route, Navigate, useLocation } from 'react-router-dom';
import { AuthProvider } from '@/contexts/AuthContext';
import { QueryProvider } from '@/providers/QueryProvider';
import { useRealtime } from '@/hooks/useRealtime';

import { useUIStore } from '@/stores/useUIStore';

import { LeftNavigation } from '@/app/components/layout/LeftNavigation';

// Lazy load pages for performance optimization (Code Splitting)
// Using .then(m => ({ default: m.ComponentName })) pattern for named exports
const DataExplorerPage = lazy(() => import('@/app/components/pages/DataExplorerPage').then(m => ({ default: m.DataExplorerPage })));
const LiveTradingPage = lazy(() => import('@/app/components/pages/LiveTradingPage').then(m => ({ default: m.LiveTradingPage })));
const AlertsPage = lazy(() => import('@/app/components/pages/AlertsPage').then(m => ({ default: m.AlertsPage })));
const SystemStatusPage = lazy(() => import('@/app/components/pages/SystemStatusPage').then(m => ({ default: m.SystemStatusPage })));
const DataQualityPage = lazy(() => import('@/app/components/pages/DataQualityPage').then(m => ({ default: m.DataQualityPage })));
const StockExplorerPage = lazy(() => import('@/app/components/pages/StockExplorerPage').then(m => ({ default: m.StockExplorerPage })));
const StockDetailPage = lazy(() => import('@/app/components/pages/StockDetailPage').then(m => ({ default: m.StockDetailPage })));
const DebugSymbolsPage = lazy(() => import('@/app/components/pages/DebugSymbolsPage').then(m => ({ default: m.DebugSymbolsPage })));
const DataProfilingPage = lazy(() => import('@/app/components/pages/DataProfilingPage').then(m => ({ default: m.DataProfilingPage })));
const RuntimeConfigPage = lazy(() => import('@/app/components/pages/RuntimeConfigPage').then(m => ({ default: m.RuntimeConfigPage })));
const StrategyConfigPage = lazy(() => import('@/app/components/pages/StrategyConfigPage').then(m => ({ default: m.StrategyConfigPage })));
const SymbolPurgeByCriteriaPage = lazy(() =>
  import('@/app/components/pages/SymbolPurgeByCriteriaPage').then((m) => ({ default: m.SymbolPurgeByCriteriaPage }))
);

import { Toaster } from '@/app/components/ui/sonner';

const ROUTE_INDICATOR_ACTIVE_MS = 280;
const ROUTE_INDICATOR_FADE_MS = 220;

function RouteTransitionIndicator() {
  const location = useLocation();
  const [phase, setPhase] = useState<'idle' | 'animating' | 'finishing'>('idle');
  const hasMounted = useRef(false);

  useEffect(() => {
    if (!hasMounted.current) {
      hasMounted.current = true;
      return;
    }

    setPhase('animating');
    const finishTimer = window.setTimeout(() => setPhase('finishing'), ROUTE_INDICATOR_ACTIVE_MS);
    const hideTimer = window.setTimeout(
      () => setPhase('idle'),
      ROUTE_INDICATOR_ACTIVE_MS + ROUTE_INDICATOR_FADE_MS
    );

    return () => {
      window.clearTimeout(finishTimer);
      window.clearTimeout(hideTimer);
    };
  }, [location.pathname, location.search, location.hash]);

  const phaseClass =
    phase === 'idle'
      ? 'w-0 opacity-0'
      : phase === 'animating'
        ? 'w-[72%] opacity-100'
        : 'w-full opacity-0';

  return (
    <div aria-hidden="true" className="pointer-events-none fixed inset-x-0 top-0 z-[120] h-[3px]">
      <div
        data-testid="route-transition-indicator"
        data-state={phase}
        className={`h-full bg-gradient-to-r from-mcm-teal via-primary to-mcm-mustard transition-[width,opacity] duration-300 ease-out motion-reduce:transition-none ${phaseClass}`}
      />
    </div>
  );
}

function AppContent() {
  // Keep query caches fresh from backend push events (Azure/prod-safe alternative to dev HMR).
  useRealtime();

  return (
    <div className="h-screen flex flex-col bg-background">
      <RouteTransitionIndicator />
      <div className="flex-1 flex overflow-hidden">
        <LeftNavigation />

        <main className="flex-1 overflow-y-auto">
          <div className="container mx-auto p-8 max-w-[1800px]">
            <Suspense fallback={
              <div className="flex h-full w-full items-center justify-center min-h-[400px]">
                <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
              </div>
            }>
              <Routes>
                <Route path="/" element={<Navigate to="/system-status" replace />} />
                <Route path="/data-explorer" element={<DataExplorerPage />} />
                <Route path="/live-trading" element={<LiveTradingPage />} />
                <Route path="/alerts" element={<AlertsPage />} />
                <Route path="/data-quality" element={<DataQualityPage />} />
                <Route path="/data-profiling" element={<DataProfilingPage />} />
                <Route path="/system-status" element={<SystemStatusPage />} />
                <Route path="/debug-symbols" element={<DebugSymbolsPage />} />
                <Route path="/runtime-config" element={<RuntimeConfigPage />} />
                <Route path="/symbol-purge" element={<SymbolPurgeByCriteriaPage />} />
                <Route path="/data-admin/symbol-purge" element={<SymbolPurgeByCriteriaPage />} />
                <Route path="/stock-explorer" element={<StockExplorerPage />} />
                <Route path="/strategies" element={<StrategyConfigPage />} />
                <Route path="/stock-detail/:ticker?" element={<StockDetailPage />} />
                <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </Suspense>
          </div>
        </main>
      </div>

      <Toaster />
    </div>
  );
}

export default function App() {
  const isDarkMode = useUIStore((s) => s.isDarkMode);

  useEffect(() => {
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDarkMode]);

  return (
    <AuthProvider>
      <QueryProvider>
        <AppContent />
      </QueryProvider>
    </AuthProvider>
  );
}
