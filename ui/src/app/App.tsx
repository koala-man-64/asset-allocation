import { useEffect, Suspense, lazy } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from '@/contexts/AuthContext';
import { QueryProvider } from '@/providers/QueryProvider';

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
const PostgresExplorerPage = lazy(() => import('@/app/components/pages/PostgresExplorerPage').then(m => ({ default: m.PostgresExplorerPage })));
const DebugSymbolsPage = lazy(() => import('@/app/components/pages/DebugSymbolsPage').then(m => ({ default: m.DebugSymbolsPage })));
const RuntimeConfigPage = lazy(() => import('@/app/components/pages/RuntimeConfigPage').then(m => ({ default: m.RuntimeConfigPage })));
const StrategyConfigPage = lazy(() => import('@/app/components/pages/StrategyConfigPage').then(m => ({ default: m.StrategyConfigPage })));

import { Toaster } from '@/app/components/ui/sonner';

function AppContent() {
  // Enable real-time updates from backend

  return (
    <div className="h-screen flex flex-col bg-background">
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
                <Route path="/system-status" element={<SystemStatusPage />} />
                <Route path="/debug-symbols" element={<DebugSymbolsPage />} />
                <Route path="/runtime-config" element={<RuntimeConfigPage />} />
                <Route path="/stock-explorer" element={<StockExplorerPage />} />
                <Route path="/strategies" element={<StrategyConfigPage />} />
                <Route path="/postgres-explorer" element={<PostgresExplorerPage />} />
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
