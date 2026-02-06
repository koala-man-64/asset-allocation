import { useEffect } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from '@/contexts/AuthContext';
import { QueryProvider } from '@/providers/QueryProvider';

import { useUIStore } from '@/stores/useUIStore';

import { LeftNavigation } from '@/app/components/layout/LeftNavigation';

import { DataExplorerPage } from '@/app/components/pages/DataExplorerPage';
import { LiveTradingPage } from '@/app/components/pages/LiveTradingPage';
import { AlertsPage } from '@/app/components/pages/AlertsPage';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { DataQualityPage } from '@/app/components/pages/DataQualityPage';
import { StockExplorerPage } from '@/app/components/pages/StockExplorerPage';
import { Toaster } from '@/app/components/ui/sonner';

import { StockDetailPage } from '@/app/components/pages/StockDetailPage';
import { PostgresExplorerPage } from '@/app/components/pages/PostgresExplorerPage';
import { DebugSymbolsPage } from '@/app/components/pages/DebugSymbolsPage';
import { RuntimeConfigPage } from '@/app/components/pages/RuntimeConfigPage';

function AppContent() {
  // Enable real-time updates from backend

  return (
    <div className="h-screen flex flex-col bg-background">
      <div className="flex-1 flex overflow-hidden">
        <LeftNavigation />

        <main className="flex-1 overflow-y-auto">
          <div className="container mx-auto p-8 max-w-[1800px]">
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
              <Route path="/postgres-explorer" element={<PostgresExplorerPage />} />
              <Route path="/stock-detail/:ticker?" element={<StockDetailPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
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
