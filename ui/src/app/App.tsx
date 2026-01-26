import { useEffect } from 'react';
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { AuthProvider } from '@/contexts/AuthContext';
import { QueryProvider } from '@/providers/QueryProvider';
import { useRealtime } from '@/hooks/useRealtime';
import { useUIStore } from '@/stores/useUIStore';

import { LeftNavigation } from '@/app/components/layout/LeftNavigation';
import { RunCart } from '@/app/components/layout/RunCart';
import { OverviewPage } from '@/app/components/pages/OverviewPage';
import { RunComparePage } from '@/app/components/pages/RunComparePage';
import { DeepDivePage } from '@/app/components/pages/DeepDivePage';
import { AttributionPage } from '@/app/components/pages/AttributionPage';
import { RiskPage } from '@/app/components/pages/RiskPage';
import { ExecutionPage } from '@/app/components/pages/ExecutionPage';
import { RobustnessPage } from '@/app/components/pages/RobustnessPage';
import { PortfolioPage } from '@/app/components/pages/PortfolioPage';
import { DataExplorerPage } from '@/app/components/pages/DataExplorerPage';
import { SignalMonitorPage } from '@/app/components/pages/SignalMonitorPage';
import { LiveTradingPage } from '@/app/components/pages/LiveTradingPage';
import { AlertsPage } from '@/app/components/pages/AlertsPage';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { StockExplorerPage } from '@/app/components/pages/StockExplorerPage';
import { Toaster } from '@/app/components/ui/sonner';

import { StockDetailPage } from '@/app/components/pages/StockDetailPage';

function AppContent() {
  const navigate = useNavigate();

  // Enable real-time updates from backend
  useRealtime();

  return (
    <div className="h-screen flex flex-col bg-background">
      <div className="flex-1 flex overflow-hidden">
        <LeftNavigation />

        <main className="flex-1 overflow-y-auto">
          <div className="container mx-auto p-8 max-w-[1800px]">
            <Routes>
              <Route path="/" element={<Navigate to="/system-status" replace />} />
              <Route path="/overview" element={<OverviewPage />} />
              <Route path="/compare" element={<RunComparePage />} />
              <Route path="/deep-dive" element={<DeepDivePage />} />
              <Route path="/attribution" element={<AttributionPage />} />
              <Route path="/risk" element={<RiskPage />} />
              <Route path="/execution" element={<ExecutionPage />} />
              <Route path="/robustness" element={<RobustnessPage />} />
              <Route path="/portfolio" element={<PortfolioPage />} />
              <Route path="/data-explorer" element={<DataExplorerPage />} />
              <Route path="/signals" element={<SignalMonitorPage />} />
              <Route path="/live-trading" element={<LiveTradingPage />} />
              <Route path="/alerts" element={<AlertsPage />} />
              <Route path="/system-status" element={<SystemStatusPage />} />
              <Route path="/stock-explorer" element={<StockExplorerPage />} />
              <Route path="/stock/:ticker?" element={<StockDetailPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </div>
        </main>
      </div>

      <RunCart
        onCompare={() => navigate('/compare')}
        onPortfolioBuilder={() => navigate('/portfolio')}
      />
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
