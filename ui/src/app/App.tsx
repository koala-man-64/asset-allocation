import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { AuthProvider } from '@/contexts/AuthContext';
import { AppProvider } from '@/contexts/AppContext';
import { QueryProvider } from '@/providers/QueryProvider';
import { useDataSync } from '@/hooks/useDataQueries';
import { useRealtime } from '@/hooks/useRealtime';

import { AppHeader } from '@/app/components/layout/AppHeader';
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
import { DataPage } from '@/app/components/pages/DataPage';
import { DataTiersPage } from '@/app/components/pages/DataTiersPage';
import { SignalMonitorPage } from '@/app/components/pages/SignalMonitorPage';
import { LiveTradingPage } from '@/app/components/pages/LiveTradingPage';
import { AlertsPage } from '@/app/components/pages/AlertsPage';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { Toaster } from '@/app/components/ui/sonner';

function AppContent() {
  const navigate = useNavigate();

  // Sync DataService mode with global state
  useDataSync();
  // Enable real-time updates from backend
  useRealtime();

  return (
    <div className="h-screen flex flex-col bg-background">
      <AppHeader />

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
              <Route path="/data" element={<DataPage onNavigate={(page) => navigate(`/${page}`)} />} />
              <Route path="/signals" element={<SignalMonitorPage />} />
              <Route path="/live-trading" element={<LiveTradingPage />} />
              <Route path="/alerts" element={<AlertsPage />} />
              <Route path="/system-status" element={<SystemStatusPage />} />
              <Route path="/data-tiers" element={<DataTiersPage />} />
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
  return (
    <AuthProvider>
      <QueryProvider>
        <AppProvider>
          <AppContent />
        </AppProvider>
      </QueryProvider>
    </AuthProvider>
  );
}
