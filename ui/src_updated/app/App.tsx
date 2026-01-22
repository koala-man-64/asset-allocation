import { useState } from 'react';
import { AppProvider } from '@/contexts/AppContext';
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
import { SignalMonitorPage } from '@/app/components/pages/SignalMonitorPage';
import { LiveTradingPage } from '@/app/components/pages/LiveTradingPage';
import { AlertsPage } from '@/app/components/pages/AlertsPage';
import { SystemStatusPage } from '@/app/components/pages/SystemStatusPage';
import { DataTiersPage } from '@/app/components/pages/DataTiersPage';

function AppContent() {
  const [activePage, setActivePage] = useState('overview');
  
  const renderPage = () => {
    switch (activePage) {
      case 'overview':
        return <OverviewPage />;
      case 'compare':
        return <RunComparePage />;
      case 'deep-dive':
        return <DeepDivePage />;
      case 'attribution':
        return <AttributionPage />;
      case 'risk':
        return <RiskPage />;
      case 'execution':
        return <ExecutionPage />;
      case 'robustness':
        return <RobustnessPage />;
      case 'portfolio':
        return <PortfolioPage />;
      case 'data':
        return <DataPage onNavigate={setActivePage} />;
      case 'signals':
        return <SignalMonitorPage />;
      case 'live-trading':
        return <LiveTradingPage />;
      case 'alerts':
        return <AlertsPage />;
      case 'system':
        return <SystemStatusPage />;
      case 'data-tiers':
        return <DataTiersPage />;
      default:
        return <OverviewPage />;
    }
  };
  
  return (
    <AppProvider>
      <div className="h-screen flex flex-col bg-background">
        <AppHeader />
        
        <div className="flex-1 flex overflow-hidden">
          <LeftNavigation 
            activePage={activePage} 
            onNavigate={setActivePage} 
          />
          
          <main className="flex-1 overflow-y-auto">
            <div className="container mx-auto p-8 max-w-[1800px]">
              {renderPage()}
            </div>
          </main>
        </div>
        
        <RunCart 
          onCompare={() => setActivePage('compare')}
          onPortfolioBuilder={() => setActivePage('portfolio')}
        />
      </div>
    </AppProvider>
  );
}

export default function App() {
  return <AppContent />;
}