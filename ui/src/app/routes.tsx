import { Suspense, lazy } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';

const DataExplorerPage = lazy(() =>
  import('@/features/data-explorer/DataExplorerPage').then((m) => ({ default: m.DataExplorerPage }))
);
const RegimeMonitorPage = lazy(() =>
  import('@/features/regimes/RegimeMonitorPage').then((m) => ({ default: m.RegimeMonitorPage }))
);
const SystemStatusPage = lazy(() =>
  import('@/features/system-status/SystemStatusPage').then((m) => ({ default: m.SystemStatusPage }))
);
const DataQualityPage = lazy(() =>
  import('@/features/data-quality/DataQualityPage').then((m) => ({ default: m.DataQualityPage }))
);
const DataProfilingPage = lazy(() =>
  import('@/features/data-profiling/DataProfilingPage').then((m) => ({
    default: m.DataProfilingPage
  }))
);
const StockExplorerPage = lazy(() =>
  import('@/features/stocks/StockExplorerPage').then((m) => ({ default: m.StockExplorerPage }))
);
const StockDetailPage = lazy(() =>
  import('@/features/stocks/StockDetailPage').then((m) => ({ default: m.StockDetailPage }))
);
const PostgresExplorerPage = lazy(() =>
  import('@/features/postgres-explorer/PostgresExplorerPage').then((m) => ({
    default: m.PostgresExplorerPage
  }))
);
const DebugSymbolsPage = lazy(() =>
  import('@/features/debug-symbols/DebugSymbolsPage').then((m) => ({
    default: m.DebugSymbolsPage
  }))
);
const RuntimeConfigPage = lazy(() =>
  import('@/features/runtime-config/RuntimeConfigPage').then((m) => ({
    default: m.RuntimeConfigPage
  }))
);
const StrategyConfigPage = lazy(() =>
  import('@/features/strategies/StrategyConfigPage').then((m) => ({
    default: m.StrategyConfigPage
  }))
);
const UniverseConfigPage = lazy(() =>
  import('@/features/universes/UniverseConfigPage').then((m) => ({
    default: m.UniverseConfigPage
  }))
);
const RankingConfigPage = lazy(() =>
  import('@/features/rankings/RankingConfigPage').then((m) => ({
    default: m.RankingConfigPage
  }))
);
const StrategyDataCatalogPage = lazy(() =>
  import('@/features/strategy-exploration/StrategyDataCatalogPage').then((m) => ({
    default: m.StrategyDataCatalogPage
  }))
);
const SymbolPurgeByCriteriaPage = lazy(() =>
  import('@/features/symbol-purge/SymbolPurgeByCriteriaPage').then((m) => ({
    default: m.SymbolPurgeByCriteriaPage
  }))
);

function RouteLoadingFallback() {
  return (
    <div className="flex h-full min-h-[400px] w-full items-center justify-center">
      <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-primary" />
    </div>
  );
}

export function AppRoutes() {
  return (
    <Suspense fallback={<RouteLoadingFallback />}>
      <Routes>
        <Route path="/" element={<Navigate to="/system-status" replace />} />
        <Route path="/data-explorer" element={<DataExplorerPage />} />
        <Route path="/regimes" element={<RegimeMonitorPage />} />
        <Route path="/data-quality" element={<DataQualityPage />} />
        <Route path="/data-profiling" element={<DataProfilingPage />} />
        <Route path="/system-status" element={<SystemStatusPage />} />
        <Route path="/debug-symbols" element={<DebugSymbolsPage />} />
        <Route path="/runtime-config" element={<RuntimeConfigPage />} />
        <Route path="/symbol-purge" element={<SymbolPurgeByCriteriaPage />} />
        <Route path="/stock-explorer" element={<StockExplorerPage />} />
        <Route path="/strategies" element={<StrategyConfigPage />} />
        <Route path="/universes" element={<UniverseConfigPage />} />
        <Route path="/rankings" element={<RankingConfigPage />} />
        <Route path="/strategy-exploration" element={<StrategyDataCatalogPage />} />
        <Route path="/postgres-explorer" element={<PostgresExplorerPage />} />
        <Route path="/stock-detail/:ticker?" element={<StockDetailPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Suspense>
  );
}
