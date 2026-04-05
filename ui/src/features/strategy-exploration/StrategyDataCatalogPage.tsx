import { AlertTriangle } from 'lucide-react';

import { Alert, AlertDescription, AlertTitle } from '@/app/components/ui/alert';

import { StrategyDataCatalogAtlas } from './components/StrategyDataCatalogAtlas';
import { StrategyDataCatalogDetailPanel } from './components/StrategyDataCatalogDetailPanel';
import { StrategyDataCatalogHero } from './components/StrategyDataCatalogHero';
import { StrategyDataCatalogNavigator } from './components/StrategyDataCatalogNavigator';
import { useStrategyDataCatalog } from './hooks/useStrategyDataCatalog';

export function StrategyDataCatalogPage() {
  const controller = useStrategyDataCatalog();
  const { hero, alerts, atlas, actions } = controller;

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Data Platform</p>
        <h1 className="page-title">Domain Atlas</h1>
        <p className="page-subtitle">
          A single editorial surface for the medallion stack: live domain coverage, inferred
          domain-to-table links, and per-column contracts with names, descriptions, and data types.
        </p>
      </div>

      <StrategyDataCatalogHero {...hero} />

      {alerts.statusErrorMessage ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>System metadata is unavailable</AlertTitle>
          <AlertDescription>{alerts.statusErrorMessage}</AlertDescription>
        </Alert>
      ) : null}

      {alerts.tableCatalogErrorMessage ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Postgres catalog is unavailable</AlertTitle>
          <AlertDescription>{alerts.tableCatalogErrorMessage}</AlertDescription>
        </Alert>
      ) : null}

      {alerts.tableCatalogWarnings.length > 0 ? (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Partial catalog coverage</AlertTitle>
          <AlertDescription>{alerts.tableCatalogWarnings.join(' ')}</AlertDescription>
        </Alert>
      ) : null}

      <StrategyDataCatalogAtlas
        atlasLayers={atlas.layers}
        selectedLayer={atlas.selectedLayer}
        selectedDomain={atlas.selectedDomain}
        isStatusLoading={hero.isStatusLoading}
        onClearFocus={actions.clearDomainFocus}
        onFocusDomain={actions.focusDomain}
      />

      <section className="grid gap-6 xl:grid-cols-[340px_minmax(0,1fr)]">
        <StrategyDataCatalogNavigator controller={controller} />
        <div className="space-y-6">
          <StrategyDataCatalogDetailPanel controller={controller} />
        </div>
      </section>
    </div>
  );
}
