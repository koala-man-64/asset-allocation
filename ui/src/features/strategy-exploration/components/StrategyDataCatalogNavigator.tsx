import { Loader2, Search } from 'lucide-react';

import { Badge } from '@/app/components/ui/badge';
import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Skeleton } from '@/app/components/ui/skeleton';

import type { StrategyDataCatalogController } from '../hooks/useStrategyDataCatalog';
import { formatInt, titleCase } from '../lib/strategyDataCatalog';

type Props = {
  controller: StrategyDataCatalogController;
};

export function StrategyDataCatalogNavigator({ controller }: Props) {
  const { atlas, navigator, actions } = controller;

  return (
    <aside className="mcm-panel h-fit overflow-hidden p-4 sm:p-5 xl:sticky xl:top-6">
      <div className="space-y-5">
        <div className="space-y-1">
          <p className="page-kicker">Atlas Navigator</p>
          <h2 className="font-display text-xl font-black uppercase tracking-[0.08em] text-foreground">
            Table Catalog
          </h2>
          <p className="text-sm text-muted-foreground">
            Filter the contract list, then inspect one table at a time.
          </p>
        </div>

        <div className="relative">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            aria-label="Search table catalog"
            value={navigator.search}
            onChange={(event) => actions.setNavigatorSearch(event.target.value)}
            placeholder="Search tables, domains, or loaded columns"
            className="pl-10"
          />
        </div>

        <div className="space-y-2">
          <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
            Medallion Filter
          </div>
          <div className="grid grid-cols-2 gap-2">
            <Button
              type="button"
              variant={navigator.selectedLayer === 'all' ? 'default' : 'outline'}
              className="justify-center"
              onClick={() => actions.selectLayer('all')}
            >
              All Layers
            </Button>
            {atlas.layers.map((layer) => (
              <Button
                key={layer.key}
                type="button"
                variant={navigator.selectedLayer === layer.key ? 'default' : 'outline'}
                className="justify-center"
                onClick={() => actions.selectLayer(layer.key)}
              >
                {layer.label}
              </Button>
            ))}
          </div>
        </div>

        <div className="rounded-[1.35rem] border border-mcm-walnut/15 bg-mcm-cream/55 px-4 py-3">
          <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
            Active Focus
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            <Badge variant="secondary">
              {navigator.selectedLayer === 'all'
                ? 'All medallions'
                : titleCase(navigator.selectedLayer)}
            </Badge>
            <Badge variant="secondary">
              {navigator.selectedDomain
                ? atlas.layers
                    .flatMap((layer) => layer.domains)
                    .find((domain) => domain.key === navigator.selectedDomain)?.label ||
                  titleCase(navigator.selectedDomain)
                : 'All domains'}
            </Badge>
            <Badge variant="secondary">
              {formatInt(navigator.filteredTables.length)} visible tables
            </Badge>
          </div>
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
              Contract List
            </div>
            {navigator.isLoading ? (
              <span className="inline-flex items-center gap-1 text-[10px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                <Loader2 className="h-3 w-3 animate-spin" />
                loading
              </span>
            ) : null}
          </div>

          <div className="max-h-[760px] space-y-2 overflow-y-auto pr-1">
            {navigator.isLoading && !navigator.filteredTables.length ? (
              Array.from({ length: 6 }).map((_, index) => (
                <Skeleton key={index} className="h-24 rounded-[1.2rem]" />
              ))
            ) : navigator.filteredTables.length === 0 ? (
              <div className="rounded-[1.2rem] border border-dashed border-mcm-walnut/20 bg-mcm-paper/70 p-5 text-sm text-muted-foreground">
                No tables matched the current medallion, domain, and search filters.
              </div>
            ) : (
              navigator.filteredTables.map((table) => {
                const isSelected = navigator.selectedTable?.key === table.key;
                const detailState = navigator.tableDetailsByKey[table.key];
                const columnCount = detailState?.data?.columns.length;

                return (
                  <button
                    key={table.key}
                    type="button"
                    aria-pressed={isSelected}
                    className={`w-full rounded-[1.25rem] border px-4 py-4 text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-mcm-teal ${
                      isSelected
                        ? 'border-mcm-teal bg-mcm-teal/10 shadow-[0_0_0_2px_rgba(0,128,128,0.14)]'
                        : 'border-mcm-walnut/12 bg-mcm-paper hover:bg-mcm-cream/85'
                    }`}
                    onClick={() => actions.selectTable(table.key)}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="space-y-2">
                        <div className="font-display text-base font-black uppercase tracking-[0.08em] text-foreground">
                          {table.tableName}
                        </div>
                        <div className="flex flex-wrap gap-2">
                          <Badge variant="outline">{table.layerLabel}</Badge>
                          {table.domainLabel ? (
                            <Badge variant="secondary">{table.domainLabel}</Badge>
                          ) : null}
                        </div>
                      </div>
                      {detailState?.isLoading ? (
                        <Loader2 className="h-4 w-4 animate-spin text-mcm-teal" />
                      ) : null}
                    </div>

                    <div className="mt-3 text-xs text-mcm-walnut/65">
                      {(
                        table.domainDescription ||
                        'Serving-table contract for this medallion slice.'
                      ).trim()}
                    </div>

                    <div className="mt-4 grid grid-cols-2 gap-2 text-xs">
                      <div className="rounded-[0.9rem] bg-mcm-cream/70 px-3 py-2">
                        <div className="text-[10px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                          Schema
                        </div>
                        <div className="mt-1 font-mono font-bold text-foreground">
                          {table.schemaName}
                        </div>
                      </div>
                      <div className="rounded-[0.9rem] bg-mcm-cream/70 px-3 py-2">
                        <div className="text-[10px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                          Columns
                        </div>
                        <div className="mt-1 font-mono font-bold text-foreground">
                          {columnCount
                            ? formatInt(columnCount)
                            : detailState?.isLoading
                              ? '...'
                              : 'Open'}
                        </div>
                      </div>
                    </div>
                  </button>
                );
              })
            )}
          </div>
        </div>
      </div>
    </aside>
  );
}
