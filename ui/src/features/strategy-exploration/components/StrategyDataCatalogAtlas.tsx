import { Button } from '@/app/components/ui/button';
import { Badge } from '@/app/components/ui/badge';
import { Skeleton } from '@/app/components/ui/skeleton';

import type { LayerAtlas, LayerFilter, MedallionKey } from '../lib/strategyDataCatalog';
import {
  LAYER_VISUALS,
  formatBytes,
  formatDateRangeLabel,
  formatInt
} from '../lib/strategyDataCatalog';

type Props = {
  atlasLayers: LayerAtlas[];
  selectedLayer: LayerFilter;
  selectedDomain: string | null;
  isStatusLoading: boolean;
  onClearFocus: () => void;
  onFocusDomain: (layerKey: MedallionKey, domainKey: string) => void;
};

export function StrategyDataCatalogAtlas({
  atlasLayers,
  selectedLayer,
  selectedDomain,
  isStatusLoading,
  onClearFocus,
  onFocusDomain
}: Props) {
  return (
    <section className="space-y-4">
      <div className="page-header-row">
        <div className="page-header">
          <p className="page-kicker">Domain Coverage</p>
          <h2 className="page-title">Medallion Strips</h2>
          <p className="page-subtitle">
            Click any domain tile to focus the atlas on that medallion/domain slice.
          </p>
        </div>
        {selectedDomain ? (
          <Button type="button" variant="outline" onClick={onClearFocus}>
            Clear Domain Focus
          </Button>
        ) : null}
      </div>

      {isStatusLoading && !atlasLayers.length ? (
        <div className="grid gap-4 xl:grid-cols-4">
          {Array.from({ length: 4 }).map((_, index) => (
            <Skeleton key={index} className="h-[280px] rounded-[1.75rem]" />
          ))}
        </div>
      ) : atlasLayers.length === 0 ? (
        <div className="rounded-[1.6rem] border-2 border-dashed border-mcm-walnut/25 bg-mcm-paper/65 p-6 text-sm text-muted-foreground">
          System status did not publish any medallion domain metadata for this deployment.
        </div>
      ) : (
        <div className="grid gap-4 xl:grid-cols-4">
          {atlasLayers.map((layer) => {
            const visual = LAYER_VISUALS[layer.key];
            return (
              <section
                key={layer.key}
                className={`relative overflow-hidden rounded-[1.8rem] border px-4 py-4 shadow-[0_18px_38px_rgba(119,63,26,0.08)] ${visual.shellClassName}`}
              >
                <div
                  className={`absolute right-4 top-4 h-16 w-16 rounded-full blur-2xl ${visual.glowClassName}`}
                />
                <div className="relative space-y-4">
                  <div className="space-y-2">
                    <Badge variant="outline" className={visual.chipClassName}>
                      {layer.label}
                    </Badge>
                    <div>
                      <div className="font-display text-xl font-black uppercase tracking-[0.08em] text-foreground">
                        {formatInt(layer.domains.length)} domains
                      </div>
                      <p className="mt-1 text-sm text-mcm-walnut/65">{layer.description}</p>
                    </div>
                  </div>

                  <div className="space-y-3">
                    {layer.domains.length === 0 ? (
                      <div className="rounded-[1.2rem] border border-dashed border-mcm-walnut/20 bg-mcm-paper/60 p-4 text-sm text-muted-foreground">
                        No domain tiles were published for this medallion.
                      </div>
                    ) : (
                      layer.domains.map((domain) => {
                        const isActive =
                          selectedDomain === domain.key && selectedLayer === layer.key;
                        return (
                          <button
                            key={`${layer.key}-${domain.key}`}
                            type="button"
                            aria-pressed={isActive}
                            aria-label={`Focus ${layer.label} ${domain.label} domain`}
                            className={`w-full rounded-[1.25rem] border px-4 py-4 text-left transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-mcm-teal ${
                              isActive
                                ? visual.activeClassName
                                : 'border-mcm-walnut/12 bg-mcm-paper/70 hover:bg-mcm-paper'
                            }`}
                            onClick={() => onFocusDomain(layer.key, domain.key)}
                          >
                            <div className="flex items-start justify-between gap-3">
                              <div>
                                <div className="font-display text-lg font-black uppercase tracking-[0.08em] text-foreground">
                                  {domain.label}
                                </div>
                                <div className="mt-1 text-xs text-mcm-walnut/60">
                                  {(
                                    domain.description || 'No domain description published.'
                                  ).trim()}
                                </div>
                              </div>
                              {domain.metadata?.type ? (
                                <Badge variant="secondary">{domain.metadata.type}</Badge>
                              ) : null}
                            </div>

                            <div className="mt-4 grid grid-cols-2 gap-2 text-xs text-mcm-walnut/70">
                              <StatChip
                                label="Symbols"
                                value={formatInt(domain.metadata?.symbolCount)}
                              />
                              <StatChip
                                label="Columns"
                                value={formatInt(
                                  domain.metadata?.columnCount ?? domain.metadata?.columns?.length
                                )}
                              />
                              <StatChip label="Tables" value={formatInt(domain.tableCount)} />
                              <StatChip
                                label="Range"
                                value={formatDateRangeLabel(domain.metadata)}
                                compact
                              />
                            </div>

                            <div className="mt-3 flex items-center justify-between text-[11px] uppercase tracking-[0.16em] text-mcm-walnut/55">
                              <span>{domain.status || 'status n/a'}</span>
                              <span>{formatBytes(domain.metadata?.totalBytes)}</span>
                            </div>
                          </button>
                        );
                      })
                    )}
                  </div>
                </div>
              </section>
            );
          })}
        </div>
      )}
    </section>
  );
}

function StatChip({ label, value, compact }: { label: string; value: string; compact?: boolean }) {
  return (
    <div className="rounded-[1rem] bg-mcm-cream/65 px-3 py-2">
      <div className="text-[10px] uppercase tracking-[0.18em] text-mcm-walnut/55">{label}</div>
      <div className={`mt-1 font-mono font-bold text-foreground ${compact ? 'text-[11px]' : ''}`}>
        {value}
      </div>
    </div>
  );
}
