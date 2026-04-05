import { ListChecks } from 'lucide-react';

import { Badge } from '@/app/components/ui/badge';

import { formatInt } from '../lib/strategyDataCatalog';

type Props = {
  medallionCount: number;
  totalDomainCount: number;
  totalTableCount: number;
  isStatusLoading: boolean;
  isStatusFetching: boolean;
  isTableCatalogLoading: boolean;
};

export function StrategyDataCatalogHero({
  medallionCount,
  totalDomainCount,
  totalTableCount,
  isStatusLoading,
  isStatusFetching,
  isTableCatalogLoading
}: Props) {
  return (
    <section className="relative overflow-hidden rounded-[2rem] border-2 border-mcm-walnut bg-mcm-paper px-6 py-6 shadow-[12px_12px_0px_0px_rgba(119,63,26,0.12)]">
      <div className="absolute inset-0 bg-[linear-gradient(145deg,rgba(119,63,26,0.08),transparent_36%),linear-gradient(315deg,rgba(0,128,128,0.08),transparent_42%),linear-gradient(0deg,rgba(225,173,1,0.08),transparent_55%)]" />
      <div className="absolute left-[12%] top-8 h-32 w-32 rounded-full bg-mcm-mustard/20 blur-3xl" />
      <div className="absolute bottom-2 right-[18%] h-36 w-36 rounded-full bg-mcm-teal/16 blur-3xl" />

      <div className="relative grid gap-6 xl:grid-cols-[minmax(0,1.3fr)_360px]">
        <div className="space-y-5">
          <Badge variant="outline" className="border-mcm-walnut/30 bg-mcm-paper/70">
            Data Dictionary Surface
          </Badge>

          <div className="max-w-3xl space-y-3">
            <h2 className="font-display text-[clamp(2rem,4vw,3.8rem)] font-black uppercase leading-[0.92] tracking-[0.04em] text-foreground">
              Medallions first. Domains second. Column contracts on demand.
            </h2>
            <p className="max-w-[64ch] text-base text-mcm-walnut/70">
              Domain cards are sourced from the live system-status snapshot. Table contracts come
              from Postgres metadata, with gold lookup annotations filling in richer column
              descriptions where they exist.
            </p>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <SummaryTile
              label="Medallions"
              value={formatInt(medallionCount)}
              note="Live layers represented across the atlas."
            />
            <SummaryTile
              label="Domains"
              value={formatInt(totalDomainCount)}
              note="System-status domains with metadata and health context."
            />
            <SummaryTile
              label="Postgres Tables"
              value={formatInt(totalTableCount)}
              note="Queryable table contracts grouped by medallion schema."
            />
          </div>
        </div>

        <div className="rounded-[1.6rem] border border-mcm-walnut/15 bg-mcm-paper/80 p-5 shadow-[0_14px_32px_rgba(119,63,26,0.08)]">
          <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/60">
            <ListChecks className="h-4 w-4 text-mcm-teal" />
            Coverage Notes
          </div>
          <div className="mt-4 space-y-3 text-sm text-mcm-walnut/70">
            <p>
              Use the medallion strips to understand what the platform publishes. Use the table
              navigator to inspect the actual serving contracts that analysts and agents consume.
            </p>
            <p>
              When a column description is unavailable, the page shows the gap directly instead of
              inventing one. That makes missing metadata visible instead of burying it.
            </p>
            <div className="rounded-[1.2rem] border border-mcm-walnut/10 bg-mcm-cream/70 p-4">
              <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/60">
                Current Feeds
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                <Badge variant="secondary">
                  {isStatusLoading || isStatusFetching
                    ? 'Refreshing system snapshot'
                    : 'System snapshot loaded'}
                </Badge>
                <Badge variant="secondary">
                  {isTableCatalogLoading ? 'Loading Postgres catalog' : 'Postgres catalog loaded'}
                </Badge>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

function SummaryTile({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="rounded-[1.4rem] border border-mcm-walnut/15 bg-mcm-paper/75 px-4 py-4 shadow-[0_12px_32px_rgba(119,63,26,0.08)]">
      <div className="text-[10px] font-black uppercase tracking-[0.22em] text-mcm-walnut/55">
        {label}
      </div>
      <div className="mt-2 font-display text-3xl font-black tracking-[0.04em] text-foreground">
        {value}
      </div>
      <div className="mt-1 text-sm text-muted-foreground">{note}</div>
    </div>
  );
}
