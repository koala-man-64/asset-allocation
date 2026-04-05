import { Loader2, RefreshCw, Search } from 'lucide-react';

import { Button } from '@/app/components/ui/button';
import { Input } from '@/app/components/ui/input';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';

import type { SymbolPurgeController } from '../hooks/useSymbolPurgeController';
import {
  aggregationOptions,
  domainOptions,
  formFieldClass,
  formInputClass,
  formLabelClass,
  formSelectClass,
  layerOptions,
  operatorOptions
} from '../lib/symbolPurge';

type Props = {
  controller: SymbolPurgeController;
};

export function SymbolPurgeCriteriaPanel({ controller }: Props) {
  const { criteria, columns, derived, candidate, actions } = controller;

  return (
    <section className="mcm-panel p-4 sm:p-5">
      <div className="mb-3 flex items-start justify-between gap-3">
        <div>
          <p className="page-kicker">Live Operations</p>
          <h1 className="page-title leading-[1.05]">Symbol Purge Console</h1>
          <p className="page-subtitle mt-1 max-w-[30ch] leading-relaxed">
            Build a rule, review candidate symbols, then execute a destructive bulk purge.
          </p>
        </div>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-9 px-4"
              onClick={() => void actions.runPreview()}
            >
              Preview
            </Button>
          </TooltipTrigger>
          <TooltipContent>Run candidate preview</TooltipContent>
        </Tooltip>
      </div>

      <div className="space-y-3">
        <div className={formFieldClass}>
          <label className={formLabelClass}>Medallion layer</label>
          <select
            value={criteria.layer}
            className={formSelectClass}
            onChange={(event) => actions.setLayer(event.target.value as typeof criteria.layer)}
          >
            {layerOptions.map((layerKey) => (
              <option key={layerKey} value={layerKey}>
                {layerKey.toUpperCase()}
              </option>
            ))}
          </select>
          {derived.showBronzeWarning ? (
            <p className="text-[11px] leading-relaxed text-amber-600">
              Bronze-wide criteria are approximated from the silver preview layer. Silver/gold is
              recommended.
            </p>
          ) : null}
        </div>

        <div className={formFieldClass}>
          <label className={formLabelClass}>Domain</label>
          <select
            value={criteria.domain}
            className={formSelectClass}
            onChange={(event) => actions.setDomain(event.target.value as typeof criteria.domain)}
          >
            {domainOptions.map((entry) => (
              <option key={entry.value} value={entry.value}>
                {entry.label}
              </option>
            ))}
          </select>
        </div>

        <div className={formFieldClass}>
          <label className={formLabelClass}>Column</label>
          <select
            value={criteria.column}
            className={formSelectClass}
            disabled={columns.columnsLoading}
            onChange={(event) => actions.setColumn(event.target.value)}
          >
            <option value="" disabled>
              {columns.columnsLoading ? 'Loading columns...' : 'Select a column'}
            </option>
            {columns.availableColumns.map((column) => (
              <option key={column} value={column}>
                {column}
              </option>
            ))}
          </select>
          <div className="mt-2 flex items-center justify-between gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="h-8 px-3 text-xs"
              onClick={() => void actions.refreshColumns()}
              disabled={columns.columnsLoading}
            >
              {columns.columnsLoading ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <RefreshCw className="h-3.5 w-3.5" />
              )}
              {columns.columnsRequireRetrieve ? 'Retrieve Columns' : 'Refresh Columns'}
            </Button>
            <p className="text-[11px] text-muted-foreground">Source: common ADLS cache</p>
          </div>
          {columns.columnsRequireRetrieve ? (
            <p className="text-[11px] text-amber-600">
              Columns are not cached for this layer/domain yet.
            </p>
          ) : null}
          {columns.columnsError ? (
            <p className="text-[11px] text-destructive">{columns.columnsError}</p>
          ) : null}
        </div>

        <div className="grid grid-cols-2 gap-2.5">
          <div className={formFieldClass}>
            <label className={formLabelClass}>Rule type</label>
            <select
              value={criteria.operator}
              className={formSelectClass}
              onChange={(event) =>
                actions.setOperator(event.target.value as typeof criteria.operator)
              }
            >
              {operatorOptions.map((entry) => (
                <option key={entry.value} value={entry.value}>
                  {entry.label}
                </option>
              ))}
            </select>
          </div>
          <div className={formFieldClass}>
            <label className={formLabelClass}>
              {derived.isPercentMode ? 'Percent (1-100)' : 'Numeric value'}
            </label>
            <Input
              type="text"
              value={criteria.value}
              onChange={(event) => actions.setValue(event.target.value)}
              className={formInputClass}
              placeholder={derived.isPercentMode ? 'e.g. 90' : 'e.g. 100'}
            />
            {!derived.isValueValid || !derived.isPercentValid ? (
              <p className="text-[11px] text-destructive">
                {derived.isValueValid
                  ? 'Percentile must be between 1 and 100.'
                  : 'Numeric value must be finite.'}
              </p>
            ) : null}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2.5">
          <div className={formFieldClass}>
            <label className={formLabelClass}>Aggregation</label>
            <select
              value={criteria.aggregation}
              className={formSelectClass}
              onChange={(event) =>
                actions.setAggregation(event.target.value as typeof criteria.aggregation)
              }
            >
              {aggregationOptions.map((entry) => (
                <option key={entry.value} value={entry.value}>
                  {entry.label}
                </option>
              ))}
            </select>
          </div>

          <div className={formFieldClass}>
            <label className={formLabelClass}>Recent Row Count</label>
            <Input
              type="number"
              min={1}
              value={criteria.recentRows}
              onChange={(event) => actions.setRecentRows(Number(event.target.value) || 1)}
              className={formInputClass}
            />
          </div>
        </div>

        <Button
          onClick={() => void actions.runPreview()}
          disabled={!derived.canPreview || candidate.loading}
          className="h-10 w-full gap-2"
        >
          {candidate.loading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Search className="h-4 w-4" />
          )}
          {candidate.loading ? 'Previewing...' : 'Preview symbols'}
        </Button>

        {candidate.validationError ? (
          <p className="text-[11px] text-destructive">{candidate.validationError}</p>
        ) : null}

        <div className="rounded-xl border border-border/70 bg-muted/30 p-2.5 text-xs text-muted-foreground">
          <p className="font-semibold text-foreground">Rule summary</p>
          <p className="mt-1 break-words font-mono">
            {derived.previewExpression || 'No valid rule yet.'}
          </p>
        </div>
      </div>
    </section>
  );
}
