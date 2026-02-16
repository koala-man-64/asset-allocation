import React, { useState, useCallback } from 'react';
import { DataService } from '@/services/DataService';
import { DataTable } from '@/app/components/common/DataTable';
import { Button } from '@/app/components/ui/button';
import { Database, RefreshCw } from 'lucide-react';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';

// Utility to debounce or just simple state for now
export const DataExplorerPage: React.FC = () => {
  const [layer, setLayer] = useState<'silver' | 'gold' | 'bronze'>('gold');
  const [domain, setDomain] = useState<string>('market');
  const [ticker, setTicker] = useState<string>('');
  const [limit, setLimit] = useState<number>(100);
  const [data, setData] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const tickerRequired = domain.startsWith('finance/') && layer !== 'bronze';

  const fetchData = useCallback(async () => {
    const normalizedTicker = ticker.trim().toUpperCase();
    if (tickerRequired && !normalizedTicker) {
      setError('Ticker is required for Finance domains.');
      setData([]);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const result = await DataService.getGenericData(
        layer,
        domain,
        normalizedTicker || undefined,
        limit
      );
      setData(result);
    } catch (err) {
      setError(formatSystemStatusText(err));
    } finally {
      setLoading(false);
    }
  }, [layer, domain, ticker, limit, tickerRequired]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      fetchData();
    }
  };

  const controlClass =
    'h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm font-mono outline-none transition-shadow focus-visible:ring-2 focus-visible:ring-ring/40';

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title flex items-center gap-2">
          <Database className="h-5 w-5 text-mcm-teal" />
          Data Explorer
        </h1>
        <p className="page-subtitle">
          Direct access to generic data layers. Query by domain and limit results.
        </p>
      </div>

      <div className="mcm-panel p-4 sm:p-5">
        {/* Layer Selector */}
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-[180px_260px_180px_140px_1fr_auto] lg:items-end">
          <div className="space-y-2">
            <label htmlFor="data-explorer-layer">Layer</label>
            <select
              id="data-explorer-layer"
              value={layer}
              onChange={(e) => setLayer(e.target.value as 'silver' | 'gold' | 'bronze')}
              className={controlClass}
            >
              <option value="bronze">BRONZE</option>
              <option value="silver">SILVER</option>
              <option value="gold">GOLD</option>
            </select>
          </div>

          <div className="space-y-2">
            <label htmlFor="data-explorer-domain">Domain</label>
            <select
              id="data-explorer-domain"
              value={domain}
              onChange={(e) => setDomain(e.target.value)}
              className={controlClass}
            >
              <option value="market">market</option>
              <option value="earnings">earnings</option>
              <option value="price-target">price-target</option>
              <option value="finance/balance_sheet">finance/balance_sheet</option>
              <option value="finance/income_statement">finance/income_statement</option>
              <option value="finance/cash_flow">finance/cash_flow</option>
              <option value="finance/valuation">finance/valuation</option>
            </select>
          </div>

          <div className="space-y-2">
            <label htmlFor="data-explorer-ticker">Ticker ({tickerRequired ? 'Req' : 'Opt'})</label>
            <input
              id="data-explorer-ticker"
              type="text"
              value={ticker}
              onChange={(e) => setTicker(e.target.value.toUpperCase())}
              onKeyDown={handleKeyDown}
              className={`${controlClass} uppercase`}
              placeholder="AAPL"
            />
          </div>

          <div className="space-y-2">
            <label htmlFor="data-explorer-limit">Limit</label>
            <input
              id="data-explorer-limit"
              type="number"
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              onKeyDown={handleKeyDown}
              min={1}
              max={10000}
              className={controlClass}
            />
          </div>

          <div />

          <Button
            onClick={fetchData}
            disabled={loading || (tickerRequired && !ticker.trim())}
            className="h-10 gap-2 px-6"
          >
            {loading ? <RefreshCw className="h-4 w-4 animate-spin" /> : null}
            {loading ? 'Loading…' : 'Fetch Data'}
          </Button>
        </div>
      </div>

      {tickerRequired && !ticker.trim() && (
        <div className="text-xs font-mono text-muted-foreground">
          Enter a ticker to query finance domains.
        </div>
      )}

      {layer === 'bronze' && !tickerRequired && !ticker.trim() && (
        <div className="text-xs font-mono text-muted-foreground">
          Bronze ticker is optional. Leaving it blank returns the first matching file found for the
          selected domain.
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/10 p-4 font-mono text-sm text-destructive">
          <strong>Error:</strong> {error}
        </div>
      )}

      <div className="flex-1 overflow-hidden flex flex-col min-h-[400px]">
        <DataTable data={data} className="flex-1" />
        <div className="mt-2 text-right font-mono text-xs text-muted-foreground">
          Returning {data.length} rows.
        </div>
      </div>
    </div>
  );
};
