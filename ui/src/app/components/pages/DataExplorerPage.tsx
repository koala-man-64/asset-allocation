import React, { useState, useCallback } from 'react';
import { DataService } from '@/services/DataService';
import { DataTable } from '@/app/components/common/DataTable';

// Utility to debounce or just simple state for now
export const DataExplorerPage: React.FC = () => {
    const [layer, setLayer] = useState<'silver' | 'gold' | 'bronze'>('gold');
    const [domain, setDomain] = useState<string>('market');
    const [ticker, setTicker] = useState<string>('');
    const [limit, setLimit] = useState<number>(100);
    const [data, setData] = useState<Record<string, unknown>[]>([]);
    const [loading, setLoading] = useState<boolean>(false);
    const [error, setError] = useState<string | null>(null);

    const fetchData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await DataService.getGenericData(layer, domain, ticker || undefined, limit);
            setData(result);
        } catch (err) {
            setError(err instanceof Error ? err.message : String(err));
        } finally {
            setLoading(false);
        }
    }, [layer, domain, ticker, limit]);

    const handleKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            fetchData();
        }
    };

    return (
        <div className="p-6 min-h-screen bg-gray-50 flex flex-col gap-6">
            <div className="flex flex-col gap-2">
                <h1 className="text-2xl font-bold text-gray-800 font-mono tracking-tight uppercase">
                    Data Explorer // Top N Rows
                </h1>
                <p className="text-sm text-gray-500 font-mono">
                    Direct access to generic data layers. Query by domain and limit results.
                </p>
            </div>

            <div className="bg-white p-4 border border-gray-300 shadow-sm flex flex-wrap gap-4 items-end">
                {/* Layer Selector */}
                <div className="flex flex-col gap-1">
                    <label className="text-xs font-bold text-gray-600 font-mono uppercase">Layer</label>
                    <select
                        value={layer}
                        onChange={(e) => setLayer(e.target.value as 'silver' | 'gold' | 'bronze')}
                        className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-32 bg-gray-50"
                    >
                        <option value="bronze">BRONZE</option>
                        <option value="silver">SILVER</option>
                        <option value="gold">GOLD</option>
                    </select>
                </div>

                {/* Domain Selector (Strict Dropdown) */}
                <div className="flex flex-col gap-1">
                    <label className="text-xs font-bold text-gray-600 font-mono uppercase">Domain</label>
                    <select
                        value={domain}
                        onChange={(e) => setDomain(e.target.value)}
                        className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-48 bg-gray-50"
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

                {/* Ticker Input */}
                <div className="flex flex-col gap-1">
                    <label className="text-xs font-bold text-gray-600 font-mono uppercase">Ticker (Opt)</label>
                    <input
                        type="text"
                        value={ticker}
                        onChange={(e) => setTicker(e.target.value.toUpperCase())}
                        onKeyDown={handleKeyDown}
                        className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-32 bg-gray-50 uppercase"
                        placeholder="AAPL"
                    />
                </div>

                {/* Limit Input */}
                <div className="flex flex-col gap-1">
                    <label className="text-xs font-bold text-gray-600 font-mono uppercase">Limit (N)</label>
                    <input
                        type="number"
                        value={limit}
                        onChange={(e) => setLimit(Number(e.target.value))}
                        onKeyDown={handleKeyDown}
                        min={1}
                        max={10000}
                        className="border border-gray-300 px-3 py-2 text-sm font-mono focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none w-24 bg-gray-50"
                    />
                </div>

                {/* Fetch Button */}
                <button
                    onClick={fetchData}
                    disabled={loading}
                    className="bg-gray-800 text-white px-6 py-2 text-sm font-bold font-mono tracking-wide hover:bg-black disabled:bg-gray-400 transition-colors shadow-sm ml-auto"
                >
                    {loading ? 'LOADING...' : 'FETCH DATA'}
                </button>
            </div>

            {error && (
                <div className="p-4 border-l-4 border-red-500 bg-red-50 text-red-700 font-mono text-sm">
                    <strong>ERROR:</strong> {error}
                </div>
            )}

            <div className="flex-1 overflow-hidden flex flex-col min-h-[400px]">
                <DataTable data={data} className="flex-1 shadow-sm" />
                <div className="mt-2 text-xs text-gray-400 font-mono text-right">
                    Returning {data.length} rows.
                </div>
            </div>
        </div>
    );
};
