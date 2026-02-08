import { useState, useRef, useCallback, useEffect } from 'react';
import { DataService } from '@/services/DataService';
import {
    domainKey,
    getProbeIdForRow,
    isValidTickerSymbol,
    normalizeDomainName,
    normalizeLayerName,
    type DomainRow
} from '../app/components/pages/data-quality/dataQualityUtils';

export type ProbeStatus = 'idle' | 'running' | 'pass' | 'warn' | 'fail';

export type ProbeResult = {
    status: ProbeStatus;
    at: string;
    ms?: number;
    title: string;
    detail?: string;
    meta?: Record<string, unknown>;
};

const PROBE_TIMEOUT_MS = 20_000;
const RUN_ALL_CONCURRENCY = 3;

interface UseDataProbesProps {
    financeSubDomain: string;
    ticker: string;
    rows: DomainRow[];
}

export function useDataProbes({ financeSubDomain, ticker, rows }: UseDataProbesProps) {
    const [probeResults, setProbeResults] = useState<Record<string, ProbeResult>>({});
    const [isRunningAll, setIsRunningAll] = useState(false);
    const [runAllStatusMessage, setRunAllStatusMessage] = useState<string | null>(null);

    const runAllCancelledRef = useRef(false);
    const runAllControllersRef = useRef<Set<AbortController>>(new Set());

    // Cleanup on unmount
    useEffect(() => {
        return () => {
            runAllCancelledRef.current = true;
            // eslint-disable-next-line react-hooks/exhaustive-deps
            for (const controller of runAllControllersRef.current) {
                controller.abort();
            }
            runAllControllersRef.current.clear();
        };
    }, []);

    const runProbe = useCallback(
        async (
            id: string,
            title: string,
            fn: (
                signal: AbortSignal
            ) => Promise<{ ok: boolean; detail?: string; meta?: Record<string, unknown> }>
        ) => {
            const started = performance.now();
            const controller = new AbortController();
            runAllControllersRef.current.add(controller);
            const timeoutHandle = window.setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);

            setProbeResults((prev) => ({
                ...prev,
                [id]: {
                    status: 'running',
                    title,
                    at: new Date().toISOString()
                }
            }));

            try {
                const result = await fn(controller.signal);
                const ms = performance.now() - started;
                const status: ProbeStatus = result.ok ? 'pass' : 'fail';
                console.info('[DataQualityProbe] completed', {
                    probeId: id,
                    title,
                    status,
                    durationMs: Math.round(ms)
                });
                setProbeResults((prev) => ({
                    ...prev,
                    [id]: {
                        status,
                        title,
                        at: new Date().toISOString(),
                        ms,
                        detail: result.detail,
                        meta: result.meta
                    }
                }));
            } catch (err: unknown) {
                const ms = performance.now() - started;
                const isAbort = controller.signal.aborted;
                const message = isAbort
                    ? runAllCancelledRef.current
                        ? 'Probe cancelled.'
                        : `Probe timed out after ${Math.round(PROBE_TIMEOUT_MS / 1000)}s.`
                    : err instanceof Error
                        ? err.message
                        : String(err);

                console.warn('[DataQualityProbe] failed', {
                    probeId: id,
                    title,
                    durationMs: Math.round(ms),
                    reason: message
                });

                setProbeResults((prev) => ({
                    ...prev,
                    [id]: {
                        status: 'fail',
                        title,
                        at: new Date().toISOString(),
                        ms,
                        detail: message
                    }
                }));
            } finally {
                window.clearTimeout(timeoutHandle);
                runAllControllersRef.current.delete(controller);
            }
        },
        []
    );

    const probeForRow = useCallback(
        async (row: DomainRow) => {
            const layer = normalizeLayerName(row.layerName);
            const domain = normalizeDomainName(row.domain.name);
            const resolvedTicker = ticker.trim().toUpperCase();

            const probeId = getProbeIdForRow(row.layerName, row.domain.name, financeSubDomain) || `row:${domainKey(row)}`;

            if (!resolvedTicker) {
                setProbeResults((prev) => ({
                    ...prev,
                    [probeId]: {
                        status: 'fail',
                        title: 'Probe',
                        at: new Date().toISOString(),
                        detail: 'Ticker is required.'
                    }
                }));
                return;
            }

            if (!isValidTickerSymbol(resolvedTicker)) {
                setProbeResults((prev) => ({
                    ...prev,
                    [probeId]: {
                        status: 'fail',
                        title: 'Probe',
                        at: new Date().toISOString(),
                        detail: 'Ticker must match ^[A-Z][A-Z0-9.-]{0,9}$.'
                    }
                }));
                return;
            }

            const marketCheck = async (signal: AbortSignal) => {
                const data = await DataService.getMarketData(resolvedTicker, layer, signal);
                const count = Array.isArray(data) ? data.length : 0;
                return {
                    ok: count > 0,
                    detail: count > 0 ? `Rows: ${count.toLocaleString()}` : 'No rows returned.',
                    meta: { count }
                };
            };

            const financeCheck = async (signal: AbortSignal) => {
                const data = await DataService.getFinanceData(
                    resolvedTicker,
                    financeSubDomain,
                    layer,
                    signal
                );
                const count = Array.isArray(data) ? data.length : 0;
                return {
                    ok: count > 0,
                    detail: count > 0 ? `Rows: ${count.toLocaleString()}` : 'No rows returned.',
                    meta: { count, subDomain: financeSubDomain }
                };
            };

            const genericCheck = async (signal: AbortSignal) => {
                const data = await DataService.getGenericData(
                    layer,
                    domain,
                    resolvedTicker,
                    undefined,
                    signal
                );
                const count = Array.isArray(data) ? data.length : 0;
                const sampleKeys =
                    count > 0 && data && typeof data[0] === 'object' && data[0] !== null
                        ? Object.keys(data[0] as object).slice(0, 8)
                        : [];
                return {
                    ok: count > 0,
                    detail:
                        count > 0
                            ? `Rows: ${count.toLocaleString()} • Keys: ${sampleKeys.join(', ') || '—'}`
                            : 'No rows returned.',
                    meta: { count, sampleKeys }
                };
            };

            if ((layer === 'silver' || layer === 'gold') && domain === 'market') {
                await runProbe(`probe:${layer}:market`, `Market (${layer})`, marketCheck);
                return;
            }

            if ((layer === 'silver' || layer === 'gold') && domain === 'finance') {
                await runProbe(`probe:${layer}:finance:${financeSubDomain}`, `Finance (${layer})`, financeCheck);
                return;
            }

            if (
                (layer === 'silver' || layer === 'gold') &&
                (domain === 'earnings' || domain === 'price-target')
            ) {
                await runProbe(`probe:${layer}:${domain}`, `${domain} (${layer})`, genericCheck);
                return;
            }

            setProbeResults((prev) => ({
                ...prev,
                [probeId]: {
                    status: 'warn',
                    title: 'Probe',
                    at: new Date().toISOString(),
                    detail: 'No active probe is defined for this container/folder.'
                }
            }));
        },
        [financeSubDomain, runProbe, ticker]
    );

    const runAll = useCallback(async () => {
        if (isRunningAll) return;
        const supported = rows.filter((row) => {
            const layer = normalizeLayerName(row.layerName);
            const domain = normalizeDomainName(row.domain.name);
            return (
                (layer === 'silver' || layer === 'gold') &&
                ['market', 'finance', 'earnings', 'price-target'].includes(domain)
            );
        });

        if (supported.length === 0) {
            setRunAllStatusMessage('No supported probes found in current ledger.');
            return;
        }

        runAllCancelledRef.current = false;
        setRunAllStatusMessage(null);
        setIsRunningAll(true);
        const queue = [...supported];

        const workers = Array.from(
            { length: Math.min(RUN_ALL_CONCURRENCY, queue.length) },
            async () => {
                while (!runAllCancelledRef.current) {
                    const row = queue.shift();
                    if (!row) {
                        return;
                    }
                    await probeForRow(row);
                }
            }
        );

        try {
            await Promise.all(workers);
            setRunAllStatusMessage(
                runAllCancelledRef.current ? 'Probe run cancelled.' : 'Probe run complete.'
            );
        } finally {
            setIsRunningAll(false);
        }
    }, [isRunningAll, probeForRow, rows]);

    const cancelRunAll = useCallback(() => {
        if (!isRunningAll) return;
        runAllCancelledRef.current = true;
        for (const controller of runAllControllersRef.current) {
            controller.abort();
        }
        setRunAllStatusMessage('Cancelling probes...');
    }, [isRunningAll]);

    return {
        probeResults,
        setProbeResults, // Exported if needed for manual sets/clears
        runProbe,
        probeForRow,
        runAll,
        cancelRunAll,
        isRunningAll,
        runAllStatusMessage,
        setRunAllStatusMessage
    };
}
