import { useState, useRef, useCallback, useEffect } from 'react';
import { DataService } from '@/services/DataService';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import {
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
    ticker?: string;
    rows: DomainRow[];
}

export function useDataProbes({ ticker, rows }: UseDataProbesProps) {
    const [probeResults, setProbeResults] = useState<Record<string, ProbeResult>>({});
    const [isRunningAll, setIsRunningAll] = useState(false);
    const [runAllStatusMessage, setRunAllStatusMessage] = useState<string | null>(null);

    const normalizedTicker = String(ticker || '')
        .trim()
        .toUpperCase();
    const hasValidTicker = isValidTickerSymbol(normalizedTicker);
    const skipMessage = normalizedTicker
        ? 'Invalid ticker format. Probes skipped.'
        : 'No ticker provided. Probes skipped.';

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
                    : formatSystemStatusText(err);

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
            if (!hasValidTicker) {
                setRunAllStatusMessage(skipMessage);
                return;
            }
            const layer = normalizeLayerName(row.layerName);
            const domain = normalizeDomainName(row.domain.name);
            const probeId = getProbeIdForRow(row.layerName, row.domain.name, normalizedTicker);

            if (!layer || !domain || !probeId) {
                return;
            }

            if (layer === 'platinum') {
                setProbeResults((prev) => ({
                    ...prev,
                    [probeId]: {
                        status: 'warn',
                        title: `${domain} (${layer})`,
                        at: new Date().toISOString(),
                        detail: 'Validation endpoint currently supports bronze/silver/gold only.'
                    }
                }));
                return;
            }

            await runProbe(probeId, `${domain} (${layer})`, async (signal: AbortSignal) => {
                const report = await DataService.getDataQualityValidation(
                    layer,
                    domain,
                    normalizedTicker,
                    signal
                );
                const status = String(report.status || '').toLowerCase();
                const rowCount = Number(report.rowCount || 0);
                const isError = status === 'error';
                const detail = isError
                    ? formatSystemStatusText(report.error) || 'Validation endpoint returned an error.'
                    : status === 'empty'
                        ? 'Dataset reachable • 0 rows (empty).'
                        : `Rows: ${rowCount.toLocaleString()} • Status: ${status || 'healthy'}`;

                return {
                    ok: !isError,
                    detail,
                    meta: {
                        layer,
                        domain,
                        ticker: normalizedTicker,
                        status,
                        rowCount,
                        sampleLimit: report.sampleLimit ?? null
                    }
                };
            });
        },
        [hasValidTicker, normalizedTicker, runProbe, skipMessage]
    );

    const runAll = useCallback(async () => {
        if (isRunningAll) return;
        if (!hasValidTicker) {
            setRunAllStatusMessage(skipMessage);
            return;
        }
        const supported = rows.filter((row) => normalizeLayerName(row.layerName) !== null);

        if (supported.length === 0) {
            setRunAllStatusMessage('No probe targets found in current ledger.');
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
    }, [hasValidTicker, isRunningAll, probeForRow, rows, skipMessage]);

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
