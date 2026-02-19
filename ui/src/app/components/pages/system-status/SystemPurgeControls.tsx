import React, { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { AlertTriangle, Loader2, Trash2 } from 'lucide-react';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger
} from '@/app/components/ui/alert-dialog';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';

export type PurgeScope = 'layer-domain' | 'layer' | 'domain';

const PURGE_POLL_INTERVAL_MS = 1000;
const PURGE_POLL_TIMEOUT_MS = 5 * 60_000;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const normalizeLayerKey = (value: string) =>
  value.toLowerCase().trim().replace(/\s+/g, '-').replace(/_/g, '-');

export const normalizeDomainKey = (value: string) => {
  const cleaned = normalizeLayerKey(value);
  return cleaned === 'targets' ? 'price-target' : cleaned;
};

const titleCase = (value: string) =>
  value
    .replace(/-/g, ' ')
    .split(' ')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');

export function PurgeActionIcon({
  scope,
  layer,
  domain,
  displayLayer,
  displayDomain,
  className,
  iconClassName,
  disabled,
  tooltip
}: {
  scope: PurgeScope;
  layer?: string;
  domain?: string;
  displayLayer?: string;
  displayDomain?: string;
  className?: string;
  iconClassName?: string;
  disabled?: boolean;
  tooltip?: string;
}) {
  const queryClient = useQueryClient();
  const [isBusy, setIsBusy] = useState(false);

  const waitForPurgeResult = async (operationId: string) => {
    const startedAt = Date.now();
    let attempt = 0;
    while (true) {
      let operation: unknown;
      try {
        operation = await DataService.getPurgeOperation(operationId);
      } catch {
        if (Date.now() - startedAt > PURGE_POLL_TIMEOUT_MS) {
          throw new Error(
            `Purge status polling is still failing after timeout. Operation may still be running. Check system status. operationId=${operationId}`
          );
        }

        const delay = PURGE_POLL_INTERVAL_MS + Math.min(attempt * 250, 2000);
        await sleep(delay);
        attempt += 1;
        continue;
      }

      const polledOperation = operation as {
        status?: string;
        result?: {
          totalDeleted?: number;
        };
        error?: string;
      };
      if (polledOperation.status === 'succeeded') {
        if (!polledOperation.result) {
          throw new Error('Purge completed with no result payload.');
        }
        return polledOperation.result;
      }
      if (polledOperation.status === 'failed') {
        throw new Error(polledOperation.error || 'Purge failed.');
      }
      if (Date.now() - startedAt > PURGE_POLL_TIMEOUT_MS) {
        throw new Error(`Purge is still running. Check system status for progress. operationId=${operationId}`);
      }

      const delay = PURGE_POLL_INTERVAL_MS + Math.min(attempt * 250, 2000);
      await sleep(delay);
      attempt += 1;
    }
  };

  const describePurgeFailure = (operationId: string | null, error: unknown) => {
    return operationId
      ? `Purge failed for operation ${operationId}: ${error instanceof Error ? error.message : String(error)}`
      : `Purge failed: ${error instanceof Error ? error.message : String(error)}`;
  };

  const handleConfirm = async () => {
    setIsBusy(true);
    let operationId: string | null = null;
    try {
      const operation = await DataService.purgeData({
        scope,
        layer,
        domain,
        confirm: true
      });
      operationId = operation.operationId;
      const result =
        operation.status === 'succeeded'
          ? operation.result
          : await waitForPurgeResult(operation.operationId);

      if (!result) {
        throw new Error('Purge returned no completion result.');
      }

      toast.success(`Purged ${result.totalDeleted} blob(s).`);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (err: unknown) {
      toast.error(describePurgeFailure(operationId, err));
    } finally {
      setIsBusy(false);
    }
  };

  const displayLayerName = displayLayer || titleCase(layer ?? '');
  const displayDomainName = displayDomain || titleCase(domain ?? '');
  const scopeLabel =
    scope === 'layer'
      ? `entire ${displayLayerName} layer`
      : scope === 'domain'
        ? `all ${displayDomainName} data`
        : `${displayLayerName} • ${displayDomainName}`;

  const trigger = (
    <AlertDialogTrigger asChild>
      <button
        type="button"
        className={className ?? 'p-1 hover:bg-slate-100 text-slate-500 hover:text-rose-600 rounded'}
        aria-label={tooltip || `Purge ${scopeLabel}`}
        title={tooltip || `Purge ${scopeLabel}`}
        disabled={disabled || isBusy}
      >
        <Trash2 className={iconClassName ?? 'h-4 w-4'} />
      </button>
    </AlertDialogTrigger>
  );

  return (
    <AlertDialog>
      {tooltip ? (
        <Tooltip>
          <TooltipTrigger asChild>{trigger}</TooltipTrigger>
          <TooltipContent side="bottom">{tooltip}</TooltipContent>
        </Tooltip>
      ) : (
        trigger
      )}
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-destructive" />
            Confirm purge
          </AlertDialogTitle>
          <AlertDialogDescription>
            This will permanently delete all blobs for <strong>{scopeLabel}</strong>. Containers
            remain, but the data cannot be recovered.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            onClick={() => void handleConfirm()}
            disabled={isBusy}
          >
            {isBusy ? (
              <span className="inline-flex items-center gap-2">
                <Loader2 className="h-4 w-4 animate-spin" />
                Purging
              </span>
            ) : (
              'Purge'
            )}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
