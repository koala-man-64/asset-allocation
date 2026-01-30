import React, { useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { AlertTriangle, Trash2 } from 'lucide-react';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/app/components/ui/alert-dialog';
import { Tooltip, TooltipContent, TooltipTrigger } from '@/app/components/ui/tooltip';
import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';

export type PurgeScope = 'layer-domain' | 'layer' | 'domain';

export const normalizeLayerKey = (value: string) =>
  value
    .toLowerCase()
    .trim()
    .replace(/\s+/g, '-')
    .replace(/_/g, '-');

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
  tooltip,
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

  const handleConfirm = async () => {
    setIsBusy(true);
    try {
      const result = await DataService.purgeData({
        scope,
        layer,
        domain,
        confirm: true,
      });
      toast.success(`Purged ${result.totalDeleted} blob(s).`);
      void queryClient.invalidateQueries({ queryKey: queryKeys.systemHealth() });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      toast.error(`Purge failed: ${message}`);
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
            This will permanently delete all blobs for <strong>{scopeLabel}</strong>. Containers remain, but the data
            cannot be recovered.
          </AlertDialogDescription>
        </AlertDialogHeader>
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            onClick={() => void handleConfirm()}
            disabled={isBusy}
          >
            Purge
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
