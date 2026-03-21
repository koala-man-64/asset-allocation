import { useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { RefreshCw, Save, ShieldAlert, Trash2 } from 'lucide-react';
import { useDebugSymbolsQuery, queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import { Button } from '@/app/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/app/components/ui/card';
import { Textarea } from '@/app/components/ui/textarea';
import { Badge } from '@/app/components/ui/badge';
import { formatTimeAgo } from '@/app/components/pages/system-status/SystemStatusHelpers';
import { formatSystemStatusText } from '@/utils/formatSystemStatusText';
import { PageLoader } from '@/app/components/common/PageLoader';

const MAX_PREVIEW = 20;

function normalizeSymbols(raw: string): string[] {
  const trimmed = raw.trim();
  if (!trimmed) return [];

  if (trimmed.startsWith('[')) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed
          .map((item) => String(item).trim())
          .filter(Boolean)
          .map((item) => item.toUpperCase());
      }
    } catch {
      // Fallback to CSV parsing.
    }
  }

  return trimmed
    .replace(/[\n;]+/g, ',')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => item.toUpperCase());
}

export function DebugSymbolsPage() {
  const debugSymbolsQuery = useDebugSymbolsQuery();
  const queryClient = useQueryClient();

  const [symbolsInput, setSymbolsInput] = useState('');
  const [hasLocalChanges, setHasLocalChanges] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);

  useEffect(() => {
    if (!debugSymbolsQuery.data || hasLocalChanges) return;
    setSymbolsInput(String(debugSymbolsQuery.data.symbols || '').trim());
  }, [debugSymbolsQuery.data, hasLocalChanges]);

  const normalizedSymbols = useMemo(() => normalizeSymbols(symbolsInput), [symbolsInput]);
  const isInvalid = normalizedSymbols.length === 0;

  const currentSymbols = String(debugSymbolsQuery.data?.symbols || '').trim();
  const isConfigured = currentSymbols.length > 0;
  const isDirty = symbolsInput.trim() !== currentSymbols;

  const updatedAgo = formatTimeAgo(debugSymbolsQuery.data?.updatedAt || null);

  const handleReset = () => {
    setHasLocalChanges(false);
    setSymbolsInput(currentSymbols);
  };

  const handleSave = async () => {
    setIsSaving(true);
    try {
      await DataService.setDebugSymbols({
        symbols: symbolsInput
      });
      toast.success('Debug symbols updated.');
      setHasLocalChanges(false);
      void queryClient.invalidateQueries({ queryKey: queryKeys.debugSymbols() });
    } catch (err) {
      const message = formatSystemStatusText(err);
      toast.error(`Failed to update debug symbols: ${message}`);
    } finally {
      setIsSaving(false);
    }
  };

  const handleDelete = async () => {
    setIsDeleting(true);
    try {
      await DataService.deleteDebugSymbols();
      toast.success('Debug symbols removed.');
      setHasLocalChanges(false);
      setSymbolsInput('');
      void queryClient.invalidateQueries({ queryKey: queryKeys.debugSymbols() });
    } catch (err) {
      const message = formatSystemStatusText(err);
      toast.error(`Failed to remove debug symbols: ${message}`);
    } finally {
      setIsDeleting(false);
    }
  };

  if (debugSymbolsQuery.isLoading) {
    return <PageLoader text="Loading Debug Configuration..." />;
  }

  if (debugSymbolsQuery.error) {
    return (
      <div className="mcm-panel rounded-lg border border-destructive/30 bg-destructive/10 p-6 text-destructive">
        <div className="flex items-center gap-2 font-mono text-sm uppercase">
          <ShieldAlert className="h-4 w-4" />
          Debug Symbols Unavailable
        </div>
        <p className="mt-3 text-sm">{formatSystemStatusText(debugSymbolsQuery.error)}</p>
      </div>
    );
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <p className="page-kicker">Live Operations</p>
        <h1 className="page-title">Debug Symbols</h1>
        <p className="page-subtitle">
          Control the symbol allowlist stored in Postgres runtime config and applied at ETL startup.
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
        <Card className="mcm-panel">
          <CardHeader className="space-y-2">
            <CardTitle className="flex items-center justify-between">
              <span>Configuration</span>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  className="gap-2"
                  onClick={() =>
                    void queryClient.invalidateQueries({ queryKey: queryKeys.debugSymbols() })
                  }
                  disabled={debugSymbolsQuery.isFetching}
                >
                  <RefreshCw
                    className={`h-4 w-4 ${debugSymbolsQuery.isFetching ? 'animate-spin' : ''}`}
                  />
                  Refresh
                </Button>
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span>Updated</span>
                  <Badge
                    variant="outline"
                    className="font-mono text-[10px] uppercase tracking-widest"
                  >
                    {updatedAgo}
                  </Badge>
                </div>
              </div>
            </CardTitle>
            <p className="text-xs text-muted-foreground">
              Symbols can be comma-separated or provided as a JSON array.
            </p>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="rounded-lg border border-border/60 bg-muted/20 px-4 py-3">
              <div className="text-xs uppercase text-muted-foreground">Presence</div>
              <div className="text-sm">
                {isConfigured
                  ? 'Stored symbols are active on job startup.'
                  : 'No debug-symbol allowlist is currently stored.'}
              </div>
            </div>

            <div className="space-y-2">
              <label className="text-xs uppercase text-muted-foreground">Symbols</label>
              <Textarea
                value={symbolsInput}
                onChange={(event) => {
                  setSymbolsInput(event.target.value);
                  setHasLocalChanges(true);
                }}
                placeholder="AAPL, MSFT, NVDA"
                className="min-h-[160px] font-mono text-sm"
              />
            </div>

            <div className="flex items-center gap-3">
              <Button
                onClick={handleSave}
                disabled={isSaving || !isDirty || isInvalid}
                className="gap-2"
              >
                {isSaving ? (
                  <RefreshCw className="h-4 w-4 animate-spin" />
                ) : (
                  <Save className="h-4 w-4" />
                )}
                Save changes
              </Button>
              <Button variant="outline" onClick={handleReset} disabled={isSaving || !isDirty}>
                Reset
              </Button>
              <Button
                variant="outline"
                onClick={handleDelete}
                disabled={isDeleting || !isConfigured}
                className="gap-2"
              >
                {isDeleting ? (
                  <RefreshCw className="h-4 w-4 animate-spin" />
                ) : (
                  <Trash2 className="h-4 w-4" />
                )}
                Remove
              </Button>
            </div>
            {isInvalid && (
              <p className="text-xs text-destructive">
                Add at least one symbol before saving debug filtering.
              </p>
            )}
          </CardContent>
        </Card>

        <Card className="mcm-panel">
          <CardHeader>
            <CardTitle>Normalized Preview</CardTitle>
            <p className="text-xs text-muted-foreground">
              {normalizedSymbols.length
                ? `${normalizedSymbols.length} symbols detected`
                : 'No symbols configured'}
            </p>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex flex-wrap gap-2">
              {normalizedSymbols.slice(0, MAX_PREVIEW).map((symbol) => (
                <Badge key={symbol} variant="secondary" className="font-mono text-[11px]">
                  {symbol}
                </Badge>
              ))}
              {normalizedSymbols.length > MAX_PREVIEW && (
                <Badge variant="outline" className="font-mono text-[11px]">
                  +{normalizedSymbols.length - MAX_PREVIEW}
                </Badge>
              )}
            </div>
            <div className="rounded-lg border border-dashed border-border/70 bg-muted/30 p-3 text-xs text-muted-foreground">
              Jobs pull this list from Postgres runtime config on startup. Removing the row makes
              debug filtering disappear entirely.
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
