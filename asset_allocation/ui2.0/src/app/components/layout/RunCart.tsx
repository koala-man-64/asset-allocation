// Run comparison cart/tray component

import { X, GitCompare, Folder } from 'lucide-react';
import { useApp } from '@/contexts/AppContext';
import { useRunList, useRunSummaries } from '@/services/backtestHooks';
import { formatNumber, formatPercentDecimal } from '@/utils/format';
import { Button } from '@/app/components/ui/button';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from '@/app/components/ui/sheet';
import { useMemo } from 'react';

interface RunCartProps {
  onCompare: () => void;
  onPortfolioBuilder: () => void;
}

export function RunCart({ onCompare, onPortfolioBuilder }: RunCartProps) {
  const { selectedRuns, removeFromCart, clearCart, cartOpen, setCartOpen } = useApp();
  
  const selectedRunIds = useMemo(() => Array.from(selectedRuns.values()), [selectedRuns]);
  const { runs } = useRunList({ limit: 200, offset: 0 });
  const runsById = useMemo(() => new Map(runs.map((r) => [r.run_id, r])), [runs]);
  const { summaries } = useRunSummaries(selectedRunIds, { source: 'auto' });
  
  const getColorForIndex = (index: number): string => {
    const colors = ['bg-blue-500', 'bg-green-500', 'bg-orange-500', 'bg-purple-500', 'bg-pink-500', 'bg-yellow-500'];
    return colors[index % colors.length];
  };
  
  return (
    <Sheet open={cartOpen} onOpenChange={setCartOpen}>
      <SheetContent side="right" className="w-[400px] sm:w-[540px]">
        <SheetHeader>
          <SheetTitle>Selected Runs for Comparison</SheetTitle>
          <SheetDescription>Select strategies from the Overview table to compare</SheetDescription>
        </SheetHeader>
        
        <div className="mt-6 space-y-4">
          {selectedRuns.size === 0 ? (
            <div className="text-center py-12 text-muted-foreground">
              <p>No runs selected</p>
              <p className="text-sm mt-2">Select strategies from the Overview table to compare</p>
            </div>
          ) : (
            <>
              <div className="space-y-2">
                {selectedRunIds.map((runId, index) => {
                  const run = runsById.get(runId);
                  const summary = summaries[runId] ?? null;
                  const name = run?.run_name || runId;
                  const sharpe = summary ? formatNumber(Number(summary.sharpe_ratio), 2) : '—';
                  const annReturn = summary ? formatPercentDecimal(Number(summary.annualized_return), 1) : '—';

                  return (
                  <div
                    key={runId}
                    className="flex items-center gap-3 p-3 border rounded-lg hover:bg-muted/50 transition-colors"
                  >
                    <div className={`w-3 h-3 rounded-full ${getColorForIndex(index)}`} />
                    <div className="flex-1 min-w-0">
                      <div className="font-medium truncate">{name}</div>
                      <div className="text-sm text-muted-foreground">
                        Sharpe: {sharpe} | Ann. Return: {annReturn}
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeFromCart(runId)}
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                  );
                })}
              </div>
              
              <div className="pt-4 border-t space-y-2">
                <Button
                  className="w-full"
                  onClick={() => {
                    onCompare();
                    setCartOpen(false);
                  }}
                  disabled={selectedRuns.size < 2}
                >
                  <GitCompare className="h-4 w-4 mr-2" />
                  Compare {selectedRuns.size} Runs
                </Button>
                
                <Button
                  variant="outline"
                  className="w-full"
                  onClick={() => {
                    onPortfolioBuilder();
                    setCartOpen(false);
                  }}
                  disabled={selectedRuns.size < 2}
                >
                  <Folder className="h-4 w-4 mr-2" />
                  Create Portfolio
                </Button>
                
                <Button
                  variant="ghost"
                  className="w-full"
                  onClick={clearCart}
                >
                  Clear All
                </Button>
              </div>
            </>
          )}
        </div>
      </SheetContent>
    </Sheet>
  );
}
