// Run comparison cart/tray component

import { X, GitCompare, Folder } from 'lucide-react';
import { useApp } from '@/contexts/AppContext';
import { mockStrategies } from '@/data/mockData';
import { Button } from '@/app/components/ui/button';
import { Badge } from '@/app/components/ui/badge';
import { Sheet, SheetContent, SheetHeader, SheetTitle, SheetDescription } from '@/app/components/ui/sheet';

interface RunCartProps {
  onCompare: () => void;
  onPortfolioBuilder: () => void;
}

export function RunCart({ onCompare, onPortfolioBuilder }: RunCartProps) {
  const { selectedRuns, removeFromCart, clearCart, cartOpen, setCartOpen } = useApp();
  
  const selectedStrategies = mockStrategies.filter(s => selectedRuns.has(s.id));
  
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
                {selectedStrategies.map((strategy, index) => (
                  <div
                    key={strategy.id}
                    className="flex items-center gap-3 p-3 border rounded-lg hover:bg-muted/50 transition-colors"
                  >
                    <div className={`w-3 h-3 rounded-full ${getColorForIndex(index)}`} />
                    <div className="flex-1 min-w-0">
                      <div className="font-medium truncate">{strategy.name}</div>
                      <div className="text-sm text-muted-foreground">
                        Sharpe: {strategy.sharpe.toFixed(2)} | CAGR: {strategy.cagr.toFixed(1)}%
                      </div>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => removeFromCart(strategy.id)}
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </div>
                ))}
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