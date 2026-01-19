// Top sticky header with global controls

import { ShoppingCart, Download, User, Moon, Sun } from 'lucide-react';
import { useApp } from '@/contexts/AppContext';
import { Button } from '@/app/components/ui/button';
import { Badge } from '@/app/components/ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/app/components/ui/select';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/app/components/ui/dropdown-menu';

export function AppHeader() {
  const {
    selectedRuns,
    cartOpen,
    setCartOpen,
    isDarkMode,
    setIsDarkMode,
    environment,
    setEnvironment,
    dateRange,
    setDateRange,
    benchmark,
    setBenchmark,
    costModel,
    setCostModel
  } = useApp();
  
  return (
    <div className="sticky top-0 z-50 border-b bg-background">
      <div className="flex items-center justify-between h-14 px-4">
        {/* Left: Branding */}
        <div className="flex items-center gap-3">
          <h1 className="text-base font-medium">QuantCore Analytics</h1>
          <Badge 
            variant={environment === 'PROD' ? 'destructive' : 'secondary'}
            className="cursor-pointer text-xs"
            onClick={() => setEnvironment(environment === 'DEV' ? 'PROD' : 'DEV')}
          >
            {environment}
          </Badge>
        </div>
        
        {/* Center: Global Controls */}
        <div className="flex items-center gap-4 flex-1 max-w-3xl mx-4">
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">Date:</span>
            <Select value="5Y" onValueChange={(v) => {
              const presets: Record<string, any> = {
                'YTD': { start: '2025-01-01', end: '2025-01-19' },
                '1Y': { start: '2024-01-01', end: '2025-01-01' },
                '3Y': { start: '2022-01-01', end: '2025-01-01' },
                '5Y': { start: '2020-01-01', end: '2025-01-01' },
                'Max': { start: '2018-01-01', end: '2025-01-01' },
              };
              if (presets[v]) setDateRange(presets[v]);
            }}>
              <SelectTrigger className="w-20 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="YTD">YTD</SelectItem>
                <SelectItem value="1Y">1Y</SelectItem>
                <SelectItem value="3Y">3Y</SelectItem>
                <SelectItem value="5Y">5Y</SelectItem>
                <SelectItem value="Max">Max</SelectItem>
              </SelectContent>
            </Select>
          </div>
          
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">Benchmark:</span>
            <Select value={benchmark} onValueChange={setBenchmark}>
              <SelectTrigger className="w-20 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="SPY">SPY</SelectItem>
                <SelectItem value="QQQ">QQQ</SelectItem>
                <SelectItem value="ACWI">ACWI</SelectItem>
                <SelectItem value="Custom">Custom</SelectItem>
              </SelectContent>
            </Select>
          </div>
          
          <div className="flex items-center gap-2 text-xs">
            <span className="text-muted-foreground">Costs:</span>
            <Select value={costModel} onValueChange={setCostModel}>
              <SelectTrigger className="w-28 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="Zero Costs">Zero</SelectItem>
                <SelectItem value="Passive bps">Passive</SelectItem>
                <SelectItem value="Aggressive">Aggressive</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        
        {/* Right: Actions */}
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setCartOpen(!cartOpen)}
            className="relative"
          >
            <ShoppingCart className="h-4 w-4 mr-1.5" />
            {selectedRuns.size}
          </Button>
          
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm">
                <Download className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem>PDF Report</DropdownMenuItem>
              <DropdownMenuItem>Excel Export</DropdownMenuItem>
              <DropdownMenuItem>CSV Export</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
          
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setIsDarkMode(!isDarkMode)}
          >
            {isDarkMode ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          </Button>
          
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm">
                <User className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem>Settings</DropdownMenuItem>
              <DropdownMenuItem>API Keys</DropdownMenuItem>
              <DropdownMenuItem>Defaults</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    </div>
  );
}