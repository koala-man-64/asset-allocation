// Top sticky header with global controls

import { ShoppingCart, Download, User, Moon, Sun, Database, Bell } from 'lucide-react';
import { useApp } from '@/contexts/AppContext';
import { Button } from '@/app/components/ui/button';
import { Badge } from '@/app/components/ui/badge';
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
    dataSource,
    setDataSource
  } = useApp();
  
  return (
    <div className="sticky top-0 z-50 border-b bg-card shadow-sm">
      <div className="flex items-center justify-between h-16 px-6">
        {/* Left: Branding */}
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-bold text-foreground">QuantCore Analytics</h1>
          <Badge 
            variant={environment === 'PROD' ? 'destructive' : 'secondary'}
            className="cursor-pointer"
            onClick={() => setEnvironment(environment === 'DEV' ? 'PROD' : 'DEV')}
          >
            {environment}
          </Badge>
        </div>
        
        {/* Center: Data Source Toggle */}
        <div className="flex items-center gap-3">
          <Database className="h-4 w-4 text-muted-foreground" />
          <span className="text-sm text-muted-foreground font-medium">Data:</span>
          <Button
            variant={dataSource === 'mock' ? 'default' : 'outline'}
            size="sm"
            onClick={() => setDataSource('mock')}
            className="h-8"
          >
            Mock
          </Button>
          <Button
            variant={dataSource === 'live' ? 'default' : 'outline'}
            size="sm"
            onClick={() => setDataSource('live')}
            className="h-8"
          >
            Live
          </Button>
        </div>
        
        {/* Right: Actions */}
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="relative h-9 w-9 rounded-full"
          >
            <Bell className="h-5 w-5" />
            <Badge className="absolute -top-1 -right-1 h-5 w-5 flex items-center justify-center p-0 bg-orange-500 text-white text-xs border-2 border-card">
              1
            </Badge>
          </Button>
          
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setCartOpen(!cartOpen)}
            className="relative h-9 w-9 rounded-full"
          >
            <ShoppingCart className="h-5 w-5" />
            {selectedRuns.size > 0 && (
              <Badge className="absolute -top-1 -right-1 h-5 w-5 flex items-center justify-center p-0 bg-primary text-primary-foreground text-xs">
                {selectedRuns.size}
              </Badge>
            )}
          </Button>
          
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm" className="h-9 w-9 rounded-full">
                <Download className="h-5 w-5" />
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
            className="h-9 w-9 rounded-full"
          >
            {isDarkMode ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
          </Button>
          
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm" className="h-9 w-9 rounded-full">
                <User className="h-5 w-5" />
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