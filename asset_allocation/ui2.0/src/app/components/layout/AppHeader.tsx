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
import { useAuth } from '@/contexts/AuthContext';

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
  const auth = useAuth();

  return (
    <div className="sticky top-0 z-50 border-b border-sidebar-border bg-sidebar shadow-sm text-sidebar-foreground transition-colors duration-300">
      <div className="flex items-center justify-between h-16 px-6">
        {/* Left: Branding */}
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-bold text-sidebar-foreground">QuantCore Analytics</h1>
          <Badge
            variant={environment === 'PROD' ? 'destructive' : 'secondary'}
            className="cursor-pointer"
            onClick={() => setEnvironment(environment === 'DEV' ? 'PROD' : 'DEV')}
          >
            {environment}
          </Badge>
        </div>

        {/* Center: Global Controls */}
        <div className="flex items-center gap-4 flex-1 justify-center max-w-4xl mx-4">
          {/* Data Source Toggle */}
          <div className="flex items-center gap-2">
            <Database className="h-4 w-4 text-sidebar-foreground/70" />
            <div className="flex bg-sidebar-accent rounded-lg p-0.5">
              <button
                className={`px-2 py-0.5 text-xs font-medium rounded-md transition-all ${dataSource === 'mock' ? 'bg-sidebar-primary text-sidebar-primary-foreground shadow-sm' : 'text-sidebar-foreground/70 hover:text-sidebar-foreground'}`}
                onClick={() => setDataSource('mock')}
              >
                Mock
              </button>
              <button
                className={`px-2 py-0.5 text-xs font-medium rounded-md transition-all ${dataSource === 'live' ? 'bg-sidebar-primary text-sidebar-primary-foreground shadow-sm' : 'text-sidebar-foreground/70 hover:text-sidebar-foreground'}`}
                onClick={() => setDataSource('live')}
              >
                Live
              </button>
            </div>
          </div>
        </div>

        {/* Right: Actions */}
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            className="relative h-9 w-9 rounded-full text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
          >
            <Bell className="h-5 w-5" />
            <Badge className="absolute -top-1 -right-1 h-4 w-4 flex items-center justify-center p-0 bg-orange-500 text-white text-[10px] border-2 border-sidebar">
              1
            </Badge>
          </Button>

          <Button
            variant="ghost"
            size="sm"
            onClick={() => setCartOpen(!cartOpen)}
            className="relative h-9 w-9 rounded-full text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
          >
            <ShoppingCart className="h-5 w-5" />
            {selectedRuns.size > 0 && (
              <Badge className="absolute -top-1 -right-1 h-4 w-4 flex items-center justify-center p-0 bg-primary text-primary-foreground text-[10px]">
                {selectedRuns.size}
              </Badge>
            )}
          </Button>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm" className="h-9 w-9 rounded-full text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground">
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
            className="h-9 w-9 rounded-full text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
          >
            {isDarkMode ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
          </Button>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="sm" className="h-9 w-9 rounded-full text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground">
                <User className="h-5 w-5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {auth.enabled && auth.userLabel && (
                <DropdownMenuItem disabled>{auth.userLabel}</DropdownMenuItem>
              )}
              {auth.enabled && !auth.authenticated && (
                <DropdownMenuItem onClick={auth.signIn}>Sign in</DropdownMenuItem>
              )}
              {auth.enabled && auth.authenticated && (
                <DropdownMenuItem onClick={auth.signOut}>Sign out</DropdownMenuItem>
              )}
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
