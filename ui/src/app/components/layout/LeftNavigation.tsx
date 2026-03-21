import { useState, useEffect, type ElementType } from 'react';
import { NavLink } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import { Button } from '@/app/components/ui/button';
import { cn } from '@/app/components/ui/utils';
import { getCentralClockParts } from '@/app/components/pages/system-status/systemStatusClock';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger
} from '@/app/components/ui/tooltip';
import {
  Folder,
  ChevronLeft,
  ChevronRight,
  Activity,
  Target,
  Layers3,
  BarChart3,
  ScanSearch,
  Pin,
  PinOff,
  Globe,
  Orbit,
  Database,
  Bug,
  SlidersHorizontal,
  Filter
} from 'lucide-react';
import Cookies from 'js-cookie';

interface NavItem {
  path: string;
  label: string;
  icon: ElementType;
}

interface NavSection {
  title: string;
  items: NavItem[];
}

const navSections: NavSection[] = [
  {
    title: 'MARKET INTELLIGENCE',
    items: [
      { path: '/stock-explorer', label: 'Stock Explorer', icon: Globe },
      { path: '/stock-detail', label: 'Live Stock View', icon: Target }
    ]
  },
  {
    title: 'LIVE OPERATIONS',
    items: [
      { path: '/data-explorer', label: 'Data Explorer', icon: Folder },
      { path: '/data-quality', label: 'Data Quality', icon: ScanSearch },
      { path: '/data-profiling', label: 'Data Profiling', icon: BarChart3 },
      { path: '/regimes', label: 'Regime Monitor', icon: Orbit },
      { path: '/system-status', label: 'System Status', icon: Activity },
      { path: '/debug-symbols', label: 'Debug Symbols', icon: Bug },
      { path: '/symbol-purge', label: 'Symbol Purge', icon: Filter },
      { path: '/runtime-config', label: 'Runtime Config', icon: SlidersHorizontal },
      { path: '/strategy-exploration', label: 'Strategy Exploration', icon: Target },
      { path: '/strategies', label: 'Run Configurations', icon: Target },
      { path: '/universes', label: 'Universe Configurations', icon: Globe },
      { path: '/rankings', label: 'Ranking Configurations', icon: Layers3 },
      { path: '/postgres-explorer', label: 'Postgres Explorer', icon: Database }
    ]
  }
];

// Helper to find item by path
const findNavItem = (path: string): NavItem | undefined => {
  for (const section of navSections) {
    const found = section.items.find((item) => item.path === path);
    if (found) return found;
  }
  return undefined;
};

const PINNED_TABS_COOKIE = 'ag_pinned_tabs';
const EXPANDED_NAV_WIDTH_CLASS = 'w-[280px]';

export function LeftNavigation() {
  const [collapsed, setCollapsed] = useState(false);
  const [pinnedPaths, setPinnedPaths] = useState<string[]>([]);
  const [isClient, setIsClient] = useState(false);
  const [clockNow, setClockNow] = useState(() => new Date());
  const queryClient = useQueryClient();

  useEffect(() => {
    setIsClient(true);
    const saved = Cookies.get(PINNED_TABS_COOKIE);
    if (saved) {
      try {
        setPinnedPaths(JSON.parse(saved));
      } catch (e) {
        console.error('Failed to parse pinned tabs cookie', e);
      }
    }
  }, []);

  useEffect(() => {
    const handle = window.setInterval(() => setClockNow(new Date()), 1000);
    return () => window.clearInterval(handle);
  }, []);

  const centralClock = getCentralClockParts(clockNow);

  const togglePin = (path: string) => {
    setPinnedPaths((prev) => {
      const next = prev.includes(path) ? prev.filter((p) => p !== path) : [...prev, path];

      Cookies.set(PINNED_TABS_COOKIE, JSON.stringify(next), { expires: 365 });
      return next;
    });
  };

  const pinnedItems = pinnedPaths
    .map((path) => findNavItem(path))
    .filter((item): item is NavItem => !!item);

  const visibleSections = navSections
    .map((section) => ({
      ...section,
      items: section.items.filter((item) => !pinnedPaths.includes(item.path))
    }))
    .filter((section) => section.items.length > 0);

  // Grouped Rendering Helper
  const renderNavItem = (item: NavItem, isPinnedSection: boolean = false) => {
    const isPinned = pinnedPaths.includes(item.path);

    return (
      <div key={item.path} className="group relative flex items-center">
        <TooltipProvider delayDuration={0}>
          <Tooltip>
            <TooltipTrigger asChild>
              <NavLink
                to={item.path}
                onMouseEnter={() => {
                  if (item.path === '/data-quality' || item.path === '/system-status') {
                    if (item.path === '/data-quality') {
                      queryClient.prefetchQuery({
                        queryKey: queryKeys.systemHealth(),
                        queryFn: async () => {
                          const response = await DataService.getSystemHealthWithMeta();
                          return response.data;
                        },
                        staleTime: 30000
                      });
                    }
                    if (item.path === '/system-status') {
                      queryClient.prefetchQuery({
                        queryKey: queryKeys.systemStatusView(),
                        queryFn: async () => DataService.getSystemStatusView(),
                        staleTime: 30000
                      });
                    }
                  }
                }}
                className={({ isActive }) =>
                  cn(
                    'w-full px-3 py-2 rounded-md transition-colors',
                    'hover:bg-accent/50 group-hover:pr-9', // Make space for pin button on hover
                    isActive
                      ? 'bg-accent text-accent-foreground font-medium'
                      : 'text-muted-foreground hover:text-foreground',
                    collapsed && 'justify-center px-2'
                  )
                }
              >
                {({ isActive }) => (
                  <span
                    className={cn('flex min-w-0 items-center gap-3', collapsed && 'justify-center')}
                  >
                    <item.icon className={cn('h-4 w-4 shrink-0', isActive && 'text-primary')} />
                    {!collapsed && <span className="min-w-0 flex-1 truncate">{item.label}</span>}
                  </span>
                )}
              </NavLink>
            </TooltipTrigger>
            {collapsed && (
              <TooltipContent side="right" className="flex items-center gap-4">
                {item.label}
              </TooltipContent>
            )}
          </Tooltip>
        </TooltipProvider>

        {/* Pin Action Button - Absolute positioned to the right */}
        {!collapsed && isClient && (
          <button
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              togglePin(item.path);
            }}
            className={cn(
              'absolute right-2 p-1 rounded-sm opacity-0 group-hover:opacity-100 transition-opacity hover:bg-background/80 hover:text-foreground text-muted-foreground/50',
              // If it's the pinned section, we show the unpin icon (always visible if desired, or on hover)
              // If it's the normal section, we show pin icon if not already pinned
              isPinnedSection && 'text-muted-foreground hover:text-red-400',
              !isPinnedSection && isPinned && 'text-primary opacity-100' // Already pinned indicator in main list
            )}
            title={isPinned ? 'Unpin' : 'Pin to top'}
          >
            {isPinned ? <PinOff className="h-3.5 w-3.5" /> : <Pin className="h-3.5 w-3.5" />}
          </button>
        )}
      </div>
    );
  };

  return (
    <div
      className={cn(
        'group/sidebar flex flex-col border-r bg-card h-full transition-all duration-300 ease-in-out',
        collapsed ? 'w-[64px]' : EXPANDED_NAV_WIDTH_CLASS
      )}
    >
      <div className="flex h-14 items-center border-b px-3 justify-between">
        {!collapsed && <span className="font-semibold px-2">Asset Allocation</span>}
        <Button
          variant="ghost"
          size="icon"
          className={cn('h-8 w-8', collapsed && 'mx-auto')}
          onClick={() => setCollapsed(!collapsed)}
        >
          {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
        </Button>
      </div>

      <div className="flex-1 overflow-y-auto py-4 gap-6 flex flex-col">
        {/* HOTLIST SECTION */}
        {pinnedItems.length > 0 && (
          <div className="px-3">
            {!collapsed && (
              <h4 className="mb-2 px-2 text-xs font-semibold text-muted-foreground/50 tracking-wider flex items-center gap-2">
                <Pin className="h-3 w-3" /> PINNED
              </h4>
            )}
            <div className="space-y-1">{pinnedItems.map((item) => renderNavItem(item, true))}</div>
            {!collapsed && <div className="my-4 border-b border-border/40" />}
          </div>
        )}

        {visibleSections.map((section) => (
          <div key={section.title} className="px-3">
            {!collapsed && (
              <h4 className="mb-2 px-2 text-xs font-semibold text-muted-foreground/70 tracking-wider">
                {section.title}
              </h4>
            )}
            <div className="space-y-1">{section.items.map((item) => renderNavItem(item))}</div>
          </div>
        ))}
      </div>

      {!collapsed && (
        <div className="border-t px-4 py-3">
          <div
            role="status"
            aria-live="polite"
            aria-atomic="true"
            className="flex flex-col gap-1 text-left text-muted-foreground/80"
          >
            <span className="text-[10px] font-semibold uppercase tracking-[0.16em]">
              UPTIME CLOCK
            </span>
            <span className="font-mono text-xs text-foreground/75">
              {centralClock.time} {centralClock.tz}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
