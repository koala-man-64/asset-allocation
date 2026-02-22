import { useState, useEffect, type ElementType } from 'react';
import { NavLink } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/hooks/useDataQueries';
import { DataService } from '@/services/DataService';
import { Button } from '@/app/components/ui/button';
import { cn } from '@/app/components/ui/utils';
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
  TrendingUp,
  Bell,
  BarChart3,
  ScanSearch,
  Pin,
  PinOff,
  Globe,
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
      { path: '/stock-detail', label: 'Live Stock View', icon: TrendingUp }
    ]
  },
  {
    title: 'LIVE OPERATIONS',
    items: [
      { path: '/data-explorer', label: 'Data Explorer', icon: Folder },
        { path: '/data-quality', label: 'Data Quality', icon: ScanSearch },
        { path: '/data-profiling', label: 'Data Profiling', icon: BarChart3 },
        { path: '/live-trading', label: 'Live Trading', icon: TrendingUp },
        { path: '/alerts', label: 'Alerts', icon: Bell },
      { path: '/system-status', label: 'System Status', icon: Activity },
      { path: '/debug-symbols', label: 'Debug Symbols', icon: Bug },
      { path: '/symbol-purge', label: 'Symbol Purge', icon: Filter },
      { path: '/runtime-config', label: 'Runtime Config', icon: SlidersHorizontal }
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

export function LeftNavigation() {
  const [collapsed, setCollapsed] = useState(false);
  const [pinnedPaths, setPinnedPaths] = useState<string[]>([]);
  const [isClient, setIsClient] = useState(false);
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
                    queryClient.prefetchQuery({
                      queryKey: queryKeys.systemHealth(),
                      queryFn: async () => {
                        const response = await DataService.getSystemHealthWithMeta();
                        return response.data;
                      },
                      staleTime: 30000
                    });
                    if (item.path === '/system-status') {
                      queryClient.prefetchQuery({
                        queryKey: queryKeys.domainMetadataSnapshot('all', 'all'),
                        queryFn: async () =>
                          DataService.getDomainMetadataSnapshot({
                            cacheOnly: true
                          }),
                        staleTime: 5 * 60 * 1000
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
        collapsed ? 'w-[64px]' : 'w-[240px]'
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

        {navSections.map((section) => (
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
        <div className="p-4 border-t text-xs text-muted-foreground text-center">v2.5.0-beta</div>
      )}
    </div>
  );
}
