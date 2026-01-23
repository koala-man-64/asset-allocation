import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { Button } from '@/app/components/ui/button';
import { cn } from '@/app/components/ui/utils';
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/app/components/ui/tooltip';
import {
  LayoutDashboard,
  GitCompare,
  FileText,
  PieChart,
  Shield,
  DollarSign,
  Target,
  Folder,
  Database,
  ChevronLeft,
  ChevronRight,
  Activity,
  Zap,
  TrendingUp,
  Bell,
  Layers,
  ChevronDown,
  ChevronUp
} from 'lucide-react';

interface NavItem {
  path: string;
  label: string;
  icon: React.ElementType;
}

interface NavSection {
  title: string;
  items: NavItem[];
}


const navSections: NavSection[] = [
  {
    title: 'SYSTEM',
    items: [
      { path: '/system', label: 'System Status', icon: Activity },
      { path: '/data', label: 'Data & Lineage', icon: Database },
      { path: '/data-tiers', label: 'Data Tiers', icon: Layers },
    ]
  },
  {
    title: 'ANALYSIS',
    items: [
      { path: '/', label: 'Overview', icon: LayoutDashboard },
      { path: '/compare', label: 'Run Compare', icon: GitCompare },
      { path: '/deep-dive', label: 'Deep Dive', icon: FileText },
      { path: '/attribution', label: 'Attribution', icon: PieChart },
    ]
  },
  {
    title: 'RISK & PERFORMANCE',
    items: [
      { path: '/risk', label: 'Risk & Exposures', icon: Shield },
      { path: '/execution', label: 'Execution & Costs', icon: DollarSign },
      { path: '/robustness', label: 'Robustness', icon: Target },
    ]
  },
  {
    title: 'PORTFOLIO',
    items: [
      { path: '/portfolio', label: 'Portfolio Builder', icon: Folder },
    ]
  },
  {
    title: 'LIVE OPERATIONS',
    items: [
      { path: '/signals', label: 'Signal Monitor', icon: Zap },
      { path: '/live-trading', label: 'Live Trading', icon: TrendingUp },
      { path: '/alerts', label: 'Alert Management', icon: Bell },
    ]
  }
];

export function LeftNavigation() {
  const [collapsed, setCollapsed] = useState(false);
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    'ANALYSIS': true,
    'RISK & PERFORMANCE': true,
    'PORTFOLIO': true,
    'LIVE OPERATIONS': true,
    'SYSTEM': true
  });

  const toggleSection = (sectionTitle: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [sectionTitle]: !prev[sectionTitle]
    }));
  };

  return (
    <TooltipProvider>
      <div className={cn(
        "border-r transition-all duration-300 flex flex-col bg-sidebar text-sidebar-foreground border-sidebar-border",
        collapsed ? "w-16" : "w-64"
      )}>


        <div className="flex-1 py-4 overflow-y-auto">
          {navSections.map((section, sectionIndex) => {
            const isExpanded = expandedSections[section.title];

            return (
              <div key={section.title} className={cn(sectionIndex > 0 && "mt-6")}>
                {!collapsed && (
                  <button
                    onClick={() => toggleSection(section.title)}
                    className="w-full px-4 py-2 text-xs font-bold text-sidebar-foreground/60 tracking-wider hover:text-sidebar-foreground/80 transition-colors flex items-center justify-between group"
                  >
                    <span>{section.title}</span>
                    {isExpanded ? (
                      <ChevronDown className="h-3 w-3 opacity-60 group-hover:opacity-100 transition-opacity" />
                    ) : (
                      <ChevronUp className="h-3 w-3 opacity-60 group-hover:opacity-100 transition-opacity" />
                    )}
                  </button>
                )}
                {collapsed && sectionIndex > 0 && (
                  <div className="mx-3 my-2 border-t border-sidebar-border" />
                )}
                <div
                  className={cn(
                    "space-y-1 overflow-hidden transition-all duration-300",
                    !collapsed && !isExpanded && "max-h-0 opacity-0",
                    !collapsed && isExpanded && "max-h-[500px] opacity-100",
                    collapsed && "max-h-[500px] opacity-100"
                  )}
                >
                  {section.items.map(item => {
                    const Icon = item.icon;

                    const buttonContent = (
                      <NavLink
                        key={item.path}
                        to={item.path}
                        className={({ isActive }) => cn(
                          "w-full flex items-center justify-start text-sm py-2 text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground transition-colors",
                          collapsed ? "px-3 justify-center" : "px-4",
                          isActive && "bg-sidebar-accent text-sidebar-primary font-semibold border-l-4 border-sidebar-primary -ml-[4px] pl-[calc(1rem-4px)]"
                          // Note: removed rounded-l-none from original as we aren't using Button component here directly to avoid nesting issues or we can wrap Button with asChild
                        )}
                        title={collapsed ? item.label : undefined}
                      >
                        {({ isActive }) => (
                          <>
                            <Icon className={cn("h-5 w-5", !collapsed && "mr-3", isActive && "text-sidebar-primary")} />
                            {!collapsed && <span>{item.label}</span>}
                          </>
                        )}
                      </NavLink>
                    );

                    if (collapsed) {
                      return (
                        <Tooltip key={item.path}>
                          <TooltipTrigger asChild>
                            {buttonContent}
                          </TooltipTrigger>
                          <TooltipContent side="right">
                            {item.label}
                          </TooltipContent>
                        </Tooltip>
                      );
                    }

                    return buttonContent;
                  })}
                </div>
              </div>
            );
          })}
        </div>

        <div className="p-3 border-t border-sidebar-border">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="sm"
                className="w-full text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground"
                onClick={() => setCollapsed(!collapsed)}
              >
                {collapsed ? <ChevronRight className="h-5 w-5" /> : <ChevronLeft className="h-5 w-5" />}
              </Button>
            </TooltipTrigger>
            <TooltipContent side="right">
              {collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            </TooltipContent>
          </Tooltip>
        </div>
      </div>
    </TooltipProvider>
  );
}
