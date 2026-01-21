import { useState } from 'react';
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
  id: string;
  label: string;
  icon: React.ElementType;
}

interface NavSection {
  title: string;
  items: NavItem[];
}

interface LeftNavigationProps {
  activePage: string;
  onNavigate: (page: string) => void;
  // Props from src matching (though src_updated is self-managed, we keep these for compatibility if needed, or remove them from App.tsx)
}

const navSections: NavSection[] = [
  {
    title: 'ANALYSIS',
    items: [
      { id: 'overview', label: 'Overview', icon: LayoutDashboard },
      { id: 'compare', label: 'Run Compare', icon: GitCompare },
      { id: 'deep-dive', label: 'Deep Dive', icon: FileText },
      { id: 'attribution', label: 'Attribution', icon: PieChart },
    ]
  },
  {
    title: 'RISK & PERFORMANCE',
    items: [
      { id: 'risk', label: 'Risk & Exposures', icon: Shield },
      { id: 'execution', label: 'Execution & Costs', icon: DollarSign },
      { id: 'robustness', label: 'Robustness', icon: Target },
    ]
  },
  {
    title: 'PORTFOLIO',
    items: [
      { id: 'portfolio', label: 'Portfolio Builder', icon: Folder },
    ]
  },
  {
    title: 'LIVE OPERATIONS',
    items: [
      { id: 'signals', label: 'Signal Monitor', icon: Zap },
      { id: 'live-trading', label: 'Live Trading', icon: TrendingUp },
      { id: 'alerts', label: 'Alert Management', icon: Bell },
    ]
  },
  {
    title: 'SYSTEM',
    items: [
      { id: 'data', label: 'Data & Lineage', icon: Database },
      { id: 'data-tiers', label: 'Data Tiers', icon: Layers },
      { id: 'system', label: 'System Status', icon: Activity },
    ]
  }
];

export function LeftNavigation({ activePage, onNavigate }: LeftNavigationProps) {
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
                    const isActive = activePage === item.id;

                    const buttonContent = (
                      <Button
                        key={item.id}
                        variant="ghost"
                        size="default"
                        className={cn(
                          "w-full justify-start text-sidebar-foreground hover:bg-sidebar-accent hover:text-sidebar-accent-foreground",
                          collapsed ? "px-3" : "px-4",
                          isActive && "bg-sidebar-accent text-sidebar-primary font-semibold border-l-4 border-sidebar-primary rounded-l-none"
                        )}
                        onClick={() => onNavigate(item.id)}
                      >
                        <Icon className={cn("h-5 w-5", !collapsed && "mr-3", isActive && "text-sidebar-primary")} />
                        {!collapsed && <span>{item.label}</span>}
                      </Button>
                    );

                    if (collapsed) {
                      return (
                        <Tooltip key={item.id}>
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