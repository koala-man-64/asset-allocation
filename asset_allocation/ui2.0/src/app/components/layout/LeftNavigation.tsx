// Left navigation rail with collapsible sections

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
  ChevronRight
} from 'lucide-react';
import { useState } from 'react';
import { Button } from '@/app/components/ui/button';
import { cn } from '@/app/components/ui/utils';

interface NavItem {
  id: string;
  label: string;
  icon: any;
}

const navItems: NavItem[] = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'compare', label: 'Run Compare', icon: GitCompare },
  { id: 'deep-dive', label: 'Single Run Deep Dive', icon: FileText },
  { id: 'attribution', label: 'Attribution', icon: PieChart },
  { id: 'risk', label: 'Risk & Exposures', icon: Shield },
  { id: 'execution', label: 'Execution & Costs', icon: DollarSign },
  { id: 'robustness', label: 'Robustness', icon: Target },
  { id: 'portfolio', label: 'Portfolio Builder', icon: Folder },
  { id: 'data', label: 'Data & Lineage', icon: Database },
];

interface LeftNavigationProps {
  activePage: string;
  onNavigate: (page: string) => void;
}

export function LeftNavigation({ activePage, onNavigate }: LeftNavigationProps) {
  const [collapsed, setCollapsed] = useState(false);
  
  return (
    <div className={cn(
      "border-r bg-background transition-all duration-300 flex flex-col",
      collapsed ? "w-16" : "w-56"
    )}>
      <div className="flex-1 py-4">
        {navItems.map(item => {
          const Icon = item.icon;
          const isActive = activePage === item.id;
          
          return (
            <Button
              key={item.id}
              variant={isActive ? 'secondary' : 'ghost'}
              className={cn(
                "w-full justify-start mb-0.5",
                collapsed ? "px-4" : "px-4",
                isActive && "bg-secondary"
              )}
              onClick={() => onNavigate(item.id)}
            >
              <Icon className={cn("h-4 w-4", !collapsed && "mr-3")} />
              {!collapsed && <span className="text-sm">{item.label}</span>}
            </Button>
          );
        })}
      </div>
      
      <div className="p-2 border-t">
        <Button
          variant="ghost"
          size="sm"
          className="w-full"
          onClick={() => setCollapsed(!collapsed)}
        >
          {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
        </Button>
      </div>
    </div>
  );
}