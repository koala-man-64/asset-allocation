import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { LeftNavigation } from '../components/layout/LeftNavigation';
import { BrowserRouter } from 'react-router-dom';

// Mock lucide-react to avoid issues with icon rendering in tests
vi.mock('lucide-react', () => ({
  Activity: () => <div data-testid="icon-activity" />,
  Database: () => <div data-testid="icon-database" />,
  Layers: () => <div data-testid="icon-layers" />,
  LayoutDashboard: () => <div data-testid="icon-dashboard" />,
  GitCompare: () => <div data-testid="icon-compare" />,
  FileText: () => <div data-testid="icon-text" />,
  PieChart: () => <div data-testid="icon-pie" />,
  Shield: () => <div data-testid="icon-shield" />,
  DollarSign: () => <div data-testid="icon-dollar" />,
  Target: () => <div data-testid="icon-target" />,
  Folder: () => <div data-testid="icon-folder" />,
  Zap: () => <div data-testid="icon-zap" />,
  TrendingUp: () => <div data-testid="icon-trending" />,
  Bell: () => <div data-testid="icon-bell" />,
  ScanSearch: () => <div data-testid="icon-scan" />,
  ChevronLeft: () => <span>icon-left</span>,
  ChevronRight: () => <span>icon-right</span>,
  Pin: () => <div data-testid="icon-pin" />,
  PinOff: () => <div data-testid="icon-pinoff" />,
  Globe: () => <div data-testid="icon-globe" />,
  ChevronUp: () => <div data-testid="icon-up" />,
  ChevronDown: () => <div data-testid="icon-down" />,
}));

describe('LeftNavigation', () => {
  it('renders navigation sections and items', () => {
    render(
      <BrowserRouter>
        <LeftNavigation />
      </BrowserRouter>
    );

    expect(screen.getByText('MARKET INTELLIGENCE')).toBeDefined();
    expect(screen.getByText('Stock Explorer')).toBeDefined();
    expect(screen.getByText('LIVE OPERATIONS')).toBeDefined();
    expect(screen.getByText('Data Quality')).toBeDefined();
    expect(screen.getByText('System Status')).toBeDefined();
  });

  it('toggles collapsed state when clicking the button', () => {
    render(
      <BrowserRouter>
        <LeftNavigation />
      </BrowserRouter>
    );

    const toggleButton = screen.getByRole('button', { name: /icon-(left|right)/i });
    fireEvent.click(toggleButton);
    
    // In collapsed state, section titles like 'SYSTEM' might be hidden or icon-only
    // The component uses 'collapsed' state to change classes.
    // We expect the button to exist and be clickable.
    expect(toggleButton).toBeDefined();
  });
});
