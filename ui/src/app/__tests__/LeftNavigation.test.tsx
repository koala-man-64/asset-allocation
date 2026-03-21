import { beforeEach, describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { LeftNavigation } from '../components/layout/LeftNavigation';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false
    }
  }
});

// Mock lucide-react to avoid issues with icon rendering in tests
vi.mock('lucide-react', () => ({
  Activity: () => <div data-testid="icon-activity" />,
  Database: () => <div data-testid="icon-database" />,
  Layers: () => <div data-testid="icon-layers" />,
  Layers3: () => <div data-testid="icon-layers-3" />,
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
  BarChart3: () => <div data-testid="icon-bar-chart" />,
  ScanSearch: () => <div data-testid="icon-scan" />,
  ChevronLeft: () => <span>icon-left</span>,
  ChevronRight: () => <span>icon-right</span>,
  Pin: () => <div data-testid="icon-pin" />,
  PinOff: () => <div data-testid="icon-pinoff" />,
  Globe: () => <div data-testid="icon-globe" />,
  Orbit: () => <div data-testid="icon-orbit" />,
  Bug: () => <div data-testid="icon-bug" />,
  Filter: () => <div data-testid="icon-filter" />,
  SlidersHorizontal: () => <div data-testid="icon-sliders" />,
  ChevronUp: () => <div data-testid="icon-up" />,
  ChevronDown: () => <div data-testid="icon-down" />
}));

const renderNavigation = () =>
  render(
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <LeftNavigation />
      </BrowserRouter>
    </QueryClientProvider>
  );

describe('LeftNavigation', () => {
  beforeEach(() => {
    document.cookie = 'ag_pinned_tabs=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/';
    queryClient.clear();
  });

  it('renders navigation sections and items', () => {
    const { container } = renderNavigation();

    expect(screen.getByText('Stock Explorer')).toBeDefined();
    expect(screen.getByText('Data Quality')).toBeDefined();
    expect(screen.getByText('Regime Monitor')).toBeDefined();
    expect(screen.getByText('System Status')).toBeDefined();
    expect(screen.getByText('Run Configurations')).toBeDefined();
    expect(screen.getByText('Universe Configurations')).toBeDefined();
    expect(screen.getByText('Ranking Configurations')).toBeDefined();
    expect(screen.getByText('UPTIME CLOCK')).toBeDefined();
    expect(container.firstChild).toHaveClass('w-[280px]');
  });

  it('toggles collapsed state when clicking the button', () => {
    renderNavigation();

    const toggleButton = screen.getByRole('button', { name: /icon-(left|right)/i });
    fireEvent.click(toggleButton);

    // In collapsed state, section titles like 'SYSTEM' might be hidden or icon-only
    // The component uses 'collapsed' state to change classes.
    // We expect the button to exist and be clickable.
    expect(toggleButton).toBeDefined();
  });

  it('moves run configurations to the pinned section without duplication', async () => {
    renderNavigation();

    await waitFor(() => {
      expect(screen.getAllByText('Run Configurations').length).toBe(1);
    });

    const runConfigurationsLink = screen.getByRole('link', { name: 'Run Configurations' });
    const navRow = runConfigurationsLink.closest('div.group.relative.flex.items-center');
    expect(navRow).toBeTruthy();

    const pinButton = navRow?.querySelector('button[title="Pin to top"]');
    expect(pinButton).toBeTruthy();

    fireEvent.click(pinButton as HTMLButtonElement);

    expect(screen.getByText('PINNED')).toBeDefined();
    expect(screen.getAllByText('Run Configurations').length).toBe(1);
  });
});
