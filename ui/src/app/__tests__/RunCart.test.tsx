import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { RunCart } from '../components/layout/RunCart';
import { useApp } from '@/contexts/AppContext';
import { useRunList, useRunSummaries } from '@/services/backtestHooks';

// Mock context and hooks
vi.mock('@/contexts/AppContext', () => ({
  useApp: vi.fn(),
}));

vi.mock('@/services/backtestHooks', () => ({
  useRunList: vi.fn(),
  useRunSummaries: vi.fn(),
}));

// Mock icons
vi.mock('lucide-react', () => ({
  X: () => <div data-testid="icon-x" />,
  GitCompare: () => <div data-testid="icon-compare" />,
  Folder: () => <div data-testid="icon-folder" />,
}));

// Mock UI components that might be complex
vi.mock('@/app/components/ui/sheet', () => ({
  Sheet: ({ children, open }: any) => open ? <div>{children}</div> : null,
  SheetContent: ({ children }: any) => <div>{children}</div>,
  SheetHeader: ({ children }: any) => <div>{children}</div>,
  SheetTitle: ({ children }: any) => <div>{children}</div>,
  SheetDescription: ({ children }: any) => <div>{children}</div>,
}));

describe('RunCart', () => {
  const mockOnCompare = vi.fn();
  const mockOnPortfolioBuilder = vi.fn();
  const mockRemoveFromCart = vi.fn();
  const mockClearCart = vi.fn();
  const mockSetCartOpen = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    (useRunList as any).mockReturnValue({ runs: [] });
    (useRunSummaries as any).mockReturnValue({ summaries: [] });
  });

  it('renders empty state when no runs are selected', () => {
    (useApp as any).mockReturnValue({
      selectedRuns: new Map(),
      removeFromCart: mockRemoveFromCart,
      clearCart: mockClearCart,
      cartOpen: true,
      setCartOpen: mockSetCartOpen,
    });

    render(<RunCart onCompare={mockOnCompare} onPortfolioBuilder={mockOnPortfolioBuilder} />);
    expect(screen.getByText('No runs selected')).toBeDefined();
  });

  it('renders selected runs and enables buttons when 2+ runs are selected', () => {
    const selectedRuns = new Map([['run1', 'Strategy A'], ['run2', 'Strategy B']]);
    (useApp as any).mockReturnValue({
      selectedRuns,
      removeFromCart: mockRemoveFromCart,
      clearCart: mockClearCart,
      cartOpen: true,
      setCartOpen: mockSetCartOpen,
    });

    (useRunSummaries as any).mockReturnValue({
      summaries: [
        { run_id: 'run1', sharpe: 1.5, annual_return: 0.2 },
        { run_id: 'run2', sharpe: 1.2, annual_return: 0.15 },
      ]
    });

    render(<RunCart onCompare={mockOnCompare} onPortfolioBuilder={mockOnPortfolioBuilder} />);
    
    expect(screen.getByText('Strategy A')).toBeDefined();
    expect(screen.getByText('Strategy B')).toBeDefined();
    
    const compareButton = screen.getByText(/Compare 2 Runs/i);
    expect(compareButton).not.toHaveProperty('disabled', true);
    
    fireEvent.click(compareButton);
    expect(mockOnCompare).toHaveBeenCalled();
  });
});
