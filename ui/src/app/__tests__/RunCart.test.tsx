import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import type { ReactNode } from 'react';
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
type SheetProps = { children?: ReactNode; open?: boolean };
type SheetChildProps = { children?: ReactNode };

vi.mock('@/app/components/ui/sheet', () => ({
  Sheet: ({ children, open }: SheetProps) => open ? <div>{children}</div> : null,
  SheetContent: ({ children }: SheetChildProps) => <div>{children}</div>,
  SheetHeader: ({ children }: SheetChildProps) => <div>{children}</div>,
  SheetTitle: ({ children }: SheetChildProps) => <div>{children}</div>,
  SheetDescription: ({ children }: SheetChildProps) => <div>{children}</div>,
}));

describe('RunCart', () => {
  const mockedUseApp = vi.mocked(useApp);
  const mockedUseRunList = vi.mocked(useRunList);
  const mockedUseRunSummaries = vi.mocked(useRunSummaries);

  const mockOnCompare = vi.fn();
  const mockOnPortfolioBuilder = vi.fn();
  const mockRemoveFromCart = vi.fn();
  const mockClearCart = vi.fn();
  const mockSetCartOpen = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    mockedUseRunList.mockReturnValue({ runs: [] } as unknown as ReturnType<typeof useRunList>);
    mockedUseRunSummaries.mockReturnValue({ summaries: {} } as unknown as ReturnType<typeof useRunSummaries>);
  });

  it('renders empty state when no runs are selected', () => {
    mockedUseApp.mockReturnValue({
      selectedRuns: new Set<string>(),
      removeFromCart: mockRemoveFromCart,
      clearCart: mockClearCart,
      cartOpen: true,
      setCartOpen: mockSetCartOpen,
    } as unknown as ReturnType<typeof useApp>);

    render(<RunCart onCompare={mockOnCompare} onPortfolioBuilder={mockOnPortfolioBuilder} />);
    expect(screen.getByText('No runs selected')).toBeDefined();
  });

  it('renders selected runs and enables buttons when 2+ runs are selected', () => {
    const selectedRuns = new Set(['run1', 'run2']);
    mockedUseApp.mockReturnValue({
      selectedRuns,
      removeFromCart: mockRemoveFromCart,
      clearCart: mockClearCart,
      cartOpen: true,
      setCartOpen: mockSetCartOpen,
    } as unknown as ReturnType<typeof useApp>);

    mockedUseRunList.mockReturnValue({
      runs: [
        { run_id: 'run1', status: 'completed', submitted_at: '2024-01-01', run_name: 'Strategy A' },
        { run_id: 'run2', status: 'completed', submitted_at: '2024-01-01', run_name: 'Strategy B' },
      ],
    } as unknown as ReturnType<typeof useRunList>);

    mockedUseRunSummaries.mockReturnValue({
      summaries: {
        run1: { sharpe_ratio: 1.5, annualized_return: 0.2 },
        run2: { sharpe_ratio: 1.2, annualized_return: 0.15 },
      },
    } as unknown as ReturnType<typeof useRunSummaries>);

    render(<RunCart onCompare={mockOnCompare} onPortfolioBuilder={mockOnPortfolioBuilder} />);
    
    expect(screen.getByText('Strategy A')).toBeDefined();
    expect(screen.getByText('Strategy B')).toBeDefined();
    
    const compareButton = screen.getByText(/Compare 2 Runs/i);
    expect(compareButton).not.toHaveProperty('disabled', true);
    
    fireEvent.click(compareButton);
    expect(mockOnCompare).toHaveBeenCalled();
  });
});
