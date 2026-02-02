import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import type { ReactNode } from 'react';
import { RunCart } from '../components/layout/RunCart';
import { useUIStore } from '@/stores/useUIStore';
import { useRunList, useRunSummaries } from '@/services/backtestHooks';

// Mock store and hooks
vi.mock('@/stores/useUIStore', () => ({
  useUIStore: vi.fn()
}));

vi.mock('@/services/backtestHooks', () => ({
  useRunList: vi.fn(),
  useRunSummaries: vi.fn()
}));

// Mock icons
vi.mock('lucide-react', () => ({
  X: () => <div data-testid="icon-x" />,
  GitCompare: () => <div data-testid="icon-compare" />,
  Folder: () => <div data-testid="icon-folder" />
}));

// Mock UI components that might be complex
type SheetProps = { children?: ReactNode; open?: boolean };
type SheetChildProps = { children?: ReactNode };

vi.mock('@/app/components/ui/sheet', () => ({
  Sheet: ({ children, open }: SheetProps) => (open ? <div>{children}</div> : null),
  SheetContent: ({ children }: SheetChildProps) => <div>{children}</div>,
  SheetHeader: ({ children }: SheetChildProps) => <div>{children}</div>,
  SheetTitle: ({ children }: SheetChildProps) => <div>{children}</div>,
  SheetDescription: ({ children }: SheetChildProps) => <div>{children}</div>
}));

describe('RunCart', () => {
  const mockedUseUIStore = vi.mocked(useUIStore);
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
    mockedUseRunSummaries.mockReturnValue({ summaries: {} } as unknown as ReturnType<
      typeof useRunSummaries
    >);
  });

  it('renders empty state when no runs are selected', () => {
    mockedUseUIStore.mockReturnValue({
      selectedRuns: [],
      removeFromCart: mockRemoveFromCart,
      clearCart: mockClearCart,
      cartOpen: true,
      setCartOpen: mockSetCartOpen
    } as unknown as ReturnType<typeof useUIStore>);

    render(<RunCart onCompare={mockOnCompare} onPortfolioBuilder={mockOnPortfolioBuilder} />);
    expect(screen.getByText('No runs selected')).toBeDefined();
  });

  it('renders selected runs and enables buttons when 2+ runs are selected', () => {
    const selectedRuns = ['run1', 'run2'];
    mockedUseUIStore.mockReturnValue({
      selectedRuns,
      removeFromCart: mockRemoveFromCart,
      clearCart: mockClearCart,
      cartOpen: true,
      setCartOpen: mockSetCartOpen
    } as unknown as ReturnType<typeof useUIStore>);

    mockedUseRunList.mockReturnValue({
      runs: [
        { run_id: 'run1', status: 'completed', submitted_at: '2024-01-01', run_name: 'Strategy A' },
        { run_id: 'run2', status: 'completed', submitted_at: '2024-01-01', run_name: 'Strategy B' }
      ]
    } as unknown as ReturnType<typeof useRunList>);

    mockedUseRunSummaries.mockReturnValue({
      summaries: {
        run1: { sharpe_ratio: 1.5, annualized_return: 0.2 },
        run2: { sharpe_ratio: 1.2, annualized_return: 0.15 }
      }
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
