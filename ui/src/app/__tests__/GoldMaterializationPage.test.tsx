import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

import { GoldMaterializationPage } from '@/app/components/pages/GoldMaterializationPage';
import { DataService } from '@/services/DataService';

const triggerJobMock = vi.fn();
const runtimeConfigCatalogResponse = {
  data: {
    items: [
      {
        key: 'GOLD_MARKET_BY_DATE_ENABLED',
        description: 'Enable materialization',
        example: 'true'
      },
      {
        key: 'GOLD_MARKET_BY_DATE_PATH',
        description: 'Target path',
        example: 'market_by_date'
      },
      {
        key: 'GOLD_BY_DATE_DOMAIN',
        description: 'Domain',
        example: 'market'
      },
      {
        key: 'GOLD_MARKET_BY_DATE_COLUMNS',
        description: 'Projection columns',
        example: 'close,volume'
      },
      {
        key: 'MATERIALIZE_YEAR_MONTH',
        description: 'Month partition',
        example: '2026-02'
      }
    ]
  },
  isLoading: false,
  isFetching: false,
  error: null
};
const runtimeConfigResponse = {
  data: {
    scope: 'global',
    items: []
  },
  isLoading: false,
  isFetching: false,
  error: null
};

vi.mock('sonner', () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
    message: vi.fn()
  }
}));

vi.mock('@/hooks/useJobTrigger', () => ({
  useJobTrigger: () => ({
    triggeringJob: null,
    triggerJob: triggerJobMock
  })
}));

vi.mock('@/hooks/useDataQueries', () => ({
  queryKeys: {
    runtimeConfig: (scope: string) => ['runtimeConfig', scope],
    runtimeConfigCatalog: () => ['runtimeConfigCatalog'],
    systemHealth: () => ['systemHealth']
  },
  useRuntimeConfigCatalogQuery: () => runtimeConfigCatalogResponse,
  useRuntimeConfigQuery: () => runtimeConfigResponse
}));

vi.mock('@/services/DataService', () => ({
  DataService: {
    getDomainColumns: vi.fn(),
    refreshDomainColumns: vi.fn(),
    setRuntimeConfig: vi.fn(),
    deleteRuntimeConfig: vi.fn()
  }
}));

const createQueryClient = () =>
  new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false }
    }
  });

describe('GoldMaterializationPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(DataService.setRuntimeConfig).mockResolvedValue({
      scope: 'global',
      key: 'GOLD_MARKET_BY_DATE_ENABLED',
      enabled: true,
      value: 'true',
      description: 'desc',
      updatedAt: null,
      updatedBy: null
    });
    vi.mocked(DataService.getDomainColumns).mockResolvedValue({
      layer: 'gold',
      domain: 'market',
      columns: ['date', 'symbol', 'close', 'volume', 'return_1d'],
      found: true,
      promptRetrieve: false,
      source: 'common-file',
      cachePath: 'metadata/domain-columns.json',
      updatedAt: null
    });
    vi.mocked(DataService.refreshDomainColumns).mockResolvedValue({
      layer: 'gold',
      domain: 'market',
      columns: ['date', 'symbol', 'close', 'volume', 'return_1d'],
      found: true,
      promptRetrieve: false,
      source: 'common-file',
      cachePath: 'metadata/domain-columns.json',
      updatedAt: null
    });
    vi.mocked(DataService.deleteRuntimeConfig).mockResolvedValue({
      scope: 'global',
      key: 'GOLD_MARKET_BY_DATE_ENABLED',
      deleted: true
    });
  });

  it('renders the control screen', () => {
    const queryClient = createQueryClient();

    render(
      <QueryClientProvider client={queryClient}>
        <GoldMaterializationPage />
      </QueryClientProvider>
    );

    expect(screen.getByRole('heading', { name: 'Gold Materialization' })).toBeInTheDocument();
    expect(screen.getByText('By-Date Controls')).toBeInTheDocument();
    expect(screen.getByText('Runtime Source Map')).toBeInTheDocument();
  });

  it('saves runtime-config overrides', async () => {
    const queryClient = createQueryClient();

    render(
      <QueryClientProvider client={queryClient}>
        <GoldMaterializationPage />
      </QueryClientProvider>
    );

    fireEvent.change(screen.getByLabelText('Target Path'), { target: { value: 'market_by_date_v2' } });
    fireEvent.change(screen.getByLabelText('Included Columns'), {
      target: { value: 'close,volume,return_1d' }
    });

    fireEvent.click(screen.getByRole('button', { name: /Save DB Overrides/i }));

    await waitFor(() => {
      expect(DataService.setRuntimeConfig).toHaveBeenCalledTimes(5);
    });

    expect(DataService.setRuntimeConfig).toHaveBeenCalledWith(
      expect.objectContaining({ key: 'GOLD_BY_DATE_DOMAIN', value: 'market' })
    );
    expect(DataService.setRuntimeConfig).toHaveBeenCalledWith(
      expect.objectContaining({ key: 'GOLD_MARKET_BY_DATE_PATH', value: 'market_by_date_v2' })
    );
    expect(DataService.setRuntimeConfig).toHaveBeenCalledWith(
      expect.objectContaining({ key: 'GOLD_MARKET_BY_DATE_COLUMNS', value: 'close,volume,return_1d' })
    );
  });

  it('triggers the gold job', async () => {
    const queryClient = createQueryClient();

    render(
      <QueryClientProvider client={queryClient}>
        <GoldMaterializationPage />
      </QueryClientProvider>
    );

    fireEvent.click(screen.getByRole('button', { name: /Run Gold Job/i }));

    await waitFor(() => {
      expect(triggerJobMock).toHaveBeenCalledWith('gold-market-job', ['systemHealth']);
    });
  });
});
