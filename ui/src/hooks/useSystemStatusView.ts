import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/hooks/useDataQueries';
import {
  mergeSystemHealthWithJobOverrides,
  useSystemHealthJobOverrides
} from '@/hooks/useSystemHealthJobOverrides';
import type { SystemStatusViewResponse } from '@/services/apiService';
import { DataService } from '@/services/DataService';

const SYSTEM_STATUS_VIEW_REFETCH_INTERVAL_MS = 10_000;
const SYSTEM_STATUS_VIEW_STORAGE_KEY = 'asset-allocation.systemStatusView';

function readStoredSystemStatusView(): SystemStatusViewResponse | undefined {
  if (typeof window === 'undefined') {
    return undefined;
  }

  try {
    const raw = window.sessionStorage.getItem(SYSTEM_STATUS_VIEW_STORAGE_KEY);
    if (!raw) {
      return undefined;
    }

    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      window.sessionStorage.removeItem(SYSTEM_STATUS_VIEW_STORAGE_KEY);
      return undefined;
    }

    return parsed as SystemStatusViewResponse;
  } catch {
    try {
      window.sessionStorage.removeItem(SYSTEM_STATUS_VIEW_STORAGE_KEY);
    } catch {
      // Ignore storage cleanup failures and continue without persisted view state.
    }
    return undefined;
  }
}

function writeStoredSystemStatusView(data?: SystemStatusViewResponse): void {
  if (typeof window === 'undefined') {
    return;
  }

  try {
    if (!data) {
      window.sessionStorage.removeItem(SYSTEM_STATUS_VIEW_STORAGE_KEY);
      return;
    }

    window.sessionStorage.setItem(SYSTEM_STATUS_VIEW_STORAGE_KEY, JSON.stringify(data));
  } catch {
    // Ignore storage failures and continue with the live query cache.
  }
}

function syncSystemStatusRelatedCaches(
  queryClient: ReturnType<typeof useQueryClient>,
  data: SystemStatusViewResponse
): void {
  queryClient.setQueryData(queryKeys.systemHealth(), data.systemHealth);
  queryClient.setQueryData(queryKeys.domainMetadataSnapshot('all', 'all'), data.metadataSnapshot);
  writeStoredSystemStatusView(data);
}

export interface UseSystemStatusViewQueryOptions {
  autoRefresh?: boolean;
}

export function useSystemStatusViewQuery(options: UseSystemStatusViewQueryOptions = {}) {
  const autoRefresh = options.autoRefresh ?? false;
  const queryClient = useQueryClient();
  const jobOverrides = useSystemHealthJobOverrides();
  const initialViewRef = useRef<SystemStatusViewResponse | undefined>(readStoredSystemStatusView());
  const forceRefreshPromiseRef = useRef<Promise<SystemStatusViewResponse> | null>(null);
  const [isForceRefreshing, setIsForceRefreshing] = useState(false);

  const query = useQuery<SystemStatusViewResponse>({
    queryKey: queryKeys.systemStatusView(),
    queryFn: async () => DataService.getSystemStatusView(),
    initialData: () =>
      queryClient.getQueryData<SystemStatusViewResponse>(queryKeys.systemStatusView()) ??
      initialViewRef.current,
    placeholderData: (previousData) => previousData ?? initialViewRef.current,
    retry: 3,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false
  });

  useEffect(() => {
    if (!query.data) {
      return;
    }
    syncSystemStatusRelatedCaches(queryClient, query.data);
  }, [query.data, queryClient]);

  const refresh = useCallback(async (): Promise<SystemStatusViewResponse> => {
    if (forceRefreshPromiseRef.current) {
      return forceRefreshPromiseRef.current;
    }

    setIsForceRefreshing(true);
    const request = DataService.getSystemStatusView({ refresh: true })
      .then((fresh) => {
        queryClient.setQueryData(queryKeys.systemStatusView(), fresh);
        syncSystemStatusRelatedCaches(queryClient, fresh);
        return fresh;
      })
      .finally(() => {
        forceRefreshPromiseRef.current = null;
        setIsForceRefreshing(false);
      });

    forceRefreshPromiseRef.current = request;
    return request;
  }, [queryClient]);

  useEffect(() => {
    if (!autoRefresh) {
      return undefined;
    }

    const handle = window.setInterval(() => {
      void refresh();
    }, SYSTEM_STATUS_VIEW_REFETCH_INTERVAL_MS);

    return () => {
      window.clearInterval(handle);
    };
  }, [autoRefresh, refresh]);

  const data = useMemo<SystemStatusViewResponse | undefined>(() => {
    if (!query.data) return query.data;
    return {
      ...query.data,
      systemHealth:
        mergeSystemHealthWithJobOverrides(query.data.systemHealth, jobOverrides.data) ??
        query.data.systemHealth
    };
  }, [jobOverrides.data, query.data]);

  return {
    ...query,
    data,
    isFetching: query.isFetching || isForceRefreshing,
    refresh
  };
}
