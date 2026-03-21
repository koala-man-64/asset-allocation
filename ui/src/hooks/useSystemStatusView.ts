import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';

import { queryKeys } from '@/hooks/useDataQueries';
import {
  mergeSystemHealthWithJobOverrides,
  useSystemHealthJobOverrides
} from '@/hooks/useSystemHealthJobOverrides';
import type { SystemStatusViewResponse } from '@/services/apiService';
import { DataService } from '@/services/DataService';

const SYSTEM_STATUS_VIEW_REFETCH_INTERVAL_MS = 10_000;

export interface UseSystemStatusViewQueryOptions {
  autoRefresh?: boolean;
}

export function useSystemStatusViewQuery(
  options: UseSystemStatusViewQueryOptions = {}
) {
  const autoRefresh = options.autoRefresh ?? false;
  const jobOverrides = useSystemHealthJobOverrides();
  const query = useQuery<SystemStatusViewResponse>({
    queryKey: queryKeys.systemStatusView(),
    queryFn: async () => DataService.getSystemStatusView({ refresh: true }),
    placeholderData: (previousData) => previousData,
    retry: 3,
    refetchInterval: autoRefresh ? SYSTEM_STATUS_VIEW_REFETCH_INTERVAL_MS : false
  });

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
    data
  };
}
