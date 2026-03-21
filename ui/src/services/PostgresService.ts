import { request } from './apiService';

export interface TableRequest {
  schema_name: string;
  table_name: string;
}

export interface PostgresColumnMetadata {
  name: string;
  data_type: string;
  description?: string | null;
  nullable: boolean;
  primary_key: boolean;
  editable: boolean;
  edit_reason?: string | null;
}

export interface PostgresTableMetadata extends TableRequest {
  primary_key: string[];
  can_edit: boolean;
  edit_reason?: string | null;
  columns: PostgresColumnMetadata[];
}

export interface QueryRequest {
  schema_name: string;
  table_name: string;
  limit?: number;
  offset?: number;
  filters?: QueryFilter[];
}

export type GoldColumnLookupStatus = 'draft' | 'reviewed' | 'approved';

export interface GoldColumnLookupRow {
  schema: string;
  table: string;
  column: string;
  data_type: string;
  description: string;
  calculation_type: string;
  calculation_notes?: string | null;
  calculation_expression?: string | null;
  calculation_dependencies: string[];
  source_job?: string | null;
  status: GoldColumnLookupStatus;
  updated_at?: string | null;
}

export interface GoldColumnLookupResponse {
  rows: GoldColumnLookupRow[];
  limit: number;
  offset: number;
  has_more: boolean;
}

export interface GoldColumnLookupRequest {
  table?: string;
  q?: string;
  status?: GoldColumnLookupStatus;
  limit?: number;
  offset?: number;
}

export type QueryFilterOperator =
  | 'eq'
  | 'neq'
  | 'contains'
  | 'starts_with'
  | 'ends_with'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'is_null'
  | 'is_not_null';

export interface QueryFilter {
  column_name: string;
  operator: QueryFilterOperator;
  value?: string | number | boolean | null;
}

export interface PurgeTableResponse extends TableRequest {
  row_count: number;
}

export interface UpdateRowRequest extends TableRequest {
  match: Record<string, unknown>;
  values: Record<string, unknown>;
}

export interface UpdateRowResponse extends TableRequest {
  row_count: number;
  updated_columns: string[];
}

export const PostgresService = {
  async listSchemas(): Promise<string[]> {
    return request<string[]>('/system/postgres/schemas');
  },

  async listTables(schema: string): Promise<string[]> {
    return request<string[]>(`/system/postgres/schemas/${schema}/tables`);
  },

  async getTableMetadata(schema: string, table: string): Promise<PostgresTableMetadata> {
    return request<PostgresTableMetadata>(
      `/system/postgres/schemas/${schema}/tables/${table}/metadata`
    );
  },

  async listGoldLookupTables(): Promise<string[]> {
    return request<string[]>('/system/postgres/gold-column-lookup/tables');
  },

  async listGoldColumnLookup(req: GoldColumnLookupRequest = {}): Promise<GoldColumnLookupResponse> {
    const params = new URLSearchParams();
    if (req.table) params.set('table', req.table);
    if (req.q) params.set('q', req.q);
    if (req.status) params.set('status', req.status);
    if (typeof req.limit === 'number') params.set('limit', String(req.limit));
    if (typeof req.offset === 'number') params.set('offset', String(req.offset));

    const query = params.toString();
    const path = query
      ? `/system/postgres/gold-column-lookup?${query}`
      : '/system/postgres/gold-column-lookup';
    return request<GoldColumnLookupResponse>(path);
  },

  async queryTable(req: QueryRequest): Promise<Record<string, unknown>[]> {
    return request<Record<string, unknown>[]>('/system/postgres/query', {
      method: 'POST',
      body: JSON.stringify(req)
    });
  },

  async updateRow(req: UpdateRowRequest): Promise<UpdateRowResponse> {
    return request<UpdateRowResponse>('/system/postgres/update', {
      method: 'POST',
      body: JSON.stringify(req)
    });
  },

  async purgeTable(req: TableRequest): Promise<PurgeTableResponse> {
    return request<PurgeTableResponse>('/system/postgres/purge', {
      method: 'POST',
      body: JSON.stringify(req)
    });
  }
};
