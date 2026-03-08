import { request } from './apiService';

export interface TableRequest {
  schema_name: string;
  table_name: string;
}

export interface PostgresColumnMetadata {
  name: string;
  data_type: string;
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
    return request<PostgresTableMetadata>(`/system/postgres/schemas/${schema}/tables/${table}/metadata`);
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
