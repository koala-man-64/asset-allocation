import { request } from './apiService';

export interface TableRequest {
  schema_name: string;
  table_name: string;
}

export interface QueryRequest {
  schema_name: string;
  table_name: string;
  limit?: number;
  offset?: number;
}

export interface PurgeTableResponse extends TableRequest {
  row_count: number;
}

export const PostgresService = {
  async listSchemas(): Promise<string[]> {
    return request<string[]>('/system/postgres/schemas');
  },

  async listTables(schema: string): Promise<string[]> {
    return request<string[]>(`/system/postgres/schemas/${schema}/tables`);
  },

  async queryTable(req: QueryRequest): Promise<Record<string, unknown>[]> {
    return request<Record<string, unknown>[]>('/system/postgres/query', {
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
