import { request } from './apiService';

export interface QueryRequest {
  schema_name: string;
  table_name: string;
  limit?: number;
  offset?: number;
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
  }
};
