/**
 * Database Connectivity API Client
 * Phase 1B: BQ-VZ-DB-CONNECT
 */

import { getApiUrl } from "@/lib/api";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface DatabaseConnection {
  id: string;
  name: string;
  db_type: "postgresql" | "mysql";
  host: string;
  port: number;
  database: string;
  username: string;
  ssl_mode: "disable" | "prefer" | "require";
  extra_options: string | null;
  status: "configured" | "connected" | "error";
  error_message: string | null;
  last_connected_at: string | null;
  last_sync_at: string | null;
  table_count: number | null;
  created_at: string;
  updated_at: string;
}

export interface ConnectionCreateRequest {
  name: string;
  db_type: "postgresql" | "mysql";
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  ssl_mode: "disable" | "prefer" | "require";
  extra_options?: string;
}

export interface ConnectionUpdateRequest {
  name?: string;
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  password?: string;
  ssl_mode?: "disable" | "prefer" | "require";
  extra_options?: string;
}

export interface TestConnectionResult {
  ok: boolean;
  latency_ms?: number;
  server_version?: string;
  error?: string;
}

export interface TableColumn {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
}

export interface TableInfo {
  name: string;
  schema: string;
  columns: TableColumn[];
  primary_key: string[];
  estimated_rows: number;
}

export interface SchemaResponse {
  tables: TableInfo[];
  partial: boolean;
  warning?: string;
}

export interface ExtractRequest {
  tables?: { table: string; schema?: string; row_limit?: number }[];
  custom_sql?: string;
  dataset_name?: string;
}

export interface ExtractResponse {
  status: string;
  dataset_ids: string[];
  message: string;
}

export interface DirectQueryRequest {
  sql: string;
  limit?: number;
}

export interface DirectQueryResponse {
  columns: string[];
  rows: (string | number | boolean | null)[][];
  row_count: number;
  truncated: boolean;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function getHeaders(): Record<string, string> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  const apiKey = typeof window !== "undefined" ? localStorage.getItem("vectoraiz_api_key") : null;
  if (apiKey) headers["X-API-Key"] = apiKey;
  return headers;
}

async function dbFetch<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const url = `${getApiUrl()}/api/v1/db${endpoint}`;
  const response = await fetch(url, {
    ...options,
    headers: { ...getHeaders(), ...(options.headers as Record<string, string>) },
  });

  if (response.status === 401) {
    localStorage.removeItem("vectoraiz_api_key");
  }

  // 204 No Content (DELETE)
  if (response.status === 204) {
    return undefined as T;
  }

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: "Unknown error" }));
    throw new Error(error.detail || `API error: ${response.status}`);
  }

  return response.json();
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

export const databaseApi = {
  // CRUD
  list: () => dbFetch<DatabaseConnection[]>("/connections"),

  get: (id: string) => dbFetch<DatabaseConnection>(`/connections/${id}`),

  create: (data: ConnectionCreateRequest) =>
    dbFetch<DatabaseConnection>("/connections", {
      method: "POST",
      body: JSON.stringify(data),
    }),

  update: (id: string, data: ConnectionUpdateRequest) =>
    dbFetch<DatabaseConnection>(`/connections/${id}`, {
      method: "PUT",
      body: JSON.stringify(data),
    }),

  delete: (id: string) =>
    dbFetch<void>(`/connections/${id}`, { method: "DELETE" }),

  // Test connectivity
  test: (id: string) =>
    dbFetch<TestConnectionResult>(`/connections/${id}/test`, { method: "POST" }),

  // Schema introspection
  schema: (id: string, schema?: string) => {
    const params = schema ? `?schema=${encodeURIComponent(schema)}` : "";
    return dbFetch<SchemaResponse>(`/connections/${id}/schema${params}`);
  },

  // Extract tables
  extract: (id: string, data: ExtractRequest) =>
    dbFetch<ExtractResponse>(`/connections/${id}/extract`, {
      method: "POST",
      body: JSON.stringify(data),
    }),

  // Direct query against connected database (read-only)
  query: (id: string, data: DirectQueryRequest) =>
    dbFetch<DirectQueryResponse>(`/connections/${id}/query`, {
      method: "POST",
      body: JSON.stringify(data),
    }),
};
