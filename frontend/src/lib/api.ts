// API Configuration and Client

// Get API URL from localStorage or environment variable or default.
// Default is empty string (same-origin relative URLs) so the frontend
// works when served from the backend on any host (Railway, Docker, etc.).
export function getApiUrl(): string {
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem('vectoraiz_api_url');
    if (stored) return stored;
  }
  // VITE_API_URL is injected at build time by Railway.
  // Fallback: if not set AND we're on dev.vectoraiz.com, use the known backend.
  const envUrl = import.meta.env.VITE_API_URL;
  if (envUrl) return envUrl;
  
  // Smart fallback for split-service deploys (frontend ≠ backend origin)
  if (typeof window !== 'undefined' && window.location.hostname === 'dev.vectoraiz.com') {
    return 'https://vectoraiz-backend-production.up.railway.app';
  }
  return ''; // same-origin (works when frontend is served from backend)
}

// Read stored API key for auth header injection
function getStoredApiKey(): string | null {
  if (typeof window !== 'undefined') {
    return localStorage.getItem('vectoraiz_api_key');
  }
  return null;
}

// Generic fetch wrapper with error handling
async function apiFetch<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${getApiUrl()}${endpoint}`;
  const apiKey = getStoredApiKey();

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string>),
  };

  if (apiKey) {
    headers['X-API-Key'] = apiKey;
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (response.status === 401) {
    // Clear invalid key — will trigger re-auth via AuthContext
    localStorage.removeItem('vectoraiz_api_key');
  }

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Unknown error' }));
    // Support both flat { detail: "..." } and structured { error: { safe_message: "..." } } formats
    const message = error.detail || error.error?.safe_message || error.error?.title || `API error: ${response.status}`;
    throw new Error(message);
  }

  return response.json();
}

// Dataset types from API
export interface ApiDataset {
  id: string;
  original_filename: string;
  file_type: string;
  status: 'uploaded' | 'extracting' | 'preview_ready' | 'indexing' | 'ready' | 'cancelled' | 'error' | 'uploading' | 'processing';
  error?: string;
  created_at: string;
  updated_at: string;
  metadata: {
    row_count?: number;
    column_count?: number;
    columns?: Array<{ name: string; type: string }>;
    size_bytes?: number;
    index_status?: { status: string; rows_indexed?: number };
    pii_scan?: { overall_risk: string; columns_with_pii: number };
  };
}

export interface DatasetListResponse {
  datasets: ApiDataset[];
  count: number;
}

export interface SearchResult {
  dataset_id: string;
  dataset_name: string;
  score: number;
  row_index: number;
  text_content: string;
  row_data: Record<string, unknown>;
}

export interface SearchResponse {
  query: string;
  results: SearchResult[];
  total: number;
  datasets_searched: number;
  duration_ms: number;
}

export interface SQLResponse {
  query: string;
  columns: string[];
  data: Record<string, unknown>[];
  row_count: number;
  duration_ms: number;
  truncated: boolean;
}

export interface SQLTable {
  table_name: string;
  dataset_id: string;
  row_count: number;
  column_count: number;
}

export interface SQLTablesResponse {
  tables: SQLTable[];
  count: number;
}

export interface TableSchemaColumn {
  name: string;
  type: string;
  nullable: boolean;
}

export interface TableSchemaResponse {
  dataset_id: string;
  table_name: string;
  columns: TableSchemaColumn[];
}

export interface DatasetSampleResponse {
  dataset_id: string;
  sample: Record<string, unknown>[];
  count: number;
}

export interface DatasetStatisticsResponse {
  dataset_id: string;
  statistics: {
    column: string;
    type: string;
    count: number;
    null_count: number;
    unique_count?: number;
    min?: number;
    max?: number;
    mean?: number;
    median?: number;
    std?: number;
    top_values?: { value: string; count: number }[];
  }[];
}

export interface DatasetProfileResponse {
  dataset_id: string;
  column_profiles: {
    column: string;
    type: string;
    stats: Record<string, unknown>;
  }[];
}

// BQ-VZ-DATA-READINESS: Combined readiness report
export interface DatasetReadinessResponse {
  dataset_id: string;
  schema_report: Record<string, unknown> | null;
  pii_risk: {
    overall_risk: string;
    privacy_score: number;
    columns_with_pii: number;
    columns_clean: number;
    column_results: {
      column: string;
      pii_detected: boolean;
      pii_types: string[];
      risk_level: string;
      max_confidence: number;
    }[];
  } | null;
  quality_scorecard: {
    dataset_id: string;
    completeness: { score: number; details: string[] };
    validity: { score: number; details: string[] };
    consistency: { score: number; details: string[] };
    uniqueness: { score: number; details: string[] };
    overall_score: number;
    grade: string;
  } | null;
  statistical_profile: {
    dataset_id: string;
    row_count: number;
    column_count: number;
    columns: {
      column_name: string;
      dtype: string;
      null_rate: number;
      hll_distinct_estimate: number;
      quantiles: Record<string, number> | null;
      frequent_items: { value: string; estimate: number }[] | null;
    }[];
  } | null;
}

export interface UploadResponse {
  dataset_id: string;
  status: string;
}

export interface DatasetStatusResponse {
  dataset_id: string;
  status: string;
  error?: string;
}

export interface HealthResponse {
  status: string;
  timestamp: string;
}

export interface ReadyResponse {
  status: string;
  checks: Record<string, { status: string; message?: string }>;
}

export interface PIIScanResponse {
  dataset_id: string;
  scan_status: string;
  overall_risk: string;
  columns_scanned: number;
  columns_with_pii: number;
  column_results: {
    column: string;
    pii_types: string[];
    risk_level: string;
    sample_matches?: string[];
  }[];
}

export interface PIIEntitiesResponse {
  entities: {
    type: string;
    description: string;
    risk_level: string;
  }[];
  count: number;
}

export interface VectorHealthResponse {
  status: string;
  collections: number;
  total_vectors: number;
}

export interface VectorCollectionsResponse {
  collections: {
    name: string;
    vectors_count: number;
    status: string;
  }[];
  count: number;
}

export interface SearchStatsResponse {
  total_datasets: number;
  total_vectors: number;
  datasets: {
    dataset_id: string;
    dataset_name: string;
    vectors_count: number;
    status: string;
  }[];
}

export interface SearchSuggestResponse {
  query: string;
  suggestions: {
    text: string;
    score: number;
  }[];
}

// BQ-109: Dataset preview types
export interface DatasetPreviewResponse {
  dataset_id: string;
  status: string;
  file: {
    original_filename: string;
    file_type: string;
    size_bytes: number;
    encoding?: string;
  } | null;
  preview: {
    text?: string;
    kind: string;
    row_count_estimate: number;
    column_count: number;
    schema: Array<{ name: string; type: string }>;
    sample_rows: Record<string, unknown>[];
  } | null;
  warnings?: string[];
  actions?: {
    confirm_url: string;
    cancel_url: string;
  };
  error_message?: string;
}

// BQ-108: Batch upload types
export interface BatchItemAccepted {
  client_file_index: number;
  original_filename: string;
  relative_path?: string;
  size_bytes: number;
  status: 'accepted';
  dataset_id: string;
  preview_url: string;
  status_url: string;
}

export interface BatchItemRejected {
  client_file_index: number;
  original_filename: string;
  status: 'rejected';
  error_code: string;
  error: string;
}

export type BatchItem = BatchItemAccepted | BatchItemRejected;

export interface BatchUploadResponse {
  batch_id: string;
  accepted: number;
  rejected: number;
  items: BatchItem[];
}

export interface BatchStatusItem {
  dataset_id: string;
  original_filename: string;
  status: string;
  size_bytes: number;
}

export interface BatchStatusResponse {
  batch_id: string;
  total: number;
  by_status: Record<string, number>;
  items: BatchStatusItem[];
}

// Dataset API
export class DuplicateFileError extends Error {
  existingDataset: { id: string; filename: string; status: string; created_at: string };
  constructor(detail: string, existing: { id: string; filename: string; status: string; created_at: string }) {
    super(detail);
    this.name = 'DuplicateFileError';
    this.existingDataset = existing;
  }
}

export class UploadAbortedError extends Error {
  constructor() {
    super('Upload cancelled');
    this.name = 'UploadAbortedError';
  }
}

export const datasetsApi = {
  list: () => apiFetch<DatasetListResponse>('/api/datasets/'),

  get: (id: string) => apiFetch<ApiDataset>(`/api/datasets/${id}`),
  
  upload: async (file: File, options?: { allowDuplicate?: boolean }): Promise<UploadResponse> => {
    const formData = new FormData();
    formData.append('file', file);

    const headers: Record<string, string> = {};
    const apiKey = getStoredApiKey();
    if (apiKey) {
      headers['X-API-Key'] = apiKey;
    }

    const params = options?.allowDuplicate ? '?allow_duplicate=true' : '';
    const response = await fetch(`${getApiUrl()}/api/datasets/upload${params}`, {
      method: 'POST',
      headers,
      body: formData,
    });

    if (response.status === 401) {
      localStorage.removeItem('vectoraiz_api_key');
    }

    if (response.status === 409) {
      const body = await response.json().catch(() => ({}));
      if (body.error === 'duplicate_filename') {
        throw new DuplicateFileError(body.detail, body.existing_dataset);
      }
    }

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Upload failed' }));
      throw new Error(error.detail);
    }

    return response.json();
  },
  
  uploadWithProgress: (
    file: File,
    options?: { allowDuplicate?: boolean; batchId?: string; onProgress?: (percent: number) => void }
  ): { promise: Promise<UploadResponse>; abort: () => void } => {
    const xhr = new XMLHttpRequest();
    const qp = new URLSearchParams();
    if (options?.allowDuplicate) qp.set('allow_duplicate', 'true');
    if (options?.batchId) qp.set('batch_id', options.batchId);
    const qs = qp.toString();
    xhr.open('POST', `${getApiUrl()}/api/datasets/upload${qs ? `?${qs}` : ''}`);

    const apiKey = getStoredApiKey();
    if (apiKey) {
      xhr.setRequestHeader('X-API-Key', apiKey);
    }

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && options?.onProgress) {
        // Cap at 95% — the last 5% is server-side processing
        const pct = Math.min(Math.round((e.loaded / e.total) * 100), 95);
        options.onProgress(pct);
      }
    };

    const promise = new Promise<UploadResponse>((resolve, reject) => {
      xhr.onabort = () => reject(new UploadAbortedError());

      xhr.onload = () => {
        if (xhr.status === 401) {
          localStorage.removeItem('vectoraiz_api_key');
        }
        let body: Record<string, unknown>;
        try {
          body = JSON.parse(xhr.responseText || '{}');
        } catch {
          reject(new Error('Upload failed — invalid server response'));
          return;
        }
        if (xhr.status === 409 && body.error === 'duplicate_filename') {
          reject(new DuplicateFileError(body.detail as string, body.existing_dataset as any));
          return;
        }
        if (xhr.status >= 200 && xhr.status < 300) {
          options?.onProgress?.(100);
          resolve(body as unknown as UploadResponse);
        } else {
          reject(new Error((body.detail as string) || 'Upload failed'));
        }
      };

      xhr.onerror = () => reject(new Error('Network error'));
      xhr.ontimeout = () => reject(new Error('Upload timed out'));
    });

    const formData = new FormData();
    formData.append('file', file);
    xhr.send(formData);

    return { promise, abort: () => xhr.abort() };
  },

  delete: (id: string) => apiFetch<{ message: string }>(`/api/datasets/${id}`, { method: 'DELETE' }),
  
  getSample: (id: string, limit = 10) => 
    apiFetch<DatasetSampleResponse>(
      `/api/datasets/${id}/sample?limit=${limit}`
    ),
  
  getStatistics: (id: string) =>
    apiFetch<DatasetStatisticsResponse>(`/api/datasets/${id}/statistics`),
  
  getProfile: (id: string) =>
    apiFetch<DatasetProfileResponse>(`/api/datasets/${id}/profile`),
  
  getFull: (id: string) =>
    apiFetch<ApiDataset>(`/api/datasets/${id}/full`),
  
  getStatus: (id: string) =>
    apiFetch<DatasetStatusResponse>(`/api/datasets/${id}/status`),

  getPreview: (id: string) =>
    apiFetch<DatasetPreviewResponse>(`/api/datasets/${id}/preview`),

  confirm: (id: string) =>
    apiFetch<{ dataset_id: string; status: string }>(`/api/datasets/${id}/confirm`, { method: 'POST' }),

  getReadiness: (id: string) =>
    apiFetch<DatasetReadinessResponse>(`/api/datasets/${id}/readiness`),

  // BQ-108: Batch upload
  batchUpload: async (files: File[], paths?: string[]): Promise<BatchUploadResponse> => {
    const formData = new FormData();
    for (const file of files) {
      formData.append('files', file);
    }
    if (paths) {
      formData.append('paths', JSON.stringify(paths));
    }

    const headers: Record<string, string> = {};
    const apiKey = getStoredApiKey();
    if (apiKey) {
      headers['X-API-Key'] = apiKey;
    }

    const response = await fetch(`${getApiUrl()}/api/datasets/batch`, {
      method: 'POST',
      headers,
      body: formData,
    });

    if (response.status === 401) {
      localStorage.removeItem('vectoraiz_api_key');
    }

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Batch upload failed' }));
      throw new Error(error.detail);
    }

    return response.json();
  },

  getBatchStatus: (batchId: string) =>
    apiFetch<BatchStatusResponse>(`/api/datasets/batch/${batchId}`),

  uploadBatchSummary: (batchId: string, accepted: number, rejected: number, failedFilenames: string[]) =>
    apiFetch<{ ok: boolean }>('/api/datasets/upload-summary', {
      method: 'POST',
      body: JSON.stringify({ batch_id: batchId, accepted, rejected, failed_filenames: failedFilenames }),
    }),
};

// Search API
export const searchApi = {
  search: (query: string, options?: { dataset_id?: string; limit?: number; min_score?: number }) => {
    const params = new URLSearchParams({ q: query });
    if (options?.dataset_id) params.append('dataset_id', options.dataset_id);
    if (options?.limit) params.append('limit', options.limit.toString());
    if (options?.min_score) params.append('min_score', options.min_score.toString());
    
    return apiFetch<SearchResponse>(`/api/search?${params}`);
  },
  
  searchDataset: (datasetId: string, query: string, limit = 10) =>
    apiFetch<SearchResponse>(`/api/search/dataset/${datasetId}?q=${encodeURIComponent(query)}&limit=${limit}`),
  
  suggest: (query: string) =>
    apiFetch<SearchSuggestResponse>(`/api/search/suggest?q=${encodeURIComponent(query)}`),
  
  stats: () =>
    apiFetch<SearchStatsResponse>('/api/search/stats'),
};

// SQL API
export const sqlApi = {
  query: (sql: string, options?: { dataset_id?: string; limit?: number; offset?: number }) =>
    apiFetch<SQLResponse>('/api/sql/query', {
      method: 'POST',
      body: JSON.stringify({
        query: sql,
        dataset_id: options?.dataset_id,
        limit: options?.limit || 1000,
        offset: options?.offset || 0,
      }),
    }),
  
  tables: () =>
    apiFetch<SQLTablesResponse>('/api/sql/tables'),
  
  tableSchema: (datasetId: string) =>
    apiFetch<TableSchemaResponse>(`/api/sql/tables/${datasetId}`),
  
  validate: (sql: string) =>
    apiFetch<{ query: string; valid: boolean; error?: string }>('/api/sql/validate', {
      method: 'POST',
      body: JSON.stringify({ query: sql }),
    }),
};

// PII API
export const piiApi = {
  scan: (datasetId: string) =>
    apiFetch<PIIScanResponse>(`/api/pii/scan/${datasetId}`, { method: 'POST' }),
  
  getScan: (datasetId: string) =>
    apiFetch<PIIScanResponse>(`/api/pii/scan/${datasetId}`),
  
  entities: () =>
    apiFetch<PIIEntitiesResponse>('/api/pii/entities'),
};

// System info types
export interface SystemInfo {
  mode: string;
  version: string;
  features: Record<string, boolean>;
  system: {
    cpu_cores: number;
    memory_gb: number;
    recommended_concurrent_uploads: number;
  };
}

// System API
export const systemApi = {
  info: () => apiFetch<SystemInfo>('/api/system/info'),
};

// Health API
export const healthApi = {
  check: () => apiFetch<HealthResponse>('/api/health'),
  ready: () => apiFetch<ReadyResponse>('/api/health/ready'),
};

// Vectors API
export const vectorsApi = {
  health: () => apiFetch<VectorHealthResponse>('/api/vectors/health'),
  collections: () => apiFetch<VectorCollectionsResponse>('/api/vectors/collections'),
};

// Notification types
export interface ApiNotification {
  id: string;
  type: 'info' | 'success' | 'warning' | 'error' | 'action_required';
  category: 'upload' | 'processing' | 'system' | 'diagnostic';
  title: string;
  message: string;
  metadata_json: string | null;
  read: boolean;
  batch_id: string | null;
  source: string;
  created_at: string;
}

export interface NotificationListResponse {
  notifications: ApiNotification[];
  count: number;
}

export interface UnreadCountResponse {
  count: number;
}

// Notifications API
export const notificationsApi = {
  list: (params?: { limit?: number; offset?: number; category?: string; unread_only?: boolean }) => {
    const searchParams = new URLSearchParams();
    if (params?.limit) searchParams.append('limit', params.limit.toString());
    if (params?.offset) searchParams.append('offset', params.offset.toString());
    if (params?.category) searchParams.append('category', params.category);
    if (params?.unread_only) searchParams.append('unread_only', 'true');
    const qs = searchParams.toString();
    return apiFetch<NotificationListResponse>(`/api/notifications${qs ? `?${qs}` : ''}`);
  },

  unreadCount: () =>
    apiFetch<UnreadCountResponse>('/api/notifications/unread-count'),

  markRead: (id: string) =>
    apiFetch<ApiNotification>(`/api/notifications/${id}/read`, { method: 'PATCH' }),

  markAllRead: () =>
    apiFetch<{ marked: number }>('/api/notifications/read-all', { method: 'POST' }),

  delete: (id: string) =>
    apiFetch<{ message: string }>(`/api/notifications/${id}`, { method: 'DELETE' }),
};

// Diagnostics API (Phase 4)
export interface DiagnosticTransmitResponse {
  success: boolean;
  transmission_id: string;
  timestamp: string;
  size_bytes: number;
}

export const diagnosticsApi = {
  transmit: () =>
    apiFetch<DiagnosticTransmitResponse>('/api/diagnostics/transmit', { method: 'POST' }),
};

// Auth types
export interface AuthUser {
  id: string;
  username: string;
  role: string;
}

export interface AuthSetupResponse {
  message: string;
  user: AuthUser;
  api_key: string;
}

export interface AuthLoginResponse {
  user_id: number;
  username: string;
  api_key: string;
}

export interface AuthMeResponse {
  user_id: number;
  username: string;
  role: string;
  is_active: boolean;
}

export interface AuthKeyInfo {
  key_id: string;
  label: string;
  scopes: string[];
  created_at: string;
  last_used_at: string | null;
  revoked: boolean;
}

export interface AuthKeyCreatedResponse {
  key_id: string;
  full_key: string;
  label: string;
  scopes: string[];
}

// Auth API
export const authApi = {
  setup: (username: string, password: string) =>
    apiFetch<AuthSetupResponse>('/api/auth/setup', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),

  login: (username: string, password: string) =>
    apiFetch<AuthLoginResponse>('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username, password }),
    }),

  me: () => apiFetch<AuthMeResponse>('/api/auth/me'),

  listKeys: () => apiFetch<AuthKeyInfo[]>('/api/auth/keys'),

  createKey: (label: string, scopes: string[] = ['all']) =>
    apiFetch<AuthKeyCreatedResponse>('/api/auth/keys', {
      method: 'POST',
      body: JSON.stringify({ label, scopes }),
    }),

  revokeKey: (keyId: string) =>
    apiFetch<{ message: string }>(`/api/auth/keys/${keyId}`, {
      method: 'DELETE',
    }),
};

// BQ-VZ-ARTIFACTS: Artifact types
export interface ApiArtifact {
  id: string;
  schema_version: number;
  filename: string;
  format: 'txt' | 'csv' | 'json' | 'md' | 'html';
  size_bytes: number;
  content_hash: string;
  created_at: string;
  source: string;
  source_ref: string | null;
  description: string | null;
  dataset_refs: string[];
  user_id: string;
  starred: boolean;
  expired: boolean;
}

export interface ArtifactListResponse {
  artifacts: ApiArtifact[];
  total: number;
}

// Artifacts API
export const artifactsApi = {
  list: (params?: { offset?: number; limit?: number; format_filter?: string }) => {
    const searchParams = new URLSearchParams();
    if (params?.offset) searchParams.append('offset', params.offset.toString());
    if (params?.limit) searchParams.append('limit', params.limit.toString());
    if (params?.format_filter) searchParams.append('format_filter', params.format_filter);
    const qs = searchParams.toString();
    return apiFetch<ArtifactListResponse>(`/api/artifacts${qs ? `?${qs}` : ''}`);
  },

  get: (id: string) => apiFetch<ApiArtifact>(`/api/artifacts/${id}`),

  delete: (id: string) =>
    apiFetch<{ status: string }>(`/api/artifacts/${id}`, { method: 'DELETE' }),

  star: (id: string, starred: boolean) =>
    apiFetch<ApiArtifact>(`/api/artifacts/${id}/star`, {
      method: 'PATCH',
      body: JSON.stringify({ starred }),
    }),

  downloadUrl: (id: string) => `${getApiUrl()}/api/artifacts/${id}/download`,
};

// BQ-VZ-LOCAL-IMPORT: Local Import types
export interface ImportBrowseEntry {
  name: string;
  type: "file" | "directory";
  size_bytes?: number;
  extension?: string;
}

export interface ImportBrowseResponse {
  path: string;
  entries: ImportBrowseEntry[];
  total: number;
  limit: number;
  offset: number;
}

export interface ImportScanResponse {
  files: { relative_path: string; size_bytes: number; extension: string }[];
  total_files: number;
  total_bytes: number;
  skipped: number;
  truncated: boolean;
}

export interface ImportStartResponse {
  job_id: string;
  total_files: number;
  total_bytes: number;
  status: string;
}

export interface ImportProgress {
  files_total: number;
  files_complete: number;
  files_copying: number;
  files_pending: number;
  bytes_copied: number;
  bytes_total: number;
  current_file?: string;
  current_file_pct?: number;
}

export interface ImportFileResult {
  file: string;
  status: string;
  dataset_id?: string;
  error?: string;
}

export interface ImportStatusResponse {
  job_id: string;
  status: string;
  progress: ImportProgress;
  results: ImportFileResult[];
}

// Local Import API
export const importApi = {
  browse: (path: string, limit = 500, offset = 0) =>
    apiFetch<ImportBrowseResponse>(
      `/api/datasets/import/browse?path=${encodeURIComponent(path)}&limit=${limit}&offset=${offset}`
    ),

  scan: (path: string, recursive = true, maxDepth = 5) =>
    apiFetch<ImportScanResponse>('/api/datasets/import/scan', {
      method: 'POST',
      body: JSON.stringify({ path, recursive, max_depth: maxDepth }),
    }),

  start: (path: string, files: string[]) =>
    apiFetch<ImportStartResponse>('/api/datasets/import/start', {
      method: 'POST',
      body: JSON.stringify({ path, files }),
    }),

  getStatus: (jobId: string) =>
    apiFetch<ImportStatusResponse>(`/api/datasets/import/${jobId}`),

  cancel: (jobId: string) =>
    apiFetch<{ status: string }>(`/api/datasets/import/${jobId}/cancel`, {
      method: 'POST',
    }),
};


