export interface Env {
  DB_BUCKET: R2Bucket;
  FIREBASE_PROJECT_ID: string;
  /// Fallback admin whitelist — dùng khi Firestore không reachable/rules deny.
  /// Firestore `users/{uid}.admin=true` là nguồn chính (single source of truth).
  ADMIN_UIDS: string;
}

export interface AuthContext {
  uid: string;
  email: string | null;
  idToken: string;
  isAdmin: boolean;
  premium: boolean;
  expiredAt: string | null;
  createdAt: string | null;
}

// =============================================================
// Sync v9 wire formats
// =============================================================

/**
 * Manifest schema — phải match Rust `sync_v9::types::Manifest` exact.
 * JSON thuần, không có binary.
 */
export interface V9Manifest {
  version: number;
  uid: string;
  latest_snapshot: V9ManifestSnapshot | null;
  deltas: V9ManifestDeltaEntry[];
  updated_at_ms: number;
}

export interface V9ManifestSnapshot {
  key: string;
  clock_ms: number;
  size_bytes: number;
}

export interface V9ManifestDeltaEntry {
  table: string;
  key: string;
  cursor_lo: string;
  cursor_hi: string;
  clock_ms: number;
  size_bytes: number;
  row_count: number;
}

export interface ManifestGetResponse {
  ok: true;
  manifest: V9Manifest | null;
  etag: string | null;
}

export interface ManifestPutRequest {
  manifest: V9Manifest;
  expectedEtag: string | null;
}

export interface ManifestPutResponse {
  ok: true;
  etag: string;
}

export interface DeltaUploadResponse {
  ok: true;
  etag: string;
  sizeBytes: number;
}

export interface ErrorResponse {
  ok: false;
  code: number;
  error: string;
}
