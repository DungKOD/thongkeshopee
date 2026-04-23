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

export interface MetadataResponse {
  ok: true;
  exists: boolean;
  fileId: string | null;
  sizeBytes: number | null;
  lastModified: number | null;
  fingerprint: string | null;
  /// R2 etag — client dùng làm `expectedEtag` trong upload kế tiếp (CAS).
  /// null nếu object không tồn tại.
  etag: string | null;
}

/// v8.1+ upload: body là raw zstd bytes, metadata trong HTTP headers
/// (X-Mtime-Ms, X-Fingerprint, X-Expected-Etag). Không còn JSON body.
///
/// Worker response shape sau upload thành công:
export interface UploadResponse {
  ok: true;
  fileId: string;
  sizeBytes: number;
  lastModified: number;
  fingerprint: string;
  /// Etag MỚI sau upload thành công — client lưu vào sync_state để
  /// upload lần sau attach làm expectedEtag.
  etag: string;
}

/// v8.1+ download: response body là raw zstd bytes, metadata trong response
/// headers (X-Size-Bytes, X-Last-Modified-Ms, ETag). Không còn JSON envelope.
/// Giữ interface này chỉ cho doc — không còn được deserialize vì không phải JSON.

export interface ErrorResponse {
  ok: false;
  code: number;
  error: string;
}
