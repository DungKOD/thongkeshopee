export interface Env {
  DB_BUCKET: R2Bucket;
  FIREBASE_PROJECT_ID: string;
  ADMIN_UIDS: string;
}

export interface AuthContext {
  uid: string;
  email: string | null;
  isAdmin: boolean;
}

export interface MetadataResponse {
  ok: true;
  exists: boolean;
  fileId: string | null;
  sizeBytes: number | null;
  lastModified: number | null;
  fingerprint: string | null;
}

export interface UploadRequest {
  base64Data: string;
  mtimeMs: number;
  fingerprint: string;
}

export interface UploadResponse {
  ok: true;
  fileId: string;
  sizeBytes: number;
  lastModified: number;
  fingerprint: string;
}

export interface DownloadResponse {
  ok: true;
  base64Data: string;
  sizeBytes: number;
  lastModified: number;
}

export interface ErrorResponse {
  ok: false;
  code: number;
  error: string;
}
