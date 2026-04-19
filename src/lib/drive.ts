import { invoke } from "./tauri";

export interface DriveCheckResult {
  existed: boolean;
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
  fingerprint: string | null;
}

export interface DriveMetadataResult {
  exists: boolean;
  file_id?: string;
  size_bytes?: number;
  last_modified_ms?: number;
  fingerprint?: string | null;
}

export interface DriveUploadResult {
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
  fingerprint: string;
}

export function machineFingerprint(): Promise<string> {
  return invoke<string>("machine_fingerprint");
}

export interface DriveDownloadResult {
  target_path: string;
  size_bytes: number;
  last_modified_ms: number;
}

function url(): string {
  const u = import.meta.env.VITE_APPS_SCRIPT_URL;
  if (!u) throw new Error("VITE_APPS_SCRIPT_URL chưa cấu hình trong .env.local");
  return u;
}

export function driveCheckOrCreate(idToken: string): Promise<DriveCheckResult> {
  return invoke<DriveCheckResult>("drive_check_or_create", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveMetadata(idToken: string): Promise<DriveMetadataResult> {
  return invoke<DriveMetadataResult>("drive_metadata", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveUploadDb(idToken: string): Promise<DriveUploadResult> {
  return invoke<DriveUploadResult>("drive_upload_db", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveDownloadDb(idToken: string): Promise<DriveDownloadResult> {
  return invoke<DriveDownloadResult>("drive_download_db", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function driveApplyPending(): Promise<boolean> {
  return invoke<boolean>("drive_apply_pending");
}

export interface UserListFileMeta {
  fileId: string;
  sizeBytes: number;
  lastModified: number;
}

export interface UserListEntry {
  uid: string;
  email: string | null;
  localPart: string | null;
  premium: boolean;
  admin: boolean;
  expiredAt: string | null;
  createdAt: string | null;
  file: UserListFileMeta | null;
}

export function driveListUsers(idToken: string): Promise<UserListEntry[]> {
  return invoke<UserListEntry[]>("drive_list_users", {
    appsScriptUrl: url(),
    idToken,
  });
}

export function adminDownloadUserDb(
  idToken: string,
  targetLocalPart: string,
): Promise<string> {
  return invoke<string>("admin_download_user_db", {
    appsScriptUrl: url(),
    idToken,
    targetLocalPart,
  });
}

export interface VideoDownloadLog {
  id: number;
  url: string;
  downloaded_at_ms: number;
  status: string;
}

export function listVideoDownloadsFromPath(
  dbPath: string,
  limit: number,
  offset: number,
): Promise<VideoDownloadLog[]> {
  return invoke<VideoDownloadLog[]>("list_video_downloads_from_path", {
    dbPath,
    limit,
    offset,
  });
}

export function listVideoDownloads(
  limit: number,
  offset: number,
): Promise<VideoDownloadLog[]> {
  return invoke<VideoDownloadLog[]>("list_video_downloads", {
    limit,
    offset,
  });
}
