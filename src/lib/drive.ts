import { invoke } from "./tauri";

export interface DriveCheckResult {
  existed: boolean;
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
}

export interface DriveMetadataResult {
  exists: boolean;
  file_id?: string;
  size_bytes?: number;
  last_modified_ms?: number;
}

export interface DriveUploadResult {
  file_id: string;
  size_bytes: number;
  last_modified_ms: number;
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
