import { invoke } from "./tauri";

export interface ImportedFileInfo {
  id: number;
  filename: string;
  kind: string;
  importedAt: string;
  rowCount: number;
  dayDate: string | null;
  revertedAt: string | null;
  accountName: string | null;
  activeRows: number;
}

export interface RevertResult {
  fileId: number;
  filename: string;
  revertedAt: string;
  clicksDeleted: number;
  ordersDeleted: number;
  fbAdsDeleted: number;
  daysDeleted: number;
}

export function listImportedFiles(): Promise<ImportedFileInfo[]> {
  return invoke<ImportedFileInfo[]>("list_imported_files");
}

export function revertImport(fileId: number): Promise<RevertResult> {
  return invoke<RevertResult>("revert_import", { fileId });
}

/** Xóa 1 entry lịch sử đã hoàn tác khỏi danh sách. */
export function deleteImportHistoryEntry(fileId: number): Promise<void> {
  return invoke<void>("delete_import_history_entry", { fileId });
}

/** Xóa tất cả entries đã hoàn tác. Trả về số dòng đã xóa. */
export function deleteAllRevertedHistory(): Promise<number> {
  return invoke<number>("delete_all_reverted_history");
}
