import { invoke } from "./tauri";

/// Snapshot 1 workspace từ backend (key đã camelCase trong Rust DTO).
export interface Workspace {
  id: string;
  name: string;
  color: string;
  createdAt: string;
  lastOpenedAt: string | null;
  isActive: boolean;
}

export function listWorkspaces(): Promise<Workspace[]> {
  return invoke<Workspace[]>("list_workspaces");
}

export function getActiveWorkspace(): Promise<Workspace> {
  return invoke<Workspace>("get_active_workspace");
}

export function createWorkspace(
  name: string,
  color: string,
): Promise<Workspace> {
  return invoke<Workspace>("create_workspace", { name, color });
}

export function renameWorkspace(id: string, name: string): Promise<void> {
  return invoke<void>("rename_workspace", { id, name });
}

export function updateWorkspaceColor(
  id: string,
  color: string,
): Promise<void> {
  return invoke<void>("update_workspace_color", { id, color });
}

/// Backend hot-swap 4 DB connection + read pool sang workspace mới (KHÔNG
/// restart app). Caller phải gọi `window.location.reload()` sau resolve để
/// React re-mount với data workspace mới.
export function switchWorkspace(id: string): Promise<void> {
  return invoke<void>("switch_workspace", { id });
}

export function deleteWorkspace(id: string): Promise<void> {
  return invoke<void>("delete_workspace", { id });
}
