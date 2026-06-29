import { useCallback, useEffect, useState } from "react";
import {
  createWorkspace,
  deleteWorkspace,
  listWorkspaces,
  renameWorkspace,
  switchWorkspace,
  updateWorkspaceColor,
  type Workspace,
} from "../lib/workspaces";

export interface UseWorkspacesResult {
  workspaces: Workspace[];
  active: Workspace | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  create: (name: string, color: string) => Promise<Workspace>;
  rename: (id: string, name: string) => Promise<void>;
  setColor: (id: string, color: string) => Promise<void>;
  switchTo: (id: string) => Promise<void>;
  remove: (id: string) => Promise<void>;
}

export function useWorkspaces(): UseWorkspacesResult {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const list = await listWorkspaces();
      setWorkspaces(list);
      setError(null);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const create = useCallback(
    async (name: string, color: string) => {
      const ws = await createWorkspace(name, color);
      await refresh();
      return ws;
    },
    [refresh],
  );

  const rename = useCallback(
    async (id: string, name: string) => {
      await renameWorkspace(id, name);
      await refresh();
    },
    [refresh],
  );

  const setColor = useCallback(
    async (id: string, color: string) => {
      await updateWorkspaceColor(id, color);
      await refresh();
    },
    [refresh],
  );

  /// Switch hot-swap 4 DB connection + read pool ở backend (KHÔNG restart app).
  /// Caller phải `window.location.reload()` sau resolve để React re-mount với
  /// data từ workspace mới.
  const switchTo = useCallback(async (id: string) => {
    await switchWorkspace(id);
  }, []);

  const remove = useCallback(
    async (id: string) => {
      await deleteWorkspace(id);
      await refresh();
    },
    [refresh],
  );

  const active = workspaces.find((w) => w.isActive) ?? null;

  return {
    workspaces,
    active,
    loading,
    error,
    refresh,
    create,
    rename,
    setColor,
    switchTo,
    remove,
  };
}
