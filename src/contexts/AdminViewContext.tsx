import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { auth } from "../lib/firebase";
import {
  adminExitViewUserDb,
  adminViewUserDb,
  type AdminViewInfo,
} from "../lib/sync";

interface AdminViewContextValue {
  /// Info user đang được xem. null = chế độ bình thường (DB của admin).
  view: AdminViewInfo | null;
  /// True khi đang swap connection (download DB / reopen).
  busy: boolean;
  /// Enter view mode — download DB target + swap connection read-only.
  enter: (target: {
    uid: string;
    localPart: string;
    email: string | null;
  }) => Promise<void>;
  /// Exit — reopen DB gốc read-write.
  exit: () => Promise<void>;
}

const AdminViewContext = createContext<AdminViewContextValue | null>(null);

interface AdminViewProviderProps {
  children: ReactNode;
}

export function AdminViewProvider({ children }: AdminViewProviderProps) {
  const [view, setView] = useState<AdminViewInfo | null>(null);
  const [busy, setBusy] = useState(false);

  const enter = useCallback<AdminViewContextValue["enter"]>(async (target) => {
    const current = auth.currentUser;
    if (!current) throw new Error("Chưa đăng nhập");
    setBusy(true);
    try {
      const idToken = await current.getIdToken(false);
      const info = await adminViewUserDb(
        idToken,
        target.uid,
        target.localPart,
        target.email,
      );
      setView(info);
    } finally {
      setBusy(false);
    }
  }, []);

  const exit = useCallback(async () => {
    setBusy(true);
    try {
      await adminExitViewUserDb();
      setView(null);
    } finally {
      setBusy(false);
    }
  }, []);

  const value = useMemo<AdminViewContextValue>(
    () => ({ view, busy, enter, exit }),
    [view, busy, enter, exit],
  );

  return (
    <AdminViewContext.Provider value={value}>
      {children}
    </AdminViewContext.Provider>
  );
}

export function useAdminView(): AdminViewContextValue {
  const ctx = useContext(AdminViewContext);
  if (!ctx) throw new Error("useAdminView must be used within AdminViewProvider");
  return ctx;
}
