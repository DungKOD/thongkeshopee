import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { useAuth } from "./AuthContext";
import {
  listShopeeAccounts,
  type ShopeeAccount,
} from "../lib/accounts";

/// Tên reserved cho account "Mặc định" — catch-all cho sub_id chưa gán
/// account nào. Sau v13 migration `id` là content_id hash (không còn = 1),
/// nên check bằng NAME thay vì ID const.
export const DEFAULT_ACCOUNT_NAME = "Mặc định";

/// Check account có phải bucket "Mặc định" không.
export function isDefaultAccount(a: { name: string }): boolean {
  return a.name === DEFAULT_ACCOUNT_NAME;
}

/// Filter UI — "all" = tất cả, "account" = 1 TK cụ thể.
/// Account "Mặc định" là catch-all bucket cho sub_id chưa gán explicit
/// account (Shopee/manual gán FK Mặc định + FB không match account nào).
export type AccountFilter =
  | { kind: "all" }
  | { kind: "account"; id: number };

interface AccountContextValue {
  /// List account từ DB. Null = đang load lần đầu.
  accounts: ShopeeAccount[] | null;
  /// ID động của account "Mặc định" — lookup từ list theo name. Null nếu
  /// list chưa load hoặc không có account nào tên "Mặc định".
  defaultAccountId: number | null;
  /// Active filter cho UI (Overview/DayBlock query sẽ filter theo).
  filter: AccountFilter;
  setFilter: (f: AccountFilter) => void;
  /// Active account cho import/manual entry (phải là account thật, không bucket ảo).
  /// Default = account đầu tiên non-Mặc định trong list.
  activeAccountId: number | null;
  setActiveAccountId: (id: number) => void;
  /// Re-fetch list từ DB (sau khi tạo/rename/delete).
  refresh: () => Promise<void>;
}

const AccountContext = createContext<AccountContextValue | null>(null);

interface AccountProviderProps {
  children: ReactNode;
}

export function AccountProvider({ children }: AccountProviderProps) {
  const { user } = useAuth();
  const uid = user?.uid ?? null;
  const [accounts, setAccounts] = useState<ShopeeAccount[] | null>(null);
  const [filter, setFilter] = useState<AccountFilter>({ kind: "all" });
  const [activeAccountId, setActiveAccountId] = useState<number | null>(null);

  const refresh = useCallback(async () => {
    try {
      const list = await listShopeeAccounts();
      setAccounts(list);
      // Ưu tiên non-Mặc định cho activeAccountId — Mặc định là bucket
      // orphan, user không chủ động import/manual vào đó. Check theo NAME
      // (id không còn stable sau v13 content_id migration).
      const defaultId = list.find(isDefaultAccount)?.id ?? null;
      const nonDefault = list.filter((a) => !isDefaultAccount(a));
      setActiveAccountId((prev) => {
        if (prev === null || prev === defaultId) {
          return nonDefault.length > 0 ? nonDefault[0].id : null;
        }
        if (!list.find((a) => a.id === prev)) {
          return nonDefault.length > 0 ? nonDefault[0].id : null;
        }
        return prev;
      });
    } catch (e) {
      console.error("[accounts] refresh failed:", e);
    }
  }, []);

  const defaultAccountId = accounts?.find(isDefaultAccount)?.id ?? null;

  // CRITICAL phân quyền: user UID đổi → CLEAR state ngay (không giữ list
  // account + filter của user cũ). KHÔNG tự refresh ở đây vì race với
  // `switch_db_to_user`: AccountContext effect fire NGAY khi authUid đổi,
  // nhưng DbState còn trỏ vào user cũ cho đến khi switch_db_to_user xong →
  // refresh ở đây đọc DB CŨ, hiện list sai.
  //
  // Refresh thực sự do `useCloudSync.onRemoteApplied` trigger SAU khi switch
  // hoàn tất (xem App.tsx `onRemoteApplied` callback).
  useEffect(() => {
    setAccounts(null);
    setFilter({ kind: "all" });
    setActiveAccountId(null);
  }, [uid]);

  return (
    <AccountContext.Provider
      value={{
        accounts,
        defaultAccountId,
        filter,
        setFilter,
        activeAccountId,
        setActiveAccountId,
        refresh,
      }}
    >
      {children}
    </AccountContext.Provider>
  );
}

export function useAccounts(): AccountContextValue {
  const ctx = useContext(AccountContext);
  if (!ctx) throw new Error("useAccounts must be inside AccountProvider");
  return ctx;
}
