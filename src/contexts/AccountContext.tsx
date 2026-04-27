import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
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
///
/// `id` là string — content_id hash có thể > 2^53 (không an toàn JS Number).
export type AccountFilter =
  | { kind: "all" }
  | { kind: "account"; id: string };

/// localStorage key prefix — per-uid để multi-tenant không leak filter giữa
/// các user dùng chung máy (memory: multi-tenant DB isolation).
const FILTER_STORAGE_PREFIX = "thongkeshopee.accountfilter.v1.";

function loadAccountFilter(uid: string | null): AccountFilter {
  if (!uid) return { kind: "all" };
  try {
    const raw = localStorage.getItem(FILTER_STORAGE_PREFIX + uid);
    if (!raw) return { kind: "all" };
    const parsed = JSON.parse(raw);
    if (parsed?.kind === "all") return { kind: "all" };
    if (parsed?.kind === "account" && typeof parsed.id === "string") {
      return { kind: "account", id: parsed.id };
    }
    return { kind: "all" };
  } catch {
    return { kind: "all" };
  }
}

function persistAccountFilter(uid: string | null, f: AccountFilter): void {
  if (!uid) return;
  try {
    localStorage.setItem(FILTER_STORAGE_PREFIX + uid, JSON.stringify(f));
  } catch {
    // ignore quota / privacy-mode errors
  }
}

interface AccountContextValue {
  /// List account từ DB. Null = đang load lần đầu.
  accounts: ShopeeAccount[] | null;
  /// ID động của account "Mặc định" — lookup từ list theo name. Null nếu
  /// list chưa load hoặc không có account nào tên "Mặc định".
  defaultAccountId: string | null;
  /// Active filter cho UI (Overview/DayBlock query sẽ filter theo).
  filter: AccountFilter;
  setFilter: (f: AccountFilter) => void;
  /// Active account cho import/manual entry (phải là account thật, không bucket ảo).
  /// Default = account đầu tiên non-Mặc định trong list.
  activeAccountId: string | null;
  setActiveAccountId: (id: string) => void;
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
  // Init từ localStorage — uid lúc mount có thể null (chưa login), khi đó
  // load trả về "all". Effect [uid] phía dưới sẽ re-load khi uid xuất hiện.
  const [filter, setFilterState] = useState<AccountFilter>(() =>
    loadAccountFilter(uid),
  );
  const [activeAccountId, setActiveAccountId] = useState<string | null>(null);

  /// Mirror uid vào ref — refresh + setFilter callback đọc qua ref để tránh
  /// stale closure: App.tsx capture refreshAccounts ở mount-time với uid=null
  /// (auth chưa load), khi callback execute async sau đó, uid thực sự đã có
  /// nhưng closure giữ null → persistAccountFilter no-op → state đổi nhưng
  /// storage không update.
  const uidRef = useRef(uid);
  useEffect(() => {
    uidRef.current = uid;
  }, [uid]);

  /// Wrap setFilter — mọi thay đổi filter đều persist ngay theo uid hiện tại.
  /// User chuyển TK trong dropdown → reload app vẫn giữ TK đã chọn.
  const setFilter = useCallback((f: AccountFilter) => {
    setFilterState(f);
    persistAccountFilter(uidRef.current, f);
  }, []);

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
      // Validate persisted filter — account đã bị xóa từ máy khác (sync pull)
      // hoặc user xóa thủ công thì filter trỏ id rỗng → query empty. Fallback
      // về "all" + persist update để lần sau không lặp lại.
      //
      // CRITICAL: chỉ validate khi `list` có ít nhất 1 account. List rỗng có
      // thể do DB chưa ready ở refresh đầu mount (sync chưa pull xong) — wipe
      // ở đây sẽ ăn mất filter user đã save. Khi sync pull xong, refresh sẽ
      // chạy lại với list thật và validate đúng. Edge case "user thật sự xóa
      // hết account" rất hiếm + harmless (filter Y vô nghĩa, query trả empty,
      // user pick "all" thủ công).
      if (list.length > 0) {
        setFilterState((prev) => {
          if (prev.kind !== "account") return prev;
          if (list.some((a) => a.id === prev.id)) return prev;
          const fallback: AccountFilter = { kind: "all" };
          persistAccountFilter(uidRef.current, fallback);
          return fallback;
        });
      }
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
  //
  // Filter: load từ localStorage cho uid mới (per-uid storage). Mỗi user có
  // filter riêng — switch user A→B sẽ load filter của B, không leak filter
  // của A. Logout (uid=null) reset về "all".
  useEffect(() => {
    setAccounts(null);
    setFilterState(loadAccountFilter(uid));
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
