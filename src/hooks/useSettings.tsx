import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { invoke } from "../lib/tauri";
import { useAuth } from "../contexts/AuthContext";

export interface ProfitFees {
  /** % thuế + phí sàn khấu trừ từ hoa hồng (vd 10.98). */
  taxAndPlatformRate: number;
  /** % dự phòng hoàn/hủy đơn (vd 9). */
  returnReserveRate: number;
}

export interface Settings {
  clickSources: Record<string, boolean>;
  profitFees: ProfitFees;
  /** Toggle auto push lên R2 sau mỗi mutation (import/save/delete). True =
   *  debounce 45s + COUNT_THRESHOLD/MAX_WAIT_MS triggers như cũ. False =
   *  status vẫn show "dirty" nhưng KHÔNG fire debounce — user phải bấm
   *  "Đồng bộ ngay" trong SyncBadge. Pull/RTDB notify/2h safety tick vẫn
   *  hoạt động bình thường. Default true (backward compat). */
  autoSyncEnabled: boolean;
}

const DEFAULT_PROFIT_FEES: ProfitFees = {
  taxAndPlatformRate: 10.98,
  returnReserveRate: 9,
};
const DEFAULT_SETTINGS: Settings = {
  clickSources: {},
  profitFees: DEFAULT_PROFIT_FEES,
  autoSyncEnabled: true,
};

// =========================================================
// Key namespace cho app_settings table
// =========================================================
// Convention dot-namespace để FE-Rust thống nhất. Value = JSON.stringify
// (string | number | boolean) → Rust trust raw, không validate type.
const KEY_PROFIT_FEE_TAX = "profit_fee.tax_and_platform_rate";
const KEY_PROFIT_FEE_RETURN = "profit_fee.return_reserve_rate";
/// Legacy DB key (v0.4.x trở về trước). Đã chuyển sang localStorage —
/// migration đọc 1 lần vào localStorage rồi ignore. Row DB giữ nguyên
/// (build cũ trên máy khác có thể còn ghi — vô hại, code mới ignore).
const KEY_AUTO_SYNC_LEGACY_DB = "auto_sync_enabled";
/// localStorage key cho auto-sync toggle. Per-machine UX preference,
/// KHÔNG nằm trong sync pipeline → toggle không bao giờ trigger "Chờ
/// đồng bộ". Plain key (không prefix `thongkeshopee.` / `smartcalc:`)
/// để survive `wipeUserLocalStorage` khi đổi user — auto-sync là setting
/// của máy, không phải của user.
const LS_AUTO_SYNC = "auto_sync_enabled";
const CLICK_SOURCE_PREFIX = "click_source.";

function readAutoSyncFromLocalStorage(): boolean {
  if (typeof localStorage === "undefined") return true;
  const raw = localStorage.getItem(LS_AUTO_SYNC);
  if (raw === null) return true; // default ON
  try {
    const parsed = JSON.parse(raw);
    return typeof parsed === "boolean" ? parsed : true;
  } catch {
    return true;
  }
}

function writeAutoSyncToLocalStorage(enabled: boolean): void {
  if (typeof localStorage === "undefined") return;
  try {
    localStorage.setItem(LS_AUTO_SYNC, JSON.stringify(enabled));
  } catch {
    // ignore quota / disabled storage
  }
}

interface SettingEntry {
  key: string;
  value: string;
}

/// Custom event báo sync layer biết settings vừa mutate. AppInner listen +
/// gọi markMutation → useCloudSync trigger debounce push (cùng pipeline data).
const MUTATION_EVENT = "app-setting-changed";

function dispatchMutation() {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent(MUTATION_EVENT));
  }
}

/// Array entries → Settings. Defaults fill cho key thiếu (data từ DB cũ
/// trước khi đầy đủ keys, hoặc fresh DB).
///
/// `autoSyncEnabled` đọc từ localStorage (per-machine), KHÔNG từ DB. Trả
/// thêm `legacyAutoSync` để caller migrate nếu DB còn row cũ + localStorage
/// chưa có giá trị.
function entriesToSettings(entries: SettingEntry[]): {
  settings: Settings;
  legacyAutoSync: boolean | null;
} {
  const s: Settings = {
    clickSources: {},
    profitFees: { ...DEFAULT_PROFIT_FEES },
    autoSyncEnabled: readAutoSyncFromLocalStorage(),
  };
  let legacyAutoSync: boolean | null = null;
  for (const { key, value } of entries) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(value);
    } catch {
      continue; // value corrupted → skip, dùng default
    }
    if (key === KEY_PROFIT_FEE_TAX) {
      if (typeof parsed === "number" && Number.isFinite(parsed) && parsed >= 0) {
        s.profitFees.taxAndPlatformRate = parsed;
      }
    } else if (key === KEY_PROFIT_FEE_RETURN) {
      if (typeof parsed === "number" && Number.isFinite(parsed) && parsed >= 0) {
        s.profitFees.returnReserveRate = parsed;
      }
    } else if (key === KEY_AUTO_SYNC_LEGACY_DB) {
      if (typeof parsed === "boolean") {
        legacyAutoSync = parsed;
      }
    } else if (key.startsWith(CLICK_SOURCE_PREFIX)) {
      const src = key.slice(CLICK_SOURCE_PREFIX.length);
      if (src && typeof parsed === "boolean") {
        s.clickSources[src] = parsed;
      }
    }
  }
  return { settings: s, legacyAutoSync };
}

interface SettingsContextValue {
  settings: Settings;
  setClickSource: (source: string, enabled: boolean) => void;
  registerSources: (sources: string[]) => void;
  getEnabledSet: () => Set<string>;
  setProfitFee: (key: keyof ProfitFees, value: number) => void;
  setAutoSyncEnabled: (enabled: boolean) => void;
  /// Re-load settings từ DB. AppInner gọi sau khi DB swap (isStartupPhase
  /// xuống false hoặc onRemoteApplied) để tránh race: SettingsProvider mount
  /// trước switch_db_to_user xong → list_app_settings đọc pre-auth DB → empty.
  reload: () => Promise<void>;
  /// True khi loadFromDb đã hoàn tất ít nhất 1 lần với uid hiện tại. False
  /// khi pre-auth (uid=null) hoặc giữa lúc đang load. Bug B fix: consumer
  /// (App.tsx registerSources useEffect) gate trên flag này — tránh race
  /// loadFromDb wipe in-memory clickSources rồi registerSources persist
  /// lại settings dẫn đến mutation event → sync badge "dirty" sau mỗi pull.
  hydrated: boolean;
}

const SettingsContext = createContext<SettingsContextValue | null>(null);

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<Settings>(DEFAULT_SETTINGS);
  // uid từ auth — khi đổi user (admin view, switch acc), DB swap → reload từ
  // user DB mới. Pre-auth (uid=null) load từ pre-auth DB (default settings).
  const { user } = useAuth();
  const uid = user?.uid ?? null;
  // Track first-load để tránh mutation event lúc hydrate (mutation thật mới
  // dispatch). State lifecycle: initial DEFAULT → load from DB → first user
  // change → dispatch event.
  //
  // Ref dùng cho callbacks closure (persistKey/registerSources). State `hydrated`
  // mirror ref → expose qua context để consumer (App.tsx) re-run effect khi
  // hydration done. Bug B fix: nếu chỉ dùng ref, consumer không nhận signal
  // false→true → registerSources không re-fire sau load → referrer mới ko
  // được persist; hoặc fire trước load → wipe lại bởi setSettings(loaded) →
  // re-fire infinite loop.
  const hydratedRef = useRef(false);
  const [hydrated, setHydrated] = useState(false);

  /// Internal load — list_app_settings từ DB hiện tại (pre-auth hoặc user DB
  /// sau swap). Pre-auth không có user → return sớm, hold default settings.
  ///
  /// `auto_sync_enabled` đọc từ localStorage (per-machine preference).
  /// Migration legacy: nếu DB còn row cũ + localStorage chưa có giá trị,
  /// copy DB → localStorage. Row DB được giữ nguyên (build cũ trên máy
  /// khác có thể vẫn ghi vào — code mới ignore, không gây hại).
  const loadFromDb = useCallback(async (): Promise<void> => {
    if (uid === null) {
      hydratedRef.current = false;
      setHydrated(false);
      return;
    }
    try {
      const entries = await invoke<SettingEntry[]>("list_app_settings");
      const { settings: loaded, legacyAutoSync } = entriesToSettings(entries);
      if (
        legacyAutoSync !== null &&
        localStorage.getItem(LS_AUTO_SYNC) === null
      ) {
        writeAutoSyncToLocalStorage(legacyAutoSync);
        loaded.autoSyncEnabled = legacyAutoSync;
      }
      setSettings(loaded);
      hydratedRef.current = true;
      setHydrated(true);
    } catch (err) {
      console.warn("[useSettings] load failed, dùng default:", err);
    }
  }, [uid]);

  // Initial load on mount + on uid change. Load best-effort từ DB hiện tại
  // (pre-auth → empty → default; user DB → actual).
  useEffect(() => {
    hydratedRef.current = false;
    void loadFromDb();
  }, [loadFromDb]);

  /// Public reload — AppInner gọi sau DB swap (onRemoteApplied / startup done)
  /// để pick up settings từ user DB sau merge với remote.
  const reload = useCallback(async () => {
    await loadFromDb();
  }, [loadFromDb]);

  /// Helper persist 1 key → DB. Bỏ qua nếu chưa hydrate (initial load chưa
  /// xong) — tránh race ghi default value đè data DB chưa load.
  ///
  /// BE trả `false` khi value trùng row hiện có → no-op, KHÔNG dispatch
  /// mutation event để tránh SyncBadge "Chờ đồng bộ" khi user set lại cùng
  /// giá trị (vd toggle click source rồi toggle lại).
  const persistKey = useCallback(async (key: string, value: unknown) => {
    if (!hydratedRef.current) return;
    try {
      const changed = await invoke<boolean>("set_app_setting", {
        key,
        value: JSON.stringify(value),
      });
      if (changed) dispatchMutation();
    } catch (err) {
      console.warn(`[useSettings] persist ${key} failed:`, err);
    }
  }, []);

  const setClickSource = useCallback(
    (source: string, enabled: boolean) => {
      setSettings((prev) => ({
        ...prev,
        clickSources: { ...prev.clickSources, [source]: enabled },
      }));
      void persistKey(CLICK_SOURCE_PREFIX + source, enabled);
    },
    [persistKey],
  );

  const registerSources = useCallback(
    (sources: string[]) => {
      // Bug B guard: skip hoàn toàn nếu chưa hydrate. Nếu xử lý setSettings
      // trước hydrate, loadFromDb sau đó sẽ wipe state qua setSettings(loaded)
      // → reset clickSources → registerSources fire lại với cùng sources →
      // setTimeout persistKey (lúc này hydrated=true) → mutation spurious →
      // SyncBadge "dirty" sau mỗi pull. Defense-in-depth: consumer cũng nên
      // gate trên context.hydrated để tránh fire effect sớm.
      if (!hydratedRef.current) return;
      setSettings((prev) => {
        const cs = { ...prev.clickSources };
        const newKeys: string[] = [];
        for (const s of sources) {
          if (s && !(s in cs)) {
            cs[s] = true;
            newKeys.push(s);
          }
        }
        if (newKeys.length === 0) return prev;
        // Persist new keys ngoài state setter (avoid double-dispatch loop).
        // setTimeout 0 để defer ra ngoài render commit.
        setTimeout(() => {
          for (const k of newKeys) {
            void persistKey(CLICK_SOURCE_PREFIX + k, true);
          }
        }, 0);
        return { ...prev, clickSources: cs };
      });
    },
    [persistKey],
  );

  const getEnabledSet = useCallback((): Set<string> => {
    return new Set(
      Object.entries(settings.clickSources)
        .filter(([, enabled]) => enabled)
        .map(([k]) => k),
    );
  }, [settings]);

  const setProfitFee = useCallback(
    (key: keyof ProfitFees, value: number) => {
      const safe = Number.isFinite(value) && value >= 0 ? value : 0;
      setSettings((prev) => ({
        ...prev,
        profitFees: { ...prev.profitFees, [key]: safe },
      }));
      const dbKey =
        key === "taxAndPlatformRate"
          ? KEY_PROFIT_FEE_TAX
          : KEY_PROFIT_FEE_RETURN;
      void persistKey(dbKey, safe);
    },
    [persistKey],
  );

  const setAutoSyncEnabled = useCallback((enabled: boolean) => {
    setSettings((prev) =>
      prev.autoSyncEnabled === enabled
        ? prev
        : { ...prev, autoSyncEnabled: enabled },
    );
    // Per-machine preference: ghi localStorage, KHÔNG động đến app_settings
    // table → toggle hoàn toàn KHÔNG ảnh hưởng sync pipeline.
    writeAutoSyncToLocalStorage(enabled);
  }, []);

  return (
    <SettingsContext.Provider
      value={{
        settings,
        setClickSource,
        registerSources,
        getEnabledSet,
        setProfitFee,
        setAutoSyncEnabled,
        reload,
        hydrated,
      }}
    >
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings(): SettingsContextValue {
  const ctx = useContext(SettingsContext);
  if (!ctx) throw new Error("useSettings must be used within SettingsProvider");
  return ctx;
}

/// Subscribe vào mutation event để tích hợp với sync pipeline. AppInner gọi
/// useEffect listen `MUTATION_EVENT`, fire markMutation từ useDbStats.
/// Export const để external code subscribe nhất quán.
export const APP_SETTING_MUTATION_EVENT = MUTATION_EVENT;

/**
 * Tính shopeeClicks hiển thị từ breakdown theo settings.
 * Referrer không có trong settings (ví dụ "Nhập tay" khi chưa đăng ký) mặc định = enabled.
 */
export function sumFiltered(
  breakdown: Record<string, number>,
  clickSources: Record<string, boolean>,
): number {
  let total = 0;
  for (const [ref, n] of Object.entries(breakdown)) {
    if (clickSources[ref] !== false) total += n;
  }
  return total;
}
