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

/// Mode khớp tuple sub_id giữa FB ad và Shopee anchor.
/// - `exact`: slot-by-slot equality (default). Chỉ merge khi tuple FB là
///   vec-prefix của tuple Shopee (hoặc ngược lại).
/// - `substring`: thêm substring matching trên joined canonical
///   (case-insensitive, min 3 ký tự). Cho phép "dungcamp1" merge với "camp1"
///   khi FB campaign đặt tên dài hơn subid Shopee.
export type SubIdMatchMode = "exact" | "substring";

export interface Settings {
  clickSources: Record<string, boolean>;
  profitFees: ProfitFees;
  subIdMatchMode: SubIdMatchMode;
}

const DEFAULT_PROFIT_FEES: ProfitFees = {
  taxAndPlatformRate: 10.98,
  returnReserveRate: 9,
};
const DEFAULT_SETTINGS: Settings = {
  clickSources: {},
  profitFees: DEFAULT_PROFIT_FEES,
  subIdMatchMode: "exact",
};

const KEY_PROFIT_FEE_TAX = "profit_fee.tax_and_platform_rate";
const KEY_PROFIT_FEE_RETURN = "profit_fee.return_reserve_rate";
const KEY_SUB_ID_MATCH_MODE = "subIdMatchMode";
const CLICK_SOURCE_PREFIX = "click_source.";

interface SettingEntry {
  key: string;
  value: string;
}

function entriesToSettings(entries: SettingEntry[]): Settings {
  const s: Settings = {
    clickSources: {},
    profitFees: { ...DEFAULT_PROFIT_FEES },
    subIdMatchMode: "exact",
  };
  for (const { key, value } of entries) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(value);
    } catch {
      continue;
    }
    if (key === KEY_PROFIT_FEE_TAX) {
      if (typeof parsed === "number" && Number.isFinite(parsed) && parsed >= 0) {
        s.profitFees.taxAndPlatformRate = parsed;
      }
    } else if (key === KEY_PROFIT_FEE_RETURN) {
      if (typeof parsed === "number" && Number.isFinite(parsed) && parsed >= 0) {
        s.profitFees.returnReserveRate = parsed;
      }
    } else if (key === KEY_SUB_ID_MATCH_MODE) {
      if (parsed === "exact" || parsed === "substring") {
        s.subIdMatchMode = parsed;
      }
    } else if (key.startsWith(CLICK_SOURCE_PREFIX)) {
      const src = key.slice(CLICK_SOURCE_PREFIX.length);
      if (src && typeof parsed === "boolean") {
        s.clickSources[src] = parsed;
      }
    }
  }
  return s;
}

interface SettingsContextValue {
  settings: Settings;
  setClickSource: (source: string, enabled: boolean) => void;
  registerSources: (sources: string[]) => void;
  getEnabledSet: () => Set<string>;
  setProfitFee: (key: keyof ProfitFees, value: number) => void;
  setSubIdMatchMode: (mode: SubIdMatchMode) => void;
  reload: () => Promise<void>;
  hydrated: boolean;
}

const SettingsContext = createContext<SettingsContextValue | null>(null);

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<Settings>(DEFAULT_SETTINGS);
  const { user } = useAuth();
  const uid = user?.uid ?? null;
  const hydratedRef = useRef(false);
  const [hydrated, setHydrated] = useState(false);

  const loadFromDb = useCallback(async (): Promise<void> => {
    if (uid === null) {
      hydratedRef.current = false;
      setHydrated(false);
      return;
    }
    try {
      const entries = await invoke<SettingEntry[]>("list_app_settings");
      const loaded = entriesToSettings(entries);
      setSettings(loaded);
      hydratedRef.current = true;
      setHydrated(true);
    } catch (err) {
      console.warn("[useSettings] load failed, dùng default:", err);
    }
  }, [uid]);

  useEffect(() => {
    hydratedRef.current = false;
    void loadFromDb();
  }, [loadFromDb]);

  const reload = useCallback(async () => {
    await loadFromDb();
  }, [loadFromDb]);

  const persistKey = useCallback(async (key: string, value: unknown) => {
    if (!hydratedRef.current) return;
    try {
      await invoke<boolean>("set_app_setting", {
        key,
        value: JSON.stringify(value),
      });
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

  const setSubIdMatchMode = useCallback(
    (mode: SubIdMatchMode) => {
      setSettings((prev) => ({ ...prev, subIdMatchMode: mode }));
      void persistKey(KEY_SUB_ID_MATCH_MODE, mode);
    },
    [persistKey],
  );

  return (
    <SettingsContext.Provider
      value={{
        settings,
        setClickSource,
        registerSources,
        getEnabledSet,
        setProfitFee,
        setSubIdMatchMode,
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
