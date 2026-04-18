import {
  createContext,
  useCallback,
  useContext,
  useState,
  type ReactNode,
} from "react";

export interface ProfitFees {
  /** % thuế + phí sàn khấu trừ từ hoa hồng (vd 10.98). */
  taxAndPlatformRate: number;
  /** % dự phòng hoàn/hủy đơn (vd 9). */
  returnReserveRate: number;
}

export interface Settings {
  clickSources: Record<string, boolean>;
  profitFees: ProfitFees;
}

const STORAGE_KEY = "thongkeshopee.settings.v1";
const DEFAULT_PROFIT_FEES: ProfitFees = {
  taxAndPlatformRate: 10.98,
  returnReserveRate: 9,
};
const DEFAULT_SETTINGS: Settings = {
  clickSources: {},
  profitFees: DEFAULT_PROFIT_FEES,
};

function loadSettings(): Settings {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT_SETTINGS;
    const parsed = JSON.parse(raw);
    return {
      clickSources: { ...(parsed.clickSources ?? {}) },
      profitFees: {
        ...DEFAULT_PROFIT_FEES,
        ...(parsed.profitFees ?? {}),
      },
    };
  } catch {
    return DEFAULT_SETTINGS;
  }
}

function persist(s: Settings) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(s));
  } catch {
    // ignore quota / privacy-mode errors
  }
}

interface SettingsContextValue {
  settings: Settings;
  setClickSource: (source: string, enabled: boolean) => void;
  registerSources: (sources: string[]) => void;
  getEnabledSet: () => Set<string>;
  setProfitFee: (key: keyof ProfitFees, value: number) => void;
}

const SettingsContext = createContext<SettingsContextValue | null>(null);

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<Settings>(loadSettings);

  const setClickSource = useCallback((source: string, enabled: boolean) => {
    setSettings((prev) => {
      const next: Settings = {
        ...prev,
        clickSources: { ...prev.clickSources, [source]: enabled },
      };
      persist(next);
      return next;
    });
  }, []);

  const registerSources = useCallback((sources: string[]) => {
    setSettings((prev) => {
      const cs = { ...prev.clickSources };
      let changed = false;
      for (const s of sources) {
        if (s && !(s in cs)) {
          cs[s] = true;
          changed = true;
        }
      }
      if (!changed) return prev;
      const next: Settings = { ...prev, clickSources: cs };
      persist(next);
      return next;
    });
  }, []);

  const getEnabledSet = useCallback((): Set<string> => {
    return new Set(
      Object.entries(settings.clickSources)
        .filter(([, enabled]) => enabled)
        .map(([k]) => k),
    );
  }, [settings]);

  const setProfitFee = useCallback(
    (key: keyof ProfitFees, value: number) => {
      setSettings((prev) => {
        const safe = Number.isFinite(value) && value >= 0 ? value : 0;
        const next: Settings = {
          ...prev,
          profitFees: { ...prev.profitFees, [key]: safe },
        };
        persist(next);
        return next;
      });
    },
    [],
  );

  return (
    <SettingsContext.Provider
      value={{
        settings,
        setClickSource,
        registerSources,
        getEnabledSet,
        setProfitFee,
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

/** Phần trăm tổng giữ lại của hoa hồng sau khi trừ thuế+phí sàn và dự phòng hoàn hủy. */
export function netCommissionRatio(fees: ProfitFees): number {
  const deduct = (fees.taxAndPlatformRate + fees.returnReserveRate) / 100;
  return Math.max(0, 1 - deduct);
}
