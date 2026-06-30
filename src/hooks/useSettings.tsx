import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { invoke } from "../lib/tauri";
import { useAuth } from "../contexts/AuthContext";
import {
  sumFiltered as sumFilteredPure,
  type ProfitFees as ProfitFeesPure,
} from "../lib/profitFees";

// Re-export để consumers cũ (formulas.ts, components, worker) không phải sửa
// import path. Source of truth nằm ở `lib/profitFees.ts` — pure module worker-safe.
export type ProfitFees = ProfitFeesPure;
export const sumFiltered = sumFilteredPure;

/// Mode khớp tuple sub_id giữa FB ad và Shopee anchor.
/// - `exact`: slot-by-slot equality (default). Chỉ merge khi tuple FB là
///   vec-prefix của tuple Shopee (hoặc ngược lại).
/// - `substring`: thêm substring matching trên joined canonical
///   (case-insensitive, min 3 ký tự). Cho phép "dungcamp1" merge với "camp1"
///   khi FB campaign đặt tên dài hơn subid Shopee.
export type SubIdMatchMode = "exact" | "substring";

/// Cấu hình watermark logo Page gắn lên video tải về. Áp dụng cho tab Download.
export interface VideoWatermarkSettings {
  /// % chiều rộng video — chiều rộng logo. 1..=50.
  sizePct: number;
  /// Opacity 0..=1.
  opacity: number;
  /// % chiều rộng video — padding trên + phải. 0..=20.
  paddingPct: number;
  /// Chống ăn chôm: logo nhảy 4 góc mỗi 5s (random). Default ON — mọi video
  /// tải về sẽ tự động có random 4 góc để chống trộm. User có thể tắt trong
  /// Settings nếu muốn logo cố định ở top-right.
  antiTheft: boolean;
}

/// AI content settings cho tab Sản phẩm Shopee. Khi bật, mỗi lần fetch SP
/// thành công sẽ tự động gọi OpenAI để sinh content FB ads. API key lưu
/// SQLite plaintext (consistent với pattern lưu token hiện có); user nên dùng
/// key có quyền hạn hẹp + cap chi tiêu trong dashboard OpenAI.
export interface AiContentSettings {
  enabled: boolean;
  apiKey: string;
  /// Model OpenAI dùng cho chat/completions. Default `gpt-4o-mini`. Hỗ trợ
  /// model arbitrary (gpt-4o, gpt-4.1, ...) để user tự đổi qua input custom.
  model: string;
}

export interface Settings {
  clickSources: Record<string, boolean>;
  profitFees: ProfitFees;
  subIdMatchMode: SubIdMatchMode;
  videoWatermark: VideoWatermarkSettings;
  aiContent: AiContentSettings;
}

const DEFAULT_PROFIT_FEES: ProfitFees = {
  taxAndPlatformRate: 10.98,
  returnReserveRate: 9,
};
const DEFAULT_VIDEO_WATERMARK: VideoWatermarkSettings = {
  sizePct: 12,
  opacity: 0.9,
  paddingPct: 4,
  // Default ON — bảo vệ video khỏi bị trộm crop logo cố định góc.
  antiTheft: true,
};
const DEFAULT_AI_CONTENT: AiContentSettings = {
  enabled: false,
  apiKey: "",
  model: "gpt-4o-mini",
};
const DEFAULT_SETTINGS: Settings = {
  clickSources: {},
  profitFees: DEFAULT_PROFIT_FEES,
  subIdMatchMode: "exact",
  videoWatermark: DEFAULT_VIDEO_WATERMARK,
  aiContent: DEFAULT_AI_CONTENT,
};

const KEY_PROFIT_FEE_TAX = "profit_fee.tax_and_platform_rate";
const KEY_PROFIT_FEE_RETURN = "profit_fee.return_reserve_rate";
const KEY_SUB_ID_MATCH_MODE = "subIdMatchMode";
const KEY_WATERMARK_SIZE = "video_watermark.size_pct";
const KEY_WATERMARK_OPACITY = "video_watermark.opacity";
const KEY_WATERMARK_PADDING = "video_watermark.padding_pct";
const KEY_WATERMARK_ANTI_THEFT = "video_watermark.anti_theft";
const KEY_AI_ENABLED = "ai_content.enabled";
const KEY_AI_API_KEY = "ai_content.api_key";
const KEY_AI_MODEL = "ai_content.model";
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
    videoWatermark: { ...DEFAULT_VIDEO_WATERMARK },
    aiContent: { ...DEFAULT_AI_CONTENT },
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
    } else if (key === KEY_WATERMARK_SIZE) {
      if (typeof parsed === "number" && parsed >= 1 && parsed <= 50) {
        s.videoWatermark.sizePct = parsed;
      }
    } else if (key === KEY_WATERMARK_OPACITY) {
      if (typeof parsed === "number" && parsed >= 0 && parsed <= 1) {
        s.videoWatermark.opacity = parsed;
      }
    } else if (key === KEY_WATERMARK_PADDING) {
      if (typeof parsed === "number" && parsed >= 0 && parsed <= 20) {
        s.videoWatermark.paddingPct = parsed;
      }
    } else if (key === KEY_WATERMARK_ANTI_THEFT) {
      if (typeof parsed === "boolean") {
        s.videoWatermark.antiTheft = parsed;
      }
    } else if (key === KEY_AI_ENABLED) {
      if (typeof parsed === "boolean") s.aiContent.enabled = parsed;
    } else if (key === KEY_AI_API_KEY) {
      if (typeof parsed === "string") s.aiContent.apiKey = parsed;
    } else if (key === KEY_AI_MODEL) {
      if (typeof parsed === "string" && parsed.trim()) {
        s.aiContent.model = parsed.trim();
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
  setVideoWatermark: (key: keyof VideoWatermarkSettings, value: number) => void;
  setVideoWatermarkAntiTheft: (enabled: boolean) => void;
  setAiContent: (patch: Partial<AiContentSettings>) => void;
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
        const newEntries: SettingEntry[] = [];
        for (const s of sources) {
          if (s && !(s in cs)) {
            cs[s] = true;
            newEntries.push({
              key: CLICK_SOURCE_PREFIX + s,
              value: JSON.stringify(true),
            });
          }
        }
        if (newEntries.length === 0) return prev;
        // BULK transaction thay vì N invoke set_app_setting riêng lẻ. Khi user
        // import CSV với nhiều nguồn click mới (vd 50-100 referrers), N round-
        // trip BE x N lần acquire DbState mutex → có thể "treo" UI ở progress
        // "Đang tải nguồn click...". Bulk dùng 1 prepared stmt trong 1 tx.
        setTimeout(() => {
          void invoke<void>("set_app_settings_bulk", { entries: newEntries })
            .catch((err) =>
              console.warn("[useSettings] bulk register sources failed:", err),
            );
        }, 0);
        return { ...prev, clickSources: cs };
      });
    },
    [],
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

  const setVideoWatermark = useCallback(
    (key: keyof VideoWatermarkSettings, value: number) => {
      // Chỉ áp cho field number — antiTheft (boolean) dùng setter riêng.
      if (key === "antiTheft") return;
      const clamp = (lo: number, hi: number) =>
        Math.min(hi, Math.max(lo, Number.isFinite(value) ? value : lo));
      let safe = value;
      let dbKey: string;
      if (key === "sizePct") {
        safe = clamp(1, 50);
        dbKey = KEY_WATERMARK_SIZE;
      } else if (key === "opacity") {
        safe = clamp(0, 1);
        dbKey = KEY_WATERMARK_OPACITY;
      } else {
        safe = clamp(0, 20);
        dbKey = KEY_WATERMARK_PADDING;
      }
      setSettings((prev) => ({
        ...prev,
        videoWatermark: { ...prev.videoWatermark, [key]: safe },
      }));
      void persistKey(dbKey, safe);
    },
    [persistKey],
  );

  const setVideoWatermarkAntiTheft = useCallback(
    (enabled: boolean) => {
      setSettings((prev) => ({
        ...prev,
        videoWatermark: { ...prev.videoWatermark, antiTheft: enabled },
      }));
      void persistKey(KEY_WATERMARK_ANTI_THEFT, enabled);
    },
    [persistKey],
  );

  /// Patch 1 hoặc nhiều field AI cùng lúc. Persist riêng từng key đã đổi
  /// (không bulk vì các field độc lập và user thường chỉ đổi 1 lúc).
  const setAiContent = useCallback(
    (patch: Partial<AiContentSettings>) => {
      setSettings((prev) => {
        const next = { ...prev.aiContent, ...patch };
        if (patch.enabled !== undefined && patch.enabled !== prev.aiContent.enabled) {
          void persistKey(KEY_AI_ENABLED, next.enabled);
        }
        if (patch.apiKey !== undefined && patch.apiKey !== prev.aiContent.apiKey) {
          void persistKey(KEY_AI_API_KEY, next.apiKey);
        }
        if (patch.model !== undefined && patch.model !== prev.aiContent.model) {
          void persistKey(KEY_AI_MODEL, next.model);
        }
        return { ...prev, aiContent: next };
      });
    },
    [persistKey],
  );

  // useMemo: KHÔNG tạo object literal mới mỗi render. Nếu thiếu, mọi
  // consumer useSettings() (hàng trăm VideoRow + DayBlock) sẽ re-render
  // bất cứ khi nào SettingsProvider re-render — kể cả khi data thực không
  // đổi. Đây là silent killer của memoization.
  const value = useMemo<SettingsContextValue>(
    () => ({
      settings,
      setClickSource,
      registerSources,
      getEnabledSet,
      setProfitFee,
      setSubIdMatchMode,
      setVideoWatermark,
      setVideoWatermarkAntiTheft,
      setAiContent,
      reload,
      hydrated,
    }),
    [
      settings,
      setClickSource,
      registerSources,
      getEnabledSet,
      setProfitFee,
      setSubIdMatchMode,
      setVideoWatermark,
      setVideoWatermarkAntiTheft,
      setAiContent,
      reload,
      hydrated,
    ],
  );

  return (
    <SettingsContext.Provider value={value}>
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings(): SettingsContextValue {
  const ctx = useContext(SettingsContext);
  if (!ctx) throw new Error("useSettings must be used within SettingsProvider");
  return ctx;
}

