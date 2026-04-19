import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { DayBlock } from "./components/DayBlock";
import { SubIdTimelineBlock } from "./components/SubIdTimelineBlock";
import { ManualEntryDialog } from "./components/ManualEntryDialog";
import { SettingsDialog } from "./components/SettingsDialog";
import { RulesDialog } from "./components/RulesDialog";
import { PendingChangesBar } from "./components/PendingChangesBar";
import { ImportPreviewDialog } from "./components/ImportPreviewDialog";
import { DownloadVideoPage } from "./components/DownloadVideoPage";
import { useDbStats, todayIso } from "./hooks/useDbStats";
import { SettingsProvider, useSettings } from "./hooks/useSettings";
import { useToast } from "./components/ToastProvider";
import { commitCsvBatch, previewCsvBatch } from "./lib/dbImport";
import type { PreviewBatch } from "./lib/dbImport";
import type { UiRow } from "./types";
import { fmtDate, fmtInt } from "./formulas";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { usePremium } from "./hooks/usePremium";
import { LoginScreen } from "./components/LoginScreen";
import { PaywallScreen } from "./components/PaywallScreen";
import "./App.css";

// =========================================================
// Filter state: recent N days (default + infinite scroll) OR explicit range
// =========================================================

/**
 * Filter mode:
 * - `recent` có `canExpand`:
 *   - `canExpand=true`  → mặc định paginated 7, scroll xuống load more 7 ngày.
 *   - `canExpand=false` → shortcut khóa cứng N ngày, scroll không load thêm.
 * - `range`: khoảng ngày explicit (date picker hoặc shortcut "Tháng trước").
 * - `all`: hiện toàn bộ data ("Từ trước đến nay").
 */
type FilterMode =
  | { type: "recent"; count: number; canExpand: boolean }
  | { type: "range"; from: string; to: string }
  | { type: "all" };

/** Đầu + cuối của tháng trước so với today local. Trả YYYY-MM-DD. */
function prevMonthRange(): { from: string; to: string } {
  const now = new Date();
  // Day 0 của month hiện tại = last day của previous month.
  const end = new Date(now.getFullYear(), now.getMonth(), 0);
  const start = new Date(end.getFullYear(), end.getMonth(), 1);
  const fmt = (d: Date) => {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
  };
  return { from: fmt(start), to: fmt(end) };
}

const DEFAULT_RECENT = 7;
const LOAD_MORE_STEP = 7;
const FILTER_STORAGE_KEY = "thongkeshopee.filter.v2";

const DEFAULT_MODE: FilterMode = {
  type: "recent",
  count: DEFAULT_RECENT,
  canExpand: true,
};

function loadFilterMode(): FilterMode {
  try {
    const raw = localStorage.getItem(FILTER_STORAGE_KEY);
    if (!raw) return DEFAULT_MODE;
    const parsed = JSON.parse(raw);
    if (parsed?.type === "recent" && Number.isFinite(parsed.count)) {
      return {
        type: "recent",
        count: Math.max(1, Math.floor(parsed.count)),
        canExpand: !!parsed.canExpand,
      };
    }
    if (parsed?.type === "range" && typeof parsed.from === "string") {
      return {
        type: "range",
        from: parsed.from,
        to: typeof parsed.to === "string" ? parsed.to : "",
      };
    }
    if (parsed?.type === "all") return { type: "all" };
  } catch {
    // ignore
  }
  return DEFAULT_MODE;
}

function saveFilterMode(m: FilterMode) {
  try {
    localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(m));
  } catch {
    // ignore
  }
}

function AppInner() {
  const {
    days,
    referrers,
    loading,
    error,
    refetch,
    saveManualEntry,
    pendingRowDeletes,
    pendingDayDeletes,
    toggleRowPending,
    toggleDayPending,
    clearPending,
    commitPending,
    pendingCount,
  } = useDbStats();

  const { showToast } = useToast();
  const { settings, setClickSource, registerSources, setProfitFee } =
    useSettings();

  // Auto-register referrers từ DB vào settings.clickSources.
  // Mỗi khi DB thay đổi (import mới, xóa, v.v.) → refetch → referrers mới → register.
  // registerSources chỉ thêm referrer mới (default enabled), không đụng trạng thái cũ.
  useEffect(() => {
    if (referrers.length > 0) registerSources(referrers);
  }, [referrers, registerSources]);

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"stats" | "download">("stats");
  const [filterMode, setFilterMode] = useState<FilterMode>(() =>
    loadFilterMode(),
  );
  const [subIdQuery, setSubIdQuery] = useState("");
  const [selectedSubId, setSelectedSubId] = useState<string | null>(null);
  const [subIdFocused, setSubIdFocused] = useState(false);
  const subIdInputRef = useRef<HTMLInputElement>(null);
  const [entryDialog, setEntryDialog] = useState<{
    date: string;
    row?: UiRow | null;
  } | null>(null);
  const [previewBatch, setPreviewBatch] = useState<PreviewBatch | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleImportClick = () => fileInputRef.current?.click();

  const handleFilesSelected = async (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const files = Array.from(e.target.files ?? []);
    e.target.value = "";
    if (files.length === 0) return;

    try {
      const batch = await previewCsvBatch(files);
      setPreviewBatch(batch);
    } catch (err) {
      showToast({
        message: (err as Error).message ?? String(err),
        duration: 10000,
      });
    }
  };

  const handleConfirmImport = useCallback(async () => {
    if (!previewBatch) return;
    const results = await commitCsvBatch(previewBatch);
    const date = previewBatch.dayDate;
    const totalNew = results.reduce((a, r) => a + r.inserted, 0);
    const totalReplace = results.reduce((a, r) => a + r.duplicated, 0);
    setPreviewBatch(null);
    await refetch();
    showToast({
      message: `Đã import ngày ${fmtDate(date)}: ${fmtInt(totalNew)} dòng mới${
        totalReplace > 0 ? `, ${fmtInt(totalReplace)} dòng replace` : ""
      }`,
      duration: 5000,
    });
  }, [previewBatch, refetch, showToast]);

  const handleSaveEntry = useCallback(
    async (input: Parameters<typeof saveManualEntry>[0]) => {
      await saveManualEntry(input);
      setEntryDialog(null);
      showToast({
        message: "Đã lưu thay đổi",
        duration: 2500,
      });
    },
    [saveManualEntry, showToast],
  );

  const handleCommitPending = useCallback(async () => {
    try {
      await commitPending();
      showToast({ message: "Đã lưu toàn bộ thay đổi", duration: 3000 });
    } catch (e) {
      showToast({
        message: `Lỗi: ${(e as Error).message}`,
        duration: 7000,
      });
    }
  }, [commitPending, showToast]);

  const productsCount = useMemo(
    () => days.reduce((a, d) => a + d.rows.length, 0),
    [days],
  );

  // Persist filter mode vào localStorage mỗi khi đổi.
  useEffect(() => {
    saveFilterMode(filterMode);
  }, [filterMode]);

  /**
   * Áp filter vào `days`:
   * - `recent`: take N dòng đầu (data sorted DESC) — infinite scroll tăng N thêm.
   * - `range`: lọc trong `[min(from,to), max(from,to)]`.
   * - `all`: toàn bộ data.
   */
  const filteredDays = useMemo(() => {
    if (filterMode.type === "all") return days;
    if (filterMode.type === "recent") {
      return days.slice(0, filterMode.count);
    }
    const { from, to } = filterMode;
    if (!from && !to) return days;
    const a = from || to;
    const b = to || from;
    const [lo, hi] = a <= b ? [a, b] : [b, a];
    return days.filter((d) => d.date >= lo && d.date <= hi);
  }, [days, filterMode]);

  // Chỉ default-paginated mode mới expand khi scroll.
  const canLoadMore =
    filterMode.type === "recent" &&
    filterMode.canExpand &&
    filterMode.count < days.length;

  const clearFilter = () => setFilterMode(DEFAULT_MODE);

  const setRecentDays = useCallback(
    (n: number) =>
      setFilterMode({ type: "recent", count: n, canExpand: false }),
    [],
  );

  const setPrevMonth = useCallback(() => {
    const r = prevMonthRange();
    setFilterMode({ type: "range", from: r.from, to: r.to });
  }, []);

  const setAllTime = useCallback(
    () => setFilterMode({ type: "all" }),
    [],
  );

  // Detect active cho highlight shortcut "Tháng trước".
  const prevMonth = useMemo(() => prevMonthRange(), []);
  const isPrevMonthActive =
    filterMode.type === "range" &&
    filterMode.from === prevMonth.from &&
    filterMode.to === prevMonth.to;

  const setDateFrom = (v: string) => {
    setFilterMode((m) => ({
      type: "range",
      from: v,
      to: m.type === "range" ? m.to : "",
    }));
  };
  const setDateTo = (v: string) => {
    setFilterMode((m) => ({
      type: "range",
      from: m.type === "range" ? m.from : "",
      to: v,
    }));
  };

  // Hiển thị dates trong picker:
  // - range mode → lưu trực tiếp trong filterMode.
  // - shortcut recent (!canExpand) → derive từ filteredDays.
  // - all → derive từ toàn bộ days nếu có.
  // - default paginated → để trống (chưa chọn range cụ thể).
  const { dateFrom, dateTo } = useMemo<{ dateFrom: string; dateTo: string }>(() => {
    if (filterMode.type === "range") {
      return { dateFrom: filterMode.from, dateTo: filterMode.to };
    }
    if (filterMode.type === "all" && days.length > 0) {
      return {
        dateFrom: days[days.length - 1].date,
        dateTo: days[0].date,
      };
    }
    if (
      filterMode.type === "recent" &&
      !filterMode.canExpand &&
      filteredDays.length > 0
    ) {
      return {
        dateFrom: filteredDays[filteredDays.length - 1].date,
        dateTo: filteredDays[0].date,
      };
    }
    return { dateFrom: "", dateTo: "" };
  }, [filterMode, filteredDays, days]);

  // ============================================================
  // Search theo sub_id (displayName). Gợi ý gồm cả displayName đầy đủ lẫn
  // các prefix hierarchy (split by "-") để user chọn level thích hợp.
  // Ví dụ row "dammaxi-0416" → suggestions có "dammaxi" + "dammaxi-0416".
  // Chọn "dammaxi" → prefix-match → bắt được cả "dammaxi", "dammaxi-0412",
  // "dammaxi-0416"... dù chúng xuất hiện trên các ngày khác nhau.
  // ============================================================
  const allSubIds = useMemo(() => {
    const set = new Set<string>();
    for (const d of days) {
      for (const r of d.rows) {
        const name = r.displayName;
        if (!name) continue;
        set.add(name);
        const parts = name.split("-").filter((p) => p);
        // Prefix levels để user chọn theo cây.
        for (let i = 1; i < parts.length; i++) {
          set.add(parts.slice(0, i).join("-"));
        }
        // Từng part riêng để user search ngang (VD "dammaxi" match cả
        // "dammaxi-0410" lẫn "MuseStudio-dammaxi" khi naming không nhất quán).
        for (const p of parts) set.add(p);
      }
    }
    return Array.from(set).sort();
  }, [days]);

  /**
   * Suggestions filter:
   * - Rỗng → show toàn bộ sub_id (sorted), dropdown scroll nội bộ nếu nhiều.
   * - Có query → match theo **startsWith** tại bất kỳ part nào của suggestion.
   *   VD query "damma" → match "dammaxi", "dammaxi-0412", "MuseStudio-dammaxi"
   *   vì part "dammaxi" startsWith("damma"). Tránh noise từ substring giữa part.
   */
  const suggestions = useMemo(() => {
    const q = subIdQuery.toLowerCase().trim();
    if (!q) return allSubIds;
    return allSubIds.filter((s) =>
      s
        .toLowerCase()
        .split("-")
        .some((part) => part.startsWith(q)),
    );
  }, [allSubIds, subIdQuery]);

  /**
   * row displayName match "selected" khi **mọi part của selected đều có trong row parts**
   * (subset của set parts sau khi split "-"). Ví dụ:
   * - selected "dammaxi" match "dammaxi", "dammaxi-0410", "MuseStudio-dammaxi"
   * - selected "dammaxi-0412" match "dammaxi-0412", "dammaxi-0412-v1",
   *   "MuseStudio-dammaxi-0412" (vì cả "dammaxi" và "0412" đều có)
   * - selected "MuseStudio-dammaxi" match "MuseStudio-dammaxi", "MuseStudio-dammaxi-0412"
   *   nhưng KHÔNG match "dammaxi-0410" (thiếu "MuseStudio" part)
   */
  const matchSubId = (rowDisplayName: string, selected: string) => {
    if (!selected) return true;
    const rowParts = new Set(rowDisplayName.split("-").filter((p) => p));
    const selParts = selected.split("-").filter((p) => p);
    return selParts.every((p) => rowParts.has(p));
  };

  // Combined filter: date filter → sub_id filter (prefix-compatible).
  const finalDays = useMemo(() => {
    if (!selectedSubId) return filteredDays;
    return filteredDays
      .map((d) => ({
        ...d,
        rows: d.rows.filter((r) => matchSubId(r.displayName, selectedSubId)),
      }))
      .filter((d) => d.rows.length > 0);
  }, [filteredDays, selectedSubId]);

  const clearSubId = () => {
    setSelectedSubId(null);
    setSubIdQuery("");
    // Refocus input ngay lập tức để dropdown mở lại, user gõ tiếp không bị mất.
    requestAnimationFrame(() => {
      subIdInputRef.current?.focus();
      setSubIdFocused(true);
    });
  };

  // Infinite scroll: observer sentinel → load more 7 ngày khi tới gần đáy.
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!canLoadMore) return;
    const el = loadMoreRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          setFilterMode((m) =>
            m.type === "recent" && m.canExpand
              ? { ...m, count: m.count + LOAD_MORE_STEP }
              : m,
          );
        }
      },
      { rootMargin: "200px" },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [canLoadMore]);

  return (
    <main className="min-h-full bg-surface-0 pb-24">
      <header className="sticky top-0 z-30 bg-gradient-to-r from-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center justify-between px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="material-symbols-rounded text-3xl text-white">
              analytics
            </span>
            <div>
              <h1 className="text-xl font-semibold tracking-tight text-white">
                Thống kê Shopee Affiliate
              </h1>
              <p className="text-xs text-white/70">
                Data từ database — manual override luôn ưu tiên raw CSV
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setRulesOpen(true)}
              className="btn-ripple flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
              title="Quy tắc sử dụng"
              aria-label="Quy tắc"
            >
              <span className="material-symbols-rounded text-base">
                menu_book
              </span>
              Quy tắc
            </button>
            <button
              onClick={() => setSettingsOpen(true)}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
              title="Cài đặt"
              aria-label="Cài đặt"
            >
              <span className="material-symbols-rounded">settings</span>
            </button>
            {activeTab === "stats" && (
              <>
                <button
                  onClick={handleImportClick}
                  className="btn-ripple flex items-center gap-2 rounded-lg border border-white/50 px-4 py-2 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
                >
                  <span className="material-symbols-rounded text-base">
                    upload_file
                  </span>
                  Import CSV
                </button>
                <button
                  onClick={() => setEntryDialog({ date: todayIso() })}
                  className="btn-ripple flex items-center gap-2 rounded-lg bg-white px-4 py-2 text-sm font-medium text-shopee-600 shadow-elev-2 hover:shadow-elev-4"
                >
                  <span className="material-symbols-rounded text-base">
                    add
                  </span>
                  Thêm dòng
                </button>
              </>
            )}
          </div>
        </div>

        {/* Tab nav */}
        <nav className="flex gap-1 px-6">
          <TabButton
            active={activeTab === "stats"}
            onClick={() => setActiveTab("stats")}
            icon="analytics"
            label="Thống kê"
          />
          <TabButton
            active={activeTab === "download"}
            onClick={() => setActiveTab("download")}
            icon="download"
            label="Download video"
          />
        </nav>
      </header>

      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,text/csv"
        multiple
        className="hidden"
        onChange={handleFilesSelected}
      />

      <div className="p-6">
        {activeTab === "download" ? (
          <DownloadVideoPage />
        ) : loading ? (
          <div className="mx-auto max-w-xl py-12 text-center text-white/60">
            Đang tải...
          </div>
        ) : error ? (
          <div className="mx-auto max-w-xl rounded-lg border border-red-500/50 bg-red-900/30 p-6 text-red-200">
            Lỗi DB: {error}
            <button
              onClick={() => void refetch()}
              className="ml-3 rounded-md bg-red-500 px-3 py-1 text-sm text-white hover:bg-red-600"
            >
              Thử lại
            </button>
          </div>
        ) : days.length === 0 ? (
          <div className="mx-auto flex max-w-xl flex-col items-center gap-4 rounded-2xl border border-surface-8 bg-surface-1 p-12 text-center shadow-elev-1">
            <span className="material-symbols-rounded text-6xl text-shopee-400">
              calendar_month
            </span>
            <div>
              <h2 className="text-lg font-medium text-white/90">
                Chưa có data nào
              </h2>
              <p className="mt-1 text-sm text-white/60">
                Bắt đầu bằng import CSV hoặc thêm dòng thủ công
              </p>
            </div>
            <div className="mt-2 flex flex-wrap items-center justify-center gap-2">
              <button
                onClick={() => setEntryDialog({ date: todayIso() })}
                className="btn-ripple flex items-center gap-2 rounded-lg bg-shopee-500 px-5 py-2.5 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
              >
                <span className="material-symbols-rounded text-base">add</span>
                Thêm dòng đầu tiên
              </button>
              <button
                onClick={handleImportClick}
                className="btn-ripple flex items-center gap-2 rounded-lg border border-surface-8 bg-surface-4 px-5 py-2.5 text-sm font-medium text-white/90 hover:bg-surface-6"
              >
                <span className="material-symbols-rounded text-base">
                  upload_file
                </span>
                Import CSV
              </button>
            </div>
          </div>
        ) : (
          <>
            <div className="sticky top-[92px] z-20 -mx-6 mb-4 border-b border-surface-8 bg-surface-0/95 px-6 py-3 backdrop-blur">
              <div className="flex items-start gap-3 rounded-xl border border-surface-8 bg-surface-2 px-4 py-2 text-sm">
               <div className="flex flex-1 flex-wrap items-center gap-x-3 gap-y-2 min-w-0">
                {/* Group 2: Date range (bám trái) */}
                <div className="flex shrink-0 items-center gap-1.5">
                  <span
                    className="material-symbols-rounded shrink-0 text-shopee-400"
                    title="Khoảng ngày"
                  >
                    date_range
                  </span>
                  <input
                    type="date"
                    value={dateFrom}
                    onChange={(e) => setDateFrom(e.currentTarget.value)}
                    title="Từ ngày"
                    className="w-[135px] shrink-0 rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-sm text-white/90 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
                  />
                  <span className="text-white/40">–</span>
                  <input
                    type="date"
                    value={dateTo}
                    onChange={(e) => setDateTo(e.currentTarget.value)}
                    title="Đến ngày"
                    className="w-[135px] shrink-0 rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-sm text-white/90 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
                  />
                </div>

                <span className="hidden h-6 w-px bg-surface-8 md:inline-block" />

                {/* Group 3: Shortcuts (bám trái, ngay sau date) */}
                <div className="flex shrink-0 flex-wrap items-center gap-1">
                  <ShortcutButton
                    active={
                      filterMode.type === "recent" &&
                      !filterMode.canExpand &&
                      filterMode.count === 1
                    }
                    onClick={() => setRecentDays(1)}
                  >
                    Ngày gần nhất
                  </ShortcutButton>
                  <ShortcutButton
                    active={
                      filterMode.type === "recent" &&
                      !filterMode.canExpand &&
                      filterMode.count === 7
                    }
                    onClick={() => setRecentDays(7)}
                  >
                    7 ngày
                  </ShortcutButton>
                  <ShortcutButton
                    active={
                      filterMode.type === "recent" &&
                      !filterMode.canExpand &&
                      filterMode.count === 14
                    }
                    onClick={() => setRecentDays(14)}
                  >
                    14 ngày
                  </ShortcutButton>
                  <ShortcutButton
                    active={
                      filterMode.type === "recent" &&
                      !filterMode.canExpand &&
                      filterMode.count === 30
                    }
                    onClick={() => setRecentDays(30)}
                  >
                    30 ngày
                  </ShortcutButton>
                  <ShortcutButton
                    active={isPrevMonthActive}
                    onClick={setPrevMonth}
                  >
                    Tháng trước
                  </ShortcutButton>
                  <ShortcutButton
                    active={filterMode.type === "all"}
                    onClick={setAllTime}
                  >
                    Từ trước đến nay
                  </ShortcutButton>
                </div>

                {(filterMode.type === "range" ||
                  filterMode.type === "all" ||
                  (filterMode.type === "recent" &&
                    !filterMode.canExpand)) && (
                  <button
                    onClick={clearFilter}
                    className="btn-ripple flex shrink-0 items-center gap-1 rounded-lg px-2 py-1 text-xs text-white/70 hover:bg-white/5 hover:text-white"
                    title="Bỏ lọc ngày — về paginated 7 ngày mặc định"
                  >
                    <span className="material-symbols-rounded text-sm">
                      close
                    </span>
                    Bỏ lọc
                  </button>
                )}

                <span className="hidden h-6 w-px bg-surface-8 md:inline-block" />

                {/* Group 1: Sub_id search (flex-1 fill remaining) */}
                <div className="relative flex min-w-[200px] max-w-[340px] flex-1 items-center gap-1.5">
                  <span
                    className="material-symbols-rounded shrink-0 text-shopee-400"
                    title="Sub_id"
                  >
                    tag
                  </span>
                  <input
                    ref={subIdInputRef}
                    type="text"
                    value={selectedSubId ?? subIdQuery}
                    onChange={(e) => {
                      setSelectedSubId(null);
                      setSubIdQuery(e.currentTarget.value);
                      setSubIdFocused(true);
                    }}
                    onFocus={() => setSubIdFocused(true)}
                    onClick={() => setSubIdFocused(true)}
                    onBlur={() =>
                      setTimeout(() => setSubIdFocused(false), 150)
                    }
                    placeholder="Tìm Sub_id..."
                    className="w-full min-w-0 rounded-md border border-surface-8 bg-surface-1 px-2.5 py-1 text-sm text-white/90 placeholder:text-white/30 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
                  />
                  {selectedSubId && (
                    <button
                      onClick={clearSubId}
                      className="btn-ripple flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-white/50 hover:bg-white/10 hover:text-white"
                      title="Bỏ chọn sub_id"
                    >
                      <span className="material-symbols-rounded text-sm">
                        close
                      </span>
                    </button>
                  )}
                  <span
                    className="shrink-0 whitespace-nowrap rounded-full bg-surface-6 px-2 py-0.5 text-[11px] font-medium text-white/55"
                    title={`Tổng ${allSubIds.length} sub_id trong DB`}
                  >
                    {allSubIds.length}
                  </span>
                  {subIdFocused && suggestions.length > 0 && (
                    <ul className="absolute left-6 right-0 top-full z-30 mt-1 max-h-[400px] overflow-y-auto rounded-lg border border-surface-8 bg-surface-4 shadow-elev-16">
                      {suggestions.map((s) => (
                        <li key={s}>
                          <button
                            type="button"
                            onMouseDown={(e) => {
                              e.preventDefault();
                              setSelectedSubId(s);
                              setSubIdQuery("");
                              setSubIdFocused(false);
                            }}
                            className="block w-full truncate px-3 py-1.5 text-left font-mono text-xs text-white/85 hover:bg-shopee-900/30 hover:text-shopee-200"
                            title={s}
                          >
                            {s}
                          </button>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

               </div>

                {/* Right anchor: badge counter, luôn bám phải */}
                <div className="flex shrink-0 items-center gap-2 pt-1">
                  <span className="whitespace-nowrap rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-300">
                    {finalDays.length} / {days.length} ngày
                    {canLoadMore && " · scroll để xem thêm"}
                  </span>
                </div>
              </div>
            </div>
            {finalDays.length === 0 ? (
              <div className="mx-auto max-w-xl rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-12 text-center text-white/60">
                <span className="material-symbols-rounded text-5xl text-white/30">
                  search_off
                </span>
                <p className="mt-3">
                  {selectedSubId
                    ? `Không có dòng nào khớp "${selectedSubId}" trong khoảng lọc.`
                    : "Không có ngày nào khớp với khoảng lọc. Thử mở rộng khoảng hoặc bỏ lọc."}
                </p>
              </div>
            ) : (
              <>
                {selectedSubId ? (
                  <SubIdTimelineBlock
                    subId={selectedSubId}
                    days={finalDays}
                    pendingRowDeletes={pendingRowDeletes}
                    onToggleRowDelete={(r) =>
                      toggleRowPending(r.dayDate, r.subIds)
                    }
                    onEditRow={(r) =>
                      setEntryDialog({ date: r.dayDate, row: r })
                    }
                  />
                ) : (
                  finalDays.map((day) => (
                    <DayBlock
                      key={day.date}
                      day={day}
                      pendingDayDeletes={pendingDayDeletes}
                      pendingRowDeletes={pendingRowDeletes}
                      onToggleDayDelete={toggleDayPending}
                      onToggleRowDelete={(r) =>
                        toggleRowPending(r.dayDate, r.subIds)
                      }
                      onEditRow={(r) =>
                        setEntryDialog({ date: r.dayDate, row: r })
                      }
                      onEditDay={(date) => setEntryDialog({ date })}
                    />
                  ))
                )}
                {canLoadMore && (
                  <div
                    ref={loadMoreRef}
                    className="mx-auto flex max-w-xs items-center justify-center gap-2 py-4 text-xs text-white/50"
                  >
                    <span className="material-symbols-rounded animate-spin text-base">
                      progress_activity
                    </span>
                    Đang tải thêm ngày cũ hơn...
                  </div>
                )}
              </>
            )}
          </>
        )}
      </div>

      <SettingsDialog
        isOpen={settingsOpen}
        settings={settings}
        daysCount={days.length}
        productsCount={productsCount}
        onToggleClickSource={setClickSource}
        onSetProfitFee={setProfitFee}
        onClose={() => setSettingsOpen(false)}
      />

      <RulesDialog isOpen={rulesOpen} onClose={() => setRulesOpen(false)} />

      {entryDialog && (
        <ManualEntryDialog
          isOpen={true}
          initialDate={entryDialog.date}
          initialRow={entryDialog.row}
          onSave={handleSaveEntry}
          onClose={() => setEntryDialog(null)}
        />
      )}

      <ImportPreviewDialog
        batch={previewBatch}
        onConfirm={handleConfirmImport}
        onCancel={() => setPreviewBatch(null)}
      />

      <PendingChangesBar
        count={pendingCount}
        onCommit={handleCommitPending}
        onCancel={clearPending}
      />
    </main>
  );
}

function ShortcutButton({
  active,
  onClick,
  children,
}: {
  active?: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`btn-ripple rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
        active
          ? "border border-shopee-500 bg-shopee-500/20 text-shopee-200"
          : "border border-surface-8 bg-surface-1 text-white/80 hover:border-shopee-500/50 hover:bg-shopee-900/20 hover:text-shopee-200"
      }`}
    >
      {children}
    </button>
  );
}

interface TabButtonProps {
  active: boolean;
  onClick: () => void;
  icon: string;
  label: string;
}

function TabButton({ active, onClick, icon, label }: TabButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`btn-ripple flex items-center gap-1.5 rounded-t-lg px-4 py-2 text-sm font-medium transition-colors ${
        active
          ? "bg-surface-0 text-shopee-300"
          : "text-white/70 hover:bg-white/10 hover:text-white"
      }`}
    >
      <span className="material-symbols-rounded text-base">{icon}</span>
      {label}
    </button>
  );
}

function AuthGate() {
  const { user, loading: authLoading } = useAuth();
  const { status, expiredAt } = usePremium();

  if (authLoading) return <LoadingScreen />;
  if (!user) return <LoginScreen />;
  if (status === "loading") return <LoadingScreen />;
  if (status === "inactive" || status === "expired") {
    return <PaywallScreen expiredAt={expiredAt} reason={status} />;
  }

  return (
    <SettingsProvider>
      <AppInner />
    </SettingsProvider>
  );
}

function LoadingScreen() {
  return (
    <main className="flex min-h-screen items-center justify-center bg-surface-0">
      <div className="flex flex-col items-center gap-3 text-white/60">
        <span className="material-symbols-rounded animate-spin text-4xl text-shopee-400">
          progress_activity
        </span>
        <span className="text-sm">Đang tải...</span>
      </div>
    </main>
  );
}

function App() {
  return (
    <AuthProvider>
      <AuthGate />
    </AuthProvider>
  );
}

export default App;
