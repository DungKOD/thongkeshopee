import {
  lazy,
  Suspense,
  useCallback,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  useTransition,
} from "react";
import { AppTabsNav } from "./components/AppTabsNav";
import { DateRangePopover } from "./components/DateRangePopover";
import { SubIdSuggestionDropdown } from "./components/SubIdSuggestionDropdown";
import { SplashScreen } from "./components/SplashScreen";
import { LazyDayBlock } from "./components/LazyDayBlock";
import { OverviewTab, clearOverviewInsightsCache } from "./components/OverviewTab";
import { SubIdTimelineBlock } from "./components/SubIdTimelineBlock";
import { ManualEntryDialog } from "./components/ManualEntryDialog";
import { SettingsDialog } from "./components/SettingsDialog";
import { TokenManagerDialog } from "./components/TokenManagerDialog";
import { RulesDialog } from "./components/RulesDialog";
import { PendingChangesBar } from "./components/PendingChangesBar";
import { ImportPreviewDialog } from "./components/ImportPreviewDialog";

// Lazy-load các tab/dialog nặng (video API, calculator, html-to-image) — chỉ
// fetch chunk khi user thực sự mở, giảm initial bundle.
const DownloadVideoPage = lazy(() =>
  import("./components/DownloadVideoPage").then((m) => ({
    default: m.DownloadVideoPage,
  })),
);
const UploadVideoPage = lazy(() =>
  import("./components/UploadVideoPage").then((m) => ({
    default: m.UploadVideoPage,
  })),
);
const CampaignBatchPage = lazy(() =>
  import("./components/CampaignBatchPage").then((m) => ({
    default: m.CampaignBatchPage,
  })),
);
const ShopeeProductPage = lazy(() =>
  import("./components/ShopeeProductPage").then((m) => ({
    default: m.ShopeeProductPage,
  })),
);
const ShopeeAffiliatePage = lazy(() =>
  import("./components/ShopeeAffiliatePage").then((m) => ({
    default: m.ShopeeAffiliatePage,
  })),
);
const OtherToolsPage = lazy(() =>
  import("./components/OtherToolsPage").then((m) => ({
    default: m.OtherToolsPage,
  })),
);
const SmartCalculator = lazy(() =>
  import("./components/SmartCalculator").then((m) => ({
    default: m.SmartCalculator,
  })),
);
import {
  makeFilterKey,
  useDbStats,
  type DaysFilter,
} from "./hooks/useDbStats";
import {
  LOAD_MORE_STEP,
  prevMonthRange,
  currentMonthRange,
  useFilterMode,
} from "./hooks/useFilterMode";
import { SettingsProvider, useSettings } from "./hooks/useSettings";
import { useToast } from "./components/ToastProvider";
import { commitCsvBatch, previewCsvBatch } from "./lib/dbImport";
import type { PreviewBatch } from "./lib/dbImport";
import { FbHierarchyImportDialog } from "./components/FbHierarchyImportDialog";
import type { UiDay, UiRow } from "./types";
import { fmtDate, fmtInt } from "./formulas";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { AccountProvider, useAccounts } from "./contexts/AccountContext";
import { AccountFilterDropdown } from "./components/AccountFilterDropdown";
import { AccountManagerDialog } from "./components/AccountManagerDialog";
import { ImportAccountPickerDialog } from "./components/ImportAccountPickerDialog";
import { ScrollToTopButton } from "./components/ScrollToTopButton";
import { DayScreenshotDialog } from "./components/DayScreenshotDialog";
import {
  captureElementToBlob,
  prefetchFontEmbedCSS,
} from "./lib/screenshot";
import { UpdatesDropdown } from "./components/UpdatesDropdown";
import { WorkspaceBadge } from "./components/WorkspaceBadge";
import { LoginScreen } from "./components/LoginScreen";
import { SessionKickedDialog } from "./components/SessionKickedDialog";
import { PremiumLockedScreen } from "./components/PremiumLockedScreen";
import { isPremiumActive } from "./lib/userProfile";
import { UserMenu } from "./components/UserMenu";
import { DevCredit } from "./components/DevCredit";
import "./App.css";

function LazyTabFallback() {
  return (
    <div className="mx-auto flex max-w-xl flex-col items-center gap-3 py-16 text-center text-white/60">
      <span className="material-symbols-rounded animate-spin text-4xl text-shopee-400">
        sync
      </span>
      <span className="text-sm">Đang tải...</span>
    </div>
  );
}

type AppTab =
  | "stats"
  | "overview"
  | "download"
  | "upload"
  | "bulkcamp"
  | "shopee"
  | "smartlink"
  | "other";

function AppInner() {
  const { signOut: authSignOut } = useAuth();
  const [activeTab, setActiveTabRaw] = useState<AppTab>("stats");

  // useTransition: React 19 idiomatic cho non-urgent UI update. `isTabPending`
  // = true trong khi React render tab mới ở background priority → có thể show
  // visual cue ngay (giữ tab cũ + indicator) thay vì freeze chờ render xong.
  const [isTabPending, startTabTransition] = useTransition();
  const setActiveTab = useCallback(
    (tab: AppTab) => {
      startTabTransition(() => setActiveTabRaw(tab));
    },
    [startTabTransition],
  );

  // Track lazy tabs đã từng mount (Download/Upload chunks). Giữ mounted khi
  // user switch sang tab khác → không phải refetch chunk + không re-trigger
  // useEffect khi quay lại.
  // Mount-once + CSS-hide pattern: tab đã visit ở lại trong DOM, switch tab
  // chỉ là toggle CSS → instant không cần React mount lại. Stats là initial
  // active tab → seed sẵn vào Set để render từ frame đầu.
  // Overview cũng lazy-then-persistent: lần đầu click có cost (Recharts +
  // KPI + 5 BE insights queries), nhưng switch sau đó instant.
  const [mountedLazyTabs, setMountedLazyTabs] = useState<Set<AppTab>>(
    () => new Set<AppTab>(["stats"]),
  );
  useEffect(() => {
    if (
      activeTab === "download" ||
      activeTab === "upload" ||
      activeTab === "bulkcamp" ||
      activeTab === "shopee" ||
      activeTab === "smartlink" ||
      activeTab === "other" ||
      activeTab === "overview"
    ) {
      setMountedLazyTabs((prev) => {
        if (prev.has(activeTab)) return prev;
        const next = new Set(prev);
        next.add(activeTab);
        return next;
      });
    }
  }, [activeTab]);

  // Prefetch lazy chunks trong idle time sau mount → click tab lần đầu vẫn
  // instant (chunk đã trong cache khi React.lazy resolve).
  useEffect(() => {
    const schedule =
      typeof requestIdleCallback === "function"
        ? requestIdleCallback
        : (cb: () => void) => setTimeout(cb, 1500);
    const handle = schedule(() => {
      void import("./components/DownloadVideoPage");
      void import("./components/UploadVideoPage");
      void import("./components/CampaignBatchPage");
      void import("./components/ShopeeProductPage");
      void import("./components/ShopeeAffiliatePage");
      void import("./components/OtherToolsPage");
      void import("./components/SmartCalculator");
    });
    return () => {
      if (typeof cancelIdleCallback === "function" && typeof handle === "number") {
        cancelIdleCallback(handle);
      } else if (typeof handle === "number") {
        clearTimeout(handle);
      }
    };
  }, []);

  const statsFilter = useFilterMode("stats");
  const overviewFilter = useFilterMode("overview");
  const activeFilter = activeTab === "overview" ? overviewFilter : statsFilter;

  const filterMode = activeFilter.mode;
  const setFilterMode = activeFilter.setMode;
  const setRecentDays = activeFilter.setRecent;
  const setPrevMonth = activeFilter.setPrevMonth;
  const setCurrentMonth = activeFilter.setCurrentMonth;
  const setAllTime = activeFilter.setAllTime;
  const setRange = activeFilter.setRange;

  const [subIdQuery, setSubIdQuery] = useState("");
  const [selectedSubId, setSelectedSubId] = useState<string | null>(null);
  // Defer suggestion filtering: allSubIds có thể 10k+ → mỗi keystroke filter
  // sync block input. Deferred query làm input update tức thì, suggestions
  // catch-up ở background.
  const deferredSubIdQuery = useDeferredValue(subIdQuery);

  const {
    filter: accountFilter,
    activeAccountId,
    refresh: refreshAccounts,
  } = useAccounts();

  // pageLimit cap số ngày BE trả cho stats tab (overview tab: undefined → full).
  // Đảm bảo first paint nhanh kể cả "Từ trước đến nay" với DB lớn.
  const pageLimit = activeFilter.pageLimit;
  const effectiveFilter = useMemo<DaysFilter>(() => {
    const base: DaysFilter = (() => {
      if (filterMode.type === "recent") return { limit: filterMode.count };
      if (filterMode.type === "range") {
        const { from, to } = filterMode;
        const limitField = pageLimit !== undefined ? { limit: pageLimit } : {};
        if (!from && !to) return { ...limitField };
        const a = from || to;
        const b = to || from;
        const [lo, hi] = a <= b ? [a, b] : [b, a];
        return { fromDate: lo, toDate: hi, ...limitField };
      }
      // "all" mode
      return pageLimit !== undefined ? { limit: pageLimit } : {};
    })();
    return {
      ...base,
      ...(selectedSubId ? { subIdFilter: selectedSubId } : {}),
      accountFilter,
    };
  }, [filterMode, selectedSubId, accountFilter, pageLimit]);

  const {
    days,
    daysFilterKey,
    overview,
    referrers,
    loading,
    refreshing,
    progress,
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
  } = useDbStats({ filter: effectiveFilter });

  // useDeferredValue (React 18+/19 idiomatic): khi data mới về (filter switch,
  // refetch), `days` đổi ngay nhưng `deferredDays` còn giữ giá trị cũ — React
  // render data mới ở background priority. Mọi consumer downstream (DayBlock
  // list, OverviewTab analytics, SubIdTimeline) đọc từ deferredDays → KHÔNG
  // block UI thread khi swap data.
  //
  // Khi render mới xong, deferredDays catch-up = days. Trong khoảng đó,
  // `isDataStale` = true → có thể show indicator nhỏ (đã có refreshing pill).
  const deferredDays = useDeferredValue(days);
  // CRITICAL: defer cùng `daysFilterKey` để pair với `deferredDays`. Nếu chỉ
  // dùng `daysFilterKey` urgent thì khi useDbStats fetch xong Overview data
  // (setDays + setDaysFilterKey cùng commit), `daysFilterKey` cập nhật ngay
  // urgent → isDaysForOverview = true, nhưng `deferredDays` còn LAG ở Stats
  // data → overviewDaysProp = Stats data → flash 1 frame "1 ngày" trước khi
  // deferredDays catch up Overview. Pairing đảm bảo cả 2 cùng lag → gate
  // chỉ "mở" khi deferredDays đã thực sự là Overview data.
  const deferredDaysFilterKey = useDeferredValue(daysFilterKey);
  const isDataStale = deferredDays !== days;

  // Compute expected DaysFilter cho riêng Overview tab (regardless activeTab).
  // Khi Stats active, effectiveFilter = Stats filter → useDbStats fetch Stats
  // data. Overview tab sẽ stuck với "live" days = Stats data → analytics tính
  // sai (vd Stats 1 ngày → Overview chart show 1 ngày). Bằng cách so sánh
  // deferredDaysFilterKey với key kỳ vọng của Overview, ta biết days hiện tại
  // có thuộc Overview filter không, và freeze props nếu không khớp.
  const overviewExpectedFilter = useMemo<DaysFilter>(() => {
    const overviewMode = overviewFilter.mode;
    const overviewPageLimit = overviewFilter.pageLimit;
    const base: DaysFilter = (() => {
      if (overviewMode.type === "recent") return { limit: overviewMode.count };
      if (overviewMode.type === "range") {
        const { from, to } = overviewMode;
        const limitField =
          overviewPageLimit !== undefined ? { limit: overviewPageLimit } : {};
        if (!from && !to) return { ...limitField };
        const a = from || to;
        const b = to || from;
        const [lo, hi] = a <= b ? [a, b] : [b, a];
        return { fromDate: lo, toDate: hi, ...limitField };
      }
      return overviewPageLimit !== undefined ? { limit: overviewPageLimit } : {};
    })();
    return {
      ...base,
      ...(selectedSubId ? { subIdFilter: selectedSubId } : {}),
      accountFilter,
    };
  }, [overviewFilter.mode, overviewFilter.pageLimit, selectedSubId, accountFilter]);
  const overviewExpectedFilterKey = useMemo(
    () => makeFilterKey(overviewExpectedFilter),
    [overviewExpectedFilter],
  );
  // Compare DEFERRED key (paired với deferredDays) → đảm bảo cả 2 đại diện
  // cùng 1 commit. Tránh race urgent-key + lagged-days gây flash data sai.
  const isDaysForOverview = deferredDaysFilterKey === overviewExpectedFilterKey;

  // Invalidate insights cache khi metadata `overview` đổi reference — chỉ xảy
  // ra sau mutation (save/import/revert/refetch button). Filter switch KHÔNG
  // bump `overview` → cache vẫn hit, tab switch instant. Compare sync trong
  // render để cache clear TRƯỚC khi OverviewTab render với data mới — tránh
  // flash 1 frame data cũ. Initial mount: ref khớp với initial overview → no-op.
  const lastOverviewRef = useRef(overview);
  if (lastOverviewRef.current !== overview) {
    lastOverviewRef.current = overview;
    clearOverviewInsightsCache();
  }

  // Frozen snapshot cho Overview tab props. Tracking:
  //  - overviewDaysRef: data days khi useDbStats LAST trả về data thuộc về
  //    Overview's filter (isDaysForOverview === true).
  //  - overviewFilterRef: filter tương ứng.
  // Khi Stats active → useDbStats fetch Stats data → daysFilterKey !== Overview
  // expected → isDaysForOverview=false → OverviewTab giữ ref cũ → analytics +
  // BE invokes KHÔNG re-fire với Stats data → không flash 1-ngày trong chart
  // khi user chuyển sang Overview. Khi data Overview thực sự load xong, ref
  // update + OverviewTab nhận data mới đúng 1 lần.
  const overviewDaysRef = useRef<UiDay[]>([]);
  const overviewFilterRef = useRef<DaysFilter>(overviewExpectedFilter);
  useEffect(() => {
    if (isDaysForOverview) {
      overviewDaysRef.current = deferredDays;
      overviewFilterRef.current = overviewExpectedFilter;
    }
  }, [isDaysForOverview, deferredDays, overviewExpectedFilter]);
  const overviewDaysProp = isDaysForOverview
    ? deferredDays
    : overviewDaysRef.current;
  const overviewFilterProp = isDaysForOverview
    ? overviewExpectedFilter
    : overviewFilterRef.current;

  const {
    settings,
    setClickSource,
    registerSources,
    setProfitFee,
    setSubIdMatchMode,
    setVideoWatermark,
    setVideoWatermarkAntiTheft,
    setAiContent,
    hydrated: settingsHydrated,
  } = useSettings();

  const { showToast } = useToast();

  useEffect(() => {
    if (!settingsHydrated) return;
    if (referrers.length > 0) registerSources(referrers);
  }, [settingsHydrated, referrers, registerSources]);

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const [accountMgrOpen, setAccountMgrOpen] = useState(false);
  const [tokenManagerOpen, setTokenManagerOpen] = useState(false);
  const [calcOpen, setCalcOpen] = useState<boolean>(() => {
    try {
      return JSON.parse(localStorage.getItem("smartcalc:open") ?? "false");
    } catch {
      return false;
    }
  });
  useEffect(() => {
    try {
      localStorage.setItem("smartcalc:open", JSON.stringify(calcOpen));
    } catch {
      /* quota */
    }
  }, [calcOpen]);
  const [subIdFocused, setSubIdFocused] = useState(false);
  const subIdInputRef = useRef<HTMLInputElement>(null);

  const overviewCaptureRef = useRef<HTMLDivElement | null>(null);
  const [overviewCapturing, setOverviewCapturing] = useState(false);
  const [overviewScreenshotBlob, setOverviewScreenshotBlob] =
    useState<Blob | null>(null);
  const handleOverviewScreenshot = async () => {
    if (!overviewCaptureRef.current || overviewCapturing) return;
    setOverviewCapturing(true);
    try {
      const target = (overviewCaptureRef.current
        .firstElementChild as HTMLElement | null) ??
        overviewCaptureRef.current;
      const blob = await captureElementToBlob(target, {
        backgroundColor: "#121212",
      });
      setOverviewScreenshotBlob(blob);
    } catch (e) {
      console.error("overview screenshot failed", e);
      showToast({
        message: `Chụp ảnh thất bại: ${(e as Error).message ?? e}`,
        duration: 5000,
      });
    } finally {
      setOverviewCapturing(false);
    }
  };
  const [entryDialog, setEntryDialog] = useState<{
    date: string;
    row?: UiRow | null;
  } | null>(null);
  const [previewBatch, setPreviewBatch] = useState<PreviewBatch | null>(null);
  const [importAccountId, setImportAccountId] = useState<string | null>(null);
  const [accountPickerOpen, setAccountPickerOpen] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [fbHierarchyOpen, setFbHierarchyOpen] = useState(false);

  const handleImportClick = () => setAccountPickerOpen(true);

  const handleAccountPicked = (accountId: string) => {
    setImportAccountId(accountId);
    setAccountPickerOpen(false);
    setTimeout(() => fileInputRef.current?.click(), 0);
  };

  const handleFilesSelected = async (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const files = Array.from(e.target.files ?? []);
    e.target.value = "";
    if (files.length === 0) {
      setImportAccountId(null);
      return;
    }

    try {
      const batch = await previewCsvBatch(files);
      setPreviewBatch(batch);
    } catch (err) {
      setImportAccountId(null);
      showToast({
        message: (err as Error).message ?? String(err),
        duration: 10000,
      });
    }
  };

  const handleConfirmImport = useCallback(async (
    fbTaxRates: Record<number, number>,
  ) => {
    if (!previewBatch || importAccountId === null) return;
    const results = await commitCsvBatch(
      previewBatch,
      importAccountId,
      fbTaxRates,
    );
    await refreshAccounts();
    const totalNew = results.reduce((a, r) => a + r.inserted, 0);
    const totalReplace = results.reduce((a, r) => a + r.duplicated, 0);
    const totalSkipped = results.reduce((a, r) => a + r.skipped, 0);
    const totalMcnMismatch = results.reduce(
      (a, r) => a + (r.mcnMismatchCount ?? 0),
      0,
    );
    const dateRange = (() => {
      if (results.length === 0) return "";
      const from = results.map((r) => r.dayDateFrom).sort()[0];
      const to = results.map((r) => r.dayDateTo).sort().reverse()[0];
      return from === to ? fmtDate(from) : `${fmtDate(from)} → ${fmtDate(to)}`;
    })();
    setPreviewBatch(null);
    await refetch();
    showToast({
      message:
        results.length === 0
          ? "Tất cả file đã import trước đó — không có gì để commit"
          : `Đã import ${dateRange}: ${fmtInt(totalNew)} dòng mới${
              totalReplace > 0 ? `, ${fmtInt(totalReplace)} replace` : ""
            }${totalSkipped > 0 ? `, ${fmtInt(totalSkipped)} skip` : ""}`,
      duration: 5000,
    });
    if (totalMcnMismatch > 0) {
      showToast({
        message: `Cảnh báo: ${fmtInt(totalMcnMismatch)} đơn lệch công thức MCN (net ≠ total - fee > 0.5đ). Check lại export Shopee.`,
        duration: 8000,
      });
    }
  }, [previewBatch, refetch, showToast, importAccountId, refreshAccounts]);

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

  // Stable callbacks → memo DayBlock skip re-render khi day reference giữ
  // nguyên (cache hit). Tạo inline trong map sẽ break memo vì mỗi render
  // sinh function mới.
  const handleRowDelete = useCallback(
    (r: UiRow) => toggleRowPending(r.dayDate, r.subIds, r.accountId),
    [toggleRowPending],
  );
  const handleEditRow = useCallback(
    (r: UiRow) => setEntryDialog({ date: r.dayDate, row: r }),
    [],
  );
  const handleEditDay = useCallback(
    (date: string) => setEntryDialog({ date }),
    [],
  );

  const totalDaysInDb = overview.totalDaysCount;

  // canLoadMore: tổng quát cho 3 mode
  //  - recent + canExpand: count < tổng → tăng count theo LOAD_MORE_STEP
  //  - range/all (chỉ stats tab có pageLimit): BE trả đúng pageLimit → còn data → scroll thêm
  //  - overview tab (pageLimit undefined): không scroll, KPI full data
  const canLoadMore =
    (filterMode.type === "recent" &&
      filterMode.canExpand &&
      filterMode.count < totalDaysInDb) ||
    (pageLimit !== undefined &&
      (filterMode.type === "range" || filterMode.type === "all") &&
      deferredDays.length >= pageLimit &&
      deferredDays.length < totalDaysInDb);

  const prevMonth = useMemo(() => prevMonthRange(), []);
  const isPrevMonthActive =
    filterMode.type === "range" &&
    filterMode.from === prevMonth.from &&
    filterMode.to === prevMonth.to;

  const currentMonth = useMemo(() => currentMonthRange(), []);
  const isCurrentMonthActive =
    filterMode.type === "range" &&
    filterMode.from === currentMonth.from &&
    filterMode.to === currentMonth.to;

  const { dateFrom, dateTo } = useMemo<{ dateFrom: string; dateTo: string }>(() => {
    if (filterMode.type === "range") {
      return { dateFrom: filterMode.from, dateTo: filterMode.to };
    }
    if (filterMode.type === "all") {
      return {
        dateFrom: overview.oldestDate ?? "",
        dateTo: overview.newestDate ?? "",
      };
    }
    if (
      filterMode.type === "recent" &&
      !filterMode.canExpand &&
      deferredDays.length > 0
    ) {
      return {
        dateFrom: deferredDays[deferredDays.length - 1].date,
        dateTo: deferredDays[0].date,
      };
    }
    return { dateFrom: "", dateTo: "" };
  }, [filterMode, deferredDays, overview.oldestDate, overview.newestDate]);

  // Suggestions: filter có early-exit + cap 200 result. allSubIds có thể 10k+
  // → .filter() chuẩn (toLowerCase + split + some) tốn 30-100ms blocking, mỗi
  // keystroke gây giật input dù đã useDeferredValue. Dropdown chỉ render top
  // ~15 nên cap 200 đủ rộng cho mọi case khả thi. Empty query: trả full list
  // (dropdown tự virtualize/limit hiển thị).
  const suggestions = useMemo(() => {
    const q = deferredSubIdQuery.toLowerCase().trim();
    if (!q) return overview.allSubIds;
    const SUGGESTIONS_CAP = 200;
    const out: string[] = [];
    for (const s of overview.allSubIds) {
      const lower = s.toLowerCase();
      // Inline check thay vì .split().some() — không tạo Array trung gian.
      let matches = false;
      let partStart = 0;
      for (let i = 0; i <= lower.length; i++) {
        if (i === lower.length || lower.charCodeAt(i) === 45 /* '-' */) {
          if (i > partStart && lower.startsWith(q, partStart)) {
            matches = true;
            break;
          }
          partStart = i + 1;
        }
      }
      if (matches) {
        out.push(s);
        if (out.length >= SUGGESTIONS_CAP) break;
      }
    }
    return out;
  }, [overview.allSubIds, deferredSubIdQuery]);

  const clearSubId = () => {
    setSelectedSubId(null);
    setSubIdQuery("");
    requestAnimationFrame(() => {
      subIdInputRef.current?.focus();
      setSubIdFocused(true);
    });
  };

  const extendPage = activeFilter.extendPage;
  const loadMoreRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!canLoadMore) return;
    const el = loadMoreRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (!entries[0]?.isIntersecting) return;
        // recent + canExpand → tăng count (giữ semantics cũ).
        // range/all → extendPage tăng pageLimit (refetch với limit lớn hơn).
        if (filterMode.type === "recent" && filterMode.canExpand) {
          setFilterMode((m) =>
            m.type === "recent" && m.canExpand
              ? { ...m, count: m.count + LOAD_MORE_STEP }
              : m,
          );
        } else {
          extendPage(LOAD_MORE_STEP);
        }
      },
      { rootMargin: "200px" },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [canLoadMore, filterMode, setFilterMode, extendPage]);

  return (
    <main className="min-h-full bg-surface-0 pb-24">
      <header className="sticky top-0 z-30 bg-gradient-to-r from-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center justify-between px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="material-symbols-rounded text-3xl text-white">
              analytics
            </span>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-xl font-semibold tracking-tight text-white">
                  Shopee Affiliate Tracker
                </h1>
                <span className="rounded-md bg-white/15 px-1.5 py-0.5 font-mono text-[11px] font-semibold tabular-nums text-white/90">
                  v{__APP_VERSION__}
                </span>
                <WorkspaceBadge />
              </div>
              <p className="text-xs text-white/70">
                Data từ database — manual override luôn ưu tiên raw CSV
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <UpdatesDropdown
              currentVersion={__APP_VERSION__}
              repo="DungKOD/thongkeshopee"
              limit={10}
            />
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
              onClick={() => setCalcOpen((o) => !o)}
              className={`btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white transition-colors ${
                calcOpen ? "bg-white/20" : "hover:bg-white/10 active:bg-white/20"
              }`}
              title={calcOpen ? "Đóng máy tính" : "Mở máy tính"}
              aria-label="Máy tính thông minh"
              aria-pressed={calcOpen}
            >
              <span className="material-symbols-rounded">calculate</span>
            </button>
            <button
              onClick={() => setAccountMgrOpen(true)}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
              title="Quản lý TK Shopee"
              aria-label="Quản lý TK Shopee"
            >
              <span className="material-symbols-rounded">manage_accounts</span>
            </button>
            <button
              onClick={() => setTokenManagerOpen(true)}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
              title="Token Manager — quản lý token FB Pages / Ad Accounts / Shopee"
              aria-label="Token Manager"
            >
              <span className="material-symbols-rounded">key</span>
            </button>
            <button
              onClick={() => setSettingsOpen(true)}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
              title="Cài đặt"
              aria-label="Cài đặt"
            >
              <span className="material-symbols-rounded">settings</span>
            </button>
            <UserMenu onRequestSignOut={() => void authSignOut()} />
            {activeTab === "stats" && (
              <>
                <button
                  onClick={() => setFbHierarchyOpen(true)}
                  className="btn-ripple flex items-center gap-2 rounded-lg border border-violet-500/60 px-3 py-2 text-sm font-medium text-violet-300 hover:bg-violet-500/10 active:bg-violet-500/20"
                  title="Import FB Ads — CSV hoặc Excel (.xlsx). Format 3 cấp: chiến dịch → nhóm → quảng cáo."
                >
                  <span className="material-symbols-rounded text-base">
                    campaign
                  </span>
                  Import FB
                </button>
                <button
                  onClick={handleImportClick}
                  className="btn-ripple flex items-center gap-2 rounded-lg border border-white/50 px-4 py-2 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
                  title="Import Shopee — CSV click hoặc hoa hồng"
                >
                  <span className="material-symbols-rounded text-base">
                    upload_file
                  </span>
                  Import Shopee
                </button>
              </>
            )}
          </div>
        </div>

        <AppTabsNav activeTab={activeTab} onChange={setActiveTab} />
      </header>

      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,text/csv"
        multiple
        className="hidden"
        onChange={handleFilesSelected}
      />

      {/* SWR refresh bar: mỏng, ngay dưới header. Không chiếm chỗ, không
          xô layout. Chỉ hiển thị khi đang background refresh (đã có data). */}
      {refreshing && !loading && (
        <div
          className="pointer-events-none sticky top-[92px] z-[40] -mb-1 h-0.5 overflow-hidden bg-shopee-500/15"
          role="progressbar"
          aria-valuemin={0}
          aria-valuemax={progress.total || 1}
          aria-valuenow={progress.done}
          aria-label={progress.label || "Đang cập nhật"}
        >
          <div
            className="h-full bg-shopee-400 shadow-[0_0_8px_rgba(238,77,45,0.6)] transition-[width] duration-300 ease-out"
            style={{
              width: `${
                progress.total > 0
                  ? Math.max((progress.done / progress.total) * 100, 8)
                  : 30
              }%`,
            }}
          />
        </div>
      )}

      <div className="p-6">
        {/* Lazy tabs: mount-once, hide bằng CSS khi inactive. Tránh
            re-fetch chunk + reset state mỗi lần user switch tab. */}
        {mountedLazyTabs.has("download") && (
          <div className={activeTab === "download" ? "" : "hidden"}>
            <Suspense fallback={<LazyTabFallback />}>
              <DownloadVideoPage />
            </Suspense>
          </div>
        )}
        {mountedLazyTabs.has("upload") && (
          <div className={activeTab === "upload" ? "" : "hidden"}>
            <Suspense fallback={<LazyTabFallback />}>
              <UploadVideoPage />
            </Suspense>
          </div>
        )}
        {mountedLazyTabs.has("bulkcamp") && (
          <div className={activeTab === "bulkcamp" ? "" : "hidden"}>
            <Suspense fallback={<LazyTabFallback />}>
              <CampaignBatchPage />
            </Suspense>
          </div>
        )}
        {mountedLazyTabs.has("shopee") && (
          <div className={activeTab === "shopee" ? "" : "hidden"}>
            <Suspense fallback={<LazyTabFallback />}>
              <ShopeeProductPage />
            </Suspense>
          </div>
        )}
        {mountedLazyTabs.has("smartlink") && (
          <div className={activeTab === "smartlink" ? "" : "hidden"}>
            <Suspense fallback={<LazyTabFallback />}>
              <ShopeeAffiliatePage />
            </Suspense>
          </div>
        )}
        {mountedLazyTabs.has("other") && (
          <div className={activeTab === "other" ? "" : "hidden"}>
            <Suspense fallback={<LazyTabFallback />}>
              <OtherToolsPage />
            </Suspense>
          </div>
        )}
        {activeTab === "download" ||
        activeTab === "upload" ||
        activeTab === "bulkcamp" ||
        activeTab === "shopee" ||
        activeTab === "smartlink" ||
        activeTab === "other" ? (
          // Lazy chunk đang tải lần đầu → fallback nằm trong Suspense ở trên.
          // Block stats/overview rendering hoàn toàn trong khi xem lazy tabs.
          null
        ) : loading ? (
          <div className="mx-auto flex w-full max-w-md flex-col items-center gap-4 py-16 text-center text-white/70">
            <span className="material-symbols-rounded animate-spin text-4xl text-shopee-400">
              sync
            </span>
            <div className="w-full">
              <div className="mb-2 flex items-center justify-between text-xs">
                <span className="truncate text-white/70">
                  {progress.label || "Đang tải data..."}
                </span>
                <span className="ml-2 shrink-0 font-mono tabular-nums text-shopee-300">
                  {progress.total > 0
                    ? Math.round((progress.done / progress.total) * 100)
                    : 0}
                  %
                </span>
              </div>
              <div
                className="h-2 w-full overflow-hidden rounded-full bg-surface-4"
                role="progressbar"
                aria-valuemin={0}
                aria-valuemax={progress.total || 1}
                aria-valuenow={progress.done}
                aria-label={progress.label || "Đang tải data..."}
              >
                <div
                  className="h-full rounded-full bg-shopee-500 transition-[width] duration-300 ease-out"
                  style={{
                    width: `${
                      progress.total > 0
                        ? (progress.done / progress.total) * 100
                        : 0
                    }%`,
                  }}
                />
              </div>
              {progress.total > 0 && (
                <div className="mt-1 text-right font-mono text-[11px] tabular-nums text-white/40">
                  {progress.done} / {progress.total}
                </div>
              )}
            </div>
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
        ) : totalDaysInDb === 0 ? (
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
                onClick={() => setFbHierarchyOpen(true)}
                className="btn-ripple flex items-center gap-2 rounded-lg border border-violet-500/60 px-5 py-2.5 text-sm font-medium text-violet-300 hover:bg-violet-500/10 active:bg-violet-500/20"
                title="Import FB Ads — CSV hoặc Excel (.xlsx). Format 3 cấp: chiến dịch → nhóm → quảng cáo."
              >
                <span className="material-symbols-rounded text-base">
                  campaign
                </span>
                Import FB
              </button>
              <button
                onClick={handleImportClick}
                className="btn-ripple flex items-center gap-2 rounded-lg border border-white/50 px-5 py-2.5 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
                title="Import Shopee — CSV click hoặc hoa hồng"
              >
                <span className="material-symbols-rounded text-base">
                  upload_file
                </span>
                Import Shopee
              </button>
            </div>
          </div>
        ) : (
          <>
            <div className="sticky top-[92px] z-20 -mx-6 mb-4 border-b border-surface-8 bg-surface-0/95 px-6 py-3 backdrop-blur">
              <div className="flex items-start gap-3 rounded-xl border border-surface-8 bg-surface-2 px-4 py-2 text-sm">
               <div className="flex flex-1 flex-wrap items-center gap-x-3 gap-y-2 min-w-0">
                <DateRangePopover
                  mode={filterMode}
                  isPrevMonthActive={isPrevMonthActive}
                  isCurrentMonthActive={isCurrentMonthActive}
                  setRecent={setRecentDays}
                  setPrevMonth={setPrevMonth}
                  setCurrentMonth={setCurrentMonth}
                  setAllTime={setAllTime}
                  setRange={setRange}
                />

                <span className="hidden h-6 w-px bg-surface-8 md:inline-block" />

                <AccountFilterDropdown />

                <span className="hidden h-6 w-px bg-surface-8 md:inline-block" />

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
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        const pick = suggestions[0] ?? subIdQuery.trim();
                        if (!pick) return;
                        setSelectedSubId(pick);
                        setSubIdQuery("");
                        setSubIdFocused(false);
                        subIdInputRef.current?.blur();
                      } else if (e.key === "Escape") {
                        setSubIdFocused(false);
                        subIdInputRef.current?.blur();
                      }
                    }}
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
                    title={`Tổng ${overview.allSubIds.length} sub_id trong DB`}
                  >
                    {overview.allSubIds.length}
                  </span>
                  <SubIdSuggestionDropdown
                    anchorRef={subIdInputRef}
                    open={subIdFocused && suggestions.length > 0}
                    suggestions={suggestions}
                    onPick={(s) => {
                      setSelectedSubId(s);
                      setSubIdQuery("");
                      setSubIdFocused(false);
                    }}
                  />
                </div>

               </div>

                <div className="flex shrink-0 items-center gap-2 pt-1">
                  <button
                    type="button"
                    onClick={() => {
                      // Manual reload: bust cả LRU cache trong useDbStats
                      // (đã clear bên trong refetch) lẫn insights cache module-
                      // level của OverviewTab. Sau khi overview reference đổi,
                      // sync invalidator ở trên cũng sẽ chạy — clear sớm ở
                      // đây để user click thấy phản hồi ngay (loading pill).
                      clearOverviewInsightsCache();
                      void refetch();
                    }}
                    disabled={refreshing}
                    title="Tải lại dữ liệu (clear cache + refetch)"
                    className="btn-ripple flex items-center gap-1 rounded-lg border border-surface-8 bg-surface-2 px-2.5 py-1 text-xs font-medium text-white/80 hover:bg-surface-4 hover:text-white disabled:opacity-50"
                  >
                    <span
                      className={`material-symbols-rounded text-base text-shopee-400 ${refreshing ? "animate-spin" : ""}`}
                    >
                      {refreshing ? "sync" : "refresh"}
                    </span>
                    <span className="hidden sm:inline">
                      {refreshing ? "Đang tải..." : "Tải lại"}
                    </span>
                  </button>
                  {activeTab === "overview" && (
                    <button
                      type="button"
                      onClick={() => void handleOverviewScreenshot()}
                      onMouseEnter={() => prefetchFontEmbedCSS()}
                      disabled={overviewCapturing}
                      title="Chụp ảnh tab Tổng quan"
                      className="btn-ripple flex items-center gap-1 rounded-lg border border-surface-8 bg-surface-2 px-2.5 py-1 text-xs font-medium text-white/80 hover:bg-surface-4 hover:text-white disabled:opacity-50"
                    >
                      <span
                        className={`material-symbols-rounded text-base text-shopee-400 ${overviewCapturing ? "animate-spin" : ""}`}
                      >
                        {overviewCapturing ? "sync" : "photo_camera"}
                      </span>
                      <span className="hidden sm:inline">
                        {overviewCapturing ? "Đang chụp..." : "Chụp ảnh"}
                      </span>
                    </button>
                  )}
                  <span className="whitespace-nowrap rounded-full bg-shopee-900/40 px-2 py-0.5 text-xs font-medium text-shopee-300">
                    {deferredDays.length} / {totalDaysInDb} ngày
                    {canLoadMore && " · scroll để xem thêm"}
                  </span>
                  {(refreshing || isDataStale || isTabPending) && (
                    <span
                      className="flex items-center gap-1 whitespace-nowrap rounded-full bg-shopee-500/15 px-2 py-0.5 text-xs font-medium text-shopee-300"
                      title={isTabPending ? "Đang chuyển tab" : progress.label}
                    >
                      <span className="material-symbols-rounded animate-spin text-sm">
                        sync
                      </span>
                      {isTabPending ? "Đang chuyển..." : "Đang cập nhật"}
                    </span>
                  )}
                </div>
              </div>
            </div>
            {deferredDays.length === 0 ? (
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
                {/* Stats panel — always mounted khi đang ở group stats/overview.
                    Tab switch Overview → Stats giờ chỉ là CSS toggle (instant)
                    thay vì React unmount Overview + mount LazyDayBlock list.
                    Khi activeTab === "overview", Stats CSS-hidden nhưng vẫn
                    giữ DOM + state → switch lại instant. */}
                <div style={{ display: activeTab === "overview" ? "none" : undefined }}>
                  {selectedSubId ? (
                    <SubIdTimelineBlock
                      subId={selectedSubId}
                      days={deferredDays}
                      pendingRowDeletes={pendingRowDeletes}
                      onToggleRowDelete={handleRowDelete}
                      onEditRow={handleEditRow}
                      accountFilter={accountFilter}
                    />
                  ) : (
                    <>
                      {/* LazyDayBlock: mỗi day chỉ mount thật khi sắp vào
                          viewport (400px margin) → N ngày = N div placeholder
                          cheap + 1-2 DayBlock thực render. */}
                      {deferredDays.map((day, idx) => (
                        <LazyDayBlock
                          key={day.date}
                          day={day}
                          eager={idx < 1}
                          pendingDayDeletes={pendingDayDeletes}
                          pendingRowDeletes={pendingRowDeletes}
                          onToggleDayDelete={toggleDayPending}
                          onToggleRowDelete={handleRowDelete}
                          onEditRow={handleEditRow}
                          onEditDay={handleEditDay}
                          accountFilter={accountFilter}
                        />
                      ))}
                    </>
                  )}
                  {canLoadMore && (
                    <div
                      ref={loadMoreRef}
                      className="mx-auto flex max-w-xs items-center justify-center gap-2 py-4 text-xs text-white/50"
                    >
                      <span className="material-symbols-rounded animate-spin text-base">
                        sync
                      </span>
                      Đang tải thêm ngày cũ hơn...
                    </div>
                  )}
                </div>

                {/* Overview panel — lazy mount lần đầu click, sau đó persist
                    với CSS hide. Lần 2+ switch Stats↔Overview chỉ là CSS toggle. */}
                {mountedLazyTabs.has("overview") && (
                  <div
                    ref={overviewCaptureRef}
                    style={{ display: activeTab === "overview" ? undefined : "none" }}
                  >
                    <OverviewTab
                      days={overviewDaysProp}
                      dateFrom={dateFrom}
                      dateTo={dateTo}
                      totalDaysInDb={totalDaysInDb}
                      currentFilter={overviewFilterProp}
                      accountFilter={accountFilter}
                    />
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
        daysCount={overview.totalDaysCount}
        productsCount={overview.totalRowsCount}
        onToggleClickSource={setClickSource}
        onSetProfitFee={setProfitFee}
        onSetSubIdMatchMode={(mode) => {
          // BE đọc match-mode từ `app_settings` mỗi query → refetch sau
          // khi update để UI re-aggregate theo mode mới.
          setSubIdMatchMode(mode);
          void refetch();
        }}
        onSetVideoWatermark={setVideoWatermark}
        onSetVideoWatermarkAntiTheft={setVideoWatermarkAntiTheft}
        onSetAiContent={setAiContent}
        onClose={() => setSettingsOpen(false)}
        onImportReverted={() => {
          void refetch();
        }}
      />

      <RulesDialog isOpen={rulesOpen} onClose={() => setRulesOpen(false)} />

      <TokenManagerDialog
        isOpen={tokenManagerOpen}
        onClose={() => setTokenManagerOpen(false)}
      />

      {entryDialog && (
        <ManualEntryDialog
          isOpen={true}
          initialDate={entryDialog.date}
          initialRow={entryDialog.row}
          shopeeAccountId={
            entryDialog.row?.shopeeAccountId ??
            entryDialog.row?.accountId ??
            (accountFilter.kind === "account" ? accountFilter.id : activeAccountId)
          }
          onSave={handleSaveEntry}
          onClose={() => setEntryDialog(null)}
        />
      )}

      <ImportAccountPickerDialog
        isOpen={accountPickerOpen}
        onPick={handleAccountPicked}
        onClose={() => setAccountPickerOpen(false)}
      />

      <FbHierarchyImportDialog
        isOpen={fbHierarchyOpen}
        onClose={() => setFbHierarchyOpen(false)}
        onImported={() => {
          void refetch();
        }}
      />

      <ImportPreviewDialog
        batch={previewBatch}
        shopeeAccountId={importAccountId}
        onConfirm={handleConfirmImport}
        onCancel={() => {
          setPreviewBatch(null);
          setImportAccountId(null);
        }}
      />

      <AccountManagerDialog
        isOpen={accountMgrOpen}
        onClose={() => setAccountMgrOpen(false)}
        onDataChanged={() => {
          void refetch();
        }}
      />

      <DayScreenshotDialog
        isOpen={!!overviewScreenshotBlob}
        blob={overviewScreenshotBlob}
        date={dateFrom || ""}
        dateLabel={
          dateFrom && dateTo
            ? dateFrom === dateTo
              ? fmtDate(dateFrom)
              : `${fmtDate(dateFrom)} → ${fmtDate(dateTo)}`
            : ""
        }
        title="Ảnh tab Tổng quan"
        defaultFileName={`thongkee-tongquan-${dateFrom || "all"}${dateTo && dateTo !== dateFrom ? `-${dateTo}` : ""}.png`}
        onClose={() => setOverviewScreenshotBlob(null)}
      />

      <ScrollToTopButton />

      <PendingChangesBar
        count={pendingCount}
        onCommit={handleCommitPending}
        onCancel={clearPending}
      />

      {calcOpen && (
        <Suspense fallback={null}>
          <SmartCalculator isOpen={calcOpen} onClose={() => setCalcOpen(false)} />
        </Suspense>
      )}
      <DevCredit variant="floating" />
    </main>
  );
}


function AuthGate() {
  const {
    user,
    loading: authLoading,
    authError,
    kickInfo,
    acknowledgeKick,
    userProfile,
    profileLoading,
  } = useAuth();

  if (authLoading) return <SplashScreen title="Đang tải..." />;

  if (authError) {
    return (
      <SplashScreen
        title="Lỗi xác thực"
        subtitle={authError}
        error
        onRetry={() => window.location.reload()}
      />
    );
  }

  // Dialog overlay portal — render NGAY khi kickInfo set, kể cả khi user
  // chưa kịp signOut. Tránh "flash" window vài chục ms user có thể tương tác
  // với app sau khi đã bị kick mà chưa kịp hiển thị dialog.
  const kickDialog = kickInfo ? (
    <SessionKickedDialog info={kickInfo} onAcknowledge={acknowledgeKick} />
  ) : null;

  if (!user) {
    return (
      <>
        <LoginScreen />
        {kickDialog}
      </>
    );
  }

  // User đã login — check premium trước khi cho vào app:
  // 1. profileLoading: đang fetch /users/{uid} lần đầu → splash.
  // 2. !isPremiumActive: chưa có quyền premium hoặc đã hết hạn → khóa.
  // 3. Active → vào app bình thường.
  if (profileLoading) {
    return <SplashScreen title="Đang kiểm tra quyền truy cập..." />;
  }

  if (!isPremiumActive(userProfile)) {
    return (
      <>
        <PremiumLockedScreen />
        {kickDialog}
      </>
    );
  }

  return (
    <>
      <SettingsProvider key={user.uid}>
        <AccountProvider>
          <AppInner />
        </AccountProvider>
      </SettingsProvider>
      {kickDialog}
    </>
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
