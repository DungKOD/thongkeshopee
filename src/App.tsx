import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { DayBlock } from "./components/DayBlock";
import { OverviewTab } from "./components/OverviewTab";
import { SubIdTimelineBlock } from "./components/SubIdTimelineBlock";
import { ManualEntryDialog } from "./components/ManualEntryDialog";
import { SettingsDialog } from "./components/SettingsDialog";
import { RulesDialog } from "./components/RulesDialog";
import { PendingChangesBar } from "./components/PendingChangesBar";
import { ImportPreviewDialog } from "./components/ImportPreviewDialog";
import { DownloadVideoPage } from "./components/DownloadVideoPage";
import { useDbStats, todayIso, type DaysFilter } from "./hooks/useDbStats";
import {
  LOAD_MORE_STEP,
  prevMonthRange,
  useFilterMode,
} from "./hooks/useFilterMode";
import { SettingsProvider, useSettings } from "./hooks/useSettings";
import { useToast } from "./components/ToastProvider";
import { commitCsvBatch, previewCsvBatch } from "./lib/dbImport";
import type { PreviewBatch } from "./lib/dbImport";
import { FbHierarchyImportDialog } from "./components/FbHierarchyImportDialog";
import type { UiRow } from "./types";
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
import { LoginScreen } from "./components/LoginScreen";
import { UserMenu } from "./components/UserMenu";
import { DevCredit } from "./components/DevCredit";
import { SmartCalculator } from "./components/SmartCalculator";
import "./App.css";

function AppInner() {
  const { signOut: authSignOut } = useAuth();
  const [activeTab, setActiveTab] = useState<
    "stats" | "overview" | "download"
  >("stats");

  const statsFilter = useFilterMode("stats");
  const overviewFilter = useFilterMode("overview");
  const activeFilter = activeTab === "overview" ? overviewFilter : statsFilter;

  const filterMode = activeFilter.mode;
  const setFilterMode = activeFilter.setMode;
  const setRecentDays = activeFilter.setRecent;
  const setPrevMonth = activeFilter.setPrevMonth;
  const setAllTime = activeFilter.setAllTime;
  const setDateFrom = activeFilter.setDateFrom;
  const setDateTo = activeFilter.setDateTo;

  const [subIdQuery, setSubIdQuery] = useState("");
  const [selectedSubId, setSelectedSubId] = useState<string | null>(null);

  const {
    filter: accountFilter,
    activeAccountId,
    refresh: refreshAccounts,
  } = useAccounts();

  const effectiveFilter = useMemo<DaysFilter>(() => {
    const base: DaysFilter = (() => {
      if (filterMode.type === "recent") return { limit: filterMode.count };
      if (filterMode.type === "range") {
        const { from, to } = filterMode;
        if (!from && !to) return {};
        const a = from || to;
        const b = to || from;
        const [lo, hi] = a <= b ? [a, b] : [b, a];
        return { fromDate: lo, toDate: hi };
      }
      return {};
    })();
    return {
      ...base,
      ...(selectedSubId ? { subIdFilter: selectedSubId } : {}),
      accountFilter,
    };
  }, [filterMode, selectedSubId, accountFilter]);

  const {
    days,
    overview,
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
  } = useDbStats({ filter: effectiveFilter });

  const {
    settings,
    setClickSource,
    registerSources,
    setProfitFee,
    setSubIdMatchMode,
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
        pixelRatio: 2,
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

  const totalDaysInDb = overview.totalDaysCount;

  const canLoadMore =
    filterMode.type === "recent" &&
    filterMode.canExpand &&
    filterMode.count < totalDaysInDb;

  const prevMonth = useMemo(() => prevMonthRange(), []);
  const isPrevMonthActive =
    filterMode.type === "range" &&
    filterMode.from === prevMonth.from &&
    filterMode.to === prevMonth.to;

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
      days.length > 0
    ) {
      return {
        dateFrom: days[days.length - 1].date,
        dateTo: days[0].date,
      };
    }
    return { dateFrom: "", dateTo: "" };
  }, [filterMode, days, overview.oldestDate, overview.newestDate]);

  const suggestions = useMemo(() => {
    const q = subIdQuery.toLowerCase().trim();
    if (!q) return overview.allSubIds;
    return overview.allSubIds.filter((s) =>
      s
        .toLowerCase()
        .split("-")
        .some((part) => part.startsWith(q)),
    );
  }, [overview.allSubIds, subIdQuery]);

  const clearSubId = () => {
    setSelectedSubId(null);
    setSubIdQuery("");
    requestAnimationFrame(() => {
      subIdInputRef.current?.focus();
      setSubIdFocused(true);
    });
  };

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
              <div className="flex items-center gap-2">
                <h1 className="text-xl font-semibold tracking-tight text-white">
                  Shopee Affiliate Tracker
                </h1>
                <span className="rounded-md bg-white/15 px-1.5 py-0.5 font-mono text-[11px] font-semibold tabular-nums text-white/90">
                  v{__APP_VERSION__}
                </span>
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

        <nav className="flex gap-1 px-6">
          <TabButton
            active={activeTab === "stats"}
            onClick={() => setActiveTab("stats")}
            icon="analytics"
            label="Thống kê"
          />
          <TabButton
            active={activeTab === "overview"}
            onClick={() => setActiveTab("overview")}
            icon="insights"
            label="Tổng quan"
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
          <div className="mx-auto flex max-w-xl flex-col items-center gap-3 py-16 text-center text-white/60">
            <span className="material-symbols-rounded animate-spin text-4xl text-shopee-400">
              sync
            </span>
            <span className="text-sm">Đang tải data...</span>
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

                <div className="flex shrink-0 items-center gap-2 pt-1">
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
                    {days.length} / {totalDaysInDb} ngày
                    {canLoadMore && " · scroll để xem thêm"}
                  </span>
                </div>
              </div>
            </div>
            {days.length === 0 ? (
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
            ) : activeTab === "overview" ? (
              <div ref={overviewCaptureRef}>
                <OverviewTab
                  days={days}
                  dateFrom={dateFrom}
                  dateTo={dateTo}
                  totalDaysInDb={totalDaysInDb}
                  currentFilter={effectiveFilter}
                  accountFilter={accountFilter}
                />
              </div>
            ) : (
              <>
                {selectedSubId ? (
                  <SubIdTimelineBlock
                    subId={selectedSubId}
                    days={days}
                    pendingRowDeletes={pendingRowDeletes}
                    onToggleRowDelete={(r) =>
                      toggleRowPending(r.dayDate, r.subIds, r.accountId)
                    }
                    onEditRow={(r) =>
                      setEntryDialog({ date: r.dayDate, row: r })
                    }
                    accountFilter={accountFilter}
                  />
                ) : (
                  days.map((day) => (
                    <DayBlock
                      key={day.date}
                      day={day}
                      pendingDayDeletes={pendingDayDeletes}
                      pendingRowDeletes={pendingRowDeletes}
                      onToggleDayDelete={toggleDayPending}
                      onToggleRowDelete={(r) =>
                        toggleRowPending(r.dayDate, r.subIds, r.accountId)
                      }
                      onEditRow={(r) =>
                        setEntryDialog({ date: r.dayDate, row: r })
                      }
                      onEditDay={(date) => setEntryDialog({ date })}
                      accountFilter={accountFilter}
                    />
                  ))
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
        onClose={() => setSettingsOpen(false)}
        onImportReverted={() => {
          void refetch();
        }}
      />

      <RulesDialog isOpen={rulesOpen} onClose={() => setRulesOpen(false)} />

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

      <SmartCalculator
        isOpen={calcOpen}
        onClose={() => setCalcOpen(false)}
      />
      <DevCredit variant="floating" />
    </main>
  );
}

interface ShortcutButtonProps {
  active?: boolean;
  onClick: () => void;
  children: ReactNode;
}

function ShortcutButton({ active, onClick, children }: ShortcutButtonProps) {
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
  const { user, loading: authLoading, authError } = useAuth();

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

  if (!user) return <LoginScreen />;

  return (
    <SettingsProvider key={user.uid}>
      <AccountProvider>
        <AppInner />
      </AccountProvider>
    </SettingsProvider>
  );
}

interface SplashScreenProps {
  title: string;
  subtitle?: string;
  error?: boolean;
  onRetry?: () => void;
}

function SplashScreen({ title, subtitle, error, onRetry }: SplashScreenProps) {
  return (
    <main className="min-h-screen bg-surface-0 px-6 text-white">
      <div className="flex flex-col items-center pt-[32vh]">
        <span
          className={`material-symbols-rounded text-7xl ${
            error ? "text-red-400" : "animate-spin text-shopee-400"
          }`}
        >
          {error ? "error" : "cloud_sync"}
        </span>
        <div className="mt-6 flex flex-col items-center gap-3">
          <h1 className="text-center text-2xl font-semibold text-white/95">
            {title}
          </h1>
          {subtitle && (
            <p className="max-w-md break-words text-center text-base text-white/70">
              {subtitle}
            </p>
          )}
          {onRetry && (
            <button
              onClick={onRetry}
              className="btn-ripple mt-2 flex items-center gap-2 rounded-lg border border-white/40 bg-white/10 px-4 py-2 text-sm font-medium text-white hover:bg-white/20"
            >
              <span className="material-symbols-rounded text-base">refresh</span>
              Tải lại
            </button>
          )}
        </div>
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
