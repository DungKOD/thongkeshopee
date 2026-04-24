import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import type { UiRow } from "./types";
import { fmtDate, fmtInt } from "./formulas";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import {
  AdminViewProvider,
  useAdminView,
} from "./contexts/AdminViewContext";
import { AccountProvider, useAccounts } from "./contexts/AccountContext";
import { AccountFilterDropdown } from "./components/AccountFilterDropdown";
import { AccountManagerDialog } from "./components/AccountManagerDialog";
import { ImportAccountPickerDialog } from "./components/ImportAccountPickerDialog";
import { ScrollToTopButton } from "./components/ScrollToTopButton";
import { usePremium, useIsAdmin } from "./hooks/usePremium";
import { useCloudSync, type SyncPhase } from "./hooks/useCloudSync";
import { useSelfPresence } from "./hooks/usePresence";
import { SyncBadge } from "./components/SyncBadge";
import { CloseWarningDialog } from "./components/CloseWarningDialog";
import { getCurrentWindow } from "@tauri-apps/api/window";
import { LoginScreen } from "./components/LoginScreen";
import { PaywallScreen } from "./components/PaywallScreen";
import { UserListDialog } from "./components/UserListDialog";
import { UserMenu } from "./components/UserMenu";
import { DevCredit } from "./components/DevCredit";
import { SmartCalculator } from "./components/SmartCalculator";
import { VideoLogsTab } from "./components/VideoLogsTab";
import "./App.css";

function AppInner() {
  const { signOut: authSignOut } = useAuth();
  // Khai báo sớm để filter có thể derive theo tab.
  const [activeTab, setActiveTab] = useState<
    "stats" | "overview" | "download" | "video-logs"
  >("stats");

  // Mỗi tab giữ filter riêng — cùng logic (useFilterMode), chỉ khác state
  // instance + localStorage scope. Active hook chọn theo `activeTab`.
  const statsFilter = useFilterMode("stats");
  const overviewFilter = useFilterMode("overview");
  const activeFilter = activeTab === "overview" ? overviewFilter : statsFilter;

  // Filter persist TRONG SESSION (in-memory). Chuyển tab stats ↔ overview
  // giữ nguyên filter đã chọn của mỗi tab. Reload app / logout → AppInner
  // remount → state reset về "Ngày gần nhất" (1 ngày) — default cho cả 2 tab.
  // Không còn auto-reset khi chuyển qua Overview như version cũ.

  // Alias để không đụng call sites hiện có trong component.
  const filterMode = activeFilter.mode;
  const setFilterMode = activeFilter.setMode;
  const setRecentDays = activeFilter.setRecent;
  const setPrevMonth = activeFilter.setPrevMonth;
  const setAllTime = activeFilter.setAllTime;
  const setDateFrom = activeFilter.setDateFrom;
  const setDateTo = activeFilter.setDateTo;

  const [subIdQuery, setSubIdQuery] = useState("");
  const [selectedSubId, setSelectedSubId] = useState<string | null>(null);

  // Account filter + active account từ context. Default filter {kind:"all"}
  // = không filter (backward compat với code trước multi-account).
  const {
    filter: accountFilter,
    activeAccountId,
    refresh: refreshAccounts,
  } = useAccounts();

  // Filter args gửi xuống Rust. BE nhận từ_date/to_date/limit → trả slice days,
  // sub_id_filter → subset match trên display_name (xem Rust `display_name_subset_match`),
  // account_filter → Shopee FK + FB attribution qua sub_ids JOIN.
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
      return {}; // all
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
    mutationVersion,
    markMutation,
  } = useDbStats({ filter: effectiveFilter });

  const { view: adminView, busy: adminBusy, exit: adminExit } = useAdminView();
  const inAdminView = adminView !== null;


  // Sync v2: pull-merge-push. Khi vào app: metadata check; nếu dirty hoặc remote
  // mới + khác máy → pull-merge-push + refetch UI. UI chặn overlay suốt startup
  // (cả checking + syncing) để user không thao tác với data cũ trước khi merge xong.
  //
  // Khi admin đang xem DB của user khác → disable sync (dirty của DB khác không
  // được upload lên R2 của admin).
  // CRITICAL phân quyền: sau khi `switch_db_to_user` swap DB sang user mới
  // (owner_changed=true) hoặc merge remote, PHẢI refetch cả days/rows lẫn
  // account list. AccountContext dep uid-change đã refetch lần 1, nhưng race
  // với switch_db_to_user (AccountContext effect fire trước switch xong) →
  // cần re-trigger ở đây để đảm bảo list account đúng sau khi DB swapped.
  const onRemoteApplied = useCallback(async () => {
    await refetch();
    await refreshAccounts();
  }, [refetch, refreshAccounts]);

  const {
    status: syncStatus,
    isStartupPhase,
    syncPhase,
    lastSyncAt,
    error: syncError,
    forceSync,
  } = useCloudSync({
    mutationVersion,
    enabled: !inAdminView,
    onRemoteApplied,
  });

  // Khi swap sang DB khác (hoặc thoát) → refetch + clear pending changes
  // của admin (pending state UI không hợp lệ với DB mới).
  // Gọi refreshAccounts() để AccountContext không giữ list TK của user vừa
  // xem (hoặc state stale từ trước) — filter dropdown phải khớp DB đang hiển thị.
  useEffect(() => {
    clearPending();
    void refetch();
    void refreshAccounts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [adminView?.uid]);

  // Khi vào admin view mode: force tab về "stats" nếu đang ở tab bị ẩn
  // (download / video-logs). Tránh màn trắng vì tab không còn hiển thị.
  useEffect(() => {
    if (
      inAdminView &&
      (activeTab === "download" || activeTab === "video-logs")
    ) {
      setActiveTab("stats");
    }
  }, [inAdminView, activeTab]);

  // Intercept Tauri close event — nếu DB dirty, chặn đóng app, show dialog
  // cảnh báo. User chọn sync+tắt, tắt luôn, hoặc huỷ.
  // Ref để handler luôn đọc status mới nhất (closure trong listener persist).
  const syncStatusRef = useRef(syncStatus);
  useEffect(() => {
    syncStatusRef.current = syncStatus;
  }, [syncStatus]);
  const forceSyncRef = useRef(forceSync);
  useEffect(() => {
    forceSyncRef.current = forceSync;
  }, [forceSync]);
  // Flag "cho phép đóng bỏ qua guard" — user đã xác nhận trong dialog
  // (sync xong hoặc tắt luôn). Handler thấy flag = true → return, không
  // preventDefault → Tauri close binh thường.
  const bypassCloseGuardRef = useRef(false);

  useEffect(() => {
    // Admin view: dirty của DB khác, không warn (sync bị disable rồi).
    if (inAdminView) return;
    let unlisten: (() => void) | null = null;
    let disposed = false;
    // Sync handler — async có thể gây Tauri race với preventDefault. Sync an toàn.
    getCurrentWindow()
      .onCloseRequested((event) => {
        if (bypassCloseGuardRef.current) {
          // User đã confirm trong dialog → cho đóng luôn, bỏ qua mọi check.
          return;
        }
        const s = syncStatusRef.current;
        if (s !== "dirty" && s !== "error") {
          // Clean → không preventDefault → Tauri close window như bình thường.
          return;
        }
        event.preventDefault();
        setCloseWarningOpen(true);
      })
      .then((fn) => {
        if (disposed) fn();
        else unlisten = fn;
      })
      .catch((e) => console.error("[close] listener setup failed:", e));
    return () => {
      disposed = true;
      if (unlisten) unlisten();
    };
  }, [inAdminView]);

  const handleSyncAndClose = useCallback(async () => {
    setCloseSyncing(true);
    try {
      await forceSyncRef.current();
    } catch (e) {
      console.error("[close] forceSync failed:", e);
      // Sync fail → không tự đóng, để user thấy error + chọn lại (tắt luôn).
      setCloseSyncing(false);
      return;
    }
    setCloseSyncing(false);
    setCloseWarningOpen(false);
    // Bypass flag trước khi close() → handler thấy true → return → close OK.
    bypassCloseGuardRef.current = true;
    await getCurrentWindow().close();
  }, []);

  const handleCloseAnyway = useCallback(async () => {
    setCloseWarningOpen(false);
    bypassCloseGuardRef.current = true;
    await getCurrentWindow().close();
  }, []);

  const handleCancelClose = useCallback(() => {
    setCloseWarningOpen(false);
  }, []);

  // Logout flow: check dirty trước khi signOut. Nếu dirty → dialog 3 lựa chọn
  // (đồng bộ rồi logout / logout luôn / hủy). Không dirty → signOut ngay.
  const requestSignOut = useCallback(async () => {
    if (syncStatusRef.current === "dirty") {
      setLogoutDialogOpen(true);
      return;
    }
    await authSignOut();
  }, [authSignOut]);

  const handleSyncAndSignOut = useCallback(async () => {
    setLogoutSyncing(true);
    try {
      await forceSyncRef.current();
    } catch (e) {
      console.error("[logout] forceSync failed:", e);
      setLogoutSyncing(false);
      return; // giữ dialog để user thấy lỗi + chọn lại
    }
    setLogoutSyncing(false);
    setLogoutDialogOpen(false);
    await authSignOut();
  }, [authSignOut]);

  const handleSignOutAnyway = useCallback(async () => {
    setLogoutDialogOpen(false);
    await authSignOut();
  }, [authSignOut]);

  const handleCancelSignOut = useCallback(() => {
    setLogoutDialogOpen(false);
  }, []);

  const { showToast } = useToast();
  const { settings, setClickSource, registerSources, setProfitFee } =
    useSettings();

  // Auto-register referrers từ DB vào settings.clickSources.
  // Mỗi khi DB thay đổi (import mới, xóa, v.v.) → refetch → referrers mới → register.
  // registerSources chỉ thêm referrer mới (default enabled), không đụng trạng thái cũ.
  useEffect(() => {
    if (referrers.length > 0) registerSources(referrers);
  }, [referrers, registerSources]);

  // Silent auto-check cập nhật 1 lần sau login. Nếu có bản mới → toast
  // nhắc user mở Settings để cài. Fail im lặng (offline / endpoint trả 404
  // khi chưa có Release) — không spam error toast.
  const updateCheckedRef = useRef(false);
  useEffect(() => {
    if (updateCheckedRef.current) return;
    updateCheckedRef.current = true;
    void (async () => {
      try {
        const { checkForUpdate } = await import("./lib/updater");
        const info = await checkForUpdate();
        if (info) {
          showToast({
            message: `Có bản mới v${info.version} — mở Cài đặt để cập nhật.`,
            duration: 8000,
          });
        }
      } catch {
        // offline / no release yet — silent
      }
    })();
  }, [showToast]);

  const isAdmin = useIsAdmin();
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [rulesOpen, setRulesOpen] = useState(false);
  const [userListOpen, setUserListOpen] = useState(false);
  const [accountMgrOpen, setAccountMgrOpen] = useState(false);
  // Close warning: Tauri intercept event, show dialog nếu dirty.
  const [closeWarningOpen, setCloseWarningOpen] = useState(false);
  const [closeSyncing, setCloseSyncing] = useState(false);
  // Logout warning: user bấm đăng xuất mà DB dirty → hỏi sync trước.
  const [logoutDialogOpen, setLogoutDialogOpen] = useState(false);
  const [logoutSyncing, setLogoutSyncing] = useState(false);
  // Máy tính — open state lift lên App để header button toggle được.
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
  const [entryDialog, setEntryDialog] = useState<{
    date: string;
    row?: UiRow | null;
  } | null>(null);
  const [previewBatch, setPreviewBatch] = useState<PreviewBatch | null>(null);
  // TK user pick trong ImportAccountPickerDialog — giữ xuyên suốt flow import.
  // null khi chưa pick (dialog đóng) hoặc không có batch pending.
  const [importAccountId, setImportAccountId] = useState<number | null>(null);
  const [accountPickerOpen, setAccountPickerOpen] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Flow: bấm "Import CSV" → mở AccountPicker → user chọn TK → mở file picker
  // → parse → preview (hiển thị TK đã pick) → confirm → commit.
  const handleImportClick = () => setAccountPickerOpen(true);

  const handleAccountPicked = (accountId: number) => {
    setImportAccountId(accountId);
    setAccountPickerOpen(false);
    // Defer mở file picker 1 tick để dialog đóng animation xong.
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

  const handleConfirmImport = useCallback(async () => {
    if (!previewBatch || importAccountId === null) return;
    const results = await commitCsvBatch(previewBatch, importAccountId);
    await refreshAccounts();
    const totalNew = results.reduce((a, r) => a + r.inserted, 0);
    const totalReplace = results.reduce((a, r) => a + r.duplicated, 0);
    const totalSkipped = results.reduce((a, r) => a + r.skipped, 0);
    // Date range của batch thực tế đã commit (dayDateFrom/To từ kết quả Rust).
    const dateRange = (() => {
      if (results.length === 0) return "";
      const from = results.map((r) => r.dayDateFrom).sort()[0];
      const to = results.map((r) => r.dayDateTo).sort().reverse()[0];
      return from === to ? fmtDate(from) : `${fmtDate(from)} → ${fmtDate(to)}`;
    })();
    setPreviewBatch(null);
    markMutation();
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
  }, [previewBatch, refetch, showToast, markMutation, importAccountId, refreshAccounts]);

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

  // BE đã filter theo effectiveFilter → `days` chính là slice cần render.
  // `overview.totalDaysCount` là tổng ngày trong DB, dùng cho pagination UI.
  const totalDaysInDb = overview.totalDaysCount;

  // Chỉ default-paginated mode mới expand khi scroll. So sánh count đang show
  // với total ngày trong DB (không phải `days.length` vì BE có thể trả ít hơn
  // limit khi data thưa).
  const canLoadMore =
    filterMode.type === "recent" &&
    filterMode.canExpand &&
    filterMode.count < totalDaysInDb;

  // Detect active cho highlight shortcut "Tháng trước".
  const prevMonth = useMemo(() => prevMonthRange(), []);
  const isPrevMonthActive =
    filterMode.type === "range" &&
    filterMode.from === prevMonth.from &&
    filterMode.to === prevMonth.to;

  // Hiển thị dates trong picker:
  // - range mode → lưu trực tiếp trong filterMode.
  // - shortcut recent (!canExpand) → derive từ `days` (BE đã cắt đúng slice).
  // - all → derive từ overview.oldest/newest (bounds toàn DB, không cần scan FE).
  // - default paginated → để trống (chưa chọn range cụ thể).
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

  // Suggestions dropdown: filter `overview.allSubIds` theo query. Dataset từ
  // overview (toàn DB) nên user chọn được cả sub_id từ ngày không trong slice.
  // Match = startsWith tại bất kỳ part nào — tránh noise substring giữa part.
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

  if (isStartupPhase) {
    // Nếu switch_db_to_user fail → hook keep isStartupPhase=true + set syncError.
    // Splash hiển thị error thay vì text "đang đồng bộ" để user không bị kẹt
    // nhìn vào loading spinner vô thời hạn. Retry = logout + login lại.
    if (syncStatus === "error" && syncError) {
      return (
        <SplashScreen
          title="Không mở được database"
          subtitle={syncError}
          lastSyncAt={lastSyncAt}
          error
          onRetry={() => void authSignOut()}
        />
      );
    }
    return (
      <SplashScreen {...splashTextFor(syncPhase)} lastSyncAt={lastSyncAt} />
    );
  }

  return (
    <main className="min-h-full bg-surface-0 pb-24">
      {inAdminView && adminView && (
        <AdminViewBanner
          email={adminView.email}
          localPart={adminView.local_part}
          busy={adminBusy}
          onExit={() => void adminExit()}
        />
      )}
      <header className="sticky top-0 z-30 bg-gradient-to-r from-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center justify-between px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="material-symbols-rounded text-3xl text-white">
              analytics
            </span>
            <div>
              <h1 className="text-xl font-semibold tracking-tight text-white">
                Shopee Affiliate Tracker
              </h1>
              <p className="text-xs text-white/70">
                Data từ database — manual override luôn ưu tiên raw CSV
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            {!inAdminView && (
              <SyncBadge
                status={syncStatus}
                lastSyncAt={lastSyncAt}
                error={syncError}
                onForce={forceSync}
              />
            )}
            {!inAdminView && (
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
            )}
            {isAdmin && !inAdminView && (
              <button
                onClick={() => setUserListOpen(true)}
                className="btn-ripple flex items-center gap-1.5 rounded-lg border border-white/40 px-3 py-2 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
                title="Danh sách user (admin)"
                aria-label="Danh sách user"
              >
                <span className="material-symbols-rounded text-base">
                  admin_panel_settings
                </span>
                User
              </button>
            )}
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
            {!inAdminView && (
              <button
                onClick={() => setAccountMgrOpen(true)}
                className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
                title="Quản lý TK Shopee"
                aria-label="Quản lý TK Shopee"
              >
                <span className="material-symbols-rounded">manage_accounts</span>
              </button>
            )}
            {!inAdminView && (
              <button
                onClick={() => setSettingsOpen(true)}
                className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
                title="Cài đặt"
                aria-label="Cài đặt"
              >
                <span className="material-symbols-rounded">settings</span>
              </button>
            )}
            {!inAdminView && <UserMenu onRequestSignOut={requestSignOut} />}
            {activeTab === "stats" && !inAdminView && (
              <button
                onClick={handleImportClick}
                className="btn-ripple flex items-center gap-2 rounded-lg border border-white/50 px-4 py-2 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
              >
                <span className="material-symbols-rounded text-base">
                  upload_file
                </span>
                Import CSV
              </button>
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
            active={activeTab === "overview"}
            onClick={() => setActiveTab("overview")}
            icon="insights"
            label="Tổng quan"
          />
          {!inAdminView && (
            <TabButton
              active={activeTab === "download"}
              onClick={() => setActiveTab("download")}
              icon="download"
              label="Download video"
            />
          )}
          {isAdmin && !inAdminView && (
            <TabButton
              active={activeTab === "video-logs"}
              onClick={() => setActiveTab("video-logs")}
              icon="admin_panel_settings"
              label="Video Logs"
            />
          )}
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
        ) : activeTab === "video-logs" && isAdmin ? (
          <VideoLogsTab />
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
                {inAdminView ? "User này chưa có data" : "Chưa có data nào"}
              </h2>
              <p className="mt-1 text-sm text-white/60">
                {inAdminView
                  ? "DB của user đang xem chưa có ngày nào được import"
                  : "Bắt đầu bằng import CSV hoặc thêm dòng thủ công"}
              </p>
            </div>
            {!inAdminView && (
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
            )}
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

                <span className="hidden h-6 w-px bg-surface-8 md:inline-block" />

                {/* Account filter — tách theo TK Shopee affiliate. Nút
                    quản lý TK đã chuyển lên header bên trái nút Cài đặt. */}
                <AccountFilterDropdown />

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
                    onKeyDown={(e) => {
                      // Enter: chọn suggestion đầu tiên (nếu có), fallback: giữ nguyên query làm sub_id filter.
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

                {/* Right anchor: badge counter, luôn bám phải */}
                <div className="flex shrink-0 items-center gap-2 pt-1">
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
              <OverviewTab
                days={days}
                dateFrom={dateFrom}
                dateTo={dateTo}
                totalDaysInDb={totalDaysInDb}
                currentFilter={effectiveFilter}
                accountFilter={accountFilter}
              />
            ) : (
              <>
                {selectedSubId ? (
                  <SubIdTimelineBlock
                    subId={selectedSubId}
                    days={days}
                    pendingRowDeletes={pendingRowDeletes}
                    onToggleRowDelete={(r) =>
                      toggleRowPending(r.dayDate, r.subIds)
                    }
                    onEditRow={(r) =>
                      setEntryDialog({ date: r.dayDate, row: r })
                    }
                    readOnly={inAdminView}
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
                        toggleRowPending(r.dayDate, r.subIds)
                      }
                      onEditRow={(r) =>
                        setEntryDialog({ date: r.dayDate, row: r })
                      }
                      onEditDay={(date) => setEntryDialog({ date })}
                      readOnly={inAdminView}
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
        onClose={() => setSettingsOpen(false)}
        onImportReverted={() => void refetch()}
      />

      <RulesDialog isOpen={rulesOpen} onClose={() => setRulesOpen(false)} />

      {isAdmin && (
        <UserListDialog
          isOpen={userListOpen}
          onClose={() => setUserListOpen(false)}
        />
      )}

      {entryDialog && (
        <ManualEntryDialog
          isOpen={true}
          initialDate={entryDialog.date}
          initialRow={entryDialog.row}
          shopeeAccountId={activeAccountId}
          onSave={handleSaveEntry}
          onClose={() => setEntryDialog(null)}
        />
      )}

      <ImportAccountPickerDialog
        isOpen={accountPickerOpen}
        onPick={handleAccountPicked}
        onClose={() => setAccountPickerOpen(false)}
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
          markMutation();
          void refetch();
        }}
      />

      {/* FAB — hiện khi scroll sâu, click về đầu page */}
      <ScrollToTopButton />

      <CloseWarningDialog
        isOpen={closeWarningOpen}
        syncing={closeSyncing}
        onSyncAndClose={handleSyncAndClose}
        onCloseAnyway={handleCloseAnyway}
        onCancel={handleCancelClose}
      />

      <CloseWarningDialog
        isOpen={logoutDialogOpen}
        syncing={logoutSyncing}
        title="Data chưa đồng bộ lên R2"
        description="Vẫn còn thay đổi chưa upload lên R2. Nếu đăng xuất luôn, khi bạn đăng nhập ở máy khác sẽ không thấy những thay đổi này."
        syncLabel="Đồng bộ lên R2 rồi đăng xuất"
        anywayLabel="Đăng xuất luôn (chấp nhận mất đồng bộ R2)"
        onSyncAndClose={handleSyncAndSignOut}
        onCloseAnyway={handleSignOutAnyway}
        onCancel={handleCancelSignOut}
      />

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

interface AdminViewBannerProps {
  email: string | null;
  localPart: string;
  busy: boolean;
  onExit: () => void;
}

function AdminViewBanner({
  email,
  localPart,
  busy,
  onExit,
}: AdminViewBannerProps) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-amber-500/40 bg-amber-600/90 px-6 py-2 text-sm text-white shadow-elev-4">
      <div className="flex items-center gap-2 min-w-0">
        <span className="material-symbols-rounded text-base">
          admin_panel_settings
        </span>
        <span className="font-medium whitespace-nowrap">Chế độ xem admin:</span>
        <span
          className="truncate font-mono text-white/95"
          title={email ?? localPart}
        >
          {email ?? `${localPart}.db`}
        </span>
        <span className="hidden rounded-full bg-white/20 px-2 py-0.5 text-[11px] font-medium md:inline">
          READ-ONLY
        </span>
      </div>
      <button
        type="button"
        onClick={onExit}
        disabled={busy}
        className="btn-ripple flex shrink-0 items-center gap-1 rounded-md border border-white/60 bg-white/10 px-3 py-1 text-xs font-medium text-white hover:bg-white/20 disabled:opacity-50"
        title="Thoát chế độ xem — quay lại DB của bạn"
      >
        <span
          className={`material-symbols-rounded text-sm ${busy ? "animate-spin" : ""}`}
        >
          {busy ? "sync" : "logout"}
        </span>
        Thoát
      </button>
    </div>
  );
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
  // Presence tracking — mount 1 lần ở AuthGate, hook tự re-subscribe khi
  // uid đổi qua auth.onAuthStateChanged. Chạy bất kể status paywall vì
  // user vẫn "online" khi đang ở màn PaywallScreen.
  useSelfPresence();

  if (authLoading) return <SplashScreen title="Đang tải..." />;
  if (!user) return <LoginScreen />;
  if (status === "loading") return <SplashScreen title="Đang tải..." />;
  if (status === "inactive" || status === "expired") {
    return <PaywallScreen expiredAt={expiredAt} reason={status} />;
  }

  // CRITICAL phân quyền: key={user.uid} force remount toàn bộ subtree khi uid
  // đổi. Lý do: SettingsProvider/AppInner khởi tạo state từ localStorage SYNC
  // tại mount (useState(loadSettings), useFilterMode, smartcalc:open…).
  // `wipeUserLocalStorage` trong useCloudSync chạy SAU mount → chỉ xoá
  // localStorage, không reset React state. Không remount = user B thấy config
  // của user A (click sources, profit fee %, filter range, calc open state).
  // Key theo uid đảm bảo: uid đổi → unmount cũ → localStorage wiped → mount mới
  // đọc default.
  return (
    <SettingsProvider key={user.uid}>
      <AdminViewProvider>
        <AccountProvider>
          <AppInner />
        </AccountProvider>
      </AdminViewProvider>
    </SettingsProvider>
  );
}

/// Map sync phase → text cho SplashScreen. Null (chưa có event) → default
/// "Đang đồng bộ lên R2..." để UX nhất quán với lúc init.
function splashTextFor(phase: SyncPhase): { title: string; subtitle?: string } {
  switch (phase) {
    case "downloading":
      return {
        title: "Đang tải xuống...",
        subtitle: "Lấy dữ liệu mới từ R2.",
      };
    case "merging":
      return {
        title: "Đang hợp nhất...",
        subtitle: "Kết hợp dữ liệu local với bản từ R2.",
      };
    case "uploading":
      return {
        title: "Đang đẩy lên R2...",
        subtitle: "Lưu thay đổi cuối cùng.",
      };
    default:
      return {
        title: "Đang đồng bộ lên R2...",
        subtitle: "Hợp nhất dữ liệu từ các máy khác, vui lòng chờ.",
      };
  }
}

/// Fullscreen splash — icon xoay anchor ở vị trí cố định (32vh từ top),
/// text flow dưới. Khi subtitle/timestamp dài, text wrap xuống nhưng icon
/// KHÔNG di chuyển — UX ổn định, user vẫn biết đang sync.
///
/// error=true → icon chuyển error_outline (không xoay) + optional nút retry.
function SplashScreen({
  title,
  subtitle,
  lastSyncAt,
  error,
  onRetry,
}: {
  title: string;
  subtitle?: string;
  lastSyncAt?: Date | null;
  error?: boolean;
  onRetry?: () => void;
}) {
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
          {lastSyncAt && (
            <p className="text-sm text-white/50">
              Lần cuối: {lastSyncAt.toLocaleString("vi-VN")}
            </p>
          )}
          {onRetry && (
            <button
              onClick={onRetry}
              className="btn-ripple mt-2 flex items-center gap-2 rounded-lg border border-white/40 bg-white/10 px-4 py-2 text-sm font-medium text-white hover:bg-white/20"
            >
              <span className="material-symbols-rounded text-base">logout</span>
              Đăng xuất và thử lại
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
