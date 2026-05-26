import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { revealItemInDir } from "@tauri-apps/plugin-opener";
import { save as dialogSave, open as dialogOpen } from "@tauri-apps/plugin-dialog";
import type {
  ProfitFees,
  Settings,
  SubIdMatchMode,
} from "../hooks/useSettings";
import { ImportHistorySection } from "./ImportHistorySection";
import { ConfirmDialog } from "./ConfirmDialog";
import { invoke } from "../lib/tauri";

interface AppDataPaths {
  appDataDir: string;
  activeDbPath: string;
  activeImportsDir: string;
}

interface SettingsDialogProps {
  isOpen: boolean;
  settings: Settings;
  daysCount: number;
  productsCount: number;
  onToggleClickSource: (source: string, enabled: boolean) => void;
  onSetProfitFee: (key: keyof ProfitFees, value: number) => void;
  onSetSubIdMatchMode: (mode: SubIdMatchMode) => void;
  onClose: () => void;
  /** Trigger reload lịch sử import ngoài (bump khi có import/delete). */
  importHistoryReloadKey?: number;
  /** Callback sau khi revert import — parent reload days/overview. */
  onImportReverted?: () => void;
}

export function SettingsDialog({
  isOpen,
  settings,
  daysCount,
  productsCount,
  onToggleClickSource,
  onSetProfitFee,
  onSetSubIdMatchMode,
  onClose,
  importHistoryReloadKey,
  onImportReverted,
}: SettingsDialogProps) {
  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  const [paths, setPaths] = useState<AppDataPaths | null>(null);
  const [pathsError, setPathsError] = useState<string | null>(null);
  useEffect(() => {
    if (!isOpen) return;
    let cancelled = false;
    (async () => {
      try {
        const p = await invoke<AppDataPaths>("get_app_data_paths");
        if (!cancelled) setPaths(p);
      } catch (e) {
        if (!cancelled) setPathsError((e as Error).message ?? String(e));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [isOpen]);

  // Lock guard cho 2 section nhạy cảm — sửa nhầm có hệ quả lớn (recompute
  // toàn bộ profit / filter click). Default LOCKED mỗi lần mở dialog. User
  // phải bấm 🔒 → 🔓 để sửa, sau đó nên bấm lại để khoá. State per-section
  // (user thường chỉ sửa 1 trong 2). Reset về locked khi dialog đóng.
  const [feesLocked, setFeesLocked] = useState(true);
  const [sourcesLocked, setSourcesLocked] = useState(true);
  useEffect(() => {
    if (!isOpen) {
      setFeesLocked(true);
      setSourcesLocked(true);
    }
  }, [isOpen]);

  const [exportState, setExportState] = useState<"idle" | "busy" | "ok" | "err">("idle");
  const [exportError, setExportError] = useState<string | null>(null);

  const handleExportDb = async () => {
    setExportError(null);
    const now = new Date();
    const stamp = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}`;
    const destPath = await dialogSave({
      title: "Xuất Database",
      defaultPath: `thongkeshopee_backup_${stamp}.db`,
      filters: [{ name: "SQLite Database", extensions: ["db"] }],
    });
    if (!destPath) return;
    setExportState("busy");
    try {
      await invoke("export_db", { destPath });
      setExportState("ok");
      setTimeout(() => setExportState("idle"), 2500);
    } catch (e) {
      setExportError(String(e));
      setExportState("err");
    }
  };

  const [importConfirmOpen, setImportConfirmOpen] = useState(false);
  const [pendingImportPath, setPendingImportPath] = useState<string | null>(null);
  const [importState, setImportState] = useState<"idle" | "busy" | "err">("idle");
  const [importError, setImportError] = useState<string | null>(null);

  const handlePickImport = async () => {
    setImportError(null);
    const selected = await dialogOpen({
      title: "Chọn file Database",
      multiple: false,
      filters: [{ name: "SQLite Database", extensions: ["db"] }],
    });
    if (!selected || typeof selected !== "string") return;
    setPendingImportPath(selected);
    setImportConfirmOpen(true);
  };

  const handleConfirmImport = async () => {
    if (!pendingImportPath) return;
    setImportConfirmOpen(false);
    setImportState("busy");
    try {
      await invoke("import_db", { srcPath: pendingImportPath });
      window.location.reload();
    } catch (e) {
      setImportError(String(e));
      setImportState("err");
    }
  };

  const [clearConfirmOpen, setClearConfirmOpen] = useState(false);
  const [clearing, setClearing] = useState(false);

  const handleClearData = async () => {
    setClearConfirmOpen(false);
    setClearing(true);
    try {
      await invoke("clear_app_data");
      window.location.reload();
    } catch (e) {
      console.error("[clear_app_data]", e);
      setClearing(false);
    }
  };

  if (!isOpen) return null;

  const sources = Object.entries(settings.clickSources).sort((a, b) =>
    a[0].localeCompare(b[0]),
  );

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return (
    <>
    {createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="flex max-h-[calc(100vh-2rem)] w-full max-w-5xl flex-col overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="settings-dialog-title"
      >
        <header className="flex shrink-0 items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-shopee-400">
            settings
          </span>
          <h2
            id="settings-dialog-title"
            className="flex-1 text-lg font-semibold text-white/90"
          >
            Cài đặt
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/70 hover:bg-white/10 hover:text-white"
            title="Đóng (Esc)"
            aria-label="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <div className="min-h-0 flex-1 space-y-6 overflow-y-auto px-6 py-5">
          <section>
            <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                insights
              </span>
              Thống kê chung
            </h3>
            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-xl bg-surface-6 px-4 py-3 shadow-elev-1">
                <p className="text-xs text-white/50">Số ngày đã lưu</p>
                <p className="mt-1 text-2xl font-semibold tabular-nums text-shopee-300">
                  {daysCount}
                </p>
              </div>
              <div className="rounded-xl bg-surface-6 px-4 py-3 shadow-elev-1">
                <p className="text-xs text-white/50">Tổng sản phẩm</p>
                <p className="mt-1 text-2xl font-semibold tabular-nums text-shopee-300">
                  {productsCount}
                </p>
              </div>
            </div>
          </section>

          <section>
            <div className="mb-1 flex items-center justify-between gap-2">
              <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
                <span className="material-symbols-rounded text-base">
                  payments
                </span>
                Phí khấu trừ lợi nhuận
              </h3>
              <LockButton
                locked={feesLocked}
                onToggle={() => setFeesLocked((v) => !v)}
              />
            </div>
            <p className="mb-3 text-xs text-white/50">
              Net = Hoa hồng × (1 − Thuế) − Hoa hồng <i>pending</i> × Dự phòng
            </p>
            <div className="grid grid-cols-2 gap-3">
              <FeeInput
                label="Thuế + phí sàn"
                value={settings.profitFees.taxAndPlatformRate}
                onChange={(v) => onSetProfitFee("taxAndPlatformRate", v)}
                disabled={feesLocked}
              />
              <FeeInput
                label="Dự phòng hoàn/hủy"
                value={settings.profitFees.returnReserveRate}
                onChange={(v) => onSetProfitFee("returnReserveRate", v)}
                disabled={feesLocked}
              />
            </div>
            <div className="mt-2 flex items-start gap-2 rounded-lg border border-amber-500/30 bg-amber-950/20 px-3 py-2 text-xs text-amber-200">
              <span className="material-symbols-rounded mt-0.5 text-sm text-amber-400">
                info
              </span>
              <div>
                <div className="font-semibold">Dự phòng hoàn/hủy</div>
                <div className="mt-0.5 text-amber-200/80">
                  CHỈ trừ từ hoa hồng của đơn trạng thái{" "}
                  <b>"Đang chờ xử lý"</b> và{" "}
                  <b>"Chưa thanh toán"</b> (các đơn có rủi ro bị hủy). Đơn đã
                  hoàn thành/thanh toán không bị trừ dự phòng (đã chắc chắn).
                  Thuế + phí sàn thì áp cho mọi đơn.
                </div>
              </div>
            </div>
          </section>

          <section>
            <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">link</span>
              Cách khớp Sub_id ↔ tên Camp/Nhóm QC
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Quyết định khi nào FB ad merge với Shopee subid trong cùng dòng UI.
            </p>
            <div className="space-y-2">
              <label
                className={`flex cursor-pointer items-start gap-3 rounded-lg border px-4 py-3 transition-colors ${
                  settings.subIdMatchMode === "exact"
                    ? "border-shopee-500 bg-shopee-900/20"
                    : "border-surface-8 bg-surface-1 hover:bg-surface-2"
                }`}
              >
                <input
                  type="radio"
                  name="sub-id-match-mode"
                  value="exact"
                  checked={settings.subIdMatchMode === "exact"}
                  onChange={() => onSetSubIdMatchMode("exact")}
                  className="mt-1 h-4 w-4 accent-shopee-500"
                />
                <div className="flex-1 text-sm">
                  <div className="font-semibold text-white/90">
                    Khớp chính xác (mặc định)
                  </div>
                  <div className="mt-1 text-xs text-white/60">
                    Tuple sub_id phải bằng nhau từng slot. Ví dụ:{" "}
                    <span className="font-mono text-shopee-300">camp1</span>{" "}
                    và{" "}
                    <span className="font-mono text-shopee-300">
                      camp1-0412
                    </span>{" "}
                    → merge. Còn{" "}
                    <span className="font-mono text-shopee-300">camp1</span>{" "}
                    và{" "}
                    <span className="font-mono text-shopee-300">dungcamp1</span>{" "}
                    → KHÔNG merge.
                  </div>
                </div>
              </label>
              <label
                className={`flex cursor-pointer items-start gap-3 rounded-lg border px-4 py-3 transition-colors ${
                  settings.subIdMatchMode === "substring"
                    ? "border-shopee-500 bg-shopee-900/20"
                    : "border-surface-8 bg-surface-1 hover:bg-surface-2"
                }`}
              >
                <input
                  type="radio"
                  name="sub-id-match-mode"
                  value="substring"
                  checked={settings.subIdMatchMode === "substring"}
                  onChange={() => onSetSubIdMatchMode("substring")}
                  className="mt-1 h-4 w-4 accent-shopee-500"
                />
                <div className="flex-1 text-sm">
                  <div className="font-semibold text-white/90">
                    Chứa nhau (substring)
                  </div>
                  <div className="mt-1 text-xs text-white/60">
                    Giống chính xác, PLUS cho phép 2 tên chứa nhau
                    (case-insensitive, tối thiểu 3 ký tự). Ví dụ:{" "}
                    <span className="font-mono text-shopee-300">camp1</span>{" "}
                    và{" "}
                    <span className="font-mono text-shopee-300">dungcamp1</span>{" "}
                    → merge.
                  </div>
                  <div className="mt-1.5 flex items-start gap-1.5 text-[11px] text-amber-300/90">
                    <span className="material-symbols-rounded mt-0.5 text-xs text-amber-400">
                      warning
                    </span>
                    <span>
                      Có thể false positive khi sub_id ngắn/giống nhau
                      ngẫu nhiên (vd "camp" match "scamper").
                    </span>
                  </div>
                </div>
              </label>
            </div>
          </section>

          <section>
            <div className="mb-1 flex items-center justify-between gap-2">
              <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
                <span className="material-symbols-rounded text-base">
                  filter_list
                </span>
                Nguồn Click Shopee
              </h3>
              <LockButton
                locked={sourcesLocked}
                onToggle={() => setSourcesLocked((v) => !v)}
              />
            </div>
            <p className="mb-3 text-xs text-white/50">
              Chọn loại referrer (cột "Người giới thiệu") sẽ được tính vào ô
              Click Shopee. Danh sách tự cập nhật sau mỗi lần Import.
            </p>

            {sources.length === 0 ? (
              <div className="flex items-center gap-3 rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/50">
                <span className="material-symbols-rounded text-base">info</span>
                Chưa Import file click. Hãy Import để phát hiện các nguồn
                referrer.
              </div>
            ) : (
              <ul
                className={`divide-y divide-surface-8 overflow-hidden rounded-xl bg-surface-6 ${
                  sourcesLocked ? "opacity-60" : ""
                }`}
              >
                {sources.map(([source, enabled]) => (
                  <li key={source}>
                    <label
                      className={`flex items-center gap-3 px-4 py-3 text-sm text-white/85 transition-colors ${
                        sourcesLocked
                          ? "cursor-not-allowed"
                          : "cursor-pointer hover:bg-white/5"
                      }`}
                      title={sourcesLocked ? "Đã khoá — bấm 🔒 ở header để mở" : undefined}
                    >
                      <input
                        type="checkbox"
                        checked={enabled}
                        disabled={sourcesLocked}
                        onChange={(e) =>
                          onToggleClickSource(source, e.currentTarget.checked)
                        }
                        className="h-4 w-4 accent-shopee-500 disabled:cursor-not-allowed"
                      />
                      <span className="flex-1">{source}</span>
                      {enabled ? (
                        <span className="material-symbols-rounded text-base text-shopee-400">
                          check_circle
                        </span>
                      ) : null}
                    </label>
                  </li>
                ))}
              </ul>
            )}
          </section>

          <ImportHistorySection
            reloadKey={importHistoryReloadKey}
            onReverted={onImportReverted}
          />

          {/* Đường dẫn data app — hữu ích cho support/debug/backup thủ công. */}
          <section>
            <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">folder</span>
              Đường dẫn data app
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Data (DB + CSV imports) lưu ở local machine. Click{" "}
              <b>Copy</b> để paste đường dẫn hoặc <b>Mở</b> để xem thư mục.
            </p>
            {pathsError && (
              <div className="mb-2 rounded-lg border border-red-500/40 bg-red-900/20 p-2 text-xs text-red-200">
                Lỗi: {pathsError}
              </div>
            )}
            {paths ? (
              <div className="space-y-2">
                <PathRow
                  label="Thư mục app data"
                  path={paths.appDataDir}
                  revealAsDir
                />
                <PathRow
                  label="File database"
                  path={paths.activeDbPath}
                  revealAsDir={false}
                />
                <PathRow
                  label="Thư mục CSV imports"
                  path={paths.activeImportsDir}
                  revealAsDir
                />
              </div>
            ) : !pathsError ? (
              <div className="text-xs text-white/40">Đang tải...</div>
            ) : null}
          </section>

          <section>
            <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base text-blue-400">
                database
              </span>
              Sao lưu &amp; Khôi phục
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Xuất toàn bộ database ra file <span className="font-mono">.db</span> để backup hoặc chuyển máy.
              Nhập file backup để khôi phục — app sẽ tự khởi động lại.
            </p>
            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                onClick={handleExportDb}
                disabled={exportState === "busy"}
                className={`btn-ripple flex items-center gap-2 rounded-lg border px-4 py-2 text-sm font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${
                  exportState === "ok"
                    ? "border-green-500/40 bg-green-500/10 text-green-300"
                    : "border-blue-500/40 bg-blue-500/10 text-blue-300 hover:bg-blue-500/20"
                }`}
              >
                <span className="material-symbols-rounded text-base">
                  {exportState === "busy" ? "hourglass_empty" : exportState === "ok" ? "check" : "download"}
                </span>
                {exportState === "busy" ? "Đang xuất…" : exportState === "ok" ? "Đã xuất thành công" : "Xuất DB"}
              </button>
              <button
                type="button"
                onClick={handlePickImport}
                disabled={importState === "busy"}
                className="btn-ripple flex items-center gap-2 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-2 text-sm font-semibold text-amber-300 transition-colors hover:bg-amber-500/20 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <span className="material-symbols-rounded text-base">
                  {importState === "busy" ? "hourglass_empty" : "upload"}
                </span>
                {importState === "busy" ? "Đang nhập…" : "Nhập DB"}
              </button>
            </div>
            {exportState === "err" && exportError && (
              <p className="mt-2 text-xs text-red-300">Lỗi xuất: {exportError}</p>
            )}
            {importState === "err" && importError && (
              <p className="mt-2 text-xs text-red-300">Lỗi nhập: {importError}</p>
            )}
          </section>

          <section>
            <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base text-red-400">
                delete_forever
              </span>
              Xóa dữ liệu
            </h3>
            <div className="rounded-xl border border-red-500/25 bg-red-950/15 px-4 py-3">
              <p className="mb-3 text-xs text-white/60">
                Xóa toàn bộ dữ liệu thống kê (đơn hàng, click, FB ads, cài
                đặt, lịch sử import). <b className="text-white/80">Giữ lại</b>{" "}
                trạng thái đăng nhập. App sẽ tự khởi động lại.
              </p>
              <button
                type="button"
                onClick={() => setClearConfirmOpen(true)}
                disabled={clearing}
                className="btn-ripple flex items-center gap-2 rounded-lg border border-red-500/40 bg-red-500/10 px-4 py-2 text-sm font-semibold text-red-300 transition-colors hover:bg-red-500/20 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <span className="material-symbols-rounded text-base">
                  {clearing ? "hourglass_empty" : "delete_forever"}
                </span>
                {clearing ? "Đang xóa và khởi động lại…" : "Xóa tất cả dữ liệu"}
              </button>
            </div>
          </section>

          {/* App info — version + build metadata. Cuối Settings làm footer info. */}
          <section className="rounded-xl border border-surface-8 bg-surface-1 px-4 py-3">
            <div className="flex items-center justify-between text-xs text-white/60">
              <span className="flex items-center gap-1.5">
                <span className="material-symbols-rounded text-sm text-shopee-400">
                  info
                </span>
                ThongKeShopee
              </span>
              <span className="font-mono tabular-nums text-white/80">
                v{__APP_VERSION__}
              </span>
            </div>
          </section>

        </div>
      </div>
    </div>,
    document.body,
    )}
    <ConfirmDialog
      isOpen={clearConfirmOpen}
      title="Xóa toàn bộ dữ liệu?"
      message="Hành động này xóa vĩnh viễn tất cả đơn hàng, click, FB ads, lịch sử import và cài đặt. Không thể hoàn tác. Tài khoản đăng nhập sẽ được giữ lại."
      confirmLabel="Xóa & Khởi động lại"
      cancelLabel="Hủy"
      danger
      onConfirm={handleClearData}
      onClose={() => setClearConfirmOpen(false)}
    />
    <ConfirmDialog
      isOpen={importConfirmOpen}
      title="Nhập Database?"
      message={
        <span>
          Toàn bộ dữ liệu hiện tại sẽ bị thay thế bằng file backup. App sẽ tự
          khởi động lại sau khi nhập.
          {pendingImportPath && (
            <span className="mt-2 block truncate font-mono text-xs text-white/60">
              {pendingImportPath}
            </span>
          )}
        </span>
      }
      confirmLabel="Nhập & Khởi động lại"
      cancelLabel="Hủy"
      danger
      onConfirm={handleConfirmImport}
      onClose={() => { setImportConfirmOpen(false); setPendingImportPath(null); }}
    />
    </>
  );
}

interface PathRowProps {
  label: string;
  path: string;
  /// `true` = path là thư mục → `revealItemInDir(path)` mở Explorer vào
  /// folder đó. `false` = path là file → reveal select file trong parent.
  revealAsDir: boolean;
}

function PathRow({ label, path, revealAsDir }: PathRowProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(path);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Fallback — textarea trick cho old browsers/Tauri webview fallback.
      const ta = document.createElement("textarea");
      ta.value = path;
      document.body.appendChild(ta);
      ta.select();
      try {
        document.execCommand("copy");
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      } catch {
        // ignore
      }
      document.body.removeChild(ta);
    }
  };

  const handleReveal = async () => {
    try {
      // revealItemInDir accept cả file lẫn folder. File → open parent +
      // highlight. Folder → open folder.
      await revealItemInDir(path);
    } catch (e) {
      console.error("[reveal] failed:", e);
    }
  };

  return (
    <div className="rounded-xl border border-surface-8 bg-surface-6 px-3 py-2">
      <div className="mb-1 flex items-center justify-between">
        <span className="text-xs text-white/55">{label}</span>
        <div className="flex shrink-0 items-center gap-1">
          <button
            type="button"
            onClick={handleCopy}
            className={`btn-ripple flex items-center gap-1 rounded-md px-2 py-0.5 text-[11px] font-medium transition-colors ${
              copied
                ? "bg-green-500/20 text-green-300"
                : "bg-shopee-500/20 text-shopee-200 hover:bg-shopee-500/30"
            }`}
            title="Copy đường dẫn"
          >
            <span className="material-symbols-rounded text-sm">
              {copied ? "check" : "content_copy"}
            </span>
            {copied ? "Đã copy" : "Copy"}
          </button>
          <button
            type="button"
            onClick={handleReveal}
            className="btn-ripple flex items-center gap-1 rounded-md bg-surface-1 px-2 py-0.5 text-[11px] font-medium text-white/80 hover:bg-surface-4"
            title={revealAsDir ? "Mở thư mục" : "Mở thư mục chứa file"}
          >
            <span className="material-symbols-rounded text-sm">folder_open</span>
            Mở
          </button>
        </div>
      </div>
      <div
        className="truncate font-mono text-xs text-white/80"
        title={path}
      >
        {path}
      </div>
    </div>
  );
}

interface FeeInputProps {
  label: string;
  value: number;
  onChange: (v: number) => void;
  disabled?: boolean;
}

function FeeInput({ label, value, onChange, disabled = false }: FeeInputProps) {
  return (
    <label
      className={`flex flex-col gap-1 rounded-xl bg-surface-6 px-3 py-2 shadow-elev-1 ${
        disabled ? "opacity-60" : ""
      }`}
      title={disabled ? "Đã khoá — bấm 🔒 ở header để mở" : undefined}
    >
      <span className="text-xs text-white/55">{label}</span>
      <div className="flex items-center gap-1">
        <input
          type="number"
          inputMode="decimal"
          step="0.01"
          min="0"
          max="100"
          value={value}
          disabled={disabled}
          // Wheel scroll mặc định tăng/giảm number input — dễ tai nạn khi user
          // scroll dialog. Blur input khi wheel → mất focus → wheel không sửa.
          onWheel={(e) => (e.target as HTMLInputElement).blur()}
          onChange={(e) => {
            const raw = e.currentTarget.value;
            const n = raw === "" ? 0 : Number(raw);
            onChange(Number.isFinite(n) ? n : 0);
          }}
          className="w-full rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-right text-lg font-semibold tabular-nums text-shopee-300 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500 disabled:cursor-not-allowed"
        />
        <span className="text-sm text-white/60">%</span>
      </div>
    </label>
  );
}

/// Lock toggle cho section nhạy cảm — bảo vệ khỏi click nhầm. Default LOCKED
/// mỗi lần dialog mở; user phải bấm chủ động để mở khoá. State quản lý ở
/// parent (SettingsDialog).
function LockButton({
  locked,
  onToggle,
}: {
  locked: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onToggle}
      className={`btn-ripple flex shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wider transition-colors ${
        locked
          ? "bg-amber-500/15 text-amber-300 hover:bg-amber-500/25"
          : "bg-green-500/20 text-green-300 hover:bg-green-500/30"
      }`}
      title={locked ? "Bấm để mở khoá sửa" : "Bấm để khoá lại"}
      aria-pressed={!locked}
    >
      <span className="material-symbols-rounded text-sm">
        {locked ? "lock" : "lock_open"}
      </span>
      {locked ? "Đã khoá" : "Đang sửa"}
    </button>
  );
}
