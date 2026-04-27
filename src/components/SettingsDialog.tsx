import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { revealItemInDir } from "@tauri-apps/plugin-opener";
import type { ProfitFees, Settings } from "../hooks/useSettings";
import { useIsAdmin } from "../hooks/usePremium";
import { AdminDeviceManagement } from "./AdminDeviceManagement";
import { ImportHistorySection } from "./ImportHistorySection";
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
  onSetAutoSyncEnabled: (enabled: boolean) => void;
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
  onSetAutoSyncEnabled,
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

  // Fetch app data paths khi dialog mở. Không fetch ở mount (lazy).
  const [paths, setPaths] = useState<AppDataPaths | null>(null);
  const [pathsError, setPathsError] = useState<string | null>(null);
  const [netLogDir, setNetLogDir] = useState<string | null>(null);
  useEffect(() => {
    if (!isOpen) return;
    let cancelled = false;
    (async () => {
      try {
        const [p, dir] = await Promise.all([
          invoke<AppDataPaths>("get_app_data_paths"),
          invoke<string>("get_net_log_dir"),
        ]);
        if (!cancelled) {
          setPaths(p);
          setNetLogDir(dir);
        }
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

  const isAdmin = useIsAdmin();

  if (!isOpen) return null;

  const sources = Object.entries(settings.clickSources).sort((a, b) =>
    a[0].localeCompare(b[0]),
  );

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
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
              <span className="material-symbols-rounded text-base">
                cloud_sync
              </span>
              Đồng bộ tự động
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Bật để tự động đẩy thay đổi (import/sửa/xóa) lên R2 sau 45 giây.
              Tắt thì phải bấm <b>"Đồng bộ ngay"</b> trong icon đám mây.
            </p>
            <label className="flex cursor-pointer items-center gap-3 rounded-xl bg-surface-6 px-4 py-3 text-sm text-white/85 shadow-elev-1 transition-colors hover:bg-white/5">
              <input
                type="checkbox"
                checked={settings.autoSyncEnabled}
                onChange={(e) =>
                  onSetAutoSyncEnabled(e.currentTarget.checked)
                }
                className="h-4 w-4 accent-shopee-500"
              />
              <span className="flex-1">
                <span className="font-medium">
                  Tự động sync sau mỗi thao tác
                </span>
                <span className="ml-2 text-xs text-white/50">
                  (debounce 45s, gom batch lên R2)
                </span>
              </span>
              {settings.autoSyncEnabled ? (
                <span className="rounded-full bg-green-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-green-300">
                  ON
                </span>
              ) : (
                <span className="rounded-full bg-amber-500/20 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-amber-300">
                  OFF
                </span>
              )}
            </label>
            {!settings.autoSyncEnabled && (
              <div className="mt-2 flex items-start gap-2 rounded-lg border border-amber-500/30 bg-amber-950/20 px-3 py-2 text-xs text-amber-200">
                <span className="material-symbols-rounded mt-0.5 text-sm text-amber-400">
                  warning
                </span>
                <div>
                  Auto-sync đang tắt. Thay đổi local <b>không tự đẩy lên R2</b>{" "}
                  — nếu mở app trên máy khác (hoặc app crash), data mới có thể
                  chưa được backup. Pull từ R2 và RTDB notify vẫn hoạt động.
                </div>
              </div>
            )}
          </section>

          {isAdmin && <AdminDeviceManagement />}

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
                {netLogDir && (
                  <PathRow
                    label="Net log (request history daily)"
                    path={netLogDir}
                    revealAsDir
                  />
                )}
              </div>
            ) : !pathsError ? (
              <div className="text-xs text-white/40">Đang tải...</div>
            ) : null}
            <p className="mt-2 text-[11px] text-white/40">
              Net log: 1 file/ngày (<code>YYYY-MM-DD.log</code>). Lưu mọi
              request FE phát ra (R2, Firebase, admin) — phân tích cost +
              optimize sau. Plain text, dễ grep.
            </p>
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
