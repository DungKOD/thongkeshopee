import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { revealItemInDir } from "@tauri-apps/plugin-opener";
import type { ProfitFees, Settings } from "../hooks/useSettings";
import { ImportHistorySection } from "./ImportHistorySection";
import { invoke } from "../lib/tauri";
import { getAuthToken } from "../lib/firebase";
import { syncV9NuclearReset } from "../lib/sync_v9";

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
            <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
              <span className="material-symbols-rounded text-base">
                payments
              </span>
              Phí khấu trừ lợi nhuận
            </h3>
            <p className="mb-3 text-xs text-white/50">
              Net = Hoa hồng × (1 − Thuế) − Hoa hồng <i>pending</i> × Dự phòng
            </p>
            <div className="grid grid-cols-2 gap-3">
              <FeeInput
                label="Thuế + phí sàn"
                value={settings.profitFees.taxAndPlatformRate}
                onChange={(v) => onSetProfitFee("taxAndPlatformRate", v)}
              />
              <FeeInput
                label="Dự phòng hoàn/hủy"
                value={settings.profitFees.returnReserveRate}
                onChange={(v) => onSetProfitFee("returnReserveRate", v)}
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
                filter_list
              </span>
              Nguồn Click Shopee
            </h3>
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
              <ul className="divide-y divide-surface-8 overflow-hidden rounded-xl bg-surface-6">
                {sources.map(([source, enabled]) => (
                  <li key={source}>
                    <label className="flex cursor-pointer items-center gap-3 px-4 py-3 text-sm text-white/85 transition-colors hover:bg-white/5">
                      <input
                        type="checkbox"
                        checked={enabled}
                        onChange={(e) =>
                          onToggleClickSource(source, e.currentTarget.checked)
                        }
                        className="h-4 w-4 accent-shopee-500"
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

          {/* Reset sync state — recovery khi R2 có delta cũ không tương thích. */}
          <ResetSyncSection />

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

/// Section reset sync state — fix case R2 có delta cũ mismatch FK sau
/// migration schema. Không xóa data local, chỉ reset metadata sync.
function ResetSyncSection() {
  const [busy, setBusy] = useState(false);
  const [nuclearBusy, setNuclearBusy] = useState(false);
  const [done, setDone] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const onReset = async () => {
    const ok = confirm(
      "Reset sync state?\n\n" +
        "• DATA LOCAL GIỮ NGUYÊN (an toàn).\n" +
        "• Chỉ reset cursor + manifest metadata.\n" +
        "• LẦN SYNC TIẾP THEO sẽ re-push toàn bộ data từ đầu.\n\n" +
        "LƯU Ý: sau reset, cần xóa dữ liệu cũ trên R2 (Cloudflare dashboard) " +
        "hoặc click 'Xóa R2 + Reset (1-click)' để tự động.",
    );
    if (!ok) return;
    setBusy(true);
    setError(null);
    try {
      await invoke<void>("sync_v9_reset_local_state");
      setDone("Đã reset local. Cần wipe R2 thủ công rồi sync lại.");
      setTimeout(() => setDone(null), 5000);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  const onNuclear = async () => {
    const ok = confirm(
      "⚠️ XÓA R2 + Reset local state (1-click)?\n\n" +
        "• Data R2 của BẠN sẽ bị archive (giữ 30 ngày rồi xóa).\n" +
        "• Data local GIỮ NGUYÊN — không mất gì.\n" +
        "• Local sync state reset — lần sync tiếp sẽ re-push toàn bộ.\n\n" +
        "Dùng khi: lỗi 'FOREIGN KEY constraint failed' do delta R2 cũ.\n\n" +
        "Tiếp tục?",
    );
    if (!ok) return;
    setNuclearBusy(true);
    setError(null);
    try {
      const idToken = await getAuthToken();
      await syncV9NuclearReset(idToken);
      setDone(
        "Đã xóa R2 + reset local. Click 'Đồng bộ R2 ngay' để push data fresh.",
      );
      setTimeout(() => setDone(null), 8000);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setNuclearBusy(false);
    }
  };

  return (
    <section>
      <h3 className="mb-1 flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
        <span className="material-symbols-rounded text-base">
          restart_alt
        </span>
        Reset sync state (debug)
      </h3>
      <p className="mb-3 text-xs text-white/50">
        Chỉ dùng khi gặp lỗi sync "FK constraint failed" do delta cũ trên R2
        không còn khớp schema mới. Reset không xóa data local, chỉ reset
        metadata để lần sync tiếp re-push toàn bộ từ đầu.
      </p>
      <div className="rounded-xl border border-red-500/40 bg-red-950/20 p-3">
        <div className="flex items-start gap-2">
          <span className="material-symbols-rounded mt-0.5 text-base text-red-400">
            priority_high
          </span>
          <div className="flex-1 text-xs text-red-100/90">
            <b>Khuyến nghị: dùng "Xóa R2 + Reset (1-click)"</b> để fix nhanh.
            Data local sẽ giữ nguyên, chỉ xóa R2 + reset metadata. Sau đó
            click "Đồng bộ R2" để push fresh lên R2.
          </div>
        </div>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => void onNuclear()}
            disabled={nuclearBusy || busy}
            className="btn-ripple flex items-center gap-1.5 rounded-md bg-red-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-red-700 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <span
              className={`material-symbols-rounded text-sm ${nuclearBusy ? "animate-spin" : ""}`}
            >
              {nuclearBusy ? "sync" : "delete_sweep"}
            </span>
            {nuclearBusy ? "Đang xóa R2..." : "Xóa R2 + Reset (1-click)"}
          </button>
          <button
            type="button"
            onClick={() => void onReset()}
            disabled={busy || nuclearBusy}
            className="btn-ripple flex items-center gap-1.5 rounded-md border border-amber-500/60 bg-amber-900/30 px-3 py-1.5 text-xs font-medium text-amber-200 hover:bg-amber-900/50 disabled:cursor-not-allowed disabled:opacity-50"
            title="Chỉ reset local state, không xóa R2. Phải vào Cloudflare dashboard xóa thủ công sau."
          >
            <span
              className={`material-symbols-rounded text-sm ${busy ? "animate-spin" : ""}`}
            >
              {busy ? "sync" : "restart_alt"}
            </span>
            Chỉ reset local
          </button>
          {done && (
            <span className="flex items-center gap-1 text-xs text-green-300">
              <span className="material-symbols-rounded text-sm">
                check_circle
              </span>
              {done}
            </span>
          )}
          {error && (
            <span className="text-xs text-red-300" title={error}>
              Lỗi: {error}
            </span>
          )}
        </div>
      </div>
    </section>
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
}

function FeeInput({ label, value, onChange }: FeeInputProps) {
  return (
    <label className="flex flex-col gap-1 rounded-xl bg-surface-6 px-3 py-2 shadow-elev-1">
      <span className="text-xs text-white/55">{label}</span>
      <div className="flex items-center gap-1">
        <input
          type="number"
          inputMode="decimal"
          step="0.01"
          min="0"
          max="100"
          value={value}
          onChange={(e) => {
            const raw = e.currentTarget.value;
            const n = raw === "" ? 0 : Number(raw);
            onChange(Number.isFinite(n) ? n : 0);
          }}
          className="w-full rounded-md border border-surface-8 bg-surface-1 px-2 py-1 text-right text-lg font-semibold tabular-nums text-shopee-300 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
        />
        <span className="text-sm text-white/60">%</span>
      </div>
    </label>
  );
}
