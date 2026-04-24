import { useState } from "react";
import type { LastSyncStats, SyncStatus } from "../hooks/useCloudSync";
import { MySyncLogDialog } from "./MySyncLogDialog";

interface SyncBadgeProps {
  status: SyncStatus;
  lastSyncAt: Date | null;
  lastSyncStats: LastSyncStats | null;
  /// True = máy khác vừa push (RTDB event chưa apply). Enable nút sync ở
  /// status=idle để user chủ động pull nếu muốn, dù auto đã fire.
  hasRemoteChangePending: boolean;
  error: string | null;
  onForce: () => Promise<void>;
}

function fmtBytes(n: number): string {
  if (n === 0) return "0 B";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(2)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

interface Display {
  icon: string;
  label: string;
  color: string;
  spin?: boolean;
}

function getDisplay(
  status: SyncStatus,
  lastSyncAt: Date | null,
): Display {
  switch (status) {
    case "checking":
      return {
        icon: "cloud_sync",
        label: "Kiểm tra manifest...",
        color: "text-white/70",
        spin: true,
      };
    case "bootstrap":
      return {
        icon: "cloud_download",
        label: "Đang khởi tạo từ R2...",
        color: "text-blue-300",
        spin: true,
      };
    case "syncing":
      return {
        icon: "cloud_sync",
        label: "Đang đồng bộ R2...",
        color: "text-blue-300",
        spin: true,
      };
    case "dirty":
      return {
        icon: "pending",
        label: "Chờ đồng bộ R2",
        color: "text-amber-300",
      };
    case "error":
      return {
        icon: "cloud_off",
        label: "Lỗi đồng bộ R2",
        color: "text-red-300",
      };
    case "offline":
      return {
        icon: "wifi_off",
        label: "Offline",
        color: "text-white/50",
      };
    case "idle":
    default:
      return {
        icon: "cloud_done",
        label: lastSyncAt
          ? `Đã đồng bộ R2 ${formatTime(lastSyncAt)}`
          : "Đã đồng bộ R2",
        color: "text-green-300",
      };
  }
}

function formatTime(d: Date): string {
  const now = Date.now();
  const diff = Math.floor((now - d.getTime()) / 1000);
  if (diff < 60) return "vừa xong";
  if (diff < 3600) return `${Math.floor(diff / 60)}p trước`;
  return d.toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" });
}

export function SyncBadge({
  status,
  lastSyncAt,
  lastSyncStats,
  hasRemoteChangePending,
  error,
  onForce,
}: SyncBadgeProps) {
  const [showMenu, setShowMenu] = useState(false);
  const [logOpen, setLogOpen] = useState(false);
  const display = getDisplay(status, lastSyncAt);
  // Nút force enable khi:
  // - dirty: local có mutation chưa push
  // - error: retry cần thiết
  // - idle + hasRemoteChangePending: máy khác vừa push, user muốn pull
  // Disable khi idle sạch hai phía — không có gì để đồng bộ, tiết kiệm request.
  const canForce =
    status === "dirty" ||
    status === "error" ||
    (status === "idle" && hasRemoteChangePending);

  // Dirty/error → highlight container (ring + subtle pulse) để user thấy
  // rõ DB chưa sync, khuyến khích click "Sync ngay" hoặc đợi auto.
  const needsAttention = status === "dirty" || status === "error";

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setShowMenu((v) => !v)}
        className={`btn-ripple flex max-w-[180px] items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-medium text-white transition-all ${
          needsAttention
            ? "bg-amber-500/15 ring-1 ring-amber-400/60 animate-pulse hover:bg-amber-500/25 hover:ring-amber-400"
            : "hover:bg-white/10"
        }`}
        title={
          error ??
          (lastSyncAt ? `Sync lần cuối: ${lastSyncAt.toLocaleString("vi-VN")}` : "")
        }
      >
        <span
          className={`material-symbols-rounded text-base ${display.color} ${display.spin ? "animate-spin" : ""}`}
        >
          {display.icon}
        </span>
        <span
          className={`truncate ${display.color}`}
          title={display.label}
        >
          {display.label}
        </span>
      </button>

      {showMenu && (
        <>
          <div
            className="fixed inset-0 z-30"
            onClick={() => setShowMenu(false)}
          />
          <div className="absolute right-0 top-full z-40 mt-1 w-64 rounded-lg border border-surface-8 bg-surface-2 shadow-elev-16">
            <div className="border-b border-surface-8 px-3 py-2 text-xs text-white/50">
              Đồng bộ lên R2
            </div>
            <div className="px-3 py-2 text-xs">
              <div className="flex items-center gap-2">
                <span
                  className={`material-symbols-rounded text-sm ${display.color}`}
                >
                  {display.icon}
                </span>
                <span className="text-white/80">{display.label}</span>
              </div>
              {lastSyncAt && (
                <div className="mt-1 text-white/50">
                  Lần cuối: {lastSyncAt.toLocaleString("vi-VN")}
                </div>
              )}
              {lastSyncStats && (
                <div className="mt-2 grid grid-cols-2 gap-2 rounded border border-surface-8 bg-surface-1 p-2 text-[11px]">
                  <div
                    className="flex items-center gap-1"
                    title={`Downloaded ${lastSyncStats.pulledDeltas} delta file(s)`}
                  >
                    <span className="material-symbols-rounded text-xs text-blue-300">
                      arrow_downward
                    </span>
                    <span className="text-white/50">Tải về</span>
                    <span className="ml-auto font-mono tabular-nums text-blue-200">
                      {fmtBytes(lastSyncStats.downloadBytes)}
                    </span>
                  </div>
                  <div
                    className="flex items-center gap-1"
                    title={`Uploaded ${lastSyncStats.pushedDeltas} delta file(s), skipped ${lastSyncStats.skippedIdentical} identical`}
                  >
                    <span className="material-symbols-rounded text-xs text-green-300">
                      arrow_upward
                    </span>
                    <span className="text-white/50">Tải lên</span>
                    <span className="ml-auto font-mono tabular-nums text-green-200">
                      {fmtBytes(lastSyncStats.uploadBytes)}
                    </span>
                  </div>
                  {(lastSyncStats.pulledDeltas > 0 ||
                    lastSyncStats.pushedDeltas > 0 ||
                    lastSyncStats.skippedIdentical > 0) && (
                    <div className="col-span-2 flex items-center gap-2 text-[10px] text-white/40">
                      <span>
                        {lastSyncStats.pulledDeltas} pull /{" "}
                        {lastSyncStats.pushedDeltas} push
                      </span>
                      {lastSyncStats.skippedIdentical > 0 && (
                        <span title="Table không đổi nội dung so với upload trước → skip">
                          · {lastSyncStats.skippedIdentical} skip
                        </span>
                      )}
                    </div>
                  )}
                </div>
              )}
              {error && (
                <div
                  className="mt-2 rounded border border-red-500/40 bg-red-900/20 p-2 text-red-200"
                  title={error}
                >
                  <div className="truncate">{error}</div>
                </div>
              )}
            </div>
            <div className="flex flex-col gap-1 border-t border-surface-8 p-2">
              <button
                type="button"
                onClick={async () => {
                  setShowMenu(false);
                  await onForce();
                }}
                disabled={!canForce}
                title={
                  canForce
                    ? "Đồng bộ thay đổi giữa máy này và R2"
                    : "Không có thay đổi để đồng bộ (cả local và remote đều sạch)"
                }
                className="btn-ripple flex w-full items-center justify-center gap-1.5 rounded bg-shopee-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-shopee-600 disabled:cursor-not-allowed disabled:opacity-40"
              >
                <span className="material-symbols-rounded text-sm">
                  cloud_upload
                </span>
                {canForce ? "Đồng bộ lên R2 ngay" : "Không có thay đổi"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setShowMenu(false);
                  setLogOpen(true);
                }}
                className="btn-ripple flex w-full items-center justify-center gap-1.5 rounded border border-surface-8 bg-surface-4 px-3 py-1.5 text-xs font-medium text-white/85 hover:bg-surface-6"
                title="Xem lịch sử event sync v9 lưu trên máy"
              >
                <span className="material-symbols-rounded text-sm">
                  history
                </span>
                Xem sync log
              </button>
            </div>
          </div>
        </>
      )}
      <MySyncLogDialog isOpen={logOpen} onClose={() => setLogOpen(false)} />
    </div>
  );
}
