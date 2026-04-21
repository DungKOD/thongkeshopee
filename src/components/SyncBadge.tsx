import { useState } from "react";
import type { SyncStatus } from "../hooks/useCloudSync";

interface SyncBadgeProps {
  status: SyncStatus;
  lastSyncAt: Date | null;
  error: string | null;
  onForce: () => Promise<void>;
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
        label: "Kiểm tra Drive...",
        color: "text-white/70",
        spin: true,
      };
    case "syncing":
      return {
        icon: "cloud_sync",
        label: "Đang sync...",
        color: "text-blue-300",
        spin: true,
      };
    case "dirty":
      return {
        icon: "pending",
        label: "Chờ sync",
        color: "text-amber-300",
      };
    case "error":
      return {
        icon: "cloud_off",
        label: "Lỗi sync",
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
          ? `Đã sync ${formatTime(lastSyncAt)}`
          : "Đã sync",
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
  error,
  onForce,
}: SyncBadgeProps) {
  const [showMenu, setShowMenu] = useState(false);
  const display = getDisplay(status, lastSyncAt);
  const canForce = status === "idle" || status === "dirty" || status === "error";

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setShowMenu((v) => !v)}
        className="btn-ripple flex max-w-[180px] items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-medium text-white hover:bg-white/10"
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
              Đồng bộ Drive
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
              {error && (
                <div
                  className="mt-2 rounded border border-red-500/40 bg-red-900/20 p-2 text-red-200"
                  title={error}
                >
                  <div className="truncate">{error}</div>
                </div>
              )}
            </div>
            <div className="border-t border-surface-8 p-2">
              <button
                type="button"
                onClick={async () => {
                  setShowMenu(false);
                  await onForce();
                }}
                disabled={!canForce}
                className="btn-ripple flex w-full items-center justify-center gap-1.5 rounded bg-shopee-500 px-3 py-1.5 text-xs font-medium text-white hover:bg-shopee-600 disabled:opacity-50"
              >
                <span className="material-symbols-rounded text-sm">
                  cloud_upload
                </span>
                Sync ngay
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
