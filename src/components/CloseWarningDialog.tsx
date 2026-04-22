import { useEffect } from "react";
import { createPortal } from "react-dom";

interface CloseWarningDialogProps {
  isOpen: boolean;
  syncing: boolean;
  /// Label actions — customize cho "tắt app" vs "đăng xuất" vs khác.
  /// Default: tắt app.
  title?: string;
  description?: string;
  syncLabel?: string;
  syncBusyLabel?: string;
  anywayLabel?: string;
  /// User chọn đồng bộ trước khi tắt.
  onSyncAndClose: () => void;
  /// User chọn tắt luôn (mất dữ liệu chưa sync).
  onCloseAnyway: () => void;
  /// User hủy — không tắt.
  onCancel: () => void;
}

/// Cảnh báo khi user tắt app mà DB còn `dirty` (mutation chưa upload R2).
/// 3 lựa chọn: đồng bộ + tắt, tắt luôn, huỷ.
export function CloseWarningDialog({
  isOpen,
  syncing,
  title = "Data chưa đồng bộ lên R2",
  description = "Vẫn còn thay đổi chưa upload lên R2. Nếu tắt app bây giờ, data mới sẽ ở local — máy khác chưa thấy.",
  syncLabel = "Đồng bộ lên R2 rồi tắt",
  syncBusyLabel = "Đang đồng bộ lên R2...",
  anywayLabel = "Tắt luôn (chấp nhận mất đồng bộ R2)",
  onSyncAndClose,
  onCloseAnyway,
  onCancel,
}: CloseWarningDialogProps) {
  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !syncing) onCancel();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onCancel, syncing]);

  if (!isOpen) return null;

  return createPortal(
    <div
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget && !syncing) onCancel();
      }}
    >
      <div
        className="w-[min(92vw,480px)] rounded-xl bg-surface-2 p-6 shadow-elev-16"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="close-warn-title"
      >
        <div className="mb-4 flex items-start gap-3">
          <span className="material-symbols-rounded text-3xl text-amber-400">
            warning
          </span>
          <div className="flex-1">
            <h2
              id="close-warn-title"
              className="text-lg font-semibold text-white"
            >
              {title}
            </h2>
            <p className="mt-1 text-sm text-white/70">{description}</p>
          </div>
        </div>

        <div className="flex flex-col gap-2">
          <button
            type="button"
            onClick={onSyncAndClose}
            disabled={syncing}
            className="btn-ripple flex items-center justify-center gap-2 rounded-lg bg-shopee-500 px-4 py-2.5 text-sm font-semibold text-white shadow-elev-2 hover:bg-shopee-600 disabled:cursor-not-allowed disabled:opacity-60"
          >
            <span
              className={`material-symbols-rounded text-base ${syncing ? "animate-spin" : ""}`}
            >
              {syncing ? "sync" : "cloud_upload"}
            </span>
            {syncing ? syncBusyLabel : syncLabel}
          </button>
          <button
            type="button"
            onClick={onCloseAnyway}
            disabled={syncing}
            className="btn-ripple rounded-lg border border-red-500/50 bg-red-500/10 px-4 py-2.5 text-sm font-medium text-red-300 hover:bg-red-500/20 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {anywayLabel}
          </button>
          <button
            type="button"
            onClick={onCancel}
            disabled={syncing}
            className="btn-ripple rounded-lg px-4 py-2.5 text-sm text-white/70 hover:bg-white/5 disabled:cursor-not-allowed disabled:opacity-60"
          >
            Hủy — quay lại app
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
