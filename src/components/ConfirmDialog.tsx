import { useEffect } from "react";
import { createPortal } from "react-dom";

interface ConfirmDialogProps {
  isOpen: boolean;
  title: string;
  message: string;
  confirmLabel?: string;
  cancelLabel?: string;
  danger?: boolean;
  onConfirm: () => void;
  onClose: () => void;
}

export function ConfirmDialog({
  isOpen,
  title,
  message,
  confirmLabel = "Xác nhận",
  cancelLabel = "Hủy",
  danger = false,
  onConfirm,
  onClose,
}: ConfirmDialogProps) {
  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const confirmCls = danger
    ? "bg-red-500 hover:bg-red-600"
    : "bg-shopee-500 hover:bg-shopee-600";

  const titleCls = danger ? "text-red-300" : "text-white/90";

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="w-full max-w-md overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby="confirm-dialog-title"
      >
        <header className="flex items-center gap-3 px-6 py-4">
          <span
            className={`material-symbols-rounded text-2xl ${
              danger ? "text-red-400" : "text-shopee-400"
            }`}
          >
            {danger ? "warning" : "help"}
          </span>
          <h2
            id="confirm-dialog-title"
            className={`text-lg font-semibold ${titleCls}`}
          >
            {title}
          </h2>
        </header>

        <div className="px-6 pb-4">
          <p className="text-sm leading-relaxed text-white/75">{message}</p>
        </div>

        <footer className="flex justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white/80 hover:bg-white/5"
          >
            {cancelLabel}
          </button>
          <button
            type="button"
            onClick={onConfirm}
            autoFocus
            className={`btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white shadow-elev-2 hover:shadow-elev-4 ${confirmCls}`}
          >
            {confirmLabel}
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
