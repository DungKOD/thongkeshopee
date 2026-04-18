import { useState } from "react";
import { ConfirmDialog } from "./ConfirmDialog";

interface PendingChangesBarProps {
  count: number;
  onCommit: () => Promise<void>;
  onCancel: () => void;
}

/**
 * Floating bar bottom hiển thị số thay đổi pending. Hiện ra khi có ít nhất
 * 1 row/day đang gạch ngang. "Lưu thay đổi" mở ConfirmDialog danger (có shake)
 * trước khi commit vào DB.
 */
export function PendingChangesBar({
  count,
  onCommit,
  onCancel,
}: PendingChangesBarProps) {
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [committing, setCommitting] = useState(false);

  if (count === 0) return null;

  const handleCommit = async () => {
    setCommitting(true);
    try {
      await onCommit();
      setConfirmOpen(false);
    } finally {
      setCommitting(false);
    }
  };

  return (
    <>
      <div
        role="region"
        aria-label="Thay đổi chưa lưu"
        className="fixed inset-x-4 bottom-4 z-40 mx-auto flex max-w-3xl items-center justify-between gap-3 rounded-xl border border-amber-500/60 bg-surface-8 px-5 py-3 shadow-elev-16"
      >
        <div className="flex items-center gap-3">
          <span className="flex h-9 w-9 items-center justify-center rounded-full bg-amber-500 text-base font-black text-white">
            {count}
          </span>
          <div>
            <p className="text-sm font-semibold text-amber-200">
              Có {count} thay đổi chưa lưu
            </p>
            <p className="text-xs text-white/60">
              Bấm "Lưu thay đổi" để xóa khỏi database, hoặc "Hủy" để giữ nguyên.
            </p>
          </div>
        </div>
        <div className="flex shrink-0 gap-2">
          <button
            type="button"
            onClick={onCancel}
            className="btn-ripple rounded-lg px-4 py-2 text-sm font-medium text-white/80 hover:bg-white/5"
          >
            Hủy
          </button>
          <button
            type="button"
            onClick={() => setConfirmOpen(true)}
            className="btn-ripple flex items-center gap-2 rounded-lg bg-red-500 px-4 py-2 text-sm font-semibold text-white shadow-elev-2 hover:bg-red-600 hover:shadow-elev-4"
          >
            <span className="material-symbols-rounded text-base">save</span>
            Lưu thay đổi
          </button>
        </div>
      </div>

      <ConfirmDialog
        isOpen={confirmOpen}
        title={`Xóa ${count} mục đã chọn`}
        message={`Toàn bộ ${count} mục bạn đang đánh dấu sẽ bị xóa khỏi database và KHÔNG thể khôi phục. Raw CSV gốc tương ứng cũng sẽ bị xóa. Tiếp tục?`}
        confirmLabel={committing ? "Đang xóa..." : "Xác nhận xóa"}
        cancelLabel="Quay lại"
        danger
        onConfirm={handleCommit}
        onClose={() => {
          if (!committing) setConfirmOpen(false);
        }}
      />
    </>
  );
}
