import { useCallback, useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { useAccounts, isDefaultAccount } from "../contexts/AccountContext";
import {
  countFbLinkedToAccount,
  createShopeeAccount,
  deleteShopeeAccount,
  renameShopeeAccount,
  type ShopeeAccount,
} from "../lib/accounts";

interface AccountManagerDialogProps {
  isOpen: boolean;
  onClose: () => void;
  /// Gọi sau khi xóa account (data raw bị wipe) → parent refetch days/rows
  /// để UI chính không hiện stale data. App.tsx wire markMutation + refetch.
  onDataChanged?: () => void;
}

const DEFAULT_COLORS = [
  "#ff6b35",
  "#f7c59f",
  "#efefd0",
  "#9dd9d2",
  "#4f86c6",
  "#845ec2",
  "#d65db1",
  "#ff6f91",
  "#ff9671",
  "#ffc75f",
];

export function AccountManagerDialog({
  isOpen,
  onClose,
  onDataChanged,
}: AccountManagerDialogProps) {
  const { accounts, refresh } = useAccounts();
  const [newName, setNewName] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState("");
  // Confirm dialog: khi user bấm xóa account (có/không data).
  const [confirmDelete, setConfirmDelete] = useState<ShopeeAccount | null>(null);
  /// Số FB ads khớp sub_id với account đang preview xóa (null = đang load).
  const [fbLinkedCount, setFbLinkedCount] = useState<number | null>(null);
  /// Checkbox "xóa luôn FB ads khớp" — default TÍCH khi có FB khớp.
  const [alsoDeleteFb, setAlsoDeleteFb] = useState(true);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !saving) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose, saving]);

  const handleCreate = useCallback(async () => {
    const name = newName.trim();
    if (!name) return;
    setSaving(true);
    setError(null);
    try {
      const color = DEFAULT_COLORS[(accounts?.length ?? 0) % DEFAULT_COLORS.length];
      await createShopeeAccount(name, color);
      setNewName("");
      await refresh();
      // Account thêm vào DB → sync_state dirty qua trigger, nhưng FE cần
      // markMutation để debounce auto-sync kick in ngay.
      onDataChanged?.();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [newName, accounts, refresh, onDataChanged]);

  const handleRename = useCallback(
    async (id: number) => {
      const name = editName.trim();
      if (!name) return;
      setSaving(true);
      setError(null);
      try {
        await renameShopeeAccount(id, name);
        setEditingId(null);
        await refresh();
        onDataChanged?.();
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setSaving(false);
      }
    },
    [editName, refresh, onDataChanged],
  );

  const handleDelete = useCallback((account: ShopeeAccount) => {
    // Mọi case (có/không data) đều qua ConfirmDialog shake để user xác nhận.
    setConfirmDelete(account);
    setFbLinkedCount(null);
    setAlsoDeleteFb(true);
  }, []);

  // Khi mở confirm dialog, fetch số FB ads sẽ bị cuốn theo.
  useEffect(() => {
    if (!confirmDelete) return;
    let cancelled = false;
    setFbLinkedCount(null);
    countFbLinkedToAccount(confirmDelete.id)
      .then((n) => {
        if (!cancelled) setFbLinkedCount(n);
      })
      .catch((e) => {
        if (!cancelled) {
          console.error("[accounts] count FB linked failed:", e);
          setFbLinkedCount(0);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [confirmDelete]);

  const handleConfirmDelete = useCallback(async () => {
    if (!confirmDelete) return;
    setSaving(true);
    setError(null);
    try {
      // Nếu không có FB khớp thì flag không quan trọng, gửi false cũng OK.
      const flag = (fbLinkedCount ?? 0) > 0 ? alsoDeleteFb : false;
      await deleteShopeeAccount(confirmDelete.id, flag);
      setConfirmDelete(null);
      await refresh();
      // Raw data của account bị xóa → main UI phải refetch, nếu không hiện stale.
      onDataChanged?.();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [confirmDelete, fbLinkedCount, alsoDeleteFb, refresh, onDataChanged]);

  if (!isOpen) return null;

  const content = (
    <div
      onMouseDown={(e) => {
        if (e.target === e.currentTarget && !saving) onClose();
      }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
    >
      <div className="w-[min(92vw,540px)] overflow-hidden rounded-xl bg-surface-2 shadow-elev-4">
        <header className="flex items-center justify-between bg-shopee-500 px-6 py-4">
          <h2 className="flex items-center gap-2 text-lg font-semibold text-white">
            <span className="material-symbols-rounded">storefront</span>
            Quản lý TK Shopee
          </h2>
          <button
            onClick={onClose}
            className="rounded-lg p-1 text-white/80 hover:bg-white/15 hover:text-white"
            disabled={saving}
            title="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <div className="p-6">
        {error && (
          <div className="mb-4 rounded-md border border-red-500/50 bg-red-500/10 px-3 py-2 text-sm text-red-300">
            {error}
          </div>
        )}

        <section className="mb-5">
          <h3 className="mb-2 text-xs font-medium uppercase tracking-wider text-white/60">
            Tạo account mới
          </h3>
          <div className="flex gap-2">
            <input
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="Tên account"
              disabled={saving}
              onKeyDown={(e) => {
                if (e.key === "Enter") void handleCreate();
              }}
              className="flex-1 rounded-md border border-surface-8 bg-surface-1 px-3 py-2 text-sm text-white/90 placeholder:text-white/30 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
            />
            <button
              onClick={handleCreate}
              disabled={saving || !newName.trim()}
              className="rounded-md bg-shopee-500 px-4 py-2 text-sm font-medium text-white hover:bg-shopee-600 disabled:opacity-40"
            >
              Thêm
            </button>
          </div>
        </section>

        <section>
          <h3 className="mb-2 text-xs font-medium uppercase tracking-wider text-white/60">
            Danh sách ({accounts?.length ?? 0})
          </h3>
          <ul className="space-y-1.5">
            {(accounts ?? []).map((a) => {
              const isEditing = editingId === a.id;
              const isDefault = isDefaultAccount(a);
              return (
                <li
                  key={a.id}
                  className={`flex items-center gap-3 rounded-md border border-surface-8 bg-surface-1 px-3 py-2 ${
                    isDefault ? "opacity-70" : ""
                  }`}
                >
                  <span
                    className="h-3 w-3 shrink-0 rounded-full"
                    style={{ backgroundColor: a.color ?? "#888" }}
                  />
                  {isEditing ? (
                    <input
                      type="text"
                      value={editName}
                      onChange={(e) => setEditName(e.target.value)}
                      autoFocus
                      onBlur={() => void handleRename(a.id)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") void handleRename(a.id);
                        if (e.key === "Escape") setEditingId(null);
                      }}
                      className="flex-1 rounded border border-shopee-500 bg-surface-0 px-2 py-1 text-sm text-white/90"
                    />
                  ) : (
                    <span className="flex-1 text-sm text-white/90">
                      {a.name}
                      {isDefault && (
                        <span
                          className="ml-2 rounded bg-white/10 px-1.5 py-0.5 text-[10px] uppercase tracking-wider text-white/60"
                          title="Bucket hệ thống — chứa sub_id chưa gán TK nào. Không thể sửa/xóa."
                        >
                          hệ thống
                        </span>
                      )}
                    </span>
                  )}
                  <span className="text-xs text-white/50">
                    {a.rowCount} dòng
                  </span>
                  <button
                    onClick={() => {
                      setEditingId(a.id);
                      setEditName(a.name);
                    }}
                    disabled={saving || isEditing || isDefault}
                    className="rounded p-1 text-white/60 hover:bg-white/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-30 disabled:hover:bg-transparent"
                    title={
                      isDefault
                        ? "TK hệ thống — không đổi tên được"
                        : "Đổi tên"
                    }
                  >
                    <span className="material-symbols-rounded text-base">
                      edit
                    </span>
                  </button>
                  <button
                    onClick={() => void handleDelete(a)}
                    disabled={saving || isDefault}
                    className="rounded p-1 text-red-400 hover:bg-red-500/20 hover:text-red-300 disabled:cursor-not-allowed disabled:opacity-30 disabled:hover:bg-transparent"
                    title={
                      isDefault
                        ? "TK hệ thống — không xóa được"
                        : a.rowCount > 0
                        ? "Xóa — sẽ hỏi chuyển data sang account khác"
                        : "Xóa"
                    }
                  >
                    <span className="material-symbols-rounded text-base">
                      delete
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
        </div>
      </div>

      {confirmDelete && (
        <div
          onMouseDown={(e) => e.stopPropagation()}
          className="fixed inset-0 z-[60] flex items-center justify-center bg-black/70 p-4"
        >
          <div className="w-[min(92vw,480px)] animate-shake overflow-hidden rounded-2xl border-2 border-red-500 bg-surface-4 shadow-elev-24 shadow-red-900/40">
            <div className="flex items-start gap-3 bg-red-950/60 px-6 py-4">
              <span className="mt-0.5 flex h-11 w-11 shrink-0 items-center justify-center rounded-full bg-red-500 text-2xl font-black text-white shadow-lg shadow-red-900/50">
                !
              </span>
              <div className="flex-1">
                <p className="text-xs font-bold uppercase tracking-widest text-red-300">
                  Cảnh báo — Không thể hoàn tác
                </p>
                <h2 className="mt-0.5 text-xl font-bold text-white">
                  Xóa "{confirmDelete.name}"
                </h2>
              </div>
            </div>
            <div className="space-y-3 px-6 py-4">
              <p className="text-sm leading-relaxed text-white/90">
                {confirmDelete.rowCount > 0 ? (
                  <>
                    Account này đang có{" "}
                    <b className="text-red-300">{confirmDelete.rowCount} dòng</b>{" "}
                    data. Bấm xóa sẽ <b>xóa vĩnh viễn</b> cả account và toàn bộ
                    data (clicks, orders, manual entries).
                  </>
                ) : (
                  <>Xóa account này khỏi hệ thống.</>
                )}
              </p>
              {fbLinkedCount === null ? (
                <p className="text-xs italic text-white/40">
                  Đang kiểm tra FB ads khớp sub_id...
                </p>
              ) : fbLinkedCount > 0 ? (
                <label className="flex cursor-pointer items-start gap-2 rounded-md border border-red-500/40 bg-red-950/30 px-3 py-2 text-sm text-white/90 hover:bg-red-950/50">
                  <input
                    type="checkbox"
                    checked={alsoDeleteFb}
                    onChange={(e) => setAlsoDeleteFb(e.target.checked)}
                    disabled={saving}
                    className="mt-0.5 h-4 w-4 accent-red-500"
                  />
                  <span className="flex-1">
                    Xóa luôn{" "}
                    <b className="text-red-300">{fbLinkedCount} dòng FB ads</b>{" "}
                    khớp sub_id với account này.
                    <span className="mt-0.5 block text-xs text-white/50">
                      Bỏ tích nếu muốn giữ FB ads để xóa tay sau.
                    </span>
                  </span>
                </label>
              ) : null}
            </div>
            <footer className="flex justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
              <button
                onClick={() => setConfirmDelete(null)}
                disabled={saving}
                className="btn-ripple rounded-lg px-5 py-2 text-sm font-medium text-white/80 hover:bg-white/5"
              >
                Huỷ
              </button>
              <button
                onClick={handleConfirmDelete}
                disabled={saving}
                autoFocus
                className="btn-ripple rounded-lg bg-red-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 hover:bg-red-600 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Xóa vĩnh viễn
              </button>
            </footer>
          </div>
        </div>
      )}
    </div>
  );

  return createPortal(content, document.body);
}
