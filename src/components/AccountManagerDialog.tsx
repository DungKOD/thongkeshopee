import { useCallback, useEffect, useState } from "react";
import { createPortal } from "react-dom";
import { useAccounts, DEFAULT_ACCOUNT_ID } from "../contexts/AccountContext";
import {
  createShopeeAccount,
  deleteShopeeAccount,
  reassignShopeeAccountData,
  renameShopeeAccount,
  type ShopeeAccount,
} from "../lib/accounts";

interface AccountManagerDialogProps {
  isOpen: boolean;
  onClose: () => void;
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
}: AccountManagerDialogProps) {
  const { accounts, refresh } = useAccounts();
  const [newName, setNewName] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState("");
  // Reassign dialog state: khi user bấm xóa account còn data → hỏi chuyển sang đâu.
  const [reassignFrom, setReassignFrom] = useState<ShopeeAccount | null>(null);
  const [reassignTo, setReassignTo] = useState<number | null>(null);

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
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [newName, accounts, refresh]);

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
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setSaving(false);
      }
    },
    [editName, refresh],
  );

  const handleDelete = useCallback(
    async (account: ShopeeAccount) => {
      if (account.rowCount > 0) {
        // Còn data → hỏi reassign.
        setReassignFrom(account);
        const others = (accounts ?? []).filter((a) => a.id !== account.id);
        setReassignTo(others.length > 0 ? others[0].id : null);
        return;
      }
      if (!confirm(`Xóa account "${account.name}"?`)) return;
      setSaving(true);
      setError(null);
      try {
        await deleteShopeeAccount(account.id);
        await refresh();
      } catch (e) {
        setError((e as Error).message);
      } finally {
        setSaving(false);
      }
    },
    [accounts, refresh],
  );

  const handleConfirmReassign = useCallback(async () => {
    if (!reassignFrom || reassignTo === null) return;
    setSaving(true);
    setError(null);
    try {
      await reassignShopeeAccountData(reassignFrom.id, reassignTo);
      await deleteShopeeAccount(reassignFrom.id);
      setReassignFrom(null);
      setReassignTo(null);
      await refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  }, [reassignFrom, reassignTo, refresh]);

  if (!isOpen) return null;

  const content = (
    <div
      onMouseDown={(e) => {
        if (e.target === e.currentTarget && !saving) onClose();
      }}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm"
    >
      <div className="w-[min(92vw,540px)] rounded-xl bg-surface-2 p-6 shadow-elev-4">
        <header className="mb-5 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-white">
            Quản lý TK Shopee
          </h2>
          <button
            onClick={onClose}
            className="rounded-lg p-1 text-white/60 hover:bg-white/10 hover:text-white"
            disabled={saving}
            title="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

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
              placeholder="Tên account (vd: TK Cao Thắng)"
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
              const isDefault = a.id === DEFAULT_ACCOUNT_ID;
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

      {reassignFrom && (
        <div
          onMouseDown={(e) => e.stopPropagation()}
          className="fixed inset-0 z-[60] flex items-center justify-center bg-black/70"
        >
          <div className="w-[min(92vw,460px)] rounded-xl bg-surface-2 p-5 shadow-elev-4">
            <h3 className="mb-3 text-base font-semibold text-white">
              Xóa "{reassignFrom.name}" ({reassignFrom.rowCount} dòng)
            </h3>
            <p className="mb-4 text-sm text-white/70">
              Chọn account nhận data:
            </p>
            <select
              value={reassignTo ?? ""}
              onChange={(e) => setReassignTo(Number(e.target.value))}
              className="mb-4 w-full rounded-md border border-surface-8 bg-surface-1 px-3 py-2 text-sm text-white/90 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
            >
              {(accounts ?? [])
                .filter(
                  (a) =>
                    a.id !== reassignFrom.id && a.id !== DEFAULT_ACCOUNT_ID,
                )
                .map((a) => (
                  <option key={a.id} value={a.id}>
                    {a.name}
                  </option>
                ))}
            </select>
            <div className="flex justify-end gap-2">
              <button
                onClick={() => {
                  setReassignFrom(null);
                  setReassignTo(null);
                }}
                disabled={saving}
                className="rounded-md border border-surface-8 px-4 py-1.5 text-sm text-white/80 hover:bg-white/5"
              >
                Huỷ
              </button>
              <button
                onClick={handleConfirmReassign}
                disabled={saving || reassignTo === null}
                className="rounded-md bg-red-500 px-4 py-1.5 text-sm font-medium text-white hover:bg-red-600 disabled:opacity-40"
              >
                Chuyển + Xóa
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );

  return createPortal(content, document.body);
}
