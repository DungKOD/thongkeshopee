import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { useAccounts, DEFAULT_ACCOUNT_ID } from "../contexts/AccountContext";
import { createShopeeAccount } from "../lib/accounts";

interface ImportAccountPickerDialogProps {
  isOpen: boolean;
  /// User pick account xong → gọi callback với id → parent trigger file picker.
  onPick: (accountId: number) => void;
  onClose: () => void;
}

/// Gate dialog mở NGAY SAU "Import CSV" button — user pick/tạo TK Shopee
/// trước khi chọn file. Tất cả file Shopee trong batch sẽ tag cùng TK này.
/// FB files attribution derive sub_id + day, không cần TK.
export function ImportAccountPickerDialog({
  isOpen,
  onPick,
  onClose,
}: ImportAccountPickerDialogProps) {
  const { accounts, activeAccountId, refresh } = useAccounts();
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  // Filter Mặc định ra — user không được chọn account này (reserved cho orphan).
  const pickableAccounts = useMemo(
    () => (accounts ?? []).filter((a) => a.id !== DEFAULT_ACCOUNT_ID),
    [accounts],
  );

  // Default selection: activeAccountId (nếu non-default) → hoặc account đầu
  // tiên trong list pickable → hoặc null (user phải tạo mới).
  const defaultId = useMemo(() => {
    if (activeAccountId !== null && activeAccountId !== DEFAULT_ACCOUNT_ID) {
      return activeAccountId;
    }
    if (pickableAccounts.length > 0) return pickableAccounts[0].id;
    return null;
  }, [activeAccountId, pickableAccounts]);

  useEffect(() => {
    if (!isOpen) return;
    setSelectedId(defaultId);
    setCreating(false);
    setNewName("");
    setError(null);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !saving) onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, defaultId, onClose, saving]);

  if (!isOpen) return null;

  const handleCreate = async () => {
    const name = newName.trim();
    if (!name) return;
    setSaving(true);
    setError(null);
    try {
      const id = await createShopeeAccount(name);
      await refresh();
      setSelectedId(id);
      setCreating(false);
      setNewName("");
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const handleConfirm = () => {
    if (selectedId === null) {
      setError("Chưa chọn account");
      return;
    }
    onPick(selectedId);
  };

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget && !saving) onClose();
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div className="w-[min(92vw,460px)] rounded-xl bg-surface-2 p-6 shadow-elev-4">
        <header className="mb-4 flex items-center justify-between">
          <div>
            <p className="text-[11px] font-medium uppercase tracking-wider text-white/50">
              Import CSV
            </p>
            <h2 className="text-lg font-semibold text-white">
              Chọn TK Shopee
            </h2>
          </div>
          <button
            onClick={onClose}
            disabled={saving}
            className="rounded-lg p-1 text-white/60 hover:bg-white/10 hover:text-white"
            title="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <p className="mb-3 text-xs text-white/60">
          File Shopee (click + hoa hồng) sẽ gắn với TK này. File FB ads tự
          gắn qua sub_id, không cần chọn.
        </p>

        {error && (
          <div className="mb-3 rounded-md border border-red-500/50 bg-red-500/10 px-3 py-2 text-sm text-red-300">
            {error}
          </div>
        )}

        <section className="mb-4">
          {pickableAccounts.length === 0 ? (
            <div className="rounded-md border border-dashed border-surface-8 px-3 py-6 text-center text-sm text-white/60">
              Chưa có TK nào — tạo mới bên dưới để import
            </div>
          ) : (
            <ul className="max-h-[280px] space-y-1 overflow-y-auto">
              {pickableAccounts.map((a) => (
                <li key={a.id}>
                  <label
                    className={`flex cursor-pointer items-center gap-3 rounded-md border px-3 py-2 transition-colors ${
                      selectedId === a.id
                        ? "border-shopee-500 bg-shopee-500/10"
                        : "border-surface-8 hover:bg-white/5"
                    }`}
                  >
                    <input
                      type="radio"
                      name="import-account"
                      value={a.id}
                      checked={selectedId === a.id}
                      onChange={() => setSelectedId(a.id)}
                      className="accent-shopee-500"
                    />
                    <span
                      className="h-3 w-3 shrink-0 rounded-full"
                      style={{ backgroundColor: a.color ?? "#888" }}
                    />
                    <span className="flex-1 text-sm text-white/90">{a.name}</span>
                    <span className="text-xs text-white/50">
                      {a.rowCount} dòng
                    </span>
                  </label>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="mb-5">
          {creating ? (
            <div className="flex gap-2">
              <input
                type="text"
                value={newName}
                autoFocus
                onChange={(e) => setNewName(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleCreate();
                  if (e.key === "Escape") {
                    setCreating(false);
                    setNewName("");
                  }
                }}
                placeholder="Tên TK mới"
                className="flex-1 rounded-md border border-shopee-500 bg-surface-1 px-3 py-2 text-sm text-white/90 placeholder:text-white/30 focus:outline-none"
              />
              <button
                onClick={handleCreate}
                disabled={saving || !newName.trim()}
                className="rounded-md bg-shopee-500 px-4 py-2 text-sm font-medium text-white hover:bg-shopee-600 disabled:opacity-40"
              >
                Tạo
              </button>
              <button
                onClick={() => {
                  setCreating(false);
                  setNewName("");
                }}
                disabled={saving}
                className="rounded-md border border-surface-8 px-3 py-2 text-sm text-white/70 hover:bg-white/5"
              >
                Hủy
              </button>
            </div>
          ) : (
            <button
              onClick={() => setCreating(true)}
              className="flex w-full items-center justify-center gap-2 rounded-md border border-dashed border-surface-8 py-2 text-sm text-white/70 hover:border-shopee-500 hover:bg-white/5 hover:text-white"
            >
              <span className="material-symbols-rounded text-base">add</span>
              Tạo TK mới
            </button>
          )}
        </section>

        <footer className="flex justify-end gap-2">
          <button
            onClick={onClose}
            disabled={saving}
            className="rounded-md border border-surface-8 px-4 py-2 text-sm text-white/80 hover:bg-white/5"
          >
            Hủy
          </button>
          <button
            onClick={handleConfirm}
            disabled={saving || selectedId === null}
            className="flex items-center gap-1.5 rounded-md bg-shopee-500 px-4 py-2 text-sm font-medium text-white hover:bg-shopee-600 disabled:opacity-40"
          >
            <span className="material-symbols-rounded text-base">
              upload_file
            </span>
            Chọn file
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
