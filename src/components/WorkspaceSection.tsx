import { useState } from "react";
import { ConfirmDialog } from "./ConfirmDialog";
import { useWorkspaces } from "../hooks/useWorkspaces";
import type { Workspace } from "../lib/workspaces";

/// Palette mặc định cho workspace mới. Khác với DEFAULT_COLORS của shopee
/// accounts (tone tươi hơn) — để user dễ phân biệt 2 khái niệm khi cùng
/// hiện trên app.
const WORKSPACE_COLORS = [
  "#3b82f6", // blue
  "#10b981", // emerald
  "#f59e0b", // amber
  "#ef4444", // red
  "#8b5cf6", // violet
  "#ec4899", // pink
  "#14b8a6", // teal
  "#f97316", // orange
];

/// SettingsDialog return `null` khi đóng → component bị unmount → state local
/// reset tự nhiên. Không cần prop visibility riêng.
export function WorkspaceSection() {
  const {
    workspaces,
    active,
    loading,
    error,
    refresh,
    create,
    rename,
    setColor,
    switchTo,
    remove,
  } = useWorkspaces();

  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState("");
  const [newColor, setNewColor] = useState(WORKSPACE_COLORS[0]);
  const [createBusy, setCreateBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const [editingId, setEditingId] = useState<string | null>(null);
  const [editName, setEditName] = useState("");

  const [switchTarget, setSwitchTarget] = useState<Workspace | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Workspace | null>(null);
  /// Workspace đang trong quá trình switch (BE hot-swap + reload UI). Khi
  /// có giá trị → render full-screen overlay chặn user click chỗ khác đồng
  /// thời cho feedback rõ ràng (BE swap nhiều DB có thể kéo dài vài giây).
  const [switchingTo, setSwitchingTo] = useState<Workspace | null>(null);

  const handleCreate = async () => {
    const name = newName.trim();
    if (!name) return;
    setCreateBusy(true);
    setActionError(null);
    try {
      await create(name, newColor);
      setNewName("");
      setNewColor(WORKSPACE_COLORS[0]);
      setCreating(false);
    } catch (e) {
      setActionError((e as Error).message ?? String(e));
    } finally {
      setCreateBusy(false);
    }
  };

  const handleRename = async (id: string) => {
    const name = editName.trim();
    if (!name) {
      setEditingId(null);
      return;
    }
    setActionError(null);
    try {
      await rename(id, name);
      setEditingId(null);
    } catch (e) {
      setActionError((e as Error).message ?? String(e));
    }
  };

  const handleColor = async (id: string, color: string) => {
    setActionError(null);
    try {
      await setColor(id, color);
    } catch (e) {
      setActionError((e as Error).message ?? String(e));
    }
  };

  const handleConfirmSwitch = async () => {
    if (!switchTarget) return;
    const target = switchTarget;
    setSwitchTarget(null);
    // Bật overlay TRƯỚC khi await — giúp user thấy phản hồi ngay frame kế.
    setSwitchingTo(target);
    try {
      // Backend hot-swap 4 DB + pool xong → reload UI để React re-mount
      // với data workspace mới. Không dùng `app.restart()` (race với
      // tauri-plugin-single-instance gây trắng webview).
      await switchTo(target.id);
      window.location.reload();
      // KHÔNG clear `switchingTo` ở đây — overlay tiếp tục cover trong khi
      // browser bắt đầu reload (mất vài trăm ms). Page reload xong, component
      // remount fresh nên state tự reset.
    } catch (e) {
      setSwitchingTo(null);
      setActionError((e as Error).message ?? String(e));
    }
  };

  const handleConfirmDelete = async () => {
    if (!deleteTarget) return;
    const target = deleteTarget;
    setDeleteTarget(null);
    try {
      await remove(target.id);
    } catch (e) {
      setActionError((e as Error).message ?? String(e));
    }
  };

  return (
    <section>
      <div className="mb-1 flex items-center justify-between gap-2">
        <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wider text-white/70">
          <span className="material-symbols-rounded text-base text-shopee-400">
            workspaces
          </span>
          Workspace (DB profile)
        </h3>
        {!loading && active && (
          <span
            className="rounded-full px-2.5 py-0.5 text-[11px] font-semibold uppercase tracking-wider text-white shadow-elev-1"
            style={{ backgroundColor: active.color }}
            title="Workspace đang dùng"
          >
            {active.name}
          </span>
        )}
      </div>
      <p className="mb-3 text-xs text-white/50">
        Mỗi workspace là 1 database riêng (đơn hàng + ads + token FB) — tách
        biệt theo từng người bạn. Chuyển workspace → app khởi động lại để load
        DB tương ứng.
      </p>

      {error && (
        <div className="mb-2 rounded-lg border border-red-500/40 bg-red-900/20 p-2 text-xs text-red-200">
          Lỗi: {error}
        </div>
      )}
      {actionError && (
        <div className="mb-2 rounded-lg border border-amber-500/40 bg-amber-900/20 p-2 text-xs text-amber-200">
          {actionError}
        </div>
      )}

      {loading ? (
        <div className="space-y-2" aria-busy="true">
          {[0, 1].map((i) => (
            <div
              key={i}
              className="h-14 animate-pulse rounded-xl bg-surface-6"
            />
          ))}
        </div>
      ) : workspaces.length === 0 ? (
        <div className="rounded-xl border border-dashed border-surface-12 bg-surface-2 px-4 py-4 text-sm text-white/60">
          Chưa có workspace nào. Hãy tạo workspace đầu tiên.
        </div>
      ) : (
        <ul className="space-y-2">
          {workspaces.map((ws) => (
            <li key={ws.id}>
              <WorkspaceRow
                ws={ws}
                isEditing={editingId === ws.id}
                editName={editName}
                onEditStart={() => {
                  setEditingId(ws.id);
                  setEditName(ws.name);
                }}
                onEditChange={setEditName}
                onEditCommit={() => handleRename(ws.id)}
                onEditCancel={() => setEditingId(null)}
                onColorPick={(c) => handleColor(ws.id, c)}
                onSwitch={() => setSwitchTarget(ws)}
                onDelete={() => setDeleteTarget(ws)}
              />
            </li>
          ))}
        </ul>
      )}

      <div className="mt-3">
        {!creating ? (
          <button
            type="button"
            onClick={() => setCreating(true)}
            className="btn-ripple flex items-center gap-2 rounded-lg border border-shopee-500/40 bg-shopee-500/10 px-4 py-2 text-sm font-semibold text-shopee-200 hover:bg-shopee-500/20"
          >
            <span className="material-symbols-rounded text-base">add</span>
            Tạo workspace mới
          </button>
        ) : (
          <div className="rounded-xl border border-surface-8 bg-surface-1 p-3">
            <label className="block text-xs text-white/60">
              Tên workspace (vd "Bạn A")
            </label>
            <input
              autoFocus
              type="text"
              value={newName}
              onChange={(e) => setNewName(e.currentTarget.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void handleCreate();
                else if (e.key === "Escape") setCreating(false);
              }}
              placeholder="Đặt tên..."
              className="mt-1 w-full rounded-md border border-surface-8 bg-surface-4 px-2.5 py-1.5 text-sm text-white/90 placeholder:text-white/30 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500"
            />
            <div className="mt-2 flex items-center gap-2">
              <span className="text-xs text-white/60">Màu:</span>
              <ColorPalette value={newColor} onPick={setNewColor} />
            </div>
            <div className="mt-3 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => {
                  setCreating(false);
                  setNewName("");
                  setActionError(null);
                }}
                className="btn-ripple rounded-md px-3 py-1.5 text-sm text-white/70 hover:bg-white/5"
              >
                Hủy
              </button>
              <button
                type="button"
                onClick={() => void handleCreate()}
                disabled={createBusy || !newName.trim()}
                className="btn-ripple rounded-md bg-shopee-500 px-3 py-1.5 text-sm font-semibold text-white hover:bg-shopee-600 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {createBusy ? "Đang tạo..." : "Tạo"}
              </button>
            </div>
          </div>
        )}
      </div>

      <ConfirmDialog
        isOpen={!!switchTarget}
        title={`Chuyển sang "${switchTarget?.name ?? ""}"?`}
        message="App sẽ load DB của workspace này. Mọi thay đổi chưa lưu sẽ mất."
        confirmLabel="Chuyển workspace"
        cancelLabel="Hủy"
        onConfirm={handleConfirmSwitch}
        onClose={() => setSwitchTarget(null)}
      />

      {switchingTo && <SwitchingOverlay target={switchingTo} />}

      <ConfirmDialog
        isOpen={!!deleteTarget}
        title={`Xóa workspace "${deleteTarget?.name ?? ""}"?`}
        message="Toàn bộ DB + file CSV của workspace này bị xóa vĩnh viễn. Không thể hoàn tác."
        confirmLabel="Xóa workspace"
        cancelLabel="Hủy"
        danger
        onConfirm={handleConfirmDelete}
        onClose={() => setDeleteTarget(null)}
      />

      <div className="mt-2">
        <button
          type="button"
          onClick={() => void refresh()}
          className="text-[11px] text-white/40 hover:text-white/70"
        >
          Tải lại danh sách
        </button>
      </div>
    </section>
  );
}

interface WorkspaceRowProps {
  ws: Workspace;
  isEditing: boolean;
  editName: string;
  onEditStart: () => void;
  onEditChange: (v: string) => void;
  onEditCommit: () => void;
  onEditCancel: () => void;
  onColorPick: (color: string) => void;
  onSwitch: () => void;
  onDelete: () => void;
}

function WorkspaceRow({
  ws,
  isEditing,
  editName,
  onEditStart,
  onEditChange,
  onEditCommit,
  onEditCancel,
  onColorPick,
  onSwitch,
  onDelete,
}: WorkspaceRowProps) {
  const [paletteOpen, setPaletteOpen] = useState(false);
  const activeCls = ws.isActive
    ? "border-shopee-500/60 bg-shopee-900/15"
    : "border-surface-8 bg-surface-6";

  return (
    <div
      className={`flex items-center gap-3 rounded-xl border px-3 py-2.5 ${activeCls}`}
    >
      <button
        type="button"
        onClick={() => setPaletteOpen((o) => !o)}
        className="relative flex h-7 w-7 shrink-0 items-center justify-center rounded-full ring-2 ring-white/10 hover:ring-white/30"
        style={{ backgroundColor: ws.color }}
        title="Đổi màu"
        aria-label="Đổi màu workspace"
      >
        {ws.isActive && (
          <span className="material-symbols-rounded text-sm text-white">
            check
          </span>
        )}
      </button>

      <div className="min-w-0 flex-1">
        {isEditing ? (
          <input
            autoFocus
            type="text"
            value={editName}
            onChange={(e) => onEditChange(e.currentTarget.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") onEditCommit();
              else if (e.key === "Escape") onEditCancel();
            }}
            onBlur={onEditCommit}
            className="w-full rounded-md border border-shopee-500 bg-surface-4 px-2 py-0.5 text-sm font-semibold text-white outline-none"
          />
        ) : (
          <button
            type="button"
            onClick={onEditStart}
            className="truncate text-left text-sm font-semibold text-white/90 hover:text-shopee-300"
            title="Bấm để đổi tên"
          >
            {ws.name}
          </button>
        )}
        <div className="mt-0.5 flex items-center gap-2 text-[11px] text-white/40">
          <span className="font-mono">{ws.id}</span>
          {ws.isActive && (
            <span className="rounded-full bg-shopee-500/20 px-1.5 py-0.5 font-semibold uppercase tracking-wide text-shopee-300">
              đang dùng
            </span>
          )}
        </div>
      </div>

      <div className="flex shrink-0 items-center gap-1">
        {!ws.isActive && (
          <>
            <button
              type="button"
              onClick={onSwitch}
              className="btn-ripple flex items-center gap-1 rounded-md border border-shopee-500/40 bg-shopee-500/10 px-2.5 py-1 text-xs font-semibold text-shopee-200 hover:bg-shopee-500/20"
              title="Chuyển sang workspace này"
            >
              <span className="material-symbols-rounded text-sm">
                login
              </span>
              Chuyển
            </button>
            <button
              type="button"
              onClick={onDelete}
              className="btn-ripple flex h-8 w-8 items-center justify-center rounded-md text-white/50 hover:bg-red-500/10 hover:text-red-300"
              title="Xóa workspace"
              aria-label="Xóa workspace"
            >
              <span className="material-symbols-rounded text-base">
                delete
              </span>
            </button>
          </>
        )}
      </div>

      {paletteOpen && (
        <div className="absolute z-10 mt-12 rounded-xl border border-surface-8 bg-surface-4 p-2 shadow-elev-8">
          <ColorPalette
            value={ws.color}
            onPick={(c) => {
              onColorPick(c);
              setPaletteOpen(false);
            }}
          />
        </div>
      )}
    </div>
  );
}

interface ColorPaletteProps {
  value: string;
  onPick: (color: string) => void;
}

function ColorPalette({ value, onPick }: ColorPaletteProps) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {WORKSPACE_COLORS.map((c) => (
        <button
          key={c}
          type="button"
          onClick={() => onPick(c)}
          className={`h-6 w-6 rounded-full ring-2 transition-all ${
            value === c
              ? "scale-110 ring-white"
              : "ring-white/10 hover:ring-white/40"
          }`}
          style={{ backgroundColor: c }}
          aria-label={`Chọn màu ${c}`}
          title={c}
        />
      ))}
    </div>
  );
}

interface SwitchingOverlayProps {
  target: Workspace;
}

/// Full-screen overlay khi switch workspace: chặn user click + cho feedback
/// rõ ràng (BE pre-open 4 DB + swap pool có thể kéo dài 0.5-3s tùy size data).
/// `position: fixed` + z-index cao để cover cả SettingsDialog đang mở.
function SwitchingOverlay({ target }: SwitchingOverlayProps) {
  return (
    <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/70 backdrop-blur-sm">
      <div className="flex flex-col items-center gap-4 rounded-2xl border border-surface-8 bg-surface-1 px-8 py-7 shadow-elev-8">
        <div className="relative flex h-14 w-14 items-center justify-center">
          <span
            className="absolute inset-0 animate-ping rounded-full opacity-60"
            style={{ backgroundColor: target.color }}
          />
          <span
            className="relative flex h-12 w-12 items-center justify-center rounded-full text-white shadow-elev-4"
            style={{ backgroundColor: target.color }}
          >
            <span className="material-symbols-rounded animate-spin text-2xl">
              sync
            </span>
          </span>
        </div>
        <div className="text-center">
          <div className="text-base font-semibold text-white">
            Đang chuyển sang "{target.name}"
          </div>
          <div className="mt-1 text-xs text-white/60">
            Đang mở database + reload giao diện...
          </div>
        </div>
      </div>
    </div>
  );
}
