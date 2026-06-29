import { useEffect, useState } from "react";
import { getActiveWorkspace, type Workspace } from "../lib/workspaces";

/// Badge hiển thị workspace đang active trên top bar — fetch 1 lần lúc mount.
/// Không subscribe live changes vì switch workspace luôn kéo theo restart app
/// (component sẽ tự fetch lại sau reload).
export function WorkspaceBadge() {
  const [ws, setWs] = useState<Workspace | null>(null);

  useEffect(() => {
    let cancelled = false;
    getActiveWorkspace()
      .then((w) => {
        if (!cancelled) setWs(w);
      })
      .catch((e) => {
        console.error("[WorkspaceBadge] load failed:", e);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!ws) return null;

  return (
    <span
      className="flex items-center gap-1 rounded-md px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wider text-white shadow-elev-1"
      style={{ backgroundColor: ws.color }}
      title={`Workspace: ${ws.name}`}
    >
      <span className="material-symbols-rounded text-sm">workspaces</span>
      {ws.name}
    </span>
  );
}
