import { useEffect, useRef, useState } from "react";
import {
  checkForUpdate,
  downloadAndInstall,
  type UpdateInfo,
} from "../lib/updater";

interface UpdatesDropdownProps {
  currentVersion: string;
  /** `owner/repo` trên GitHub — dùng để gọi Releases API list release notes. */
  repo: string;
  /** Số release hiển thị tối đa trong dropdown. */
  limit?: number;
}

interface ReleaseItem {
  tag_name: string;
  name: string;
  published_at: string;
  body: string;
  html_url: string;
  draft: boolean;
}

export function UpdatesDropdown({
  currentVersion,
  repo,
  limit = 10,
}: UpdatesDropdownProps) {
  const [open, setOpen] = useState(false);
  const [releases, setReleases] = useState<ReleaseItem[] | null>(null);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [updateInfo, setUpdateInfo] = useState<UpdateInfo | null>(null);
  const [dlState, setDlState] = useState<
    | { phase: "idle" }
    | { phase: "downloading"; pct: number; downloaded: number; total: number }
    | { phase: "installing" }
    | { phase: "error"; message: string }
  >({ phase: "idle" });
  const rootRef = useRef<HTMLDivElement>(null);

  // Silent check on mount → set updateInfo nếu có bản mới (dùng cho red dot).
  // Fail im lặng: offline / endpoint 404 → không hiển red dot, không log.
  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const info = await checkForUpdate();
        if (!cancelled) setUpdateInfo(info);
      } catch {
        /* silent */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Lazy-fetch release history chỉ khi dropdown mở lần đầu.
  useEffect(() => {
    if (!open || releases !== null) return;
    setListLoading(true);
    setListError(null);
    fetch(
      `https://api.github.com/repos/${repo}/releases?per_page=${limit}`,
    )
      .then((r) =>
        r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)),
      )
      .then((data: ReleaseItem[]) => {
        setReleases(data.filter((x) => !x.draft));
      })
      .catch((e) =>
        setListError(e instanceof Error ? e.message : String(e)),
      )
      .finally(() => setListLoading(false));
  }, [open, releases, repo, limit]);

  // Click ngoài / Escape → đóng dropdown.
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    window.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      window.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const hasUpdate = updateInfo !== null;
  const busy = dlState.phase === "downloading" || dlState.phase === "installing";

  const handleDownload = async () => {
    if (!updateInfo) return;
    setDlState({ phase: "downloading", pct: 0, downloaded: 0, total: 0 });
    try {
      await downloadAndInstall(updateInfo, (ev) => {
        if (ev.kind === "Started") {
          setDlState({
            phase: "downloading",
            pct: 0,
            downloaded: 0,
            total: ev.contentLength,
          });
        } else if (ev.kind === "Progress") {
          const total = ev.total || 0;
          setDlState({
            phase: "downloading",
            downloaded: ev.downloaded,
            total,
            pct: total > 0 ? Math.min(100, Math.round((ev.downloaded / total) * 100)) : 0,
          });
        } else if (ev.kind === "Finished") {
          setDlState({ phase: "installing" });
        }
      });
      // `downloadAndInstall` gọi relaunch → app exit, code dưới không chạy.
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setDlState({ phase: "error", message });
    }
  };

  return (
    <div ref={rootRef} className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="btn-ripple relative flex h-9 w-9 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
        title={hasUpdate ? `Có bản mới v${updateInfo.version}` : "Cập nhật"}
        aria-label="Cập nhật"
      >
        <span className="material-symbols-rounded">file_download</span>
        {hasUpdate && (
          <span className="absolute right-1.5 top-1.5 h-2.5 w-2.5 rounded-full bg-rose-500 ring-2 ring-shopee-500" />
        )}
      </button>

      {open && (
        <div className="absolute right-0 top-full z-50 mt-2 w-[380px] overflow-hidden rounded-xl bg-surface-4 shadow-elev-24 ring-1 ring-white/10">
          <header className="flex items-center justify-between border-b border-surface-8 px-4 py-3">
            <h3 className="flex items-center gap-2 text-sm font-semibold text-white/90">
              <span className="material-symbols-rounded text-base text-shopee-400">
                system_update
              </span>
              Cập nhật
            </h3>
            {hasUpdate ? (
              <span className="rounded-full bg-shopee-500 px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-white">
                Mới: v{updateInfo.version}
              </span>
            ) : (
              <span className="flex items-center gap-1 text-xs text-emerald-300">
                <span className="material-symbols-rounded text-sm">
                  check_circle
                </span>
                Mới nhất
              </span>
            )}
          </header>

          <div className="max-h-[min(70vh,480px)] space-y-3 overflow-y-auto p-3">
            {listLoading && (
              <div className="flex items-center gap-2 px-2 py-4 text-sm text-white/60">
                <span className="material-symbols-rounded animate-spin text-base">
                  progress_activity
                </span>
                Đang tải lịch sử...
              </div>
            )}

            {listError && !listLoading && (
              <div className="rounded-lg border border-rose-500/30 bg-rose-950/20 px-3 py-2 text-xs text-rose-200">
                <div className="font-semibold">Không tải được danh sách</div>
                <div className="mt-0.5 text-rose-200/80">{listError}</div>
              </div>
            )}

            {releases && releases.length === 0 && !listLoading && (
              <div className="px-2 py-4 text-center text-sm text-white/50">
                Chưa có bản release nào được publish.
              </div>
            )}

            {releases?.map((r, idx) => {
              const isLatest = idx === 0;
              const tagVersion = r.tag_name.replace(/^v/, "");
              const isCurrent = tagVersion === currentVersion;
              return (
                <article
                  key={r.tag_name}
                  className={`rounded-lg border px-3 py-2 ${
                    isLatest && hasUpdate
                      ? "border-shopee-500/40 bg-shopee-500/5"
                      : "border-transparent bg-surface-2"
                  }`}
                >
                  <header className="mb-1.5 flex items-center justify-between gap-2">
                    <div className="flex flex-wrap items-center gap-1.5">
                      <h4 className="text-sm font-semibold text-white/90">
                        {r.name || r.tag_name}
                      </h4>
                      {isCurrent && (
                        <span className="rounded bg-emerald-500/20 px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-emerald-300">
                          Đang dùng
                        </span>
                      )}
                    </div>
                    <span className="shrink-0 font-mono text-[11px] text-white/50">
                      {r.published_at.slice(0, 10)}
                    </span>
                  </header>

                  {r.body?.trim() && (
                    <div className="max-h-32 overflow-y-auto whitespace-pre-wrap text-xs leading-relaxed text-white/75">
                      {r.body.trim()}
                    </div>
                  )}

                  {isLatest && hasUpdate && (
                    <div className="mt-2">
                      {dlState.phase === "idle" && (
                        <button
                          type="button"
                          onClick={handleDownload}
                          disabled={busy}
                          className="btn-ripple flex items-center gap-1.5 rounded-lg bg-shopee-500 px-3 py-1.5 text-xs font-semibold text-white hover:bg-shopee-400 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          <span className="material-symbols-rounded text-sm">
                            file_download
                          </span>
                          Download
                        </button>
                      )}

                      {dlState.phase === "downloading" && (
                        <div className="space-y-1">
                          <div className="flex justify-between text-[11px]">
                            <span className="text-white/70">Đang tải...</span>
                            <span className="font-mono tabular-nums text-white/85">
                              {dlState.pct}%
                              {dlState.total > 0 && (
                                <>
                                  {" "}
                                  · {(dlState.downloaded / 1024 / 1024).toFixed(1)}/
                                  {(dlState.total / 1024 / 1024).toFixed(1)}MB
                                </>
                              )}
                            </span>
                          </div>
                          <div className="h-1 overflow-hidden rounded-full bg-surface-8">
                            <div
                              className="h-full bg-shopee-500 transition-[width]"
                              style={{ width: `${dlState.pct}%` }}
                            />
                          </div>
                        </div>
                      )}

                      {dlState.phase === "installing" && (
                        <div className="flex items-center gap-2 text-xs text-white/75">
                          <span className="material-symbols-rounded animate-spin text-sm">
                            progress_activity
                          </span>
                          Đang cài đặt — app sẽ khởi động lại...
                        </div>
                      )}

                      {dlState.phase === "error" && (
                        <div className="space-y-1.5">
                          <div className="rounded-md border border-rose-500/30 bg-rose-950/20 px-2 py-1.5 text-[11px] text-rose-200">
                            {dlState.message}
                          </div>
                          <button
                            type="button"
                            onClick={() => setDlState({ phase: "idle" })}
                            className="text-[11px] text-white/60 hover:text-white"
                          >
                            Thử lại
                          </button>
                        </div>
                      )}
                    </div>
                  )}
                </article>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
