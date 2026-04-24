import { useUpdater } from "../hooks/useUpdater";

interface UpdateSectionProps {
  currentVersion: string;
}

export function UpdateSection({ currentVersion }: UpdateSectionProps) {
  const { state, check, install, reset } = useUpdater();

  const isBusy =
    state.phase === "checking" ||
    state.phase === "downloading" ||
    state.phase === "installing";

  const pct =
    state.phase === "downloading" && state.total > 0
      ? Math.min(100, Math.round((state.downloaded / state.total) * 100))
      : 0;

  return (
    <section className="rounded-xl border border-surface-8 bg-surface-1 px-4 py-3">
      <header className="mb-2 flex items-center justify-between gap-2">
        <h3 className="flex items-center gap-2 text-sm font-semibold text-white/85">
          <span className="material-symbols-rounded text-base text-shopee-400">
            system_update
          </span>
          Cập nhật phiên bản
        </h3>
        <span className="font-mono text-xs tabular-nums text-white/60">
          hiện tại: v{currentVersion}
        </span>
      </header>

      {state.phase === "idle" && (
        <button
          type="button"
          onClick={check}
          className="btn-ripple rounded-lg bg-shopee-500 px-3 py-1.5 text-sm font-medium text-white hover:bg-shopee-400"
        >
          Kiểm tra cập nhật
        </button>
      )}

      {state.phase === "checking" && (
        <div className="flex items-center gap-2 text-sm text-white/70">
          <span className="material-symbols-rounded animate-spin text-base">
            progress_activity
          </span>
          Đang kiểm tra...
        </div>
      )}

      {state.phase === "up-to-date" && (
        <div className="flex items-center justify-between gap-2">
          <span className="flex items-center gap-2 text-sm text-emerald-300">
            <span className="material-symbols-rounded text-base">
              check_circle
            </span>
            Bạn đang dùng bản mới nhất.
          </span>
          <button
            type="button"
            onClick={reset}
            className="text-xs text-white/60 hover:text-white"
          >
            Đóng
          </button>
        </div>
      )}

      {state.phase === "available" && (
        <div className="space-y-3">
          <div className="flex items-center justify-between gap-2">
            <span className="flex items-center gap-2 text-sm font-medium text-shopee-300">
              <span className="material-symbols-rounded text-base">
                new_releases
              </span>
              Có bản mới: v{state.info.version}
            </span>
            {state.info.date && (
              <span className="font-mono text-xs text-white/50">
                {state.info.date.slice(0, 10)}
              </span>
            )}
          </div>
          {state.info.body && (
            <div className="max-h-40 overflow-y-auto whitespace-pre-wrap rounded-lg bg-surface-2 px-3 py-2 text-xs text-white/75">
              {state.info.body}
            </div>
          )}
          <div className="flex gap-2">
            <button
              type="button"
              onClick={install}
              disabled={isBusy}
              className="btn-ripple rounded-lg bg-shopee-500 px-3 py-1.5 text-sm font-medium text-white hover:bg-shopee-400 disabled:cursor-not-allowed disabled:opacity-60"
            >
              Tải & Cài đặt
            </button>
            <button
              type="button"
              onClick={reset}
              className="rounded-lg border border-surface-8 px-3 py-1.5 text-sm text-white/75 hover:bg-white/5"
            >
              Để sau
            </button>
          </div>
        </div>
      )}

      {state.phase === "downloading" && (
        <div className="space-y-2">
          <div className="flex items-center justify-between text-xs">
            <span className="text-white/70">Đang tải v{state.info.version}...</span>
            <span className="font-mono tabular-nums text-white/80">
              {pct}%
              {state.total > 0 && (
                <> · {(state.downloaded / 1024 / 1024).toFixed(1)}/
                  {(state.total / 1024 / 1024).toFixed(1)} MB</>
              )}
            </span>
          </div>
          <div className="h-1.5 overflow-hidden rounded-full bg-surface-8">
            <div
              className="h-full bg-shopee-500 transition-[width]"
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
      )}

      {state.phase === "installing" && (
        <div className="flex items-center gap-2 text-sm text-white/75">
          <span className="material-symbols-rounded animate-spin text-base">
            progress_activity
          </span>
          Đang cài đặt — app sẽ khởi động lại...
        </div>
      )}

      {state.phase === "error" && (
        <div className="space-y-2">
          <div className="flex items-start gap-2 rounded-lg border border-rose-500/30 bg-rose-950/20 px-3 py-2 text-xs text-rose-200">
            <span className="material-symbols-rounded mt-0.5 text-sm text-rose-400">
              error
            </span>
            <div className="flex-1">
              <div className="font-semibold">Lỗi cập nhật</div>
              <div className="mt-0.5 break-words text-rose-200/80">
                {state.message}
              </div>
            </div>
          </div>
          <button
            type="button"
            onClick={reset}
            className="text-xs text-white/60 hover:text-white"
          >
            Thử lại sau
          </button>
        </div>
      )}
    </section>
  );
}
