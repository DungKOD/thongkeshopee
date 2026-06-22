import { useCallback, useState } from "react";
import { useFbPages } from "../hooks/useFbPages";
import { useFbReelQueue } from "../hooks/useFbReelQueue";
import { FbPageManagerDialog } from "./FbPageManagerDialog";
import { FbUploadForm } from "./FbUploadForm";
import { FbReelHistoryTable } from "./FbReelHistoryTable";

export function UploadVideoPage() {
  const {
    pages,
    loading: pagesLoading,
    error: pagesError,
    refresh: refreshPages,
  } = useFbPages();
  const {
    posts,
    uploading,
    error: queueError,
    refresh: refreshQueue,
    startAll,
    retry,
    publishNow,
  } = useFbReelQueue();
  const [pageManagerOpen, setPageManagerOpen] = useState(false);

  const handleEnqueued = useCallback(() => {
    void refreshQueue();
  }, [refreshQueue]);

  const handlePagesChanged = useCallback(() => {
    void refreshPages();
  }, [refreshPages]);

  return (
    <div className="mx-auto max-w-3xl space-y-5">
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-blue-700 via-blue-600 to-blue-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">upload</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">Đăng video lên Page</h1>
            <p className="mt-0.5 text-xs text-white/75">
              Tự động đăng Reels lên Facebook Page qua Graph API
            </p>
          </div>
          <button
            type="button"
            onClick={() => setPageManagerOpen(true)}
            className="btn-ripple flex items-center gap-2 rounded-lg border border-white/40 px-3 py-1.5 text-sm font-medium text-white hover:bg-white/10"
          >
            <span className="material-symbols-rounded text-base">
              manage_accounts
            </span>
            {pages.length > 0 ? `${pages.length} Page` : "Thêm Page"}
          </button>
        </div>
      </section>

      {pagesError && (
        <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
          Lỗi đọc Pages: {pagesError}
        </div>
      )}

      {pagesLoading ? (
        <section className="flex items-center justify-center gap-3 rounded-2xl border border-surface-8 bg-surface-1 py-10 text-white/55">
          <span className="material-symbols-rounded animate-spin text-2xl text-blue-400">
            sync
          </span>
          <span className="text-sm">Đang tải danh sách Page...</span>
        </section>
      ) : (
        <FbUploadForm
          pages={pages}
          uploading={uploading}
          onEnqueued={handleEnqueued}
          onPublishNow={publishNow}
          onOpenPageManager={() => setPageManagerOpen(true)}
        />
      )}

      {queueError && (
        <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
          Lỗi đọc hàng đợi: {queueError}
        </div>
      )}

      <FbReelHistoryTable
        posts={posts}
        uploading={uploading}
        onStartAll={() => void startAll()}
        onRetry={(id) => void retry(id)}
        onChanged={() => void refreshQueue()}
      />

      <FbPageManagerDialog
        isOpen={pageManagerOpen}
        savedPages={pages}
        onClose={() => setPageManagerOpen(false)}
        onChanged={handlePagesChanged}
      />
    </div>
  );
}
