import { useMemo, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import {
  fbDebugVideoInfo,
  fbDeletePost,
  fbRefetchPostStatus,
  type FbReelPost,
  type FbReelStatus,
} from "../lib/fbReels";
import { fmtBytes, fmtHistoryTime } from "../formulas";

interface FbReelHistoryTableProps {
  posts: FbReelPost[];
  uploading: boolean;
  onStartAll: () => void;
  onRetry: (postId: number) => void;
  onChanged: () => void;
}

const STATUS_FILTERS: { value: FbReelStatus | "all"; label: string }[] = [
  { value: "all", label: "Tất cả" },
  { value: "pending", label: "Chờ đăng" },
  { value: "uploading", label: "Đang upload" },
  { value: "publishing", label: "Đang publish" },
  { value: "processing", label: "FB đang xử lý" },
  { value: "scheduled", label: "Đã lên lịch" },
  { value: "published", label: "Đã đăng" },
  { value: "failed", label: "Lỗi" },
];

const STATUS_STYLES: Record<
  FbReelStatus,
  { label: string; cls: string; icon: string }
> = {
  pending: {
    label: "Chờ đăng",
    cls: "bg-white/10 text-white/70 border-white/20",
    icon: "schedule",
  },
  uploading: {
    label: "Đang upload",
    cls: "bg-blue-500/20 text-blue-200 border-blue-500/40",
    icon: "cloud_upload",
  },
  publishing: {
    label: "Đang publish",
    cls: "bg-blue-500/20 text-blue-200 border-blue-500/40",
    icon: "send",
  },
  // FB đã nhận video, đang transcode + validate. App đợi background poll
  // (60s/lần) hoặc user bấm Cập nhật để verify FB đã publish thật chưa.
  processing: {
    label: "FB đang xử lý",
    cls: "bg-violet-500/20 text-violet-200 border-violet-500/40",
    icon: "hourglass_top",
  },
  scheduled: {
    label: "Đã lên lịch",
    cls: "bg-amber-500/20 text-amber-200 border-amber-500/40",
    icon: "event",
  },
  published: {
    label: "Đã đăng",
    cls: "bg-emerald-500/20 text-emerald-200 border-emerald-500/40",
    icon: "check_circle",
  },
  failed: {
    label: "Lỗi",
    cls: "bg-red-500/20 text-red-200 border-red-500/40",
    icon: "error",
  },
};

export function FbReelHistoryTable({
  posts,
  uploading,
  onStartAll,
  onRetry,
  onChanged,
}: FbReelHistoryTableProps) {
  const [statusFilter, setStatusFilter] = useState<FbReelStatus | "all">("all");

  const filtered = useMemo(() => {
    if (statusFilter === "all") return posts;
    return posts.filter((p) => p.status === statusFilter);
  }, [posts, statusFilter]);

  const counts = useMemo(() => {
    const c = {
      pending: 0,
      processing: 0,
      scheduled: 0,
      published: 0,
      failed: 0,
    };
    for (const p of posts) {
      if (p.status === "pending") c.pending++;
      else if (p.status === "processing") c.processing++;
      else if (p.status === "scheduled") c.scheduled++;
      else if (p.status === "published") c.published++;
      else if (p.status === "failed") c.failed++;
    }
    return c;
  }, [posts]);

  const handleDelete = async (postId: number) => {
    if (!confirm("Xóa post khỏi lịch sử? (không un-publish trên FB)")) return;
    try {
      await fbDeletePost(postId);
      onChanged();
    } catch (e) {
      alert((e as Error).message ?? String(e));
    }
  };

  const handleOpenPermalink = (url: string) => {
    void openUrl(url);
  };

  const handleRefetch = async (postId: number) => {
    try {
      await fbRefetchPostStatus(postId);
      onChanged();
    } catch (e) {
      alert((e as Error).message ?? String(e));
    }
  };

  if (posts.length === 0) {
    return (
      <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-8 text-center text-white/55">
        <span className="material-symbols-rounded text-5xl text-white/30">
          inbox
        </span>
        <p className="mt-2 text-sm">Chưa có video nào trong hàng đợi</p>
      </section>
    );
  }

  return (
    <section className="rounded-2xl border border-surface-8 bg-surface-1 p-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold uppercase tracking-wider text-white/65">
            Hàng đợi & lịch sử
          </h3>
          <div className="mt-1 flex flex-wrap items-center gap-1.5 text-[11px]">
            <span className="rounded-full bg-white/8 px-2 py-0.5 text-white/55">
              Tổng {posts.length}
            </span>
            {counts.pending > 0 && (
              <span className="rounded-full bg-white/10 px-2 py-0.5 text-white/70">
                {counts.pending} chờ đăng
              </span>
            )}
            {counts.processing > 0 && (
              <span className="rounded-full bg-violet-500/20 px-2 py-0.5 text-violet-200">
                {counts.processing} FB đang xử lý
              </span>
            )}
            {counts.scheduled > 0 && (
              <span className="rounded-full bg-amber-500/20 px-2 py-0.5 text-amber-200">
                {counts.scheduled} đã lên lịch
              </span>
            )}
            {counts.published > 0 && (
              <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-emerald-200">
                {counts.published} đã đăng
              </span>
            )}
            {counts.failed > 0 && (
              <span className="rounded-full bg-red-500/20 px-2 py-0.5 text-red-200">
                {counts.failed} lỗi
              </span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          <select
            value={statusFilter}
            onChange={(e) =>
              setStatusFilter(e.currentTarget.value as FbReelStatus | "all")
            }
            className="rounded-md border border-surface-8 bg-surface-2 px-2 py-1.5 text-xs text-white/90 focus:border-blue-500 focus:outline-none"
          >
            {STATUS_FILTERS.map((f) => (
              <option key={f.value} value={f.value}>
                {f.label}
              </option>
            ))}
          </select>
          <button
            type="button"
            onClick={onStartAll}
            disabled={uploading || counts.pending === 0}
            className="btn-ripple flex items-center gap-2 rounded-lg bg-blue-500 px-4 py-2 text-sm font-medium text-white hover:bg-blue-600 disabled:opacity-50"
          >
            {uploading ? (
              <>
                <span className="material-symbols-rounded animate-spin text-base">
                  sync
                </span>
                Đang đăng...
              </>
            ) : (
              <>
                <span className="material-symbols-rounded text-base">
                  play_arrow
                </span>
                Bắt đầu đăng ({counts.pending})
              </>
            )}
          </button>
        </div>
      </div>

      <ul className="space-y-2">
        {filtered.map((post) => (
          <PostRow
            key={post.id}
            post={post}
            onRetry={onRetry}
            onDelete={handleDelete}
            onOpenPermalink={handleOpenPermalink}
            onRefetch={handleRefetch}
            uploading={uploading}
          />
        ))}
      </ul>
    </section>
  );
}

interface PostRowProps {
  post: FbReelPost;
  uploading: boolean;
  onRetry: (postId: number) => void;
  onDelete: (postId: number) => Promise<void>;
  onOpenPermalink: (url: string) => void;
  onRefetch: (postId: number) => Promise<void>;
}

function PostRow({
  post,
  uploading,
  onRetry,
  onDelete,
  onOpenPermalink,
  onRefetch,
}: PostRowProps) {
  const meta = STATUS_STYLES[post.status];
  const isActive =
    post.status === "uploading" ||
    post.status === "publishing" ||
    post.status === "processing";
  const fileName = post.filePath.split(/[\\/]/).pop() ?? post.filePath;
  // "Cập nhật" button: cho row scheduled (verify FB đã đăng chưa), processing
  // (FB đang transcode), HOẶC published mà chưa có permalink (FB transcoding
  // chậm). Background poll đã chạy 60s/lần, nút này cho user force-check ngay.
  const showRefetchBtn =
    post.status === "scheduled" ||
    post.status === "processing" ||
    (post.status === "published" && !post.fbPermalink);
  // "Xem chi tiết FB" — gọi GET /{video_id} trả raw JSON cho user/dev copy
  // diagnostic. Chỉ hiện khi đã có video_id (đã qua start_upload).
  const showDebugBtn =
    post.fbVideoId !== null &&
    (post.status === "processing" ||
      post.status === "failed" ||
      post.status === "scheduled" ||
      (post.status === "published" && !post.fbPermalink));

  const [debugInfo, setDebugInfo] = useState<string | null>(null);
  const [debugLoading, setDebugLoading] = useState(false);

  const handleShowDebug = async () => {
    setDebugLoading(true);
    try {
      const raw = await fbDebugVideoInfo(post.id);
      setDebugInfo(raw);
    } catch (e) {
      setDebugInfo(`Lỗi gọi FB: ${(e as Error).message ?? String(e)}`);
    } finally {
      setDebugLoading(false);
    }
  };

  const handleCopyDebug = async () => {
    if (!debugInfo) return;
    try {
      await navigator.clipboard.writeText(debugInfo);
    } catch {
      // clipboard không cho phép — user select & copy thủ công.
    }
  };

  return (
    <li className="rounded-lg border border-surface-8 bg-surface-2 p-3">
      <div className="flex items-start gap-3">
        <span
          className={`material-symbols-rounded text-2xl ${
            isActive
              ? post.status === "processing"
                ? "animate-pulse text-violet-300"
                : "animate-pulse text-blue-300"
              : "text-white/45"
          }`}
        >
          {!isActive
            ? "movie"
            : post.status === "processing"
              ? "hourglass_top"
              : "cloud_upload"}
        </span>

        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="truncate text-sm font-medium text-white/90">
              {fileName}
            </span>
            <span
              className={`flex shrink-0 items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium ${meta.cls}`}
            >
              <span className="material-symbols-rounded text-xs">
                {meta.icon}
              </span>
              {meta.label}
            </span>
          </div>

          <div className="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-[11px] text-white/55">
            <span>Page: {post.pageName}</span>
            <span>Size: {fmtBytes(post.fileSize)}</span>
            <span>Tạo: {fmtHistoryTime(post.createdAtMs)}</span>
            {post.scheduledTimeMs && (
              <span className="text-amber-200">
                Đăng lúc: {fmtHistoryTime(post.scheduledTimeMs)}
              </span>
            )}
            {post.publishedAtMs && (
              <span className="text-emerald-200">
                Đã đăng: {fmtHistoryTime(post.publishedAtMs)}
              </span>
            )}
          </div>

          {post.caption && (
            <p className="mt-1 line-clamp-2 text-xs text-white/70">
              "{post.caption}"
            </p>
          )}

          {isActive && (
            <div className="mt-2">
              {post.status === "publishing" || post.status === "processing" ? (
                // publishing/processing không có progress thực — hiện
                // indeterminate animation thay vì stuck 100% (gây tưởng đơ).
                // processing dùng màu violet để phân biệt với publishing (xanh).
                <div className="h-1.5 overflow-hidden rounded-full bg-surface-1">
                  <div
                    className={`animate-progress-indeterminate h-full w-1/3 bg-gradient-to-r ${
                      post.status === "processing"
                        ? "from-violet-500 to-violet-300"
                        : "from-blue-500 to-blue-300"
                    }`}
                  />
                </div>
              ) : (
                <div className="h-1.5 overflow-hidden rounded-full bg-surface-1">
                  <div
                    className="h-full bg-gradient-to-r from-blue-500 to-blue-400 transition-all duration-300"
                    style={{ width: `${post.progress}%` }}
                  />
                </div>
              )}
              <div className="mt-0.5 text-[11px] text-white/55">
                {post.status === "publishing"
                  ? "Đang gọi FB publish API..."
                  : post.status === "processing"
                    ? "FB đang transcode + validate video (có thể mất 30-90s)"
                    : `${post.progress}% · upload binary`}
              </div>
            </div>
          )}

          {post.errorMessage && (
            <p className="mt-1 line-clamp-3 text-[11px] text-red-300">
              {post.errorMessage}
            </p>
          )}

          {debugInfo !== null && (
            <div className="mt-2 rounded-md border border-violet-500/40 bg-violet-950/30 p-2">
              <div className="mb-1 flex items-center justify-between">
                <span className="text-[11px] font-semibold uppercase text-violet-200">
                  FB raw response
                </span>
                <div className="flex items-center gap-1">
                  <button
                    type="button"
                    onClick={() => void handleCopyDebug()}
                    className="rounded px-1.5 py-0.5 text-[11px] text-violet-200 hover:bg-violet-500/30"
                    title="Copy JSON"
                  >
                    <span className="material-symbols-rounded text-xs">
                      content_copy
                    </span>
                  </button>
                  <button
                    type="button"
                    onClick={() => setDebugInfo(null)}
                    className="rounded px-1.5 py-0.5 text-[11px] text-violet-200 hover:bg-violet-500/30"
                    title="Đóng"
                  >
                    <span className="material-symbols-rounded text-xs">
                      close
                    </span>
                  </button>
                </div>
              </div>
              <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-all rounded bg-black/30 p-2 font-mono text-[10px] leading-tight text-violet-100">
                {debugInfo}
              </pre>
              <p className="mt-1 text-[10px] text-violet-300/70">
                Xem field <code>status.publishing_phase</code> +{" "}
                <code>processing_phase</code> để biết FB đang ở bước nào.
                <code>publish_status</code> hoặc <code>errors</code> tiết
                lộ lý do video không lên.
              </p>
            </div>
          )}
        </div>

        <div className="flex shrink-0 items-center gap-1">
          {post.fbPermalink && (
            <button
              type="button"
              onClick={() => onOpenPermalink(post.fbPermalink!)}
              className="flex h-7 w-7 items-center justify-center rounded-full text-blue-300 hover:bg-blue-500/20"
              title="Mở Reel trên FB"
            >
              <span className="material-symbols-rounded text-base">
                open_in_new
              </span>
            </button>
          )}
          {showRefetchBtn && (
            <button
              type="button"
              onClick={() => void onRefetch(post.id)}
              className="flex h-7 w-7 items-center justify-center rounded-full text-cyan-300 hover:bg-cyan-500/20"
              title={
                post.status === "scheduled"
                  ? "Kiểm tra FB đã đăng chưa"
                  : "Cập nhật permalink (FB đang transcode)"
              }
            >
              <span className="material-symbols-rounded text-base">
                cached
              </span>
            </button>
          )}
          {showDebugBtn && (
            <button
              type="button"
              onClick={() => void handleShowDebug()}
              disabled={debugLoading}
              className="flex h-7 w-7 items-center justify-center rounded-full text-violet-300 hover:bg-violet-500/20 disabled:opacity-50"
              title="Xem JSON FB trả về cho video này (debug)"
            >
              <span className="material-symbols-rounded text-base">
                {debugLoading ? "sync" : "bug_report"}
              </span>
            </button>
          )}
          {post.status === "failed" && (
            <button
              type="button"
              onClick={() => onRetry(post.id)}
              disabled={uploading}
              className="flex h-7 w-7 items-center justify-center rounded-full text-amber-300 hover:bg-amber-500/20 disabled:opacity-50"
              title="Thử lại"
            >
              <span className="material-symbols-rounded text-base">
                refresh
              </span>
            </button>
          )}
          {!isActive && (
            <button
              type="button"
              onClick={() => void onDelete(post.id)}
              className="flex h-7 w-7 items-center justify-center rounded-full text-red-300 hover:bg-red-500/20"
              title="Xóa khỏi lịch sử"
            >
              <span className="material-symbols-rounded text-base">
                delete
              </span>
            </button>
          )}
        </div>
      </div>
    </li>
  );
}
