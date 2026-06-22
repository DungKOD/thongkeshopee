import { useMemo, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import {
  fbDeletePost,
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

  const pendingCount = useMemo(
    () => posts.filter((p) => p.status === "pending").length,
    [posts],
  );

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
          <p className="text-[11px] text-white/45">
            {posts.length} post · {pendingCount} chờ đăng
          </p>
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
            disabled={uploading || pendingCount === 0}
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
                Bắt đầu đăng ({pendingCount})
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
}

function PostRow({
  post,
  uploading,
  onRetry,
  onDelete,
  onOpenPermalink,
}: PostRowProps) {
  const meta = STATUS_STYLES[post.status];
  const isActive = post.status === "uploading" || post.status === "publishing";
  const fileName = post.filePath.split(/[\\/]/).pop() ?? post.filePath;

  return (
    <li className="rounded-lg border border-surface-8 bg-surface-2 p-3">
      <div className="flex items-start gap-3">
        <span
          className={`material-symbols-rounded text-2xl ${
            isActive ? "animate-pulse text-blue-300" : "text-white/45"
          }`}
        >
          {isActive ? "cloud_upload" : "movie"}
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
              <div className="h-1.5 overflow-hidden rounded-full bg-surface-1">
                <div
                  className="h-full bg-gradient-to-r from-blue-500 to-blue-400 transition-all duration-300"
                  style={{ width: `${post.progress}%` }}
                />
              </div>
              <div className="mt-0.5 text-[11px] text-white/55">
                {post.progress}%
                {post.status === "publishing" && " · đang publish..."}
              </div>
            </div>
          )}

          {post.errorMessage && (
            <p className="mt-1 line-clamp-3 text-[11px] text-red-300">
              {post.errorMessage}
            </p>
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
