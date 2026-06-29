import { useEffect, useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
import { convertFileSrc } from "@tauri-apps/api/core";
import { fbEnqueueReel, type FbPage } from "../lib/fbReels";

interface PublishArgs {
  pageId: string;
  filePath: string;
  caption: string | null;
  scheduledTimeMs: number | null;
}

interface FbUploadFormProps {
  pages: FbPage[];
  /** Có upload đang chạy không — disable nút "Thêm" để tránh race. */
  uploading: boolean;
  onEnqueued: () => void;
  /** "Đăng ngay" 1 video — skip queue. Chỉ dùng khi chọn đúng 1 file. */
  onPublishNow: (args: PublishArgs) => Promise<void>;
  /** Sau khi enqueue nhiều file → trigger startAll để upload tuần tự. */
  onStartQueue: () => Promise<void>;
  onOpenPageManager: () => void;
}

const MIN_SCHEDULE_OFFSET_MS = 10 * 60 * 1000;

/// FB Reels duration constraints — ngoài khoảng này FB sẽ reject sau khi upload.
const MIN_DURATION_S = 3;
const MAX_DURATION_S = 90;

/// Hard cap số file/lần để tránh user lỡ chọn cả folder vài trăm video → UI
/// freeze do probe metadata + enqueue chuỗi quá dài.
const MAX_FILES_PER_BATCH = 50;

interface VideoMeta {
  durationSec: number;
  width: number;
  height: number;
}

interface SelectedFile {
  path: string;
  meta: VideoMeta | null;
  /// Warning hiển thị inline — sai duration / sai ratio / probe lỗi.
  warning: string | null;
  /// Đang probe metadata (loading state để UI hiện spinner).
  probing: boolean;
}

/// Đọc metadata video qua HTMLVideoElement — tải metadata-only, không stream
/// full file. Tauri `convertFileSrc` map từ local path sang URL scheme webview
/// đọc được. Trả `Promise<VideoMeta>`. Reject nếu file không decode được.
function probeVideoMeta(localPath: string): Promise<VideoMeta> {
  return new Promise((resolve, reject) => {
    const url = convertFileSrc(localPath);
    const v = document.createElement("video");
    v.preload = "metadata";
    v.muted = true;
    const cleanup = () => {
      v.remove();
    };
    v.onloadedmetadata = () => {
      const meta: VideoMeta = {
        durationSec: v.duration,
        width: v.videoWidth,
        height: v.videoHeight,
      };
      cleanup();
      resolve(meta);
    };
    v.onerror = () => {
      cleanup();
      reject(new Error("Không decode được video (codec không hỗ trợ?)"));
    };
    v.src = url;
  });
}

/// Build warning string từ metadata — null nếu OK.
function buildWarning(meta: VideoMeta): string | null {
  const warnings: string[] = [];
  if (meta.durationSec < MIN_DURATION_S) {
    warnings.push(
      `${meta.durationSec.toFixed(1)}s < tối thiểu ${MIN_DURATION_S}s`,
    );
  } else if (meta.durationSec > MAX_DURATION_S) {
    warnings.push(
      `${meta.durationSec.toFixed(1)}s > tối đa ${MAX_DURATION_S}s`,
    );
  }
  if (meta.width > 0 && meta.height > 0) {
    const ratio = meta.width / meta.height;
    if (Math.abs(ratio - 9 / 16) > 0.03) {
      warnings.push(
        `${meta.width}×${meta.height} (${ratio.toFixed(2)}) ≠ 9:16`,
      );
    }
  }
  return warnings.length > 0 ? warnings.join(" · ") : null;
}

export function FbUploadForm({
  pages,
  uploading,
  onEnqueued,
  onPublishNow,
  onStartQueue,
  onOpenPageManager,
}: FbUploadFormProps) {
  const [pageId, setPageId] = useState<string>(pages[0]?.pageId ?? "");
  const [selectedFiles, setSelectedFiles] = useState<SelectedFile[]>([]);
  const [caption, setCaption] = useState("");
  const [scheduleOn, setScheduleOn] = useState(false);
  const [scheduleAt, setScheduleAt] = useState<string>("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<"enqueue" | "now" | null>(null);

  // Sync pageId default từ list — nếu list thay đổi (vd page bị xóa hoặc
  // page mới thêm) và pageId hiện không còn hợp lệ → fallback page đầu tiên.
  useEffect(() => {
    if (pages.length === 0) return;
    if (!pageId || !pages.some((p) => p.pageId === pageId)) {
      setPageId(pages[0].pageId);
    }
  }, [pages, pageId]);

  // Page hiện chọn có token hết hạn → block upload + show inline warning.
  const selectedPage = pages.find((p) => p.pageId === pageId) ?? null;
  const selectedTokenExpired = selectedPage?.tokenExpired ?? false;
  const fileCount = selectedFiles.length;
  const isMulti = fileCount > 1;

  /** Validate form (chung cho cả 1 lẫn nhiều file). Trả base args (không có
   * filePath — caller tự fill cho từng file) hoặc set error + return null. */
  const validateBase = (): Omit<PublishArgs, "filePath"> | null => {
    if (!pageId) {
      setError("Chọn Page để đăng");
      return null;
    }
    if (selectedFiles.length === 0) {
      setError("Chọn ít nhất 1 video");
      return null;
    }
    if (scheduleOn) {
      if (!scheduleAt) {
        setError("Chọn thời gian đăng");
        return null;
      }
      const ms = new Date(scheduleAt).getTime();
      if (Number.isNaN(ms)) {
        setError("Thời gian không hợp lệ");
        return null;
      }
      if (ms - Date.now() < MIN_SCHEDULE_OFFSET_MS) {
        setError("FB yêu cầu thời gian lên lịch ≥ 10 phút từ hiện tại");
        return null;
      }
    }
    return {
      pageId,
      caption: caption.trim() || null,
      scheduledTimeMs: scheduleOn ? new Date(scheduleAt).getTime() : null,
    };
  };

  const resetForm = () => {
    setSelectedFiles([]);
    setCaption("");
    setScheduleOn(false);
    setScheduleAt("");
  };

  const handlePickFiles = async () => {
    setError(null);
    try {
      const picked = await open({
        multiple: true,
        directory: false,
        filters: [{ name: "Video", extensions: ["mp4", "mov"] }],
      });
      if (!picked) return;
      const paths = (Array.isArray(picked) ? picked : [picked]).filter(
        (p): p is string => typeof p === "string",
      );
      if (paths.length === 0) return;

      if (paths.length > MAX_FILES_PER_BATCH) {
        setError(
          `Quá nhiều file (${paths.length}). Giới hạn ${MAX_FILES_PER_BATCH} video/lần.`,
        );
        return;
      }

      // Dedupe vs selection hiện có (append mode — user có thể pick lần 2 để
      // bổ sung thêm video từ folder khác).
      const existing = new Set(selectedFiles.map((f) => f.path));
      const newPaths = paths.filter((p) => !existing.has(p));
      if (newPaths.length === 0) return;

      // Insert placeholder rows ngay → UI thấy file đã chọn, sau đó async
      // probe từng file để fill metadata + warning.
      const placeholders: SelectedFile[] = newPaths.map((p) => ({
        path: p,
        meta: null,
        warning: null,
        probing: true,
      }));
      setSelectedFiles((prev) => [...prev, ...placeholders]);

      // Probe parallel — metadata-only nên nhẹ; UI cập nhật từng file khi xong.
      await Promise.all(
        newPaths.map(async (path) => {
          try {
            const meta = await probeVideoMeta(path);
            setSelectedFiles((prev) =>
              prev.map((f) =>
                f.path === path
                  ? {
                      path,
                      meta,
                      warning: buildWarning(meta),
                      probing: false,
                    }
                  : f,
              ),
            );
          } catch (probeErr) {
            setSelectedFiles((prev) =>
              prev.map((f) =>
                f.path === path
                  ? {
                      path,
                      meta: null,
                      warning: `Không probe được metadata: ${(probeErr as Error).message}`,
                      probing: false,
                    }
                  : f,
              ),
            );
          }
        }),
      );
    } catch (e) {
      setError((e as Error).message ?? String(e));
    }
  };

  const handleRemoveFile = (path: string) => {
    setSelectedFiles((prev) => prev.filter((f) => f.path !== path));
  };

  const handleClearFiles = () => {
    setSelectedFiles([]);
  };

  /** Enqueue toàn bộ selectedFiles — share base args (page/caption/schedule).
   * Trả số lượng đã enqueue thành công. Throw nếu có file fail (caller hiển thị
   * error nhưng các file đã enqueue trước đó vẫn nằm trong DB). */
  const enqueueAll = async (
    base: Omit<PublishArgs, "filePath">,
  ): Promise<number> => {
    let done = 0;
    for (const f of selectedFiles) {
      await fbEnqueueReel({ ...base, filePath: f.path });
      done++;
    }
    return done;
  };

  const handleEnqueue = async () => {
    setError(null);
    const base = validateBase();
    if (!base) return;

    setSubmitting("enqueue");
    try {
      await enqueueAll(base);
      resetForm();
      onEnqueued();
    } catch (e) {
      setError((e as Error).message ?? String(e));
      // Refresh để show các file đã enqueue thành công trước khi fail.
      onEnqueued();
    } finally {
      setSubmitting(null);
    }
  };

  const handlePublishNow = async () => {
    setError(null);
    const base = validateBase();
    if (!base) return;

    setSubmitting("now");
    try {
      if (selectedFiles.length === 1) {
        // Single file: dùng publishNow path (skip queue, immediate upload).
        await onPublishNow({ ...base, filePath: selectedFiles[0].path });
        resetForm();
      } else {
        // Multi file: enqueue tất cả → trigger queue start. Queue tự upload
        // tuần tự, đảm bảo không spam FB rate limit.
        await enqueueAll(base);
        resetForm();
        onEnqueued();
        await onStartQueue();
      }
    } catch (e) {
      setError((e as Error).message ?? String(e));
      onEnqueued();
    } finally {
      setSubmitting(null);
    }
  };

  if (pages.length === 0) {
    return (
      <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-8 text-center">
        <span className="material-symbols-rounded text-5xl text-blue-400">
          badge
        </span>
        <h3 className="mt-3 text-lg font-semibold text-white/90">
          Chưa có Facebook Page nào
        </h3>
        <p className="mt-1 text-sm text-white/55">
          Thêm Page bằng Access Token để bắt đầu đăng video
        </p>
        <button
          type="button"
          onClick={onOpenPageManager}
          className="btn-ripple mx-auto mt-4 flex items-center gap-2 rounded-lg bg-blue-500 px-5 py-2.5 text-sm font-medium text-white hover:bg-blue-600"
        >
          <span className="material-symbols-rounded text-base">add</span>
          Thêm Facebook Page
        </button>
      </section>
    );
  }

  // Count warnings/probing để hiện badge tổng quan.
  const warnCount = selectedFiles.filter((f) => f.warning).length;
  const probingCount = selectedFiles.filter((f) => f.probing).length;

  const publishLabel = (() => {
    if (submitting === "now") return "Đang đăng...";
    if (fileCount === 0) {
      return scheduleOn ? "Lên lịch đăng" : "Đăng ngay";
    }
    if (fileCount === 1) {
      return scheduleOn ? "Lên lịch đăng" : "Đăng ngay";
    }
    return scheduleOn
      ? `Lên lịch ${fileCount} video`
      : `Đăng ${fileCount} video ngay`;
  })();

  const enqueueLabel = (() => {
    if (submitting === "enqueue") return "Đang thêm...";
    if (fileCount <= 1) return "Thêm vào hàng đợi";
    return `Thêm ${fileCount} video vào hàng đợi`;
  })();

  return (
    <section className="rounded-2xl border border-surface-8 bg-surface-1 p-5">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/65">
          Thêm video vào hàng đợi
        </h3>
        <button
          type="button"
          onClick={onOpenPageManager}
          className="flex items-center gap-1 rounded-md px-2 py-1 text-xs text-blue-300 hover:bg-white/5"
        >
          <span className="material-symbols-rounded text-sm">settings</span>
          Quản lý Page
        </button>
      </div>

      <div className="space-y-4">
        <div>
          <label className="mb-1 block text-xs font-medium text-white/70">
            Facebook Page
          </label>
          <select
            value={pageId}
            onChange={(e) => setPageId(e.currentTarget.value)}
            className="w-full rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            {pages.map((p) => (
              <option key={p.pageId} value={p.pageId}>
                {p.name}
                {p.tokenExpired ? " (token hết hạn)" : ""}
              </option>
            ))}
          </select>
          {selectedTokenExpired && (
            <div className="mt-1.5 flex items-start gap-2 rounded-md border border-red-500/40 bg-red-950/30 px-2.5 py-1.5 text-[11px] text-red-200">
              <span className="material-symbols-rounded shrink-0 text-sm">
                error
              </span>
              <span>
                Token Page hết hạn. Vào <em>Quản lý Page</em> dán token mới
                rồi save lại.
              </span>
            </div>
          )}
        </div>

        <div>
          <div className="mb-1 flex items-end justify-between">
            <label className="block text-xs font-medium text-white/70">
              Video (mp4 / mov, ≤ 100MB, dọc 9:16, 3-90s) — chọn nhiều file
              cùng lúc
            </label>
            {fileCount > 0 && (
              <button
                type="button"
                onClick={handleClearFiles}
                className="text-[11px] text-white/45 hover:text-white/75"
              >
                Xóa tất cả ({fileCount})
              </button>
            )}
          </div>
          <button
            type="button"
            onClick={() => void handlePickFiles()}
            className="flex w-full items-center gap-3 rounded-lg border border-dashed border-surface-8 bg-surface-2 px-4 py-3 text-left hover:border-blue-500 hover:bg-surface-4"
          >
            <span className="material-symbols-rounded text-2xl text-blue-300">
              {fileCount > 0 ? "video_library" : "upload_file"}
            </span>
            <div className="min-w-0 flex-1">
              {fileCount > 0 ? (
                <>
                  <div className="text-sm text-white/90">
                    Đã chọn {fileCount} video
                    {warnCount > 0 && (
                      <span className="ml-2 text-amber-300">
                        · {warnCount} cảnh báo
                      </span>
                    )}
                    {probingCount > 0 && (
                      <span className="ml-2 text-blue-300">
                        · {probingCount} đang đọc...
                      </span>
                    )}
                  </div>
                  <div className="text-[11px] text-white/40">
                    Bấm để chọn thêm video (sẽ append vào danh sách)
                  </div>
                </>
              ) : (
                <span className="text-sm text-white/60">
                  Bấm để chọn 1 hoặc nhiều file video từ máy
                </span>
              )}
            </div>
          </button>
          <p className="mt-1 text-[11px] text-white/40">
            Video không đúng tỷ lệ 9:16 hoặc &gt; 90s có thể bị FB từ chối.
          </p>

          {fileCount > 0 && (
            <ul className="mt-2 max-h-72 space-y-1 overflow-y-auto rounded-lg border border-surface-8 bg-surface-2 p-2">
              {selectedFiles.map((f) => (
                <li
                  key={f.path}
                  className="flex items-start gap-2 rounded-md bg-surface-1 px-2 py-1.5"
                >
                  <span
                    className={`material-symbols-rounded shrink-0 text-base ${
                      f.warning
                        ? "text-amber-300"
                        : f.probing
                          ? "animate-pulse text-blue-300"
                          : "text-emerald-300"
                    }`}
                  >
                    {f.probing
                      ? "hourglass_empty"
                      : f.warning
                        ? "warning"
                        : "video_file"}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm text-white/90">
                      {f.path.split(/[\\/]/).pop()}
                    </div>
                    {f.meta && (
                      <div className="text-[11px] text-white/50">
                        {f.meta.durationSec.toFixed(1)}s · {f.meta.width}×
                        {f.meta.height}
                      </div>
                    )}
                    {f.warning && (
                      <div className="text-[11px] text-amber-300">
                        {f.warning}
                      </div>
                    )}
                  </div>
                  <button
                    type="button"
                    onClick={() => handleRemoveFile(f.path)}
                    className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-white/45 hover:bg-red-500/20 hover:text-red-300"
                    title="Bỏ video này"
                  >
                    <span className="material-symbols-rounded text-base">
                      close
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-white/70">
            Caption (tùy chọn)
            {isMulti && (
              <span className="ml-1 text-white/45">
                — dùng chung cho cả {fileCount} video
              </span>
            )}
          </label>
          <textarea
            value={caption}
            onChange={(e) => setCaption(e.currentTarget.value)}
            placeholder="Mô tả + hashtag..."
            rows={3}
            className="w-full resize-y rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90 placeholder:text-white/30 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>

        <div className="rounded-lg border border-surface-8 bg-surface-2 p-3">
          <label className="flex cursor-pointer items-center gap-2">
            <input
              type="checkbox"
              checked={scheduleOn}
              onChange={(e) => setScheduleOn(e.currentTarget.checked)}
              className="h-4 w-4 accent-blue-500"
            />
            <span className="text-sm text-white/85">Lên lịch đăng</span>
            <span className="text-[11px] text-white/40">
              (FB yêu cầu ≥ 10 phút từ hiện tại)
            </span>
          </label>
          {scheduleOn && (
            <>
              <input
                type="datetime-local"
                value={scheduleAt}
                onChange={(e) => setScheduleAt(e.currentTarget.value)}
                className="mt-2 w-full rounded-md border border-surface-8 bg-surface-1 px-3 py-1.5 text-sm text-white/90 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              {isMulti && (
                <p className="mt-1.5 text-[11px] text-amber-200">
                  Tất cả {fileCount} video sẽ được set cùng lịch đăng — FB có
                  thể publish lần lượt trong khoảng vài phút quanh giờ đó.
                </p>
              )}
            </>
          )}
        </div>

        {error && (
          <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
            {error}
          </div>
        )}

        <div className="grid grid-cols-2 gap-2">
          <button
            type="button"
            onClick={() => void handlePublishNow()}
            disabled={
              !!submitting ||
              uploading ||
              fileCount === 0 ||
              !pageId ||
              selectedTokenExpired
            }
            className="btn-ripple flex items-center justify-center gap-2 rounded-lg bg-blue-500 px-4 py-2.5 text-sm font-semibold text-white shadow-elev-2 hover:bg-blue-600 hover:shadow-elev-4 disabled:opacity-50 disabled:shadow-none"
            title={
              scheduleOn
                ? "Upload ngay, FB sẽ đăng theo lịch đã chọn"
                : isMulti
                  ? "Enqueue tất cả + bắt đầu upload tuần tự ngay"
                  : "Upload và đăng ngay lập tức"
            }
          >
            {submitting === "now" ? (
              <span className="material-symbols-rounded animate-spin text-base">
                sync
              </span>
            ) : (
              <span className="material-symbols-rounded text-base">
                rocket_launch
              </span>
            )}
            {publishLabel}
          </button>

          <button
            type="button"
            onClick={() => void handleEnqueue()}
            disabled={
              !!submitting ||
              uploading ||
              fileCount === 0 ||
              !pageId ||
              selectedTokenExpired
            }
            className="btn-ripple flex items-center justify-center gap-2 rounded-lg border border-blue-500/60 bg-transparent px-4 py-2.5 text-sm font-medium text-blue-200 hover:bg-blue-500/10 disabled:opacity-50"
            title="Thêm vào hàng đợi, sau đó bấm 'Bắt đầu đăng' để upload hàng loạt"
          >
            {submitting === "enqueue" ? (
              <span className="material-symbols-rounded animate-spin text-base">
                sync
              </span>
            ) : (
              <span className="material-symbols-rounded text-base">
                playlist_add
              </span>
            )}
            {enqueueLabel}
          </button>
        </div>
      </div>
    </section>
  );
}
