import { useState } from "react";
import { open } from "@tauri-apps/plugin-dialog";
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
  /** "Đăng ngay" — skip queue, upload luôn 1 video. */
  onPublishNow: (args: PublishArgs) => Promise<void>;
  onOpenPageManager: () => void;
}

const MIN_SCHEDULE_OFFSET_MS = 10 * 60 * 1000;

export function FbUploadForm({
  pages,
  uploading,
  onEnqueued,
  onPublishNow,
  onOpenPageManager,
}: FbUploadFormProps) {
  const [pageId, setPageId] = useState<string>(pages[0]?.pageId ?? "");
  const [filePath, setFilePath] = useState<string | null>(null);
  const [caption, setCaption] = useState("");
  const [scheduleOn, setScheduleOn] = useState(false);
  const [scheduleAt, setScheduleAt] = useState<string>("");
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState<"enqueue" | "now" | null>(null);

  if (!pageId && pages[0]) setPageId(pages[0].pageId);

  /** Validate form → trả args hoặc set error & return null. */
  const validateAndBuild = (): PublishArgs | null => {
    if (!pageId) {
      setError("Chọn Page để đăng");
      return null;
    }
    if (!filePath) {
      setError("Chọn video để đăng");
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
      filePath,
      caption: caption.trim() || null,
      scheduledTimeMs: scheduleOn ? new Date(scheduleAt).getTime() : null,
    };
  };

  const resetForm = () => {
    setFilePath(null);
    setCaption("");
    setScheduleOn(false);
    setScheduleAt("");
  };

  const handlePickFile = async () => {
    setError(null);
    try {
      const picked = await open({
        multiple: false,
        directory: false,
        filters: [{ name: "Video", extensions: ["mp4", "mov"] }],
      });
      if (!picked || typeof picked !== "string") return;
      setFilePath(picked);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    }
  };

  const handleEnqueue = async () => {
    setError(null);
    const args = validateAndBuild();
    if (!args) return;

    setSubmitting("enqueue");
    try {
      await fbEnqueueReel(args);
      resetForm();
      onEnqueued();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setSubmitting(null);
    }
  };

  const handlePublishNow = async () => {
    setError(null);
    const args = validateAndBuild();
    if (!args) return;

    setSubmitting("now");
    try {
      await onPublishNow(args);
      resetForm();
    } catch (e) {
      setError((e as Error).message ?? String(e));
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
              </option>
            ))}
          </select>
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-white/70">
            Video (mp4 / mov, tối đa 100MB, dọc 9:16, 3-90s)
          </label>
          <button
            type="button"
            onClick={() => void handlePickFile()}
            className="flex w-full items-center gap-3 rounded-lg border border-dashed border-surface-8 bg-surface-2 px-4 py-3 text-left hover:border-blue-500 hover:bg-surface-4"
          >
            <span className="material-symbols-rounded text-2xl text-blue-300">
              {filePath ? "video_file" : "upload_file"}
            </span>
            <div className="min-w-0 flex-1">
              {filePath ? (
                <>
                  <div className="truncate text-sm text-white/90">
                    {filePath.split(/[\\/]/).pop()}
                  </div>
                  <div className="font-mono text-[11px] text-white/40">
                    {filePath}
                  </div>
                </>
              ) : (
                <span className="text-sm text-white/60">
                  Bấm để chọn file video từ máy
                </span>
              )}
            </div>
          </button>
          <p className="mt-1 text-[11px] text-white/40">
            Video không đúng tỷ lệ 9:16 hoặc &gt; 90s có thể bị FB từ chối.
          </p>
        </div>

        <div>
          <label className="mb-1 block text-xs font-medium text-white/70">
            Caption (tùy chọn)
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
            <input
              type="datetime-local"
              value={scheduleAt}
              onChange={(e) => setScheduleAt(e.currentTarget.value)}
              className="mt-2 w-full rounded-md border border-surface-8 bg-surface-1 px-3 py-1.5 text-sm text-white/90 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
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
            disabled={!!submitting || uploading || !filePath || !pageId}
            className="btn-ripple flex items-center justify-center gap-2 rounded-lg bg-blue-500 px-4 py-2.5 text-sm font-semibold text-white shadow-elev-2 hover:bg-blue-600 hover:shadow-elev-4 disabled:opacity-50 disabled:shadow-none"
            title={
              scheduleOn
                ? "Upload ngay, FB sẽ đăng theo lịch đã chọn"
                : "Upload và đăng ngay lập tức"
            }
          >
            {submitting === "now" ? (
              <>
                <span className="material-symbols-rounded animate-spin text-base">
                  sync
                </span>
                Đang đăng...
              </>
            ) : (
              <>
                <span className="material-symbols-rounded text-base">
                  rocket_launch
                </span>
                {scheduleOn ? "Lên lịch đăng" : "Đăng ngay"}
              </>
            )}
          </button>

          <button
            type="button"
            onClick={() => void handleEnqueue()}
            disabled={!!submitting || uploading || !filePath || !pageId}
            className="btn-ripple flex items-center justify-center gap-2 rounded-lg border border-blue-500/60 bg-transparent px-4 py-2.5 text-sm font-medium text-blue-200 hover:bg-blue-500/10 disabled:opacity-50"
            title="Thêm vào hàng đợi, sau đó bấm 'Bắt đầu đăng' để upload hàng loạt"
          >
            {submitting === "enqueue" ? (
              <>
                <span className="material-symbols-rounded animate-spin text-base">
                  sync
                </span>
                Đang thêm...
              </>
            ) : (
              <>
                <span className="material-symbols-rounded text-base">
                  playlist_add
                </span>
                Thêm vào hàng đợi
              </>
            )}
          </button>
        </div>
      </div>
    </section>
  );
}
