import { useEffect, useState } from "react";
import { save } from "@tauri-apps/plugin-dialog";
import { listen } from "@tauri-apps/api/event";
import { invoke } from "../lib/tauri";

/**
 * Tải video từ nhiều nền tảng (TikTok, Douyin, Xiaohongshu, FB, IG, YouTube...).
 * Flow: user dán URL → getVideoInfo → hiển thị thumbnail + metadata →
 *       user bấm Tải → save dialog → downloadVideo ghi file.
 */

interface VideoInfo {
  title: string;
  author: string;
  cover: string;
  duration: number;
  platform: string;
  downloadUrl: string;
  filename: string;
}

interface PlatformChip {
  name: string;
  icon: string;
  gradient: string;
}

const PLATFORMS: PlatformChip[] = [
  { name: "TikTok", icon: "music_note", gradient: "from-gray-700 to-gray-900" },
  { name: "Douyin", icon: "videocam", gradient: "from-gray-800 to-black" },
  { name: "Xiaohongshu", icon: "book", gradient: "from-red-500 to-red-700" },
  { name: "YouTube", icon: "smart_display", gradient: "from-red-600 to-red-800" },
  { name: "Facebook", icon: "thumb_up", gradient: "from-blue-600 to-blue-800" },
  {
    name: "Instagram",
    icon: "photo_camera",
    gradient: "from-purple-500 via-pink-500 to-orange-400",
  },
  { name: "Twitter/X", icon: "close", gradient: "from-gray-700 to-gray-900" },
  { name: "Reddit", icon: "forum", gradient: "from-orange-500 to-orange-700" },
];

function fmtDuration(seconds: number): string {
  if (seconds <= 0) return "";
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${s.toString().padStart(2, "0")}`;
}

interface Progress {
  downloaded: number;
  total: number;
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024)
    return `${(n / (1024 * 1024)).toFixed(2)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function DownloadVideoPage() {
  const [url, setUrl] = useState("");
  const [loading, setLoading] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [info, setInfo] = useState<VideoInfo | null>(null);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState("");

  // Listen Tauri event cho download progress.
  useEffect(() => {
    const unlisten = listen<Progress>("download-progress", (event) => {
      setProgress(event.payload);
    });
    return () => {
      unlisten.then((fn) => fn());
    };
  }, []);

  const handleFetch = async () => {
    if (!url.trim()) return;
    setLoading(true);
    setError("");
    setInfo(null);
    setSuccess("");
    try {
      const data = await invoke<VideoInfo>("get_video_info", {
        url: url.trim(),
      });
      setInfo(data);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = async () => {
    if (!info) return;
    const defaultName =
      info.filename || `${info.platform.toLowerCase()}_${Date.now()}.mp4`;
    const savePath = await save({
      defaultPath: defaultName,
      filters: [{ name: "Video", extensions: ["mp4", "webm", "mkv", "jpg"] }],
    });
    if (!savePath) return;

    setDownloading(true);
    setProgress({ downloaded: 0, total: 0 });
    setError("");
    setSuccess("");
    const sourceUrl = url.trim();
    try {
      const path = await invoke<string>("download_video", {
        downloadUrl: info.downloadUrl,
        savePath,
      });
      setSuccess(`Đã lưu: ${path}`);
      void invoke("log_video_download", {
        url: sourceUrl,
        status: "success",
      }).catch(() => {});
    } catch (e) {
      setError(String(e));
      void invoke("log_video_download", {
        url: sourceUrl,
        status: "failed",
      }).catch(() => {});
    } finally {
      setDownloading(false);
      setProgress(null);
    }
  };

  const handlePaste = async () => {
    try {
      const text = await navigator.clipboard.readText();
      if (text.trim()) setUrl(text.trim());
    } catch {
      /* clipboard blocked */
    }
  };

  const handleClear = () => {
    setUrl("");
    setInfo(null);
    setError("");
    setSuccess("");
  };

  return (
    <div className="mx-auto max-w-3xl space-y-5">
      {/* ============ Hero card ============ */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-shopee-700 via-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">download</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">Tải video</h1>
            <p className="mt-0.5 text-xs text-white/75">
              Hỗ trợ TikTok · Douyin · Xiaohongshu · YouTube · FB · IG · X · Reddit
            </p>
          </div>
        </div>

        {/* Platform chips - scroll ngang nếu tràn */}
        <div className="border-t border-white/10 bg-black/10 px-6 py-2.5">
          <div className="flex flex-wrap gap-1.5">
            {PLATFORMS.map((p) => (
              <span
                key={p.name}
                className={`inline-flex items-center gap-1 rounded-full bg-gradient-to-r ${p.gradient} px-2.5 py-1 text-[11px] font-medium text-white shadow-elev-1`}
              >
                <span className="material-symbols-rounded text-sm">
                  {p.icon}
                </span>
                {p.name}
              </span>
            ))}
          </div>
        </div>
      </section>

      {/* ============ Input card ============ */}
      <section className="rounded-2xl bg-surface-2 p-4 shadow-elev-2">
        <label className="mb-2 flex items-center gap-1.5 text-xs font-medium uppercase tracking-wider text-white/60">
          <span className="material-symbols-rounded text-base">link</span>
          Link video
        </label>
        <div className="flex flex-nowrap items-stretch gap-2">
          {/* Input + icon dùng flex container thay vì absolute → icon luôn
              nằm gọn trong khung bo tròn, cùng hàng với text input. */}
          <div className="flex h-12 min-w-0 flex-1 items-center rounded-xl border border-surface-8 bg-surface-1 pr-2 transition-colors focus-within:border-shopee-500 focus-within:ring-2 focus-within:ring-shopee-500/30">
            <input
              type="text"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleFetch()}
              placeholder="Dán link vào đây..."
              className="h-full min-w-0 flex-1 bg-transparent pl-4 pr-2 text-sm text-white/90 placeholder:text-white/30 focus:outline-none"
            />
            {url ? (
              <button
                type="button"
                onClick={handleClear}
                className="btn-ripple flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-white/50 transition-colors hover:bg-white/10 hover:text-white"
                title="Xóa"
                aria-label="Xóa"
              >
                <span className="material-symbols-rounded text-base">
                  close
                </span>
              </button>
            ) : (
              <button
                type="button"
                onClick={handlePaste}
                className="btn-ripple flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-white/50 transition-colors hover:bg-shopee-500/20 hover:text-shopee-300"
                title="Dán từ clipboard"
                aria-label="Dán"
              >
                <span className="material-symbols-rounded text-base">
                  content_paste
                </span>
              </button>
            )}
          </div>
          <button
            onClick={handleFetch}
            disabled={loading || !url.trim()}
            className="btn-ripple flex h-12 shrink-0 items-center gap-2 whitespace-nowrap rounded-xl bg-shopee-500 px-5 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-shopee-600 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-50 disabled:shadow-none"
          >
            <span
              className={`material-symbols-rounded text-base ${
                loading ? "animate-spin" : ""
              }`}
            >
              {loading ? "progress_activity" : "search"}
            </span>
            {loading ? "Đang tìm..." : "Tìm video"}
          </button>
        </div>

        {/* Indeterminate progress bar khi fetch */}
        {loading && (
          <div className="mt-3 flex items-center gap-2">
            <div className="relative h-1 flex-1 overflow-hidden rounded-full bg-shopee-500/20">
              <div className="animate-progress-indeterminate absolute inset-y-0 w-1/3 rounded-full bg-shopee-500" />
            </div>
            <span className="text-[11px] text-white/50">
              Đang lấy info từ platform...
            </span>
          </div>
        )}
      </section>

      {/* ============ Error / Success banners ============ */}
      {error && (
        <div className="flex items-start gap-3 rounded-xl border border-red-500/40 bg-red-950/40 px-4 py-3 shadow-elev-1">
          <span className="material-symbols-rounded mt-0.5 shrink-0 text-red-400">
            error
          </span>
          <div className="min-w-0 flex-1 text-sm text-red-100">
            <p className="font-semibold text-red-300">Không tải được</p>
            <p className="mt-0.5 whitespace-pre-line text-red-200/90">{error}</p>
          </div>
        </div>
      )}

      {success && (
        <div className="flex items-start gap-3 rounded-xl border border-green-500/40 bg-green-950/40 px-4 py-3 shadow-elev-1">
          <span className="material-symbols-rounded mt-0.5 shrink-0 text-green-400">
            task_alt
          </span>
          <div className="min-w-0 flex-1 text-sm">
            <p className="font-semibold text-green-300">Tải thành công</p>
            <p
              className="mt-0.5 break-all font-mono text-xs text-green-200/80"
              title={success}
            >
              {success.replace("Đã lưu: ", "")}
            </p>
          </div>
        </div>
      )}

      {/* ============ Video preview card ============ */}
      {info && (
        <section className="overflow-hidden rounded-2xl bg-surface-2 shadow-elev-4">
          {/* Card header */}
          <div className="flex items-center justify-between gap-3 border-b border-surface-8 bg-surface-4 px-5 py-3">
            <span className="inline-flex items-center gap-1.5 rounded-full bg-shopee-500/20 px-2.5 py-1 text-xs font-semibold text-shopee-300">
              <span className="material-symbols-rounded text-sm">
                verified
              </span>
              {info.platform}
            </span>
            {info.duration > 0 && (
              <span className="inline-flex items-center gap-1 text-xs text-white/60">
                <span className="material-symbols-rounded text-sm">
                  schedule
                </span>
                <span className="tabular-nums">
                  {fmtDuration(info.duration)}
                </span>
              </span>
            )}
          </div>

          {/* Card body */}
          <div className="flex flex-col gap-5 p-5 md:flex-row">
            {info.cover ? (
              <div className="shrink-0 self-start">
                <img
                  src={info.cover}
                  alt="Cover"
                  className="h-60 w-[150px] rounded-xl object-cover shadow-elev-8"
                  referrerPolicy="no-referrer"
                  onError={(e) => {
                    (e.currentTarget as HTMLImageElement).style.display =
                      "none";
                  }}
                />
              </div>
            ) : (
              <div className="flex h-60 w-[150px] shrink-0 items-center justify-center rounded-xl bg-surface-6">
                <span className="material-symbols-rounded text-4xl text-white/20">
                  image
                </span>
              </div>
            )}

            <div className="flex min-w-0 flex-1 flex-col justify-between gap-4">
              <div className="space-y-3">
                {info.title ? (
                  <h2 className="text-base font-semibold leading-snug text-white/95">
                    {info.title}
                  </h2>
                ) : (
                  <h2 className="italic text-sm text-white/40">
                    (Không có tiêu đề)
                  </h2>
                )}

                {info.author && (
                  <div className="flex items-center gap-1.5 text-sm text-white/70">
                    <span className="material-symbols-rounded text-base text-white/40">
                      person
                    </span>
                    <span>{info.author}</span>
                  </div>
                )}

                {info.filename && (
                  <div
                    className="flex items-start gap-1.5 text-xs text-white/45"
                    title={info.filename}
                  >
                    <span className="material-symbols-rounded mt-0.5 text-sm text-white/30">
                      description
                    </span>
                    <span className="truncate font-mono">{info.filename}</span>
                  </div>
                )}
              </div>

              <div className="flex flex-col gap-2">
                <button
                  onClick={handleDownload}
                  disabled={downloading}
                  className="btn-ripple flex w-full items-center justify-center gap-2 rounded-xl bg-green-500 px-5 py-3 text-sm font-semibold text-white shadow-elev-2 transition-all hover:bg-green-600 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-50 md:w-auto md:self-start md:px-8"
                >
                  <span
                    className={`material-symbols-rounded text-base ${
                      downloading ? "animate-spin" : ""
                    }`}
                  >
                    {downloading ? "progress_activity" : "download"}
                  </span>
                  {downloading ? "Đang tải..." : "Tải video HD"}
                </button>

                {downloading && progress && (
                  <DownloadProgressBar progress={progress} />
                )}
              </div>
            </div>
          </div>
        </section>
      )}

      {/* ============ How to use (khi idle) ============ */}
      {!info && !loading && !error && (
        <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-6">
          <div className="mb-4 flex items-center gap-2">
            <span className="material-symbols-rounded text-lg text-shopee-400">
              tips_and_updates
            </span>
            <h3 className="text-xs font-semibold uppercase tracking-wider text-white/70">
              Cách sử dụng
            </h3>
          </div>
          <div className="space-y-2.5">
            <Step n={1}>Copy link video từ một trong các nền tảng bên trên</Step>
            <Step n={2}>
              Dán vào ô link, bấm{" "}
              <span className="rounded bg-surface-6 px-1.5 py-0.5 font-mono text-[11px] text-shopee-300">
                Tìm video
              </span>{" "}
              hoặc Enter
            </Step>
            <Step n={3}>Xem trước thumbnail + thông tin, bấm "Tải video HD" để lưu</Step>
          </div>
        </section>
      )}
    </div>
  );
}

function Step({ n, children }: { n: number; children: React.ReactNode }) {
  return (
    <div className="flex items-start gap-3">
      <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-shopee-500/20 text-xs font-bold text-shopee-300">
        {n}
      </span>
      <p className="flex-1 text-sm leading-relaxed text-white/70">{children}</p>
    </div>
  );
}

function DownloadProgressBar({ progress }: { progress: Progress }) {
  const hasTotal = progress.total > 0;
  const percent = hasTotal
    ? Math.min(100, (progress.downloaded / progress.total) * 100)
    : 0;

  return (
    <div className="w-full md:w-auto md:min-w-[360px]">
      <div className="relative h-2 overflow-hidden rounded-full bg-green-500/20">
        {hasTotal ? (
          <div
            className="h-full rounded-full bg-green-500 transition-[width] duration-150 ease-out"
            style={{ width: `${percent}%` }}
          />
        ) : (
          <div className="animate-progress-indeterminate absolute inset-y-0 w-1/3 rounded-full bg-green-500" />
        )}
      </div>
      <div className="mt-1 flex items-center justify-between gap-2 text-[11px] tabular-nums text-white/55">
        <span>
          {fmtBytes(progress.downloaded)}
          {hasTotal && ` / ${fmtBytes(progress.total)}`}
        </span>
        {hasTotal ? (
          <span className="font-semibold text-green-300">
            {percent.toFixed(1)}%
          </span>
        ) : (
          <span>Đang tải...</span>
        )}
      </div>
    </div>
  );
}
