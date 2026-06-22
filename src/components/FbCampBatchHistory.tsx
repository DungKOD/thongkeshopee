import { useMemo, useState } from "react";
import { openUrl } from "@tauri-apps/plugin-opener";
import { type FbCampBatch, type FbCampJob } from "../lib/fbAds";
import { fmtHistoryTime } from "../formulas";

interface FbCampBatchHistoryProps {
  batches: FbCampBatch[];
  currentJobs: FbCampJob[];
  currentBatchId: number | null;
  uploading: boolean;
  onSelectBatch: (batchId: number) => void;
  onRetryJob: (jobId: number) => void;
}

const BATCH_STATUS: Record<
  string,
  { label: string; cls: string; icon: string }
> = {
  pending: {
    label: "Chờ",
    cls: "bg-white/10 text-white/70 border-white/20",
    icon: "schedule",
  },
  running: {
    label: "Đang chạy",
    cls: "bg-violet-500/20 text-violet-200 border-violet-500/40",
    icon: "play_arrow",
  },
  completed: {
    label: "Xong",
    cls: "bg-emerald-500/20 text-emerald-200 border-emerald-500/40",
    icon: "check_circle",
  },
  cancelled: {
    label: "Hủy",
    cls: "bg-amber-500/20 text-amber-200 border-amber-500/40",
    icon: "cancel",
  },
  failed: {
    label: "Lỗi",
    cls: "bg-red-500/20 text-red-200 border-red-500/40",
    icon: "error",
  },
};

const JOB_STATUS: Record<
  string,
  { label: string; cls: string; icon: string; isActive: boolean }
> = {
  pending: {
    label: "Chờ",
    cls: "text-white/55",
    icon: "schedule",
    isActive: false,
  },
  uploading_video: {
    label: "Upload video",
    cls: "text-blue-300",
    icon: "cloud_upload",
    isActive: true,
  },
  creating_creative: {
    label: "Creative",
    cls: "text-blue-300",
    icon: "image",
    isActive: true,
  },
  creating_campaign: {
    label: "Campaign",
    cls: "text-blue-300",
    icon: "campaign",
    isActive: true,
  },
  creating_adset: {
    label: "Ad Set",
    cls: "text-blue-300",
    icon: "group",
    isActive: true,
  },
  creating_ad: {
    label: "Ad",
    cls: "text-blue-300",
    icon: "ad_units",
    isActive: true,
  },
  done: {
    label: "Done",
    cls: "text-emerald-300",
    icon: "check_circle",
    isActive: false,
  },
  failed: {
    label: "Lỗi",
    cls: "text-red-300",
    icon: "error",
    isActive: false,
  },
};

export function FbCampBatchHistory({
  batches,
  currentJobs,
  currentBatchId,
  uploading,
  onSelectBatch,
  onRetryJob,
}: FbCampBatchHistoryProps) {
  const [expanded, setExpanded] = useState(false);

  const currentBatch = useMemo(
    () => batches.find((b) => b.batchId === currentBatchId) ?? null,
    [batches, currentBatchId],
  );

  if (batches.length === 0) {
    return null;
  }

  return (
    <section className="rounded-2xl border border-surface-8 bg-surface-1 p-4">
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center gap-2 text-left"
      >
        <span className="material-symbols-rounded text-base text-white/65">
          {expanded ? "expand_less" : "expand_more"}
        </span>
        <h3 className="text-sm font-semibold uppercase tracking-wider text-white/65">
          Lịch sử batch
        </h3>
        <span className="text-[11px] text-white/40">({batches.length})</span>
      </button>

      {expanded && (
        <div className="mt-3 space-y-3">
          {/* Batch list */}
          <ul className="space-y-1">
            {batches.slice(0, 20).map((b) => {
              const meta = BATCH_STATUS[b.status] ?? BATCH_STATUS.pending;
              const isSelected = b.batchId === currentBatchId;
              return (
                <li key={b.batchId}>
                  <button
                    type="button"
                    onClick={() => onSelectBatch(b.batchId)}
                    className={`flex w-full items-center gap-3 rounded-md border px-3 py-2 text-left ${
                      isSelected
                        ? "border-violet-500/60 bg-violet-500/10"
                        : "border-surface-8 hover:bg-white/5"
                    }`}
                  >
                    <span
                      className={`flex shrink-0 items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-medium ${meta.cls}`}
                    >
                      <span className="material-symbols-rounded text-xs">
                        {meta.icon}
                      </span>
                      {meta.label}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-sm font-medium text-white/90">
                        {b.templateName}
                      </div>
                      <div className="text-[11px] text-white/45">
                        {fmtHistoryTime(b.startedAtMs)} ·{" "}
                        {b.totalRows} rows · {b.successCount} OK ·{" "}
                        {b.failedCount} fail
                      </div>
                    </div>
                  </button>
                </li>
              );
            })}
          </ul>

          {/* Jobs of selected batch */}
          {currentBatch && currentJobs.length > 0 && (
            <div className="rounded-lg border border-surface-8 bg-surface-2 p-3">
              <h4 className="mb-2 text-xs font-semibold text-white/75">
                Jobs của batch #{currentBatch.batchId} ({currentJobs.length})
              </h4>
              <div className="max-h-96 space-y-1 overflow-y-auto">
                {currentJobs.map((job) => (
                  <JobRow
                    key={job.jobId}
                    job={job}
                    accountId={currentBatch.accountId}
                    uploading={uploading}
                    onRetry={onRetryJob}
                  />
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  );
}

interface JobRowProps {
  job: FbCampJob;
  accountId: string;
  uploading: boolean;
  onRetry: (jobId: number) => void;
}

function JobRow({ job, accountId, uploading, onRetry }: JobRowProps) {
  const meta = JOB_STATUS[job.status] ?? JOB_STATUS.pending;

  const openCampOnFb = () => {
    if (!job.fbCampaignId) return;
    void openUrl(
      `https://business.facebook.com/adsmanager/manage/campaigns?act=${accountId}&selected_campaign_ids=${job.fbCampaignId}`,
    );
  };

  return (
    <div className="rounded-md border border-surface-8 bg-surface-1 p-2 text-xs">
      <div className="flex items-center gap-2">
        <span className="w-8 shrink-0 text-center font-mono text-[10px] text-white/40">
          #{job.rowIndex + 1}
        </span>
        <span
          className={`material-symbols-rounded text-sm ${meta.cls} ${
            meta.isActive ? "animate-pulse" : ""
          }`}
        >
          {meta.icon}
        </span>
        <div className="min-w-0 flex-1">
          <div className="truncate text-white/90">{job.campName}</div>
          <div className="truncate text-[10px] text-white/45">
            {meta.label}
            {job.fbCampaignId && ` · cam: ${job.fbCampaignId}`}
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-1">
          {job.fbCampaignId && (
            <button
              type="button"
              onClick={openCampOnFb}
              className="flex h-6 w-6 items-center justify-center rounded text-violet-300 hover:bg-violet-500/20"
              title="Mở camp trên Ads Manager"
            >
              <span className="material-symbols-rounded text-sm">
                open_in_new
              </span>
            </button>
          )}
          {job.status === "failed" && (
            <button
              type="button"
              onClick={() => onRetry(job.jobId)}
              disabled={uploading}
              className="flex h-6 w-6 items-center justify-center rounded text-amber-300 hover:bg-amber-500/20 disabled:opacity-50"
              title="Retry"
            >
              <span className="material-symbols-rounded text-sm">refresh</span>
            </button>
          )}
        </div>
      </div>

      {meta.isActive && (
        <div className="mt-1 h-1 overflow-hidden rounded-full bg-surface-4">
          <div
            className="h-full bg-gradient-to-r from-violet-500 to-violet-400 transition-all duration-300"
            style={{ width: `${job.progress}%` }}
          />
        </div>
      )}

      {job.errorMessage && (
        <p className="mt-1 line-clamp-2 text-[10px] text-red-300">
          {job.errorMessage}
        </p>
      )}
    </div>
  );
}
