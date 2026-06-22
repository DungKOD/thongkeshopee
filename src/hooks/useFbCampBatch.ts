import { useCallback, useEffect, useRef, useState } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import {
  fbAdsCreateBatch,
  fbAdsListBatches,
  fbAdsListJobs,
  fbAdsRetryJob,
  type CampBatchProgressEvent,
  type CampRow,
  type FbCampBatch,
  type FbCampJob,
} from "../lib/fbAds";

interface BatchState {
  batches: FbCampBatch[];
  /** Jobs của batch đang inspect — key = jobId. */
  jobs: Map<number, FbCampJob>;
  currentBatchId: number | null;
  uploading: boolean;
  error: string | null;
}

export interface UseFbCampBatchResult {
  batches: FbCampBatch[];
  /** Jobs của batch hiện tại (sorted theo rowIndex). */
  currentJobs: FbCampJob[];
  currentBatchId: number | null;
  uploading: boolean;
  error: string | null;
  refreshBatches: () => Promise<void>;
  loadBatchJobs: (batchId: number) => Promise<void>;
  startBatch: (args: {
    templateId: number;
    draftId: number | null;
    rows: CampRow[];
  }) => Promise<number | null>;
  retryJob: (jobId: number) => Promise<void>;
}

/**
 * Hook quản lý batch execution + subscribe progress events.
 *
 * - `startBatch` gọi backend tạo batch + spawn executor → trả batch_id ngay.
 * - Backend emit `fb_ads_progress` mỗi stage của mỗi job → hook update jobs map.
 * - Backend emit `fb_ads_batch_completed` khi xong → refresh batches + jobs.
 */
export function useFbCampBatch(): UseFbCampBatchResult {
  const [state, setState] = useState<BatchState>({
    batches: [],
    jobs: new Map(),
    currentBatchId: null,
    uploading: false,
    error: null,
  });
  const aliveRef = useRef(true);

  const refreshBatches = useCallback(async () => {
    try {
      const list = await fbAdsListBatches(50);
      if (!aliveRef.current) return;
      setState((s) => ({ ...s, batches: list }));
    } catch (e) {
      if (!aliveRef.current) return;
      setState((s) => ({ ...s, error: (e as Error).message ?? String(e) }));
    }
  }, []);

  const loadBatchJobs = useCallback(async (batchId: number) => {
    try {
      const list = await fbAdsListJobs(batchId);
      if (!aliveRef.current) return;
      const map = new Map<number, FbCampJob>();
      for (const j of list) map.set(j.jobId, j);
      setState((s) => ({ ...s, jobs: map, currentBatchId: batchId }));
    } catch (e) {
      if (!aliveRef.current) return;
      setState((s) => ({ ...s, error: (e as Error).message ?? String(e) }));
    }
  }, []);

  // Subscribe events
  useEffect(() => {
    let unlistenProgress: UnlistenFn | null = null;
    let unlistenCompleted: UnlistenFn | null = null;

    void (async () => {
      unlistenProgress = await listen<CampBatchProgressEvent>(
        "fb_ads_progress",
        (ev) => {
          const p = ev.payload;
          setState((s) => {
            if (s.currentBatchId !== null && s.currentBatchId !== p.batchId) {
              return s; // event của batch khác đang inspect, bỏ qua
            }
            const job = s.jobs.get(p.jobId);
            if (!job) return s;
            const next = new Map(s.jobs);
            next.set(p.jobId, { ...job, status: p.status, progress: p.progress });
            return { ...s, jobs: next };
          });
        },
      );
      unlistenCompleted = await listen<{ batchId: number }>(
        "fb_ads_batch_completed",
        (ev) => {
          void refreshBatches();
          // Refresh jobs if inspecting this batch
          setState((s) => {
            if (s.currentBatchId === ev.payload.batchId) {
              void loadBatchJobs(ev.payload.batchId);
            }
            return { ...s, uploading: false };
          });
        },
      );
    })();

    return () => {
      aliveRef.current = false;
      if (unlistenProgress) unlistenProgress();
      if (unlistenCompleted) unlistenCompleted();
    };
  }, [refreshBatches, loadBatchJobs]);

  useEffect(() => {
    void refreshBatches();
  }, [refreshBatches]);

  const startBatch = useCallback(
    async (args: {
      templateId: number;
      draftId: number | null;
      rows: CampRow[];
    }) => {
      setState((s) => ({ ...s, uploading: true, error: null }));
      try {
        const id = await fbAdsCreateBatch(args);
        await refreshBatches();
        await loadBatchJobs(id);
        return id;
      } catch (e) {
        setState((s) => ({
          ...s,
          uploading: false,
          error: (e as Error).message ?? String(e),
        }));
        return null;
      }
    },
    [refreshBatches, loadBatchJobs],
  );

  const retryJob = useCallback(
    async (jobId: number) => {
      try {
        await fbAdsRetryJob(jobId);
        setState((s) => ({ ...s, uploading: true }));
        if (state.currentBatchId !== null) {
          await loadBatchJobs(state.currentBatchId);
        }
        await refreshBatches();
      } catch (e) {
        setState((s) => ({
          ...s,
          error: (e as Error).message ?? String(e),
        }));
      }
    },
    [loadBatchJobs, refreshBatches, state.currentBatchId],
  );

  const currentJobs = Array.from(state.jobs.values()).sort(
    (a, b) => a.rowIndex - b.rowIndex,
  );

  return {
    batches: state.batches,
    currentJobs,
    currentBatchId: state.currentBatchId,
    uploading: state.uploading,
    error: state.error,
    refreshBatches,
    loadBatchJobs,
    startBatch,
    retryJob,
  };
}
