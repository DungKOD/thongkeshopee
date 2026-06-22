import { useCallback, useEffect, useRef, useState } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import {
  fbEnqueueReel,
  fbListPosts,
  fbUploadReel,
  type FbReelPost,
  type UploadProgressEvent,
} from "../lib/fbReels";

interface PublishNowArgs {
  pageId: string;
  filePath: string;
  caption: string | null;
  scheduledTimeMs: number | null;
}

interface QueueState {
  /** Posts hiển thị trên UI — merge từ DB + realtime progress events. */
  posts: FbReelPost[];
  /** Có upload đang chạy không (sequential — chỉ 1). */
  uploading: boolean;
  error: string | null;
}

export interface UseFbReelQueueResult extends QueueState {
  /** Reload toàn bộ history từ DB. */
  refresh: () => Promise<void>;
  /** Bắt đầu upload tất cả posts đang `pending` theo thứ tự FIFO. */
  startAll: () => Promise<void>;
  /** Retry 1 post failed. */
  retry: (postId: number) => Promise<void>;
  /** Enqueue + upload ngay 1 video, skip queue. Throw nếu upload fail. */
  publishNow: (args: PublishNowArgs) => Promise<void>;
}

/**
 * Hook quản lý queue upload Reels.
 *
 * - Subscribe `fb_upload_progress` events từ backend → update post realtime.
 * - `startAll` chạy sequential — upload từng `pending` post tới khi hết
 *   hoặc gặp lỗi. Stop sớm nếu user navigate đi chỗ khác (component unmount).
 */
export function useFbReelQueue(): UseFbReelQueueResult {
  const [state, setState] = useState<QueueState>({
    posts: [],
    uploading: false,
    error: null,
  });
  const aliveRef = useRef(true);

  const refresh = useCallback(async () => {
    try {
      const posts = await fbListPosts({ limit: 500 });
      if (!aliveRef.current) return;
      setState((s) => ({ ...s, posts, error: null }));
    } catch (e) {
      if (!aliveRef.current) return;
      setState((s) => ({ ...s, error: (e as Error).message ?? String(e) }));
    }
  }, []);

  // Subscribe Tauri progress events.
  useEffect(() => {
    let unlisten: UnlistenFn | null = null;
    void (async () => {
      unlisten = await listen<UploadProgressEvent>(
        "fb_upload_progress",
        (ev) => {
          const p = ev.payload;
          setState((s) => ({
            ...s,
            posts: s.posts.map((post) =>
              post.id === p.postId
                ? { ...post, status: p.status, progress: p.progress }
                : post,
            ),
          }));
        },
      );
    })();
    return () => {
      aliveRef.current = false;
      if (unlisten) unlisten();
    };
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const startAll = useCallback(async () => {
    setState((s) => ({ ...s, uploading: true, error: null }));
    try {
      // Snapshot list pending từ DB để đảm bảo thứ tự FIFO chuẩn.
      const all = await fbListPosts({ limit: 500 });
      const pending = all
        .filter((p) => p.status === "pending")
        .sort((a, b) => a.createdAtMs - b.createdAtMs);

      for (const post of pending) {
        if (!aliveRef.current) break;
        try {
          await fbUploadReel(post.id);
        } catch (e) {
          // Lỗi đã được backend persist vào DB + emit failed event.
          // Tiếp tục post kế tiếp (không stop toàn queue).
          console.error(`Upload post ${post.id} failed:`, e);
        }
      }
      await refresh();
    } finally {
      if (aliveRef.current) {
        setState((s) => ({ ...s, uploading: false }));
      }
    }
  }, [refresh]);

  const retry = useCallback(
    async (postId: number) => {
      setState((s) => ({ ...s, uploading: true, error: null }));
      try {
        await fbUploadReel(postId);
      } catch (e) {
        console.error(`Retry post ${postId} failed:`, e);
      } finally {
        await refresh();
        if (aliveRef.current) {
          setState((s) => ({ ...s, uploading: false }));
        }
      }
    },
    [refresh],
  );

  const publishNow = useCallback(
    async (args: PublishNowArgs) => {
      setState((s) => ({ ...s, uploading: true, error: null }));
      let postId: number | null = null;
      try {
        postId = await fbEnqueueReel(args);
        // Refresh sớm để UI hiển thị row pending ngay khi enqueue xong.
        await refresh();
        await fbUploadReel(postId);
      } catch (e) {
        console.error("publishNow failed:", e);
        if (aliveRef.current) {
          setState((s) => ({
            ...s,
            error: (e as Error).message ?? String(e),
          }));
        }
        throw e;
      } finally {
        await refresh();
        if (aliveRef.current) {
          setState((s) => ({ ...s, uploading: false }));
        }
      }
    },
    [refresh],
  );

  return { ...state, refresh, startAll, retry, publishNow };
}
