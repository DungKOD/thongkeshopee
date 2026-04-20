import { useCallback, useEffect, useRef, useState } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { auth } from "../lib/firebase";
import { invoke } from "../lib/tauri";
import {
  driveMetadata,
  drivePullMergePush,
  driveUploadDb,
  machineFingerprint,
} from "../lib/drive";

export type SyncStatus =
  | "checking"
  | "idle"
  | "dirty"
  | "syncing"
  | "error"
  | "offline";

export type SyncPhase = "downloading" | "merging" | "uploading" | null;

interface SyncStateDto {
  dirty: boolean;
  last_synced_at_ms: number | null;
  last_synced_remote_mtime_ms: number | null;
  last_error: string | null;
}

interface UseDriveSyncResult {
  status: SyncStatus;
  /// True từ lúc hook được mount đến khi startup check hoàn tất lần đầu.
  /// App.tsx dùng để chặn UI splash suốt startup kể cả khi status chuyển "syncing".
  isStartupPhase: boolean;
  /// Phase hiện tại đang chạy trong backend (downloading/merging/uploading).
  /// null khi không có sync đang chạy.
  syncPhase: SyncPhase;
  lastSyncAt: Date | null;
  error: string | null;
  forceSync: () => Promise<void>;
}

interface UseDriveSyncOptions {
  mutationVersion: number;
  enabled: boolean;
  /// Gọi sau mỗi lần sync thành công (merge có thể đã thêm row từ remote
  /// hoặc apply tombstones) để UI refetch data mới.
  onRemoteApplied?: () => void | Promise<void>;
}

const DEBOUNCE_MS = 15_000;
const IDLE_MS = 30_000;
const IDLE_CHECK_MS = 5_000;
// Exponential backoff: 30s, 60s, 120s, 300s (cap 5min).
const RETRY_BACKOFF_MS = [30_000, 60_000, 120_000, 300_000];

function backoffFor(attempt: number): number {
  const idx = Math.min(attempt, RETRY_BACKOFF_MS.length - 1);
  return RETRY_BACKOFF_MS[idx];
}

const ACTIVITY_EVENTS = ["mousedown", "keydown", "wheel", "touchstart"] as const;

export function useDriveSync({
  mutationVersion,
  enabled,
  onRemoteApplied,
}: UseDriveSyncOptions): UseDriveSyncResult {
  const [status, setStatus] = useState<SyncStatus>(() =>
    typeof navigator !== "undefined" && !navigator.onLine ? "offline" : "checking",
  );
  const [lastSyncAt, setLastSyncAt] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isStartupPhase, setIsStartupPhase] = useState(true);
  const [syncPhase, setSyncPhase] = useState<SyncPhase>(null);

  const debounceRef = useRef<number | null>(null);
  const retryRef = useRef<number | null>(null);
  const idleCheckRef = useRef<number | null>(null);
  const retryAttemptRef = useRef(0);
  const lastActivityRef = useRef(Date.now());
  const startupDoneRef = useRef(false);
  const statusRef = useRef<SyncStatus>(status);
  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  const onRemoteAppliedRef = useRef(onRemoteApplied);
  useEffect(() => {
    onRemoteAppliedRef.current = onRemoteApplied;
  }, [onRemoteApplied]);

  const clearDebounce = () => {
    if (debounceRef.current !== null) {
      clearTimeout(debounceRef.current);
      debounceRef.current = null;
    }
  };
  const clearRetry = () => {
    if (retryRef.current !== null) {
      clearTimeout(retryRef.current);
      retryRef.current = null;
    }
  };

  const refreshSyncState = useCallback(async (): Promise<SyncStateDto> => {
    const s = await invoke<SyncStateDto>("sync_state_get");
    setLastSyncAt(
      s.last_synced_at_ms ? new Date(s.last_synced_at_ms) : null,
    );
    return s;
  }, []);

  /// Sync strategy thông minh:
  /// - Fetch metadata trước. Nếu remote mới + khác máy → pull-merge-push (merge data).
  /// - Ngược lại → upload thẳng (rẻ hơn, bỏ qua merge khi không cần).
  const doSync = useCallback(async (): Promise<void> => {
    const current = auth.currentUser;
    if (!current) return;
    if (!navigator.onLine) {
      setStatus("offline");
      return;
    }
    clearDebounce();
    clearRetry();
    setStatus("syncing");
    setError(null);
    try {
      const idToken = await current.getIdToken(false);
      const [metadata, localFp, beforeSync] = await Promise.all([
        driveMetadata(idToken),
        machineFingerprint(),
        refreshSyncState(),
      ]);

      const remoteMtime = metadata.last_modified_ms ?? 0;
      const remoteFp = metadata.fingerprint ?? null;
      const storedRemoteMtime = beforeSync.last_synced_remote_mtime_ms ?? 0;
      const remoteChanged =
        (metadata.exists ?? false) && remoteMtime > storedRemoteMtime;
      const differentMachine = remoteFp === null ? true : remoteFp !== localFp;
      const needMerge = remoteChanged && differentMachine;

      const res = needMerge
        ? await drivePullMergePush(idToken)
        : await driveUploadDb(idToken);
      setLastSyncAt(new Date(res.last_modified_ms));

      const state = await refreshSyncState();
      retryAttemptRef.current = 0;

      // Chỉ refetch UI khi merge (đã chạm vào local DB). Upload thẳng không cần.
      if (needMerge) {
        try {
          await onRemoteAppliedRef.current?.();
        } catch {
          // ignore — best effort.
        }
      }

      if (state.dirty) {
        setStatus("dirty");
        scheduleDebounce();
      } else {
        setStatus("idle");
      }
    } catch (e) {
      const msg = (e as Error).message ?? String(e);
      setError(msg);
      setStatus("error");
      try {
        await invoke("sync_state_record_error", { message: msg });
      } catch {
        // ignore — best effort.
      }
      const delay = backoffFor(retryAttemptRef.current);
      retryAttemptRef.current += 1;
      retryRef.current = window.setTimeout(() => {
        retryRef.current = null;
        void doSync();
      }, delay);
    } finally {
      setSyncPhase(null);
    }
    // scheduleDebounce khai báo bên dưới — forward ref qua closure.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [refreshSyncState]);

  const scheduleDebounce = useCallback(() => {
    clearDebounce();
    const timerId = window.setTimeout(() => {
      debounceRef.current = null;
      void doSync();
    }, DEBOUNCE_MS);
    debounceRef.current = timerId;
  }, [doSync]);

  /// Startup check: kiểm tra metadata Drive → nếu có dirty local HOẶC
  /// remote mới + khác máy → chạy doSync (pull-merge-push). Merge chạy trong
  /// cùng connection nên không cần restart.
  const doStartupCheck = useCallback(async (): Promise<void> => {
    const current = auth.currentUser;
    if (!current) return;
    if (!navigator.onLine) {
      setStatus("offline");
      return;
    }
    setStatus("checking");
    setError(null);
    try {
      const [idToken, localFp] = await Promise.all([
        current.getIdToken(false),
        machineFingerprint(),
      ]);
      const remote = await driveMetadata(idToken);

      const syncState = await refreshSyncState();

      // Case 1: remote chưa tồn tại → doSync sẽ skip pull + upload lần đầu.
      if (!remote.exists) {
        await doSync();
        return;
      }

      const remoteMtime = remote.last_modified_ms ?? 0;
      const remoteFp = remote.fingerprint ?? null;
      const storedRemoteMtime = syncState.last_synced_remote_mtime_ms ?? 0;

      const remoteChanged = remoteMtime > storedRemoteMtime;
      const differentMachine =
        remoteFp === null ? true : remoteFp !== localFp;

      // Case 2: dirty local HOẶC remote mới từ máy khác → pull-merge-push.
      if (syncState.dirty || (remoteChanged && differentMachine)) {
        await doSync();
        return;
      }

      // Case 3: đã đồng bộ, không cần làm gì.
      retryAttemptRef.current = 0;
      setStatus("idle");
    } catch (e) {
      const msg = (e as Error).message ?? String(e);
      setError(msg);
      setStatus("error");
      const delay = backoffFor(retryAttemptRef.current);
      retryAttemptRef.current += 1;
      retryRef.current = window.setTimeout(() => {
        retryRef.current = null;
        void doStartupCheck();
      }, delay);
    }
  }, [doSync, refreshSyncState]);

  // Startup check — chạy 1 lần khi enabled chuyển true.
  useEffect(() => {
    if (!enabled) return;
    if (startupDoneRef.current) return;
    startupDoneRef.current = true;
    void (async () => {
      try {
        await doStartupCheck();
      } finally {
        setIsStartupPhase(false);
      }
    })();
  }, [enabled, doStartupCheck]);

  // Listen sync-phase events từ Rust backend — update syncPhase để UI render text đúng.
  useEffect(() => {
    if (!enabled) return;
    let unlisten: UnlistenFn | null = null;
    let cancelled = false;
    listen<string>("sync-phase", (event) => {
      const p = event.payload;
      if (p === "downloading" || p === "merging" || p === "uploading") {
        setSyncPhase(p);
      } else {
        setSyncPhase(null);
      }
    }).then((fn) => {
      if (cancelled) fn();
      else unlisten = fn;
    });
    return () => {
      cancelled = true;
      if (unlisten) unlisten();
    };
  }, [enabled]);

  // Mutation → debounce sync. Reset timer mỗi mutation (user đề xuất: giây thứ 7
  // có mutation mới → 15s đếm lại từ đầu).
  useEffect(() => {
    if (!enabled) return;
    if (
      status === "checking" ||
      status === "syncing" ||
      status === "offline"
    ) {
      return;
    }
    if (mutationVersion === 0) return;

    setStatus("dirty");
    scheduleDebounce();

    return () => {
      clearDebounce();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mutationVersion, enabled, scheduleDebounce]);

  // Idle flush: user idle IDLE_MS + status dirty → sync sớm (bỏ qua debounce).
  useEffect(() => {
    if (!enabled) return;
    const onActivity = () => {
      lastActivityRef.current = Date.now();
    };
    ACTIVITY_EVENTS.forEach((ev) =>
      window.addEventListener(ev, onActivity, { passive: true }),
    );
    idleCheckRef.current = window.setInterval(() => {
      if (statusRef.current !== "dirty") return;
      if (Date.now() - lastActivityRef.current < IDLE_MS) return;
      clearDebounce();
      void doSync();
    }, IDLE_CHECK_MS);
    return () => {
      ACTIVITY_EVENTS.forEach((ev) =>
        window.removeEventListener(ev, onActivity),
      );
      if (idleCheckRef.current !== null) {
        clearInterval(idleCheckRef.current);
        idleCheckRef.current = null;
      }
    };
  }, [enabled, doSync]);

  // Online/offline listeners.
  useEffect(() => {
    if (!enabled) return;
    const onOnline = () => {
      retryAttemptRef.current = 0;
      clearRetry();
      void (async () => {
        const s = await refreshSyncState();
        if (s.dirty) {
          await doSync();
        } else {
          setStatus("idle");
        }
      })();
    };
    const onOffline = () => {
      clearDebounce();
      clearRetry();
      setStatus("offline");
    };
    window.addEventListener("online", onOnline);
    window.addEventListener("offline", onOffline);
    return () => {
      window.removeEventListener("online", onOnline);
      window.removeEventListener("offline", onOffline);
    };
  }, [enabled, doSync, refreshSyncState]);

  // On-exit flush — user tắt app khi dirty → best effort push.
  useEffect(() => {
    if (!enabled) return;
    const handler = () => {
      if (statusRef.current === "dirty") {
        void doSync();
      }
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [enabled, doSync]);

  const forceSync = useCallback(async (): Promise<void> => {
    clearDebounce();
    clearRetry();
    retryAttemptRef.current = 0;
    await doSync();
  }, [doSync]);

  return { status, isStartupPhase, syncPhase, lastSyncAt, error, forceSync };
}
