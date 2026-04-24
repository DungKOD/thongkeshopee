import { useCallback, useEffect, useRef, useState } from "react";
import { listen, type UnlistenFn } from "@tauri-apps/api/event";
import { auth, getAuthToken } from "../lib/firebase";
import { invoke } from "../lib/tauri";
import {
  machineFingerprint,
  syncMetadata,
  syncPullMergePush,
  syncUploadDb,
  type SyncMetadataResult,
} from "../lib/sync";

export type SyncStatus =
  | "checking"
  | "idle"
  | "dirty"
  | "syncing"
  | "error"
  | "offline";

/// Xóa localStorage keys mang dữ liệu user — gọi khi đổi owner (user B
/// login trên máy vừa dùng user A). Không touch key của Firebase SDK /
/// Tauri / browser (vd `firebase:*`, `__tauri__*`) — chúng có scope riêng.
/// Nếu thêm key mới mang dữ liệu user, BẮT BUỘC thêm vào list này.
function wipeUserLocalStorage(): void {
  const prefixes = ["smartcalc:", "thongkeshopee."];
  const toRemove: string[] = [];
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (key && prefixes.some((p) => key.startsWith(p))) {
      toRemove.push(key);
    }
  }
  for (const k of toRemove) {
    try {
      localStorage.removeItem(k);
    } catch {
      // ignore
    }
  }
}

export type SyncPhase = "downloading" | "merging" | "uploading" | null;

interface SyncStateDto {
  dirty: boolean;
  /** Counter mutation — = 0 nghĩa là DB fresh (chưa edit gì từ install này).
   *  Dùng detect reinstall-scenario để tránh upload DB rỗng đè remote. */
  changeId: number;
  lastUploadedChangeId: number;
  last_synced_at_ms: number | null;
  last_synced_remote_mtime_ms: number | null;
  last_error: string | null;
  ownerUid: string | null;
}

interface UseCloudSyncResult {
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

interface UseCloudSyncOptions {
  mutationVersion: number;
  enabled: boolean;
  /// Gọi sau mỗi lần sync thành công (merge có thể đã thêm row từ remote
  /// hoặc apply tombstones) để UI refetch data mới.
  onRemoteApplied?: () => void | Promise<void>;
}

/// Adaptive debounce: mutation đầu tiên dùng DEBOUNCE_BASE_MS, mỗi mutation
/// tiếp theo (reset trong window) tăng ladder lên. Cap ở DEBOUNCE_MAX_MS.
/// Kết quả: user edit liên tục → gộp thành 1 sync cuối thay vì N syncs.
///
/// Tradeoff: user edit 1 lần xong thôi → debounce BASE (45s, giảm R2 ops).
/// User edit 10 lần trong 1 phút → debounce extend dần lên MAX (120s), chỉ
/// 1 upload cuối. Combined skip-identical hash (sync.rs) → mutation không
/// đổi DB (revert + redo cùng state) KHÔNG tốn upload.
const DEBOUNCE_BASE_MS = 45_000;
const DEBOUNCE_MAX_MS = 120_000;
const DEBOUNCE_STEP_MS = 15_000;
const IDLE_MS = 30_000;
const IDLE_CHECK_MS = 5_000;
// Exponential backoff: 30s, 60s, 120s, 300s (cap 5min).
const RETRY_BACKOFF_MS = [30_000, 60_000, 120_000, 300_000];

function backoffFor(attempt: number): number {
  const idx = Math.min(attempt, RETRY_BACKOFF_MS.length - 1);
  return RETRY_BACKOFF_MS[idx];
}

/// Compute adaptive debounce delay dựa trên consecutive mutations.
/// - 1st mutation: BASE (45s)
/// - 2nd: 60s
/// - 3rd: 75s
/// - 6th+: capped at MAX (120s)
function adaptiveDebounce(consecutive: number): number {
  const delay = DEBOUNCE_BASE_MS + consecutive * DEBOUNCE_STEP_MS;
  return Math.min(delay, DEBOUNCE_MAX_MS);
}

const ACTIVITY_EVENTS = ["mousedown", "keydown", "wheel", "touchstart"] as const;

/** Signal Rust trả khi CAS etag mismatch — keep in sync với `sync.rs` const. */
const ETAG_CONFLICT_PREFIX = "ETAG_CONFLICT";

export function useCloudSync({
  mutationVersion,
  enabled,
  onRemoteApplied,
}: UseCloudSyncOptions): UseCloudSyncResult {
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
  /// Đếm mutations liên tục trong window hiện tại (reset khi debounce fire).
  /// Dùng compute delay adaptive: edit càng nhiều liên tục → delay dài hơn.
  const consecutiveMutationsRef = useRef(0);
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
  ///
  /// `prefetched` optional — khi caller vừa fetch metadata + syncState (vd
  /// `doStartupCheck` ngay trước đó), pass xuống để skip Promise.all ở đầu
  /// function. Tiết kiệm 1 Worker `/metadata` req + 1 local invoke. Values
  /// vẫn valid vì giữa 2 await không có mutation (sequential trong startup).
  const doSync = useCallback(
    async (prefetched?: {
      metadata: SyncMetadataResult;
      localFp: string;
      syncState: SyncStateDto;
    }): Promise<void> => {
    const current = auth.currentUser;
    if (!current) return;
    if (!navigator.onLine) {
      setStatus("offline");
      return;
    }
    clearDebounce();
    clearRetry();
    // Reset counter — sync vừa fire xong (bất kể qua debounce, idle flush,
    // hay forceSync), next mutation sẽ start từ BASE = 15s (snappy UX).
    consecutiveMutationsRef.current = 0;
    setStatus("syncing");
    setError(null);
    try {
      const idToken = await getAuthToken();
      const { metadata, localFp, beforeSync } = prefetched
        ? {
            metadata: prefetched.metadata,
            localFp: prefetched.localFp,
            beforeSync: prefetched.syncState,
          }
        : await (async () => {
            const [m, fp, s] = await Promise.all([
              syncMetadata(idToken),
              machineFingerprint(),
              refreshSyncState(),
            ]);
            return { metadata: m, localFp: fp, beforeSync: s };
          })();

      const remoteMtime = metadata.last_modified_ms ?? 0;
      const remoteFp = metadata.fingerprint ?? null;
      const storedRemoteMtime = beforeSync.last_synced_remote_mtime_ms ?? 0;
      const remoteChanged =
        (metadata.exists ?? false) && remoteMtime > storedRemoteMtime;
      const differentMachine = remoteFp === null ? true : remoteFp !== localFp;
      // Fresh install: local chưa có mutation nào (changeId=0) + R2 đã có
      // data → BẮT BUỘC pull-merge-push bất kể fingerprint. Không dùng upload
      // vì sẽ đè DB rỗng lên remote (Rust-side cũng reject, đây là defensive
      // FE để UX tốt hơn: chạy merge thẳng thay vì fail error).
      const localFresh =
        beforeSync.changeId === 0 && beforeSync.lastUploadedChangeId === 0;
      const freshWithRemote = localFresh && (metadata.exists ?? false);
      const needMerge = freshWithRemote || (remoteChanged && differentMachine);
      console.log("[sync] doSync decision:", {
        localFresh,
        "metadata.exists": metadata.exists,
        freshWithRemote,
        remoteChanged,
        differentMachine,
        needMerge,
        action: needMerge ? "syncPullMergePush" : "syncUploadDb",
        changeId: beforeSync.changeId,
        lastUploadedChangeId: beforeSync.lastUploadedChangeId,
      });

      // CAS upload retry: nếu upload thẳng bị 412 (máy khác đã upload giữa
      // chừng), Rust trả error message bắt đầu "ETAG_CONFLICT" → tự động
      // route sang pull-merge-push để merge data máy kia rồi re-upload.
      // pull-merge-push đã có internal retry 3 lần nên không cần wrap thêm.
      let res;
      let mergeHappened = needMerge;
      if (needMerge) {
        res = await syncPullMergePush(idToken);
      } else {
        try {
          res = await syncUploadDb(idToken, metadata.exists ?? false);
        } catch (e) {
          const msg = (e as Error).message ?? String(e);
          if (msg.startsWith(ETAG_CONFLICT_PREFIX)) {
            console.log("[sync] upload CAS conflict → fallback pull-merge-push");
            res = await syncPullMergePush(idToken);
            mergeHappened = true;
          } else {
            throw e;
          }
        }
      }
      setLastSyncAt(new Date(res.last_modified_ms));

      const state = await refreshSyncState();
      retryAttemptRef.current = 0;

      // Chỉ refetch UI khi merge (đã chạm vào local DB). Upload thẳng không cần.
      if (mergeHappened) {
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
    const delay = adaptiveDebounce(consecutiveMutationsRef.current);
    const timerId = window.setTimeout(() => {
      debounceRef.current = null;
      // Reset counter khi debounce fire — next mutation sau đó sẽ start từ BASE.
      consecutiveMutationsRef.current = 0;
      void doSync();
    }, delay);
    debounceRef.current = timerId;
  }, [doSync]);

  /// Startup check: kiểm tra metadata R2 → nếu có dirty local HOẶC remote mới
  /// + khác máy → chạy doSync (pull-merge-push). Merge chạy trong cùng
  /// connection nên không cần restart.
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
        getAuthToken(),
        machineFingerprint(),
      ]);
      const remote = await syncMetadata(idToken);

      const syncState = await refreshSyncState();

      // Case 1: remote chưa tồn tại → doSync sẽ skip pull + upload lần đầu.
      // Pass prefetched để doSync reuse metadata + syncState thay vì refetch.
      if (!remote.exists) {
        console.log("[sync] startupCheck: remote không tồn tại → doSync (upload init)");
        await doSync({ metadata: remote, localFp, syncState });
        return;
      }

      const remoteMtime = remote.last_modified_ms ?? 0;
      const remoteFp = remote.fingerprint ?? null;
      const storedRemoteMtime = syncState.last_synced_remote_mtime_ms ?? 0;

      const remoteChanged = remoteMtime > storedRemoteMtime;
      const differentMachine =
        remoteFp === null ? true : remoteFp !== localFp;
      // Local fresh = changeId=0 + lastUploadedChangeId=0 → DB chưa có mutation
      // nào từ install này (reinstall, clear DB, fresh login). BẮT BUỘC pull khi
      // remote có data — nếu không, user sẽ thấy UI rỗng và mutation kế sẽ đè
      // mất R2 backup (change_id > 0 sau mutation → Rust-side guard không trigger).
      // Trước đây chỉ check (remoteChanged && differentMachine) → miss khi same
      // machine reinstall (fingerprint giống) → data loss.
      const localFresh =
        syncState.changeId === 0 && syncState.lastUploadedChangeId === 0;

      console.log("[sync] doStartupCheck state:", {
        "remote.exists": remote.exists,
        dirty: syncState.dirty,
        changeId: syncState.changeId,
        lastUploadedChangeId: syncState.lastUploadedChangeId,
        localFresh,
        remoteChanged,
        differentMachine,
        willSync:
          syncState.dirty || localFresh || (remoteChanged && differentMachine),
      });

      // Case 2: fresh install + remote có data (reinstall, cùng máy)
      //         HOẶC dirty local
      //         HOẶC remote mới từ máy khác
      //         → pull-merge-push.
      if (
        syncState.dirty ||
        localFresh ||
        (remoteChanged && differentMachine)
      ) {
        await doSync({ metadata: remote, localFp, syncState });
        return;
      }

      // Case 3: đã đồng bộ, không cần làm gì.
      console.log("[sync] startupCheck: không cần sync (đã đồng bộ)");
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

  // Track Firebase auth UID — logout/login cùng app session phải trigger
  // startup check lại (nếu không → flow "logout → xóa DB → login" miss sync).
  const [authUid, setAuthUid] = useState<string | null>(
    auth.currentUser?.uid ?? null,
  );
  useEffect(() => {
    const unsub = auth.onAuthStateChanged((user) => {
      setAuthUid(user?.uid ?? null);
    });
    return () => unsub();
  }, []);

  // Startup check — chạy MỖI LẦN login (kể cả cùng user, sau logout).
  // Reset ref trên logout → login tiếp theo re-run.
  //
  // CRITICAL: trước khi sync, gọi `sync_reset_for_new_user(authUid)` để đảm
  // bảo local DB thuộc về user hiện tại. Nếu DB của user khác → wipe + refetch
  // sync_state. Tránh leak data user A sang UI user B + tránh upload DB của A
  // lên R2 path users/{uid_B}/.
  useEffect(() => {
    if (!enabled) return;
    if (!authUid) {
      startupDoneRef.current = false;
      setIsStartupPhase(true);
      return;
    }
    if (startupDoneRef.current) return;
    startupDoneRef.current = true;
    void (async () => {
      // CRITICAL: nếu switch_db_to_user fail, DbState vẫn trỏ _pre_auth (rỗng)
      // → unlock UI sẽ hiển thị empty state, user tưởng mất data. Tách try
      // riêng cho switch: fail → keep splash + error banner. doStartupCheck
      // fail thì được unlock vì DB đã swap đúng, merge fail cũng không corrupt.
      try {
        const ownerChanged = await invoke<boolean>("switch_db_to_user", {
          newUid: authUid,
        });
        if (ownerChanged) {
          console.log(
            "[sync] swapped DB to users/" + authUid + "/ folder",
          );
          // Clear localStorage scoped per-app (calculator history + filter +
          // settings) vì toàn bộ đều mang dấu vết user cũ. KHÔNG clear all
          // localStorage vì Firebase SDK / Tauri có key riêng — chỉ key app.
          wipeUserLocalStorage();
        }
      } catch (e) {
        const msg = (e as Error).message ?? String(e);
        console.error("[sync] switch_db_to_user failed:", msg);
        setError(msg);
        setStatus("error");
        startupDoneRef.current = false; // allow retry on re-auth / reload
        return; // KHÔNG setIsStartupPhase(false) — splash stays với error
      }

      // Switch xong → UI có thể unlock safely. doStartupCheck/merge fail vẫn
      // OK vì DB đã đúng folder user.
      try {
        // CRITICAL phân quyền: LUÔN refetch FE state sau khi switch_db_to_user
        // xong (bất kể ownerChanged), vì DbState connection đã trỏ sang DB mới
        // của user hiện tại. AccountContext + useDbStats gọi list/query trước
        // khi switch xong sẽ đọc từ DB cũ (race condition) — refresh ở đây
        // overwrite state với data từ DB đúng.
        try {
          await onRemoteAppliedRef.current?.();
        } catch {
          // ignore — UI sẽ refresh sau startup check.
        }
        await doStartupCheck();
      } finally {
        setIsStartupPhase(false);
      }
    })();
  }, [enabled, authUid, doStartupCheck]);

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
  // có mutation mới → adaptive debounce đếm lại + ladder tăng delay.
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
    // Tăng consecutive counter — mutation tiếp theo sẽ có delay lớn hơn
    // (ladder BASE → +STEP → +STEP → ... → MAX). Cap ở MAX (60s).
    consecutiveMutationsRef.current += 1;
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
