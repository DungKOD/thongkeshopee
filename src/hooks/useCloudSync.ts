import { useCallback, useEffect, useRef, useState } from "react";
import { auth, getAuthToken } from "../lib/firebase";
import { invoke } from "../lib/tauri";
import {
  syncV9CompactIfNeeded,
  syncV9GetState,
  syncV9LogFlush,
  syncV9PullAll,
  syncV9PushAll,
  type SyncV9State,
} from "../lib/sync_v9";
import { announceMyPush, subscribeRemotePushes } from "../lib/sync_notify";

export type SyncStatus =
  | "checking"
  | "idle"
  | "dirty"
  | "syncing"
  | "error"
  | "offline"
  | "bootstrap";

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

/// Phase đang chạy trong 1 sync cycle. Dùng SplashScreen + SyncBadge render
/// text phù hợp. FE tự track theo command đang gọi — v9 commands không emit
/// event như v8, nên state chuyển chỉ quanh 2 tên "pulling" → "pushing".
export type SyncPhase = "pulling" | "pushing" | null;

/// Thống kê của sync cycle gần nhất — bytes + counts để UI hiển thị.
export interface LastSyncStats {
  /// Tổng bytes tải về từ R2 (zstd-compressed delta files).
  downloadBytes: number;
  /// Tổng bytes upload lên R2 (zstd-compressed delta files).
  uploadBytes: number;
  /// Số delta files đã pull + apply.
  pulledDeltas: number;
  /// Số delta files đã push.
  pushedDeltas: number;
  /// Số table skip upload do hash identical với lần trước (cost saved).
  skippedIdentical: number;
}

interface UseCloudSyncResult {
  status: SyncStatus;
  /// True từ lúc hook được mount đến khi startup check hoàn tất lần đầu.
  /// App.tsx dùng để chặn UI splash suốt startup kể cả khi status chuyển "syncing".
  isStartupPhase: boolean;
  /// Phase hiện tại đang chạy trong backend (pulling / pushing).
  syncPhase: SyncPhase;
  lastSyncAt: Date | null;
  /// Stats của sync cycle gần nhất (null = chưa sync lần nào).
  lastSyncStats: LastSyncStats | null;
  /// True = phát hiện máy khác vừa push (RTDB event chưa được apply). UI
  /// dùng enable nút "Sync ngay" khi status=idle (không có thay đổi local
  /// nhưng remote có). Reset false khi doSync() chạy xong (đã pull).
  hasRemoteChangePending: boolean;
  error: string | null;
  forceSync: () => Promise<void>;
}

interface UseCloudSyncOptions {
  mutationVersion: number;
  enabled: boolean;
  /// L1 form-dirty defer: user đang edit form (ManualEntryDialog) → hoãn
  /// auto-sync (debounce/idle/cron tick) để pull không apply event cho row
  /// đang edit → đè mất input. Force sync vẫn chạy khi user click.
  /// Transition true→false sẽ trigger 1 sync catch-up ngay lập tức.
  pausedByForm?: boolean;
  /// Gọi sau mỗi lần pull đã apply deltas mới từ R2 (UI phải refetch).
  onRemoteApplied?: () => void | Promise<void>;
}

/// Hybrid trigger — 3 điều kiện song song flush sync lên R2.
///
/// 1. **DEBOUNCE_MS** (45s): idle quiet → user đã ngừng edit, push.
/// 2. **COUNT_THRESHOLD** (100 mutations): gom đủ 1 bundle lớn → push ngay
///    kể cả user vẫn đang edit. Tránh case "continuous edit 1mut/20s" →
///    debounce reset vĩnh viễn → KHÔNG BAO GIỜ push (bug correctness).
/// 3. **MAX_WAIT_MS** (5min): safety cap — chưa đủ 100 mut cũng force push
///    khi mutation đầu tiên quá xa. Cap data-loss-if-crash window.
///
/// Combined skip-identical hash (sync_v9/push.rs) → mutation không đổi
/// content table KHÔNG tốn upload dù trigger fire.
const DEBOUNCE_MS = 45_000;
const COUNT_THRESHOLD = 100;
const MAX_WAIT_MS = 300_000;
const IDLE_MS = 30_000;
const IDLE_CHECK_MS = 5_000;
// Exponential backoff: 30s, 60s, 120s, 300s (cap 5min).
const RETRY_BACKOFF_MS = [30_000, 60_000, 120_000, 300_000];
/// Fallback auto-sync tick — **safety net** khi Firebase RTDB không kết nối
/// được (mất mạng Firebase RTDB nhưng Cloudflare R2 OK, hoặc chưa cấu hình
/// VITE_FIREBASE_DATABASE_URL). Realtime push notifications qua RTDB là
/// primary path (xem `sync_notify.ts`).
///
/// Tăng từ 5 phút → 30 phút vì push notifications cover phần lớn case.
/// 30 phút x 100 user x 24h = ~144k req/tháng (free tier R2 Class B
/// không tốn trong 1M đầu). Zero request khi user chỉ có 1 máy online.
/// 2h: RTDB notify đã là primary path realtime → tick chỉ là safety net khi
/// RTDB lỡ event (rare). Tăng từ 30min → 2h tiết kiệm ~36 GET manifest/day/
/// máy. Worst case lỡ tick: user thấy data máy khác chậm 2h (chỉ khi RTDB
/// fail hoàn toàn, normally < 1s qua notify).
const AUTO_SYNC_INTERVAL_MS = 2 * 60 * 60 * 1000;
/// Force sync throttle cơ bản (spam click protection).
const FORCE_SYNC_MIN_GAP_MS = 2_000;

function backoffFor(attempt: number): number {
  const idx = Math.min(attempt, RETRY_BACKOFF_MS.length - 1);
  return RETRY_BACKOFF_MS[idx];
}

const ACTIVITY_EVENTS = ["mousedown", "keydown", "wheel", "touchstart"] as const;

export function useCloudSync({
  mutationVersion,
  enabled,
  pausedByForm = false,
  onRemoteApplied,
}: UseCloudSyncOptions): UseCloudSyncResult {
  const [status, setStatus] = useState<SyncStatus>(() =>
    typeof navigator !== "undefined" && !navigator.onLine ? "offline" : "checking",
  );
  const [lastSyncAt, setLastSyncAt] = useState<Date | null>(null);
  const [lastSyncStats, setLastSyncStats] = useState<LastSyncStats | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isStartupPhase, setIsStartupPhase] = useState(true);
  const [syncPhase, setSyncPhase] = useState<SyncPhase>(null);

  const debounceRef = useRef<number | null>(null);
  const retryRef = useRef<number | null>(null);
  const idleCheckRef = useRef<number | null>(null);
  const autoSyncTickRef = useRef<number | null>(null);
  const retryAttemptRef = useRef(0);
  const lastActivityRef = useRef(Date.now());
  const startupDoneRef = useRef(false);
  /// Device fingerprint — lấy 1 lần sau mount qua Tauri cmd. Dùng cho RTDB
  /// push notification (tránh echo về chính mình).
  const myFingerprintRef = useRef<string | null>(null);
  /// Flag "có thay đổi từ máy khác" — set true khi RTDB subscriber phát hiện
  /// event từ fingerprint khác. Clear false khi doSync start (đã consume tín
  /// hiệu). UI dùng enable nút sync trong status=idle.
  const [hasRemoteChangePending, setHasRemoteChangePending] = useState(false);
  /// Timestamp forceSync cuối cùng — dùng throttle 2s cho nút "Đồng bộ ngay".
  const lastForceAtRef = useRef(0);
  /// Guard re-entry — true từ lúc doSync bắt đầu đến khi finally. Tránh
  /// 2 sync concurrent (force click khi debounce vừa fire chẳng hạn).
  const syncInFlightRef = useRef(false);
  /// True khi caller yêu cầu pull (startup / forceSync / RTDB event). False
  /// khi mutation-triggered (chỉ cần push). Set bởi caller trước khi
  /// gọi doSync, clear trong finally.
  const needPullRef = useRef(true); // startup đầu tiên luôn pull
  /// Ngày flush log cuối cùng (YYYY-MM-DD). Skip log flush nếu same date +
  /// queue chưa critical — A3 optimization giảm Class A PUT.
  const lastLogFlushDateRef = useRef<string>("");
  /// Timestamp mutation gần nhất — FE dùng để tính timer có reset không.
  /// Flat debounce 45s, không còn counter ladder.
  const lastMutationAtRef = useRef(0);
  /// Số mutations tích lũy từ lần sync gần nhất. Dùng COUNT_THRESHOLD trigger.
  const pendingCountRef = useRef(0);
  /// Timestamp mutation ĐẦU TIÊN sau lần sync gần nhất. Dùng MAX_WAIT_MS cap.
  /// 0 = chưa có mutation pending.
  const firstMutationAtRef = useRef(0);
  const statusRef = useRef<SyncStatus>(status);
  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  const onRemoteAppliedRef = useRef(onRemoteApplied);
  useEffect(() => {
    onRemoteAppliedRef.current = onRemoteApplied;
  }, [onRemoteApplied]);

  /// Mirror pausedByForm vào ref để callbacks trong subscribeRemotePushes
  /// effect đọc giá trị mới nhất mà KHÔNG cần re-subscribe RTDB listener.
  /// Mỗi lần form open/close sẽ không detach + attach lại listener (gây
  /// initial snapshot fire lại + skip event ngắn ~100ms). Dep array của
  /// RTDB effect giờ chỉ còn enabled/authUid/fingerprintReady.
  const pausedByFormRef = useRef(pausedByForm);
  useEffect(() => {
    pausedByFormRef.current = pausedByForm;
  }, [pausedByForm]);

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

  /// Pull (nhận remote) → push (gửi local). Thứ tự này giảm CAS conflict
  /// khi 2 máy cùng sync. Mỗi phase emit syncPhase để SplashScreen/SyncBadge
  /// đổi label text.
  const doSync = useCallback(async (): Promise<void> => {
    const current = auth.currentUser;
    if (!current) return;
    if (!navigator.onLine) {
      setStatus("offline");
      return;
    }
    // Reentry guard — 2 doSync concurrent sẽ ăn cùng DB lock ở Rust side,
    // UI status bounce lung tung. Caller thứ 2 bail out.
    if (syncInFlightRef.current) return;
    syncInFlightRef.current = true;
    clearDebounce();
    clearRetry();
    // Reset mutation counters — sync vừa fire xong, next mutation start fresh.
    // COUNT_THRESHOLD + MAX_WAIT_MS trigger bắt đầu đếm lại từ 0.
    lastMutationAtRef.current = 0;
    pendingCountRef.current = 0;
    firstMutationAtRef.current = 0;
    // Clear remote-pending flag — sync đang consume tín hiệu RTDB.
    setHasRemoteChangePending(false);
    setStatus("syncing");
    setError(null);
    try {
      const idToken = await getAuthToken();

      // B5 optimization: skip pull nếu không có lý do (local-only mutation
      // trigger, không có RTDB event). Pull mỗi lần tốn 1 get_manifest GET
      // dù không có delta mới. Chỉ pull khi: startup / forceSync /
      // RTDB remote push event / online handler.
      //
      // Logic safety: needPullRef mặc định true (startup, RTDB, force). Chỉ
      // khi mutation path (scheduleDebounce → doSync) mới set false.
      let pullReport = {
        appliedDeltas: 0,
        totalEvents: 0,
        skipped: 0,
        skippedByHlc: 0,
        tombstonesApplied: 0,
        rowsDeleted: 0,
        totalBytes: 0,
      };
      if (needPullRef.current) {
        setSyncPhase("pulling");
        pullReport = await syncV9PullAll(idToken);
      }

      setSyncPhase("pushing");
      const pushReport = await syncV9PushAll(idToken);

      setLastSyncAt(new Date());
      setLastSyncStats({
        downloadBytes: pullReport.totalBytes,
        uploadBytes: pushReport.totalBytes,
        pulledDeltas: pullReport.appliedDeltas,
        pushedDeltas: pushReport.uploadedCount,
        skippedIdentical: pushReport.skippedIdentical,
      });
      retryAttemptRef.current = 0;

      // UI refetch nếu pull đã apply deltas (local DB có row mới từ máy khác).
      if (pullReport.appliedDeltas > 0) {
        try {
          await onRemoteAppliedRef.current?.();
        } catch {
          // ignore — best effort.
        }
      }

      console.log("[sync v9]", {
        pulled: pullReport.appliedDeltas,
        pulledBytes: pullReport.totalBytes,
        pushed: pushReport.uploadedCount,
        pushedBytes: pushReport.totalBytes,
        skippedIdentical: pushReport.skippedIdentical,
        casRetries: pushReport.casRetries,
      });

      // Broadcast push event qua Firebase RTDB — máy khác cùng UID sẽ nhận
      // trong <1s và tự pull. Chỉ announce khi thực sự có upload (> 0 file).
      // Tolerant với lỗi: write fail không block sync flow.
      if (pushReport.uploadedCount > 0 && myFingerprintRef.current) {
        const uid = current.uid;
        void announceMyPush(
          uid,
          myFingerprintRef.current,
          pushReport.totalBytes,
        );
      }

      // Check compaction trigger (P10, best-effort). Threshold > 100 deltas
      // → tạo snapshot + clear manifest.deltas. Long-running (upload 500MB)
      // nên detach — status UI không block.
      void syncV9CompactIfNeeded(idToken)
        .then((r) => {
          if (r.triggered) {
            console.log("[sync v9 compact]", r);
          }
        })
        .catch((e) => {
          console.warn("[sync v9] compact failed:", e);
        });

      // Dirty check sau sync: có thể user vừa edit trong lúc push đang chạy
      // → pendingPushTables vẫn > 0 → debounce tiếp.
      const state = await syncV9GetState();
      if (state.freshInstallPending) {
        setStatus("bootstrap");
      } else if (state.pendingPushTables.length > 0) {
        setStatus("dirty");
        scheduleDebounce();
      } else {
        setStatus("idle");
      }

      // A3 optimization: flush log lazy. Trigger khi:
      // - Date rollover (mới ngày mới) — default case, ~1 flush/ngày
      // - OR queue > 50 events pending — avoid dồn quá nhiều trong 1 ngày
      //   nếu user active cao (UX: user mở log dialog không thấy số lớn)
      // Safety: events persist local, beforeunload flush backup khi close.
      //
      // Guard pendingLogCount > 0: tránh Tauri IPC round-trip ở Rust khi
      // queue rỗng (ví dụ đổi ngày nhưng hôm nay chưa có event nào).
      // Rust return 0 nhanh, nhưng IPC vẫn tốn ~1-2ms/call × N sync cycles.
      const today = new Date().toISOString().slice(0, 10);
      const LOG_FLUSH_QUEUE_THRESHOLD = 50;
      const shouldFlushLog =
        state.pendingLogCount > 0 &&
        (lastLogFlushDateRef.current !== today ||
          state.pendingLogCount >= LOG_FLUSH_QUEUE_THRESHOLD);
      if (shouldFlushLog) {
        lastLogFlushDateRef.current = today;
        void syncV9LogFlush(idToken).catch((e) => {
          console.warn("[sync v9] log flush failed:", e);
        });
      }
    } catch (e) {
      const msg = (e as Error).message ?? String(e);
      setError(msg);
      setStatus("error");
      const delay = backoffFor(retryAttemptRef.current);
      retryAttemptRef.current += 1;
      retryRef.current = window.setTimeout(() => {
        retryRef.current = null;
        void doSync();
      }, delay);
    } finally {
      setSyncPhase(null);
      syncInFlightRef.current = false;
      // Reset về default pull=true cho next sync (safe default).
      // Callers gọi doSync vì mutation sẽ set false trước mỗi lần gọi.
      needPullRef.current = true;
    }
    // scheduleDebounce khai báo bên dưới — forward ref qua closure.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /// Flat debounce — mỗi call clear timer cũ, setTimeout mới DEBOUNCE_MS.
  /// Mutation liên tục reset về fresh 45s, không ladder-up.
  ///
  /// B5 optimization: mutation-triggered sync KHÔNG cần pull (local dirty,
  /// push-only đủ). needPullRef=false bỏ qua manifest GET.
  const scheduleDebounce = useCallback(() => {
    clearDebounce();
    const timerId = window.setTimeout(() => {
      debounceRef.current = null;
      needPullRef.current = false; // push-only sync
      void doSync();
    }, DEBOUNCE_MS);
    debounceRef.current = timerId;
  }, [doSync]);

  /// Flush ngay không chờ debounce. Dùng cho COUNT_THRESHOLD + MAX_WAIT_MS
  /// trigger — không phải idle quiet nên vẫn push-only (B5).
  const flushNow = useCallback(() => {
    clearDebounce();
    needPullRef.current = false;
    void doSync();
  }, [doSync]);

  /// Startup check: trong v9 chỉ cần gọi doSync — sync_v9_sync_all cheap khi
  /// không có gì đổi (fetch manifest + empty diff → no-op). Manifest etag +
  /// cursor state ở Rust tự xác định cần fetch/apply gì. Không cần fingerprint
  /// check như v8.
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
      // Peek state trước để UI hiển thị đúng splash (bootstrap vs normal sync).
      const beforeState: SyncV9State = await syncV9GetState();
      if (beforeState.freshInstallPending) {
        setStatus("bootstrap");
      }
      await doSync();
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
  }, [doSync]);

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
  // CRITICAL: trước khi sync, gọi `sync_reset_for_new_user(authUid)` (qua
  // `switch_db_to_user`) để đảm bảo local DB thuộc về user hiện tại. Nếu
  // DB của user khác → wipe + refetch state. Tránh leak data user A sang
  // UI user B + tránh upload DB của A lên R2 path users/{uid_B}/.
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
      // fail thì được unlock vì DB đã swap đúng, sync fail cũng không corrupt.
      try {
        const ownerChanged = await invoke<boolean>("switch_db_to_user", {
          newUid: authUid,
        });
        if (ownerChanged) {
          console.log(
            "[sync v9] swapped DB to users/" + authUid + "/ folder",
          );
          wipeUserLocalStorage();
        }
      } catch (e) {
        const msg = (e as Error).message ?? String(e);
        console.error("[sync v9] switch_db_to_user failed:", msg);
        setError(msg);
        setStatus("error");
        startupDoneRef.current = false; // allow retry on re-auth / reload
        return; // KHÔNG setIsStartupPhase(false) — splash stays với error
      }

      // Switch xong → UI có thể unlock safely. doStartupCheck/sync fail vẫn
      // OK vì DB đã đúng folder user.
      try {
        // CRITICAL phân quyền: LUÔN refetch FE state sau khi switch_db_to_user
        // xong (bất kể ownerChanged), vì DbState connection đã trỏ sang DB
        // mới của user hiện tại. AccountContext + useDbStats gọi list/query
        // trước khi switch xong sẽ đọc từ DB cũ (race condition) — refresh ở
        // đây overwrite state với data từ DB đúng.
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

  // Mutation → hybrid trigger (count / max-wait / debounce). Gom nhiều
  // mutations vào 1 bundle, tiết kiệm R2 Class A ops + fix bug user edit
  // liên tục debounce reset vĩnh viễn không push.
  //
  // pausedByForm: user đang mở form → không flush (tránh apply event đè
  // state form đang edit). Counter vẫn đếm, trigger khi resume.
  useEffect(() => {
    if (!enabled) return;
    if (
      status === "checking" ||
      status === "syncing" ||
      status === "offline" ||
      status === "bootstrap"
    ) {
      return;
    }
    if (mutationVersion === 0) return;

    setStatus("dirty");
    const now = Date.now();
    lastMutationAtRef.current = now;
    pendingCountRef.current += 1;
    if (firstMutationAtRef.current === 0) {
      firstMutationAtRef.current = now;
    }

    if (pausedByForm) {
      return () => {
        clearDebounce();
      };
    }

    // Trigger 1: count đủ 100 mutations → flush ngay (cost-efficient bundle).
    if (pendingCountRef.current >= COUNT_THRESHOLD) {
      flushNow();
      return;
    }

    // Trigger 2: mutation đầu tiên đã quá 5 phút → flush (cap data-loss window).
    if (now - firstMutationAtRef.current >= MAX_WAIT_MS) {
      flushNow();
      return;
    }

    // Trigger 3 (default): reset 45s debounce — user ngừng → flush.
    scheduleDebounce();

    return () => {
      clearDebounce();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mutationVersion, enabled, pausedByForm, scheduleDebounce, flushNow]);

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
      if (pausedByForm) return;
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
        const s = await syncV9GetState();
        if (s.pendingPushTables.length > 0) {
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
  }, [enabled, doSync]);

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

  // Auto-sync periodic tick — nhận remote changes passively mỗi
  // AUTO_SYNC_INTERVAL_MS khi status=idle (không dirty/syncing/offline/
  // bootstrap/checking/error). Plan Q1 lock ≥5 phút để không drain cost.
  //
  // Dirty không chạy tick vì debounce + idle flush đã handle. Syncing bail
  // ra vì reentry guard. Error đang có retry backoff riêng. pausedByForm →
  // skip tick (tránh pull apply ghi đè form đang edit).
  useEffect(() => {
    if (!enabled) return;
    autoSyncTickRef.current = window.setInterval(() => {
      if (pausedByForm) return;
      if (statusRef.current !== "idle") return;
      if (!navigator.onLine) return;
      void doSync();
    }, AUTO_SYNC_INTERVAL_MS);
    return () => {
      if (autoSyncTickRef.current !== null) {
        clearInterval(autoSyncTickRef.current);
        autoSyncTickRef.current = null;
      }
    };
  }, [enabled, pausedByForm, doSync]);

  // Transition pausedByForm true → false: form vừa đóng. Trigger sync
  // catch-up nếu dirty (mutations trong lúc edit + remote có thể có update).
  const prevPausedRef = useRef(pausedByForm);
  useEffect(() => {
    if (!enabled) return;
    const wasPaused = prevPausedRef.current;
    prevPausedRef.current = pausedByForm;
    if (wasPaused && !pausedByForm) {
      // Reset idle activity marker — tránh idle-flush effect (interval 5s)
      // fire ngay lập tức sau form đóng nếu user đã idle > 30s trong lúc edit
      // form. Form resume = activity signal, đếm idle lại từ đây.
      lastActivityRef.current = Date.now();
      // Resume — flush pending + pull remote changes.
      if (statusRef.current === "dirty" || statusRef.current === "idle") {
        void doSync();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pausedByForm, enabled]);

  const forceSync = useCallback(async (): Promise<void> => {
    // Throttle 2s — user spam click nút "Đồng bộ ngay" chỉ fire 1 lần.
    const now = Date.now();
    if (now - lastForceAtRef.current < FORCE_SYNC_MIN_GAP_MS) return;
    lastForceAtRef.current = now;
    clearDebounce();
    clearRetry();
    retryAttemptRef.current = 0;
    // Ép pull: mutation path (scheduleDebounce/flushNow) có thể đã set
    // needPullRef=false và doSync đang inflight. Set true trước khi gọi
    // để inflight doSync thấy flag đúng ở check line 251, hoặc next call
    // (nếu re-entry guard bail) vẫn pull. Intent "Đồng bộ ngay" = fetch remote.
    needPullRef.current = true;
    await doSync();
  }, [doSync]);

  // Fetch machine fingerprint 1 lần — dùng cho RTDB push notification.
  // setFingerprintReady toggle để trigger subscribe effect tái chạy.
  const [fingerprintReady, setFingerprintReady] = useState(false);
  useEffect(() => {
    let cancelled = false;
    invoke<string>("machine_fingerprint")
      .then((fp) => {
        if (cancelled) return;
        myFingerprintRef.current = fp;
        setFingerprintReady(true);
      })
      .catch((e) => {
        console.warn("[sync] machine_fingerprint failed:", e);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Subscribe Firebase RTDB push events — máy khác push → ta pull ngay.
  // Event-driven thay thế cho polling 5 phút → zero R2 cost khi idle.
  // Initial snapshot KHÔNG trigger sync (đã có startup sync handle).
  useEffect(() => {
    if (!enabled) return;
    if (!authUid) return;
    const fp = myFingerprintRef.current;
    if (!fp) return; // fingerprintReady sẽ re-trigger effect khi sẵn sàng
    const unsub = subscribeRemotePushes(
      authUid,
      fp,
      ({ fingerprint, initial }) => {
        if (initial) {
          console.log("[sync] RTDB baseline:", fingerprint);
          return;
        }
        console.log("[sync] RTDB remote push detected:", fingerprint);
        // Mark flag để UI enable nút sync. Nếu app đang idle + online, tự
        // chạy doSync luôn (pull về ngay). pausedByForm → giữ flag đợi
        // resume. Syncing → doSync re-entry guard tự skip.
        setHasRemoteChangePending(true);
        if (statusRef.current === "offline") return;
        if (pausedByFormRef.current) return;
        void doSync();
      },
    );
    return unsub;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, authUid, fingerprintReady]);

  return {
    status,
    isStartupPhase,
    syncPhase,
    lastSyncAt,
    lastSyncStats,
    hasRemoteChangePending,
    error,
    forceSync,
  };
}
