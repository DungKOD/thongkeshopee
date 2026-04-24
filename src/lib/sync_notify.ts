import {
  off,
  onValue,
  ref,
  set,
  type Unsubscribe,
} from "firebase/database";
import { rtdb } from "./firebase";

/// Sync push notification qua Firebase RTDB.
///
/// **Problem:** auto-tick 5 phút polling manifest lãng phí khi không có
/// thay đổi remote. User chạy app 24h tốn hàng trăm Class B ops không cần.
///
/// **Solution:** event-driven. Mỗi lần máy push thành công (> 0 upload),
/// ghi timestamp vào RTDB path `sync_events/{uid}/{fingerprint}`. Mọi máy
/// khác của cùng UID subscribe → khi thấy entry với fingerprint khác thay
/// đổi ts → trigger pull ngay lập tức.
///
/// **Ưu điểm vs polling:**
/// - Zero R2 cost khi idle (RTDB subscribe miễn phí tier Spark)
/// - Near-realtime: máy B push → máy A nhận trong <1s (vs 5 phút polling)
/// - Scales: 100 user × nhiều device vẫn ít bandwidth
///
/// **RTDB rules (cần setup):**
/// ```json
/// "sync_events": {
///   "$uid": {
///     ".read": "auth.uid === $uid",
///     ".write": "auth.uid === $uid"
///   }
/// }
/// ```

const PATH_ROOT = "sync_events";

export interface SyncPushEvent {
  /// Client wall-clock timestamp (ms). Dùng so sánh monotonic vì RTDB
  /// serverTimestamp phức tạp dùng ở client subscriber (placeholder obj).
  ts: number;
  /// Bytes upload trong push cycle đó. Informational — để UI/debug.
  uploadBytes: number;
  /// Device fingerprint — match với key path, redundant nhưng defensive.
  fingerprint: string;
}

/// Viết event "I just pushed" lên RTDB. Caller gọi sau mỗi sync thành công
/// có `push.uploadedCount > 0`. Tolerant với lỗi network — không throw.
export async function announceMyPush(
  uid: string,
  fingerprint: string,
  uploadBytes: number,
): Promise<void> {
  if (!rtdb || !uid || !fingerprint) return;
  try {
    await set(ref(rtdb, `${PATH_ROOT}/${uid}/${fingerprint}`), {
      ts: Date.now(),
      uploadBytes,
      fingerprint,
    } satisfies SyncPushEvent);
  } catch (e) {
    console.warn("[sync_notify] announceMyPush failed:", e);
  }
}

/// Subscribe push events của user UID. Callback fire mỗi khi có entry từ
/// fingerprint KHÁC self update ts.
///
/// `myFingerprint` dùng để ignore echo của chính mình.
///
/// Baseline behavior: initial snapshot fire 1 lần — callback vẫn được gọi
/// với flag `{ initial: true }` để caller có thể bỏ qua auto-sync khi load
/// lần đầu (đã có startup sync).
export function subscribeRemotePushes(
  uid: string,
  myFingerprint: string,
  onRemotePush: (info: {
    fingerprint: string;
    ts: number;
    initial: boolean;
  }) => void,
): Unsubscribe {
  if (!rtdb || !uid || !myFingerprint) {
    return () => {};
  }
  const r = ref(rtdb, `${PATH_ROOT}/${uid}`);
  const lastSeen: Record<string, number> = {};
  let isInitial = true;

  const handler = onValue(r, (snap) => {
    const all =
      (snap.val() as Record<string, SyncPushEvent> | null) ?? {};
    const isFirst = isInitial;
    isInitial = false;

    for (const [fp, ev] of Object.entries(all)) {
      if (fp === myFingerprint) {
        // Skip self echo, nhưng ghi lại ts để tránh fire lại khi subscribe
        // lại lần sau (reconnect).
        lastSeen[fp] = ev.ts;
        continue;
      }
      const prevTs = lastSeen[fp] ?? 0;
      if (ev.ts > prevTs) {
        lastSeen[fp] = ev.ts;
        onRemotePush({
          fingerprint: fp,
          ts: ev.ts,
          initial: isFirst,
        });
      }
    }
  });

  return () => {
    off(r, "value", handler);
  };
}
