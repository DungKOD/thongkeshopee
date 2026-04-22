import {
  onDisconnect,
  onValue,
  ref,
  serverTimestamp,
  set,
  update,
  type Unsubscribe,
} from "firebase/database";
import { rtdb } from "./firebase";

/// Shape của 1 entry status trong RTDB path `/status/{uid}`.
export interface Presence {
  state: "online" | "offline";
  /// Server timestamp (ms since epoch) — fire mỗi khi state đổi.
  lastChangedAt: number;
  /// Server timestamp của heartbeat gần nhất — update mỗi HEARTBEAT_INTERVAL_MS.
  /// UI dùng field này để tính online "thực tế" (state có thể stale nếu
  /// onDisconnect không fire — xem isOnline).
  lastHeartbeatAt: number;
}

export type PresenceMap = Record<string, Presence>;

const STATUS_ROOT = "/status";

/// Client ghi heartbeat mỗi 60s. Đảm bảo server-side có bằng chứng "còn sống"
/// mới nhất — không phụ thuộc onDisconnect (không reliable trên Tauri khi
/// WebSocket close không trigger server event đúng lúc).
///
/// Trade-off interval:
///  - 30s: offline detection 90s; bandwidth gấp đôi.
///  - 60s (hiện tại): offline detection 180s; bandwidth giảm 50% → free-tier
///    chịu được ~2,000 users active 24/7 (thay vì 1,000).
const HEARTBEAT_INTERVAL_MS = 60_000;

/// Threshold xác định online: nếu heartbeat cũ hơn 180s → coi là offline dù
/// state trong DB còn là "online". 180s = 3x heartbeat interval → cho phép
/// miss 2 heartbeat (network hiccup) mà không false-positive offline.
export const ONLINE_STALE_THRESHOLD_MS = 180_000;

/// Derive online từ presence record. Ground truth là heartbeat age, KHÔNG phải
/// field `state` (vì onDisconnect có thể không fire → state stuck ở "online").
export function isOnline(p: Presence | undefined | null): boolean {
  if (!p) return false;
  if (p.state !== "online") return false;
  const hb = p.lastHeartbeatAt;
  if (typeof hb !== "number") return false;
  const age = Date.now() - hb;
  return age >= 0 && age < ONLINE_STALE_THRESHOLD_MS;
}

/// Last-seen timestamp cho UI hiển thị "X phút trước". Ưu tiên heartbeat
/// (mới hơn); fallback state-change time nếu chưa có heartbeat (entry cũ).
export function lastSeenAt(p: Presence | undefined | null): number | null {
  if (!p) return null;
  return p.lastHeartbeatAt ?? p.lastChangedAt ?? null;
}

/// Track presence cho user hiện tại (uid). Primary: heartbeat write mỗi 30s.
/// Secondary: onDisconnect cho trường hợp WebSocket close cleanly.
///
/// Stale-online issue (trên Tauri, onDisconnect có thể không fire khi đóng
/// app bằng nút X vì render process kill trước khi WebSocket close event
/// propagate lên server) → giải quyết bằng heartbeat: server có timestamp
/// mới nhất; UI check `Date.now() - lastHeartbeatAt` để biết user còn sống.
export function trackMyPresence(uid: string): Unsubscribe {
  if (!rtdb) {
    console.warn("[presence] VITE_FIREBASE_DATABASE_URL chưa config — skip");
    return () => {};
  }
  const statusRef = ref(rtdb, `${STATUS_ROOT}/${uid}`);
  const connectedRef = ref(rtdb, ".info/connected");

  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  const stopHeartbeat = () => {
    if (heartbeatTimer !== null) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  };

  const startHeartbeat = () => {
    stopHeartbeat();
    heartbeatTimer = setInterval(() => {
      if (!rtdb) return;
      void update(statusRef, {
        lastHeartbeatAt: serverTimestamp(),
      }).catch((e) => console.warn("[presence] heartbeat failed:", e));
    }, HEARTBEAT_INTERVAL_MS);
  };

  const unsubConn = onValue(connectedRef, (snap) => {
    if (snap.val() === false) {
      // Mất kết nối → dừng heartbeat. Server-side onDisconnect (đã prime)
      // sẽ fire độc lập khi detect WebSocket close.
      stopHeartbeat();
      return;
    }
    // Prime onDisconnect TRƯỚC khi set online → cover race mất mạng đột ngột.
    onDisconnect(statusRef)
      .set({
        state: "offline",
        lastChangedAt: serverTimestamp(),
        lastHeartbeatAt: serverTimestamp(),
      })
      .then(() => {
        void set(statusRef, {
          state: "online",
          lastChangedAt: serverTimestamp(),
          lastHeartbeatAt: serverTimestamp(),
        });
        startHeartbeat();
      })
      .catch((e) => console.error("[presence] onDisconnect prime failed:", e));
  });

  return () => {
    stopHeartbeat();
    unsubConn();
  };
}

/// Subscribe vào toàn bộ status (admin UI). Callback nhận map `{uid → Presence}`.
export function subscribeToAllPresence(
  onUpdate: (statuses: PresenceMap) => void,
): Unsubscribe {
  if (!rtdb) {
    onUpdate({});
    return () => {};
  }
  const allRef = ref(rtdb, STATUS_ROOT);
  return onValue(allRef, (snap) => {
    onUpdate((snap.val() as PresenceMap | null) ?? {});
  });
}
