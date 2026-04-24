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

/// Per-device entry trong RTDB path `/status/{uid}/{fingerprint}`.
/// Một user có thể login nhiều máy → mỗi máy 1 entry độc lập.
export interface DevicePresence {
  state: "online" | "offline";
  /// Server timestamp (ms since epoch) — fire mỗi khi state đổi.
  lastChangedAt: number;
  /// Server timestamp của heartbeat gần nhất — update mỗi HEARTBEAT_INTERVAL_MS.
  lastHeartbeatAt: number;
}

/// Aggregated cho 1 user — gom tất cả devices.
export interface UserPresence {
  devices: Record<string, DevicePresence>;
}

export type PresenceMap = Record<string, UserPresence>;

const STATUS_ROOT = "/status";
const SERVER_TIME_OFFSET_PATH = "/.info/serverTimeOffset";

/// Heartbeat interval — write 1 lần / 60s. Thấp hơn → bandwidth tăng;
/// cao hơn → offline detection chậm. Free-tier Firebase RTDB ~50 user
/// active 24/7 với 60s interval (180KB/giờ writes).
const HEARTBEAT_INTERVAL_MS = 60_000;

/// Threshold xác định online: heartbeat cũ hơn 180s → offline dù DB còn
/// "online". 3× heartbeat = chịu được 2 lần miss network hiccup mà
/// không false-offline.
export const ONLINE_STALE_THRESHOLD_MS = 180_000;

// =============================================================
// Server time offset — fix client clock drift
// =============================================================

/// Module-level cache offset = serverTime - clientTime (ms). Firebase
/// RTDB cập nhật realtime qua `/.info/serverTimeOffset`. Subscribe 1
/// lần module load — không cần per-component.
let serverTimeOffsetMs = 0;
let offsetSubscribed = false;

function ensureOffsetSubscribed(): void {
  if (offsetSubscribed || !rtdb) return;
  offsetSubscribed = true;
  onValue(ref(rtdb, SERVER_TIME_OFFSET_PATH), (snap) => {
    const v = snap.val();
    if (typeof v === "number") {
      serverTimeOffsetMs = v;
    }
  });
}

/// Wall-clock theo server timestamp. Dùng so sánh với
/// `lastHeartbeatAt` (write qua `serverTimestamp()`).
/// Tránh false-offline khi client clock drift > 180s.
export function serverNow(): number {
  return Date.now() + serverTimeOffsetMs;
}

// =============================================================
// Per-device aggregator helpers (pure)
// =============================================================

/// 1 device có online không (dựa heartbeat age).
function isDeviceOnline(d: DevicePresence): boolean {
  if (d.state !== "online") return false;
  const hb = d.lastHeartbeatAt;
  if (typeof hb !== "number") return false;
  const age = serverNow() - hb;
  return age >= 0 && age < ONLINE_STALE_THRESHOLD_MS;
}

/// User có ÍT NHẤT 1 device online → user online.
export function isUserOnline(up: UserPresence | undefined | null): boolean {
  if (!up) return false;
  return Object.values(up.devices).some(isDeviceOnline);
}

/// Lần "thấy" cuối cùng = max(lastHeartbeatAt, lastChangedAt) qua mọi
/// device. Trả null nếu không có device nào có timestamp.
export function userLastSeenAt(
  up: UserPresence | undefined | null,
): number | null {
  if (!up) return null;
  let max = 0;
  for (const d of Object.values(up.devices)) {
    const ts = Math.max(d.lastHeartbeatAt ?? 0, d.lastChangedAt ?? 0);
    if (ts > max) max = ts;
  }
  return max > 0 ? max : null;
}

/// Số device đang online — UI dùng "online (2 thiết bị)" khi user nhiều máy.
export function onlineDeviceCount(
  up: UserPresence | undefined | null,
): number {
  if (!up) return 0;
  return Object.values(up.devices).filter(isDeviceOnline).length;
}

// =============================================================
// Track presence cho user hiện tại (per-device)
// =============================================================

/// Track presence cho user `uid` trên device `fingerprint`. Mỗi device
/// 1 entry độc lập trong RTDB → user 2 máy không đè lên nhau.
///
/// Flow:
/// 1. Subscribe `.info/connected`
/// 2. Connect → prime `onDisconnect` set offline → set online + start heartbeat
/// 3. Heartbeat mỗi 60s update `lastHeartbeatAt = serverTimestamp()`
/// 4. Wake from sleep / tab focus / network resume → fire heartbeat ngay
///    (không đợi 60s tick) → giảm window false-offline sau wake
/// 5. Disconnect → server-side onDisconnect fire → device entry offline
///
/// Trả unsub function — caller cleanup khi user logout / unmount.
export function trackMyPresence(
  uid: string,
  fingerprint: string,
): Unsubscribe {
  if (!rtdb) {
    console.warn("[presence] VITE_FIREBASE_DATABASE_URL chưa config — skip");
    return () => {};
  }
  if (!uid || !fingerprint) {
    console.warn("[presence] uid hoặc fingerprint trống — skip");
    return () => {};
  }
  ensureOffsetSubscribed();

  const statusRef = ref(rtdb, `${STATUS_ROOT}/${uid}/${fingerprint}`);
  const connectedRef = ref(rtdb, ".info/connected");

  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  let isConnected = false;

  const writeHeartbeat = () => {
    if (!rtdb || !isConnected) return;
    void update(statusRef, {
      lastHeartbeatAt: serverTimestamp(),
    }).catch((e) => console.warn("[presence] heartbeat failed:", e));
  };

  const stopHeartbeat = () => {
    if (heartbeatTimer !== null) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  };

  const startHeartbeat = () => {
    stopHeartbeat();
    heartbeatTimer = setInterval(writeHeartbeat, HEARTBEAT_INTERVAL_MS);
  };

  // Wake from sleep / tab focus / network resume → fire heartbeat ngay
  // để giảm window false-offline (60s gap đợi setInterval tick).
  const onWake = () => {
    writeHeartbeat();
  };
  const onVisibility = () => {
    if (document.visibilityState === "visible") writeHeartbeat();
  };
  window.addEventListener("focus", onWake);
  window.addEventListener("online", onWake);
  document.addEventListener("visibilitychange", onVisibility);

  const unsubConn = onValue(connectedRef, (snap) => {
    if (snap.val() === false) {
      // Mất kết nối → dừng heartbeat. Server-side onDisconnect (đã prime)
      // fire độc lập khi server detect WebSocket close.
      isConnected = false;
      stopHeartbeat();
      return;
    }
    isConnected = true;
    // Prime onDisconnect TRƯỚC khi set online → cover race mất mạng đột ngột.
    // Set offline CHỈ device entry này — không đụng device khác cùng uid.
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
    window.removeEventListener("focus", onWake);
    window.removeEventListener("online", onWake);
    document.removeEventListener("visibilitychange", onVisibility);
  };
}

// =============================================================
// Subscribe all (admin view)
// =============================================================

/// Subscribe toàn bộ status. Callback nhận `PresenceMap` aggregated theo
/// uid (mỗi user → list devices).
///
/// **Backward compat — mixed shape parser**: RTDB sau khi migrate có thể
/// chứa CẢ old flat fields VÀ new nested device entries cùng dưới
/// `/status/{uid}/`:
/// ```
/// /status/{uid}/ = {
///   state: "online", lastChangedAt: T1, lastHeartbeatAt: T1,  ← OLD flat
///   fingerprint_abc: {state:"online", ...}                     ← NEW nested
/// }
/// ```
/// Parser tách:
/// - Flat fields (state/lastChangedAt/lastHeartbeatAt ở top-level) → wrap
///   thành device "__legacy__"
/// - Nested objects (key khác 3 reserved keys + value object có `state`)
///   → device entries thật
///
/// Aggregate qua TẤT CẢ → user online nếu BẤT KỲ entry online (legacy
/// hoặc fingerprint mới).
export function subscribeToAllPresence(
  onUpdate: (statuses: PresenceMap) => void,
): Unsubscribe {
  if (!rtdb) {
    onUpdate({});
    return () => {};
  }
  ensureOffsetSubscribed();
  const allRef = ref(rtdb, STATUS_ROOT);
  return onValue(allRef, (snap) => {
    const raw = (snap.val() as Record<string, unknown> | null) ?? {};
    const map: PresenceMap = {};
    for (const [uid, val] of Object.entries(raw)) {
      if (val === null || typeof val !== "object") continue;
      const obj = val as Record<string, unknown>;
      const devices: Record<string, DevicePresence> = {};

      // 1. Detect flat legacy fields ở top-level.
      if (
        typeof obj.state === "string" &&
        (typeof obj.lastHeartbeatAt === "number" ||
          typeof obj.lastChangedAt === "number")
      ) {
        devices.__legacy__ = {
          state: obj.state as "online" | "offline",
          lastChangedAt: (obj.lastChangedAt as number) ?? 0,
          lastHeartbeatAt: (obj.lastHeartbeatAt as number) ?? 0,
        };
      }

      // 2. Detect nested device entries — key khác 3 flat fields + value
      // có shape DevicePresence.
      for (const [key, child] of Object.entries(obj)) {
        if (
          key === "state" ||
          key === "lastChangedAt" ||
          key === "lastHeartbeatAt"
        ) {
          continue;
        }
        if (
          child !== null &&
          typeof child === "object" &&
          typeof (child as Record<string, unknown>).state === "string"
        ) {
          const c = child as Record<string, unknown>;
          devices[key] = {
            state: c.state as "online" | "offline",
            lastChangedAt: (c.lastChangedAt as number) ?? 0,
            lastHeartbeatAt: (c.lastHeartbeatAt as number) ?? 0,
          };
        }
      }

      if (Object.keys(devices).length > 0) {
        map[uid] = { devices };
      }
    }
    onUpdate(map);
  });
}
