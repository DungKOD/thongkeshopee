import {
  get,
  onValue,
  ref,
  remove,
  serverTimestamp,
  set,
  update,
  type Unsubscribe,
} from "firebase/database";
import { rtdb } from "./firebase";

/// Entry trong RTDB path `/user_devices/{uid}/{fingerprint}`. Persistent —
/// tồn tại đến khi admin xóa (không phụ thuộc presence/online state).
export interface DeviceEntry {
  hostname: string;
  os: string;
  /// Server timestamp (ms) lần đầu register device này.
  createdAt: number;
  /// Server timestamp (ms) lần login / heartbeat gần nhất.
  lastSeen: number;
}

/// Thông tin device từ Tauri command `get_device_info`.
export interface DeviceInfo {
  fingerprint: string;
  hostname: string;
  os: string;
}

/// Map { uid -> { fingerprint -> DeviceEntry } } — admin subscribe toàn bộ.
export type AllDevicesMap = Record<string, Record<string, DeviceEntry>>;

const ROOT_DEVICES = "user_devices";

function requireRtdb(): NonNullable<typeof rtdb> {
  if (!rtdb) throw new Error("RTDB chưa config (VITE_FIREBASE_DATABASE_URL)");
  return rtdb;
}

/// Upsert device entry cho user hiện tại. Lần đầu → create với
/// `createdAt = serverTimestamp()`. Lần sau → chỉ update `lastSeen`.
///
/// Không còn limit enforcement (v0.4.8+) — user login bao nhiêu máy cũng OK.
/// Tracking giữ lại để admin xem ai đang dùng máy nào + revoke từng entry.
export async function upsertMyDevice(
  uid: string,
  device: DeviceInfo,
): Promise<void> {
  const db = requireRtdb();
  const path = `${ROOT_DEVICES}/${uid}/${device.fingerprint}`;
  const snap = await get(ref(db, path));
  if (snap.exists()) {
    await update(ref(db, path), {
      hostname: device.hostname,
      os: device.os,
      lastSeen: serverTimestamp(),
    });
  } else {
    await set(ref(db, path), {
      hostname: device.hostname,
      os: device.os,
      createdAt: serverTimestamp(),
      lastSeen: serverTimestamp(),
    });
  }
}

/// Subscribe entry device hiện tại → fire khi admin xóa (snap === null).
/// Caller dùng để force signOut khi bị revoke.
export function subscribeMyDevice(
  uid: string,
  fingerprint: string,
  cb: (entry: DeviceEntry | null) => void,
): Unsubscribe {
  if (!rtdb) return () => {};
  return onValue(ref(rtdb, `${ROOT_DEVICES}/${uid}/${fingerprint}`), (snap) => {
    cb((snap.val() as DeviceEntry | null) ?? null);
  });
}

/// Admin subscribe toàn bộ devices (cross-user) cho UI quản lý.
export function subscribeAllDevices(
  cb: (map: AllDevicesMap) => void,
): Unsubscribe {
  if (!rtdb) {
    cb({});
    return () => {};
  }
  return onValue(ref(rtdb, ROOT_DEVICES), (snap) => {
    cb((snap.val() as AllDevicesMap | null) ?? {});
  });
}

/// Admin xóa 1 device entry → user trên máy đó sẽ bị signOut khi
/// `subscribeMyDevice` fire null.
export async function removeDevice(
  uid: string,
  fingerprint: string,
): Promise<void> {
  const db = requireRtdb();
  await remove(ref(db, `${ROOT_DEVICES}/${uid}/${fingerprint}`));
}
