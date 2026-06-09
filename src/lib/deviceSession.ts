import {
  doc,
  getDoc,
  onSnapshot,
  serverTimestamp,
  setDoc,
  type Unsubscribe,
} from "firebase/firestore";
import { db } from "./firebase";
import { invoke } from "./tauri";

export interface DeviceInfo {
  deviceId: string;
  deviceName: string;
  platform: string;
  isFallback: boolean;
}

/**
 * Document shape lưu ở Firestore `/sessions/{uid}`.
 * Chỉ 1 doc per user — write-last-wins → máy mới luôn thắng (auto-kick).
 */
export interface SessionDoc {
  deviceId: string;
  deviceName: string;
  platform: string;
  loginAt: { seconds: number; nanoseconds: number } | null;
}

const SESSION_COLLECTION = "sessions";
const CACHE_KEY = "deviceSession:lastValidated";
const OFFLINE_GRACE_MS = 48 * 60 * 60 * 1000;

let deviceInfoCache: DeviceInfo | null = null;

/** Lấy device info từ Tauri (cache trong module — machine ID không đổi runtime). */
export async function getDeviceInfo(): Promise<DeviceInfo> {
  if (deviceInfoCache) return deviceInfoCache;
  const info = await invoke<DeviceInfo>("get_device_id");
  deviceInfoCache = info;
  return info;
}

interface CachedValidation {
  uid: string;
  deviceId: string;
  validatedAt: number;
}

function readCache(): CachedValidation | null {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    return JSON.parse(raw) as CachedValidation;
  } catch {
    return null;
  }
}

function writeCache(uid: string, deviceId: string): void {
  try {
    const payload: CachedValidation = {
      uid,
      deviceId,
      validatedAt: Date.now(),
    };
    localStorage.setItem(CACHE_KEY, JSON.stringify(payload));
  } catch {
    /* quota */
  }
}

function clearCache(): void {
  try {
    localStorage.removeItem(CACHE_KEY);
  } catch {
    /* ignore */
  }
}

/**
 * Ghi đè doc `/sessions/{uid}` với device hiện tại — auto-kick máy cũ.
 * Realtime listener của máy cũ sẽ nhận được snapshot mới và force signOut.
 */
export async function claimSession(uid: string): Promise<DeviceInfo> {
  const info = await getDeviceInfo();
  const ref = doc(db, SESSION_COLLECTION, uid);
  await setDoc(ref, {
    deviceId: info.deviceId,
    deviceName: info.deviceName,
    platform: info.platform,
    loginAt: serverTimestamp(),
  });
  writeCache(uid, info.deviceId);
  return info;
}

/**
 * Subscribe Firestore session doc. Gọi `onKicked` khi deviceId trên doc khác
 * deviceId máy hiện tại (= máy khác đã chiếm session).
 *
 * Async để await `getDeviceInfo()` trước khi subscribe — nếu không, snapshot
 * đầu tiên có thể fire trước khi biết `myDeviceId` và bỏ sót kick check.
 *
 * Trả về unsubscribe function — caller phải gọi khi signOut hoặc unmount.
 */
export async function watchSession(
  uid: string,
  onKicked: (kickedBy: { deviceName: string; platform: string }) => void,
): Promise<Unsubscribe> {
  const ref = doc(db, SESSION_COLLECTION, uid);
  const info = await getDeviceInfo();
  const myDeviceId = info.deviceId;

  return onSnapshot(
    ref,
    (snap) => {
      if (!snap.exists()) return;
      const data = snap.data() as SessionDoc;
      if (data.deviceId && data.deviceId !== myDeviceId) {
        onKicked({
          deviceName: data.deviceName ?? "máy khác",
          platform: data.platform ?? "",
        });
      } else if (data.deviceId === myDeviceId) {
        writeCache(uid, myDeviceId);
      }
    },
    (err) => {
      console.warn("[deviceSession] snapshot error:", err);
    },
  );
}

/**
 * Result của `verifySession` — bao gồm device info của máy chiếm khi kicked,
 * để dialog hiển thị tên máy thật thay vì "máy khác".
 */
export type VerifyResult =
  | { status: "ok" | "offline" }
  | { status: "kicked"; deviceName: string; platform: string };

/**
 * Verify session khi app start (offline grace). Trả về:
 * - `"ok"` nếu Firestore confirm máy hiện tại đang giữ session
 * - `"kicked"` nếu máy khác đã chiếm session (kèm deviceName/platform)
 * - `"offline"` nếu không reach được Firestore nhưng cache còn fresh (< 48h)
 *
 * `claimSession` nên được gọi NGAY SAU verify nếu kết quả là `"ok"` hoặc
 * `"offline"` (renew loginAt + lastValidated). Nếu `"kicked"` → force signOut.
 */
export async function verifySession(uid: string): Promise<VerifyResult> {
  const info = await getDeviceInfo();
  try {
    const ref = doc(db, SESSION_COLLECTION, uid);
    const snap = await getDoc(ref);
    if (!snap.exists()) return { status: "ok" };
    const data = snap.data() as SessionDoc;
    if (!data.deviceId || data.deviceId === info.deviceId) {
      return { status: "ok" };
    }
    return {
      status: "kicked",
      deviceName: data.deviceName ?? "máy khác",
      platform: data.platform ?? "",
    };
  } catch (err) {
    console.warn("[deviceSession] verify offline:", err);
    const cached = readCache();
    const fresh =
      cached !== null &&
      cached.uid === uid &&
      cached.deviceId === info.deviceId &&
      Date.now() - cached.validatedAt < OFFLINE_GRACE_MS;
    return fresh
      ? { status: "offline" }
      : { status: "kicked", deviceName: "máy khác", platform: "" };
  }
}

/** Đọc doc session hiện tại (để hiển thị trong Settings). */
export async function readSession(uid: string): Promise<SessionDoc | null> {
  try {
    const snap = await getDoc(doc(db, SESSION_COLLECTION, uid));
    if (!snap.exists()) return null;
    return snap.data() as SessionDoc;
  } catch (err) {
    console.warn("[deviceSession] readSession error:", err);
    return null;
  }
}

/** Clear cache local — gọi khi signOut chủ động. */
export function clearSessionCache(): void {
  clearCache();
}
