import type { Unsubscribe } from "firebase/database";
import { invoke } from "./tauri";
import { subscribeMyDevice, upsertMyDevice, type DeviceInfo } from "./userDevices";

/// Kết quả register device sau login.
export type RegisterResult =
  | { ok: true; deviceInfo: DeviceInfo }
  | { ok: false; reason: "unknown"; message: string };

let cachedDeviceInfo: DeviceInfo | null = null;

/// Lấy device info từ Tauri command, cache module-level (fingerprint
/// không đổi trong lifetime của app process).
export async function getDeviceInfo(): Promise<DeviceInfo> {
  if (cachedDeviceInfo) return cachedDeviceInfo;
  const info = await invoke<DeviceInfo>("get_device_info");
  cachedDeviceInfo = info;
  return info;
}

/// Register device entry sau khi user vừa login. Upsert vào RTDB
/// `/user_devices/{uid}/{fingerprint}` để admin UI thấy ai đang dùng máy
/// nào và để `subscribeDeviceRevocation` watch từng máy (admin xóa →
/// user trên máy đó signOut).
///
/// Không có limit cap số lượng thiết bị (đã bỏ ở v0.4.8+).
export async function registerMyDeviceLogin(
  uid: string,
): Promise<RegisterResult> {
  try {
    const info = await getDeviceInfo();
    await upsertMyDevice(uid, info);
    return { ok: true, deviceInfo: info };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.includes("RTDB chưa config")) {
      // Fail-soft: dev env (no RTDB url) hoặc RTDB infra outage — không block
      // login, log warning. Trade-off: admin UI không thấy device này, nhưng
      // ko brick app khi RTDB sập. Presence cũng silent-fail same way.
      console.warn("[deviceGate] RTDB unavailable → skip device register:", msg);
      return {
        ok: true,
        deviceInfo: cachedDeviceInfo ?? (await getDeviceInfo()),
      };
    }
    console.error("[deviceGate] register failed:", e);
    return { ok: false, reason: "unknown", message: msg };
  }
}

/// Subscribe entry hiện tại — entry chuyển null → onRevoked fire (one-shot).
/// Caller (AuthContext) set deviceRevoked=true để AuthGate hiện splash.
///
/// **Race coverage**: caller upsert entry TRƯỚC khi subscribe → first emit
/// thường là entry vừa write. Nếu admin xóa trong window vài ms giữa upsert
/// và subscribe → first emit = null → vẫn fire onRevoked (không skip first
/// emit). One-shot guard tránh fire nhiều lần nếu RTDB emit lặp.
export function subscribeDeviceRevocation(
  uid: string,
  fingerprint: string,
  onRevoked: () => void,
): Unsubscribe {
  let revoked = false;
  return subscribeMyDevice(uid, fingerprint, (entry) => {
    if (revoked) return;
    if (entry === null) {
      revoked = true;
      onRevoked();
    }
  });
}
