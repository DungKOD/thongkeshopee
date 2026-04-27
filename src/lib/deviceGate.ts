import type { Unsubscribe } from "firebase/database";
import { invoke } from "./tauri";
import {
  DEFAULT_DEVICE_LIMIT,
  subscribeMyDevice,
  upsertMyDevice,
  type DeviceInfo,
} from "./userDevices";

/// Kết quả enforce — `ok:false` chỉ khi user thường vượt limit hoặc lỗi
/// system. Admin luôn `ok:true`. RTDB unavailable cũng `ok:true` (fail-soft).
export type EnforceResult =
  | { ok: true; deviceInfo: DeviceInfo }
  | {
      ok: false;
      reason: "limit_exceeded" | "unknown";
      message: string;
    };

let cachedDeviceInfo: DeviceInfo | null = null;

/// Lấy device info từ Tauri command, cache module-level (fingerprint
/// không đổi trong lifetime của app process).
export async function getDeviceInfo(): Promise<DeviceInfo> {
  if (cachedDeviceInfo) return cachedDeviceInfo;
  const info = await invoke<DeviceInfo>("get_device_info");
  cachedDeviceInfo = info;
  return info;
}

/// Enforce device limit sau khi user vừa login.
///
/// - Admin (`isAdmin === true`): upsert entry để hiển thị trong admin UI,
///   KHÔNG check limit.
/// - User thường: cố write entry. Rules `database.rules.json` enforce
///   `numChildren < limit` → fail = PERMISSION_DENIED → return
///   `limit_exceeded`. Caller (AuthContext) sẽ signOut.
///
/// Note race window vài ms khi 2 device login đồng thời — chấp nhận với
/// MVP. Admin có thể xóa entry thừa thủ công.
export async function enforceDeviceLimit(
  uid: string,
  isAdmin: boolean,
): Promise<EnforceResult> {
  try {
    const info = await getDeviceInfo();
    await upsertMyDevice(uid, info);
    return { ok: true, deviceInfo: info };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.includes("RTDB chưa config")) {
      // Fail-soft: dev env (no RTDB url) hoặc RTDB infra outage — không block
      // login, log warning. Trade-off: user vượt limit ở dev, nhưng ko brick
      // app khi RTDB sập. Presence cũng silent-fail same way.
      console.warn("[deviceGate] RTDB unavailable → skip device check:", msg);
      return {
        ok: true,
        deviceInfo: cachedDeviceInfo ?? (await getDeviceInfo()),
      };
    }
    // Firebase RTDB throw object có code "PERMISSION_DENIED" hoặc message
    // chứa "permission_denied" / "Permission denied".
    const lower = msg.toLowerCase();
    if (
      lower.includes("permission_denied") ||
      lower.includes("permission denied")
    ) {
      if (isAdmin) {
        // Admin không nên gặp permission denied — log để debug nhưng cho qua.
        console.error(
          "[deviceGate] admin gặp PERMISSION_DENIED khi upsert device:",
          msg,
        );
        return { ok: true, deviceInfo: cachedDeviceInfo ?? (await getDeviceInfo()) };
      }
      return {
        ok: false,
        reason: "limit_exceeded",
        message: `Tài khoản đã đạt giới hạn ${DEFAULT_DEVICE_LIMIT} thiết bị. Liên hệ admin (vnz.luffy@gmail.com) để xóa thiết bị cũ.`,
      };
    }
    console.error("[deviceGate] enforce failed:", e);
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
