import type { Unsubscribe } from "firebase/database";
import { invoke } from "./tauri";
import {
  DEFAULT_DEVICE_LIMIT,
  getMyDeviceLimit,
  getMyDevices,
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
/// - User thường: pre-check FE-side (đọc devices + limit hiện tại). Nếu
///   máy này đã có entry → chỉ update lastSeen. Nếu mới + count < limit →
///   register. Nếu mới + count >= limit → reject với message liệt kê các
///   thiết bị hiện có (giúp user/admin biết cái nào cần xóa).
///
/// FE-side check ưu tiên hơn rules:
/// - Diagnostic chính xác: hiển thị limit + count + danh sách devices.
/// - Tránh được trường hợp rules production stale (chưa deploy `firebase
///   deploy --only database`) → user thấy fail confusing.
/// - Rules vẫn enforce làm backstop bảo mật.
///
/// Note race window vài ms khi 2 device login đồng thời — chấp nhận với
/// MVP. Admin có thể xóa entry thừa thủ công.
export async function enforceDeviceLimit(
  uid: string,
  isAdmin: boolean,
): Promise<EnforceResult> {
  try {
    const info = await getDeviceInfo();

    // Admin: bypass limit check, vẫn upsert để hiển thị trong admin UI.
    if (isAdmin) {
      await upsertMyDevice(uid, info);
      return { ok: true, deviceInfo: info };
    }

    // FE-side pre-check: đọc state hiện tại trước khi quyết định write.
    const [devices, overrideLimit] = await Promise.all([
      getMyDevices(uid),
      getMyDeviceLimit(uid),
    ]);
    const limit = overrideLimit ?? DEFAULT_DEVICE_LIMIT;
    const fingerprints = Object.keys(devices);
    const alreadyRegistered = fingerprints.includes(info.fingerprint);

    if (!alreadyRegistered && fingerprints.length >= limit) {
      // Liệt kê hostname + os để user biết devices nào đang chiếm slot.
      // Sort theo lastSeen desc để cái cũ nhất hiển thị cuối (gợi ý xóa).
      const list = Object.values(devices)
        .sort((a, b) => (b.lastSeen ?? 0) - (a.lastSeen ?? 0))
        .map((d) => `• ${d.hostname} (${d.os})`)
        .join("\n");
      return {
        ok: false,
        reason: "limit_exceeded",
        message:
          `Tài khoản đã đạt giới hạn ${limit} thiết bị (đang dùng ${fingerprints.length}).\n\n` +
          `Thiết bị đang đăng ký:\n${list}\n\n` +
          `Liên hệ admin (vnz.luffy@gmail.com) để xóa thiết bị cũ hoặc nâng limit.`,
      };
    }

    // OK to register/update. upsertMyDevice phân biệt set vs update tự.
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
    // Backstop: rules reject (vd race window 2 device login đồng thời, hoặc
    // FE pre-check skip do read fail). Message generic không có detailed list.
    const lower = msg.toLowerCase();
    if (
      lower.includes("permission_denied") ||
      lower.includes("permission denied")
    ) {
      if (isAdmin) {
        console.error(
          "[deviceGate] admin gặp PERMISSION_DENIED khi upsert device:",
          msg,
        );
        return {
          ok: true,
          deviceInfo: cachedDeviceInfo ?? (await getDeviceInfo()),
        };
      }
      return {
        ok: false,
        reason: "limit_exceeded",
        message:
          `Đã đạt giới hạn thiết bị (RTDB rules reject). Có thể server rules ` +
          `chưa được deploy phiên bản mới. Liên hệ admin để kiểm tra và xóa ` +
          `thiết bị cũ hoặc deploy lại rules (firebase deploy --only database).`,
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
