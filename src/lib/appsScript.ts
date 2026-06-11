import { getAuthToken } from "./firebase";
import { invoke } from "./tauri";

interface ProxyResponse {
  status: number;
  body: string;
}

/**
 * Endpoint Web App của Apps Script (server/apps-script/Code.gs).
 * Inject qua VITE_APPS_SCRIPT_URL — workflow build.yml gắn vào .env.local.
 */
const APPS_SCRIPT_URL = import.meta.env.VITE_APPS_SCRIPT_URL as
  | string
  | undefined;

// Debug banner: log URL status 1 lần ngay khi module load. User mở F12
// Console là thấy ngay status — không cần đoán URL có vào không.
// eslint-disable-next-line no-console
console.log(
  "%c[appsScript] URL status:",
  "color:#888;font-weight:bold",
  APPS_SCRIPT_URL
    ? `LOADED (${APPS_SCRIPT_URL.slice(0, 60)}...)`
    : "MISSING — VITE_APPS_SCRIPT_URL chưa config trong .env.local",
);

export interface AppsScriptOk {
  ok: true;
  [key: string]: unknown;
}

export interface AppsScriptErr {
  ok: false;
  code?: number;
  error?: string;
}

export type AppsScriptResponse = AppsScriptOk | AppsScriptErr;

/**
 * Gọi action trên Apps Script Web App. Yêu cầu user đã sign-in Firebase —
 * idToken được attach trong body để server verify qua Identity Toolkit
 * (`accounts:lookup`), bất kể provider (Google/Email).
 *
 * Apps Script Web App PHẢI deploy với access = "Anyone" (không phải
 * "Anyone with Google account"). Auth app-level qua Firebase idToken trong
 * body là đủ — không cần thêm lớp Google OAuth ở HTTP level (Bearer token
 * Google chỉ sống ~1h và Firebase signInWithPopup không expose refresh token
 * → hết hạn không có cách refresh silent, user phải sign in lại).
 *
 * Content-Type cố tình dùng `text/plain` để request được coi là "simple"
 * (theo CORS spec), tránh preflight OPTIONS mà Apps Script không xử lý.
 * Apps Script doPost đọc raw body từ `e.postData.contents` → không quan
 * tâm content-type.
 *
 * Throws nếu URL chưa config, network fail, hoặc server trả ok=false.
 */
export async function callAppsScript<T extends AppsScriptOk = AppsScriptOk>(
  action: string,
  payload: Record<string, unknown> = {},
): Promise<T> {
  if (!APPS_SCRIPT_URL) {
    // eslint-disable-next-line no-console
    console.error("[appsScript] BLOCK — URL chưa config");
    throw new Error("VITE_APPS_SCRIPT_URL chưa được cấu hình");
  }
  const idToken = await getAuthToken();
  // eslint-disable-next-line no-console
  console.log(
    "%c[appsScript] →",
    "color:#0a8",
    action,
    idToken ? `(idToken ${idToken.length}b)` : "(NO idToken — chưa sign in?)",
  );
  if (!idToken) {
    throw new Error("Chưa sign in Firebase — không có idToken");
  }
  // Gọi qua Tauri Rust proxy để bypass browser CORS preflight. fetch từ
  // webview có thể gặp redirect tới `script.googleusercontent.com` mà CORS
  // không cho phép expose body. reqwest từ Rust không thuộc browser context
  // → no CORS, auto-follow redirect. Xem `src-tauri/src/commands/apps_script.rs`.
  let proxy: ProxyResponse;
  try {
    proxy = await invoke<ProxyResponse>("proxy_apps_script", {
      url: APPS_SCRIPT_URL,
      body: JSON.stringify({ action, idToken, ...payload }),
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error("[appsScript] PROXY FAIL", action, e);
    throw new Error(`Network fail: ${e instanceof Error ? e.message : String(e)}`);
  }
  if (proxy.status < 200 || proxy.status >= 300) {
    // eslint-disable-next-line no-console
    console.error("[appsScript] HTTP", proxy.status, action, proxy.body.slice(0, 500));
    const bodySnippet = proxy.body.slice(0, 200).replace(/\s+/g, " ").trim();
    if (proxy.status === 401) {
      // 401 ở HTTP layer = Apps Script Web App đang require auth Google
      // (access="Anyone with Google account") thay vì "Anyone". Đính kèm
      // snippet body để phân biệt với trường hợp khác (HTML login page vs
      // JSON error vs proxy fail).
      throw new Error(
        "Apps Script HTTP 401 — Web App vẫn đang require Google login. " +
          "Mở Manage deployments → Edit → Version='New version' → Who has access='Anyone' → Deploy. " +
          `Body: ${bodySnippet}`,
      );
    }
    throw new Error(`Apps Script HTTP ${proxy.status} — Body: ${bodySnippet}`);
  }
  let data: AppsScriptResponse;
  try {
    data = JSON.parse(proxy.body) as AppsScriptResponse;
  } catch {
    // eslint-disable-next-line no-console
    console.error("[appsScript] JSON parse fail", action, proxy.body.slice(0, 300));
    throw new Error(`Apps Script body parse fail: ${proxy.body.slice(0, 200)}`);
  }
  if (!data.ok) {
    // eslint-disable-next-line no-console
    console.error("[appsScript] ✗", action, data);
    throw new Error(
      `Apps Script ${data.code ?? ""}: ${data.error ?? "unknown error"}`,
    );
  }
  // eslint-disable-next-line no-console
  console.log("%c[appsScript] ✓", "color:#0a8", action, data);
  return data as T;
}

/**
 * Format Date sang `"HH:MM:SS DD/MM/YYYY"` — định dạng Apps Script
 * (`parseTs_` trong Code.gs) parse được. Dùng giờ máy local (Asia/HCM mặc
 * định ở VN). Nếu user ở timezone khác, log vẫn theo giờ máy họ — hợp lý
 * vì user thường xem theo giờ mình.
 */
export function formatVnTimestamp(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  const date = `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}`;
  const time = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  return `${time} ${date}`;
}
