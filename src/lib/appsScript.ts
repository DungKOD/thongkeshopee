import { getAuthToken } from "./firebase";

/**
 * Endpoint Web App của Apps Script (server/apps-script/Code.gs).
 * Inject qua VITE_APPS_SCRIPT_URL — workflow build.yml gắn vào .env.local.
 */
const APPS_SCRIPT_URL = import.meta.env.VITE_APPS_SCRIPT_URL as
  | string
  | undefined;

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
 * idToken được attach tự động để server verify qua Identity Toolkit.
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
    throw new Error("VITE_APPS_SCRIPT_URL chưa được cấu hình");
  }
  const idToken = await getAuthToken();
  const res = await fetch(APPS_SCRIPT_URL, {
    method: "POST",
    headers: { "Content-Type": "text/plain;charset=utf-8" },
    body: JSON.stringify({ action, idToken, ...payload }),
  });
  if (!res.ok) {
    throw new Error(`Apps Script HTTP ${res.status}`);
  }
  const data = (await res.json()) as AppsScriptResponse;
  if (!data.ok) {
    throw new Error(
      `Apps Script ${data.code ?? ""}: ${data.error ?? "unknown error"}`,
    );
  }
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
