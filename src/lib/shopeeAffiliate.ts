import { invoke } from "./tauri";

export interface ShopeeAffStatus {
  hasCookies: boolean;
  capturedAt: string | null;
  cookieCount: number;
  loginWindowOpen: boolean;
}

export interface LinkResult {
  originalLink: string;
  shortLink: string | null;
  longLink: string | null;
  failCode: number | null;
  error: string | null;
}

export function shopeeAffOpenLoginWindow(): Promise<void> {
  return invoke("shopee_aff_open_login_window");
}

export function shopeeAffCloseLoginWindow(): Promise<void> {
  return invoke("shopee_aff_close_login_window");
}

export function shopeeAffCaptureCookies(): Promise<ShopeeAffStatus> {
  return invoke<ShopeeAffStatus>("shopee_aff_capture_cookies");
}

export function shopeeAffGetStatus(): Promise<ShopeeAffStatus> {
  return invoke<ShopeeAffStatus>("shopee_aff_get_status");
}

export function shopeeAffClearCookies(): Promise<void> {
  return invoke("shopee_aff_clear_cookies");
}

export function shopeeAffConvertLinks(
  links: string[],
  subIds: string[],
): Promise<LinkResult[]> {
  return invoke<LinkResult[]>("shopee_aff_convert_links", { links, subIds });
}
