import { invoke } from "./tauri";

export interface ShopeeAffIdResult {
  affId: number | null;
  finalUrl: string;
  /** "utm_source" | "mmp_pid" | "none" */
  source: string;
}

/**
 * Tra Affiliate Publisher ID từ Shopee link (short hoặc full).
 * Follow redirect → parse `utm_source=an_<id>` hoặc `mmp_pid=an_<id>`.
 * Throw nếu URL không phải Shopee hoặc network fail.
 */
export function findShopeeAffId(url: string): Promise<ShopeeAffIdResult> {
  return invoke<ShopeeAffIdResult>("find_shopee_aff_id", { url });
}

export interface CleanLinkReplacement {
  original: string;
  cleaned: string;
}

export interface CleanLinkError {
  original: string;
  error: string;
}

export interface CleanLinksResult {
  outputText: string;
  replacements: CleanLinkReplacement[];
  errors: CleanLinkError[];
  totalFound: number;
}

/**
 * Tìm tất cả link Shopee trong text → follow redirect → strip tracking
 * params → replace inline. Trả output text mới + danh sách replacements.
 */
export function cleanShopeeLinks(inputText: string): Promise<CleanLinksResult> {
  return invoke<CleanLinksResult>("clean_shopee_links", { inputText });
}

export interface AffPasteParams {
  cookies: string;
  proxy: string;
  links: string[];
  subIds: string[];
  /** Header `af-ac-enc-sz-token` từ DevTools — bypass anti-fraud Shopee. */
  antiFraudToken: string;
}

export interface AffLinkResult {
  originalLink: string;
  shortLink: string | null;
  longLink: string | null;
  failCode: number | null;
  error: string | null;
}

/**
 * Convert link affiliate Shopee bằng cookies + proxy do user dán.
 * Không cần webview login, không cần GemLogin/AdsPower.
 */
export function convertAffWithPaste(
  params: AffPasteParams,
): Promise<AffLinkResult[]> {
  return invoke<AffLinkResult[]>("shopee_aff_convert_with_paste", {
    params: {
      cookies: params.cookies,
      proxy: params.proxy,
      links: params.links,
      sub_ids: params.subIds,
      anti_fraud_token: params.antiFraudToken,
    },
  });
}
