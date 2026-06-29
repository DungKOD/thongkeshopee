import { invoke } from "./tauri";

/// View tổng hợp 1 FB User Token sau khi gộp `fb_auth_tokens` (Reels) +
/// `fb_ads_auth_tokens` (Ads) theo `token_hash`.
///
/// - `source = "reels"` → token chỉ lưu cho Reels (scope pages_*)
/// - `source = "ads"`   → token chỉ lưu cho Ads (scope ads_management)
/// - `source = "both"`  → cùng token dùng cho cả 2 feature
///
/// `reelsId` / `adsId` là PK ở 2 bảng tương ứng, dùng để delete đúng row.
export interface UnifiedFbToken {
  source: "reels" | "ads" | "both";
  reelsId: number | null;
  adsId: number | null;
  label: string;
  tokenHash: string;
  addedAtMs: number;
  expired: boolean;
}

export interface TokenSummary {
  fbUserTokens: number;
  fbPages: number;
  fbAdAccounts: number;
  fbPagesExpired: number;
}

/// List FB User Token unique cross-table — UI Token Manager hiển thị ở
/// 1 chỗ duy nhất thay vì 2 list rời (FB Pages dialog + FB Ad Accounts dialog).
export function tokenListFbUserTokens(): Promise<UnifiedFbToken[]> {
  return invoke<UnifiedFbToken[]>("token_list_fb_user_tokens");
}

/// Counts để hiển thị badge trên các tab Token Manager.
export function tokenSummary(): Promise<TokenSummary> {
  return invoke<TokenSummary>("token_summary");
}
