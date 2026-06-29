//! Token Manager — view tổng hợp tất cả token đang lưu trong app.
//!
//! Mục tiêu: gom 4 nguồn token rời rạc thành 1 list UI duy nhất:
//!   1. `fb_auth_tokens` (Reels) — User Token scope pages_*
//!   2. `fb_ads_auth_tokens` (Ads) — User Token scope ads_management
//!   3. `fb_pages` — Page Token vĩnh viễn (1 cho mỗi Page)
//!   4. `fb_ad_accounts` — Ad Account access_token (1 cho mỗi account)
//!   5. `app_settings.shopee_affiliate.cookies` — Shopee browser session
//!
//! Token Manager KHÔNG tạo bảng mới — chỉ wrap query đọc/merge từ các bảng
//! hiện có. Save/delete/update vẫn dùng các per-feature command đã có
//! (fb_save_auth_token, fb_ads_save_auth_token, ...). Việc gộp duy nhất là:
//!
//!   `token_list_fb_user_tokens` — merge `fb_auth_tokens` + `fb_ads_auth_tokens`
//!   theo `token_hash` để 1 token paste cho cả Reels + Ads chỉ hiện 1 dòng
//!   trong UI (source = "both"), không phải 2 dòng giống nhau.

use serde::Serialize;
use tauri::State;

use crate::commands::{CmdError, CmdResult};
use crate::db::{FbAdsDbState, FbReelsDbState};

/// 1 row trong list FB User Token gộp. Nếu cùng `token_hash` xuất hiện ở cả
/// 2 bảng, merge thành 1 row với `source = "both"` + cả `reels_id` lẫn `ads_id`
/// để FE biết phải xóa ở bảng nào khi user delete.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnifiedFbToken {
    /// `"reels"`, `"ads"`, hoặc `"both"` — token đang lưu cho feature nào.
    pub source: String,
    /// id ở `fb_auth_tokens` — None nếu source = `"ads"` only.
    pub reels_id: Option<i64>,
    /// id ở `fb_ads_auth_tokens` — None nếu source = `"reels"` only.
    pub ads_id: Option<i64>,
    pub label: String,
    pub token_hash: String,
    /// `added_at_ms` sớm nhất giữa 2 bảng (token gốc paste lần đầu).
    pub added_at_ms: i64,
    /// OR cả 2 cờ expired — nếu 1 bên đánh dấu thì coi như expired.
    pub expired: bool,
}

/// List User Token gộp giữa `fb_auth_tokens` (Reels) + `fb_ads_auth_tokens` (Ads).
/// Dedupe theo `token_hash` — 1 token paste cho cả 2 feature chỉ hiện 1 row.
#[tauri::command]
pub fn token_list_fb_user_tokens(
    reels_db: State<'_, FbReelsDbState>,
    ads_db: State<'_, FbAdsDbState>,
) -> CmdResult<Vec<UnifiedFbToken>> {
    // Read Reels tokens. `let rows = ...; rows` pattern để materialize Vec
    // TRƯỚC khi block kết thúc, tránh lifetime issue với stmt/conn temporary.
    let reels_rows: Vec<(i64, String, String, i64, bool)> = {
        let conn = reels_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let mut stmt = conn.prepare(
            "SELECT id, label, token_hash, added_at_ms, expired
             FROM fb_auth_tokens ORDER BY added_at_ms ASC",
        )?;
        let rows: Vec<_> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)? != 0,
                ))
            })?
            .collect::<Result<_, _>>()?;
        rows
    };

    // Read Ads tokens — cùng pattern.
    let ads_rows: Vec<(i64, String, String, i64, bool)> = {
        let conn = ads_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let mut stmt = conn.prepare(
            "SELECT id, label, token_hash, added_at_ms, expired
             FROM fb_ads_auth_tokens ORDER BY added_at_ms ASC",
        )?;
        let rows: Vec<_> = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)? != 0,
                ))
            })?
            .collect::<Result<_, _>>()?;
        rows
    };

    // Merge by token_hash. Dùng Vec + linear search thay vì HashMap để giữ
    // được order ổn định (added_at_ms ASC) sau merge.
    let mut merged: Vec<UnifiedFbToken> = Vec::new();
    for (id, label, hash, added, expired) in reels_rows {
        merged.push(UnifiedFbToken {
            source: "reels".to_string(),
            reels_id: Some(id),
            ads_id: None,
            label,
            token_hash: hash,
            added_at_ms: added,
            expired,
        });
    }
    for (id, label, hash, added, expired) in ads_rows {
        if let Some(existing) = merged.iter_mut().find(|t| t.token_hash == hash) {
            existing.source = "both".to_string();
            existing.ads_id = Some(id);
            existing.expired = existing.expired || expired;
            // Giữ label sớm hơn (label Reels) — không overwrite.
            if added < existing.added_at_ms {
                existing.added_at_ms = added;
            }
        } else {
            merged.push(UnifiedFbToken {
                source: "ads".to_string(),
                reels_id: None,
                ads_id: Some(id),
                label,
                token_hash: hash,
                added_at_ms: added,
                expired,
            });
        }
    }

    // Sort theo added_at_ms DESC để token mới nhất ở đầu list.
    merged.sort_by(|a, b| b.added_at_ms.cmp(&a.added_at_ms));
    Ok(merged)
}

/// Đếm tổng quan để hiển thị badge trên tab Token Manager.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenSummary {
    /// Số FB User Token unique đã lưu (dedupe theo `token_hash` cross-table).
    pub fb_user_tokens: usize,
    pub fb_pages: usize,
    pub fb_ad_accounts: usize,
    /// Số Pages đang bị `token_expired = 1` — UI dùng để show badge cảnh báo.
    pub fb_pages_expired: usize,
}

#[tauri::command]
pub fn token_summary(
    reels_db: State<'_, FbReelsDbState>,
    ads_db: State<'_, FbAdsDbState>,
) -> CmdResult<TokenSummary> {
    // Inline đếm token theo từng bảng + Pages + Ad Accounts trong 1 lần lock
    // mỗi DB. State<'_, T> tauri 2.x không Copy/Clone nên không thể reuse sau
    // khi pass vào function khác — phải tự query trực tiếp ở đây.

    // Reels DB: đếm tokens + pages + pages_expired.
    let (reels_token_hashes, fb_pages, fb_pages_expired) = {
        let conn = reels_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT token_hash FROM fb_auth_tokens")?;
        let hashes: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        drop(stmt);
        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM fb_pages", [], |r| r.get(0))
            .unwrap_or(0);
        let expired: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM fb_pages WHERE token_expired = 1",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        (hashes, total as usize, expired as usize)
    };

    // Ads DB: đếm tokens + ad_accounts.
    let (ads_token_hashes, fb_ad_accounts) = {
        let conn = ads_db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let mut stmt = conn.prepare("SELECT token_hash FROM fb_ads_auth_tokens")?;
        let hashes: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<_, _>>()?;
        drop(stmt);
        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM fb_ad_accounts", [], |r| r.get(0))
            .unwrap_or(0);
        (hashes, total as usize)
    };

    // Dedupe cross-table — token paste cho cả 2 feature chỉ tính 1.
    let mut unique = reels_token_hashes;
    for h in ads_token_hashes {
        if !unique.iter().any(|x| x == &h) {
            unique.push(h);
        }
    }

    Ok(TokenSummary {
        fb_user_tokens: unique.len(),
        fb_pages,
        fb_ad_accounts,
        fb_pages_expired,
    })
}
