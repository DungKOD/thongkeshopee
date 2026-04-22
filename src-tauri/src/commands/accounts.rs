//! Shopee account CRUD — list/create/rename/delete.
//!
//! Mỗi account là 1 row trong `shopee_accounts` (PK auto, `name` UNIQUE).
//! Data Shopee (clicks/orders/manual_entries) tag về account qua FK
//! `shopee_account_id`. FB ads attribution derive tại query-time qua JOIN
//! `(day_date, sub_ids)` với Shopee tables đã tag.
//!
//! Safety: không cho xóa account còn data → FE phải reassign/xóa rows trước.

use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::State;

use super::{CmdError, CmdResult};
use crate::db::DbState;

/// Reserved — account "Mặc định" catch-all cho sub_id chưa gán TK. Không cho
/// rename/delete; migration seed id=1 ở `db::mod::migrate_shopee_accounts`.
const DEFAULT_ACCOUNT_ID: i64 = 1;

/// 1 account Shopee affiliate trong DB.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShopeeAccount {
    pub id: i64,
    pub name: String,
    pub color: Option<String>,
    pub created_at: String,
    /// Đếm số row Shopee (clicks + orders + manual) thuộc account — dùng UI
    /// hiện "TK A (4716 rows)" và block delete khi > 0.
    pub row_count: i64,
}

/// List tất cả account, kèm row_count (union clicks + orders + manual entries).
/// Sort theo name ASC cho UI ổn định.
#[tauri::command]
pub fn list_shopee_accounts(state: State<'_, DbState>) -> CmdResult<Vec<ShopeeAccount>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT a.id, a.name, a.color, a.created_at,
                COALESCE(
                    (SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = a.id), 0
                ) +
                COALESCE(
                    (SELECT COUNT(*) FROM raw_shopee_order_items WHERE shopee_account_id = a.id), 0
                ) +
                COALESCE(
                    (SELECT COUNT(*) FROM manual_entries WHERE shopee_account_id = a.id), 0
                ) AS row_count
         FROM shopee_accounts a
         ORDER BY a.name ASC",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(ShopeeAccount {
            id: r.get(0)?,
            name: r.get(1)?,
            color: r.get(2)?,
            created_at: r.get(3)?,
            row_count: r.get(4)?,
        })
    })?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(CmdError::from)
}

/// Tạo account mới. Trả về id mới. Fail 400 nếu name trống hoặc trùng.
#[tauri::command]
pub fn create_shopee_account(
    state: State<'_, DbState>,
    name: String,
    color: Option<String>,
) -> CmdResult<i64> {
    let trimmed = name.trim().to_string();
    if trimmed.is_empty() {
        return Err(CmdError::msg("Tên account không được để trống"));
    }
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let now = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string();
    conn.execute(
        "INSERT INTO shopee_accounts (name, color, created_at) VALUES (?1, ?2, ?3)",
        params![trimmed, color, now],
    )
    .map_err(|e| {
        if e.to_string().contains("UNIQUE constraint failed") {
            CmdError::msg(format!("Account '{trimmed}' đã tồn tại"))
        } else {
            CmdError::from(e)
        }
    })?;
    Ok(conn.last_insert_rowid())
}

/// Rename account. Không đụng FK data. Fail nếu name trùng với account khác.
#[tauri::command]
pub fn rename_shopee_account(
    state: State<'_, DbState>,
    id: i64,
    new_name: String,
) -> CmdResult<()> {
    if id == DEFAULT_ACCOUNT_ID {
        return Err(CmdError::msg(
            "TK hệ thống 'Mặc định' không thể đổi tên",
        ));
    }
    let trimmed = new_name.trim().to_string();
    if trimmed.is_empty() {
        return Err(CmdError::msg("Tên account không được để trống"));
    }
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let affected = conn
        .execute(
            "UPDATE shopee_accounts SET name = ?1 WHERE id = ?2",
            params![trimmed, id],
        )
        .map_err(|e| {
            if e.to_string().contains("UNIQUE constraint failed") {
                CmdError::msg(format!("Tên '{trimmed}' đã thuộc account khác"))
            } else {
                CmdError::from(e)
            }
        })?;
    if affected == 0 {
        return Err(CmdError::msg(format!("Account id={id} không tồn tại")));
    }
    Ok(())
}

/// Đổi màu badge UI. Không ảnh hưởng data/query.
#[tauri::command]
pub fn update_shopee_account_color(
    state: State<'_, DbState>,
    id: i64,
    color: Option<String>,
) -> CmdResult<()> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "UPDATE shopee_accounts SET color = ?1 WHERE id = ?2",
        params![color, id],
    )?;
    Ok(())
}

/// Xóa account. Block nếu còn bất kỳ row data nào FK về account (Shopee clicks/
/// orders/manual). User phải reassign hoặc xóa rows trước qua UI riêng.
/// Account default (id=1) có thể xóa nếu user muốn — chỉ khi rỗng.
#[tauri::command]
pub fn delete_shopee_account(state: State<'_, DbState>, id: i64) -> CmdResult<()> {
    if id == DEFAULT_ACCOUNT_ID {
        return Err(CmdError::msg("TK hệ thống 'Mặc định' không thể xóa"));
    }
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let row_count: i64 = conn.query_row(
        "SELECT
            COALESCE((SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?1), 0) +
            COALESCE((SELECT COUNT(*) FROM raw_shopee_order_items WHERE shopee_account_id = ?1), 0) +
            COALESCE((SELECT COUNT(*) FROM manual_entries WHERE shopee_account_id = ?1), 0)",
        params![id],
        |r| r.get(0),
    )?;
    if row_count > 0 {
        return Err(CmdError::msg(format!(
            "Account còn {row_count} dòng data — hãy chuyển sang account khác hoặc xóa data trước khi xóa account"
        )));
    }
    let affected = conn.execute(
        "DELETE FROM shopee_accounts WHERE id = ?1",
        params![id],
    )?;
    if affected == 0 {
        return Err(CmdError::msg(format!("Account id={id} không tồn tại")));
    }
    Ok(())
}

/// Reassign toàn bộ data từ account này sang account khác.
/// Dùng khi user muốn merge 2 TK hoặc chuẩn bị xóa 1 TK.
/// Atomic qua transaction — fail partial không thể xảy ra.
#[tauri::command]
pub fn reassign_shopee_account_data(
    state: State<'_, DbState>,
    from_id: i64,
    to_id: i64,
) -> CmdResult<i64> {
    if from_id == to_id {
        return Err(CmdError::msg("from_id và to_id phải khác nhau"));
    }
    if to_id == DEFAULT_ACCOUNT_ID {
        return Err(CmdError::msg(
            "Không thể chuyển data về TK 'Mặc định' — chọn TK thật",
        ));
    }
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    // Validate cả 2 account tồn tại.
    let to_exists: bool = conn
        .query_row(
            "SELECT 1 FROM shopee_accounts WHERE id = ?1",
            params![to_id],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if !to_exists {
        return Err(CmdError::msg(format!("Account đích id={to_id} không tồn tại")));
    }

    let tx = conn.transaction()?;
    let mut total: i64 = 0;
    total += tx.execute(
        "UPDATE raw_shopee_clicks SET shopee_account_id = ?1 WHERE shopee_account_id = ?2",
        params![to_id, from_id],
    )? as i64;
    total += tx.execute(
        "UPDATE raw_shopee_order_items SET shopee_account_id = ?1 WHERE shopee_account_id = ?2",
        params![to_id, from_id],
    )? as i64;
    total += tx.execute(
        "UPDATE manual_entries SET shopee_account_id = ?1 WHERE shopee_account_id = ?2",
        params![to_id, from_id],
    )? as i64;
    tx.commit()?;
    Ok(total)
}
