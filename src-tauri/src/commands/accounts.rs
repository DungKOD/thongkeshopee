//! Shopee account CRUD — list/create/rename/delete.
//!
//! Mỗi account là 1 row trong `shopee_accounts` (PK auto, `name` UNIQUE).
//! Data Shopee (clicks/orders/manual_entries) tag về account qua FK
//! `shopee_account_id`. FB ads attribution derive tại query-time qua JOIN
//! `(day_date, sub_ids)` với Shopee tables đã tag.
//!
//! Safety: không cho xóa account còn data → FE phải reassign/xóa rows trước.

use std::collections::{HashMap, HashSet};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::State;

use super::query::{is_prefix, to_canonical, Canonical};
use super::{CmdError, CmdResult};
use crate::db::DbState;

/// Tên reserved cho account "Mặc định" — catch-all cho sub_id chưa gán TK.
/// Sau v13 migration id là content_id hash (không còn = 1), nên check bằng
/// NAME thay vì ID. Migration seed ở `db::mod::migrate_shopee_accounts`.
const DEFAULT_ACCOUNT_NAME: &str = "Mặc định";

/// Check account có phải là "Mặc định" không (lookup theo id).
fn is_default_account(conn: &rusqlite::Connection, id: i64) -> CmdResult<bool> {
    use rusqlite::OptionalExtension;
    let name: Option<String> = conn
        .query_row(
            "SELECT name FROM shopee_accounts WHERE id = ?",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    Ok(name.as_deref() == Some(DEFAULT_ACCOUNT_NAME))
}

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
    // v13: id = content_id(name). Cross-machine stable — không còn autoincrement
    // collision khi 2 máy tạo cùng account fresh.
    let id = crate::sync_v9::content_id::shopee_account_id(&trimmed);
    conn.execute(
        "INSERT INTO shopee_accounts (id, name, color, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![id, trimmed, color, now],
    )
    .map_err(|e| {
        if e.to_string().contains("UNIQUE constraint failed")
            || e.to_string().contains("PRIMARY KEY")
        {
            CmdError::msg(format!("Account '{trimmed}' đã tồn tại"))
        } else {
            CmdError::from(e)
        }
    })?;
    Ok(id)
}

/// Rename account. Không đụng FK data. Fail nếu name trùng với account khác.
#[tauri::command]
pub fn rename_shopee_account(
    state: State<'_, DbState>,
    id: i64,
    new_name: String,
) -> CmdResult<()> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    if is_default_account(&conn, id)? {
        return Err(CmdError::msg(
            "TK hệ thống 'Mặc định' không thể đổi tên",
        ));
    }
    drop(conn); // release lock, reacquire below
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

/// Load map `day_date → HashSet<Canonical>` từ 3 bảng Shopee filter theo predicate.
/// `include_account` predicate: true = include row, false = skip.
fn load_shopee_canonicals_by_day(
    conn: &rusqlite::Connection,
    include_account: impl Fn(i64) -> bool,
) -> CmdResult<HashMap<String, HashSet<Canonical>>> {
    let mut out: HashMap<String, HashSet<Canonical>> = HashMap::new();
    for sql in [
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, shopee_account_id
         FROM raw_shopee_clicks",
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, shopee_account_id
         FROM raw_shopee_order_items",
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, shopee_account_id
         FROM manual_entries",
    ] {
        let mut stmt = conn.prepare(sql)?;
        let iter = stmt.query_map([], |r| {
            let day: String = r.get(0)?;
            let tuple: [String; 5] =
                [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
            let acc: i64 = r.get(6)?;
            Ok((day, tuple, acc))
        })?;
        for row in iter {
            let (day, tuple, acc) = row?;
            if !include_account(acc) {
                continue;
            }
            out.entry(day)
                .or_default()
                .insert(to_canonical(tuple));
        }
    }
    Ok(out)
}

/// Count FB ads sẽ bị "cuốn theo" khi xóa account `id`: FB ad có canonical
/// prefix-compatible với Shopee row của account này trên cùng day, VÀ
/// không còn prefix-compatible với Shopee row của account khác (safeguard
/// tránh xóa FB dùng chung cho nhiều TK).
#[tauri::command]
pub fn count_fb_linked_to_account(
    state: State<'_, DbState>,
    id: i64,
) -> CmdResult<i64> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let shop_target = load_shopee_canonicals_by_day(&conn, |a| a == id)?;
    if shop_target.is_empty() {
        return Ok(0);
    }
    let shop_other = load_shopee_canonicals_by_day(&conn, |a| a != id)?;

    let mut stmt = conn.prepare(
        "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_fb_ads",
    )?;
    let iter = stmt.query_map([], |r| {
        let day: String = r.get(0)?;
        let tuple: [String; 5] =
            [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
        Ok((day, to_canonical(tuple)))
    })?;
    let mut count: i64 = 0;
    for row in iter {
        let (day, fb_canon) = row?;
        let Some(targets) = shop_target.get(&day) else {
            continue;
        };
        let linked_to_target = targets
            .iter()
            .any(|c| is_prefix(&fb_canon, c) || is_prefix(c, &fb_canon));
        if !linked_to_target {
            continue;
        }
        let linked_to_other = shop_other
            .get(&day)
            .map(|s| {
                s.iter()
                    .any(|c| is_prefix(&fb_canon, c) || is_prefix(c, &fb_canon))
            })
            .unwrap_or(false);
        if !linked_to_other {
            count += 1;
        }
    }
    Ok(count)
}

/// Xóa account + toàn bộ data Shopee FK về account (clicks/orders/manual).
/// `also_delete_fb = true` → xóa thêm FB ads khớp sub_id prefix với Shopee
/// data của account này (và KHÔNG khớp account khác) — tránh orphan FB
/// spend hiển thị như data lạc sau khi xóa TK Shopee.
/// Account default (id=1) bảo vệ không cho xóa.
/// Atomic qua transaction: fail giữa chừng → rollback.
/// Cleanup: dọn orphan imported_files + orphan days + bump sync_state.
#[tauri::command]
pub fn delete_shopee_account(
    state: State<'_, DbState>,
    id: i64,
    also_delete_fb: Option<bool>,
) -> CmdResult<()> {
    let also_delete_fb = also_delete_fb.unwrap_or(false);

    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    if is_default_account(&conn, id)? {
        return Err(CmdError::msg("TK hệ thống 'Mặc định' không thể xóa"));
    }

    // Pre-compute FB ads cần xóa TRƯỚC khi DELETE Shopee rows (sau DELETE thì
    // shop_target rỗng → không match được). Collect list (day, tuple) để
    // DELETE trong transaction.
    let fb_to_delete: Vec<(String, [String; 5])> = if also_delete_fb {
        let shop_target = load_shopee_canonicals_by_day(&conn, |a| a == id)?;
        let shop_other = load_shopee_canonicals_by_day(&conn, |a| a != id)?;
        let mut out: Vec<(String, [String; 5])> = Vec::new();
        if !shop_target.is_empty() {
            let mut stmt = conn.prepare(
                "SELECT day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
                 FROM raw_fb_ads",
            )?;
            let iter = stmt.query_map([], |r| {
                let day: String = r.get(0)?;
                let tuple: [String; 5] =
                    [r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?];
                Ok((day, tuple))
            })?;
            for row in iter {
                let (day, tuple) = row?;
                let fb_canon = to_canonical(tuple.clone());
                let Some(targets) = shop_target.get(&day) else {
                    continue;
                };
                let linked_to_target = targets
                    .iter()
                    .any(|c| is_prefix(&fb_canon, c) || is_prefix(c, &fb_canon));
                if !linked_to_target {
                    continue;
                }
                let linked_to_other = shop_other
                    .get(&day)
                    .map(|s| {
                        s.iter().any(|c| {
                            is_prefix(&fb_canon, c) || is_prefix(c, &fb_canon)
                        })
                    })
                    .unwrap_or(false);
                if !linked_to_other {
                    out.push((day, tuple));
                }
            }
        }
        out
    } else {
        Vec::new()
    };

    let tx = conn.transaction()?;

    // DELETE FB ads đã xác định (exact match theo day + 5 sub_ids).
    if !fb_to_delete.is_empty() {
        let mut stmt = tx.prepare(
            "DELETE FROM raw_fb_ads
             WHERE day_date = ?1
               AND sub_id1 = ?2 AND sub_id2 = ?3
               AND sub_id3 = ?4 AND sub_id4 = ?5 AND sub_id5 = ?6",
        )?;
        for (day, tuple) in &fb_to_delete {
            stmt.execute(params![
                day, tuple[0], tuple[1], tuple[2], tuple[3], tuple[4]
            ])?;
        }
    }

    tx.execute(
        "DELETE FROM raw_shopee_clicks WHERE shopee_account_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM raw_shopee_order_items WHERE shopee_account_id = ?1",
        params![id],
    )?;
    tx.execute(
        "DELETE FROM manual_entries WHERE shopee_account_id = ?1",
        params![id],
    )?;
    let affected = tx.execute(
        "DELETE FROM shopee_accounts WHERE id = ?1",
        params![id],
    )?;
    if affected == 0 {
        return Err(CmdError::msg(format!("Account id={id} không tồn tại")));
    }

    // Cleanup orphan imported_files (không còn raw rows nào refer) để user
    // có thể re-import lại file sau này.
    tx.execute(
        "DELETE FROM imported_files
         WHERE id NOT IN (
             SELECT source_file_id FROM raw_shopee_clicks UNION
             SELECT source_file_id FROM raw_shopee_order_items UNION
             SELECT source_file_id FROM raw_fb_ads
         )",
        [],
    )?;
    // Cleanup orphan days (không còn raw/manual nào refer).
    tx.execute(
        "DELETE FROM days WHERE date NOT IN (
            SELECT day_date FROM raw_shopee_clicks UNION
            SELECT day_date FROM raw_shopee_order_items UNION
            SELECT day_date FROM raw_fb_ads UNION
            SELECT day_date FROM manual_entries
         )",
        [],
    )?;
    tx.commit()?;
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
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    if is_default_account(&conn, to_id)? {
        return Err(CmdError::msg(
            "Không thể chuyển data về TK 'Mặc định' — chọn TK thật",
        ));
    }
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
