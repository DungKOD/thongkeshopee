//! Commands CRUD `manual_entries`.

use rusqlite::{params, OptionalExtension};
use tauri::State;

use crate::db::types::{ManualEntryInput, ManualRowKey};
use crate::db::{now_rfc3339_z, DbState};

use super::{CmdError, CmdResult};

/// Trả `true` nếu có thay đổi DB thực sự, `false` nếu input trùng row hiện
/// có (no-op).
#[tauri::command]
pub fn save_manual_entry(
    state: State<'_, DbState>,
    input: ManualEntryInput,
) -> CmdResult<bool> {
    let mut conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;

    type ExistingRow = (
        Option<String>,
        Option<i64>,
        Option<f64>,
        Option<f64>,
        Option<i64>,
        Option<f64>,
        Option<String>,
        Option<i64>,
    );
    let existing: Option<ExistingRow> = tx
        .query_row(
            "SELECT display_name, override_clicks, override_spend, override_cpc,
                    override_orders, override_commission, notes, shopee_account_id
             FROM manual_entries
             WHERE sub_id1 = ?1 AND sub_id2 = ?2 AND sub_id3 = ?3
               AND sub_id4 = ?4 AND sub_id5 = ?5 AND day_date = ?6",
            params![
                input.sub_ids[0],
                input.sub_ids[1],
                input.sub_ids[2],
                input.sub_ids[3],
                input.sub_ids[4],
                input.day_date,
            ],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .optional()?;

    if let Some((dn, oc, os, occ, oo, ocom, n, sa)) = &existing {
        if dn == &input.display_name
            && oc == &input.override_clicks
            && os == &input.override_spend
            && occ == &input.override_cpc
            && oo == &input.override_orders
            && ocom == &input.override_commission
            && n == &input.notes
            && *sa == Some(input.shopee_account_id)
        {
            return Ok(false);
        }
    }

    let now = now_rfc3339_z();

    tx.execute(
        "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, ?)",
        params![input.day_date, now],
    )?;

    tx.execute(
        "INSERT INTO manual_entries
         (sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date,
          display_name, override_clicks, override_spend, override_cpc,
          override_orders, override_commission, notes, created_at, updated_at,
          shopee_account_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date) DO UPDATE SET
            display_name         = excluded.display_name,
            override_clicks      = excluded.override_clicks,
            override_spend       = excluded.override_spend,
            override_cpc         = excluded.override_cpc,
            override_orders      = excluded.override_orders,
            override_commission  = excluded.override_commission,
            notes                = excluded.notes,
            updated_at           = excluded.updated_at,
            shopee_account_id    = excluded.shopee_account_id",
        params![
            input.sub_ids[0],
            input.sub_ids[1],
            input.sub_ids[2],
            input.sub_ids[3],
            input.sub_ids[4],
            input.day_date,
            input.display_name,
            input.override_clicks,
            input.override_spend,
            input.override_cpc,
            input.override_orders,
            input.override_commission,
            input.notes,
            now,
            now,
            input.shopee_account_id,
        ],
    )?;

    tx.commit()?;
    Ok(true)
}

/// Xóa 1 manual entry theo key. Không ảnh hưởng raw tables.
/// `key.account_id` (nếu Some) → scope DELETE theo `shopee_account_id` để
/// không xóa nhầm manual của account khác trên cùng tuple+ngày.
#[tauri::command]
pub fn delete_manual_entry(
    state: State<'_, DbState>,
    key: ManualRowKey,
) -> CmdResult<()> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    if let Some(acc) = key.account_id {
        conn.execute(
            "DELETE FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?
               AND shopee_account_id = ?",
            params![
                key.sub_ids[0],
                key.sub_ids[1],
                key.sub_ids[2],
                key.sub_ids[3],
                key.sub_ids[4],
                key.day_date,
                acc,
            ],
        )?;
    } else {
        conn.execute(
            "DELETE FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
            params![
                key.sub_ids[0],
                key.sub_ids[1],
                key.sub_ids[2],
                key.sub_ids[3],
                key.sub_ids[4],
                key.day_date,
            ],
        )?;
    }
    Ok(())
}

/// Kiểm tra có manual entry nào cho key này không.
/// `key.account_id` (nếu Some) → check theo đúng account.
#[tauri::command]
pub fn has_manual_entry(
    state: State<'_, DbState>,
    key: ManualRowKey,
) -> CmdResult<bool> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let v: Option<i64> = if let Some(acc) = key.account_id {
        conn.query_row(
            "SELECT 1 FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?
               AND shopee_account_id = ?",
            params![
                key.sub_ids[0],
                key.sub_ids[1],
                key.sub_ids[2],
                key.sub_ids[3],
                key.sub_ids[4],
                key.day_date,
                acc,
            ],
            |r| r.get(0),
        )
        .optional()?
    } else {
        conn.query_row(
            "SELECT 1 FROM manual_entries
             WHERE sub_id1 = ? AND sub_id2 = ? AND sub_id3 = ?
               AND sub_id4 = ? AND sub_id5 = ? AND day_date = ?",
            params![
                key.sub_ids[0],
                key.sub_ids[1],
                key.sub_ids[2],
                key.sub_ids[3],
                key.sub_ids[4],
                key.day_date,
            ],
            |r| r.get(0),
        )
        .optional()?
    };
    Ok(v.is_some())
}
