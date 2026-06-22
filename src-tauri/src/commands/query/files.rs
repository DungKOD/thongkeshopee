//! Tauri command `list_imported_files` + DTO.
//!
//! Đọc audit log import từ `imported_files` + gộp active_rows từ 4 mapping
//! tables qua 1 CTE UNION ALL (v10.3).

use tauri::State;

use crate::db::DbState;

use super::super::{CmdError, CmdResult};

#[tauri::command]
pub async fn list_imported_files(state: State<'_, DbState>) -> CmdResult<Vec<ImportedFileInfo>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    // v10: JOIN shopee_accounts để trả account_name + thêm reverted_at cho
    // FE phân biệt active vs đã hoàn tác. active_rows = SUM mapping (cần để
    // user biết bao nhiêu row thực tế còn active sau khi file khác revert).
    // v10.3: gộp 4 correlated subquery COUNT(*) thành 1 UNION ALL + GROUP BY.
    // Subquery cũ chạy 4×N lookup khi mở dialog import history; phiên bản này
    // scan từng mapping table 1 lần rồi LEFT JOIN trên file_id.
    let mut stmt = conn.prepare_cached(
        "WITH mapping_counts AS (
             SELECT file_id, COUNT(*) AS cnt FROM (
                 SELECT file_id FROM clicks_to_file
                 UNION ALL SELECT file_id FROM orders_to_file
                 UNION ALL SELECT file_id FROM fb_ads_to_file
                 UNION ALL SELECT file_id FROM fb_ads_hier_to_file
             ) GROUP BY file_id
         )
         SELECT f.id, f.filename, f.kind, f.imported_at, f.row_count, f.day_date,
                f.reverted_at, sa.name AS account_name,
                COALESCE(mc.cnt, 0) AS active_rows
         FROM imported_files f
         LEFT JOIN shopee_accounts sa ON sa.id = f.shopee_account_id
         LEFT JOIN mapping_counts mc ON mc.file_id = f.id
         ORDER BY f.imported_at DESC",
    )?;
    let rows: Vec<ImportedFileInfo> = stmt
        .query_map([], |r| {
            Ok(ImportedFileInfo {
                id: r.get(0)?,
                filename: r.get(1)?,
                kind: r.get(2)?,
                imported_at: r.get(3)?,
                row_count: r.get(4)?,
                day_date: r.get(5)?,
                reverted_at: r.get(6)?,
                account_name: r.get(7)?,
                active_rows: r.get(8)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(rows)
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportedFileInfo {
    pub id: i64,
    pub filename: String,
    pub kind: String,
    pub imported_at: String,
    pub row_count: i64,
    pub day_date: Option<String>,
    pub reverted_at: Option<String>,
    pub account_name: Option<String>,
    pub active_rows: i64,
}
