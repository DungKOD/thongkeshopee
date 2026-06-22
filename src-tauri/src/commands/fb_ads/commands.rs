//! Tauri commands cho FB Ads bulk camp creator.
//!
//! Phase 1: account management + template fetch/cache + draft CRUD.
//! Phase 2 (TODO): batch execution với upload + create.

use rusqlite::params;
use tauri::{AppHandle, State};

use crate::commands::{CmdError, CmdResult};
use crate::db::FbAdsDbState;

use super::executor;
use super::graph_api;
use super::types::*;

const USER_AGENT: &str = "ThongKeShopee/0.12.0 (FbAdsBulkCamp)";

fn http_client() -> CmdResult<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| CmdError::msg(format!("không tạo được HTTP client: {e}")))
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// ============================================================
// Ad Account management
// ============================================================

/// Validate token + trả về list ad accounts user quản lý.
#[tauri::command]
pub async fn fb_ads_validate_token(
    token: String,
) -> CmdResult<Vec<FbAdAccountWithToken>> {
    let client = http_client()?;
    graph_api::list_ad_accounts(&client, &token)
        .await
        .map_err(|e| CmdError::msg(e.to_string()))
}

#[tauri::command]
pub fn fb_ads_save_accounts(
    db: State<'_, FbAdsDbState>,
    accounts: Vec<FbAdAccountWithToken>,
) -> CmdResult<()> {
    let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let tx = conn.transaction()?;
    let now = now_ms();
    for a in &accounts {
        tx.execute(
            "INSERT INTO fb_ad_accounts
             (account_id, name, currency, timezone_name, access_token, added_at_ms)
             VALUES(?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(account_id) DO UPDATE SET
                 name = excluded.name,
                 currency = excluded.currency,
                 timezone_name = excluded.timezone_name,
                 access_token = excluded.access_token,
                 added_at_ms = excluded.added_at_ms",
            params![
                a.account_id, a.name, a.currency, a.timezone_name, a.access_token, now
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

#[tauri::command]
pub fn fb_ads_list_accounts(
    db: State<'_, FbAdsDbState>,
) -> CmdResult<Vec<FbAdAccount>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT account_id, name, currency, timezone_name
         FROM fb_ad_accounts ORDER BY added_at_ms ASC",
    )?;
    let accounts: Vec<FbAdAccount> = stmt
        .query_map([], |r| {
            Ok(FbAdAccount {
                account_id: r.get(0)?,
                name: r.get(1)?,
                currency: r.get(2)?,
                timezone_name: r.get(3)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(accounts)
}

#[tauri::command]
pub fn fb_ads_delete_account(
    db: State<'_, FbAdsDbState>,
    account_id: String,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "DELETE FROM fb_ad_accounts WHERE account_id = ?1",
        params![account_id],
    )?;
    Ok(())
}

// ============================================================
// Template management
// ============================================================

/// Helper — load token cho 1 account, error nếu không tồn tại.
fn load_account_token(
    conn: &rusqlite::Connection,
    account_id: &str,
) -> CmdResult<String> {
    conn.query_row(
        "SELECT access_token FROM fb_ad_accounts WHERE account_id = ?1",
        params![account_id],
        |r| r.get::<_, String>(0),
    )
    .map_err(|_| CmdError::msg("Ad Account không tồn tại trong app"))
}

/// List campaigns thực tế trên FB của 1 ad account — user pick 1 làm template.
#[tauri::command]
pub async fn fb_ads_list_fb_campaigns(
    db: State<'_, FbAdsDbState>,
    account_id: String,
    limit: Option<i32>,
) -> CmdResult<Vec<FbCampaignSummary>> {
    let token = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        load_account_token(&conn, &account_id)?
    };
    let client = http_client()?;
    graph_api::list_campaigns(&client, &account_id, &token, limit.unwrap_or(50))
        .await
        .map_err(|e| CmdError::msg(e.to_string()))
}

/// Fetch full template structure + save snapshot vào DB.
#[tauri::command]
pub async fn fb_ads_save_template(
    db: State<'_, FbAdsDbState>,
    account_id: String,
    fb_campaign_id: String,
    template_name: String,
) -> CmdResult<i64> {
    let token = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        load_account_token(&conn, &account_id)?
    };
    let client = http_client()?;
    let snapshot = graph_api::fetch_campaign_full(&client, &account_id, &fb_campaign_id, &token)
        .await
        .map_err(|e| CmdError::msg(e.to_string()))?;

    let objective = snapshot
        .get("objective")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let snapshot_json = serde_json::to_string(&snapshot)?;

    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let now = now_ms();
    conn.execute(
        "INSERT INTO fb_camp_templates
         (account_id, fb_campaign_id, name, objective, snapshot_json, cached_at_ms)
         VALUES(?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(account_id, fb_campaign_id) DO UPDATE SET
             name = excluded.name,
             objective = excluded.objective,
             snapshot_json = excluded.snapshot_json,
             cached_at_ms = excluded.cached_at_ms",
        params![
            account_id, fb_campaign_id, template_name, objective, snapshot_json, now
        ],
    )?;

    let id: i64 = conn.query_row(
        "SELECT template_id FROM fb_camp_templates
         WHERE account_id = ?1 AND fb_campaign_id = ?2",
        params![account_id, fb_campaign_id],
        |r| r.get(0),
    )?;
    Ok(id)
}

#[tauri::command]
pub fn fb_ads_list_templates(
    db: State<'_, FbAdsDbState>,
    account_id: Option<String>,
) -> CmdResult<Vec<FbCampTemplate>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let (sql, args): (&str, Vec<&dyn rusqlite::ToSql>) = if let Some(ref aid) = account_id {
        (
            "SELECT template_id, account_id, fb_campaign_id, name, objective, cached_at_ms
             FROM fb_camp_templates WHERE account_id = ?1
             ORDER BY cached_at_ms DESC",
            vec![aid],
        )
    } else {
        (
            "SELECT template_id, account_id, fb_campaign_id, name, objective, cached_at_ms
             FROM fb_camp_templates ORDER BY cached_at_ms DESC",
            vec![],
        )
    };
    let mut stmt = conn.prepare(sql)?;
    let templates: Vec<FbCampTemplate> = stmt
        .query_map(rusqlite::params_from_iter(args), |r| {
            Ok(FbCampTemplate {
                template_id: r.get(0)?,
                account_id: r.get(1)?,
                fb_campaign_id: r.get(2)?,
                name: r.get(3)?,
                objective: r.get(4)?,
                cached_at_ms: r.get(5)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(templates)
}

#[tauri::command]
pub fn fb_ads_get_template_detail(
    db: State<'_, FbAdsDbState>,
    template_id: i64,
) -> CmdResult<FbCampTemplateDetail> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let (account_id, fb_campaign_id, name, objective, snapshot_json, cached_at_ms): (
        String,
        String,
        String,
        Option<String>,
        String,
        i64,
    ) = conn
        .query_row(
            "SELECT account_id, fb_campaign_id, name, objective, snapshot_json, cached_at_ms
             FROM fb_camp_templates WHERE template_id = ?1",
            params![template_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .map_err(|_| CmdError::msg("Template không tồn tại"))?;
    let snapshot: serde_json::Value = serde_json::from_str(&snapshot_json)?;
    Ok(FbCampTemplateDetail {
        template_id,
        account_id,
        fb_campaign_id,
        name,
        objective,
        snapshot,
        cached_at_ms,
    })
}

#[tauri::command]
pub fn fb_ads_delete_template(
    db: State<'_, FbAdsDbState>,
    template_id: i64,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "DELETE FROM fb_camp_templates WHERE template_id = ?1",
        params![template_id],
    )?;
    Ok(())
}

// ============================================================
// Draft (spreadsheet) management
// ============================================================

#[tauri::command]
pub fn fb_ads_save_draft(
    db: State<'_, FbAdsDbState>,
    draft_id: Option<i64>,
    template_id: i64,
    name: String,
    rows: Vec<CampRow>,
) -> CmdResult<i64> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let rows_json = serde_json::to_string(&rows)?;
    let now = now_ms();
    match draft_id {
        Some(id) => {
            conn.execute(
                "UPDATE fb_camp_drafts SET
                     template_id = ?1, name = ?2, rows_json = ?3, updated_at_ms = ?4
                 WHERE draft_id = ?5",
                params![template_id, name, rows_json, now, id],
            )?;
            Ok(id)
        }
        None => {
            conn.execute(
                "INSERT INTO fb_camp_drafts
                 (template_id, name, rows_json, created_at_ms, updated_at_ms)
                 VALUES(?1, ?2, ?3, ?4, ?5)",
                params![template_id, name, rows_json, now, now],
            )?;
            Ok(conn.last_insert_rowid())
        }
    }
}

#[tauri::command]
pub fn fb_ads_list_drafts(
    db: State<'_, FbAdsDbState>,
) -> CmdResult<Vec<FbCampDraftSummary>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT d.draft_id, d.template_id, t.name, d.name, d.rows_json,
                d.created_at_ms, d.updated_at_ms
         FROM fb_camp_drafts d
         LEFT JOIN fb_camp_templates t ON t.template_id = d.template_id
         ORDER BY d.updated_at_ms DESC",
    )?;
    let drafts: Vec<FbCampDraftSummary> = stmt
        .query_map([], |r| {
            let rows_json: String = r.get(4)?;
            let row_count: i64 = serde_json::from_str::<Vec<serde_json::Value>>(&rows_json)
                .map(|v| v.len() as i64)
                .unwrap_or(0);
            Ok(FbCampDraftSummary {
                draft_id: r.get(0)?,
                template_id: r.get(1)?,
                template_name: r.get(2)?,
                name: r.get(3)?,
                row_count,
                created_at_ms: r.get(5)?,
                updated_at_ms: r.get(6)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(drafts)
}

#[tauri::command]
pub fn fb_ads_get_draft(
    db: State<'_, FbAdsDbState>,
    draft_id: i64,
) -> CmdResult<FbCampDraft> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let (template_id, name, rows_json, created_at_ms, updated_at_ms): (
        i64,
        String,
        String,
        i64,
        i64,
    ) = conn
        .query_row(
            "SELECT template_id, name, rows_json, created_at_ms, updated_at_ms
             FROM fb_camp_drafts WHERE draft_id = ?1",
            params![draft_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .map_err(|_| CmdError::msg("Draft không tồn tại"))?;
    let rows: Vec<CampRow> = serde_json::from_str(&rows_json).unwrap_or_default();
    Ok(FbCampDraft {
        draft_id,
        template_id,
        name,
        rows,
        created_at_ms,
        updated_at_ms,
    })
}

#[tauri::command]
pub fn fb_ads_delete_draft(
    db: State<'_, FbAdsDbState>,
    draft_id: i64,
) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "DELETE FROM fb_camp_drafts WHERE draft_id = ?1",
        params![draft_id],
    )?;
    Ok(())
}

// ============================================================
// Batch execution — Phase 2
// ============================================================

/// Tạo batch mới — insert records vào DB + spawn executor background.
/// Trả về `batch_id` ngay, frontend subscribe `fb_ads_progress` event để
/// theo dõi.
///
/// Validate: ít nhất 1 row hợp lệ; template tồn tại; account_id của template
/// có token trong DB.
#[tauri::command]
pub fn fb_ads_create_batch(
    app: AppHandle,
    db: State<'_, FbAdsDbState>,
    template_id: i64,
    draft_id: Option<i64>,
    rows: Vec<CampRow>,
) -> CmdResult<i64> {
    if rows.is_empty() {
        return Err(CmdError::msg("Không có row nào để tạo"));
    }

    // Validate template + load context cho executor.
    let (account_id, template_name, snapshot_json, access_token) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let (acc_id, t_name, snap_json): (String, String, String) = conn
            .query_row(
                "SELECT account_id, name, snapshot_json
                 FROM fb_camp_templates WHERE template_id = ?1",
                params![template_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .map_err(|_| CmdError::msg("Template không tồn tại"))?;
        let token: String = conn
            .query_row(
                "SELECT access_token FROM fb_ad_accounts WHERE account_id = ?1",
                params![acc_id],
                |r| r.get(0),
            )
            .map_err(|_| CmdError::msg("Ad Account của template đã bị xóa — re-add"))?;
        (acc_id, t_name, snap_json, token)
    };

    let snapshot: serde_json::Value = serde_json::from_str(&snapshot_json)
        .map_err(|e| CmdError::msg(format!("template snapshot JSON hỏng: {e}")))?;

    // Validate ngắn — check rows có đủ field bắt buộc.
    for (i, r) in rows.iter().enumerate() {
        if r.camp_name.trim().is_empty()
            || r.adset_name.trim().is_empty()
            || r.ad_name.trim().is_empty()
            || r.video_path.trim().is_empty()
        {
            return Err(CmdError::msg(format!(
                "Row {} thiếu field bắt buộc (camp_name/adset_name/ad_name/video_path)",
                i + 1
            )));
        }
        if !std::path::Path::new(&r.video_path).exists() {
            return Err(CmdError::msg(format!(
                "Row {}: file video không tồn tại: {}",
                i + 1,
                r.video_path
            )));
        }
    }

    let now = now_ms();
    let batch_id = {
        let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO fb_camp_batches
             (draft_id, template_id, template_name, account_id, total_rows,
              status, started_at_ms)
             VALUES(?1, ?2, ?3, ?4, ?5, 'pending', ?6)",
            params![draft_id, template_id, template_name, account_id, rows.len() as i64, now],
        )?;
        let bid = tx.last_insert_rowid();
        for (idx, row) in rows.iter().enumerate() {
            tx.execute(
                "INSERT INTO fb_camp_jobs
                 (batch_id, row_index, camp_name, adset_name, ad_name,
                  caption, video_path, sub_id, status, progress)
                 VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'pending', 0)",
                params![
                    bid,
                    idx as i64,
                    row.camp_name.trim(),
                    row.adset_name.trim(),
                    row.ad_name.trim(),
                    if row.caption.trim().is_empty() {
                        None
                    } else {
                        Some(row.caption.trim().to_string())
                    },
                    row.video_path.trim(),
                    row.sub_id.trim(),
                ],
            )?;
        }
        tx.commit()?;
        bid
    };

    // Spawn executor — không block command, frontend nhận batch_id ngay.
    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        executor::run_batch(app_clone, batch_id, account_id, access_token, snapshot).await;
    });

    Ok(batch_id)
}

/// Retry 1 job đã fail. Reset status='pending', clear error_message, spawn
/// executor cho riêng job đó. Không reset fb_*_id đã có — executor sẽ skip
/// stage tương ứng (resume from last successful stage).
#[tauri::command]
pub fn fb_ads_retry_job(
    app: AppHandle,
    db: State<'_, FbAdsDbState>,
    job_id: i64,
) -> CmdResult<()> {
    let (batch_id, account_id, snapshot_json, access_token) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let (batch_id, template_id): (i64, i64) = conn
            .query_row(
                "SELECT j.batch_id, b.template_id
                 FROM fb_camp_jobs j
                 JOIN fb_camp_batches b ON b.batch_id = j.batch_id
                 WHERE j.job_id = ?1",
                params![job_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| CmdError::msg("Job không tồn tại"))?;
        let (acc_id, snap_json): (String, String) = conn
            .query_row(
                "SELECT account_id, snapshot_json FROM fb_camp_templates
                 WHERE template_id = ?1",
                params![template_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|_| CmdError::msg("Template gốc đã bị xóa"))?;
        let token: String = conn
            .query_row(
                "SELECT access_token FROM fb_ad_accounts WHERE account_id = ?1",
                params![acc_id],
                |r| r.get(0),
            )
            .map_err(|_| CmdError::msg("Ad Account của template đã bị xóa"))?;
        conn.execute(
            "UPDATE fb_camp_jobs SET
                 status = 'pending', error_message = NULL, finished_at_ms = NULL
             WHERE job_id = ?1",
            params![job_id],
        )?;
        conn.execute(
            "UPDATE fb_camp_batches SET
                 status = 'running', finished_at_ms = NULL
             WHERE batch_id = ?1",
            params![batch_id],
        )?;
        (batch_id, acc_id, snap_json, token)
    };

    let snapshot: serde_json::Value = serde_json::from_str(&snapshot_json)
        .map_err(|e| CmdError::msg(format!("template snapshot JSON hỏng: {e}")))?;

    let app_clone = app.clone();
    tauri::async_runtime::spawn(async move {
        executor::run_batch(app_clone, batch_id, account_id, access_token, snapshot).await;
    });

    Ok(())
}

#[tauri::command]
pub fn fb_ads_list_batches(
    db: State<'_, FbAdsDbState>,
    limit: Option<i64>,
) -> CmdResult<Vec<FbCampBatch>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let lim = limit.unwrap_or(100).clamp(1, 1000);
    let mut stmt = conn.prepare(
        "SELECT batch_id, draft_id, template_id, template_name, account_id,
                total_rows, success_count, failed_count, status,
                started_at_ms, finished_at_ms
         FROM fb_camp_batches ORDER BY started_at_ms DESC LIMIT ?1",
    )?;
    let batches: Vec<FbCampBatch> = stmt
        .query_map(params![lim], |r| {
            Ok(FbCampBatch {
                batch_id: r.get(0)?,
                draft_id: r.get(1)?,
                template_id: r.get(2)?,
                template_name: r.get(3)?,
                account_id: r.get(4)?,
                total_rows: r.get(5)?,
                success_count: r.get(6)?,
                failed_count: r.get(7)?,
                status: r.get(8)?,
                started_at_ms: r.get(9)?,
                finished_at_ms: r.get(10)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(batches)
}

#[tauri::command]
pub fn fb_ads_list_jobs(
    db: State<'_, FbAdsDbState>,
    batch_id: i64,
) -> CmdResult<Vec<FbCampJob>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let mut stmt = conn.prepare(
        "SELECT job_id, batch_id, row_index, camp_name, adset_name, ad_name,
                caption, video_path, sub_id, status, progress,
                fb_video_id, fb_creative_id, fb_campaign_id, fb_adset_id, fb_ad_id,
                error_message, started_at_ms, finished_at_ms
         FROM fb_camp_jobs WHERE batch_id = ?1 ORDER BY row_index ASC",
    )?;
    let jobs: Vec<FbCampJob> = stmt
        .query_map(params![batch_id], |r| {
            Ok(FbCampJob {
                job_id: r.get(0)?,
                batch_id: r.get(1)?,
                row_index: r.get(2)?,
                camp_name: r.get(3)?,
                adset_name: r.get(4)?,
                ad_name: r.get(5)?,
                caption: r.get(6)?,
                video_path: r.get(7)?,
                sub_id: r.get(8)?,
                status: r.get(9)?,
                progress: r.get(10)?,
                fb_video_id: r.get(11)?,
                fb_creative_id: r.get(12)?,
                fb_campaign_id: r.get(13)?,
                fb_adset_id: r.get(14)?,
                fb_ad_id: r.get(15)?,
                error_message: r.get(16)?,
                started_at_ms: r.get(17)?,
                finished_at_ms: r.get(18)?,
            })
        })?
        .collect::<Result<_, _>>()?;
    Ok(jobs)
}
