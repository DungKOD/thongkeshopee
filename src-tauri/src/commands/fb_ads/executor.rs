//! Batch executor — chạy 1 batch trong tokio task background.
//!
//! Flow cho mỗi job:
//! 1. upload video → fb_video_id
//! 2. create creative (page_id + video_id + caption + link với sub_id) → fb_creative_id
//! 3. create campaign (PAUSED) → fb_campaign_id
//! 4. create adset (clone targeting/budget từ template, PAUSED) → fb_adset_id
//! 5. create ad (link creative + adset, PAUSED) → fb_ad_id
//!
//! Throttle giữa các jobs để tránh rate limit Marketing API.
//! Emit `fb_ads_progress` events qua Tauri để UI render realtime.

use std::time::Duration;

use anyhow::Result;
use rusqlite::params;
use serde_json::Value;
use tauri::{AppHandle, Emitter, Manager};
use tokio::time::sleep;

use super::clone::{
    build_ad_payload, build_adset_payload, build_campaign_payload, build_creative_payload,
    CloneContext,
};
use super::graph_api;
use super::types::CampBatchProgress;
use super::upload;
use crate::db::FbAdsDbState;

/// Delay giữa các jobs để tránh rate limit Marketing API.
/// 200 calls/giờ user level × 5 calls/job ≈ 40 jobs/giờ tối đa nếu không
/// throttle. 30s delay = 120 jobs/giờ — vẫn dưới limit vì FB rate đếm
/// theo phức tạp call chứ không chỉ count.
const THROTTLE_BETWEEN_JOBS_MS: u64 = 30_000;

/// HTTP client user-agent riêng cho executor.
const USER_AGENT: &str = "ThongKeShopee/0.12.0 (BulkCampExecutor)";

/// Stage status enum — dùng cho cả DB string + emit event.
const STAGE_UPLOADING: &str = "uploading_video";
const STAGE_CREATING_CREATIVE: &str = "creating_creative";
const STAGE_CREATING_CAMPAIGN: &str = "creating_campaign";
const STAGE_CREATING_ADSET: &str = "creating_adset";
const STAGE_CREATING_AD: &str = "creating_ad";
const STAGE_DONE: &str = "done";
const STAGE_FAILED: &str = "failed";

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 1 job snapshot lấy từ DB.
struct JobSnapshot {
    job_id: i64,
    row_index: i64,
    camp_name: String,
    adset_name: String,
    ad_name: String,
    caption: Option<String>,
    video_path: String,
    sub_id: String,
    fb_video_id: Option<String>,
    fb_creative_id: Option<String>,
    fb_campaign_id: Option<String>,
    fb_adset_id: Option<String>,
    fb_ad_id: Option<String>,
}

/// Lock helper — get conn từ state, handle lock poison.
fn with_conn<F, T>(state: &FbAdsDbState, f: F) -> Result<T>
where
    F: FnOnce(&rusqlite::Connection) -> rusqlite::Result<T>,
{
    let guard = state.0.lock().map_err(|_| anyhow::anyhow!("lock poisoned"))?;
    f(&guard).map_err(Into::into)
}

/// Update 1 job với stage + progress + optional fb_id.
#[allow(clippy::too_many_arguments)]
fn update_job_stage(
    state: &FbAdsDbState,
    job_id: i64,
    status: &str,
    progress: i64,
    fb_video_id: Option<&str>,
    fb_creative_id: Option<&str>,
    fb_campaign_id: Option<&str>,
    fb_adset_id: Option<&str>,
    fb_ad_id: Option<&str>,
    error_message: Option<&str>,
    set_finished: bool,
) -> Result<()> {
    let finished = if set_finished { Some(now_ms()) } else { None };
    with_conn(state, |c| {
        c.execute(
            "UPDATE fb_camp_jobs SET
                 status = ?1,
                 progress = ?2,
                 fb_video_id = COALESCE(?3, fb_video_id),
                 fb_creative_id = COALESCE(?4, fb_creative_id),
                 fb_campaign_id = COALESCE(?5, fb_campaign_id),
                 fb_adset_id = COALESCE(?6, fb_adset_id),
                 fb_ad_id = COALESCE(?7, fb_ad_id),
                 error_message = ?8,
                 finished_at_ms = COALESCE(?9, finished_at_ms)
             WHERE job_id = ?10",
            params![
                status,
                progress,
                fb_video_id,
                fb_creative_id,
                fb_campaign_id,
                fb_adset_id,
                fb_ad_id,
                error_message,
                finished,
                job_id
            ],
        )?;
        Ok(())
    })
}

fn mark_job_started(state: &FbAdsDbState, job_id: i64) -> Result<()> {
    let now = now_ms();
    with_conn(state, |c| {
        c.execute(
            "UPDATE fb_camp_jobs SET started_at_ms = ?1, error_message = NULL WHERE job_id = ?2",
            params![now, job_id],
        )?;
        Ok(())
    })
}

fn update_batch_status(
    state: &FbAdsDbState,
    batch_id: i64,
    status: &str,
    set_finished: bool,
) -> Result<()> {
    let finished = if set_finished { Some(now_ms()) } else { None };
    with_conn(state, |c| {
        c.execute(
            "UPDATE fb_camp_batches SET
                 status = ?1,
                 finished_at_ms = COALESCE(?2, finished_at_ms)
             WHERE batch_id = ?3",
            params![status, finished, batch_id],
        )?;
        Ok(())
    })
}

fn increment_batch_counter(
    state: &FbAdsDbState,
    batch_id: i64,
    success: bool,
) -> Result<()> {
    let col = if success { "success_count" } else { "failed_count" };
    with_conn(state, |c| {
        c.execute(
            &format!(
                "UPDATE fb_camp_batches SET {col} = {col} + 1 WHERE batch_id = ?1"
            ),
            params![batch_id],
        )?;
        Ok(())
    })
}

fn emit_progress(app: &AppHandle, batch_id: i64, job: &JobSnapshot, status: &str, progress: i64) {
    let _ = app.emit(
        "fb_ads_progress",
        CampBatchProgress {
            batch_id,
            job_id: job.job_id,
            row_index: job.row_index,
            status: status.to_string(),
            progress,
        },
    );
}

/// Load 1 job từ DB (cho retry hoặc fresh execute).
fn load_job(state: &FbAdsDbState, job_id: i64) -> Result<JobSnapshot> {
    with_conn(state, |c| {
        c.query_row(
            "SELECT job_id, row_index, camp_name, adset_name, ad_name, caption,
                    video_path, sub_id, fb_video_id, fb_creative_id,
                    fb_campaign_id, fb_adset_id, fb_ad_id
             FROM fb_camp_jobs WHERE job_id = ?1",
            params![job_id],
            |r| {
                Ok(JobSnapshot {
                    job_id: r.get(0)?,
                    row_index: r.get(1)?,
                    camp_name: r.get(2)?,
                    adset_name: r.get(3)?,
                    ad_name: r.get(4)?,
                    caption: r.get(5)?,
                    video_path: r.get(6)?,
                    sub_id: r.get(7)?,
                    fb_video_id: r.get(8)?,
                    fb_creative_id: r.get(9)?,
                    fb_campaign_id: r.get(10)?,
                    fb_adset_id: r.get(11)?,
                    fb_ad_id: r.get(12)?,
                })
            },
        )
    })
}

fn list_pending_jobs(state: &FbAdsDbState, batch_id: i64) -> Result<Vec<i64>> {
    with_conn(state, |c| {
        let mut stmt = c.prepare(
            "SELECT job_id FROM fb_camp_jobs
             WHERE batch_id = ?1 AND status NOT IN ('done')
             ORDER BY row_index ASC",
        )?;
        let ids: Vec<i64> = stmt
            .query_map(params![batch_id], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(ids)
    })
}

/// Tạo HTTP client riêng cho executor — timeout dài hơn cho video upload.
fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs(900))
        .build()
        .map_err(Into::into)
}

/// Chạy 1 job qua đủ 5 stage. Trả Err nếu fail bất kỳ stage nào — caller
/// đã update status='failed' rồi.
#[allow(clippy::too_many_arguments)]
async fn execute_one_job(
    client: &reqwest::Client,
    state: &FbAdsDbState,
    app: &AppHandle,
    batch_id: i64,
    job: &JobSnapshot,
    account_id: &str,
    token: &str,
    ctx: &CloneContext,
) -> Result<()> {
    // Stage 1: upload video
    let video_id = if let Some(existing) = &job.fb_video_id {
        existing.clone()
    } else {
        update_job_stage(state, job.job_id, STAGE_UPLOADING, 10, None, None, None, None, None, None, false)?;
        emit_progress(app, batch_id, job, STAGE_UPLOADING, 10);
        let id = upload::upload_ad_video(client, account_id, token, &job.video_path).await?;
        update_job_stage(
            state, job.job_id, STAGE_UPLOADING, 20,
            Some(&id), None, None, None, None, None, false,
        )?;
        id
    };

    // Stage 2: create creative
    let creative_id = if let Some(existing) = &job.fb_creative_id {
        existing.clone()
    } else {
        update_job_stage(state, job.job_id, STAGE_CREATING_CREATIVE, 30, None, None, None, None, None, None, false)?;
        emit_progress(app, batch_id, job, STAGE_CREATING_CREATIVE, 30);
        let payload = build_creative_payload(
            ctx,
            &format!("CR-{}", job.ad_name),
            &video_id,
            job.caption.as_deref(),
            &job.sub_id,
        );
        let id = graph_api::create_creative(client, account_id, token, &payload).await?;
        update_job_stage(
            state, job.job_id, STAGE_CREATING_CREATIVE, 40,
            None, Some(&id), None, None, None, None, false,
        )?;
        id
    };

    // Stage 3: create campaign
    let campaign_id = if let Some(existing) = &job.fb_campaign_id {
        existing.clone()
    } else {
        update_job_stage(state, job.job_id, STAGE_CREATING_CAMPAIGN, 55, None, None, None, None, None, None, false)?;
        emit_progress(app, batch_id, job, STAGE_CREATING_CAMPAIGN, 55);
        let payload = build_campaign_payload(ctx, &job.camp_name);
        let id = graph_api::create_campaign(client, account_id, token, &payload).await?;
        update_job_stage(
            state, job.job_id, STAGE_CREATING_CAMPAIGN, 65,
            None, None, Some(&id), None, None, None, false,
        )?;
        id
    };

    // Stage 4: create adset
    let adset_id = if let Some(existing) = &job.fb_adset_id {
        existing.clone()
    } else {
        update_job_stage(state, job.job_id, STAGE_CREATING_ADSET, 75, None, None, None, None, None, None, false)?;
        emit_progress(app, batch_id, job, STAGE_CREATING_ADSET, 75);
        let payload = build_adset_payload(ctx, &job.adset_name, &campaign_id);
        let id = graph_api::create_adset(client, account_id, token, &payload).await?;
        update_job_stage(
            state, job.job_id, STAGE_CREATING_ADSET, 85,
            None, None, None, Some(&id), None, None, false,
        )?;
        id
    };

    // Stage 5: create ad
    if job.fb_ad_id.is_none() {
        update_job_stage(state, job.job_id, STAGE_CREATING_AD, 92, None, None, None, None, None, None, false)?;
        emit_progress(app, batch_id, job, STAGE_CREATING_AD, 92);
        let payload = build_ad_payload(&job.ad_name, &adset_id, &creative_id);
        let id = graph_api::create_ad(client, account_id, token, &payload).await?;
        update_job_stage(
            state, job.job_id, STAGE_DONE, 100,
            None, None, None, None, Some(&id), None, true,
        )?;
    } else {
        update_job_stage(
            state, job.job_id, STAGE_DONE, 100,
            None, None, None, None, None, None, true,
        )?;
    }

    emit_progress(app, batch_id, job, STAGE_DONE, 100);
    Ok(())
}

/// Entry point — chạy toàn bộ batch. Caller spawn vào tokio task.
pub async fn run_batch(
    app: AppHandle,
    batch_id: i64,
    account_id: String,
    token: String,
    template_snapshot: Value,
) {
    let state = match app.try_state::<FbAdsDbState>() {
        Some(s) => s,
        None => {
            eprintln!("[fb_ads executor] FbAdsDbState not available");
            return;
        }
    };

    let ctx = match CloneContext::from_snapshot(&template_snapshot) {
        Ok(c) => c,
        Err(e) => {
            let _ = update_batch_status(&state, batch_id, "failed", true);
            let msg = format!("Không parse được template: {e}");
            eprintln!("[fb_ads executor] {msg}");
            let _ = app.emit(
                "fb_ads_batch_failed",
                serde_json::json!({ "batchId": batch_id, "error": msg }),
            );
            return;
        }
    };

    let client = match http_client() {
        Ok(c) => c,
        Err(e) => {
            let _ = update_batch_status(&state, batch_id, "failed", true);
            eprintln!("[fb_ads executor] http client error: {e}");
            return;
        }
    };

    let _ = update_batch_status(&state, batch_id, "running", false);

    let job_ids = match list_pending_jobs(&state, batch_id) {
        Ok(ids) => ids,
        Err(e) => {
            eprintln!("[fb_ads executor] list jobs error: {e}");
            let _ = update_batch_status(&state, batch_id, "failed", true);
            return;
        }
    };

    for (i, job_id) in job_ids.iter().enumerate() {
        let job = match load_job(&state, *job_id) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("[fb_ads executor] load job {job_id} error: {e}");
                continue;
            }
        };

        let _ = mark_job_started(&state, job.job_id);

        match execute_one_job(&client, &state, &app, batch_id, &job, &account_id, &token, &ctx).await {
            Ok(_) => {
                let _ = increment_batch_counter(&state, batch_id, true);
            }
            Err(e) => {
                let msg = e.to_string();
                let _ = update_job_stage(
                    &state, job.job_id, STAGE_FAILED, 0,
                    None, None, None, None, None, Some(&msg), true,
                );
                emit_progress(&app, batch_id, &job, STAGE_FAILED, 0);
                let _ = increment_batch_counter(&state, batch_id, false);
            }
        }

        // Throttle giữa các jobs (skip sau job cuối).
        if i + 1 < job_ids.len() {
            sleep(Duration::from_millis(THROTTLE_BETWEEN_JOBS_MS)).await;
        }
    }

    let _ = update_batch_status(&state, batch_id, "completed", true);
    let _ = app.emit(
        "fb_ads_batch_completed",
        serde_json::json!({ "batchId": batch_id }),
    );
}

