//! DTOs cho FB Ads bulk camp creator.

use serde::{Deserialize, Serialize};

/// Ad Account user lưu — không trả token ra UI. `token_hash` là 8 hex SHA-256
/// để UI tô màu group account cùng token.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbAdAccount {
    pub account_id: String,
    pub name: String,
    pub currency: Option<String>,
    pub timezone_name: Option<String>,
    pub token_hash: String,
}

/// User Token đã lưu — meta only, raw token fetch on-demand. UI dùng
/// `token_hash` làm hue để phân biệt nhiều token.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbAdsAuthToken {
    pub id: i64,
    pub label: String,
    pub token_hash: String,
    pub added_at_ms: i64,
    pub expired: bool,
}

/// Ad Account kèm token — dùng khi validate token mới hoặc save.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbAdAccountWithToken {
    pub account_id: String,
    pub name: String,
    pub currency: Option<String>,
    pub timezone_name: Option<String>,
    pub access_token: String,
}

/// Campaign metadata để user chọn làm template.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampaignSummary {
    pub id: String,
    pub name: String,
    pub objective: Option<String>,
    pub status: Option<String>,
    pub created_time: Option<String>,
}

/// Template đã cache — snapshot JSON đầy đủ structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampTemplate {
    pub template_id: i64,
    pub account_id: String,
    pub fb_campaign_id: String,
    pub name: String,
    pub objective: Option<String>,
    pub cached_at_ms: i64,
}

/// Template với snapshot JSON đầy đủ — dùng khi preview.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampTemplateDetail {
    pub template_id: i64,
    pub account_id: String,
    pub fb_campaign_id: String,
    pub name: String,
    pub objective: Option<String>,
    pub snapshot: serde_json::Value,
    pub cached_at_ms: i64,
}

/// 1 row trong spreadsheet — user nhập đầy đủ.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CampRow {
    pub camp_name: String,
    pub adset_name: String,
    pub ad_name: String,
    pub caption: String,
    pub video_path: String,
    pub sub_id: String,
}

/// Draft = 1 spreadsheet state user đang edit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampDraft {
    pub draft_id: i64,
    pub template_id: i64,
    pub name: String,
    pub rows: Vec<CampRow>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Draft chỉ metadata — dùng khi list (không kèm rows nặng).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampDraftSummary {
    pub draft_id: i64,
    pub template_id: i64,
    pub template_name: Option<String>,
    pub name: String,
    pub row_count: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Batch tổng — 1 lần chạy create N camps.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampBatch {
    pub batch_id: i64,
    pub draft_id: Option<i64>,
    pub template_id: i64,
    pub template_name: String,
    pub account_id: String,
    pub total_rows: i64,
    pub success_count: i64,
    pub failed_count: i64,
    pub status: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

/// Job = 1 row trong batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FbCampJob {
    pub job_id: i64,
    pub batch_id: i64,
    pub row_index: i64,
    pub camp_name: String,
    pub adset_name: String,
    pub ad_name: String,
    pub caption: Option<String>,
    pub video_path: String,
    pub sub_id: String,
    pub status: String,
    pub progress: i64,
    pub fb_video_id: Option<String>,
    pub fb_creative_id: Option<String>,
    pub fb_campaign_id: Option<String>,
    pub fb_adset_id: Option<String>,
    pub fb_ad_id: Option<String>,
    pub error_message: Option<String>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
}

/// Progress event emit qua Tauri sau mỗi stage transition của 1 job.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CampBatchProgress {
    pub batch_id: i64,
    pub job_id: i64,
    pub row_index: i64,
    pub status: String,
    pub progress: i64,
}
