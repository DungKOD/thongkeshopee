//! HTTP client cho Facebook Marketing API.
//!
//! Endpoints dùng:
//! - `GET /me/adaccounts` — list ad accounts user quản lý
//! - `GET /act_xxx/campaigns` — list campaigns (filter cho user pick template)
//! - `GET /act_xxx/campaigns/{id}?fields=...` — fetch full template structure
//! - `POST /act_xxx/advideos` — upload video creative cho ad (Phase 2)
//! - `POST /act_xxx/campaigns` — tạo campaign clone (Phase 2)
//! - `POST /act_xxx/adsets` — tạo ad set (Phase 2)
//! - `POST /act_xxx/adcreatives` — tạo ad creative (Phase 2)
//! - `POST /act_xxx/ads` — tạo ad (Phase 2)

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;

use super::types::{FbAdAccountWithToken, FbCampaignSummary};

const API_VERSION: &str = "v21.0";
const GRAPH_BASE: &str = "https://graph.facebook.com";

#[derive(Deserialize)]
struct AdAccountsResponse {
    data: Vec<RawAdAccount>,
}

#[derive(Deserialize)]
struct RawAdAccount {
    id: String,
    #[allow(dead_code)]
    account_id: Option<String>,
    name: Option<String>,
    currency: Option<String>,
    timezone_name: Option<String>,
}

#[derive(Deserialize)]
struct CampaignsResponse {
    data: Vec<RawCampaignSummary>,
}

#[derive(Deserialize)]
struct RawCampaignSummary {
    id: String,
    name: String,
    objective: Option<String>,
    status: Option<String>,
    created_time: Option<String>,
}

/// Gọi `GET /me/adaccounts` — trả về list Ad Accounts user có quyền access.
///
/// `user_token` cần scope `ads_management` hoặc `ads_read`.
/// Token chính user paste vào sẽ được trả lại trong mỗi account (cùng token cho
/// tất cả) — User token có scope ads_management apply được lên mọi ad account.
pub async fn list_ad_accounts(
    client: &Client,
    user_token: &str,
) -> Result<Vec<FbAdAccountWithToken>> {
    let url = format!("{GRAPH_BASE}/{API_VERSION}/me/adaccounts");
    let resp = client
        .get(&url)
        .query(&[
            ("access_token", user_token),
            (
                "fields",
                "id,account_id,name,currency,timezone_name",
            ),
        ])
        .send()
        .await
        .context("không gọi được /me/adaccounts")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Marketing API trả lỗi {}: {}", status, body);
    }
    let parsed: AdAccountsResponse = serde_json::from_str(&body)
        .with_context(|| format!("parse /me/adaccounts: {body}"))?;
    Ok(parsed
        .data
        .into_iter()
        .map(|a| FbAdAccountWithToken {
            // `id` trả về dạng "act_123" sẵn, account_id chỉ là "123"
            account_id: a.id,
            name: a.name.unwrap_or_else(|| "(không tên)".to_string()),
            currency: a.currency,
            timezone_name: a.timezone_name,
            access_token: user_token.to_string(),
        })
        .collect())
}

/// Gọi `GET /act_xxx/campaigns` — list campaigns trong ad account.
///
/// Filter `effective_status` = ACTIVE/PAUSED để bỏ DELETED/ARCHIVED.
pub async fn list_campaigns(
    client: &Client,
    account_id: &str,
    access_token: &str,
    limit: i32,
) -> Result<Vec<FbCampaignSummary>> {
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{account_id}/campaigns");
    let resp = client
        .get(&url)
        .query(&[
            ("access_token", access_token),
            ("fields", "id,name,objective,status,created_time"),
            ("effective_status", "[\"ACTIVE\",\"PAUSED\"]"),
            ("limit", &limit.to_string()),
        ])
        .send()
        .await
        .context("không gọi được /campaigns")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("Marketing API trả lỗi {}: {}", status, body);
    }
    let parsed: CampaignsResponse = serde_json::from_str(&body)
        .with_context(|| format!("parse /campaigns: {body}"))?;
    Ok(parsed
        .data
        .into_iter()
        .map(|c| FbCampaignSummary {
            id: c.id,
            name: c.name,
            objective: c.objective,
            status: c.status,
            created_time: c.created_time,
        })
        .collect())
}

/// Fetch full structure của 1 campaign để cache làm template.
///
/// Lấy fields đầy đủ: campaign metadata + adsets + ads + creatives.
/// Trả về raw JSON để frontend hiển thị + backend lưu nguyên để Phase 2 clone.
///
/// Strategy: 3 calls song song (hoặc tuần tự cho đơn giản):
/// 1. Campaign chính
/// 2. Adsets của campaign
/// 3. Ads + creatives
pub async fn fetch_campaign_full(
    client: &Client,
    account_id: &str,
    campaign_id: &str,
    access_token: &str,
) -> Result<serde_json::Value> {
    let _ = account_id; // có thể dùng để verify ownership sau

    // Campaign level — lấy tất cả field cần để clone.
    let camp_fields = "id,name,objective,status,buying_type,special_ad_categories,\
                       special_ad_category_country,daily_budget,lifetime_budget,\
                       bid_strategy,spend_cap,start_time,stop_time";
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{campaign_id}");
    let resp = client
        .get(&url)
        .query(&[
            ("access_token", access_token),
            ("fields", camp_fields),
        ])
        .send()
        .await
        .context("không gọi được fetch_campaign")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("fetch campaign lỗi {}: {}", status, body);
    }
    let mut campaign: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse campaign: {body}"))?;

    // Adsets — list + đọc field detail.
    let adset_fields = "id,name,status,daily_budget,lifetime_budget,\
                        bid_strategy,billing_event,optimization_goal,\
                        targeting,start_time,end_time,promoted_object,\
                        attribution_spec,destination_type";
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{campaign_id}/adsets");
    let resp = client
        .get(&url)
        .query(&[
            ("access_token", access_token),
            ("fields", adset_fields),
            ("limit", "50"),
        ])
        .send()
        .await
        .context("không gọi được /adsets")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("fetch adsets lỗi {}: {}", status, body);
    }
    let adsets: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse adsets: {body}"))?;

    // Ads + creatives.
    let ad_fields = "id,name,status,adset_id,creative{id,name,object_story_spec,\
                     object_story_id,effective_object_story_id,thumbnail_url,\
                     image_url,video_id,call_to_action_type,link_url}";
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{campaign_id}/ads");
    let resp = client
        .get(&url)
        .query(&[
            ("access_token", access_token),
            ("fields", ad_fields),
            ("limit", "50"),
        ])
        .send()
        .await
        .context("không gọi được /ads")?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("fetch ads lỗi {}: {}", status, body);
    }
    let ads: serde_json::Value = serde_json::from_str(&body)
        .with_context(|| format!("parse ads: {body}"))?;

    if let serde_json::Value::Object(ref mut m) = campaign {
        m.insert("adsets".to_string(), adsets);
        m.insert("ads".to_string(), ads);
    }

    Ok(campaign)
}

// ============================================================
// POST endpoints — Phase 2 batch create
// ============================================================

#[derive(Deserialize)]
struct CreateResponse {
    id: String,
}

/// Helper chung — POST với x-www-form-urlencoded body từ JSON object.
/// FB Marketing API chấp nhận form-urlencoded; arrays/objects encode JSON
/// stringified vào field.
async fn post_create(
    client: &Client,
    url: &str,
    token: &str,
    payload: &serde_json::Value,
    debug_label: &str,
) -> Result<String> {
    let mut form: Vec<(String, String)> =
        vec![("access_token".to_string(), token.to_string())];

    if let Some(obj) = payload.as_object() {
        for (k, v) in obj {
            let val = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => continue,
                _ => v.to_string(),
            };
            form.push((k.clone(), val));
        }
    }

    let resp = client
        .post(url)
        .form(&form)
        .send()
        .await
        .with_context(|| format!("không gọi được {debug_label}"))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("{} lỗi {}: {}", debug_label, status, body);
    }
    let parsed: CreateResponse = serde_json::from_str(&body)
        .with_context(|| format!("parse {debug_label}: {body}"))?;
    Ok(parsed.id)
}

/// Tạo campaign mới — trả về campaign_id.
pub async fn create_campaign(
    client: &Client,
    account_id: &str,
    token: &str,
    payload: &serde_json::Value,
) -> Result<String> {
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{account_id}/campaigns");
    post_create(client, &url, token, payload, "create_campaign").await
}

/// Tạo ad set mới — trả về adset_id.
pub async fn create_adset(
    client: &Client,
    account_id: &str,
    token: &str,
    payload: &serde_json::Value,
) -> Result<String> {
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{account_id}/adsets");
    post_create(client, &url, token, payload, "create_adset").await
}

/// Tạo ad creative mới — trả về creative_id.
pub async fn create_creative(
    client: &Client,
    account_id: &str,
    token: &str,
    payload: &serde_json::Value,
) -> Result<String> {
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{account_id}/adcreatives");
    post_create(client, &url, token, payload, "create_creative").await
}

/// Tạo ad mới (link creative + adset) — trả về ad_id.
pub async fn create_ad(
    client: &Client,
    account_id: &str,
    token: &str,
    payload: &serde_json::Value,
) -> Result<String> {
    let url = format!("{GRAPH_BASE}/{API_VERSION}/{account_id}/ads");
    post_create(client, &url, token, payload, "create_ad").await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ad_accounts_response() {
        let body = r#"{"data":[
            {"id":"act_111","account_id":"111","name":"Acc A","currency":"VND","timezone_name":"Asia/Ho_Chi_Minh"},
            {"id":"act_222","account_id":"222","name":"Acc B","currency":"USD","timezone_name":"America/Los_Angeles"}
        ]}"#;
        let parsed: AdAccountsResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.data.len(), 2);
        assert_eq!(parsed.data[0].id, "act_111");
        assert_eq!(parsed.data[1].currency.as_deref(), Some("USD"));
    }

    #[test]
    fn parse_campaigns_response() {
        let body = r#"{"data":[
            {"id":"cam_1","name":"Shopee Test","objective":"OUTCOME_TRAFFIC","status":"PAUSED","created_time":"2025-06-01T00:00:00Z"}
        ]}"#;
        let parsed: CampaignsResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.data.len(), 1);
        assert_eq!(parsed.data[0].objective.as_deref(), Some("OUTCOME_TRAFFIC"));
    }
}
