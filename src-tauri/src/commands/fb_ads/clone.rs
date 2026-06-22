//! Build payload campaign/adset/creative/ad từ template snapshot + row data.
//!
//! Template snapshot là 1 JSON object lấy từ Marketing API:
//! ```json
//! {
//!   "id": "cam_xxx",
//!   "objective": "OUTCOME_TRAFFIC",
//!   "special_ad_categories": [],
//!   "buying_type": "AUCTION",
//!   "adsets": { "data": [ { "targeting": {...}, ... } ] },
//!   "ads": { "data": [ { "creative": { "object_story_spec": {...} } } ] }
//! }
//! ```
//!
//! Clone strategy: lấy adset/ad đầu tiên làm khuôn, áp dụng variation per row.

use anyhow::{Context, Result};
use serde_json::{json, Value};

/// Context extract sẵn từ template — page_id + base_link để mỗi job dùng lại.
#[derive(Debug, Clone)]
pub struct CloneContext {
    pub objective: String,
    pub special_ad_categories: Value,
    pub buying_type: Option<String>,
    pub page_id: String,
    pub base_link: String,
    pub adset_template: Value,
    pub creative_template: Value,
}

impl CloneContext {
    /// Parse snapshot JSON → extract fields cần để clone.
    pub fn from_snapshot(snapshot: &Value) -> Result<Self> {
        let objective = snapshot
            .get("objective")
            .and_then(|v| v.as_str())
            .unwrap_or("OUTCOME_TRAFFIC")
            .to_string();

        let special_ad_categories = snapshot
            .get("special_ad_categories")
            .cloned()
            .unwrap_or_else(|| json!([]));

        let buying_type = snapshot
            .get("buying_type")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let first_adset = snapshot
            .get("adsets")
            .and_then(|v| v.get("data"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .cloned()
            .context("template không có ad set nào — cần ít nhất 1 adset")?;

        let first_ad = snapshot
            .get("ads")
            .and_then(|v| v.get("data"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .context("template không có ad nào — cần ít nhất 1 ad")?;

        let creative_template = first_ad
            .get("creative")
            .cloned()
            .context("ad trong template không có creative")?;

        // page_id có thể nằm ở nhiều vị trí — thử từng cái.
        let page_id = creative_template
            .get("object_story_spec")
            .and_then(|s| s.get("page_id"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                creative_template
                    .get("object_story_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.split_once('_').map(|(p, _)| p))
            })
            .or_else(|| {
                creative_template
                    .get("effective_object_story_id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.split_once('_').map(|(p, _)| p))
            })
            .context("không extract được page_id từ template creative")?
            .to_string();

        // Base link: ưu tiên call_to_action.value.link trong video_data,
        // fallback link_url của creative.
        let base_link = creative_template
            .get("object_story_spec")
            .and_then(|s| s.get("video_data"))
            .and_then(|v| v.get("call_to_action"))
            .and_then(|c| c.get("value"))
            .and_then(|v| v.get("link"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                creative_template
                    .get("object_story_spec")
                    .and_then(|s| s.get("link_data"))
                    .and_then(|l| l.get("link"))
                    .and_then(|v| v.as_str())
            })
            .or_else(|| {
                creative_template
                    .get("link_url")
                    .and_then(|v| v.as_str())
            })
            .context(
                "không extract được link từ template creative — cần camp mẫu có link Shopee",
            )?
            .to_string();

        Ok(CloneContext {
            objective,
            special_ad_categories,
            buying_type,
            page_id,
            base_link,
            adset_template: first_adset,
            creative_template,
        })
    }
}

/// Apply sub_id mới vào link — nếu có sẵn `sub_id=xxx` thì replace, nếu
/// không thì append vào query string.
pub fn apply_sub_id(base_link: &str, sub_id: &str) -> String {
    if sub_id.is_empty() {
        return base_link.to_string();
    }
    if let Some((before, after)) = base_link.split_once("sub_id=") {
        let end_pos = after.find(['&', '#']).unwrap_or(after.len());
        let tail = &after[end_pos..];
        format!("{before}sub_id={sub_id}{tail}")
    } else {
        let separator = if base_link.contains('?') { '&' } else { '?' };
        format!("{base_link}{separator}sub_id={sub_id}")
    }
}

/// Build payload tạo Campaign — clone objective + special_ad_categories từ
/// template, status PAUSED.
pub fn build_campaign_payload(ctx: &CloneContext, camp_name: &str) -> Value {
    let mut payload = json!({
        "name": camp_name,
        "objective": ctx.objective,
        "status": "PAUSED",
        "special_ad_categories": ctx.special_ad_categories,
    });
    if let Some(bt) = &ctx.buying_type {
        payload["buying_type"] = json!(bt);
    }
    payload
}

/// Build payload tạo Ad Set — clone targeting/budget/billing từ template
/// adset, link với campaign_id mới.
pub fn build_adset_payload(
    ctx: &CloneContext,
    adset_name: &str,
    campaign_id: &str,
) -> Value {
    let t = &ctx.adset_template;
    let mut payload = json!({
        "name": adset_name,
        "campaign_id": campaign_id,
        "status": "PAUSED",
    });

    for field in [
        "daily_budget",
        "lifetime_budget",
        "bid_strategy",
        "billing_event",
        "optimization_goal",
        "destination_type",
        "start_time",
        "end_time",
        "attribution_spec",
    ] {
        if let Some(v) = t.get(field).filter(|v| !v.is_null()) {
            payload[field] = v.clone();
        }
    }

    // Targeting bắt buộc — nếu thiếu, Adset sẽ fail
    if let Some(targeting) = t.get("targeting").filter(|v| !v.is_null()) {
        payload["targeting"] = targeting.clone();
    } else {
        payload["targeting"] = json!({
            "geo_locations": {"countries": ["VN"]},
        });
    }

    // promoted_object cần cho 1 số objective (conversions); copy nếu có
    if let Some(po) = t.get("promoted_object").filter(|v| !v.is_null()) {
        payload["promoted_object"] = po.clone();
    }

    payload
}

/// Build payload tạo Ad Creative cho video ad — gắn video_id mới + caption +
/// link với sub_id mới.
pub fn build_creative_payload(
    ctx: &CloneContext,
    creative_name: &str,
    video_id: &str,
    caption: Option<&str>,
    sub_id: &str,
) -> Value {
    let link = apply_sub_id(&ctx.base_link, sub_id);

    // Lấy CTA type từ template, fallback SHOP_NOW.
    let cta_type = ctx
        .creative_template
        .get("object_story_spec")
        .and_then(|s| s.get("video_data"))
        .and_then(|v| v.get("call_to_action"))
        .and_then(|c| c.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("SHOP_NOW")
        .to_string();

    let mut video_data = json!({
        "video_id": video_id,
        "call_to_action": {
            "type": cta_type,
            "value": { "link": link }
        }
    });
    if let Some(c) = caption {
        if !c.is_empty() {
            video_data["message"] = json!(c);
        }
    }

    // image_url thumbnail tùy chọn — FB tự generate nếu không có
    if let Some(thumb) = ctx
        .creative_template
        .get("object_story_spec")
        .and_then(|s| s.get("video_data"))
        .and_then(|v| v.get("image_url"))
        .and_then(|v| v.as_str())
    {
        video_data["image_url"] = json!(thumb);
    }

    json!({
        "name": creative_name,
        "object_story_spec": {
            "page_id": ctx.page_id,
            "video_data": video_data,
        }
    })
}

/// Build payload tạo Ad — link creative + adset.
pub fn build_ad_payload(ad_name: &str, adset_id: &str, creative_id: &str) -> Value {
    json!({
        "name": ad_name,
        "adset_id": adset_id,
        "creative": { "creative_id": creative_id },
        "status": "PAUSED",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_snapshot() -> Value {
        json!({
            "id": "cam_999",
            "objective": "OUTCOME_TRAFFIC",
            "special_ad_categories": [],
            "buying_type": "AUCTION",
            "adsets": {
                "data": [{
                    "id": "adset_1",
                    "daily_budget": "50000",
                    "billing_event": "IMPRESSIONS",
                    "optimization_goal": "LINK_CLICKS",
                    "bid_strategy": "LOWEST_COST_WITHOUT_CAP",
                    "targeting": { "geo_locations": { "countries": ["VN"] } }
                }]
            },
            "ads": {
                "data": [{
                    "id": "ad_1",
                    "creative": {
                        "id": "cr_1",
                        "object_story_spec": {
                            "page_id": "12345",
                            "video_data": {
                                "video_id": "vid_old",
                                "call_to_action": {
                                    "type": "SHOP_NOW",
                                    "value": { "link": "https://shopee.vn/p?aff=1&sub_id=old" }
                                }
                            }
                        }
                    }
                }]
            }
        })
    }

    #[test]
    fn extract_context_ok() {
        let snap = fake_snapshot();
        let ctx = CloneContext::from_snapshot(&snap).unwrap();
        assert_eq!(ctx.objective, "OUTCOME_TRAFFIC");
        assert_eq!(ctx.page_id, "12345");
        assert!(ctx.base_link.contains("sub_id=old"));
    }

    #[test]
    fn apply_sub_id_replace() {
        let url = "https://shopee.vn/p?aff=1&sub_id=old&utm=fb";
        let new = apply_sub_id(url, "new123");
        assert_eq!(new, "https://shopee.vn/p?aff=1&sub_id=new123&utm=fb");
    }

    #[test]
    fn apply_sub_id_append_to_query() {
        let url = "https://shopee.vn/p?aff=1";
        let new = apply_sub_id(url, "new123");
        assert_eq!(new, "https://shopee.vn/p?aff=1&sub_id=new123");
    }

    #[test]
    fn apply_sub_id_append_no_query() {
        let url = "https://shopee.vn/p";
        let new = apply_sub_id(url, "new123");
        assert_eq!(new, "https://shopee.vn/p?sub_id=new123");
    }

    #[test]
    fn apply_sub_id_empty_no_change() {
        let url = "https://shopee.vn/p?sub_id=old";
        assert_eq!(apply_sub_id(url, ""), url);
    }

    #[test]
    fn build_campaign_has_paused_status() {
        let ctx = CloneContext::from_snapshot(&fake_snapshot()).unwrap();
        let p = build_campaign_payload(&ctx, "CAM-NEW");
        assert_eq!(p["name"], "CAM-NEW");
        assert_eq!(p["status"], "PAUSED");
        assert_eq!(p["objective"], "OUTCOME_TRAFFIC");
    }

    #[test]
    fn build_adset_includes_targeting() {
        let ctx = CloneContext::from_snapshot(&fake_snapshot()).unwrap();
        let p = build_adset_payload(&ctx, "AS-NEW", "cam_new");
        assert_eq!(p["status"], "PAUSED");
        assert_eq!(p["campaign_id"], "cam_new");
        assert!(p["targeting"].is_object());
        assert_eq!(p["daily_budget"], "50000");
    }

    #[test]
    fn build_creative_swaps_video_and_subid() {
        let ctx = CloneContext::from_snapshot(&fake_snapshot()).unwrap();
        let p = build_creative_payload(
            &ctx,
            "CR-NEW",
            "vid_new",
            Some("caption mới"),
            "fresh-sub",
        );
        let video_data = &p["object_story_spec"]["video_data"];
        assert_eq!(video_data["video_id"], "vid_new");
        assert_eq!(video_data["message"], "caption mới");
        let link = video_data["call_to_action"]["value"]["link"]
            .as_str()
            .unwrap();
        assert!(link.contains("sub_id=fresh-sub"));
        assert!(!link.contains("sub_id=old"));
    }

    #[test]
    fn build_ad_links_creative_and_adset() {
        let p = build_ad_payload("AD-NEW", "as_1", "cr_1");
        assert_eq!(p["adset_id"], "as_1");
        assert_eq!(p["creative"]["creative_id"], "cr_1");
        assert_eq!(p["status"], "PAUSED");
    }
}
