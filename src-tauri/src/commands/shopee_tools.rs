//! Misc Shopee utility commands cho tab "Other".
//!
//! Hiện có:
//! - `find_shopee_aff_id`: follow redirect short link → parse query string
//!   final URL → extract Shopee Affiliate Publisher ID (pattern `an_<digits>`
//!   trong `utm_source` hoặc `mmp_pid`).
//! - `clean_shopee_links`: nhận text block, tìm mọi link Shopee (short +
//!   full), follow redirect → strip toàn bộ tracking params → replace inline
//!   trong text. Parallel resolve để xử lý nhanh khi text có nhiều link.

use futures_util::stream::{self, StreamExt};
use serde::Serialize;
use std::collections::HashMap;

use super::{CmdError, CmdResult};

const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";
/// Tối đa N request HTTP follow redirect chạy song song khi clean batch.
/// 5 là sweet spot — fast enough, không gây nghi ngờ Shopee rate-limit.
const CLEAN_LINKS_CONCURRENCY: usize = 5;

/// Kết quả tra Affiliate ID:
/// - `affId` = Shopee Publisher ID dạng số (None nếu link không có tag affiliate).
/// - `finalUrl` = URL sau khi follow tất cả redirect — hữu ích cho debug.
/// - `source` = tên query param đã extract từ ("utm_source", "mmp_pid", "none").
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShopeeAffIdResult {
    pub aff_id: Option<i64>,
    pub final_url: String,
    pub source: String,
}

/// Extract aff_id từ query string (vd `utm_source=an_17353950011`).
/// Trả `Some((param_name, id))` nếu match, ưu tiên `utm_source` trước
/// `mmp_pid` (utm_source là param chính Shopee dùng từ 2024+).
fn parse_aff_id_from_query(url: &reqwest::Url) -> Option<(&'static str, i64)> {
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    // Ưu tiên utm_source vì stable hơn — mmp_pid đôi khi blank khi link
    // generate qua kênh khác (vd Tiktok in-app browser).
    for preferred in ["utm_source", "mmp_pid"] {
        if let Some((_, v)) = pairs.iter().find(|(k, _)| k == preferred) {
            if let Some(stripped) = v.strip_prefix("an_") {
                if let Ok(id) = stripped.parse::<i64>() {
                    return Some((preferred, id));
                }
            }
        }
    }
    None
}

fn is_shopee_host(host: &str) -> bool {
    let l = host.to_lowercase();
    l == "shope.ee"
        || l == "shp.ee"
        || l.ends_with(".shopee.vn")
        || l == "shopee.vn"
        || l.ends_with(".shopee.com")
        || l == "shopee.com"
        || l.contains(".shopee.")
}

/// Tra Affiliate Publisher ID từ Shopee link (short hoặc full).
///
/// Logic:
/// 1. Validate URL có scheme + host thuộc shopee.*.
/// 2. GET với redirect Policy::limited(5) — short link `shope.ee/xxx` sẽ
///    redirect 1-3 lần về `shopee.vn/<slug>/<shop_id>/<item_id>?...`.
/// 3. Parse query string final URL tìm `utm_source=an_<digits>` hoặc
///    `mmp_pid=an_<digits>`.
/// 4. Trả về số đã strip prefix.
#[tauri::command]
pub async fn find_shopee_aff_id(url: String) -> CmdResult<ShopeeAffIdResult> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(CmdError::msg("URL trống"));
    }

    let parsed = reqwest::Url::parse(trimmed)
        .map_err(|e| CmdError::msg(format!("URL không hợp lệ: {e}")))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| CmdError::msg("URL không có host"))?;
    if !is_shopee_host(host) {
        return Err(CmdError::msg(format!(
            "Không phải link Shopee (host: {host})"
        )));
    }

    let client = reqwest::Client::builder()
        .user_agent(UA)
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(std::time::Duration::from_secs(20))
        .build()?;

    let resp = client
        .get(parsed.clone())
        .send()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi kết nối: {e}")))?;

    let final_url = resp.url().clone();
    // Có thể link có param luôn từ đầu (full link), parse cả URL gốc làm
    // fallback nếu final URL bị Shopee strip params trong 1 số case.
    let (source_name, aff_id) = match parse_aff_id_from_query(&final_url) {
        Some((k, id)) => (k, Some(id)),
        None => match parse_aff_id_from_query(&parsed) {
            Some((k, id)) => (k, Some(id)),
            None => ("none", None),
        },
    };

    Ok(ShopeeAffIdResult {
        aff_id,
        final_url: final_url.to_string(),
        source: source_name.to_string(),
    })
}

// ============================================================
// clean_shopee_links — text-block link cleaner
// ============================================================

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanLinkReplacement {
    pub original: String,
    pub cleaned: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanLinkError {
    pub original: String,
    pub error: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CleanLinksResult {
    pub output_text: String,
    pub replacements: Vec<CleanLinkReplacement>,
    pub errors: Vec<CleanLinkError>,
    pub total_found: usize,
}

/// Tìm tất cả URL Shopee trong text (short + full). Pattern dùng substring
/// host check thay vì hardcoded list để bắt được mọi subdomain Shopee
/// (vn.shp.ee, s.shopee.vn, www.shopee.com.my, ...).
fn extract_shopee_urls(text: &str) -> Vec<String> {
    // Regex permissive: bắt http(s):// + chuỗi non-whitespace có chứa
    // shopee/shp.ee. False positive được lọc lại bằng `is_shopee_host`.
    use regex::Regex;
    static RX: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let rx = RX.get_or_init(|| {
        Regex::new(r#"https?://[^\s<>"'\)\]]+"#).expect("regex compile")
    });
    let mut out = Vec::new();
    for m in rx.find_iter(text) {
        let raw = m.as_str();
        // Trim dấu câu cuối thường gặp khi link nằm trong văn bản tự nhiên
        // (vd "https://shope.ee/abc." → bỏ dấu chấm).
        let cleaned = raw.trim_end_matches(|c: char| {
            matches!(c, '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '}' | '\'' | '"')
        });
        if let Ok(u) = reqwest::Url::parse(cleaned) {
            if let Some(h) = u.host_str() {
                if is_shopee_host(h) {
                    out.push(cleaned.to_string());
                }
            }
        }
    }
    out
}

/// Resolve 1 link Shopee → URL cleaned (path-only). Follow redirect, sau
/// đó clear toàn bộ query + fragment.
async fn resolve_and_clean(
    client: &reqwest::Client,
    url: &str,
) -> Result<String, String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("Lỗi kết nối: {e}"))?;
    let mut final_url = resp.url().clone();
    final_url.set_query(None);
    final_url.set_fragment(None);
    Ok(final_url.to_string())
}

/// Clean tất cả link Shopee trong text. Dedupe các link giống nhau (chỉ
/// resolve 1 lần dù xuất hiện nhiều lần). Trả replacements (unique mapping)
/// + errors riêng để FE hiển thị log.
#[tauri::command]
pub async fn clean_shopee_links(input_text: String) -> CmdResult<CleanLinksResult> {
    let found = extract_shopee_urls(&input_text);
    if found.is_empty() {
        return Ok(CleanLinksResult {
            output_text: input_text,
            replacements: vec![],
            errors: vec![],
            total_found: 0,
        });
    }

    // Dedupe — preserve order of first appearance.
    let mut unique: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for u in &found {
        if seen.insert(u.clone()) {
            unique.push(u.clone());
        }
    }

    let client = reqwest::Client::builder()
        .user_agent(UA)
        .redirect(reqwest::redirect::Policy::limited(5))
        .timeout(std::time::Duration::from_secs(20))
        .build()?;

    // Parallel resolve, bounded concurrency. `stream::iter().buffer_unordered`
    // là idiom đúng — FuturesUnordered tự await ngay khi pop, không có
    // mechanism limit, chạy tất cả cùng lúc nếu collect.
    let results: HashMap<String, Result<String, String>> = stream::iter(unique.clone())
        .map(|url| {
            let client = client.clone();
            async move {
                let r = resolve_and_clean(&client, &url).await;
                (url, r)
            }
        })
        .buffer_unordered(CLEAN_LINKS_CONCURRENCY)
        .collect()
        .await;

    // Build output text với replacement. Iter theo `unique` để giữ stable
    // order trong `replacements` (first-appearance order).
    let mut output = input_text;
    let mut replacements = Vec::new();
    let mut errors = Vec::new();
    for original in &unique {
        match results.get(original) {
            Some(Ok(cleaned)) => {
                // Chỉ replace khi cleaned thực sự KHÁC original (tránh
                // hiện "đổi" trong UI khi link đã clean sẵn).
                if cleaned != original {
                    output = output.replace(original, cleaned);
                }
                replacements.push(CleanLinkReplacement {
                    original: original.clone(),
                    cleaned: cleaned.clone(),
                });
            }
            Some(Err(e)) => {
                errors.push(CleanLinkError {
                    original: original.clone(),
                    error: e.clone(),
                });
            }
            None => {} // không xảy ra
        }
    }

    Ok(CleanLinksResult {
        output_text: output,
        replacements,
        errors,
        total_found: found.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(u: &str) -> Option<(&'static str, i64)> {
        let url = reqwest::Url::parse(u).unwrap();
        parse_aff_id_from_query(&url)
    }

    #[test]
    fn extract_from_utm_source() {
        let r = parse("https://shopee.vn/foo/1/2?utm_source=an_17353950011&utm_medium=affiliates");
        assert_eq!(r, Some(("utm_source", 17353950011)));
    }

    #[test]
    fn extract_from_mmp_pid_when_utm_missing() {
        let r = parse("https://shopee.vn/foo/1/2?mmp_pid=an_99999&other=x");
        assert_eq!(r, Some(("mmp_pid", 99999)));
    }

    #[test]
    fn prefers_utm_over_mmp() {
        let r = parse(
            "https://shopee.vn/foo/1/2?mmp_pid=an_111&utm_source=an_222",
        );
        assert_eq!(r, Some(("utm_source", 222)));
    }

    #[test]
    fn returns_none_when_no_an_prefix() {
        let r = parse("https://shopee.vn/foo/1/2?utm_source=facebook");
        assert_eq!(r, None);
    }

    #[test]
    fn returns_none_when_no_params() {
        let r = parse("https://shopee.vn/foo/1/2");
        assert_eq!(r, None);
    }

    #[test]
    fn host_check_accepts_short_and_full() {
        assert!(is_shopee_host("shope.ee"));
        assert!(is_shopee_host("shp.ee"));
        assert!(is_shopee_host("shopee.vn"));
        assert!(is_shopee_host("www.shopee.vn"));
        assert!(is_shopee_host("shopee.com"));
        assert!(is_shopee_host("s.shopee.vn"));
    }

    #[test]
    fn host_check_rejects_non_shopee() {
        assert!(!is_shopee_host("google.com"));
        assert!(!is_shopee_host("tiktok.com"));
        assert!(!is_shopee_host("example.com"));
    }

    #[test]
    fn extract_finds_short_and_full_links() {
        let text = "Xem nhanh https://shope.ee/1AsWlcMsQC và \
                    https://s.shopee.vn/3VQO0nU7Tm, https://shopee.vn/foo/1/2?utm=x.";
        let urls = extract_shopee_urls(text);
        assert_eq!(urls.len(), 3);
        assert!(urls.iter().any(|u| u == "https://shope.ee/1AsWlcMsQC"));
        assert!(urls.iter().any(|u| u == "https://s.shopee.vn/3VQO0nU7Tm"));
        assert!(urls.iter().any(|u| u == "https://shopee.vn/foo/1/2?utm=x"));
    }

    #[test]
    fn extract_ignores_non_shopee() {
        let text = "https://google.com/foo https://tiktok.com/bar";
        let urls = extract_shopee_urls(text);
        assert!(urls.is_empty());
    }

    #[test]
    fn extract_trims_trailing_punctuation() {
        let text = "Link: https://shope.ee/abc. Cảm ơn!";
        let urls = extract_shopee_urls(text);
        assert_eq!(urls, vec!["https://shope.ee/abc"]);
    }

    #[test]
    fn extract_dedupes_within_text() {
        // Cùng link xuất hiện 2 lần — extract trả 2, dedupe ở command layer.
        let text = "A https://shope.ee/x B https://shope.ee/x";
        let urls = extract_shopee_urls(text);
        assert_eq!(urls.len(), 2);
    }
}
