//! Shopee Affiliate Smart Link — convert link Shopee → affiliate link qua
//! endpoint nội bộ của dashboard (`affiliate.shopee.vn/api/v3/gql`).
//!
//! Flow:
//! 1. `shopee_aff_open_login_window` — mở 1 webview riêng trỏ tới
//!    `affiliate.shopee.vn`. User login bằng tay (giống như mở Chrome).
//! 2. `shopee_aff_capture_cookies` — sau khi user login xong, gọi
//!    `cookies_for_url` của webview để lấy cả HttpOnly cookies, serialize
//!    JSON vào `app_settings` key `shopee_affiliate.cookies`.
//! 3. `shopee_aff_convert_links` — build reqwest với header `Cookie: ...`
//!    từ cookies đã lưu, POST GraphQL `batchCustomLink`.
//!
//! Endpoint nội bộ này dùng auth bằng session cookie — KHÔNG cần appid/secret
//! của Open API Platform (tránh được lỗi 10035).

use rusqlite::params;
use serde::{Deserialize, Serialize};
use tauri::{
    AppHandle, Manager, State, WebviewUrl, WebviewWindowBuilder, WindowEvent,
};

use crate::db::{now_rfc3339_z, DbState};

use super::{CmdError, CmdResult};

const LOGIN_WINDOW_LABEL: &str = "shopee-affiliate-login";
const LOGIN_URL: &str = "https://affiliate.shopee.vn";
const GQL_ENDPOINT: &str = "https://affiliate.shopee.vn/api/v3/gql?q=batchCustomLink";
const COOKIE_SETTING_KEY: &str = "shopee_affiliate.cookies";
const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

const GQL_QUERY: &str = "query batchGetCustomLink($linkParams: [CustomLinkParam!], $sourceCaller: SourceCaller){\n  batchCustomLink(linkParams: $linkParams, sourceCaller: $sourceCaller){\n    shortLink\n    longLink\n    failCode\n  }\n}";

/// Một cookie trong store nội bộ — chỉ giữ field cần để gắn lại Cookie header
/// + để FE hiển thị (name + domain). KHÔNG lưu expires/path vì server đã set
/// domain rồi, mỗi request cứ gửi đầy đủ name=value là dashboard accept.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCookie {
    pub name: String,
    pub value: String,
    #[serde(default)]
    pub domain: String,
}

/// Payload lưu vào app_settings — list cookies + thời điểm capture để UI
/// hiển thị "đã đăng nhập X phút trước".
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CookieStorePayload {
    captured_at: String,
    cookies: Vec<StoredCookie>,
}

/// Trạng thái cookies cho UI: có cookies hay chưa, lúc nào capture, mấy cookie.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShopeeAffStatus {
    pub has_cookies: bool,
    pub captured_at: Option<String>,
    pub cookie_count: u32,
    pub login_window_open: bool,
}

/// 1 link sau convert. `failCode == 0` là OK, khác là lỗi (Shopee không
/// document cụ thể, FE hiển thị raw).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkResult {
    pub original_link: String,
    pub short_link: Option<String>,
    pub long_link: Option<String>,
    pub fail_code: Option<i64>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GqlResponse {
    data: Option<GqlData>,
    #[serde(default)]
    errors: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GqlData {
    #[serde(rename = "batchCustomLink")]
    batch_custom_link: Option<Vec<GqlLinkItem>>,
}

#[derive(Debug, Deserialize)]
struct GqlLinkItem {
    #[serde(rename = "shortLink")]
    short_link: Option<String>,
    #[serde(rename = "longLink")]
    long_link: Option<String>,
    #[serde(rename = "failCode")]
    fail_code: Option<i64>,
}

// ============ DB helpers ============

fn read_cookie_payload(state: &DbState) -> CmdResult<Option<CookieStorePayload>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM app_settings WHERE key = ?",
            [COOKIE_SETTING_KEY],
            |r| r.get(0),
        )
        .ok();
    drop(conn);
    match value {
        Some(s) if !s.is_empty() => {
            let p: CookieStorePayload = serde_json::from_str(&s)?;
            Ok(Some(p))
        }
        _ => Ok(None),
    }
}

fn write_cookie_payload(state: &DbState, payload: &CookieStorePayload) -> CmdResult<()> {
    let json = serde_json::to_string(payload)?;
    let now = now_rfc3339_z();
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "INSERT INTO app_settings(key, value, updated_at) VALUES(?, ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
        params![COOKIE_SETTING_KEY, json, now],
    )?;
    Ok(())
}

fn delete_cookie_payload(state: &DbState) -> CmdResult<()> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "DELETE FROM app_settings WHERE key = ?",
        [COOKIE_SETTING_KEY],
    )?;
    Ok(())
}

// ============ Commands ============

/// Mở (hoặc focus) cửa sổ webview trỏ tới `affiliate.shopee.vn` để user
/// login. Cookies sẽ persist trong WebView2 user data dir → mở lại lần sau
/// đã login sẵn (cho tới khi cookie hết hạn).
#[tauri::command]
pub fn shopee_aff_open_login_window(app: AppHandle) -> CmdResult<()> {
    if let Some(win) = app.get_webview_window(LOGIN_WINDOW_LABEL) {
        let _ = win.show();
        let _ = win.unminimize();
        let _ = win.set_focus();
        return Ok(());
    }

    let url = LOGIN_URL
        .parse()
        .map_err(|e| CmdError::msg(format!("invalid login URL: {e}")))?;

    let win = WebviewWindowBuilder::new(&app, LOGIN_WINDOW_LABEL, WebviewUrl::External(url))
        .title("Đăng nhập Shopee Affiliate")
        .inner_size(1100.0, 760.0)
        .user_agent(UA)
        .build()
        .map_err(|e| CmdError::msg(format!("không mở được webview: {e}")))?;

    // Nút X trên title bar: force-destroy thay vì close() để page Shopee
    // không chặn được bằng beforeunload/JS handler nào. prevent_close() +
    // destroy() đảm bảo window biến mất ngay cả khi WebView2 còn pending op.
    let win_for_event = win.clone();
    win.on_window_event(move |event| {
        if let WindowEvent::CloseRequested { api, .. } = event {
            api.prevent_close();
            let _ = win_for_event.destroy();
        }
    });
    Ok(())
}

/// Đọc cookies từ webview login, lưu vào DB. User gọi sau khi login xong.
/// Yêu cầu cửa sổ login đang mở (cookies chỉ có ở WebView2 instance đang
/// active). Tauri 2 `cookies_for_url` trả cả HttpOnly + Secure cookies.
#[tauri::command]
pub async fn shopee_aff_capture_cookies(
    app: AppHandle,
    db: State<'_, DbState>,
) -> CmdResult<ShopeeAffStatus> {
    let win = app
        .get_webview_window(LOGIN_WINDOW_LABEL)
        .ok_or_else(|| CmdError::msg("Chưa mở cửa sổ đăng nhập"))?;

    let url = LOGIN_URL
        .parse()
        .map_err(|e| CmdError::msg(format!("invalid login URL: {e}")))?;

    let raw_cookies = win
        .cookies_for_url(url)
        .map_err(|e| CmdError::msg(format!("đọc cookies thất bại: {e}")))?;

    let cookies: Vec<StoredCookie> = raw_cookies
        .into_iter()
        .filter_map(|c| {
            let name = c.name().to_string();
            let value = c.value().to_string();
            if name.is_empty() {
                None
            } else {
                Some(StoredCookie {
                    name,
                    value,
                    domain: c.domain().unwrap_or_default().to_string(),
                })
            }
        })
        .collect();

    if cookies.is_empty() {
        return Err(CmdError::msg(
            "Không lấy được cookie nào — đảm bảo bạn đã login xong trong cửa sổ",
        ));
    }

    let payload = CookieStorePayload {
        captured_at: now_rfc3339_z(),
        cookies,
    };
    write_cookie_payload(&db, &payload)?;

    Ok(ShopeeAffStatus {
        has_cookies: true,
        captured_at: Some(payload.captured_at),
        cookie_count: payload.cookies.len() as u32,
        login_window_open: true,
    })
}

#[tauri::command]
pub fn shopee_aff_get_status(
    app: AppHandle,
    db: State<'_, DbState>,
) -> CmdResult<ShopeeAffStatus> {
    let payload = read_cookie_payload(&db)?;
    let login_window_open = app.get_webview_window(LOGIN_WINDOW_LABEL).is_some();
    Ok(match payload {
        Some(p) => ShopeeAffStatus {
            has_cookies: !p.cookies.is_empty(),
            captured_at: Some(p.captured_at),
            cookie_count: p.cookies.len() as u32,
            login_window_open,
        },
        None => ShopeeAffStatus {
            has_cookies: false,
            captured_at: None,
            cookie_count: 0,
            login_window_open,
        },
    })
}

#[tauri::command]
pub fn shopee_aff_clear_cookies(db: State<'_, DbState>) -> CmdResult<()> {
    delete_cookie_payload(&db)
}

/// Force-destroy login webview. Dùng `destroy()` thay vì `close()` vì trang
/// `affiliate.shopee.vn` có thể đăng ký `beforeunload` handler (form chưa
/// submit, session checker...) khiến `close()` bị silently prevent → window
/// kẹt mở. `destroy()` bypass mọi handler JS và đóng cứng cửa sổ.
#[tauri::command]
pub fn shopee_aff_close_login_window(app: AppHandle) -> CmdResult<()> {
    if let Some(win) = app.get_webview_window(LOGIN_WINDOW_LABEL) {
        let _ = win.destroy();
    }
    Ok(())
}

/// Convert nhiều link Shopee → affiliate links trong 1 GraphQL call.
/// `sub_ids` tối đa 5 — extra bị trim. Mỗi result giữ original_link để FE map
/// lại đúng dòng dù Shopee trả về theo thứ tự khác.
#[tauri::command]
pub async fn shopee_aff_convert_links(
    db: State<'_, DbState>,
    links: Vec<String>,
    sub_ids: Vec<String>,
) -> CmdResult<Vec<LinkResult>> {
    let payload = read_cookie_payload(&db)?
        .ok_or_else(|| CmdError::msg("Chưa lưu cookies — vui lòng đăng nhập trước"))?;
    if payload.cookies.is_empty() {
        return Err(CmdError::msg("Cookies trống — đăng nhập lại"));
    }

    let unique_links: Vec<String> = {
        let mut seen = std::collections::HashSet::new();
        links
            .into_iter()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .filter(|s| seen.insert(s.clone()))
            .collect()
    };

    if unique_links.is_empty() {
        return Err(CmdError::msg("Không có link nào để convert"));
    }

    let cleaned_sub_ids: Vec<String> = sub_ids
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .take(5)
        .collect();

    // Build GraphQL variables theo đúng schema BE: mỗi link có
    // advancedLinkParams.subId1..5 (key chỉ có khi user cung cấp value).
    let link_params: Vec<serde_json::Value> = unique_links
        .iter()
        .map(|link| {
            let mut advanced = serde_json::Map::new();
            for (i, sid) in cleaned_sub_ids.iter().enumerate() {
                advanced.insert(
                    format!("subId{}", i + 1),
                    serde_json::Value::String(sid.clone()),
                );
            }
            serde_json::json!({
                "originalLink": link,
                "advancedLinkParams": advanced,
            })
        })
        .collect();

    let body = serde_json::json!({
        "operationName": "batchGetCustomLink",
        "query": GQL_QUERY,
        "variables": {
            "linkParams": link_params,
            "sourceCaller": "CUSTOM_LINK_CALLER",
        }
    });

    let cookie_header = payload
        .cookies
        .iter()
        .map(|c| format!("{}={}", c.name, c.value))
        .collect::<Vec<_>>()
        .join("; ");

    let client = reqwest::Client::builder()
        .user_agent(UA)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(CmdError::from)?;

    let resp = client
        .post(GQL_ENDPOINT)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("Origin", "https://affiliate.shopee.vn")
        .header("Referer", "https://affiliate.shopee.vn/")
        .header("Cookie", cookie_header)
        .json(&body)
        .send()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi kết nối: {e}")))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi đọc response: {e}")))?;

    if !status.is_success() {
        let preview = &text[..text.len().min(300)];
        return Err(CmdError::msg(format!(
            "Shopee trả HTTP {status} — có thể cookie hết hạn. {preview}"
        )));
    }

    let parsed: GqlResponse = serde_json::from_str(&text).map_err(|e| {
        let preview = &text[..text.len().min(300)];
        CmdError::msg(format!("Phản hồi không hợp lệ ({e}): {preview}"))
    })?;

    if let serde_json::Value::Array(arr) = &parsed.errors {
        if !arr.is_empty() {
            let msg = arr
                .iter()
                .filter_map(|v| v.get("message").and_then(|m| m.as_str()))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(CmdError::msg(format!("GraphQL error: {msg}")));
        }
    }

    let items = parsed
        .data
        .and_then(|d| d.batch_custom_link)
        .unwrap_or_default();

    // Shopee trả results theo đúng thứ tự linkParams gửi đi (theo extension
    // reference). Zip 1-1.
    let results = unique_links
        .into_iter()
        .enumerate()
        .map(|(idx, link)| {
            let item = items.get(idx);
            match item {
                Some(it) if it.fail_code == Some(0) && it.short_link.is_some() => LinkResult {
                    original_link: link,
                    short_link: it.short_link.clone(),
                    long_link: it.long_link.clone(),
                    fail_code: it.fail_code,
                    error: None,
                },
                Some(it) => LinkResult {
                    original_link: link,
                    short_link: it.short_link.clone(),
                    long_link: it.long_link.clone(),
                    fail_code: it.fail_code,
                    error: Some(format!(
                        "failCode={}",
                        it.fail_code
                            .map(|c| c.to_string())
                            .unwrap_or_else(|| "?".into())
                    )),
                },
                None => LinkResult {
                    original_link: link,
                    short_link: None,
                    long_link: None,
                    fail_code: None,
                    error: Some("Shopee không trả kết quả cho link này".into()),
                },
            }
        })
        .collect();

    Ok(results)
}
