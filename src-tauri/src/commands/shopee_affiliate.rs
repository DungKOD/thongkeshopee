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
use std::sync::{Arc, Mutex};
use tauri::{
    AppHandle, Listener, Manager, State, WebviewUrl, WebviewWindowBuilder, WindowEvent,
};

use crate::db::{now_rfc3339_z, DbState};

use super::{CmdError, CmdResult};

const LOGIN_WINDOW_LABEL: &str = "shopee-affiliate-login";
const LOGIN_URL: &str = "https://affiliate.shopee.vn/offer/custom_link";
const GQL_ENDPOINT: &str = "https://affiliate.shopee.vn/api/v3/gql?q=batchCustomLink";
const COOKIE_SETTING_KEY: &str = "shopee_affiliate.cookies";
/// UA Edge thật (có đuôi `Edg/...`) — Chrome thuần dễ bị Shopee flag là
/// WebView2/automation. UA này match Edge thật trên Windows 11.
const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0";

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

    let mut builder = WebviewWindowBuilder::new(&app, LOGIN_WINDOW_LABEL, WebviewUrl::External(url))
        .title("Đăng nhập Shopee Affiliate")
        .inner_size(1280.0, 820.0)
        .user_agent(UA)
        .center()
        .focused(true)
        .accept_first_mouse(true);
    // Enable DevTools (F12) trong dev build hoặc bất cứ khi nào cần debug.
    // User mở F12 trong webview để xem Network/Console khi gặp trang trắng.
    #[cfg(debug_assertions)]
    {
        builder = builder.devtools(true);
    }
    let win = builder
        .build()
        .map_err(|e| CmdError::msg(format!("không mở được webview: {e}")))?;

    // Nút X trên title bar: force-destroy thay vì close() để page Shopee
    // không chặn được bằng beforeunload/JS handler nào. DESTROY SPAWN trong
    // tokio task để không block event loop nếu WebView2 process hung — khi
    // trang load fail (vd Shopee return empty + JS error), destroy() trực
    // tiếp có thể bị treo. Spawn riêng đảm bảo event handler return ngay.
    let win_for_event = win.clone();
    win.on_window_event(move |event| {
        if let WindowEvent::CloseRequested { api, .. } = event {
            api.prevent_close();
            let w = win_for_event.clone();
            tauri::async_runtime::spawn(async move {
                let _ = w.destroy();
            });
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
///
/// SPAWN destroy trong task riêng để command trả về ngay — nếu WebView2
/// process hung (vd trang trắng do Shopee block), destroy() có thể block
/// vài giây. UI cần feedback nhanh để user biết đã trigger close.
#[tauri::command]
pub fn shopee_aff_close_login_window(app: AppHandle) -> CmdResult<()> {
    if let Some(win) = app.get_webview_window(LOGIN_WINDOW_LABEL) {
        tauri::async_runtime::spawn(async move {
            let _ = win.destroy();
        });
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

// ============================================================
// v2: paste cookies + proxy (no webview login, no DB persist)
// ============================================================
//
// Mục đích: cho phép user dán cookies + proxy thẳng vào UI (không cần login
// qua webview, không cần GemLogin/AdsPower) → convert link standalone.
//
// Khác `shopee_aff_convert_links`:
// - KHÔNG đọc DB — input toàn bộ từ args
// - HỖ TRỢ proxy per-call (format `host:port:user:pass`)
// - Parser cookies linh hoạt: chấp nhận cả JSON Cookie-Editor lẫn raw header

/// Output của parse cookies: vừa header string vừa extract được csrftoken
/// (Shopee API yêu cầu header `X-CSRFToken` match cookie `csrftoken` cho
/// một số endpoint anti-fraud). Khi không có cookie tên `csrftoken`, fallback
/// sang `SPC_T_ID` (đôi khi dashboard cũ dùng cái này).
struct ParsedCookies {
    header: String,
    csrf_token: Option<String>,
}

/// Parse cookies từ string user dán.
/// Format hỗ trợ:
///   1. JSON array (Cookie-Editor export): `[{"name":"x","value":"y",...},...]`
///   2. Raw Cookie header: `name1=value1; name2=value2`
/// Trả `Cookie:` header value đã build sẵn. Lỗi nếu không parse được.
fn parse_cookies_input(raw: &str) -> Result<ParsedCookies, CmdError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CmdError::msg("Cookies trống"));
    }

    // Thử JSON array trước nếu start với `[`
    let pairs: Vec<(String, String)> = if trimmed.starts_with('[') {
        #[derive(serde::Deserialize)]
        struct C {
            name: String,
            value: String,
        }
        let arr: Vec<C> = serde_json::from_str(trimmed).map_err(|e| {
            CmdError::msg(format!(
                "Cookies JSON không hợp lệ ({e}). Nếu bạn dán raw 'k=v; k=v', không cần bao bằng [].",
            ))
        })?;
        if arr.is_empty() {
            return Err(CmdError::msg("Cookies JSON là array rỗng"));
        }
        arr.into_iter()
            .filter(|c| !c.name.is_empty())
            .map(|c| (c.name, c.value))
            .collect()
    } else {
        // Raw header — accept "name=value; name=value" hoặc 1 dòng / nhiều dòng
        // (browser dev tools sometimes copy with newlines).
        trimmed
            .replace(['\n', '\r'], ";")
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty() && s.contains('='))
            .filter_map(|s| s.split_once('=').map(|(k, v)| (k.trim().to_string(), v.trim().to_string())))
            .collect()
    };

    if pairs.is_empty() {
        return Err(CmdError::msg(
            "Không parse được cookies. Hỗ trợ JSON array (Cookie-Editor) hoặc raw 'k=v; k=v'.",
        ));
    }

    let header = pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("; ");

    // Extract csrftoken cho header X-CSRFToken. Ưu tiên `csrftoken` (chuẩn
    // Shopee), fallback `SPC_T_ID`.
    let csrf_token = pairs
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("csrftoken"))
        .or_else(|| pairs.iter().find(|(k, _)| k == "SPC_T_ID"))
        .map(|(_, v)| v.clone());

    Ok(ParsedCookies { header, csrf_token })
}

/// Parse proxy format user-friendly `host:port:user:pass` → URL `http://user:pass@host:port`.
/// Cũng accept format chuẩn sẵn `http://...` / `socks5://...` thì giữ nguyên.
/// Trống → None (không dùng proxy).
fn parse_proxy_input(raw: &str) -> Result<Option<String>, CmdError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    // Đã có scheme — giữ nguyên
    if trimmed.starts_with("http://")
        || trimmed.starts_with("https://")
        || trimmed.starts_with("socks5://")
        || trimmed.starts_with("socks5h://")
    {
        return Ok(Some(trimmed.to_string()));
    }
    // Format host:port:user:pass (4 phần) hoặc host:port (2 phần)
    let parts: Vec<&str> = trimmed.split(':').collect();
    let proxy_url = match parts.len() {
        4 => format!(
            "http://{}:{}@{}:{}",
            urlencode(parts[2]),
            urlencode(parts[3]),
            parts[0],
            parts[1]
        ),
        2 => format!("http://{}:{}", parts[0], parts[1]),
        _ => {
            return Err(CmdError::msg(
                "Proxy format không hợp lệ. Dùng 'host:port:user:pass' hoặc 'host:port' hoặc URL đầy đủ.",
            ))
        }
    };
    Ok(Some(proxy_url))
}

/// URL encode 1 segment user/pass cho proxy URL. Tránh bị break nếu user/pass
/// chứa ký tự special (vd `@` hoặc `:`).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvertPasteParams {
    pub cookies: String,
    #[serde(default)]
    pub proxy: String,
    pub links: Vec<String>,
    #[serde(default)]
    pub sub_ids: Vec<String>,
    /// Token anti-fraud Shopee, header `af-ac-enc-sz-token`. Lấy từ
    /// browser DevTools Network tab khi user submit convert link thật.
    /// Format: `<part1>|<part2>|<device_id>|08|3`. CÓ THỂ refresh mỗi
    /// session/request — nếu vẫn fail, capture lại token mới.
    #[serde(default)]
    pub anti_fraud_token: String,
}

/// Convert link affiliate Shopee dùng cookies + proxy do user paste — không
/// cần webview login, không cần DB. Demo standalone cho multi-account flow.
#[tauri::command]
pub async fn shopee_aff_convert_with_paste(
    params: ConvertPasteParams,
) -> CmdResult<Vec<LinkResult>> {
    let parsed_cookies = parse_cookies_input(&params.cookies)?;
    let proxy_opt = parse_proxy_input(&params.proxy)?;

    let unique_links: Vec<String> = {
        let mut seen = std::collections::HashSet::new();
        params
            .links
            .into_iter()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .filter(|s| seen.insert(s.clone()))
            .collect()
    };
    if unique_links.is_empty() {
        return Err(CmdError::msg("Không có link nào để convert"));
    }

    let cleaned_sub_ids: Vec<String> = params
        .sub_ids
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .take(5)
        .collect();

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

    // Cookie jar persist Set-Cookie giữa warm-up GET và POST chính.
    // Anti-fraud của Shopee có thể refresh _sapid/SPC_SI khi user "navigate
    // dashboard" → cookies sau warm-up "tươi" hơn = pass anti-fraud.
    let mut builder = reqwest::Client::builder()
        .user_agent(UA)
        .cookie_store(true)
        .timeout(std::time::Duration::from_secs(30));
    if let Some(proxy_url) = &proxy_opt {
        let proxy = reqwest::Proxy::all(proxy_url)
            .map_err(|e| CmdError::msg(format!("Proxy URL không hợp lệ: {e}")))?;
        builder = builder.proxy(proxy);
    }
    let client = builder.build().map_err(CmdError::from)?;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BƯỚC 1: WARM-UP NAVIGATION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Mục đích: trông như user vừa mở trang dashboard. Shopee server
    // sẽ refresh anti-fraud cookies (_sapid, SPC_SI, ...) vào jar
    // qua Set-Cookie headers. Best-effort — fail không fatal vì cookies
    // gốc vẫn còn dùng được, chỉ giảm tỉ lệ pass anti-fraud.
    let warmup_headers = [
        ("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Accept-Language", "vi-VN,vi;q=0.9,en;q=0.8"),
        ("Sec-Ch-Ua", r#""Chromium";v="125", "Not.A/Brand";v="24""#),
        ("Sec-Ch-Ua-Mobile", "?0"),
        ("Sec-Ch-Ua-Platform", "\"Windows\""),
        ("Sec-Fetch-Dest", "document"),
        ("Sec-Fetch-Mode", "navigate"),
        ("Sec-Fetch-Site", "same-origin"),
        ("Sec-Fetch-User", "?1"),
        ("Upgrade-Insecure-Requests", "1"),
    ];
    let mut warmup_req = client
        .get("https://affiliate.shopee.vn/offer/custom_link")
        .header("Cookie", &parsed_cookies.header);
    for (k, v) in warmup_headers {
        warmup_req = warmup_req.header(k, v);
    }
    let _ = warmup_req.send().await; // ignore lỗi — không fatal

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BƯỚC 2: POST GraphQL convert
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // Headers giả lập browser chuẩn — Shopee anti-fraud check 1 số signal
    // cơ bản (Origin, Referer, X-Requested-With, X-CSRFToken, language).
    // Càng giống browser request càng ít khả năng bị `error: 90309999`.
    let mut req = client
        .post(GQL_ENDPOINT)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json")
        .header("Accept-Language", "vi-VN,vi;q=0.9,en;q=0.8")
        .header("Origin", "https://affiliate.shopee.vn")
        .header("Referer", "https://affiliate.shopee.vn/offer/custom_link")
        .header("X-Requested-With", "XMLHttpRequest")
        .header("X-Api-Source", "pc")
        .header("X-Shopee-Language", "vi")
        .header("Sec-Fetch-Dest", "empty")
        .header("Sec-Fetch-Mode", "cors")
        .header("Sec-Fetch-Site", "same-origin")
        .header("Cookie", &parsed_cookies.header);
    if let Some(csrf) = &parsed_cookies.csrf_token {
        req = req.header("X-CSRFToken", csrf);
    }
    // af-ac-enc-sz-token: token anti-fraud Shopee, derive từ device fingerprint
    // + có thể có timestamp encrypted. Bắt buộc cho 1 số request — Shopee
    // reject với error 90309999 khi thiếu.
    let token_trim = params.anti_fraud_token.trim();
    if !token_trim.is_empty() {
        req = req.header("af-ac-enc-sz-token", token_trim);
    }

    let resp = req
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            let extra = if proxy_opt.is_some() {
                " (kiểm tra proxy có hoạt động không)"
            } else {
                ""
            };
            CmdError::msg(format!("Lỗi kết nối: {e}{extra}"))
        })?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi đọc response: {e}")))?;

    if !status.is_success() {
        // Shopee đôi khi trả JSON có `is_login`/`error` ngay cả khi status
        // != 200. Parse structured để giúp user phân biệt:
        //   - is_login=false → cookie hết hạn thật
        //   - is_login=true + error 9xxxxxxx → anti-fraud reject (cookies OK
        //     nhưng request bị flag), thường do thiếu header browser hoặc
        //     bị IP/network nghi ngờ.
        if let Ok(structured) = serde_json::from_str::<serde_json::Value>(&text) {
            let is_login = structured.get("is_login").and_then(|v| v.as_bool());
            let err_code = structured.get("error").and_then(|v| v.as_i64());
            if is_login == Some(true) && err_code.is_some() {
                let code = err_code.unwrap();
                let tracking = structured
                    .get("tracking_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("-");
                return Err(CmdError::msg(format!(
                    "Shopee anti-fraud reject (HTTP {status} · code {code}) — cookies CÒN HẠN (is_login=true). \
                    Nguyên nhân thường gặp: (1) thiếu cookie csrftoken HttpOnly, \
                    (2) IP proxy bị Shopee flag, (3) cookies cũ quá lâu chưa hoạt động. \
                    Cách fix: vào affiliate.shopee.vn trên browser → vào trang Tạo link → export cookies LẦN NỮA → dán lại. \
                    tracking_id={tracking}"
                )));
            }
            if is_login == Some(false) {
                return Err(CmdError::msg(format!(
                    "Cookies HẾT HẠN (is_login=false, HTTP {status}). Login lại Shopee và export cookies mới."
                )));
            }
        }
        let preview = &text[..text.len().min(300)];
        return Err(CmdError::msg(format!(
            "Shopee trả HTTP {status}: {preview}"
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

    Ok(unique_links
        .into_iter()
        .enumerate()
        .map(|(idx, link)| match items.get(idx) {
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
                error: Some("Shopee không trả kết quả".into()),
            },
        })
        .collect())
}

#[cfg(test)]
mod paste_tests {
    use super::*;

    #[test]
    fn parses_raw_cookie_header() {
        let r = parse_cookies_input("SPC_F=abc; SPC_U=xyz").unwrap();
        assert_eq!(r.header, "SPC_F=abc; SPC_U=xyz");
        assert_eq!(r.csrf_token, None); // không có SPC_T_ID / csrftoken
    }

    #[test]
    fn parses_raw_cookie_with_newlines() {
        // DevTools đôi khi paste có \n thay vì ;
        let r = parse_cookies_input("SPC_F=abc\nSPC_U=xyz\nSPC_T_ID=123").unwrap();
        assert_eq!(r.header, "SPC_F=abc; SPC_U=xyz; SPC_T_ID=123");
        assert_eq!(r.csrf_token.as_deref(), Some("123")); // fallback SPC_T_ID
    }

    #[test]
    fn parses_cookie_editor_json() {
        let json = r#"[
            {"name":"SPC_F","value":"abc","domain":".shopee.vn"},
            {"name":"SPC_U","value":"xyz","domain":".shopee.vn"}
        ]"#;
        let r = parse_cookies_input(json).unwrap();
        assert_eq!(r.header, "SPC_F=abc; SPC_U=xyz");
        assert_eq!(r.csrf_token, None);
    }

    #[test]
    fn extracts_csrftoken_when_present() {
        // Ưu tiên `csrftoken` chính thức hơn fallback SPC_T_ID.
        let r = parse_cookies_input("SPC_T_ID=fallback; csrftoken=primary").unwrap();
        assert_eq!(r.csrf_token.as_deref(), Some("primary"));
    }

    #[test]
    fn rejects_empty() {
        assert!(parse_cookies_input("").is_err());
        assert!(parse_cookies_input("   ").is_err());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_cookies_input("notakv").is_err());
    }

    #[test]
    fn parse_proxy_host_port_user_pass() {
        let r = parse_proxy_input("160.30.22.205:44014:1906gdxgiv:1906gdxgiv").unwrap();
        assert_eq!(
            r,
            Some("http://1906gdxgiv:1906gdxgiv@160.30.22.205:44014".to_string())
        );
    }

    #[test]
    fn parse_proxy_host_port_only() {
        let r = parse_proxy_input("10.0.0.1:8080").unwrap();
        assert_eq!(r, Some("http://10.0.0.1:8080".to_string()));
    }

    #[test]
    fn parse_proxy_full_url_passthrough() {
        let r = parse_proxy_input("socks5://user:pass@h:1080").unwrap();
        assert_eq!(r, Some("socks5://user:pass@h:1080".to_string()));
    }

    #[test]
    fn parse_proxy_empty_returns_none() {
        assert_eq!(parse_proxy_input("").unwrap(), None);
        assert_eq!(parse_proxy_input("   ").unwrap(), None);
    }

    #[test]
    fn parse_proxy_url_encodes_special_chars() {
        // Password chứa `@` phải encode thành %40 để không break URL.
        let r = parse_proxy_input("h:1:u:p@ss").unwrap();
        assert_eq!(r, Some("http://u:p%40ss@h:1".to_string()));
    }

    #[test]
    fn rejects_invalid_proxy_format() {
        // 3 parts không hợp lệ
        assert!(parse_proxy_input("a:b:c").is_err());
    }
}

// ============================================================
// v3 (LV1 SAFEST): Convert qua webview.eval()
// ============================================================
//
// Cách an toàn nhất: chạy fetch() THẲNG trong tab Shopee đã login
// → mọi signal (TLS JA3, anti-fraud headers, cookies HttpOnly, JS context)
// đều y hệt user click chuột thật. Shopee không phân biệt được.
//
// Flow:
//   1. User mở login webview (existing `shopee_aff_open_login_window`)
//      và login bằng tay.
//   2. Gọi `shopee_aff_convert_via_webview` → backend:
//      a. Tạo unique req_id + event name
//      b. Setup `app.listen(event_name)` → oneshot channel
//      c. Build JS script: gọi fetch(GraphQL) + emit kết quả qua event
//      d. `window.eval(script)`
//      e. Await event (timeout 30s)
//      f. Parse + trả LinkResult[]
//
// JS script dùng `__TAURI_INTERNALS__.invoke('plugin:event|emit', ...)`
// để gửi event từ page external (affiliate.shopee.vn) về Rust.

fn build_convert_script(
    event_name: &str,
    body_json: &str,
) -> String {
    // JS template: đóng gói để tránh Shopee CSP can thiệp + handle lỗi đầy đủ.
    // Dùng IIFE async để có await mà không cần module context.
    // Escape `{` `}` của Rust format → `{{` `}}` để giữ literal trong JS.
    format!(
        r#"(async () => {{
  const REQ_BODY = {body_json};
  const emit = (payload) => {{
    try {{
      window.__TAURI_INTERNALS__.invoke('plugin:event|emit', {{
        event: '{event_name}',
        payload: JSON.stringify(payload),
      }});
    }} catch (e) {{
      console.error('Tauri emit failed', e);
    }}
  }};
  try {{
    const res = await fetch('https://affiliate.shopee.vn/api/v3/gql?q=batchCustomLink', {{
      method: 'POST',
      credentials: 'include',
      headers: {{
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'X-Api-Source': 'pc',
        'X-Shopee-Language': 'vi',
      }},
      body: JSON.stringify(REQ_BODY),
    }});
    const text = await res.text();
    let data;
    try {{ data = JSON.parse(text); }} catch (_) {{ data = null; }}
    emit({{ ok: res.ok, status: res.status, data, text }});
  }} catch (e) {{
    emit({{ ok: false, error: String(e && e.message || e) }});
  }}
}})();"#
    )
}

#[derive(Debug, Deserialize)]
struct WebviewConvertResponse {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    data: Option<serde_json::Value>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// Convert links qua webview thật của login window — LV1 an toàn nhất.
///
/// Yêu cầu: login window phải đang mở (đã login xong). Nếu đóng → caller
/// phải mở lại qua `shopee_aff_open_login_window` trước.
#[tauri::command]
pub async fn shopee_aff_convert_via_webview(
    app: AppHandle,
    links: Vec<String>,
    sub_ids: Vec<String>,
) -> CmdResult<Vec<LinkResult>> {
    let window = app
        .get_webview_window(LOGIN_WINDOW_LABEL)
        .ok_or_else(|| {
            CmdError::msg(
                "Cửa sổ login chưa mở. Bấm 'Đăng nhập' trong tab Smart Link trước.",
            )
        })?;

    // Dedupe + clean links + sub_ids — giống flow cũ.
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
    let body_json = serde_json::to_string(&body)
        .map_err(|e| CmdError::msg(format!("Lỗi build GQL body: {e}")))?;

    // Unique event name per request — tránh đụng nếu user gọi liên tiếp.
    let req_id = format!(
        "{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
        rand_suffix()
    );
    let event_name = format!("aff-webview-result-{req_id}");

    // Setup listener với oneshot channel TRƯỚC khi eval (tránh race condition
    // nếu script chạy quá nhanh emit trước khi listener active).
    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = Arc::new(Mutex::new(Some(tx)));
    let tx_clone = tx.clone();
    let listener_id = app.listen(event_name.clone(), move |event| {
        let payload = event.payload().to_string();
        if let Ok(mut guard) = tx_clone.lock() {
            if let Some(sender) = guard.take() {
                let _ = sender.send(payload);
            }
        }
    });

    // Eval script trong webview của login window. Script sẽ gọi fetch
    // CHẠY TRONG context của affiliate.shopee.vn → cookies + anti-fraud
    // tự gắn vào, không cần ta lo headers.
    let script = build_convert_script(&event_name, &body_json);
    if let Err(e) = window.eval(&script) {
        app.unlisten(listener_id);
        return Err(CmdError::msg(format!("eval JS thất bại: {e}")));
    }

    // Đợi event với timeout. 30s đủ cho 50-100 link batch convert.
    let payload_str = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        rx,
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(_)) => {
            app.unlisten(listener_id);
            return Err(CmdError::msg("Channel đóng bất thường"));
        }
        Err(_) => {
            app.unlisten(listener_id);
            return Err(CmdError::msg(
                "Timeout 30s đợi response từ webview. Có thể trang Shopee đang load chậm hoặc bị block.",
            ));
        }
    };
    app.unlisten(listener_id);

    // Payload là JSON string (do JS emit JSON.stringify). Tauri sẽ wrap thêm
    // 1 lớp khi serialize event payload → cần parse 2 lần.
    let outer: serde_json::Value = serde_json::from_str(&payload_str)
        .map_err(|e| CmdError::msg(format!("Parse event payload outer fail: {e}")))?;
    let inner_str = outer
        .as_str()
        .ok_or_else(|| CmdError::msg("Event payload không phải string JSON"))?;
    let parsed: WebviewConvertResponse = serde_json::from_str(inner_str)
        .map_err(|e| {
            let preview = &inner_str[..inner_str.len().min(300)];
            CmdError::msg(format!("Parse webview response fail: {e}: {preview}"))
        })?;

    if !parsed.ok {
        if let Some(err) = parsed.error {
            return Err(CmdError::msg(format!("WebView fetch lỗi: {err}")));
        }
        let preview = parsed
            .text
            .as_deref()
            .map(|s| &s[..s.len().min(300)])
            .unwrap_or("");
        return Err(CmdError::msg(format!(
            "Shopee trả HTTP {} qua webview: {preview}",
            parsed.status
        )));
    }

    let gql: GqlResponse = serde_json::from_value(
        parsed.data.ok_or_else(|| CmdError::msg("Response không có field data"))?,
    )
    .map_err(|e| CmdError::msg(format!("Parse GraphQL response fail: {e}")))?;

    if let serde_json::Value::Array(arr) = &gql.errors {
        if !arr.is_empty() {
            let msg = arr
                .iter()
                .filter_map(|v| v.get("message").and_then(|m| m.as_str()))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(CmdError::msg(format!("GraphQL error: {msg}")));
        }
    }

    let items = gql.data.and_then(|d| d.batch_custom_link).unwrap_or_default();
    Ok(unique_links
        .into_iter()
        .enumerate()
        .map(|(idx, link)| match items.get(idx) {
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
                error: Some("Shopee không trả kết quả".into()),
            },
        })
        .collect())
}

fn rand_suffix() -> String {
    // Pseudo-random suffix dùng chrono nano để tránh collide khi 2 req cùng millis.
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{:08x}", n)
}
