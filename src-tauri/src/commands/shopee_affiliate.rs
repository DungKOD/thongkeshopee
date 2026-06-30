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
