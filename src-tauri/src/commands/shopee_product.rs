//! Tra cứu thông tin sản phẩm Shopee qua API addlivetag.
//!
//! Endpoint: `https://data.addlivetag.com/product-data/product-data.php`
//! Cần truyền `url` (full hoặc short link Shopee) hoặc `item_id`.
//! Response: `{ status: "success"|"error", productInfo?, message? }`.
//!
//! API đã cache 24h server-side nên client KHÔNG cần cache thêm — mỗi lần
//! user paste link là 1 request.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{CmdError, CmdResult};

const API_BASE: &str = "https://data.addlivetag.com/product-data/product-data.php";
const UA: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36";

/// Thông tin sản phẩm trả về cho UI. Giữ nguyên `Value` cho các field mở rộng
/// (priceStats, latestPriceHistory) để frontend tự pick mà BE không phải bám sát
/// schema API bên thứ 3.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShopeeProductInfo {
    pub item_id: Option<i64>,
    #[serde(default)]
    pub product_name: String,
    #[serde(default)]
    pub shop_name: String,
    #[serde(default)]
    pub price: f64,
    #[serde(default)]
    pub sales: i64,
    #[serde(default)]
    pub image_url: String,
    #[serde(default)]
    pub product_link: String,
    /// Shopee trả rating dưới dạng string ("4.50") hoặc number — giữ Value.
    #[serde(default)]
    pub rating: Value,
    #[serde(default)]
    pub commission: f64,
    #[serde(default)]
    pub seller_com_final: f64,
    #[serde(default)]
    pub shopee_com_final: f64,
    #[serde(default)]
    pub is_xtra: bool,
    #[serde(default)]
    pub last_update: String,
    #[serde(default)]
    pub data_source: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    status: Option<String>,
    message: Option<String>,
    #[serde(rename = "productInfo")]
    product_info: Option<ShopeeProductInfo>,
}

fn http_client() -> Result<reqwest::Client, CmdError> {
    reqwest::Client::builder()
        .user_agent(UA)
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(CmdError::from)
}

/// Fetch thông tin sản phẩm Shopee từ URL (full hoặc short link).
/// API tự trích item_id từ URL.
#[tauri::command]
pub async fn fetch_shopee_product(url: String) -> CmdResult<ShopeeProductInfo> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(CmdError::msg("URL trống"));
    }

    let client = http_client()?;
    let resp = client
        .get(API_BASE)
        .query(&[("url", trimmed)])
        .send()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi kết nối: {e}")))?;

    let status_code = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| CmdError::msg(format!("Lỗi đọc response: {e}")))?;

    if !status_code.is_success() {
        let preview = &text[..text.len().min(200)];
        return Err(CmdError::msg(format!("HTTP {status_code}: {preview}")));
    }

    let parsed: ApiResponse = serde_json::from_str(&text).map_err(|e| {
        let preview = &text[..text.len().min(200)];
        CmdError::msg(format!("Phản hồi không hợp lệ ({e}): {preview}"))
    })?;

    if parsed.status.as_deref() == Some("error") {
        let msg = parsed
            .message
            .unwrap_or_else(|| "API trả lỗi không xác định".to_string());
        return Err(CmdError::msg(msg));
    }

    parsed
        .product_info
        .ok_or_else(|| CmdError::msg("Không có productInfo trong phản hồi"))
}
