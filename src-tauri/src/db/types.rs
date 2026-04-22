//! DTO trao đổi giữa Rust ↔ TS qua Tauri commands.
//!
//! Quy ước: `#[serde(rename_all = "camelCase")]` để map JS convention.
//! Tuple sub_id chuẩn hóa thành `[String; 5]` khi serialize.
//!
//! `allow(dead_code)` vì pha 2/3 mới sử dụng hết.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// 1 row trên UI = 1 tuple sub_id × 1 ngày, aggregate từ raw tables + manual.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiRow {
    pub day_date: String,
    pub sub_ids: [String; 5],
    pub display_name: String,

    // FB metrics (nullable nếu chưa có FB import)
    pub ads_clicks: Option<i64>,
    pub total_spend: Option<f64>,
    pub cpc: Option<f64>,
    pub impressions: Option<i64>,

    // Shopee clicks tách theo referrer: { "Facebook": 120, "Others": 30 }
    pub shopee_clicks_by_referrer: std::collections::HashMap<String, i64>,
    pub shopee_clicks_total: i64,

    // Shopee orders/commission
    pub orders_count: i64,
    pub commission_total: f64,
    /// Commission từ đơn trạng thái rủi ro huỷ (subset của commission_total).
    /// Bao gồm "Đang chờ xử lý" + "Chưa thanh toán". FE trừ `commission_pending
    /// × returnReserveRate` khỏi net commission — chỉ đơn rủi ro bị dự phòng,
    /// completed đã chắc chắn.
    pub commission_pending: f64,
    pub order_value_total: f64,

    // Source flags để UI biết dòng này có data gì
    pub has_fb: bool,
    pub has_shopee_clicks: bool,
    pub has_shopee_orders: bool,
    pub has_manual: bool,
}

/// Day-level totals — KHÔNG áp row-0 filter (spend==0 && commission==0).
/// UI dùng cho KPI tổng. Nếu sum từ `UiDay.rows` thì miss tuple nào bị filter
/// row-0 drop (có click/order nhưng không có spend/commission). Mọi field
/// ở đây luôn accurate 100% so với raw data.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UiDayTotals {
    pub ads_clicks: i64,
    pub total_spend: f64,
    pub impressions: i64,
    pub shopee_clicks_by_referrer: std::collections::HashMap<String, i64>,
    pub shopee_clicks_total: i64,
    pub orders_count: i64,
    pub commission_total: f64,
    /// Commission từ đơn rủi ro huỷ: "Đang chờ xử lý" + "Chưa thanh toán".
    pub commission_pending: f64,
    pub order_value_total: f64,
    /// Tổng phí quản lý MCN đã bị Shopee cắt trước khi payout. Hiển thị trên UI
    /// để user biết đã mất bao nhiêu vào MCN (số này đã trừ sẵn trong
    /// `commission_total` — KHÔNG trừ lần nữa).
    pub mcn_fee_total: f64,
}

/// 1 ngày hiển thị trên UI (dùng cho DayBlock).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiDay {
    pub date: String,
    pub notes: Option<String>,
    pub rows: Vec<UiRow>,
    /// Tổng day-level TRƯỚC khi row-0 filter. KPI dùng field này để đúng với
    /// raw data kể cả khi có tuple chỉ có click (không spend/commission).
    pub totals: UiDayTotals,
}

/// Payload khi user lưu manual entry (add/edit 1 dòng tay).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualEntryInput {
    pub day_date: String,
    pub sub_ids: [String; 5],
    pub display_name: Option<String>,
    pub override_clicks: Option<i64>,
    pub override_spend: Option<f64>,
    pub override_cpc: Option<f64>,
    pub override_orders: Option<i64>,
    pub override_commission: Option<f64>,
    #[serde(default)]
    pub notes: Option<String>,
    /// Account Shopee mà manual entry thuộc về. FE luôn pass giá trị (dropdown
    /// chọn account khi tạo/edit). Phải là id hợp lệ trong `shopee_accounts`.
    pub shopee_account_id: i64,
}

/// Payload batch delete: user bấm "Lưu thay đổi" sau khi đã gạch ngang.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchDeletePayload {
    /// Ngày cần xóa hoàn toàn (CASCADE raw + manual).
    pub days: Vec<String>,
    /// Các dòng manual cần xóa, key = (sub_ids, day_date).
    pub manual_rows: Vec<ManualRowKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManualRowKey {
    pub day_date: String,
    pub sub_ids: [String; 5],
}
