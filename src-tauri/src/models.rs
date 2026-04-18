use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Chi tiết từng đơn hàng để thống kê (hủy, 0đ, trung bình).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderDetail {
    pub id: String,
    pub status: String,
    pub gross_value: f64,
    pub commission: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub click_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_time: Option<String>,
}

/// Một video đã chạy ads, chỉ lưu các trường nhập tay.
/// Các chỉ số phái sinh (CPC, tỷ lệ chuyển đổi, hoa hồng, lợi nhuận, tỷ suất)
/// được tính ở tầng hiển thị.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Video {
    pub id: String,
    pub name: String,
    pub clicks: u32,
    /// Số click Shopee tách theo referrer (Facebook, Instagram, Zalo, Others...).
    /// Tổng hiển thị được tính từ filter trong settings.
    pub shopee_clicks_by_referrer: HashMap<String, u32>,
    pub total_spend: f64,
    pub orders: u32,
    pub commission: f64,
    /// CPC đọc từ file FB (ưu tiên). Nếu None → tính từ total_spend / clicks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpc: Option<f64>,
    /// Chi tiết từng đơn từ Shopee Affiliate report (nếu có).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order_details: Option<Vec<OrderDetail>>,
}

/// Một ngày thống kê, chứa danh sách video đã chạy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Day {
    pub id: String,
    pub date: String,
    pub videos: Vec<Video>,
}
