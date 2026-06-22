//! Drill-down chi tiết item theo sub_id + ngày.
//!
//! Filter dùng prefix-compatible (Exact bi-directional + Substring), match
//! đúng logic `is_compatible` của aggregation core. Pre-filter SQL pushdown
//! qua `append_subid_prefilter` (Exact mode → tận dụng `idx_orders_day_subid`).

use tauri::State;

use crate::db::DbState;

use super::super::{CmdError, CmdResult};
use super::aggregate::{
    append_subid_prefilter, canonical_to_array, is_compatible, params_to_refs,
    read_sub_id_match_mode, to_canonical,
};

/// Chi tiết 1 item trong order (từ raw_shopee_order_items) để hiển thị drill-down.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderItemDetail {
    pub order_id: String,
    pub checkout_id: String,
    pub item_id: String,
    pub model_id: String,
    pub item_name: Option<String>,
    pub shop_name: Option<String>,
    pub order_status: Option<String>,
    pub order_time: Option<String>,
    pub click_time: Option<String>,
    pub completed_time: Option<String>,
    pub price: Option<f64>,
    pub quantity: Option<i64>,
    pub order_value: Option<f64>,
    pub net_commission: Option<f64>,
    pub commission_total: Option<f64>,
    pub channel: Option<String>,
    pub sub_ids: [String; 5],
}

/// Lấy chi tiết các item đã mua qua tuple sub_id + date — dùng cho drill-down.
/// Matching theo prefix-compatible (xem `representative` ở aggregate). Order có
/// canonical là prefix của `sub_ids` truyền vào (hoặc ngược lại) đều match.
#[tauri::command]
// `account_id`: Khi filter=All và row split per-account, chỉ trả orders của
// đúng account đó. None hoặc empty = no filter (dialog aggregate cross-account).
// FE serialize string vì content_id hash > 2^53.
pub async fn get_order_items_for_row(
    state: State<'_, DbState>,
    day_date: String,
    sub_ids: [String; 5],
    account_id: Option<String>,
) -> CmdResult<Vec<OrderItemDetail>> {
    let conn = state.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let target_canonical = to_canonical(sub_ids);
    let match_mode = read_sub_id_match_mode(&conn);

    let account_id_filter: Option<i64> = account_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i64>().ok());

    let mut sql = String::from(
        "SELECT order_id, checkout_id, item_id, model_id, item_name,
                shop_name, order_status, order_time, click_time, completed_time,
                price, quantity, order_value, net_commission, commission_total,
                channel, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5
         FROM raw_shopee_order_items
         WHERE day_date = ?",
    );
    let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(day_date.clone())];
    if let Some(id) = account_id_filter {
        sql.push_str(" AND shopee_account_id = ?");
        params_vec.push(Box::new(id));
    }
    // Pre-filter sub_id ở SQL để tận dụng idx_orders_day_subid; Rust vẫn check
    // post-filter cho bi-directional prefix / Substring chính xác.
    let target_subs = canonical_to_array(&target_canonical);
    append_subid_prefilter(&mut sql, &mut params_vec, &target_subs, match_mode);
    sql.push_str(" ORDER BY order_time DESC");
    let mut stmt = conn.prepare_cached(&sql)?;
    let params_refs = params_to_refs(&params_vec);

    let iter = stmt.query_map(params_refs.as_slice(), |r| {
        let subs: [String; 5] = [
            r.get(16)?,
            r.get(17)?,
            r.get(18)?,
            r.get(19)?,
            r.get(20)?,
        ];
        Ok(OrderItemDetail {
            order_id: r.get(0)?,
            checkout_id: r.get(1)?,
            item_id: r.get(2)?,
            model_id: r.get(3)?,
            item_name: r.get(4)?,
            shop_name: r.get(5)?,
            order_status: r.get(6)?,
            order_time: r.get(7)?,
            click_time: r.get(8)?,
            completed_time: r.get(9)?,
            price: r.get(10)?,
            quantity: r.get(11)?,
            order_value: r.get(12)?,
            net_commission: r.get(13)?,
            commission_total: r.get(14)?,
            channel: r.get(15)?,
            sub_ids: subs,
        })
    })?;

    let mut out: Vec<OrderItemDetail> = Vec::new();
    for row in iter {
        let item = row?;
        let item_canonical = to_canonical(item.sub_ids.clone());
        if is_compatible(&item_canonical, &target_canonical, match_mode) {
            out.push(item);
        }
    }
    Ok(out)
}
