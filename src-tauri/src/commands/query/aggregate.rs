//! Core sub_id matching + representative selection.
//!
//! Dùng bởi `days.rs` (aggregation chính), `items.rs` (drill-down), `overview.rs`
//! (autocomplete prefix). Không đụng DB ngoài `read_sub_id_match_mode`.

use rusqlite::Connection;

/// Canonical tuple = non-empty slots có ý nghĩa. Trailing empty bỏ đi.
pub(crate) type Canonical = Vec<String>;

pub(crate) fn to_canonical(s: [String; 5]) -> Canonical {
    let mut v: Vec<String> = s.into();
    while let Some(last) = v.last() {
        if last.is_empty() {
            v.pop();
        } else {
            break;
        }
    }
    v
}

/// Mode khớp tuple sub_id. Persist trong `app_settings` key `subIdMatchMode`.
/// - `Exact`: slot-by-slot equality (default, behavior cũ). Tuple A merge với
///   B chỉ khi A là **vec-prefix** của B (slot 0..N bằng nhau hoàn toàn).
/// - `Substring`: bao gồm Exact PLUS substring matching trên joined canonical
///   (case-insensitive, min 3 ký tự). Cho phép "dungcamp1" merge với "camp1"
///   khi user đặt tên FB campaign dài hơn subid Shopee.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SubIdMatchMode {
    #[default]
    Exact,
    Substring,
}

/// Đọc mode từ `app_settings` (key = `subIdMatchMode`, value JSON string).
/// Default = Exact nếu key vắng mặt hoặc value lạ.
pub(crate) fn read_sub_id_match_mode(conn: &Connection) -> SubIdMatchMode {
    use rusqlite::OptionalExtension;
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM app_settings WHERE key = 'subIdMatchMode'",
            [],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten();
    // FE persist qua JSON.stringify → string có quote: `"substring"`.
    match raw.as_deref() {
        Some("\"substring\"") | Some("substring") => SubIdMatchMode::Substring,
        _ => SubIdMatchMode::Exact,
    }
}

/// `a` là prefix của `b` (bao gồm trường hợp a == b). Slot-level equality.
pub(crate) fn is_prefix(a: &Canonical, b: &Canonical) -> bool {
    a.len() <= b.len() && a.iter().zip(b.iter()).all(|(x, y)| x == y)
}

/// Bi-directional compatibility check theo mode đang chạy.
/// - Exact: `is_prefix(a, b) || is_prefix(b, a)` (slot-level, behavior cũ).
/// - Substring: bao gồm Exact + substring trên joined canonical
///   (case-insensitive). Yêu cầu min 3 ký tự ở chuỗi ngắn hơn để giảm false
///   positive cho subid quá ngắn (vd "ab" chứa trong "abc" sẽ noisy).
///
/// Guard: empty canonical (`[]`) chỉ compatible với chính nó (`[]`). Không
/// guard thì `is_prefix([], b)` = true với mọi `b` (zip().all() trên tập rỗng
/// luôn true) → FB ad không có sub_id bị merge nhầm vào anchor dài nhất ngày.
pub(crate) fn is_compatible(
    a: &Canonical,
    b: &Canonical,
    mode: SubIdMatchMode,
) -> bool {
    if a.is_empty() || b.is_empty() {
        return a.is_empty() && b.is_empty();
    }
    if is_prefix(a, b) || is_prefix(b, a) {
        return true;
    }
    if mode == SubIdMatchMode::Substring {
        let sa = a.join("-").to_lowercase();
        let sb = b.join("-").to_lowercase();
        if sa.is_empty() || sb.is_empty() {
            return false;
        }
        let min_len = sa.len().min(sb.len());
        if min_len < 3 {
            return sa == sb;
        }
        return sa.contains(&sb) || sb.contains(&sa);
    }
    false
}

/// Chọn "đại diện" cho 1 canonical: ưu tiên **anchor** (canonical từ Shopee
/// order = hoa hồng sản phẩm) mà compatible theo mode. Không có anchor
/// compatible → fallback về chính nó (FB campaign standalone giữ tên camp).
/// Tie-break giữa nhiều anchor: chọn dài nhất (match cụ thể nhất), rồi lex order.
pub(super) fn representative(
    c: &Canonical,
    anchors: &[Canonical],
    mode: SubIdMatchMode,
) -> Canonical {
    let mut best: Option<&Canonical> = None;
    for a in anchors {
        if !is_compatible(c, a, mode) {
            continue;
        }
        match best {
            None => best = Some(a),
            Some(b) => {
                if a.len() > b.len() || (a.len() == b.len() && a < b) {
                    best = Some(a);
                }
            }
        }
    }
    best.cloned().unwrap_or_else(|| c.clone())
}

pub(super) fn canonical_to_array(c: &Canonical) -> [String; 5] {
    std::array::from_fn(|i| c.get(i).cloned().unwrap_or_default())
}

pub(super) fn default_name(c: &Canonical) -> String {
    if c.is_empty() {
        "(chưa đặt tên)".to_string()
    } else {
        c.join("-")
    }
}

/// Test xem 1 row có match sub_ids filter không — dùng `is_compatible` theo
/// mode hiện tại (cùng logic UI aggregate đang chạy).
pub(super) fn sub_ids_match(
    row: &[String; 5],
    target: &[String; 5],
    mode: SubIdMatchMode,
) -> bool {
    let row_canon = to_canonical(row.clone());
    let target_canon = to_canonical(target.clone());
    is_compatible(&row_canon, &target_canon, mode)
}

/// Append filter day_date BETWEEN + shopee_account_id từ DaysFilter vào SQL
/// đang build. Pattern lặp 7+ lần trong insights/items — gộp lại 1 chỗ để
/// thay đổi semantics (vd thêm IN-list account) chỉ cần sửa 1 fn.
pub(super) fn append_date_account_filters(
    sql: &mut String,
    params: &mut Vec<Box<dyn rusqlite::ToSql>>,
    filter: &super::DaysFilter,
) {
    if let Some(v) = &filter.from_date {
        sql.push_str(" AND day_date >= ?");
        params.push(Box::new(v.clone()));
    }
    if let Some(v) = &filter.to_date {
        sql.push_str(" AND day_date <= ?");
        params.push(Box::new(v.clone()));
    }
    if let Some(super::AccountFilterMode::Account { id }) = &filter.account_filter {
        sql.push_str(" AND shopee_account_id = ?");
        params.push(Box::new(*id));
    }
}

/// Convert `Vec<Box<dyn ToSql>>` → `Vec<&dyn ToSql>` cho `query_map` arg.
/// Pattern lặp 10+ lần trong query module.
pub(super) fn params_to_refs(
    params: &[Box<dyn rusqlite::ToSql>],
) -> Vec<&dyn rusqlite::ToSql> {
    params.iter().map(|b| b.as_ref() as &dyn rusqlite::ToSql).collect()
}

/// Đẩy pre-filter sub_id xuống SQL để tận dụng `idx_orders_day_subid`. Logic
/// pre-filter là điều kiện CẦN của `is_compatible` — Rust vẫn check post-filter
/// cho chính xác.
///
/// - Exact + target rỗng: SQL đủ chính xác (5 cột = ''), Rust check vẫn pass.
/// - Exact + target non-empty: SQL filter `sub_id1 = target[0]` (cả 2 chiều
///   prefix đều bắt buộc slot 0 match).
/// - Substring: không pre-filter (substring không đảm bảo slot 0 match).
pub(super) fn append_subid_prefilter(
    sql: &mut String,
    params: &mut Vec<Box<dyn rusqlite::ToSql>>,
    target: &[String; 5],
    mode: SubIdMatchMode,
) {
    if mode == SubIdMatchMode::Substring {
        return;
    }
    let canon = to_canonical(target.clone());
    if canon.is_empty() {
        sql.push_str(
            " AND sub_id1 = '' AND sub_id2 = '' AND sub_id3 = '' \
             AND sub_id4 = '' AND sub_id5 = ''",
        );
    } else {
        sql.push_str(" AND sub_id1 = ?");
        params.push(Box::new(canon[0].clone()));
    }
}
