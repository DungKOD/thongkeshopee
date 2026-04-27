//! Table descriptors — metadata về mỗi bảng syncable (cursor type, PK, delta op).
//!
//! Push/Pull logic iterate `SYNC_TABLES` theo dependency order (parent table
//! phải push trước child có FK) để apply bên kia không fail FK constraint.
//!
//! Thứ tự hiện tại (plan D3):
//! 1. `shopee_accounts` — không FK ra đâu (parent cho imported_files)
//! 2. `imported_files` — FK shopee_account_id → shopee_accounts
//! 3. `raw_shopee_clicks`, `raw_shopee_order_items`, `raw_fb_ads` — FK day_date + source_file_id
//! 4. `clicks_to_file`, `orders_to_file`, `fb_ads_to_file` — mapping many-to-many
//! 5. `manual_entries` — FK day_date (days được auto-insert từ raw)
//! 6. `tombstones` — không FK, append-only
//!
//! `days` không sync trực tiếp — derive từ raw rows khi apply.

/// Alias explicit cho SQLite `rowid` expression trong SELECT. Cần thiết vì
/// tables có `INTEGER PRIMARY KEY AUTOINCREMENT` (vd `raw_shopee_order_items.id`)
/// → `rowid` alias về column PK đó, nên `column_names()` trả "id" thay vì "rowid"
/// và extract_cursor fail tìm key "rowid" trong row object. Explicit alias
/// đảm bảo column đầu luôn tên biết trước.
pub const V9_ROWID_ALIAS: &str = "__v9_rowid__";

/// Cách interpret cursor cho table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorKind {
    /// `rowid` (SQLite auto). Dùng cho raw append-only tables.
    /// Cursor stored as stringified integer ("0", "12345").
    RowId,
    /// Primary key column `id` (AUTOINCREMENT). Same as RowId in practice
    /// nhưng explicit column name → query rõ ràng hơn.
    PrimaryKey,
    /// `updated_at` column (HLC RFC3339 Z). Dùng cho mutable tables:
    /// manual_entries, shopee_accounts (sau khi thêm updated_at).
    UpdatedAt,
    /// `deleted_at` column (HLC RFC3339 Z). Dùng cho `tombstones` append-only.
    DeletedAt,
}

/// Delta operation khi capture từ table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOp {
    /// INSERT OR IGNORE theo PK (raw tables, append-only).
    Insert,
    /// Upsert với HLC check (mutable tables). Local updated_at > event → skip.
    Upsert,
    /// Tombstone entity (type + key + deleted_at). Apply qua
    /// `apply_tombstones` logic với resurrect rule.
    Tombstone,
}

/// Metadata cho 1 syncable table.
#[derive(Debug, Clone)]
pub struct TableDescriptor {
    /// Tên table match `sync_cursor_state.table_name`.
    pub name: &'static str,
    /// Cột/field dùng làm cursor.
    pub cursor_kind: CursorKind,
    /// Tên column chứa cursor value. Cho RowId = "rowid". Cho PrimaryKey
    /// thường là "id". Cho UpdatedAt = "updated_at". DeletedAt = "deleted_at".
    pub cursor_column: &'static str,
    /// Primary key columns (dùng để build `pk` JSON trong DeltaEvent).
    pub pk_columns: &'static [&'static str],
    /// Op khi capture.
    pub op: DeltaOp,
}

/// List tất cả bảng syncable, theo push dependency order.
pub const SYNC_TABLES: &[TableDescriptor] = &[
    // Cursor PHẢI dùng cột timestamp monotonic, KHÔNG được dùng `id`. Reason:
    // `id = content_id(natural_key)` = SHA-256 hash → uniform random i63, không
    // monotonic. Dùng id làm cursor → row mới có id < cursor cũ bị bỏ qua vĩnh
    // viễn → child rows (FK source_file_id) push lên R2 mà parent chưa push →
    // máy khác pull FK fail.
    TableDescriptor {
        name: "shopee_accounts",
        cursor_kind: CursorKind::UpdatedAt,
        cursor_column: "created_at",
        pk_columns: &["id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "imported_files",
        cursor_kind: CursorKind::UpdatedAt,
        cursor_column: "imported_at",
        pk_columns: &["id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "raw_shopee_clicks",
        cursor_kind: CursorKind::RowId,
        cursor_column: V9_ROWID_ALIAS,
        pk_columns: &["click_id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "raw_shopee_order_items",
        cursor_kind: CursorKind::RowId,
        cursor_column: V9_ROWID_ALIAS,
        pk_columns: &["checkout_id", "item_id", "model_id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "raw_fb_ads",
        cursor_kind: CursorKind::RowId,
        cursor_column: V9_ROWID_ALIAS,
        pk_columns: &["day_date", "level", "name"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "clicks_to_file",
        cursor_kind: CursorKind::RowId,
        cursor_column: V9_ROWID_ALIAS,
        pk_columns: &["click_id", "file_id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "orders_to_file",
        cursor_kind: CursorKind::RowId,
        cursor_column: V9_ROWID_ALIAS,
        pk_columns: &["order_item_id", "file_id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "fb_ads_to_file",
        cursor_kind: CursorKind::RowId,
        cursor_column: V9_ROWID_ALIAS,
        pk_columns: &["fb_ad_id", "file_id"],
        op: DeltaOp::Insert,
    },
    TableDescriptor {
        name: "manual_entries",
        cursor_kind: CursorKind::UpdatedAt,
        cursor_column: "updated_at",
        pk_columns: &["sub_id1", "sub_id2", "sub_id3", "sub_id4", "sub_id5", "day_date"],
        op: DeltaOp::Upsert,
    },
    // app_settings — key-value user preferences (clickSources, profitFees,
    // autoSyncEnabled). Mỗi user-DB có row riêng (per-user isolation đã handle
    // qua DB swap). Keys dynamic (vd `click_source.<referrer>`) — descriptor
    // generic over PK string nên hoạt động native. LWW per-key qua updated_at.
    TableDescriptor {
        name: "app_settings",
        cursor_kind: CursorKind::UpdatedAt,
        cursor_column: "updated_at",
        pk_columns: &["key"],
        op: DeltaOp::Upsert,
    },
    TableDescriptor {
        name: "tombstones",
        cursor_kind: CursorKind::DeletedAt,
        cursor_column: "deleted_at",
        pk_columns: &["entity_type", "entity_key"],
        op: DeltaOp::Tombstone,
    },
];

/// Tra TableDescriptor theo tên. O(n) vì list chỉ 10 entries.
pub fn find_descriptor(name: &str) -> Option<&'static TableDescriptor> {
    SYNC_TABLES.iter().find(|d| d.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_tables_has_11_entries() {
        assert_eq!(SYNC_TABLES.len(), 11, "match seed trong schema.sql (v3 thêm app_settings)");
    }

    #[test]
    fn sync_tables_names_match_seed() {
        // Phải giống danh sách seed trong schema.sql.
        let names: Vec<&str> = SYNC_TABLES.iter().map(|d| d.name).collect();
        let expected = [
            "shopee_accounts",
            "imported_files",
            "raw_shopee_clicks",
            "raw_shopee_order_items",
            "raw_fb_ads",
            "clicks_to_file",
            "orders_to_file",
            "fb_ads_to_file",
            "manual_entries",
            "app_settings",
            "tombstones",
        ];
        for name in expected {
            assert!(names.contains(&name), "descriptor thiếu {name}");
        }
    }

    #[test]
    fn find_descriptor_returns_expected() {
        let d = find_descriptor("raw_shopee_clicks").unwrap();
        assert_eq!(d.cursor_kind, CursorKind::RowId);
        assert_eq!(d.pk_columns, &["click_id"]);
        assert_eq!(d.op, DeltaOp::Insert);
    }

    #[test]
    fn find_descriptor_none_for_unknown() {
        assert!(find_descriptor("nonexistent_table").is_none());
    }

    #[test]
    fn dependency_order_parents_before_children() {
        // shopee_accounts phải trước imported_files (FK shopee_account_id).
        let acc_idx = SYNC_TABLES
            .iter()
            .position(|d| d.name == "shopee_accounts")
            .unwrap();
        let file_idx = SYNC_TABLES
            .iter()
            .position(|d| d.name == "imported_files")
            .unwrap();
        assert!(
            acc_idx < file_idx,
            "shopee_accounts phải push trước imported_files"
        );

        // imported_files phải trước raw_* (source_file_id FK).
        let clicks_idx = SYNC_TABLES
            .iter()
            .position(|d| d.name == "raw_shopee_clicks")
            .unwrap();
        assert!(
            file_idx < clicks_idx,
            "imported_files phải push trước raw_shopee_clicks"
        );

        // raw_* phải trước mapping tables (cùng click_id reference).
        let mapping_idx = SYNC_TABLES
            .iter()
            .position(|d| d.name == "clicks_to_file")
            .unwrap();
        assert!(
            clicks_idx < mapping_idx,
            "raw_shopee_clicks phải push trước clicks_to_file"
        );
    }

    #[test]
    fn mutable_tables_use_updated_at_cursor() {
        let manual = find_descriptor("manual_entries").unwrap();
        assert_eq!(manual.cursor_kind, CursorKind::UpdatedAt);
        assert_eq!(manual.cursor_column, "updated_at");
    }

    #[test]
    fn tombstones_use_deleted_at_cursor() {
        let ts = find_descriptor("tombstones").unwrap();
        assert_eq!(ts.cursor_kind, CursorKind::DeletedAt);
        assert_eq!(ts.op, DeltaOp::Tombstone);
    }
}
