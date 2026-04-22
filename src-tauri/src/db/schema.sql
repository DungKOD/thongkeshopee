-- Schema v1 — ThongKeShopee
-- ELT pattern: raw tables + manual_entries, no rollup cache.
-- Day là first-class entity, mọi raw có day_date FK CASCADE.

PRAGMA foreign_keys = ON;

-- =============================================================
-- Bảng days — container chính, UI DayBlock = 1 row.
-- =============================================================
CREATE TABLE IF NOT EXISTS days (
    date        TEXT PRIMARY KEY,      -- 'YYYY-MM-DD'
    created_at  TEXT NOT NULL,         -- ISO8601 UTC
    notes       TEXT
);

-- =============================================================
-- Bảng imported_files — audit log mọi lần import CSV.
-- =============================================================
-- day_date: earliest date trong file (informational only). Nullable +
-- KHÔNG FK to days(date) — Shopee file có thể chứa nhiều ngày (commission
-- report update đơn cũ). Nếu có FK CASCADE → xóa ngày X → wipe file metadata
-- → wipe luôn raw rows của NGÀY KHÁC cùng file qua source_file_id CASCADE.
-- Không CASCADE qua day cho imported_files = an toàn cho multi-day file.
CREATE TABLE IF NOT EXISTS imported_files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT NOT NULL,
    kind         TEXT NOT NULL,        -- 'shopee_clicks'|'shopee_commission'|'fb_ad_group'|'fb_campaign'
    imported_at  TEXT NOT NULL,
    row_count    INTEGER NOT NULL DEFAULT 0,
    file_hash    TEXT NOT NULL,        -- SHA-256 của raw content
    stored_path  TEXT,                 -- relative path trong app_data_dir/imports/
    day_date     TEXT,                 -- earliest date in file (informational)
    notes        TEXT,
    UNIQUE(file_hash)                  -- chặn import trùng
);

CREATE INDEX IF NOT EXISTS idx_imported_day     ON imported_files(day_date);
CREATE INDEX IF NOT EXISTS idx_imported_kind    ON imported_files(kind);

-- =============================================================
-- raw_shopee_clicks — 1 row/click từ WebsiteClickReport.
-- PK = click_id (natural key từ CSV).
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_shopee_clicks (
    click_id         TEXT PRIMARY KEY,
    click_time       TEXT NOT NULL,
    region           TEXT,
    sub_id_raw       TEXT,              -- chuỗi gốc trước split
    sub_id1          TEXT NOT NULL DEFAULT '',
    sub_id2          TEXT NOT NULL DEFAULT '',
    sub_id3          TEXT NOT NULL DEFAULT '',
    sub_id4          TEXT NOT NULL DEFAULT '',
    sub_id5          TEXT NOT NULL DEFAULT '',
    referrer         TEXT,
    day_date         TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id   INTEGER NOT NULL REFERENCES imported_files(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_clicks_day        ON raw_shopee_clicks(day_date);
CREATE INDEX IF NOT EXISTS idx_clicks_subid      ON raw_shopee_clicks(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_clicks_day_subid  ON raw_shopee_clicks(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

-- =============================================================
-- raw_shopee_order_items — 1 row/item trong order.
-- Dedup: UNIQUE(checkout_id, item_id, model_id) — ON CONFLICT DO UPDATE.
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_shopee_order_items (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id           TEXT NOT NULL,
    checkout_id        TEXT NOT NULL,
    item_id            TEXT NOT NULL,
    model_id           TEXT NOT NULL DEFAULT '',
    order_status       TEXT,
    order_time         TEXT,
    completed_time     TEXT,
    click_time         TEXT,
    shop_id            TEXT,
    shop_name          TEXT,
    shop_type          TEXT,
    item_name          TEXT,
    category_l1        TEXT,
    category_l2        TEXT,
    category_l3        TEXT,
    price              REAL,
    quantity           INTEGER,
    order_value        REAL,
    refund_amount      REAL,
    net_commission     REAL,              -- Hoa hồng ròng tiếp thị liên kết
    commission_total   REAL,              -- Tổng hoa hồng sản phẩm
    sub_id1            TEXT NOT NULL DEFAULT '',
    sub_id2            TEXT NOT NULL DEFAULT '',
    sub_id3            TEXT NOT NULL DEFAULT '',
    sub_id4            TEXT NOT NULL DEFAULT '',
    sub_id5            TEXT NOT NULL DEFAULT '',
    channel            TEXT,
    raw_json           TEXT,              -- JSON blob các field phụ (rate, MCN fee, note...)
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id     INTEGER NOT NULL REFERENCES imported_files(id) ON DELETE CASCADE,
    UNIQUE(checkout_id, item_id, model_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_day        ON raw_shopee_order_items(day_date);
CREATE INDEX IF NOT EXISTS idx_orders_subid      ON raw_shopee_order_items(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_orders_day_subid  ON raw_shopee_order_items(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_orders_item       ON raw_shopee_order_items(item_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON raw_shopee_order_items(order_status);

-- =============================================================
-- raw_fb_ads — unified FB ads table (campaign + ad_group).
-- `level` = 'campaign' | 'ad_group'. `clicks` và `cpc` đã normalize lúc INSERT:
--   clicks = link_clicks ?? all_clicks ?? result_count  (KQ từ obj "Link Click")
--   cpc    = link_cpc ?? all_cpc ?? cost_per_result
-- Aggregate ưu tiên level='ad_group' per sub_id tuple để tránh double-count
-- khi user import cả 2 file cùng ngày.
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_fb_ads (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    level              TEXT NOT NULL,              -- 'campaign' | 'ad_group'
    name               TEXT NOT NULL,
    sub_id1            TEXT NOT NULL DEFAULT '',
    sub_id2            TEXT NOT NULL DEFAULT '',
    sub_id3            TEXT NOT NULL DEFAULT '',
    sub_id4            TEXT NOT NULL DEFAULT '',
    sub_id5            TEXT NOT NULL DEFAULT '',
    report_start       TEXT,
    report_end         TEXT,
    status             TEXT,
    spend              REAL,
    clicks             INTEGER,                    -- normalized
    cpc                REAL,                       -- normalized
    impressions        INTEGER,
    reach              INTEGER,
    raw_json           TEXT,                       -- các field phụ (frequency, CTR, CPM, landing_views, shop_clicks, result_indicator, cost_per_result, ...)
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id     INTEGER NOT NULL REFERENCES imported_files(id) ON DELETE CASCADE,
    UNIQUE(day_date, level, name)
);

CREATE INDEX IF NOT EXISTS idx_fb_ads_day        ON raw_fb_ads(day_date);
CREATE INDEX IF NOT EXISTS idx_fb_ads_level      ON raw_fb_ads(day_date, level);
CREATE INDEX IF NOT EXISTS idx_fb_ads_subid      ON raw_fb_ads(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_fb_ads_day_subid  ON raw_fb_ads(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

-- =============================================================
-- manual_entries — user nhập tay hoặc override.
-- Identity: (sub_id1..5, day_date).
-- =============================================================
CREATE TABLE IF NOT EXISTS manual_entries (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    sub_id1            TEXT NOT NULL DEFAULT '',
    sub_id2            TEXT NOT NULL DEFAULT '',
    sub_id3            TEXT NOT NULL DEFAULT '',
    sub_id4            TEXT NOT NULL DEFAULT '',
    sub_id5            TEXT NOT NULL DEFAULT '',
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    display_name       TEXT,
    override_clicks    INTEGER,
    override_spend     REAL,
    override_cpc       REAL,
    override_orders    INTEGER,
    override_commission REAL,
    notes              TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    UNIQUE(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date)
);

CREATE INDEX IF NOT EXISTS idx_manual_day        ON manual_entries(day_date);

-- video_downloads table ĐÃ MOVE sang `video_logs.db` riêng (v4).
-- Xem `db/video_db.rs`. Primary audit = Google Sheet qua Apps Script.

-- =============================================================
-- sync_state — singleton row theo dõi trạng thái sync Drive.
-- dirty=1 → DB có thay đổi chưa upload. dirty=0 → đã sync.
-- change_id tăng monotone mỗi mutation → CAS pattern chống race
-- khi mutation xảy ra ĐANG upload (snapshot cũ, mutation sau không kịp).
-- Triggers khởi tạo trong `migrate()` để update body khi code đổi.
-- =============================================================
CREATE TABLE IF NOT EXISTS sync_state (
    id                            INTEGER PRIMARY KEY CHECK (id = 1),
    dirty                         INTEGER NOT NULL DEFAULT 1,
    last_synced_at_ms             INTEGER,
    last_synced_remote_mtime_ms   INTEGER,
    last_error                    TEXT,
    change_id                     INTEGER NOT NULL DEFAULT 0,
    last_uploaded_change_id       INTEGER NOT NULL DEFAULT 0
);

-- Seed singleton. Lần đầu: dirty=1 để force upload local data (nếu có).
INSERT OR IGNORE INTO sync_state (id, dirty) VALUES (1, 1);

-- =============================================================
-- tombstones — track deletion để merge cross-device không "hồi sinh" row
-- đã xóa. Apply khi pull-merge-push (xem plan sync v2).
--
-- entity_type:
--   'day'          — cả ngày bị xóa. entity_key = date ('YYYY-MM-DD').
--                    Apply: DELETE FROM days WHERE date=? → CASCADE raw/imported.
--   'ui_row'       — 1 "dòng UI" (tuple sub_id canonical) bị xóa staged.
--                    entity_key = '{day}|{s1}|{s2}|{s3}|{s4}|{s5}'.
--                    Apply: xóa manual_entries khớp tuple + raw rows prefix-compatible.
--   'manual_entry' — chỉ xóa manual override (không động raw).
--                    entity_key = '{day}|{s1}|{s2}|{s3}|{s4}|{s5}'.
--                    Apply: DELETE FROM manual_entries WHERE sub_ids + day_date.
-- deleted_at: ISO8601 UTC, dùng audit/debug.
-- =============================================================
CREATE TABLE IF NOT EXISTS tombstones (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type  TEXT NOT NULL CHECK (entity_type IN ('day', 'ui_row', 'manual_entry')),
    entity_key   TEXT NOT NULL,
    deleted_at   TEXT NOT NULL,
    UNIQUE(entity_type, entity_key)
);

CREATE INDEX IF NOT EXISTS idx_tombstones_type ON tombstones(entity_type);

-- =============================================================
-- shopee_accounts — 1 user Firebase có thể quản lý nhiều TK Shopee
-- affiliate. Mỗi row raw Shopee (clicks/orders/manual) tag về 1 account.
-- FB ads KHÔNG tag trực tiếp — attribution derive qua JOIN sub_ids + day.
--
-- Seed default account (id=1, 'Mặc định') ở migration để row cũ (chưa có
-- account) có default không-NULL sau ALTER TABLE ADD COLUMN.
-- =============================================================
CREATE TABLE IF NOT EXISTS shopee_accounts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    color        TEXT,
    created_at   TEXT NOT NULL
);

-- =============================================================
-- Bảng version migration (để future-proof khi thay schema).
-- =============================================================
CREATE TABLE IF NOT EXISTS _schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
