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
CREATE TABLE IF NOT EXISTS imported_files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT NOT NULL,
    kind         TEXT NOT NULL,        -- 'shopee_clicks'|'shopee_commission'|'fb_ad_group'|'fb_campaign'
    imported_at  TEXT NOT NULL,
    row_count    INTEGER NOT NULL DEFAULT 0,
    file_hash    TEXT NOT NULL,        -- SHA-256 của raw content
    stored_path  TEXT,                 -- relative path trong app_data_dir/imports/
    day_date     TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
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
-- raw_fb_ad_groups — 1 row/ad_group × date range.
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_fb_ad_groups (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    ad_group_name      TEXT NOT NULL,
    sub_id1            TEXT NOT NULL DEFAULT '',
    sub_id2            TEXT NOT NULL DEFAULT '',
    sub_id3            TEXT NOT NULL DEFAULT '',
    sub_id4            TEXT NOT NULL DEFAULT '',
    sub_id5            TEXT NOT NULL DEFAULT '',
    report_start       TEXT,
    report_end         TEXT,
    status             TEXT,
    spend              REAL,
    impressions        INTEGER,
    reach              INTEGER,
    frequency          REAL,
    link_clicks        INTEGER,
    shop_clicks        INTEGER,
    all_clicks         INTEGER,
    link_cpc           REAL,
    all_cpc            REAL,
    link_ctr           REAL,
    all_ctr            REAL,
    landing_views      INTEGER,
    cpm                REAL,
    raw_json           TEXT,
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id     INTEGER NOT NULL REFERENCES imported_files(id) ON DELETE CASCADE,
    UNIQUE(day_date, ad_group_name)
);

CREATE INDEX IF NOT EXISTS idx_fb_ad_day         ON raw_fb_ad_groups(day_date);
CREATE INDEX IF NOT EXISTS idx_fb_ad_subid       ON raw_fb_ad_groups(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_fb_ad_day_subid   ON raw_fb_ad_groups(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

-- =============================================================
-- raw_fb_campaigns — tương tự, level campaign (ít field hơn).
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_fb_campaigns (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_name      TEXT NOT NULL,
    sub_id1            TEXT NOT NULL DEFAULT '',
    sub_id2            TEXT NOT NULL DEFAULT '',
    sub_id3            TEXT NOT NULL DEFAULT '',
    sub_id4            TEXT NOT NULL DEFAULT '',
    sub_id5            TEXT NOT NULL DEFAULT '',
    report_start       TEXT,
    report_end         TEXT,
    status             TEXT,
    spend              REAL,
    impressions        INTEGER,
    reach              INTEGER,
    result_count       INTEGER,
    result_indicator   TEXT,
    raw_json           TEXT,
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id     INTEGER NOT NULL REFERENCES imported_files(id) ON DELETE CASCADE,
    UNIQUE(day_date, campaign_name)
);

CREATE INDEX IF NOT EXISTS idx_fb_camp_day       ON raw_fb_campaigns(day_date);
CREATE INDEX IF NOT EXISTS idx_fb_camp_subid     ON raw_fb_campaigns(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

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

-- =============================================================
-- Bảng version migration (để future-proof khi thay schema).
-- =============================================================
CREATE TABLE IF NOT EXISTS _schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
