-- Schema ThongKeShopee — final consolidated (post v1-v13).
-- ELT: raw tables + manual_entries là source of truth. Query on-the-fly,
-- không rollup cache. Day là first-class entity, raw có day_date FK CASCADE.
--
-- NO migrations — app fresh install only. User tự xóa DB nếu schema đổi.

PRAGMA foreign_keys = ON;

-- =============================================================
-- days — container, UI DayBlock = 1 row.
-- =============================================================
CREATE TABLE IF NOT EXISTS days (
    date        TEXT PRIMARY KEY,      -- 'YYYY-MM-DD'
    created_at  TEXT NOT NULL,
    notes       TEXT
);

-- =============================================================
-- shopee_accounts — user có thể quản lý nhiều TK Shopee affiliate.
-- id = content_id(name) — SHA-256(name) truncate i63, deterministic cross-device.
-- "Mặc định" row seed bởi runtime code (tính content_id) ở lần startup đầu.
-- =============================================================
CREATE TABLE IF NOT EXISTS shopee_accounts (
    id           INTEGER PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    color        TEXT,
    created_at   TEXT NOT NULL
);

-- =============================================================
-- imported_files — audit log mọi lần import CSV.
-- id = content_id(file_hash). day_date nullable + NO FK (file Shopee có thể
-- chứa nhiều ngày). shopee_account_id nullable (FB file không có account).
-- Không UNIQUE(file_hash) inline — thay bằng partial unique index bên dưới
-- để cho phép re-import sau revert.
-- =============================================================
CREATE TABLE IF NOT EXISTS imported_files (
    id                 INTEGER PRIMARY KEY,
    filename           TEXT NOT NULL,
    kind               TEXT NOT NULL,         -- 'shopee_clicks'|'shopee_commission'|'fb_ad_group'|'fb_campaign'
    imported_at        TEXT NOT NULL,
    row_count          INTEGER NOT NULL DEFAULT 0,
    file_hash          TEXT NOT NULL,
    stored_path        TEXT,
    day_date           TEXT,
    notes              TEXT,
    reverted_at        TEXT,                  -- NULL = active; non-NULL = soft-reverted
    shopee_account_id  INTEGER                -- informal ref shopee_accounts.id (nullable)
);

CREATE INDEX IF NOT EXISTS idx_imported_day     ON imported_files(day_date);
CREATE INDEX IF NOT EXISTS idx_imported_kind    ON imported_files(kind);
CREATE UNIQUE INDEX IF NOT EXISTS idx_imported_hash_active
    ON imported_files(file_hash) WHERE reverted_at IS NULL;

-- =============================================================
-- Mapping tables — track raw row thuộc về file nào (many-to-many).
-- Revert file X → DELETE mapping(file_id=X) → orphan raw rows xóa khi không
-- còn mapping nào trỏ tới.
-- FK ON UPDATE CASCADE (id là content-hash nhưng fresh = stable, CASCADE
-- defensive cho trường hợp future rebuild).
-- =============================================================
CREATE TABLE IF NOT EXISTS clicks_to_file (
    click_id   TEXT    NOT NULL,
    file_id    INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY(click_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_clicks_to_file_file  ON clicks_to_file(file_id);

CREATE TABLE IF NOT EXISTS orders_to_file (
    order_item_id INTEGER NOT NULL,
    file_id       INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY(order_item_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_to_file_file  ON orders_to_file(file_id);

CREATE TABLE IF NOT EXISTS fb_ads_to_file (
    fb_ad_id INTEGER NOT NULL,
    file_id  INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY(fb_ad_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_fb_ads_to_file_file  ON fb_ads_to_file(file_id);

-- =============================================================
-- raw_shopee_clicks — 1 row/click từ WebsiteClickReport.
-- PK = click_id (natural key từ CSV, stable cross-device).
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_shopee_clicks (
    click_id         TEXT PRIMARY KEY,
    click_time       TEXT NOT NULL,
    region           TEXT,
    sub_id_raw       TEXT,
    sub_id1          TEXT NOT NULL DEFAULT '',
    sub_id2          TEXT NOT NULL DEFAULT '',
    sub_id3          TEXT NOT NULL DEFAULT '',
    sub_id4          TEXT NOT NULL DEFAULT '',
    sub_id5          TEXT NOT NULL DEFAULT '',
    referrer         TEXT,
    day_date         TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id   INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    shopee_account_id INTEGER                -- informal ref, nullable
);

CREATE INDEX IF NOT EXISTS idx_clicks_day        ON raw_shopee_clicks(day_date);
CREATE INDEX IF NOT EXISTS idx_clicks_subid      ON raw_shopee_clicks(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_clicks_day_subid  ON raw_shopee_clicks(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_clicks_account    ON raw_shopee_clicks(shopee_account_id, day_date);

-- =============================================================
-- raw_shopee_order_items — 1 row/item. id = content_id(checkout_id, item_id, model_id).
-- UNIQUE(checkout_id, item_id, model_id) → ON CONFLICT DO UPDATE (status/price mới nhất).
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_shopee_order_items (
    id                     INTEGER PRIMARY KEY,
    order_id               TEXT NOT NULL,
    checkout_id            TEXT NOT NULL,
    item_id                TEXT NOT NULL,
    model_id               TEXT NOT NULL DEFAULT '',
    order_status           TEXT,
    order_time             TEXT,
    completed_time         TEXT,
    click_time             TEXT,
    shop_id                TEXT,
    shop_name              TEXT,
    shop_type              TEXT,
    item_name              TEXT,
    category_l1            TEXT,
    category_l2            TEXT,
    category_l3            TEXT,
    price                  REAL,
    quantity               INTEGER,
    order_value            REAL,
    refund_amount          REAL,
    net_commission         REAL,              -- Hoa hồng ròng affiliate nhận (CSV col 37)
    commission_total       REAL,              -- Tổng hoa hồng sản phẩm (CSV col 28, tra cứu)
    order_commission_total REAL,              -- Tổng hoa hồng đơn hàng (CSV col 31, pre-MCN)
    mcn_fee                REAL,              -- Phí quản lý MCN (CSV col 35)
    sub_id1                TEXT NOT NULL DEFAULT '',
    sub_id2                TEXT NOT NULL DEFAULT '',
    sub_id3                TEXT NOT NULL DEFAULT '',
    sub_id4                TEXT NOT NULL DEFAULT '',
    sub_id5                TEXT NOT NULL DEFAULT '',
    channel                TEXT,
    day_date               TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id         INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    shopee_account_id      INTEGER,            -- nullable
    UNIQUE(checkout_id, item_id, model_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_day        ON raw_shopee_order_items(day_date);
CREATE INDEX IF NOT EXISTS idx_orders_subid      ON raw_shopee_order_items(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_orders_day_subid  ON raw_shopee_order_items(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_orders_item       ON raw_shopee_order_items(item_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON raw_shopee_order_items(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_account    ON raw_shopee_order_items(shopee_account_id, day_date);

-- =============================================================
-- raw_fb_ads — unified FB ads (campaign + ad_group). id = content_id(day, level, name).
-- clicks/cpc normalized lúc INSERT (link_* ?? all_* ?? result_*). Aggregate
-- ưu tiên level='ad_group' per sub_id tuple để tránh double-count.
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_fb_ads (
    id                 INTEGER PRIMARY KEY,
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
    clicks             INTEGER,
    cpc                REAL,
    impressions        INTEGER,
    reach              INTEGER,
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id     INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    UNIQUE(day_date, level, name)
);

CREATE INDEX IF NOT EXISTS idx_fb_ads_day        ON raw_fb_ads(day_date);
CREATE INDEX IF NOT EXISTS idx_fb_ads_level      ON raw_fb_ads(day_date, level);
CREATE INDEX IF NOT EXISTS idx_fb_ads_subid      ON raw_fb_ads(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_fb_ads_day_subid  ON raw_fb_ads(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

-- =============================================================
-- manual_entries — user nhập tay/override. Identity: (sub_id1..5, day_date).
-- =============================================================
CREATE TABLE IF NOT EXISTS manual_entries (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    sub_id1             TEXT NOT NULL DEFAULT '',
    sub_id2             TEXT NOT NULL DEFAULT '',
    sub_id3             TEXT NOT NULL DEFAULT '',
    sub_id4             TEXT NOT NULL DEFAULT '',
    sub_id5             TEXT NOT NULL DEFAULT '',
    day_date            TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    display_name        TEXT,
    override_clicks     INTEGER,
    override_spend      REAL,
    override_cpc        REAL,
    override_orders     INTEGER,
    override_commission REAL,
    notes               TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    shopee_account_id   INTEGER,               -- nullable (app code set khi save)
    UNIQUE(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date)
);

CREATE INDEX IF NOT EXISTS idx_manual_day       ON manual_entries(day_date);
CREATE INDEX IF NOT EXISTS idx_manual_account   ON manual_entries(shopee_account_id, day_date);

-- =============================================================
-- tombstones — track deletion cho merge cross-device.
--   'day'          — cả ngày: entity_key = date.
--   'ui_row'       — 1 tuple sub_id: entity_key = '{day}|{s1}|...|{s5}'.
--   'manual_entry' — chỉ manual override (không raw): same key format.
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
-- sync_state — singleton HLC clock + owner UID.
-- =============================================================
CREATE TABLE IF NOT EXISTS sync_state (
    id                   INTEGER PRIMARY KEY CHECK (id = 1),
    owner_uid            TEXT,
    -- HLC-lite: mutation = max(now_ms, last_known_clock_ms + 1).
    last_known_clock_ms  INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO sync_state (id) VALUES (1);

-- =============================================================
-- Sync v9 tracking infrastructure.
-- =============================================================

-- Per-table cursor (upload + pull) cho delta sync.
CREATE TABLE IF NOT EXISTS sync_cursor_state (
    table_name            TEXT PRIMARY KEY,
    last_uploaded_cursor  TEXT NOT NULL DEFAULT '0',
    last_pulled_cursor    TEXT NOT NULL DEFAULT '0',
    last_uploaded_hash    TEXT,
    updated_at            TEXT NOT NULL
);

-- Seed cursor rows cho mọi syncable table.
INSERT OR IGNORE INTO sync_cursor_state (table_name, updated_at) VALUES
    ('imported_files',           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('raw_shopee_clicks',        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('raw_shopee_order_items',   strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('raw_fb_ads',               strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('clicks_to_file',           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('orders_to_file',           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('fb_ads_to_file',           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('manual_entries',           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('shopee_accounts',          strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    ('tombstones',               strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

-- Manifest state singleton (last remote etag + snapshot metadata).
CREATE TABLE IF NOT EXISTS sync_manifest_state (
    id                             INTEGER PRIMARY KEY CHECK (id = 1),
    last_remote_etag               TEXT,
    last_pulled_manifest_clock_ms  INTEGER NOT NULL DEFAULT 0,
    last_snapshot_key              TEXT,
    last_snapshot_clock_ms         INTEGER NOT NULL DEFAULT 0,
    fresh_install_pending          INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO sync_manifest_state (id) VALUES (1);

-- Ring buffer event log (user mutations) cho R2 flush.
CREATE TABLE IF NOT EXISTS sync_event_log (
    event_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    kind        TEXT NOT NULL,
    ctx_json    TEXT NOT NULL,
    uploaded_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_event_log_pending
    ON sync_event_log(uploaded_at) WHERE uploaded_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_sync_event_log_ts   ON sync_event_log(ts);
CREATE INDEX IF NOT EXISTS idx_sync_event_log_kind ON sync_event_log(kind);

-- =============================================================
-- Mapping cleanup triggers — DELETE raw row → DELETE mapping orphan.
-- =============================================================
CREATE TRIGGER IF NOT EXISTS trg_cleanup_click_mapping
AFTER DELETE ON raw_shopee_clicks
BEGIN
    DELETE FROM clicks_to_file WHERE click_id = OLD.click_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_cleanup_order_mapping
AFTER DELETE ON raw_shopee_order_items
BEGIN
    DELETE FROM orders_to_file WHERE order_item_id = OLD.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_cleanup_fb_ad_mapping
AFTER DELETE ON raw_fb_ads
BEGIN
    DELETE FROM fb_ads_to_file WHERE fb_ad_id = OLD.id;
END;
