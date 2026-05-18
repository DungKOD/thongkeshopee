-- Schema ThongKeShopee — local-only.
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
-- id = content_id(name) — SHA-256(name) truncate i63.
-- =============================================================
CREATE TABLE IF NOT EXISTS shopee_accounts (
    id           INTEGER PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    color        TEXT,
    created_at   TEXT NOT NULL
);

-- =============================================================
-- imported_files — audit log mọi lần import CSV.
-- =============================================================
CREATE TABLE IF NOT EXISTS imported_files (
    id                 INTEGER PRIMARY KEY,
    filename           TEXT NOT NULL,
    kind               TEXT NOT NULL,         -- 'shopee_clicks'|'shopee_commission'|'fb_ad_group'|'fb_campaign'|'fb_hierarchy'
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
    shopee_account_id INTEGER
);

CREATE INDEX IF NOT EXISTS idx_clicks_day        ON raw_shopee_clicks(day_date);
CREATE INDEX IF NOT EXISTS idx_clicks_subid      ON raw_shopee_clicks(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_clicks_day_subid  ON raw_shopee_clicks(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_clicks_account    ON raw_shopee_clicks(shopee_account_id, day_date);

-- =============================================================
-- raw_shopee_order_items — 1 row/item.
-- id = content_id(checkout_id, item_id, model_id).
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
    net_commission         REAL,
    commission_total       REAL,
    order_commission_total REAL,
    mcn_fee                REAL,
    sub_id1                TEXT NOT NULL DEFAULT '',
    sub_id2                TEXT NOT NULL DEFAULT '',
    sub_id3                TEXT NOT NULL DEFAULT '',
    sub_id4                TEXT NOT NULL DEFAULT '',
    sub_id5                TEXT NOT NULL DEFAULT '',
    channel                TEXT,
    day_date               TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id         INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    shopee_account_id      INTEGER,
    UNIQUE(checkout_id, item_id, model_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_day        ON raw_shopee_order_items(day_date);
CREATE INDEX IF NOT EXISTS idx_orders_subid      ON raw_shopee_order_items(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_orders_day_subid  ON raw_shopee_order_items(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_orders_item       ON raw_shopee_order_items(item_id);
CREATE INDEX IF NOT EXISTS idx_orders_status     ON raw_shopee_order_items(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_account    ON raw_shopee_order_items(shopee_account_id, day_date);

-- =============================================================
-- raw_fb_ads — unified FB ads (campaign + ad_group).
-- id = content_id(day, level, name).
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_fb_ads (
    id                 INTEGER PRIMARY KEY,
    level              TEXT NOT NULL,
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
    tax_rate           REAL NOT NULL DEFAULT 0,
    day_date           TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id     INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    UNIQUE(day_date, level, name)
);

CREATE INDEX IF NOT EXISTS idx_fb_ads_day        ON raw_fb_ads(day_date);
CREATE INDEX IF NOT EXISTS idx_fb_ads_level      ON raw_fb_ads(day_date, level);
CREATE INDEX IF NOT EXISTS idx_fb_ads_subid      ON raw_fb_ads(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_fb_ads_day_subid  ON raw_fb_ads(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

-- =============================================================
-- raw_fb_ads_hierarchy — FB ads theo cấu trúc 3 cấp đầy đủ
-- (chiến dịch → nhóm quảng cáo → quảng cáo). Bảng riêng song song với
-- raw_fb_ads (CSV cũ) để không break logic import hiện tại. Khi user
-- export FB report ở format mới có cột "Cấp độ phân phối"=ad cùng 3 cột
-- "Tên chiến dịch"/"Tên nhóm quảng cáo"/"Tên quảng cáo", row vào bảng này.
--
-- occurrence_idx: 0..N cho row có cùng (day, camp, adset, ad) — FB có thể
-- có 2+ ad cùng tên trong cùng adset (khác ad_id) → giữ nguyên cả 3 row,
-- không UPSERT collapse.
--
-- id = content_id(day_date, campaign_name, ad_set_name, ad_name, occurrence_idx).
-- =============================================================
CREATE TABLE IF NOT EXISTS raw_fb_ads_hierarchy (
    id              INTEGER PRIMARY KEY,
    campaign_name   TEXT NOT NULL,
    ad_set_name     TEXT NOT NULL,
    ad_name         TEXT NOT NULL,
    occurrence_idx  INTEGER NOT NULL DEFAULT 0,
    sub_id1         TEXT NOT NULL DEFAULT '',
    sub_id2         TEXT NOT NULL DEFAULT '',
    sub_id3         TEXT NOT NULL DEFAULT '',
    sub_id4         TEXT NOT NULL DEFAULT '',
    sub_id5         TEXT NOT NULL DEFAULT '',
    report_start    TEXT,
    report_end      TEXT,
    status          TEXT,
    spend           REAL,
    clicks          INTEGER,
    cpc             REAL,
    impressions     INTEGER,
    reach           INTEGER,
    tax_rate        REAL NOT NULL DEFAULT 0,
    day_date        TEXT NOT NULL REFERENCES days(date) ON DELETE CASCADE,
    source_file_id  INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    UNIQUE(day_date, campaign_name, ad_set_name, ad_name, occurrence_idx)
);

CREATE INDEX IF NOT EXISTS idx_fb_hier_day        ON raw_fb_ads_hierarchy(day_date);
CREATE INDEX IF NOT EXISTS idx_fb_hier_camp       ON raw_fb_ads_hierarchy(day_date, campaign_name);
CREATE INDEX IF NOT EXISTS idx_fb_hier_subid      ON raw_fb_ads_hierarchy(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);
CREATE INDEX IF NOT EXISTS idx_fb_hier_day_subid  ON raw_fb_ads_hierarchy(day_date, sub_id1, sub_id2, sub_id3, sub_id4, sub_id5);

-- Mapping fb_ads_hierarchy ↔ imported_files (parallel với fb_ads_to_file).
CREATE TABLE IF NOT EXISTS fb_ads_hier_to_file (
    fb_ad_id INTEGER NOT NULL,
    file_id  INTEGER NOT NULL REFERENCES imported_files(id) ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY(fb_ad_id, file_id)
);
CREATE INDEX IF NOT EXISTS idx_fb_ads_hier_to_file_file  ON fb_ads_hier_to_file(file_id);

-- =============================================================
-- manual_entries — user nhập tay/override.
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
    shopee_account_id   INTEGER,
    UNIQUE(sub_id1, sub_id2, sub_id3, sub_id4, sub_id5, day_date)
);

CREATE INDEX IF NOT EXISTS idx_manual_day       ON manual_entries(day_date);
CREATE INDEX IF NOT EXISTS idx_manual_account   ON manual_entries(shopee_account_id, day_date);

-- =============================================================
-- app_settings — key-value store cho user preferences.
-- =============================================================
CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

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

CREATE TRIGGER IF NOT EXISTS trg_cleanup_fb_ad_hier_mapping
AFTER DELETE ON raw_fb_ads_hierarchy
BEGIN
    DELETE FROM fb_ads_hier_to_file WHERE fb_ad_id = OLD.id;
END;
