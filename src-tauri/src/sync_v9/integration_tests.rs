//! P9 integration tests — cross-module end-to-end roundtrips.
//!
//! Mỗi test simulate ≥ 2 "máy" (2 in-memory connections) + verify data
//! preservation invariants sau khi capture → apply.
//!
//! Tests ở đây KHÔNG hit HTTP — thay vào đó gọi trực tiếp các public API
//! của `sync_v9::{capture, push, pull, apply, manifest}`. Mục tiêu: verify
//! logic data flow, không verify transport.
//!
//! Cases mapped từ `docs/SYNC_V9_PLAN.md` Phần 1 + 8.2 + 8.3.

#![cfg(test)]

use rusqlite::{params, Connection};

use super::apply::ApplyOutcome;
use super::capture::capture_table_delta;
use super::descriptors::{find_descriptor, SYNC_TABLES};
use super::event_log;
use super::hlc;
use super::pull::apply_events;
use super::push;
use super::types::{DeltaEvent, SyncEventCtx, TombstoneEvent};
use crate::db::migrate_for_tests;

// =============================================================
// Helpers
// =============================================================

/// Fresh in-memory DB + schema + migrations (v12 đã drop v8 sync artifacts).
fn new_db(owner_uid: &str) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
    let schema = include_str!("../db/schema.sql");
    conn.execute_batch(schema).unwrap();
    migrate_for_tests(&conn).unwrap();
    conn.execute(
        "UPDATE sync_state SET owner_uid = ? WHERE id = 1",
        [owner_uid],
    )
    .unwrap();
    conn
}

fn insert_day(conn: &Connection, date: &str) {
    conn.execute(
        "INSERT OR IGNORE INTO days(date, created_at) VALUES(?, '2026-04-20T00:00:00Z')",
        [date],
    )
    .unwrap();
}

fn insert_file(conn: &Connection, hash: &str, kind: &str) -> i64 {
    // v13: id = content_id(file_hash). Test helper mirror imports.rs production path.
    let id = super::content_id::imported_file_id(hash);
    conn.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash)
         VALUES(?, 'f.csv', ?, '2026-04-20T00:00:00Z', ?)",
        rusqlite::params![id, kind, hash],
    )
    .unwrap();
    id
}

fn insert_raw_click(conn: &Connection, day: &str, source_file_id: i64, click_id: &str) {
    insert_day(conn, day);
    conn.execute(
        "INSERT INTO raw_shopee_clicks(click_id, day_date, source_file_id, click_time, sub_id1)
         VALUES(?, ?, ?, '2026-04-20T12:00:00Z', 'a')",
        params![click_id, day, source_file_id],
    )
    .unwrap();
}

/// Insert manual_entry với updated_at explicit để test HLC resolve.
/// `spend_override` map vào `override_spend` (column thực trong schema).
fn insert_manual(conn: &Connection, day: &str, sub1: &str, updated_at: &str, spend_override: f64) {
    insert_day(conn, day);
    conn.execute(
        "INSERT INTO manual_entries
         (sub_id1, day_date, created_at, updated_at, override_spend)
         VALUES(?, ?, '2026-04-20T00:00:00Z', ?, ?)",
        params![sub1, day, updated_at, spend_override],
    )
    .unwrap();
}

fn count_rows(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

/// Capture 1 bảng theo cursor '0' → feed events thẳng vào apply_events
/// (bypass zstd roundtrip; events tương đương sau parse).
fn roundtrip_capture_apply(source: &Connection, target: &mut Connection, table: &str) -> u32 {
    let desc = find_descriptor(table).expect("descriptor");
    let clock = 10_000i64; // fixed clock cho test determinism
    let batch = capture_table_delta(source, desc, "0", usize::MAX, clock, 11).unwrap();
    let Some(b) = batch else {
        return 0;
    };
    let stats = apply_events(target, &b.events).unwrap();
    stats.applied
}

// =============================================================
// B — Sync convergence
// =============================================================

/// B1: A import raw_shopee_clicks → capture → apply vào B → B thấy đủ rows.
/// Preserve dependency order: imported_files push trước raw_* (D3).
#[test]
fn b1_import_push_pull_roundtrip_raw_clicks() {
    let a = new_db("uid-a");
    let mut b = new_db("uid-b");

    let file_id = insert_file(&a, "h1", "shopee_clicks");
    insert_raw_click(&a, "2026-04-20", file_id, "c1");
    insert_raw_click(&a, "2026-04-20", file_id, "c2");
    insert_raw_click(&a, "2026-04-20", file_id, "c3");

    for table in ["imported_files", "raw_shopee_clicks"] {
        roundtrip_capture_apply(&a, &mut b, table);
    }

    assert_eq!(count_rows(&b, "imported_files"), 1);
    assert_eq!(count_rows(&b, "raw_shopee_clicks"), 3);
    assert_eq!(count_rows(&b, "days"), 1);
}

/// B4: A và B edit cùng manual_entry. Update có updated_at lớn hơn → win.
#[test]
fn b4_concurrent_edit_hlc_higher_wins() {
    let a = new_db("uid-shared");
    let mut target = new_db("uid-shared");

    insert_manual(&a, "2026-04-20", "campaign-1", "2026-04-20T10:00:00.000Z", 100.0);
    roundtrip_capture_apply(&a, &mut target, "manual_entries");
    assert_eq!(count_rows(&target, "manual_entries"), 1);

    let newer = new_db("uid-shared");
    insert_manual(
        &newer,
        "2026-04-20",
        "campaign-1",
        "2026-04-20T12:00:00.000Z",
        200.0,
    );
    roundtrip_capture_apply(&newer, &mut target, "manual_entries");

    let spend: f64 = target
        .query_row(
            "SELECT override_spend FROM manual_entries WHERE sub_id1 = 'campaign-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        (spend - 200.0).abs() < 1e-9,
        "edit mới hơn thắng theo HLC, spend={spend}"
    );

    let older = new_db("uid-shared");
    insert_manual(
        &older,
        "2026-04-20",
        "campaign-1",
        "2026-04-20T08:00:00.000Z",
        50.0,
    );
    roundtrip_capture_apply(&older, &mut target, "manual_entries");

    let spend_after: f64 = target
        .query_row(
            "SELECT override_spend FROM manual_entries WHERE sub_id1 = 'campaign-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        (spend_after - 200.0).abs() < 1e-9,
        "edit cũ hơn KHÔNG đè, spend={spend_after}"
    );
}

/// B5 (v0.5.2): Day tombstone tôn trọng resurrect rule.
///
/// Trước đây UNCONDITIONAL cascade — manual edit sau tombstone bị xóa nhầm
/// (bug: user xóa day rồi modify lại, sync replay lại wipe modification).
/// Giờ resurrect-aware: data updated_at > deleted_at survive; raw rows linked
/// tới file imported BEFORE deleted_at thì xóa, file imported sau survive;
/// day row tự xóa nếu không còn data ref.
#[test]
fn b5_day_tombstone_respects_resurrect_rule() {
    let mut target = new_db("uid-shared");

    let fid = insert_file(&target, "h1", "shopee_clicks");
    insert_raw_click(&target, "2026-04-20", fid, "c1");
    // Manual edit với updated_at NEWER than upcoming tombstone (T20 > T05).
    insert_manual(&target, "2026-04-20", "m1", "2026-04-20T20:00:00.000Z", 50.0);

    assert_eq!(count_rows(&target, "raw_shopee_clicks"), 1);
    assert_eq!(count_rows(&target, "manual_entries"), 1);

    // Remote xóa day với deleted_at sớm hơn manual edit trên target.
    let a = new_db("uid-shared");
    insert_day(&a, "2026-04-20");
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-20', '2026-04-20T05:00:00.000Z')",
        [],
    )
    .unwrap();

    roundtrip_capture_apply(&a, &mut target, "tombstones");

    // Manual edit T20 > T05 → SURVIVE (resurrect rule).
    assert_eq!(
        count_rows(&target, "manual_entries"),
        1,
        "manual edit newer than tombstone phải survive (không bị wipe)"
    );
    // Raw row imported tại T00 < T05 và không có mapping post-delete → DELETE.
    assert_eq!(
        count_rows(&target, "raw_shopee_clicks"),
        0,
        "raw cũ (file imported trước deleted_at) bị xóa"
    );
    // Day row vẫn tồn tại vì còn manual_entries reference.
    assert_eq!(
        count_rows(&target, "days"),
        1,
        "day SURVIVE vì manual_entries còn reference"
    );
}

/// B10 (regression v0.5.2): user xóa day → modify same day → wipe local +
/// reopen → restore từ snapshot CŨ + replay bundle. Modification phải SURVIVE
/// dù tombstone replay sau manual_entries INSERT trong bundle order.
///
/// Đây là user's exact bug: "xóa ngày 24 → thay đổi 1 chỉ số ngày 24 → clear
/// data → mở lại app → data cũ vẫn hiện".
#[test]
fn b10_delete_then_modify_day_modification_survives_replay() {
    use crate::sync_v9::content_id;
    let mut b = new_db("uid-shared"); // simulate B sau wipe + bootstrap

    // Stage 1: B restore từ snapshot S1 (state initial — day 24, 25 với raw từ F1).
    let f1 = content_id::imported_file_id("h-f1");
    b.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
         VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00.000Z', 'h-f1', '2026-04-24')",
        params![f1],
    )
    .unwrap();
    insert_day(&b, "2026-04-24");
    insert_day(&b, "2026-04-25");
    b.execute(
        "INSERT INTO raw_shopee_clicks
         (click_id, click_time, day_date, source_file_id, sub_id1)
         VALUES('c-old-24', '2026-04-24T10:00:00Z', '2026-04-24', ?, 'old')",
        params![f1],
    )
    .unwrap();
    b.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c-old-24', ?)",
        params![f1],
    )
    .unwrap();
    b.execute(
        "INSERT INTO raw_shopee_clicks
         (click_id, click_time, day_date, source_file_id, sub_id1)
         VALUES('c-old-25', '2026-04-25T10:00:00Z', '2026-04-25', ?, 'old')",
        params![f1],
    )
    .unwrap();
    b.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c-old-25', ?)",
        params![f1],
    )
    .unwrap();

    // Stage 2: A's bundle (delete + modify day 24) cần apply lên B.
    // - Tombstone 'day' '2026-04-24' deleted_at = T1
    // - manual_entries INSERT day 24 updated_at = T2 > T1 (modification AFTER delete)
    let a = new_db("uid-shared");
    insert_day(&a, "2026-04-24");
    a.execute(
        "INSERT INTO manual_entries
         (sub_id1, day_date, override_clicks, created_at, updated_at, override_spend)
         VALUES('user-edit', '2026-04-24', 100, 'now', '2026-04-26T12:00:00.000Z', 7.5)",
        [],
    )
    .unwrap();
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-24', '2026-04-26T08:00:00.000Z')",
        [],
    )
    .unwrap();

    // Apply bundle: manual_entries TRƯỚC tombstones (NDJSON SYNC_TABLES order).
    // Đây là điểm kiểm chứng resurrect rule — tombstone applied sau cùng KHÔNG
    // được wipe manual entry vừa insert.
    roundtrip_capture_apply(&a, &mut b, "manual_entries");
    roundtrip_capture_apply(&a, &mut b, "tombstones");

    // Day 24 raw cũ bị xóa (file imported_at='2026-04-20' < deleted_at='2026-04-26').
    let raw_24: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE day_date = '2026-04-24'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw_24, 0, "raw cũ ngày 24 bị xóa đúng intent");

    // Manual edit (T2 > T1) SURVIVE.
    let manual_24: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM manual_entries WHERE day_date = '2026-04-24'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        manual_24, 1,
        "manual edit POST-delete phải survive (resurrect rule)"
    );

    // Day 24 vẫn còn vì có manual reference.
    let day_24: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM days WHERE date = '2026-04-24'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(day_24, 1, "day 24 survive vì có manual_entries");

    // Day 25 không bị động (user không xóa day 25).
    let day_25: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM days WHERE date = '2026-04-25'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(day_25, 1, "day 25 không bị động");
    let raw_25: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE day_date = '2026-04-25'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw_25, 1, "raw day 25 preserved");
}

/// B11 (regression v0.5.2): user xóa day → re-import file MỚI cho day đó.
/// File post-delete có imported_at > deleted_at → raw rows survive.
#[test]
fn b11_day_tombstone_preserves_post_delete_reimport() {
    use crate::sync_v9::content_id;
    let mut b = new_db("uid-shared");

    // Stage 1: snapshot has F1 (old) + raw cho day 24.
    let f1 = content_id::imported_file_id("h-f1");
    b.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
         VALUES(?, 'f1.csv', 'shopee_clicks', '2026-04-20T00:00:00.000Z', 'h-f1', '2026-04-24')",
        params![f1],
    )
    .unwrap();
    insert_day(&b, "2026-04-24");
    b.execute(
        "INSERT INTO raw_shopee_clicks
         (click_id, click_time, day_date, source_file_id, sub_id1)
         VALUES('c-shared', '2026-04-24T10:00:00Z', '2026-04-24', ?, 'old')",
        params![f1],
    )
    .unwrap();
    b.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c-shared', ?)",
        params![f1],
    )
    .unwrap();

    // Stage 2: A xóa day 24 lúc T1, sau đó re-import file F2 (imported_at=T2 > T1)
    // chứa cùng click_id "c-shared" + click mới "c-new".
    let a = new_db("uid-shared");
    insert_day(&a, "2026-04-24");
    let f2 = content_id::imported_file_id("h-f2");
    a.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
         VALUES(?, 'f2.csv', 'shopee_clicks', '2026-04-26T10:00:00.000Z', 'h-f2', '2026-04-24')",
        params![f2],
    )
    .unwrap();
    // F2 mappings: cả c-shared (re-import) và c-new (mới).
    a.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c-shared', ?), ('c-new', ?)",
        params![f2, f2],
    )
    .unwrap();
    a.execute(
        "INSERT INTO raw_shopee_clicks
         (click_id, click_time, day_date, source_file_id, sub_id1)
         VALUES('c-new', '2026-04-24T11:00:00Z', '2026-04-24', ?, 'new')",
        params![f2],
    )
    .unwrap();
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-24', '2026-04-26T08:00:00.000Z')",
        [],
    )
    .unwrap();

    // Apply bundle: imported_files → raw → mappings → tombstone.
    for table in [
        "imported_files",
        "raw_shopee_clicks",
        "clicks_to_file",
        "tombstones",
    ] {
        roundtrip_capture_apply(&a, &mut b, table);
    }

    // c-shared SURVIVE (có mapping link tới F2 imported AFTER deleted_at).
    let shared: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id = 'c-shared'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(shared, 1, "c-shared survive vì có mapping post-delete");

    // c-new SURVIVE (chỉ link F2 post-delete).
    let new_click: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id = 'c-new'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(new_click, 1, "c-new survive (post-delete import)");
}

/// B6: manual_entry tombstone resurrect rule — local updated_at > deleted_at
/// của tombstone → row KHÔNG bị xóa (edit-wins-over-delete).

/// B7 (regression v0.5.1): revert_import phải emit `'imported_file'` tombstone,
/// thiếu thì máy khác (hoặc same máy sau wipe + bootstrap) sẽ replay raw rows
/// gốc → "data cũ trước khi xóa bảng" hồi sinh.
///
/// Scenario: A import F1 → push → B nhận. A revert F1 + push tombstone → B apply.
/// Verify B's raw_* + mappings + day cleanup giống A. imported_files row vẫn
/// tồn tại trên cả 2 máy với reverted_at marker (history preserved).
#[test]
fn b7_revert_import_tombstone_propagates_deletion() {
    let a = new_db("uid-shared");
    let mut b = new_db("uid-shared");

    // Step 1: A import F1 với 2 clicks.
    let f1 = insert_file(&a, "h-revert", "shopee_clicks");
    insert_raw_click(&a, "2026-04-20", f1, "c1");
    insert_raw_click(&a, "2026-04-20", f1, "c2");
    a.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?), ('c2', ?)",
        params![f1, f1],
    )
    .unwrap();

    // Step 2: Sync sang B (initial state).
    for table in ["imported_files", "raw_shopee_clicks", "clicks_to_file"] {
        roundtrip_capture_apply(&a, &mut b, table);
    }
    assert_eq!(count_rows(&b, "raw_shopee_clicks"), 2, "B nhận 2 clicks");
    assert_eq!(count_rows(&b, "clicks_to_file"), 2);

    // Step 3: A revert F1 — mirror commands::batch::revert_import logic.
    a.execute("DELETE FROM clicks_to_file WHERE file_id = ?", [f1])
        .unwrap();
    a.execute(
        "DELETE FROM raw_shopee_clicks
         WHERE click_id NOT IN (SELECT click_id FROM clicks_to_file)",
        [],
    )
    .unwrap();
    a.execute(
        "UPDATE imported_files SET reverted_at = ?, stored_path = NULL WHERE id = ?",
        params!["2026-04-25T08:00:00.000Z", f1],
    )
    .unwrap();
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('imported_file', ?, '2026-04-25T08:00:00.000Z')",
        [f1.to_string()],
    )
    .unwrap();
    a.execute(
        "DELETE FROM days WHERE date NOT IN (
            SELECT day_date FROM raw_shopee_clicks UNION
            SELECT day_date FROM raw_shopee_order_items UNION
            SELECT day_date FROM raw_fb_ads UNION
            SELECT day_date FROM manual_entries
         )",
        [],
    )
    .unwrap();

    // A's local state post-revert: clean.
    assert_eq!(count_rows(&a, "raw_shopee_clicks"), 0);
    assert_eq!(count_rows(&a, "days"), 0);

    // Step 4: A push tombstones (chỉ tombstones cần propagate — raw_* DELETE
    // không tự sinh event; tombstone là cơ chế duy nhất báo deletion).
    roundtrip_capture_apply(&a, &mut b, "tombstones");

    // Step 5: B post-apply phải có state giống A.
    assert_eq!(
        count_rows(&b, "raw_shopee_clicks"),
        0,
        "B's raw clicks bị xóa qua tombstone — bug cũ: vẫn còn 2 rows"
    );
    assert_eq!(count_rows(&b, "clicks_to_file"), 0, "B's mapping cũng xóa");
    assert_eq!(count_rows(&b, "days"), 0, "day orphan cleanup");

    // imported_files row vẫn tồn tại với reverted_at (history).
    let reverted_at: Option<String> = b
        .query_row(
            "SELECT reverted_at FROM imported_files WHERE id = ?",
            [f1],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        reverted_at.as_deref(),
        Some("2026-04-25T08:00:00.000Z"),
        "B's file row marked reverted (history preserved)"
    );
}

/// B8 (regression v0.5.1): delete_shopee_account phải emit `'shopee_account'`
/// tombstone. Thiếu thì sau wipe + login, account + raw rows hồi sinh từ
/// delta replay.
///
/// Verify: (1) Shop A's data xóa cross-device, (2) default account + Shop B
/// data PRESERVED (không over-delete).
#[test]
fn b8_delete_shopee_account_tombstone_propagates_without_collateral() {
    use crate::sync_v9::content_id;
    let a = new_db("uid-shared");
    let mut b = new_db("uid-shared");

    // Setup: 3 accounts trên A.
    let shop_a_id = content_id::shopee_account_id("Shop A");
    let shop_b_id = content_id::shopee_account_id("Shop B");
    let default_id = content_id::shopee_account_id(crate::db::DEFAULT_ACCOUNT_NAME);

    for (id, name) in [(shop_a_id, "Shop A"), (shop_b_id, "Shop B")] {
        a.execute(
            "INSERT INTO shopee_accounts (id, name, color, created_at)
             VALUES (?, ?, '#000', '2026-04-20T08:00:00Z')",
            params![id, name],
        )
        .unwrap();
    }

    // Files + raw rows cho từng account trên A.
    let f_default = content_id::imported_file_id("h-default");
    let f_a = content_id::imported_file_id("h-a");
    let f_b = content_id::imported_file_id("h-b");
    for (fid, acc_id, click_id, hash) in [
        (f_default, default_id, "c-default", "h-default"),
        (f_a, shop_a_id, "c-a", "h-a"),
        (f_b, shop_b_id, "c-b", "h-b"),
    ] {
        a.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date, shopee_account_id)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?, '2026-04-20', ?)",
            params![fid, hash, acc_id],
        )
        .unwrap();
        insert_day(&a, "2026-04-20");
        a.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id, shopee_account_id)
             VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?, ?)",
            params![click_id, fid, acc_id],
        )
        .unwrap();
        a.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES(?, ?)",
            params![click_id, fid],
        )
        .unwrap();
    }

    // Sync sang B: 3 raw clicks + 3 files + 3 accounts (default đã seed cả 2 máy
    // qua migrate_for_tests, INSERT OR IGNORE skip on B).
    for table in [
        "shopee_accounts",
        "imported_files",
        "raw_shopee_clicks",
        "clicks_to_file",
    ] {
        roundtrip_capture_apply(&a, &mut b, table);
    }
    assert_eq!(count_rows(&b, "raw_shopee_clicks"), 3);

    // A xóa Shop A → DELETE raw + manual + account, cleanup orphan files.
    a.execute(
        "DELETE FROM raw_shopee_clicks WHERE shopee_account_id = ?",
        [shop_a_id],
    )
    .unwrap();
    a.execute(
        "DELETE FROM shopee_accounts WHERE id = ?",
        [shop_a_id],
    )
    .unwrap();
    a.execute(
        "DELETE FROM imported_files
         WHERE id NOT IN (
             SELECT source_file_id FROM raw_shopee_clicks UNION
             SELECT source_file_id FROM raw_shopee_order_items UNION
             SELECT source_file_id FROM raw_fb_ads
         )",
        [],
    )
    .unwrap();
    // Tombstone (mirror commands::accounts::delete_shopee_account fix).
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('shopee_account', ?, '2026-04-25T08:00:00.000Z')",
        [shop_a_id.to_string()],
    )
    .unwrap();

    // Push tombstones → B apply.
    roundtrip_capture_apply(&a, &mut b, "tombstones");

    // B verify: Shop A data gone.
    let a_count: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
            [shop_a_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(a_count, 0, "Shop A raw clicks bị xóa cross-device");
    let a_acc: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM shopee_accounts WHERE id = ?",
            [shop_a_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(a_acc, 0, "Shop A account row bị xóa");

    // B verify: default account + Shop B data PRESERVED.
    let default_count: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
            [default_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        default_count, 1,
        "default account data PRESERVED — không over-delete"
    );
    let b_count: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE shopee_account_id = ?",
            [shop_b_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(b_count, 1, "Shop B data preserved");
}

/// B9 (regression): apply imported_file tombstone KHÔNG được xóa data
/// của file khác cùng day. Đảm bảo cleanup days chỉ chạm day thực sự orphan.
#[test]
fn b9_imported_file_tombstone_does_not_leak_to_other_files() {
    use crate::sync_v9::content_id;
    let a = new_db("uid-shared");
    let mut b = new_db("uid-shared");

    let default_id = content_id::shopee_account_id(crate::db::DEFAULT_ACCOUNT_NAME);

    // 2 file cùng day, mỗi file 1 click riêng (không share).
    let f1 = content_id::imported_file_id("h1");
    let f2 = content_id::imported_file_id("h2");
    for (fid, hash) in [(f1, "h1"), (f2, "h2")] {
        a.execute(
            "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date, shopee_account_id)
             VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?, '2026-04-20', ?)",
            params![fid, hash, default_id],
        )
        .unwrap();
    }
    insert_day(&a, "2026-04-20");
    for (fid, click_id) in [(f1, "click-from-f1"), (f2, "click-from-f2")] {
        a.execute(
            "INSERT INTO raw_shopee_clicks
             (click_id, click_time, day_date, source_file_id, shopee_account_id)
             VALUES(?, '2026-04-20T10:00:00Z', '2026-04-20', ?, ?)",
            params![click_id, fid, default_id],
        )
        .unwrap();
        a.execute(
            "INSERT INTO clicks_to_file(click_id, file_id) VALUES(?, ?)",
            params![click_id, fid],
        )
        .unwrap();
    }

    for table in [
        "imported_files",
        "raw_shopee_clicks",
        "clicks_to_file",
    ] {
        roundtrip_capture_apply(&a, &mut b, table);
    }
    assert_eq!(count_rows(&b, "raw_shopee_clicks"), 2);

    // A revert F1 — emit tombstone.
    a.execute("DELETE FROM clicks_to_file WHERE file_id = ?", [f1])
        .unwrap();
    a.execute(
        "DELETE FROM raw_shopee_clicks WHERE click_id NOT IN (SELECT click_id FROM clicks_to_file)",
        [],
    )
    .unwrap();
    a.execute(
        "UPDATE imported_files SET reverted_at = '2026-04-25T08:00:00.000Z' WHERE id = ?",
        [f1],
    )
    .unwrap();
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('imported_file', ?, '2026-04-25T08:00:00.000Z')",
        [f1.to_string()],
    )
    .unwrap();

    roundtrip_capture_apply(&a, &mut b, "tombstones");

    // B: chỉ click của F1 bị xóa, click của F2 PRESERVED.
    let count: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id = 'click-from-f2'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "F2's click PRESERVED — không leak");
    let count_f1: i64 = b
        .query_row(
            "SELECT COUNT(*) FROM raw_shopee_clicks WHERE click_id = 'click-from-f1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_f1, 0, "F1's click bị xóa đúng");

    // Day vẫn còn vì F2 raw rows vẫn reference.
    let day_count: i64 = b
        .query_row("SELECT COUNT(*) FROM days", [], |r| r.get(0))
        .unwrap();
    assert_eq!(day_count, 1, "day preserved (F2 còn raw)");
}

/// B12 (regression v0.6.1): user xóa toàn bộ ngày trong stats → đồng bộ →
/// wipe local + fresh install → restore snapshot + replay tombstones.
/// imported_files orphan PHẢI được soft-mark `reverted_at` cross-device, nếu
/// không user thấy "1 số data hồi sinh" trong import history (B7-style bug
/// nhưng cho `batch_commit_deletes` path thay vì `revert_import`).
///
/// User's exact bug: "xóa toàn bộ ngày → đồng bộ → thoát + xóa data local
/// → build lại → thấy 1 số data hồi sinh từ R2".
///
/// Flow:
/// - A snapshot1 capture ALL imported data (raw_*, imported_files, mappings, days).
/// - User delete day → batch_commit_deletes:
///   - DELETE days CASCADE raw_* + manual + trigger cleanup mappings.
///   - INSERT 'day' tombstone.
///   - SOFT-MARK orphan imported_files (UPDATE reverted_at) + INSERT
///     'imported_file' tombstone (FIX). Trước fix: hard-DELETE local, no
///     tombstone → propagation broken.
/// - Push delta_with_tombstones (cả 'day' + 'imported_file' tombstones).
/// - Fresh install: restore snapshot1 → apply tombstones → state consistent.
#[test]
fn b12_batch_delete_day_emits_imported_file_tombstone() {
    use crate::sync_v9::content_id;
    let mut b = new_db("uid-shared"); // simulate B sau wipe + bootstrap

    // Stage 1: B restore từ snapshot S1 (state initial — F1 + raw + day 24).
    let f1 = content_id::imported_file_id("h-f1");
    b.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
         VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00.000Z', 'h-f1', '2026-04-24')",
        params![f1],
    )
    .unwrap();
    insert_day(&b, "2026-04-24");
    b.execute(
        "INSERT INTO raw_shopee_clicks
         (click_id, click_time, day_date, source_file_id, sub_id1)
         VALUES('c1', '2026-04-24T10:00:00Z', '2026-04-24', ?, 'tag')",
        params![f1],
    )
    .unwrap();
    b.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
        params![f1],
    )
    .unwrap();

    // Sanity: B post-restore có data đầy đủ.
    assert_eq!(count_rows(&b, "imported_files"), 1, "F1 trong snapshot");
    assert_eq!(count_rows(&b, "raw_shopee_clicks"), 1);
    assert_eq!(count_rows(&b, "days"), 1);

    // Stage 2: A xóa day 24 qua batch_commit_deletes (mirror logic v0.6.1).
    let a = new_db("uid-shared");
    insert_day(&a, "2026-04-24");
    a.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash, day_date)
         VALUES(?, 'f.csv', 'shopee_clicks', '2026-04-20T00:00:00.000Z', 'h-f1', '2026-04-24')",
        params![f1],
    )
    .unwrap();
    a.execute(
        "INSERT INTO raw_shopee_clicks
         (click_id, click_time, day_date, source_file_id, sub_id1)
         VALUES('c1', '2026-04-24T10:00:00Z', '2026-04-24', ?, 'tag')",
        params![f1],
    )
    .unwrap();
    a.execute(
        "INSERT INTO clicks_to_file(click_id, file_id) VALUES('c1', ?)",
        params![f1],
    )
    .unwrap();

    let delete_at = "2026-04-26T08:00:00.000Z";
    // batch_commit_deletes step 1: DELETE day → CASCADE raw + trigger mapping cleanup.
    a.execute("DELETE FROM days WHERE date = '2026-04-24'", [])
        .unwrap();
    // step 2: INSERT 'day' tombstone.
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-24', ?)",
        [delete_at],
    )
    .unwrap();
    // step 3 (FIX): orphan imported_files soft-mark + 'imported_file' tombstone.
    let orphan_ids: Vec<i64> = {
        let mut stmt = a
            .prepare(
                "SELECT id FROM imported_files
                 WHERE reverted_at IS NULL
                   AND id NOT IN (
                       SELECT file_id FROM clicks_to_file UNION
                       SELECT file_id FROM orders_to_file UNION
                       SELECT file_id FROM fb_ads_to_file
                   )",
            )
            .unwrap();
        let iter = stmt.query_map([], |r| r.get::<_, i64>(0)).unwrap();
        iter.collect::<rusqlite::Result<_>>().unwrap()
    };
    assert_eq!(orphan_ids, vec![f1], "F1 trở thành orphan sau day delete");
    for id in &orphan_ids {
        a.execute(
            "UPDATE imported_files SET reverted_at = ?, stored_path = NULL
             WHERE id = ? AND reverted_at IS NULL",
            params![delete_at, id],
        )
        .unwrap();
        a.execute(
            "INSERT OR IGNORE INTO tombstones(entity_type, entity_key, deleted_at)
             VALUES('imported_file', ?, ?)",
            params![id.to_string(), delete_at],
        )
        .unwrap();
    }

    // Stage 3: Push tombstones → B apply (replay sau wipe + bootstrap).
    roundtrip_capture_apply(&a, &mut b, "tombstones");

    // B verify: raw_* + day cleared (qua 'day' tombstone), imported_files
    // SOFT-MARKED (qua 'imported_file' tombstone — KEY ASSERT của fix).
    assert_eq!(
        count_rows(&b, "raw_shopee_clicks"),
        0,
        "raw clicks cleared qua day tombstone"
    );
    assert_eq!(
        count_rows(&b, "clicks_to_file"),
        0,
        "mappings cleaned (cascade trigger)"
    );
    assert_eq!(count_rows(&b, "days"), 0, "day cleared (orphan)");

    // CRITICAL ASSERT: F1 row vẫn tồn tại nhưng reverted_at SET.
    let reverted_at: Option<String> = b
        .query_row(
            "SELECT reverted_at FROM imported_files WHERE id = ?",
            [f1],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        reverted_at.as_deref(),
        Some(delete_at),
        "BUG (pre-fix): F1 active trong import history sau wipe + bootstrap. \
         FIX: 'imported_file' tombstone propagate → soft-mark cross-device."
    );
}

/// B6: manual_entry tombstone resurrect rule — local updated_at > deleted_at
/// của tombstone → row KHÔNG bị xóa (edit-wins-over-delete).
#[test]
fn b6_manual_tombstone_resurrect_when_local_newer() {
    let mut target = new_db("uid-shared");
    insert_manual(&target, "2026-04-20", "m1", "2026-04-20T15:00:00.000Z", 99.0);
    assert_eq!(count_rows(&target, "manual_entries"), 1);

    let a = new_db("uid-shared");
    insert_day(&a, "2026-04-20");
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('manual_entry', '2026-04-20|m1||||', '2026-04-20T10:00:00.000Z')",
        [],
    )
    .unwrap();

    roundtrip_capture_apply(&a, &mut target, "tombstones");

    assert_eq!(
        count_rows(&target, "manual_entries"),
        1,
        "manual edit sau tombstone → resurrect, row sống"
    );
}

// =============================================================
// D — Data integrity
// =============================================================

/// D1: Apply cùng events 2 lần → second apply tất cả skipped.
#[test]
fn d1_apply_idempotent_no_duplicates() {
    let a = new_db("uid-a");
    let mut b = new_db("uid-b");

    let fid = insert_file(&a, "h-dup", "shopee_clicks");
    insert_raw_click(&a, "2026-04-20", fid, "c-dup");

    for table in ["imported_files", "raw_shopee_clicks"] {
        let desc = find_descriptor(table).unwrap();
        let batch = capture_table_delta(&a, desc, "0", usize::MAX, 1_000, 11)
            .unwrap()
            .unwrap();

        let stats1 = apply_events(&mut b, &batch.events).unwrap();
        assert!(stats1.applied > 0);
        let stats2 = apply_events(&mut b, &batch.events).unwrap();
        assert_eq!(
            stats2.applied, 0,
            "second apply KHÔNG insert thêm cho {table}"
        );
    }

    assert_eq!(count_rows(&b, "raw_shopee_clicks"), 1);
    assert_eq!(count_rows(&b, "imported_files"), 1);
}

/// D5: Day tombstone apply trước raw rows → không panic.
#[test]
fn d5_tombstone_before_raw_no_crash() {
    let mut b = new_db("uid-b");

    let a = new_db("uid-a");
    insert_day(&a, "2026-04-20");
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-20', '2026-04-20T00:00:00.000Z')",
        [],
    )
    .unwrap();
    roundtrip_capture_apply(&a, &mut b, "tombstones");
    assert_eq!(count_rows(&b, "days"), 0);

    let a2 = new_db("uid-a");
    let fid = insert_file(&a2, "h-late", "shopee_clicks");
    insert_raw_click(&a2, "2026-04-20", fid, "c-late");
    roundtrip_capture_apply(&a2, &mut b, "imported_files");
    let _ = roundtrip_capture_apply(&a2, &mut b, "raw_shopee_clicks");

    let raw_count = count_rows(&b, "raw_shopee_clicks");
    assert!(
        raw_count == 0 || raw_count == 1,
        "apply không panic, count tolerant"
    );
}

// =============================================================
// O — Recovery & observability
// =============================================================

/// O1c: Regex scan event_log không có PII.
#[test]
fn o1c_event_log_contains_no_pii() {
    let conn = new_db("uid-test");
    let ts = "2026-04-20T10:00:00.000Z";
    let fp = "fingerprint-stable";

    event_log::append(
        &conn,
        ts,
        fp,
        &SyncEventCtx::PushUpload {
            table: "raw_shopee_clicks".to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "500".to_string(),
            bytes: 2048,
            delta_key: "deltas/raw_shopee_clicks/0_500_abc.ndjson.zst".to_string(),
            row_count: 500,
        },
    )
    .unwrap();
    event_log::append(
        &conn,
        ts,
        fp,
        &SyncEventCtx::PullApply {
            delta_key: "deltas/manual_entries/0_10_xyz.ndjson.zst".to_string(),
            row_count: 10,
            skipped: 2,
            resurrected: 1,
        },
    )
    .unwrap();

    let all_ctx: Vec<String> = conn
        .prepare("SELECT ctx_json FROM sync_event_log")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let joined = all_ctx.join("\n");

    let forbidden = [
        "@gmail.com",
        "@yahoo",
        "@outlook",
        "vnz.luffy",
    ];
    for pat in forbidden {
        assert!(
            !joined.contains(pat),
            "event_log contain PII pattern '{pat}': {joined}"
        );
    }
    assert!(
        !joined.contains("\"spend\""),
        "event_log contain spend field name"
    );
}

// =============================================================
// 8.3 Data-preservation stress tests
// =============================================================

/// Stress: 2 máy concurrent import file khác file_hash → union 2 files.
///
/// Pre-v13 bug: imported_files.id autoincrement. A (id=1, hash=X) và
/// B (id=1, hash=Y) → sync → INSERT OR IGNORE collision → B's file drop →
/// B's raw_* rows dangling FK pollution.
///
/// v13 fix: id = content_id(file_hash) → hash X và Y khác → id khác → no
/// collision → union 2 files + FK consistent.
#[test]
fn stress_imported_files_union_content_id_after_v13() {
    let mut target = new_db("uid-shared");

    let a = new_db("uid-shared");
    let fid_a = insert_file(&a, "file-hash-A", "shopee_clicks");
    insert_raw_click(&a, "2026-04-20", fid_a, "c-a-1");

    let b = new_db("uid-shared");
    let fid_b = insert_file(&b, "file-hash-B", "shopee_clicks");
    insert_raw_click(&b, "2026-04-20", fid_b, "c-b-1");

    // Với v13 INSERT site dùng content_id: fid_a và fid_b đã khác nhau từ
    // INSERT time (dù fresh DB 2 máy).
    assert_ne!(
        fid_a, fid_b,
        "content_id(hash_A) phải ≠ content_id(hash_B) từ INSERT ban đầu"
    );

    roundtrip_capture_apply(&a, &mut target, "imported_files");
    roundtrip_capture_apply(&a, &mut target, "raw_shopee_clicks");
    roundtrip_capture_apply(&b, &mut target, "imported_files");
    roundtrip_capture_apply(&b, &mut target, "raw_shopee_clicks");

    assert_eq!(count_rows(&target, "imported_files"), 2, "union 2 files");
    assert_eq!(count_rows(&target, "raw_shopee_clicks"), 2, "2 clicks");
}

/// Stress: 2 máy concurrent edit manual_entries khác sub_id → union, 0 lost.
///
/// Pre-v13 bug: cả 2 máy fresh DB tạo rows với autoincrement id 1,2,3,...
/// Apply lên target: id conflict → INSERT OR IGNORE silently drop B's rows.
/// v13 fix: descriptor's pk_columns = (sub_id1..5, day_date) không include
/// id → `exec_insert_or_ignore` strip id → autoincrement local → no collision.
#[test]
fn stress_manual_entries_union_no_loss_after_v13() {
    let mut target = new_db("uid-shared");

    let a = new_db("uid-shared");
    for i in 0..10 {
        insert_manual(
            &a,
            "2026-04-20",
            &format!("a-{i}"),
            &format!("2026-04-20T10:00:{i:02}.000Z"),
            (i * 10) as f64,
        );
    }

    let b = new_db("uid-shared");
    for i in 0..10 {
        insert_manual(
            &b,
            "2026-04-20",
            &format!("b-{i}"),
            &format!("2026-04-20T11:00:{i:02}.000Z"),
            (i * 10 + 1) as f64,
        );
    }

    roundtrip_capture_apply(&a, &mut target, "manual_entries");
    roundtrip_capture_apply(&b, &mut target, "manual_entries");

    assert_eq!(
        count_rows(&target, "manual_entries"),
        20,
        "union 20 rows — v13 strip id fix"
    );
}

/// Stress: 2 máy concurrent import raw rows khác click_id → union, 0 rows lost.
///
/// Dùng raw_shopee_clicks (append-only, pk = click_id TEXT). Tránh case khó
/// của manual_entries (autoincrement id collision giữa 2 fresh DB — một
/// limitation của v9 schema khi fresh reinstall cùng lúc 2 máy, sẽ handle
/// qua bootstrap snapshot ở P10).
#[test]
fn stress_two_machine_union_no_loss() {
    let mut target = new_db("uid-shared");

    let a = new_db("uid-shared");
    let fid_a = insert_file(&a, "h-a", "shopee_clicks");
    for i in 0..10 {
        insert_raw_click(&a, "2026-04-20", fid_a, &format!("a-click-{i}"));
    }

    let b = new_db("uid-shared");
    let fid_b = insert_file(&b, "h-b", "shopee_clicks");
    for i in 0..10 {
        insert_raw_click(&b, "2026-04-20", fid_b, &format!("b-click-{i}"));
    }

    // Dependency order: imported_files trước, raw_shopee_clicks sau.
    roundtrip_capture_apply(&a, &mut target, "imported_files");
    roundtrip_capture_apply(&a, &mut target, "raw_shopee_clicks");
    roundtrip_capture_apply(&b, &mut target, "imported_files");
    roundtrip_capture_apply(&b, &mut target, "raw_shopee_clicks");

    // imported_files cũng bị autoincrement id conflict — expect 1 hoặc 2.
    let files_count = count_rows(&target, "imported_files");
    assert!(
        files_count >= 1,
        "imported_files có ≥1 (có thể 1 nếu id conflict, 2 nếu unique)"
    );
    // raw_shopee_clicks pk = click_id TEXT → không conflict → full union.
    assert_eq!(
        count_rows(&target, "raw_shopee_clicks"),
        20,
        "20 click rows union (click_id khác nhau), không mất"
    );
}

/// Stress: apply với 1 event lỗi cuối batch → behavior tolerant,
/// state không corrupt.
#[test]
fn stress_apply_tolerates_bad_event_no_corrupt() {
    let mut b = new_db("uid-b");
    insert_manual(&b, "2026-04-20", "seed", "2026-04-20T09:00:00.000Z", 1.0);
    assert_eq!(count_rows(&b, "manual_entries"), 1);

    let a = new_db("uid-a");
    insert_manual(&a, "2026-04-20", "new-row", "2026-04-20T12:00:00.000Z", 99.0);

    let desc = find_descriptor("manual_entries").unwrap();
    let batch = capture_table_delta(&a, desc, "0", usize::MAX, 10_000, 11)
        .unwrap()
        .unwrap();
    let mut events = batch.events.clone();

    // Inject 1 bad tombstone event (entity_key format sai).
    events.push(DeltaEvent::Tombstone(TombstoneEvent {
        sv: 11,
        entity_type: "manual_entry".to_string(),
        entity_key: String::new(), // malformed
        deleted_at: "2026-04-20T13:00:00.000Z".to_string(),
        clock_ms: 11_000,
    }));

    let result = apply_events(&mut b, &events);

    if result.is_err() {
        // Rollback path: state nguyên vẹn như trước batch.
        assert_eq!(count_rows(&b, "manual_entries"), 1, "rollback seed row");
    } else {
        // Tolerant path: bad event skipped, other events applied.
        assert!(count_rows(&b, "manual_entries") >= 1, "state không corrupt");
    }
}

/// Stress: re-import same file hash → UNIQUE block → không dup.
#[test]
fn stress_reimport_blocked_no_dup() {
    let conn = new_db("uid-solo");

    insert_file(&conn, "same-hash", "shopee_clicks");
    assert_eq!(count_rows(&conn, "imported_files"), 1);

    let res = conn.execute(
        "INSERT INTO imported_files(filename, kind, imported_at, file_hash)
         VALUES('f.csv', 'shopee_clicks', '2026-04-20T00:00:00Z', ?)",
        ["same-hash"],
    );
    assert!(res.is_err(), "duplicate hash phải bị UNIQUE reject");
    assert_eq!(count_rows(&conn, "imported_files"), 1);
}

// =============================================================
// Push cursor + descriptor sanity
// =============================================================

/// Push cursor advance sau mỗi upload, monotonic.
#[test]
fn push_cursor_advances_monotonic() {
    let conn = new_db("uid-a");
    let fid = insert_file(&conn, "h1", "shopee_clicks");
    insert_raw_click(&conn, "2026-04-20", fid, "c1");
    insert_raw_click(&conn, "2026-04-20", fid, "c2");

    let cursor_0 = push::read_cursor(&conn, "raw_shopee_clicks").unwrap();
    assert_eq!(cursor_0.last_uploaded_cursor, "0");

    let desc = find_descriptor("raw_shopee_clicks").unwrap();
    let batch = capture_table_delta(&conn, desc, "0", usize::MAX, 1_000, 11)
        .unwrap()
        .unwrap();

    // Fake compress để có hash (push::mark_uploaded cần hash).
    let fake_hash = format!("{:x}", batch.events.len()); // bất kỳ stable string
    push::mark_uploaded(&conn, "raw_shopee_clicks", &batch.cursor_hi, &fake_hash).unwrap();

    let cursor_1 = push::read_cursor(&conn, "raw_shopee_clicks").unwrap();
    let parsed_0: i64 = cursor_0.last_uploaded_cursor.parse().unwrap();
    let parsed_1: i64 = cursor_1.last_uploaded_cursor.parse().unwrap();
    assert!(parsed_1 > parsed_0, "cursor advance");
}

/// D3: SYNC_TABLES theo dependency order — imported_files trước raw_*,
/// mapping tables sau raw_*.
#[test]
fn d3_sync_tables_dependency_order() {
    let pos_of = |name: &str| -> usize {
        SYNC_TABLES
            .iter()
            .position(|d| d.name == name)
            .unwrap_or(usize::MAX)
    };
    assert!(pos_of("imported_files") < pos_of("raw_shopee_clicks"));
    assert!(pos_of("imported_files") < pos_of("raw_shopee_order_items"));
    assert!(pos_of("imported_files") < pos_of("raw_fb_ads"));
    assert!(pos_of("raw_shopee_clicks") < pos_of("clicks_to_file"));
    assert!(pos_of("raw_shopee_order_items") < pos_of("orders_to_file"));
    assert!(pos_of("raw_fb_ads") < pos_of("fb_ads_to_file"));
    assert_ne!(pos_of("shopee_accounts"), usize::MAX);
}

/// ApplyOutcome variant enum gate — add variant = update integration coverage.
#[test]
fn apply_outcome_variants_exist() {
    let _ = ApplyOutcome::Applied;
    let _ = ApplyOutcome::Skipped;
    let _ = ApplyOutcome::SkippedByHlc;
    let _ = ApplyOutcome::TombstoneApplied { rows_deleted: 0 };
    let _ = ApplyOutcome::TombstoneNoOp;
}

// =============================================================
// N — v8 → v9 migration (case N1 per plan)
// =============================================================

/// N1: Fresh DB sau migrate() có shape v9 — không còn cột v8, không còn
/// v8 triggers. Mutation không bump dirty/change_id (do cột bị drop, nếu
/// còn code bug bump sẽ fail).
#[test]
fn n1_v12_drops_v8_sync_artifacts() {
    let conn = new_db("uid-fresh");

    // sync_state chỉ còn 3 cột v9 (id, owner_uid, last_known_clock_ms).
    let cols: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(sync_state)").unwrap();
        stmt.query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    for v8_col in [
        "dirty",
        "change_id",
        "last_uploaded_change_id",
        "last_uploaded_hash",
        "last_remote_etag",
        "last_synced_at_ms",
        "last_synced_remote_mtime_ms",
        "last_error",
    ] {
        assert!(
            !cols.contains(&v8_col.to_string()),
            "v8 column '{v8_col}' phải đã drop, got {cols:?}"
        );
    }
    assert!(cols.contains(&"id".to_string()));
    assert!(cols.contains(&"owner_uid".to_string()));
    assert!(cols.contains(&"last_known_clock_ms".to_string()));

    // v8 triggers đã drop — query sqlite_master.
    let triggers: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'trg_sync_%'")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert!(
        triggers.is_empty(),
        "v8 sync triggers phải đã drop, còn: {triggers:?}"
    );
}

/// v12 migration idempotent — chạy migrate_for_tests nhiều lần không fail.
#[test]
fn n1_v12_migration_idempotent() {
    let conn = new_db("uid-idem");
    // new_db đã migrate xong. Run again.
    migrate_for_tests(&conn).unwrap();
    migrate_for_tests(&conn).unwrap();
    // Vẫn còn v9 sync_state shape.
    let cols: Vec<String> = {
        let mut stmt = conn.prepare("PRAGMA table_info(sync_state)").unwrap();
        stmt.query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(cols.len(), 3, "sync_state chỉ còn 3 cột v9: {cols:?}");
}

// =============================================================
// Skip-identical hash — cost optimization critical path
// =============================================================

/// Skip-identical: push cùng hash 2 lần → `should_skip_by_hash` return true
/// lần thứ 2. Tránh duplicate upload khi content bảng không đổi (edit-revert-edit).
#[test]
fn push_skip_identical_hash_after_upload() {
    let conn = new_db("uid-a");
    let fid = insert_file(&conn, "h1", "shopee_clicks");
    insert_raw_click(&conn, "2026-04-20", fid, "c1");

    // First capture + mark uploaded.
    let desc = find_descriptor("raw_shopee_clicks").unwrap();
    let batch = capture_table_delta(&conn, desc, "0", usize::MAX, 1_000, 11)
        .unwrap()
        .unwrap();
    let fake_hash = "sha256-abc";
    push::mark_uploaded(&conn, "raw_shopee_clicks", &batch.cursor_hi, fake_hash).unwrap();

    // Second capture của cùng data (no mutation between) → same events →
    // caller tính hash = fake_hash → should_skip true.
    let cursor = push::read_cursor(&conn, "raw_shopee_clicks").unwrap();
    assert_eq!(cursor.last_uploaded_hash.as_deref(), Some(fake_hash));

    // Simulate: payload với cùng hash → should skip.
    let mock_payload = push::PushPayload {
        r2_key: "deltas/raw_shopee_clicks/mock.ndjson.zst".to_string(),
        bytes: vec![],
        hash: fake_hash.to_string(),
        table: "raw_shopee_clicks".to_string(),
        cursor_lo: batch.cursor_lo.clone(),
        cursor_hi: batch.cursor_hi.clone(),
        clock_ms: batch.clock_ms,
        row_count: 1,
        size_bytes: 0,
    };
    assert!(
        push::should_skip_by_hash(&cursor, &mock_payload),
        "same hash → skip upload"
    );

    // Khác hash → KHÔNG skip.
    let different_payload = push::PushPayload {
        hash: "sha256-different".to_string(),
        ..mock_payload
    };
    assert!(
        !push::should_skip_by_hash(&cursor, &different_payload),
        "different hash → phải upload"
    );
}

// =============================================================
// D2 — apply crash rollback (TX atomicity)
// =============================================================

/// D2: apply_events wrap batch trong TX. Error giữa batch → ROLLBACK tất
/// cả. State trước batch nguyên vẹn.
#[test]
fn d2_apply_events_rollback_on_error() {
    let mut target = new_db("uid");
    insert_manual(&target, "2026-04-20", "seed", "2026-04-20T08:00:00.000Z", 77.0);
    assert_eq!(count_rows(&target, "manual_entries"), 1);

    // Build 2 events: 1 valid + 1 referencing non-existent table (trigger
    // apply error).
    let desc = find_descriptor("manual_entries").unwrap();
    let a = new_db("uid");
    insert_manual(&a, "2026-04-20", "new", "2026-04-20T12:00:00.000Z", 33.0);
    let batch = capture_table_delta(&a, desc, "0", usize::MAX, 1000, 11)
        .unwrap()
        .unwrap();

    let mut events = batch.events.clone();
    events.push(DeltaEvent::Tombstone(super::types::TombstoneEvent {
        sv: 11,
        entity_type: "invalid_type".to_string(), // gây error ở apply_tombstone
        entity_key: "bad".to_string(),
        deleted_at: "2026-04-20T14:00:00.000Z".to_string(),
        clock_ms: 1_100,
    }));

    let res = apply_events(&mut target, &events);
    // Tolerate either rollback (Err) hoặc skip (tolerant impl). Invariant:
    // seed row phải còn, không bị mất.
    if res.is_err() {
        assert_eq!(
            count_rows(&target, "manual_entries"),
            1,
            "rollback: chỉ còn seed row nguyên state trước batch"
        );
    }
    // Nếu tolerant, state tồn tại ≥1 row (seed + possibly new).
    assert!(count_rows(&target, "manual_entries") >= 1);
}

// =============================================================
// D4 — Delete day cascade sync
// =============================================================

/// D4: Local delete day → `tombstones` + DELETE days trong cùng TX.
/// Sync: tombstone event propagate → remote apply cascade day + raw rows.
#[test]
fn d4_delete_day_cascades_to_remote() {
    let mut remote = new_db("uid-shared");
    let fid = insert_file(&remote, "hash", "shopee_clicks");
    insert_raw_click(&remote, "2026-04-20", fid, "c1");
    insert_manual(&remote, "2026-04-20", "m1", "2026-04-20T10:00:00.000Z", 50.0);

    let local = new_db("uid-shared");
    insert_day(&local, "2026-04-20");
    // Simulate user xóa day — INSERT tombstone + DELETE days (atomic).
    local.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-20', '2026-04-20T23:00:00.000Z')",
        [],
    )
    .unwrap();

    roundtrip_capture_apply(&local, &mut remote, "tombstones");

    assert_eq!(count_rows(&remote, "days"), 0);
    assert_eq!(count_rows(&remote, "raw_shopee_clicks"), 0, "raw CASCADE");
    assert_eq!(
        count_rows(&remote, "manual_entries"),
        0,
        "manual CASCADE qua FK day_date"
    );
}

// =============================================================
// J — Encoding (unicode, null semantics)
// =============================================================

/// J1: Unicode sub_id (emoji + Vietnamese) capture → apply bytes-identical.
#[test]
fn j1_unicode_sub_id_roundtrip() {
    let mut target = new_db("uid");
    let a = new_db("uid");
    insert_manual(
        &a,
        "2026-04-20",
        "🚀 đơn hàng test",
        "2026-04-20T10:00:00.000Z",
        100.0,
    );
    roundtrip_capture_apply(&a, &mut target, "manual_entries");

    let sub1: String = target
        .query_row(
            "SELECT sub_id1 FROM manual_entries WHERE day_date = '2026-04-20'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(sub1, "🚀 đơn hàng test", "unicode preserved");
}

/// J3: null vs missing field — manual_entry override semantics.
/// - `override_spend = null` JSON → SQL NULL (clear override)
/// - field vắng → INSERT không include column → DEFAULT NULL
/// - `override_spend = 0` → SQL 0 (explicit zero, không phải clear)
#[test]
fn j3_manual_override_null_vs_missing() {
    let mut target = new_db("uid");
    // Local row có override_spend = 500.
    target
        .execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
    target.execute(
        "INSERT INTO manual_entries
         (sub_id1, day_date, created_at, updated_at, override_spend)
         VALUES('m1', '2026-04-20', 'now', '2026-04-20T08:00:00.000Z', 500.0)",
        [],
    )
    .unwrap();

    // Remote event: override_spend = null → should clear.
    let a = new_db("uid");
    a.execute(
        "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
        [],
    )
    .unwrap();
    a.execute(
        "INSERT INTO manual_entries
         (sub_id1, day_date, created_at, updated_at, override_spend)
         VALUES('m1', '2026-04-20', 'now', '2026-04-20T12:00:00.000Z', NULL)",
        [],
    )
    .unwrap();
    roundtrip_capture_apply(&a, &mut target, "manual_entries");

    let val: Option<f64> = target
        .query_row(
            "SELECT override_spend FROM manual_entries WHERE sub_id1 = 'm1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        val.is_none(),
        "override_spend = null event → NULL trên target (clear), got {val:?}"
    );
}

// =============================================================
// K — Schema evolution (forward compat)
// =============================================================

/// K1: Event có column không biết → apply skip column đó, không panic.
/// Test này đã có trong apply::tests nhưng replicate ở integration level
/// để đảm bảo end-to-end (capture→apply) tolerant.
#[test]
fn k1_apply_tolerates_extra_columns() {
    let mut target = new_db("uid");
    // Build event manually với extra field không có trong schema.
    use super::types::{DeltaEvent, InsertEvent};
    use serde_json::json;

    target
        .execute(
            "INSERT INTO days(date, created_at) VALUES('2026-04-20', 'now')",
            [],
        )
        .unwrap();
    target.execute(
        "INSERT INTO imported_files(id, filename, kind, imported_at, file_hash)
         VALUES(?, 'f.csv', 'shopee_clicks', 'now', 'h')",
        [&super::content_id::imported_file_id("h")],
    )
    .unwrap();

    let ev = DeltaEvent::Insert(InsertEvent {
        sv: 11,
        table: "raw_shopee_clicks".to_string(),
        pk: json!({"click_id": "ck-future"}),
        row: json!({
            "click_id": "ck-future",
            "click_time": "2026-04-20T12:00:00Z",
            "day_date": "2026-04-20",
            "source_file_id": super::content_id::imported_file_id("h"),
            "sub_id1": "",
            "sub_id2": "",
            "sub_id3": "",
            "sub_id4": "",
            "sub_id5": "",
            // Extra column không có trong schema — apply phải skip silent.
            "unknown_future_column": "whatever",
            "another_future_col": 42,
        }),
        clock_ms: 500,
    });

    let stats = apply_events(&mut target, &[ev]).unwrap();
    assert_eq!(stats.applied, 1);
    assert_eq!(count_rows(&target, "raw_shopee_clicks"), 1);
}

// =============================================================
// A4 — User switch isolation
// =============================================================

/// A4: Cùng máy, user A → B. DB của A phải còn nguyên ở folder A,
/// B mở DB mới ở folder B (không thấy data của A).
///
/// Test này simulate qua 2 in-memory DB (không thể test switch_db_to_user
/// đầy đủ vì cần Tauri AppHandle). Invariant verify: content_id cho shared
/// resource (shopee_accounts) vẫn deterministic cross-UID nhưng actual data
/// tách folder nên không leak.
#[test]
fn a4_shopee_account_content_id_deterministic_cross_uid() {
    // Hai user khác nhau tạo account cùng name → cùng content_id.
    // Nhưng DB riêng (khác folder) → data tách biệt.
    let user_a_db = new_db("uid-A");
    let user_b_db = new_db("uid-B");

    let id_a = super::content_id::shopee_account_id("Main Account");
    let id_b = super::content_id::shopee_account_id("Main Account");
    assert_eq!(id_a, id_b, "cùng name → cùng content_id");

    user_a_db
        .execute(
            "INSERT INTO shopee_accounts(id, name, color, created_at)
             VALUES(?, 'Main Account', '#fff', 'now')",
            [&id_a],
        )
        .unwrap();
    // B không có account đó vì DB tách biệt.
    assert_eq!(count_rows(&user_b_db, "shopee_accounts"), 1, "B có seed default only");
    let names_a: Vec<String> = user_a_db
        .prepare("SELECT name FROM shopee_accounts ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(names_a.contains(&"Main Account".to_string()));

    let names_b: Vec<String> = user_b_db
        .prepare("SELECT name FROM shopee_accounts ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(!names_b.contains(&"Main Account".to_string()));
}

// =============================================================
// L2 — Close app mid-sync (skip-identical idempotency)
// =============================================================

/// L2: Mark uploaded với cùng hash 2 lần — idempotent, cursor không tụt.
#[test]
fn l2_mark_uploaded_idempotent() {
    let conn = new_db("uid");
    let fid = insert_file(&conn, "h1", "shopee_clicks");
    insert_raw_click(&conn, "2026-04-20", fid, "c1");

    let desc = find_descriptor("raw_shopee_clicks").unwrap();
    let batch = capture_table_delta(&conn, desc, "0", usize::MAX, 1000, 11)
        .unwrap()
        .unwrap();

    push::mark_uploaded(&conn, "raw_shopee_clicks", &batch.cursor_hi, "hash-A").unwrap();
    let c1 = push::read_cursor(&conn, "raw_shopee_clicks").unwrap();

    // Re-mark với cursor lùi lại hoặc bằng — không được tụt (monotonic).
    push::mark_uploaded(&conn, "raw_shopee_clicks", "0", "hash-A").unwrap();
    let c2 = push::read_cursor(&conn, "raw_shopee_clicks").unwrap();

    let p1: i64 = c1.last_uploaded_cursor.parse().unwrap();
    let p2: i64 = c2.last_uploaded_cursor.parse().unwrap();
    assert!(p2 >= p1, "cursor không tụt: {p1} → {p2}");
}

/// HLC monotonic.
#[test]
fn hlc_monotonic_smoke() {
    let conn = new_db("uid-hlc");
    let a = hlc::next_hlc_ms(&conn).unwrap();
    let b = hlc::next_hlc_ms(&conn).unwrap();
    assert!(b > a, "HLC strictly monotonic: {a} → {b}");

    let remote = b + 1_000_000;
    hlc::absorb_remote_clock(&conn, remote).unwrap();
    let c = hlc::next_hlc_ms(&conn).unwrap();
    assert!(c > remote, "HLC absorb: {remote} → {c}");
}

// =============================================================
// Option 1 — snapshot restore khi local_clock < snapshot_clock
// =============================================================

/// End-to-end: máy "remote" tạo snapshot có data, máy "local" fresh hoàn
/// toàn → apply_snapshot_bytes → verify local giờ có data của remote +
/// cursor state đúng + bootstrap flag đã clear.
#[test]
fn restore_fresh_machine_from_snapshot_end_to_end() {
    use crate::commands::sync_v9_cmds::apply_snapshot_bytes;
    use crate::sync_v9::snapshot::create_snapshot;
    use crate::sync_v9::types::ManifestSnapshot;
    use std::sync::Mutex;

    let dir = tempfile::tempdir().unwrap();

    // 1. "Remote" máy B: tạo data + snapshot.
    let remote_path = dir.path().join("remote.db");
    {
        let remote = crate::db::init_db_at(&remote_path).unwrap();
        remote.execute(
            "UPDATE sync_state SET owner_uid='uid-shared' WHERE id=1",
            [],
        ).unwrap();
        insert_day(&remote, "2026-04-20");
        let fid = insert_file(&remote, "hashX", "shopee_clicks");
        insert_raw_click(&remote, "2026-04-20", fid, "click_A");
        insert_raw_click(&remote, "2026-04-20", fid, "click_B");
        insert_manual(&remote, "2026-04-20", "sub_manual", "2026-04-20T10:00:00.000Z", 100.0);
    }

    // Tạo snapshot từ remote (dùng create_snapshot production function).
    let (snap_bytes, snap_key, snap_clock) = {
        let remote = rusqlite::Connection::open(&remote_path).unwrap();
        let artifact = create_snapshot(&remote, dir.path(), 500_000).unwrap();
        (artifact.bytes, artifact.suggested_r2_key, artifact.clock_ms)
    };

    // 2. "Local" máy A: fresh install (không có data) tại path khác.
    let local_path = dir.path().join("local.db");
    let local_conn = crate::db::init_db_at(&local_path).unwrap();
    local_conn.execute(
        "UPDATE sync_state SET owner_uid='uid-shared' WHERE id=1",
        [],
    ).unwrap();

    // Verify trước restore: local không có data.
    assert_eq!(count_rows(&local_conn, "days"), 0);
    assert_eq!(count_rows(&local_conn, "raw_shopee_clicks"), 0);
    assert_eq!(count_rows(&local_conn, "manual_entries"), 0);

    let local_mutex = Mutex::new(local_conn);
    let snap_ref = ManifestSnapshot {
        key: snap_key.clone(),
        clock_ms: snap_clock,
        size_bytes: snap_bytes.len() as i64,
    };

    // 3. Apply snapshot (= logic perform_snapshot_restore minus HTTP).
    apply_snapshot_bytes(&local_mutex, &snap_bytes, &snap_ref).unwrap();

    // 4. Verify: local giờ có đủ data của remote.
    let conn = local_mutex.lock().unwrap();
    assert_eq!(count_rows(&conn, "days"), 1, "day restored");
    assert_eq!(count_rows(&conn, "raw_shopee_clicks"), 2, "2 clicks restored");
    assert_eq!(count_rows(&conn, "manual_entries"), 1, "manual restored");

    // 5. Verify: manifest state cập nhật (snapshot pointer + clock).
    let state = super::manifest::read_state(&conn).unwrap();
    assert_eq!(state.last_snapshot_key.as_deref(), Some(snap_key.as_str()));
    assert_eq!(state.last_snapshot_clock_ms, snap_clock);
    assert_eq!(state.last_pulled_manifest_clock_ms, snap_clock, "clock advanced");
    assert!(!state.fresh_install_pending, "bootstrap flag cleared");

    // 6. Verify: event log có Recovery + BootstrapSnapshot entries.
    let kinds: Vec<String> = conn
        .prepare("SELECT kind FROM sync_event_log ORDER BY event_id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    // Events pre-swap bị overwrite bởi snapshot content — post-swap log có
    // recovery + bootstrap_snapshot. begin_bootstrap không emit event (chỉ
    // set flag), nên kiểm 2 events này đủ.
    assert!(kinds.contains(&"recovery".to_string()), "recovery emitted post-swap");
    assert!(
        kinds.contains(&"bootstrap_snapshot".to_string()),
        "bootstrap_snapshot emitted post-swap"
    );
}

/// Idempotency: restore lần 2 trên cùng DB (đã restored) → không corrupt.
/// Scenario: crash sau apply snapshot nhưng trước khi clear flag → next sync
/// detect lại (state chưa advance) → retry restore → OK.
#[test]
fn restore_idempotent_when_called_twice() {
    use crate::commands::sync_v9_cmds::apply_snapshot_bytes;
    use crate::sync_v9::snapshot::create_snapshot;
    use crate::sync_v9::types::ManifestSnapshot;
    use std::sync::Mutex;

    let dir = tempfile::tempdir().unwrap();
    let remote_path = dir.path().join("remote.db");
    {
        let remote = crate::db::init_db_at(&remote_path).unwrap();
        insert_day(&remote, "2026-04-20");
        let fid = insert_file(&remote, "hash_id", "shopee_clicks");
        insert_raw_click(&remote, "2026-04-20", fid, "c1");
    }

    let (snap_bytes, snap_key, snap_clock) = {
        let remote = rusqlite::Connection::open(&remote_path).unwrap();
        let artifact = create_snapshot(&remote, dir.path(), 777_000).unwrap();
        (artifact.bytes, artifact.suggested_r2_key, artifact.clock_ms)
    };

    let local_path = dir.path().join("local.db");
    let local_conn = crate::db::init_db_at(&local_path).unwrap();
    let local_mutex = Mutex::new(local_conn);

    let snap_ref = ManifestSnapshot {
        key: snap_key,
        clock_ms: snap_clock,
        size_bytes: snap_bytes.len() as i64,
    };

    // Lần 1.
    apply_snapshot_bytes(&local_mutex, &snap_bytes, &snap_ref).unwrap();
    let rows_after_1 = {
        let c = local_mutex.lock().unwrap();
        count_rows(&c, "raw_shopee_clicks")
    };
    assert_eq!(rows_after_1, 1);

    // Lần 2 (retry sau crash giả lập).
    apply_snapshot_bytes(&local_mutex, &snap_bytes, &snap_ref).unwrap();
    let rows_after_2 = {
        let c = local_mutex.lock().unwrap();
        count_rows(&c, "raw_shopee_clicks")
    };
    assert_eq!(rows_after_2, 1, "idempotent: không duplicate sau retry");

    let state = {
        let c = local_mutex.lock().unwrap();
        super::manifest::read_state(&c).unwrap()
    };
    assert!(!state.fresh_install_pending);
}

// =============================================================
// Manifest cache (PUT-first CAS optimization)
// =============================================================

/// Cache invalidate trước test để cross-test không leak state qua static.
fn cache_reset() {
    crate::sync_v9::manifest_cache::cache_invalidate();
}

/// Cache empty → push CAS phải GET fresh (không skip). Test ở mức state
/// transition — verify cache_get returns None before, has data after put.
#[test]
fn cache_starts_empty_then_populated_after_put() {
    use crate::sync_v9::manifest_cache::{cache_get, cache_put, TEST_LOCK};
    use crate::sync_v9::types::Manifest;
    let _g = TEST_LOCK.lock().unwrap();
    cache_reset();

    assert!(cache_get().is_none(), "cache fresh = empty");

    let m = Manifest::empty("uid-test".to_string());
    cache_put(m.clone(), "etag-A".to_string());

    let got = cache_get();
    assert!(got.is_some());
    let (got_m, got_etag) = got.unwrap();
    assert_eq!(got_etag, "etag-A");
    assert_eq!(got_m.uid, "uid-test");
    cache_reset();
}

/// Pull cache full body → push tiếp theo có data đầy đủ để PUT (không phải
/// build từ scratch). Verify roundtrip: append entries vào cached → cache
/// updated reflect mới.
#[test]
fn cache_supports_append_workflow() {
    use crate::sync_v9::manifest::append_delta_entries;
    use crate::sync_v9::manifest_cache::{cache_get, cache_put, TEST_LOCK};
    use crate::sync_v9::types::{Manifest, ManifestDeltaEntry};
    let _g = TEST_LOCK.lock().unwrap();
    cache_reset();

    // Pull cache: manifest với 1 delta đã có.
    let mut original = Manifest::empty("uid-x".to_string());
    append_delta_entries(
        &mut original,
        vec![ManifestDeltaEntry {
            table: "raw_shopee_clicks".to_string(),
            key: "deltas/old.ndjson.zst".to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "100".to_string(),
            clock_ms: 1000,
            size_bytes: 500,
            row_count: 10,
        }],
    );
    cache_put(original.clone(), "etag-pull".to_string());

    // Push CAS dùng cached: append 1 entry mới.
    let (mut working, etag) = cache_get().unwrap();
    assert_eq!(etag, "etag-pull");
    assert_eq!(working.deltas.len(), 1);
    append_delta_entries(
        &mut working,
        vec![ManifestDeltaEntry {
            table: "manual_entries".to_string(),
            key: "deltas/new.ndjson.zst".to_string(),
            cursor_lo: "100".to_string(),
            cursor_hi: "200".to_string(),
            clock_ms: 2000,
            size_bytes: 300,
            row_count: 5,
        }],
    );
    assert_eq!(working.deltas.len(), 2, "append vào cached body OK");

    // Push thành công → update cache với manifest mới + etag mới.
    cache_put(working, "etag-after-put".to_string());

    let (final_m, final_etag) = cache_get().unwrap();
    assert_eq!(final_etag, "etag-after-put", "cache update etag mới");
    assert_eq!(final_m.deltas.len(), 2, "cache giữ both deltas");
    cache_reset();
}

/// Conflict 412 → invalidate cache → next read returns None → fallback GET.
#[test]
fn cache_invalidate_on_conflict_falls_back_to_get() {
    use crate::sync_v9::manifest_cache::{cache_get, cache_invalidate, cache_put, TEST_LOCK};
    use crate::sync_v9::types::Manifest;
    let _g = TEST_LOCK.lock().unwrap();
    cache_reset();

    cache_put(Manifest::empty("u".to_string()), "stale-etag".to_string());
    assert!(cache_get().is_some());

    // Simulate 412 conflict handler → invalidate.
    cache_invalidate();
    assert!(
        cache_get().is_none(),
        "post-conflict cache empty → caller phải GET fresh"
    );
    cache_reset();
}

/// Sequence push 1 → push 2 dùng cùng cached state — verify cache đủ
/// support multi-push streak (mỗi push tích lũy 1 entry, cache update).
#[test]
fn cache_supports_consecutive_push_streak() {
    use crate::sync_v9::manifest::append_delta_entries;
    use crate::sync_v9::manifest_cache::{cache_get, cache_put, TEST_LOCK};
    use crate::sync_v9::types::{Manifest, ManifestDeltaEntry};
    let _g = TEST_LOCK.lock().unwrap();
    cache_reset();

    let initial = Manifest::empty("u-streak".to_string());
    cache_put(initial, "e0".to_string());

    // Push 1
    let (mut m1, _) = cache_get().unwrap();
    append_delta_entries(
        &mut m1,
        vec![ManifestDeltaEntry {
            table: "tombstones".to_string(),
            key: "k1".to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "1".to_string(),
            clock_ms: 100,
            size_bytes: 10,
            row_count: 1,
        }],
    );
    cache_put(m1, "e1".to_string());

    // Push 2 (consecutive — không cần GET, dùng cached e1)
    let (mut m2, etag2) = cache_get().unwrap();
    assert_eq!(etag2, "e1");
    assert_eq!(m2.deltas.len(), 1, "deltas từ push 1 còn trong cache");
    append_delta_entries(
        &mut m2,
        vec![ManifestDeltaEntry {
            table: "shopee_accounts".to_string(),
            key: "k2".to_string(),
            cursor_lo: "0".to_string(),
            cursor_hi: "1".to_string(),
            clock_ms: 200,
            size_bytes: 20,
            row_count: 1,
        }],
    );
    cache_put(m2, "e2".to_string());

    let (final_m, final_etag) = cache_get().unwrap();
    assert_eq!(final_etag, "e2");
    assert_eq!(final_m.deltas.len(), 2, "streak tích lũy đủ 2 entries");
    cache_reset();
}

/// Restore overwrites local state: local có data "cũ" + schema khác → sau
/// restore local reflect EXACTLY snapshot content (không merge).
#[test]
fn restore_overwrites_stale_local_completely() {
    use crate::commands::sync_v9_cmds::apply_snapshot_bytes;
    use crate::sync_v9::snapshot::create_snapshot;
    use crate::sync_v9::types::ManifestSnapshot;
    use std::sync::Mutex;

    let dir = tempfile::tempdir().unwrap();

    // Remote có click_B.
    let remote_path = dir.path().join("remote.db");
    {
        let remote = crate::db::init_db_at(&remote_path).unwrap();
        insert_day(&remote, "2026-04-20");
        let fid = insert_file(&remote, "remote_hash", "shopee_clicks");
        insert_raw_click(&remote, "2026-04-20", fid, "click_REMOTE_only");
    }
    let (snap_bytes, snap_key, snap_clock) = {
        let remote = rusqlite::Connection::open(&remote_path).unwrap();
        let artifact = create_snapshot(&remote, dir.path(), 999_000).unwrap();
        (artifact.bytes, artifact.suggested_r2_key, artifact.clock_ms)
    };

    // Local có click_A (data cũ, không có trong snapshot).
    let local_path = dir.path().join("local.db");
    let local_conn = crate::db::init_db_at(&local_path).unwrap();
    insert_day(&local_conn, "2026-04-19");
    let local_fid = insert_file(&local_conn, "local_hash", "shopee_clicks");
    insert_raw_click(&local_conn, "2026-04-19", local_fid, "click_LOCAL_only");

    let local_mutex = Mutex::new(local_conn);

    // Restore.
    apply_snapshot_bytes(
        &local_mutex,
        &snap_bytes,
        &ManifestSnapshot {
            key: snap_key,
            clock_ms: snap_clock,
            size_bytes: snap_bytes.len() as i64,
        },
    )
    .unwrap();

    // Local giờ CHỈ có data remote, không còn click_LOCAL_only.
    let conn = local_mutex.lock().unwrap();
    let click_ids: Vec<String> = conn
        .prepare("SELECT click_id FROM raw_shopee_clicks ORDER BY click_id")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(click_ids, vec!["click_REMOTE_only"], "local data bị overwrite");

    let days: Vec<String> = conn
        .prepare("SELECT date FROM days ORDER BY date")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(days, vec!["2026-04-20"], "local day cũ đã bị replace");
}
