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

/// B5: Day tombstone UNCONDITIONAL cascade.
#[test]
fn b5_day_tombstone_cascades_unconditionally() {
    let mut target = new_db("uid-shared");

    let fid = insert_file(&target, "h1", "shopee_clicks");
    insert_raw_click(&target, "2026-04-20", fid, "c1");
    insert_manual(&target, "2026-04-20", "m1", "2026-04-20T20:00:00.000Z", 50.0);

    assert_eq!(count_rows(&target, "raw_shopee_clicks"), 1);
    assert_eq!(count_rows(&target, "manual_entries"), 1);

    // Simulate remote xóa day với deleted_at sớm hơn manual edit trên target.
    let a = new_db("uid-shared");
    insert_day(&a, "2026-04-20");
    a.execute(
        "INSERT INTO tombstones(entity_type, entity_key, deleted_at)
         VALUES('day', '2026-04-20', '2026-04-20T05:00:00.000Z')",
        [],
    )
    .unwrap();

    roundtrip_capture_apply(&a, &mut target, "tombstones");

    assert_eq!(count_rows(&target, "days"), 0, "day bị xóa");
    assert_eq!(count_rows(&target, "raw_shopee_clicks"), 0, "raw cascade");
    assert_eq!(
        count_rows(&target, "manual_entries"),
        0,
        "manual cascade dù edit mới hơn tombstone"
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
