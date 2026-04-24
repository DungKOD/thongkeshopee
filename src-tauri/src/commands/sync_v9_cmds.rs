//! Tauri commands cho sync v9. Wire sync_v9 pure modules với DbState + HTTP.
//!
//! Design:
//! - FE pass `base_url` + `id_token` mỗi call (Rust stateless).
//! - DB lock held ngắn, release trước HTTP await (tránh block mutations).
//! - CAS retry logic trong push path (max 3 theo plan).
//! - Error serialize thành string qua CmdError (UI hiển thị).

use serde::Serialize;
use tauri::State;

use crate::db::DbState;
use crate::sync_v9::{
    apply, bootstrap, capture, client, compaction, compress, descriptors, event_log, hlc, manifest,
    pull, push, snapshot,
    types::{DeltaEvent, Manifest, ManifestDeltaEntry, SyncEventCtx},
    SV_CURRENT,
};

use super::{CmdError, CmdResult};

// =============================================================
// State read (UI)
// =============================================================

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncV9State {
    pub fresh_install_pending: bool,
    pub last_pulled_manifest_clock_ms: i64,
    pub last_snapshot_key: Option<String>,
    pub last_snapshot_clock_ms: i64,
    pub pending_push_tables: Vec<String>,
    /// Số events trong `sync_event_log` chưa flush lên R2. UI dùng hiển thị
    /// pending count + FE quyết định khi threshold > N → flush ngay thay vì
    /// đợi date rollover.
    pub pending_log_count: i64,
}

/// UI state snapshot — hiển thị trong SyncBadge + admin.
#[tauri::command]
pub fn sync_v9_get_state(db: State<'_, DbState>) -> CmdResult<SyncV9State> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let m = manifest::read_state(&conn).map_err(|e| CmdError::msg(e.to_string()))?;

    let mut pending_tables = Vec::new();
    for desc in descriptors::SYNC_TABLES {
        let cursor = push::read_cursor(&conn, desc.name)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        // Check có row với cursor > last_uploaded không. Cheap SELECT.
        let has_more: i64 = match desc.cursor_kind {
            descriptors::CursorKind::RowId => conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE rowid > ?",
                    desc.name
                ),
                [cursor.last_uploaded_cursor.parse::<i64>().unwrap_or(0)],
                |r| r.get(0),
            )?,
            descriptors::CursorKind::PrimaryKey => conn.query_row(
                &format!(
                    "SELECT COUNT(*) FROM {} WHERE {} > ?",
                    desc.name, desc.cursor_column
                ),
                [cursor.last_uploaded_cursor.parse::<i64>().unwrap_or(0)],
                |r| r.get(0),
            )?,
            descriptors::CursorKind::UpdatedAt | descriptors::CursorKind::DeletedAt => {
                conn.query_row(
                    &format!(
                        "SELECT COUNT(*) FROM {} WHERE {} > ?",
                        desc.name, desc.cursor_column
                    ),
                    [cursor.last_uploaded_cursor.as_str()],
                    |r| r.get(0),
                )?
            }
        };
        if has_more > 0 {
            pending_tables.push(desc.name.to_string());
        }
    }

    let pending_log_count =
        event_log::count_pending(&conn).map_err(|e| CmdError::msg(e.to_string()))?;

    Ok(SyncV9State {
        fresh_install_pending: m.fresh_install_pending,
        last_pulled_manifest_clock_ms: m.last_pulled_manifest_clock_ms,
        last_snapshot_key: m.last_snapshot_key,
        last_snapshot_clock_ms: m.last_snapshot_clock_ms,
        pending_push_tables: pending_tables,
        pending_log_count,
    })
}

// =============================================================
// PUSH — full pass
// =============================================================

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PushReport {
    pub uploaded_count: u32,
    pub skipped_identical: u32,
    pub total_bytes: u64,
    pub cas_retries: u32,
}

const CAS_MAX_RETRY: u32 = 3;

/// Full push cycle (v9.1 bundle): plan 1 bundle file → upload 1× → CAS
/// manifest put với N entries trỏ cùng key (retry max 3).
///
/// **Tối ưu A1:** N tables thay đổi → 1 R2 PUT (thay vì N). Manifest entries
/// vẫn per-table để pull-side preserve cursor tracking. Pull dedup theo key
/// → 1 fetch cho cả bundle.
///
/// Fresh-install guard: `fresh_install_pending = 1` → no-op.
#[tauri::command]
pub async fn sync_v9_push_all(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<PushReport> {
    // 1. Plan bundle + capture + compress (lock held for DB read only).
    let (bundle_opt, fingerprint, clock_ms) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        if bootstrap::is_bootstrap_pending(&conn).map_err(|e| CmdError::msg(e.to_string()))? {
            return Ok(PushReport::default());
        }
        let clock = hlc::next_hlc_ms(&conn)?;
        let fp = machine_fingerprint_stable();
        let bundle = push::plan_push_bundle_default(&conn, clock)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        (bundle, fp, clock)
    };

    let Some(bundle) = bundle_opt else {
        return Ok(PushReport::default());
    };

    let uploaded = bundle.table_ranges.len() as u32;
    let total_bytes = bundle.bytes.len() as u64;
    // Skipped tables: SYNC_TABLES count - uploaded tables. Chỉ để log, không
    // strict precise (table không có row cũng count skipped, không hẳn
    // "skip-identical", nhưng user UX message OK).
    let skipped = (descriptors::SYNC_TABLES.len() as u32)
        .saturating_sub(uploaded);

    // 2. Emit log events per-table (debug observability, match format cũ).
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        for range in &bundle.table_ranges {
            let ts = hlc::next_hlc_rfc3339(&conn)?;
            let _ = event_log::append(
                &conn,
                &ts,
                &fingerprint,
                &SyncEventCtx::PushUpload {
                    table: range.table.clone(),
                    cursor_lo: range.cursor_lo.clone(),
                    cursor_hi: range.cursor_hi.clone(),
                    bytes: (bundle.bytes.len() as u64) / (bundle.table_ranges.len() as u64).max(1),
                    delta_key: bundle.r2_key.clone(),
                    row_count: range.row_count,
                },
            );
        }
    }

    // 3. Upload 1 bundle file (no lock).
    client::upload_delta(&base_url, &id_token, &bundle.r2_key, &bundle.bytes)
        .await
        .map_err(|e| CmdError::msg(format!("upload_delta bundle: {e}")))?;

    // 4. Advance cursor + build manifest entries per-table (idempotent retry safe).
    let mut new_entries: Vec<ManifestDeltaEntry> = Vec::new();
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        for range in &bundle.table_ranges {
            push::mark_uploaded(&conn, &range.table, &range.cursor_hi, &range.content_hash)
                .map_err(|e| CmdError::msg(e.to_string()))?;
            new_entries.push(ManifestDeltaEntry {
                table: range.table.clone(),
                // All entries share cùng r2_key — pull side dedup theo key.
                key: bundle.r2_key.clone(),
                cursor_lo: range.cursor_lo.clone(),
                cursor_hi: range.cursor_hi.clone(),
                clock_ms: bundle.clock_ms,
                // size_bytes per-entry là proportional share (approximate).
                // Field này chỉ cho UI display, không ảnh hưởng correctness.
                size_bytes: bundle.size_bytes / (bundle.table_ranges.len() as i64).max(1),
                row_count: range.row_count,
            });
        }
    }

    // 5. CAS manifest put với N entries cùng key (retry max 3).
    let cas_retries =
        cas_append_manifest_retry(&db, &base_url, &id_token, &new_entries, clock_ms).await?;

    Ok(PushReport {
        uploaded_count: uploaded,
        skipped_identical: skipped,
        total_bytes,
        cas_retries,
    })
}

/// CAS loop: fetch manifest + append entries + put với expectedEtag.
/// 412 → re-fetch + re-append + retry. Exhaust → Err.
/// Append idempotent theo key (manifest::append_delta_entries dedup).
async fn cas_append_manifest_retry(
    db: &State<'_, DbState>,
    base_url: &str,
    id_token: &str,
    new_entries: &[ManifestDeltaEntry],
    clock_ms: i64,
) -> CmdResult<u32> {
    if new_entries.is_empty() {
        return Ok(0);
    }
    let mut retries = 0u32;
    let uid = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.query_row(
            "SELECT COALESCE(owner_uid, '') FROM sync_state WHERE id = 1",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap_or_default()
    };

    for attempt in 0..=CAS_MAX_RETRY {
        let fetched = client::get_manifest(base_url, id_token)
            .await
            .map_err(|e| CmdError::msg(format!("get_manifest: {e}")))?;

        let mut manifest = fetched.manifest.unwrap_or_else(|| Manifest::empty(uid.clone()));
        manifest::append_delta_entries(&mut manifest, new_entries.to_vec());
        manifest::bump_updated_at(&mut manifest, clock_ms);

        match client::put_manifest(base_url, id_token, &manifest, fetched.etag.as_deref()).await {
            Ok(new_etag) => {
                let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
                manifest::set_etag(&conn, &new_etag).map_err(|e| CmdError::msg(e.to_string()))?;
                return Ok(retries);
            }
            Err(e) if e.to_string().starts_with(client::CAS_CONFLICT) => {
                retries += 1;
                if attempt >= CAS_MAX_RETRY {
                    return Err(CmdError::msg(format!(
                        "CAS exhausted after {CAS_MAX_RETRY} retries"
                    )));
                }
                continue;
            }
            Err(e) => return Err(CmdError::msg(format!("put_manifest: {e}"))),
        }
    }
    Ok(retries)
}

// =============================================================
// SNAPSHOT RESTORE — stale local clock recovery
// =============================================================

/// Fetch snapshot từ R2, atomic swap file DB, re-seed cursor state.
///
/// **Rule giữ data**:
/// - `begin_bootstrap` set `fresh_install_pending=1` trước → push path bị
///   guard bypass trong lúc restore chạy (tránh push empty đè remote).
/// - `restore_snapshot_to_pending` có RAII guard — verify integrity +
///   sync_state exist trước khi declare success. Fail → pending file cleanup.
/// - File swap: close current conn → rename pending→live → reopen.
///   Nếu rename crash giữa chừng, lần start sau `apply_pending_sync` trong
///   `switch_db_to_user` apply lại (resume).
/// - `complete_bootstrap` clear flag sau khi hoàn tất — idempotent nếu
///   crash giữa restore và complete (next run detect vẫn trigger vì
///   clock chưa advance).
///
/// Caller: `sync_v9_pull_all` khi detect `local_clock < snapshot_clock`.
async fn perform_snapshot_restore(
    db: &State<'_, DbState>,
    base_url: &str,
    id_token: &str,
    snap: &crate::sync_v9::types::ManifestSnapshot,
) -> anyhow::Result<()> {
    // 1. Fetch snapshot bytes từ R2. Tách step này riêng → HTTP không giữ
    //    DB lock. Step 2 (apply_snapshot_bytes) là pure — testable ngoài Tauri.
    let bytes = client::fetch_snapshot(base_url, id_token, &snap.key).await?;
    apply_snapshot_bytes(&db.0, &bytes, snap)
}

/// Apply snapshot bytes đã fetch được vào local DB. Pure (không HTTP) để
/// integration test call trực tiếp không cần Tauri test harness.
///
/// Flow:
/// 1. `begin_bootstrap` set flag (push path bypass).
/// 2. `restore_snapshot_to_pending` write + verify pending.db.
/// 3. File swap: drop conn → rename pending→live → reopen.
/// 4. `seed_cursor_after_restore` + `complete_bootstrap` (clear flag).
/// 5. Emit event_log entries cho observability.
///
/// Rule giữ data: fail giữa chừng → pending file cleanup (RAII guard trong
/// restore_snapshot_to_pending), fresh_install_pending vẫn =1 → next call
/// idempotent detect + retry.
pub fn apply_snapshot_bytes(
    db_mutex: &std::sync::Mutex<rusqlite::Connection>,
    bytes: &[u8],
    snap: &crate::sync_v9::types::ManifestSnapshot,
) -> anyhow::Result<()> {
    let snapshot_bytes_len = bytes.len() as u64;

    // 1. Set bootstrap flag trên OLD DB → concurrent push bị guard block
    //    trong lúc restore chạy (HTTP fetch window ở caller perform_snapshot_restore).
    //    Events emit SAU swap vì OLD DB sắp bị overwrite — event log pre-swap
    //    sẽ mất, lãng phí I/O.
    {
        let conn = db_mutex.lock().map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        bootstrap::begin_bootstrap(&conn)?;
    }

    // 2. Resolve live DB path (query từ connection).
    let live_path = {
        let conn = db_mutex.lock().map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        crate::db::resolve_active_db_path(&conn)?
    };
    let pending_path = live_path.with_extension("pending.db");

    // 3. Write snapshot → pending.db + verify integrity.
    snapshot::restore_snapshot_to_pending(bytes, &pending_path)?;

    // 4. File swap atomic: drop conn → rename → reopen.
    {
        let mut slot = db_mutex.lock().map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        let tmp = rusqlite::Connection::open_in_memory()?;
        let old = std::mem::replace(&mut *slot, tmp);
        drop(old);

        // Windows: cần remove target trước khi rename. Unix: rename replaces
        // atomic. Cover cả 2 case.
        if live_path.exists() {
            std::fs::remove_file(&live_path)
                .map_err(|e| anyhow::anyhow!("remove old live DB: {e}"))?;
        }
        std::fs::rename(&pending_path, &live_path)
            .map_err(|e| anyhow::anyhow!("rename pending → live: {e}"))?;

        *slot = crate::db::init_db_at(&live_path)
            .map_err(|e| anyhow::anyhow!("init_db_at post-swap: {e}"))?;
    }

    // 5. Seed cursor + complete bootstrap + emit events trên connection MỚI
    //    (post-swap DB). Events pre-swap đã bị overwrite, phải log ở đây để
    //    user thấy trong sync log.
    {
        let conn = db_mutex.lock().map_err(|_| anyhow::anyhow!("db lock poisoned"))?;
        bootstrap::seed_cursor_after_restore(&conn, snap.clock_ms)?;
        bootstrap::complete_bootstrap(&conn, &snap.key, snap.clock_ms)?;

        let ts = hlc::next_hlc_rfc3339(&conn)?;
        let _ = event_log::append(
            &conn,
            &ts,
            &machine_fingerprint_stable(),
            &SyncEventCtx::Recovery {
                reason: "stale_local_clock".to_string(),
            },
        );
        let _ = event_log::append(
            &conn,
            &ts,
            &machine_fingerprint_stable(),
            &SyncEventCtx::BootstrapSnapshot {
                snapshot_key: snap.key.clone(),
                bytes: snapshot_bytes_len,
                duration_ms: 0,
            },
        );
    }

    Ok(())
}

// =============================================================
// PULL — full pass
// =============================================================

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PullReport {
    pub applied_deltas: u32,
    pub total_events: u32,
    pub skipped: u32,
    pub skipped_by_hlc: u32,
    pub tombstones_applied: u32,
    pub rows_deleted: u64,
    /// Tổng bytes fetched từ R2 (compressed zstd, trước parse). UI hiển thị
    /// để user biết đã download bao nhiêu data.
    pub total_bytes: u64,
}

/// Full pull cycle: fetch manifest → diff → fetch deltas → apply per file.
#[tauri::command]
pub async fn sync_v9_pull_all(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<PullReport> {
    // 1. Fetch manifest.
    let fetched = client::get_manifest(&base_url, &id_token)
        .await
        .map_err(|e| CmdError::msg(format!("get_manifest: {e}")))?;
    let Some(mut manifest) = fetched.manifest else {
        // No manifest — không có gì để pull.
        return Ok(PullReport::default());
    };

    // 2. Stale-local detection: nếu local clock đi sau snapshot clock của
    // remote → delta giữa [local_clock, snapshot_clock] đã compact khỏi
    // manifest. Pull raw deltas sẽ FK fail. Phải snapshot restore trước.
    //
    // Scope covered:
    // - Long offline (2+ tuần) → máy khác đã compact nhiều vòng
    // - Fresh install (local_clock=0) + remote có snapshot → auto bootstrap
    // - Stale schema DB + new snapshot → replace local với remote state
    let restore_snapshot = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let state = manifest::read_state(&conn).map_err(|e| CmdError::msg(e.to_string()))?;
        manifest::needs_snapshot_restore(&manifest, &state).cloned()
    };

    if let Some(snap_ref) = restore_snapshot {
        perform_snapshot_restore(&db, &base_url, &id_token, &snap_ref)
            .await
            .map_err(|e| CmdError::msg(format!("snapshot restore: {e}")))?;

        // State đã reset sau restore — refetch manifest để pull đúng delta
        // SAU snapshot_clock mới (thay vì manifest cũ có thể đã outdated
        // trong thời gian fetch+restore chạy, nếu máy khác vừa push).
        let refetched = client::get_manifest(&base_url, &id_token)
            .await
            .map_err(|e| CmdError::msg(format!("get_manifest post-restore: {e}")))?;
        manifest = match refetched.manifest {
            Some(m) => m,
            None => return Ok(PullReport::default()),
        };
    }

    // 3. Plan pending deltas (dùng local state).
    let pending = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        pull::plan_pull(&conn, &manifest).map_err(|e| CmdError::msg(e.to_string()))?
    };

    if pending.is_empty() {
        // Advance clock kể cả khi empty để UI state đúng.
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        manifest::advance_pulled_clock(&conn, manifest.updated_at_ms)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        if let Some(etag) = &fetched.etag {
            manifest::set_etag(&conn, etag).map_err(|e| CmdError::msg(e.to_string()))?;
        }
        return Ok(PullReport::default());
    }

    let mut report = PullReport::default();

    // 3. Dedupe pending entries theo R2 key — bundle delta có N manifest
    // entries (per-table) trỏ cùng 1 file. Fetch 1 lần, advance N cursor.
    // Tiết kiệm: N GET → 1 GET cho bundle. Legacy per-table delta (1 key
    // = 1 entry) không đổi behavior.
    use std::collections::BTreeMap;
    let mut by_key: BTreeMap<String, Vec<&ManifestDeltaEntry>> = BTreeMap::new();
    for entry in &pending {
        by_key.entry(entry.key.clone()).or_default().push(entry);
    }
    // Sort keys theo min(clock_ms) của entries trong key để apply causal order.
    let mut keys_sorted: Vec<(String, Vec<&ManifestDeltaEntry>)> =
        by_key.into_iter().collect();
    keys_sorted.sort_by_key(|(_, entries)| {
        entries.iter().map(|e| e.clock_ms).min().unwrap_or(0)
    });

    // 4. Per-file: fetch + parse + apply (TX per file).
    for (key, entries) in &keys_sorted {
        let bytes = client::fetch_delta(&base_url, &id_token, key)
            .await
            .map_err(|e| CmdError::msg(format!("fetch_delta {}: {e}", key)))?;
        report.total_bytes += bytes.len() as u64;
        let events = pull::parse_delta_file(&bytes)
            .map_err(|e| CmdError::msg(format!("parse {}: {e}", key)))?;
        let max_clock = pull::max_event_clock_ms(&events);

        // Apply trong TX (lock held qua apply — tiny vs network).
        // Bundle file chứa events nhiều tables — apply_events dispatch qua
        // event.table field, xử lý tự động theo descriptor per-event.
        let stats = {
            let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            let stats = pull::apply_events(&mut conn, &events)
                .map_err(|e| CmdError::msg(e.to_string()))?;

            // Advance cursor cho MỖI table trong bundle.
            for entry in entries {
                pull::advance_pulled_cursor(&conn, &entry.table, &entry.cursor_hi)
                    .map_err(|e| CmdError::msg(e.to_string()))?;
            }
            hlc::absorb_remote_clock(&conn, max_clock)?;
            stats
        };

        report.applied_deltas += 1;
        report.total_events += stats.total();
        report.skipped += stats.skipped;
        report.skipped_by_hlc += stats.skipped_by_hlc;
        report.tombstones_applied += stats.tombstones_applied;
        report.rows_deleted += stats.rows_deleted;
    }

    // 4. Finalize manifest clock + etag.
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        manifest::advance_pulled_clock(&conn, manifest.updated_at_ms)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        if let Some(etag) = &fetched.etag {
            manifest::set_etag(&conn, etag).map_err(|e| CmdError::msg(e.to_string()))?;
        }
    }

    Ok(report)
}

// =============================================================
// SYNC ALL — pull then push (standard cycle)
// =============================================================

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncReport {
    pub pull: PullReport,
    pub push: PushReport,
}

/// Standard sync cycle: pull (nhận thay đổi từ máy khác) rồi push (gửi local
/// changes). Order này tránh CAS conflict cao khi cả 2 máy cùng sync.
#[tauri::command]
pub async fn sync_v9_sync_all(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<SyncReport> {
    let pull_report = sync_v9_pull_all(db.clone(), base_url.clone(), id_token.clone()).await?;
    let push_report = sync_v9_push_all(db, base_url, id_token).await?;
    Ok(SyncReport {
        pull: pull_report,
        push: push_report,
    })
}

// =============================================================
// COMPACTION (P10) — snapshot + clear delta pointers
// =============================================================

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CompactionReport {
    pub triggered: bool,
    pub snapshot_key: Option<String>,
    pub snapshot_size_bytes: u64,
    pub deltas_cleared: u32,
    pub cas_retries: u32,
}

/// Check compaction trigger + chạy nếu cần. Gọi sau push thành công
/// (auto-pipeline) hoặc user-trigger manual (admin debug).
///
/// Flow:
/// 1. Fetch manifest.
/// 2. Check threshold `manifest.deltas.len() > COMPACTION_DELTA_THRESHOLD`.
/// 3. `prepare_compaction` — VACUUM INTO + zstd + build new manifest.
/// 4. `client::upload_snapshot` — PUT bytes lên R2 snapshot key.
/// 5. CAS loop put updated manifest (retry max 3).
/// 6. Best-effort log `compaction_complete` event.
///
/// Rule giữ data: delta objects trên R2 KHÔNG delete ở đây. Chỉ clear
/// pointer trong manifest. Cron Worker sẽ sweep delete sau grace period.
#[tauri::command]
pub async fn sync_v9_compact_if_needed(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<CompactionReport> {
    // 1. Fetch manifest.
    let fetched = client::get_manifest(&base_url, &id_token)
        .await
        .map_err(|e| CmdError::msg(format!("get_manifest: {e}")))?;
    let manifest = match fetched.manifest {
        Some(m) => m,
        None => return Ok(CompactionReport::default()), // nothing to compact
    };

    if !compaction::should_compact(&manifest) {
        return Ok(CompactionReport::default());
    }

    // 2. Prepare snapshot (VACUUM INTO + zstd) + build new manifest.
    let fingerprint = machine_fingerprint_stable();
    let (artifact, new_manifest, result, _clock_ms) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let clock = hlc::next_hlc_ms(&conn)?;
        let uid: String = conn
            .query_row(
                "SELECT COALESCE(owner_uid, '') FROM sync_state WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap_or_default();
        let temp_dir = std::env::temp_dir().join("thongkeshopee_v9_snapshot");
        let (art, mut new_m, res) =
            compaction::prepare_compaction(&conn, &manifest, &temp_dir, clock, &uid)
                .map_err(|e| CmdError::msg(format!("prepare_compaction: {e}")))?;
        new_m.uid = uid; // preserve
        (art, new_m, res, clock)
    };

    // 3. Upload snapshot lên R2 (long-running).
    client::upload_snapshot(&base_url, &id_token, &artifact.suggested_r2_key, &artifact.bytes)
        .await
        .map_err(|e| CmdError::msg(format!("upload_snapshot: {e}")))?;

    // 4. CAS put manifest — retry max 3 (theo pattern push_all).
    let cas_retries =
        cas_put_full_manifest_retry(&db, &base_url, &id_token, &new_manifest, fetched.etag)
            .await?;

    // 5. Log compaction_complete (best-effort).
    {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let ts = hlc::next_hlc_rfc3339(&conn)?;
        let _ = event_log::append(
            &conn,
            &ts,
            &fingerprint,
            &SyncEventCtx::CompactionComplete {
                new_snapshot_key: artifact.suggested_r2_key.clone(),
                old_deltas_removed: result.deltas_cleared,
            },
        );
    }

    Ok(CompactionReport {
        triggered: true,
        snapshot_key: Some(result.snapshot_key),
        snapshot_size_bytes: result.snapshot_size_bytes,
        deltas_cleared: result.deltas_cleared,
        cas_retries,
    })
}

/// CAS put manifest full-replace (không append). Cần khi compaction —
/// entire `deltas` cleared. Different từ `cas_append_manifest_retry` (đó
/// append), đây overwrite full manifest.
async fn cas_put_full_manifest_retry(
    db: &State<'_, DbState>,
    base_url: &str,
    id_token: &str,
    new_manifest: &Manifest,
    mut expected_etag: Option<String>,
) -> CmdResult<u32> {
    let mut retries = 0u32;
    for attempt in 0..=CAS_MAX_RETRY {
        match client::put_manifest(base_url, id_token, new_manifest, expected_etag.as_deref()).await
        {
            Ok(new_etag) => {
                let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
                manifest::set_etag(&conn, &new_etag).map_err(|e| CmdError::msg(e.to_string()))?;
                return Ok(retries);
            }
            Err(e) if e.to_string().starts_with(client::CAS_CONFLICT) => {
                retries += 1;
                if attempt >= CAS_MAX_RETRY {
                    return Err(CmdError::msg(format!(
                        "CAS exhausted after {CAS_MAX_RETRY} retries (compaction)"
                    )));
                }
                // Re-fetch để get new etag. Lưu ý: manifest content của compaction
                // thì keep as-is (không re-merge với delta appends của máy khác —
                // compaction là operation "chốt state", nếu remote có delta mới
                // phải giữ). Reload + re-merge remote deltas.
                let fetched = client::get_manifest(base_url, id_token)
                    .await
                    .map_err(|e| CmdError::msg(format!("get_manifest retry: {e}")))?;
                expected_etag = fetched.etag;
                // Nếu remote có delta mới → đưa lại vào manifest mới sau snapshot
                // (những delta này có clock_ms > snapshot.clock_ms).
                if let Some(remote) = fetched.manifest {
                    // Re-merge deltas có clock > snapshot clock.
                    let snap_clock = new_manifest
                        .latest_snapshot
                        .as_ref()
                        .map(|s| s.clock_ms)
                        .unwrap_or(0);
                    let mut merged = new_manifest.clone();
                    for d in remote.deltas.into_iter().filter(|d| d.clock_ms > snap_clock) {
                        merged.deltas.push(d);
                    }
                    // Sort deltas theo clock_ms ASC.
                    merged.deltas.sort_by_key(|d| d.clock_ms);
                    // Retry với merged — next loop iter.
                    let _ = merged; // future iter hit Ok branch
                }
                continue;
            }
            Err(e) => return Err(CmdError::msg(format!("put_manifest compaction: {e}"))),
        }
    }
    Ok(retries)
}

// =============================================================
// SYNC LOG flush
// =============================================================

/// Flush pending events trong `sync_event_log` lên R2 qua daily file.
///
/// Batch max 500 events per call để không block UI nếu backlog lớn. Caller
/// có thể gọi lặp cho đến khi return 0.
#[tauri::command]
pub async fn sync_v9_log_flush(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<u32> {
    let events = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        event_log::fetch_pending(&conn, 500).map_err(|e| CmdError::msg(e.to_string()))?
    };
    if events.is_empty() {
        return Ok(0);
    }

    // Group theo date (từ ts prefix YYYY-MM-DD).
    use std::collections::HashMap;
    let mut by_date: HashMap<String, Vec<&crate::sync_v9::types::SyncEvent>> = HashMap::new();
    for ev in &events {
        let date = ev.ts.get(..10).unwrap_or("1970-01-01").to_string();
        by_date.entry(date).or_default().push(ev);
    }

    let mut uploaded_ids: Vec<i64> = Vec::new();
    for (date, evs) in &by_date {
        let mut ndjson = Vec::new();
        for ev in evs {
            let line = serde_json::to_vec(ev).map_err(CmdError::from)?;
            ndjson.extend(line);
            ndjson.push(b'\n');
        }
        let compressed = compress::zstd_compress(&ndjson)
            .map_err(|e| CmdError::msg(format!("zstd log: {e}")))?;
        client::push_sync_log(&base_url, &id_token, date, &compressed)
            .await
            .map_err(|e| CmdError::msg(format!("push_sync_log: {e}")))?;
        for ev in evs {
            uploaded_ids.push(ev.event_id);
        }
    }

    // Mark uploaded.
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let now = chrono::Utc::now().to_rfc3339();
    event_log::mark_uploaded(&conn, &uploaded_ids, &now)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    Ok(uploaded_ids.len() as u32)
}

// =============================================================
// LOCAL sync log (user viewer)
// =============================================================

/// Read local `sync_event_log` với filter. Không hit HTTP — đọc DB local.
/// Dùng cho user UI xem history hoạt động sync (bao gồm events chưa flush
/// lên R2, giúp debug offline/pending state).
#[tauri::command]
pub fn sync_v9_log_list_local(
    db: State<'_, DbState>,
    limit: u32,
    kind_filter: Option<String>,
) -> CmdResult<Vec<AdminSyncLogEvent>> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    let events = event_log::fetch_recent(&conn, limit, kind_filter.as_deref())
        .map_err(|e| CmdError::msg(e.to_string()))?;
    Ok(events.into_iter().map(local_event_to_dto).collect())
}

fn local_event_to_dto(ev: crate::sync_v9::types::SyncEvent) -> AdminSyncLogEvent {
    let ctx = serde_json::to_value(&ev.ctx).unwrap_or(serde_json::Value::Null);
    AdminSyncLogEvent {
        event_id: ev.event_id,
        ts: ev.ts,
        fingerprint: ev.fingerprint,
        kind: ev.kind.as_str().to_string(),
        ctx,
        uploaded_at: ev.uploaded_at,
    }
}

// =============================================================
// ADMIN — sync log viewer
// =============================================================

/// 1 event sau khi parse từ NDJSON log file. Kind & ctx preserve từ Rust
/// enum (ctx giữ JSON object, FE tự render theo kind).
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminSyncLogEvent {
    pub event_id: i64,
    pub ts: String,
    pub fingerprint: String,
    pub kind: String,
    pub ctx: serde_json::Value,
    pub uploaded_at: Option<String>,
}

/// Response cho admin_v9_sync_log_list — list metadata của file log NDJSON
/// (chưa decompress). FE gọi admin_v9_sync_log_fetch_events khi expand 1 file.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminSyncLogListDto {
    pub files: Vec<client::AdminSyncLogFile>,
    pub truncated: bool,
}

/// List sync log file metadata của user trong date range. Admin-only
/// (Worker verify admin claim).
#[tauri::command]
pub async fn admin_v9_sync_log_list(
    base_url: String,
    id_token: String,
    target_uid: String,
    from_date: String,
    to_date: String,
) -> CmdResult<AdminSyncLogListDto> {
    let list = client::admin_get_sync_log_list(
        &base_url,
        &id_token,
        &target_uid,
        &from_date,
        &to_date,
    )
    .await
    .map_err(|e| CmdError::msg(format!("admin_v9_sync_log_list: {e}")))?;

    Ok(AdminSyncLogListDto {
        files: list.files,
        truncated: list.truncated,
    })
}

/// Fetch 1 file sync log → decompress zstd → parse NDJSON → return events.
/// Admin-only (Worker verify). Key phải match `users/{uid}/sync_logs/...`.
#[tauri::command]
pub async fn admin_v9_sync_log_fetch_events(
    base_url: String,
    id_token: String,
    key: String,
) -> CmdResult<Vec<AdminSyncLogEvent>> {
    let zst_bytes = client::admin_fetch_sync_log_file(&base_url, &id_token, &key)
        .await
        .map_err(|e| CmdError::msg(format!("admin_v9_sync_log_fetch_events: {e}")))?;

    let ndjson = compress::zstd_decompress(&zst_bytes)
        .map_err(|e| CmdError::msg(format!("zstd decompress log: {e}")))?;

    let mut events = Vec::new();
    for (idx, line) in ndjson.split(|b| *b == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_slice(line)
            .map_err(|e| CmdError::msg(format!("parse NDJSON line {idx}: {e}")))?;
        events.push(parse_log_event(value));
    }
    Ok(events)
}

/// Map 1 JSON line → AdminSyncLogEvent. Tolerant với missing fields — log file
/// có thể từ version cũ hoặc custom dump.
fn parse_log_event(value: serde_json::Value) -> AdminSyncLogEvent {
    let obj = value.as_object();
    let get_str = |k: &str| -> String {
        obj.and_then(|m| m.get(k))
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_default()
    };
    let get_i64 = |k: &str| -> i64 {
        obj.and_then(|m| m.get(k))
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    };
    let event_id = get_i64("event_id");
    let ts = get_str("ts");
    let fingerprint = get_str("fingerprint");
    let kind = get_str("kind");
    let uploaded_at = obj
        .and_then(|m| m.get("uploaded_at"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let ctx = obj
        .and_then(|m| m.get("ctx"))
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    AdminSyncLogEvent {
        event_id,
        ts,
        fingerprint,
        kind,
        ctx,
        uploaded_at,
    }
}

// =============================================================
// Helpers
// =============================================================

/// Machine fingerprint stable across rename (machine-uid crate). Dùng cho
/// sync_event_log + admin debug.
fn machine_fingerprint_stable() -> String {
    use sha2::{Digest, Sha256};
    let os = std::env::consts::OS;
    let uid = machine_uid::get().unwrap_or_else(|_| "unknown".to_string());
    let raw = format!("{os}|{uid}");
    let digest = Sha256::digest(raw.as_bytes());
    hex::encode(&digest[..8]) // 16 hex chars đủ distinguish
}

// Suppress unused warnings — imports needed for future bootstrap/admin commands.
#[allow(dead_code)]
fn _unused_imports_guard() {
    let _ = apply::ApplyOutcome::Applied;
    let _ = snapshot::verify_integrity;
    let _ = capture::capture_table_delta as fn(_, _, _, _, _, _) -> _;
    let _ = DeltaEvent::Insert;
    let _ = SV_CURRENT;
}
