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
    apply, bootstrap, capture, client, compress, descriptors, event_log, hlc, manifest, pull,
    push, snapshot,
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

    Ok(SyncV9State {
        fresh_install_pending: m.fresh_install_pending,
        last_pulled_manifest_clock_ms: m.last_pulled_manifest_clock_ms,
        last_snapshot_key: m.last_snapshot_key,
        last_snapshot_clock_ms: m.last_snapshot_clock_ms,
        pending_push_tables: pending_tables,
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

/// Full push cycle: plan → upload deltas → CAS manifest put (retry max 3).
///
/// Fresh-install guard: nếu `fresh_install_pending = 1` → no-op (rule giữ data).
#[tauri::command]
pub async fn sync_v9_push_all(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<PushReport> {
    // 1. Plan + capture + compress (lock held for DB read only).
    let (payloads, fingerprint, clock_ms) = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        if bootstrap::is_bootstrap_pending(&conn).map_err(|e| CmdError::msg(e.to_string()))? {
            return Ok(PushReport::default());
        }
        let clock = hlc::next_hlc_ms(&conn)?;
        let fp = machine_fingerprint_stable();
        let plan = push::plan_push_default(&conn, clock)
            .map_err(|e| CmdError::msg(e.to_string()))?;
        (plan, fp, clock)
    };

    if payloads.is_empty() {
        return Ok(PushReport::default());
    }

    let mut uploaded = 0u32;
    let mut skipped = 0u32;
    let mut total_bytes = 0u64;
    let mut new_entries: Vec<ManifestDeltaEntry> = Vec::new();

    // 2. Upload each (awaits, no lock).
    for payload in payloads {
        // Skip-identical check với cursor state.
        let should_skip = {
            let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            let cursor = push::read_cursor(&conn, &payload.table)
                .map_err(|e| CmdError::msg(e.to_string()))?;
            push::should_skip_by_hash(&cursor, &payload)
        };
        if should_skip {
            skipped += 1;
            continue;
        }

        // Emit log event trước khi upload (debug observability).
        {
            let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            let ts = hlc::next_hlc_rfc3339(&conn)?;
            let _ = event_log::append(
                &conn,
                &ts,
                &fingerprint,
                &SyncEventCtx::PushUpload {
                    table: payload.table.clone(),
                    cursor_lo: payload.cursor_lo.clone(),
                    cursor_hi: payload.cursor_hi.clone(),
                    bytes: payload.bytes.len() as u64,
                    delta_key: payload.r2_key.clone(),
                    row_count: payload.row_count,
                },
            );
        }

        client::upload_delta(&base_url, &id_token, &payload.r2_key, &payload.bytes)
            .await
            .map_err(|e| CmdError::msg(format!("upload_delta {}: {e}", payload.table)))?;

        total_bytes += payload.bytes.len() as u64;
        uploaded += 1;

        // Cursor advance sau upload OK (idempotent retry safe).
        {
            let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            push::mark_uploaded(&conn, &payload.table, &payload.cursor_hi, &payload.hash)
                .map_err(|e| CmdError::msg(e.to_string()))?;
        }

        new_entries.push(ManifestDeltaEntry {
            table: payload.table.clone(),
            key: payload.r2_key.clone(),
            cursor_lo: payload.cursor_lo.clone(),
            cursor_hi: payload.cursor_hi.clone(),
            clock_ms: payload.clock_ms,
            size_bytes: payload.size_bytes,
            row_count: payload.row_count,
        });
    }

    // 3. CAS manifest put (retry max 3).
    let cas_retries = cas_append_manifest_retry(&db, &base_url, &id_token, &new_entries, clock_ms)
        .await?;

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
    let Some(manifest) = fetched.manifest else {
        // No manifest — không có gì để pull.
        return Ok(PullReport::default());
    };

    // 2. Plan pending deltas (dùng local state).
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

    // 3. Per-file: fetch + parse + apply (TX per file).
    for entry in &pending {
        let bytes = client::fetch_delta(&base_url, &id_token, &entry.key)
            .await
            .map_err(|e| CmdError::msg(format!("fetch_delta {}: {e}", entry.key)))?;
        let events = pull::parse_delta_file(&bytes)
            .map_err(|e| CmdError::msg(format!("parse {}: {e}", entry.key)))?;
        let max_clock = pull::max_event_clock_ms(&events);

        // Apply trong TX (lock held qua apply — tiny vs network).
        let stats = {
            let mut conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
            let stats =
                pull::apply_events(&mut conn, &events).map_err(|e| CmdError::msg(e.to_string()))?;

            // Advance per-table cursor + absorb remote clock.
            pull::advance_pulled_cursor(&conn, &entry.table, &entry.cursor_hi)
                .map_err(|e| CmdError::msg(e.to_string()))?;
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
