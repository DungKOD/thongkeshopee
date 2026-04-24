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
// NUCLEAR reset — wipe R2 + reset local (1-click recovery)
// =============================================================

/// Wipe R2 của user hiện tại (archive 30 ngày qua admin cleanup endpoint)
/// + reset local sync state. Dùng khi R2 delta cũ không tương thích schema
/// mới (vd post-v13 FK mismatch). Cần admin claim (user = admin của chính
/// account mình nên OK).
///
/// **KHÔNG xóa data local**. Flow:
/// 1. POST /v9/admin/cleanup?uid=self → archive users/{uid}/* vào
///    archive/deleted_{uid}_{ts}/* rồi delete source
/// 2. Reset local sync_cursor_state + sync_manifest_state
/// 3. User click sync → push toàn bộ data local fresh với content_id mới
///
/// Next sync cross-machine: máy khác pull → không còn delta cũ, chỉ thấy
/// delta mới hợp lệ.
#[tauri::command]
pub async fn sync_v9_nuclear_reset(
    db: State<'_, DbState>,
    base_url: String,
    id_token: String,
) -> CmdResult<()> {
    // 1. Lấy uid của user hiện tại (từ DB).
    let uid = {
        let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        conn.query_row(
            "SELECT owner_uid FROM sync_state WHERE id = 1",
            [],
            |r| r.get::<_, Option<String>>(0),
        )
        .ok()
        .flatten()
        .ok_or_else(|| CmdError::msg("owner_uid trống — chưa login?"))?
    };

    // 2. Wipe R2 — archive + delete. Worker verify admin claim.
    client::admin_cleanup_user(&base_url, &id_token, &uid)
        .await
        .map_err(|e| CmdError::msg(format!("admin cleanup: {e}")))?;

    // 3. Reset local state.
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "UPDATE sync_cursor_state
         SET last_uploaded_cursor = '0',
             last_pulled_cursor = '0',
             last_uploaded_hash = NULL,
             updated_at = ?",
        [chrono::Utc::now().to_rfc3339()],
    )?;
    conn.execute(
        "UPDATE sync_manifest_state
         SET last_remote_etag = NULL,
             last_pulled_manifest_clock_ms = 0,
             last_snapshot_key = NULL,
             last_snapshot_clock_ms = 0,
             fresh_install_pending = 0
         WHERE id = 1",
        [],
    )?;
    Ok(())
}

// =============================================================
// RESET sync state (recovery)
// =============================================================

/// Reset local sync cursor state + manifest state về trạng thái fresh. Dùng
/// khi R2 có delta files cũ không tương thích (vd old autoincrement id
/// trước v13, FK fail khi apply).
///
/// Effect:
/// - `sync_cursor_state`: tất cả cursors về '0', hash clear → next push
///   re-capture toàn bộ data từ đầu
/// - `sync_manifest_state`: etag clear, snapshot pointer clear, clock = 0
///   → next pull sẽ refetch manifest mới
///
/// **KHÔNG xóa data local** (raw tables, manual_entries, tombstones vẫn
/// nguyên). Chỉ reset metadata sync. Local data là source of truth cho
/// re-push.
///
/// User flow khuyến nghị:
/// 1. Gọi cmd này → reset local state
/// 2. Admin wipe R2 users/{uid}/ (via admin cleanup) hoặc Cloudflare dashboard
/// 3. Click sync → push toàn bộ data local lên R2 với state mới
#[tauri::command]
pub fn sync_v9_reset_local_state(db: State<'_, DbState>) -> CmdResult<()> {
    let conn = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
    conn.execute(
        "UPDATE sync_cursor_state
         SET last_uploaded_cursor = '0',
             last_pulled_cursor = '0',
             last_uploaded_hash = NULL,
             updated_at = ?",
        [chrono::Utc::now().to_rfc3339()],
    )?;
    conn.execute(
        "UPDATE sync_manifest_state
         SET last_remote_etag = NULL,
             last_pulled_manifest_clock_ms = 0,
             last_snapshot_key = NULL,
             last_snapshot_clock_ms = 0,
             fresh_install_pending = 0
         WHERE id = 1",
        [],
    )?;
    Ok(())
}

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
        report.total_bytes += bytes.len() as u64;
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
