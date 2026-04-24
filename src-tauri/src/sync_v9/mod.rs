//! Sync v9 — per-table incremental delta sync.
//!
//! Kiến trúc: R2 lưu 3 loại object cho mỗi user `users/{uid}/`:
//! `manifest.json` (CAS etag — single source of truth), `deltas/{table}/*.ndjson.zst`
//! (append-only delta events), `snapshots/*.db.zst` (periodic compacted state).
//! Client push chỉ rows mới (cursor > last_uploaded), pull chỉ delta files
//! chưa apply (diff manifest vs local cursor_state).
//!
//! Spec đầy đủ: `docs/SYNC_V9_PLAN.md`. Tham chiếu khi implement phase sau.
//!
//! Phase 1 (hiện tại): types + event log helpers. Các phase sau sẽ thêm
//! push/pull/snapshot/compaction và remove `allow(dead_code)` khi code được wire up.

// Phase 1 deliverable = types + helpers. Phase 2+ sẽ use chúng → tắt
// dead_code cho module này cho đến khi wire up xong.
#![allow(dead_code)]

pub mod apply;
pub mod bootstrap;
pub mod capture;
pub mod client;
pub mod compaction;
pub mod compress;
pub mod content_id;
pub mod descriptors;
pub mod event_log;
pub mod hlc;
pub mod manifest;
pub mod pull;
pub mod push;
pub mod snapshot;
pub mod types;

#[cfg(test)]
mod integration_tests;

/// Schema version của delta event format. Tương ứng với `_schema_version = 11`.
/// Event reader check `event.sv` vs `SV_CURRENT` để biết có cần migrate row
/// không (K1-K3 trong plan).
pub const SV_CURRENT: u32 = 11;

/// Delta batch size threshold. Mỗi delta file NDJSON sau zstd phải ≤ giá trị
/// này. Locked = 5MB per plan Q1.
pub const DELTA_BATCH_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Ring buffer size cho `sync_event_log`. Quá giá trị này → prune từ cũ nhất.
/// Per plan O1a.
pub const EVENT_LOG_MAX: u32 = 5000;

/// Compaction trigger threshold. Manifest có nhiều hơn số delta entries này
/// → client tự động compact thành snapshot mới. Per plan Q3.
pub const COMPACTION_DELTA_THRESHOLD: usize = 100;
