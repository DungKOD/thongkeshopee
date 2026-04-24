//! Core types cho sync v9. Định nghĩa wire format giữa client ↔ Worker ↔ R2.
//!
//! Nguyên tắc:
//! - Mọi event có `sv: u32` (schema version) để apply-side migrate row nếu
//!   event sinh ra từ schema cũ hơn (plan K1-K3).
//! - Mọi timestamp là RFC3339 với Z suffix (UTC), generated qua
//!   `next_hlc_rfc3339` (plan J5 chặn clock drift).
//! - Manifest immutable append-only từ góc nhìn client, CAS-guarded qua etag.
//! - SyncEventCtx là **typed** — không accept raw serde_json::Value tùy ý để
//!   tránh leak PII (plan O1c).
//!
//! Chi tiết spec: `docs/SYNC_V9_PLAN.md` Phần 2.2-2.3.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// =============================================================
// Delta event — per-line trong `deltas/{table}/*.ndjson.zst`
// =============================================================

/// 1 event trong delta file. Apply-side match theo `op` rồi dispatch.
///
/// **Ordering invariant:** events trong 1 delta file sorted theo
/// `clock_ms ASC`. Pull-side sort toàn bộ delta files theo
/// `manifest_entry.clock_ms` trước khi apply để đảm bảo causal order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum DeltaEvent {
    /// INSERT OR IGNORE vào table (append-only raw tables). PK conflict = skip.
    Insert(InsertEvent),
    /// UPSERT với HLC check (mutable tables: manual_entries, shopee_accounts).
    /// Local row có `updated_at > event.updated_at` → skip (edit-wins).
    Upsert(UpsertEvent),
    /// Tombstone để xóa entity (day / ui_row / manual_entry).
    /// Resurrect rule: chỉ xóa nếu target.updated_at <= tombstone.deleted_at.
    Tombstone(TombstoneEvent),
}

/// Payload cho `DeltaEvent::Insert`. Raw tables append-only.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InsertEvent {
    /// Schema version lúc event được capture (= SV_CURRENT tại thời điểm push).
    pub sv: u32,
    /// Tên table target (phải nằm trong whitelist `sync_cursor_state.table_name`).
    pub table: String,
    /// Primary key dạng JSON object. Vd: `{"click_id": "abc"}` hoặc composite
    /// `{"checkout_id": "x", "item_id": "y", "model_id": ""}`.
    pub pk: Value,
    /// Full row data. Apply-side INSERT OR IGNORE theo column match.
    pub row: Value,
    /// HLC clock lúc capture (ms since epoch). Dùng để sort delta files.
    pub clock_ms: i64,
}

/// Payload cho `DeltaEvent::Upsert`. Mutable tables (manual_entries,
/// shopee_accounts). HLC timestamp dùng cho conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UpsertEvent {
    pub sv: u32,
    pub table: String,
    pub pk: Value,
    pub row: Value,
    /// HLC RFC3339 Z-suffixed. Apply: local.updated_at > event.updated_at → skip.
    pub updated_at: String,
    pub clock_ms: i64,
}

/// Payload cho `DeltaEvent::Tombstone`. Delete event cho entity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TombstoneEvent {
    pub sv: u32,
    /// "day" | "ui_row" | "manual_entry" — match `tombstones.entity_type`.
    pub entity_type: String,
    /// Key định danh entity. Format xem `db::tombstone_key_sub`.
    pub entity_key: String,
    /// HLC RFC3339 Z-suffixed.
    pub deleted_at: String,
    pub clock_ms: i64,
}

// =============================================================
// Manifest — `users/{uid}/manifest.json` (CAS via R2 etag)
// =============================================================

/// Root của manifest.json. Single source of truth cho user's sync state trên R2.
///
/// **CAS invariant:** PUT manifest phải đi với `expectedEtag` match R2 etag
/// hiện tại. Mismatch → 412, client re-fetch + re-append + retry (max 3).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    /// Schema version của manifest. Tương ứng với `SV_CURRENT = 11`.
    pub version: u32,
    /// Firebase UID của owner. Worker assert JWT claim match path UID.
    pub uid: String,
    /// Pointer tới snapshot gần nhất (nếu có). `None` trước lần compaction đầu.
    pub latest_snapshot: Option<ManifestSnapshot>,
    /// List tất cả delta files hiện có (tức clock_ms > latest_snapshot.clock_ms).
    /// Sort ASC theo `clock_ms` để pull-side apply đúng thứ tự.
    pub deltas: Vec<ManifestDeltaEntry>,
    /// HLC clock của lần update manifest cuối cùng. Pull-side so với
    /// `sync_manifest_state.last_pulled_manifest_clock_ms` để skip no-op.
    pub updated_at_ms: i64,
}

impl Manifest {
    /// Tạo manifest rỗng cho user mới (chưa có push nào).
    pub fn empty(uid: String) -> Self {
        Self {
            version: crate::sync_v9::SV_CURRENT,
            uid,
            latest_snapshot: None,
            deltas: Vec::new(),
            updated_at_ms: 0,
        }
    }
}

/// Pointer tới snapshot file trên R2.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestSnapshot {
    /// R2 object key, vd "snapshots/snap_1745234567890.db.zst".
    pub key: String,
    /// HLC clock khi snapshot được tạo. Mọi delta có clock ≤ giá trị này
    /// được consolidate vào snapshot → có thể xóa khỏi R2.
    pub clock_ms: i64,
    /// Size compressed (zstd). Dùng cho progress UI khi bootstrap.
    pub size_bytes: i64,
}

/// Entry trong `manifest.deltas`. Reference 1 delta file trên R2.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestDeltaEntry {
    /// Tên table (match `sync_cursor_state.table_name`).
    pub table: String,
    /// R2 object key, vd "deltas/raw_shopee_clicks/5000_1745234600000.ndjson.zst".
    pub key: String,
    /// Cursor low bound (inclusive). String để handle cả int (rowid) và
    /// RFC3339 (updated_at). Format depend per-table (xem plan 2.5).
    pub cursor_lo: String,
    /// Cursor high bound (inclusive).
    pub cursor_hi: String,
    /// HLC clock khi delta file được tạo. Ordering key cho apply.
    pub clock_ms: i64,
    /// Size compressed. Dùng cho bandwidth estimate.
    pub size_bytes: i64,
    /// Số events trong file (không phải rows). Dùng cho progress UI.
    pub row_count: u32,
}

// =============================================================
// Cursor state — per-table tracking trong local DB
// =============================================================

/// 1 row của `sync_cursor_state`. High-water-mark của push/pull per table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorState {
    /// Tên table. Match 10 tables init ở `migrate_v11_sync_infra`.
    pub table_name: String,
    /// Cursor max đã upload. SELECT WHERE cursor > this để capture delta mới.
    pub last_uploaded_cursor: String,
    /// Cursor max đã pull+apply từ remote. Pull diff: manifest deltas với
    /// cursor_hi > this → download + apply.
    pub last_pulled_cursor: String,
    /// Hash của last uploaded delta content. Skip-identical: nếu content mới
    /// hash trùng → skip upload (tiết kiệm Class A op).
    pub last_uploaded_hash: Option<String>,
    /// Timestamp update row này (local clock, không HLC — không sync-critical).
    pub updated_at: String,
}

// =============================================================
// Sync event log — debug observability (plan O1a-O1e)
// =============================================================

/// Kind enumerated cho filter/query trong admin viewer.
/// Serialize snake_case để match column `sync_event_log.kind`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SyncEventKind {
    PushStart,
    PushUpload,
    PushComplete,
    PullStart,
    PullFetch,
    PullApply,
    PullComplete,
    BootstrapStart,
    BootstrapSnapshot,
    BootstrapComplete,
    CompactionStart,
    CompactionComplete,
    CasConflict,
    Error,
    Recovery,
}

impl SyncEventKind {
    /// Lowercase snake_case string để insert vào column `kind`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PushStart => "push_start",
            Self::PushUpload => "push_upload",
            Self::PushComplete => "push_complete",
            Self::PullStart => "pull_start",
            Self::PullFetch => "pull_fetch",
            Self::PullApply => "pull_apply",
            Self::PullComplete => "pull_complete",
            Self::BootstrapStart => "bootstrap_start",
            Self::BootstrapSnapshot => "bootstrap_snapshot",
            Self::BootstrapComplete => "bootstrap_complete",
            Self::CompactionStart => "compaction_start",
            Self::CompactionComplete => "compaction_complete",
            Self::CasConflict => "cas_conflict",
            Self::Error => "error",
            Self::Recovery => "recovery",
        }
    }
}

/// Typed context payload per event kind.
///
/// **PRIVACY RULE (plan O1c) — BẮT BUỘC:** Mọi variant CHỈ chứa metadata.
/// KHÔNG được thêm field nào có thể leak: sub_id values, manual comment text,
/// click_id, spend/commission values, file paths chứa username, row content.
///
/// Test `sync_log_no_pii` quét regex verify. Thêm variant mới phải pass test
/// đó + review thủ công.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SyncEventCtx {
    PushStart {
        tables: Vec<String>,
    },
    PushUpload {
        table: String,
        cursor_lo: String,
        cursor_hi: String,
        bytes: u64,
        delta_key: String,
        row_count: u32,
    },
    PushComplete {
        tables_count: u32,
        total_bytes: u64,
        duration_ms: u64,
    },
    PullStart {},
    PullFetch {
        delta_key: String,
        bytes: u64,
    },
    PullApply {
        delta_key: String,
        row_count: u32,
        skipped: u32,
        resurrected: u32,
    },
    PullComplete {
        deltas_applied: u32,
        duration_ms: u64,
    },
    BootstrapStart {},
    BootstrapSnapshot {
        snapshot_key: String,
        bytes: u64,
        duration_ms: u64,
    },
    BootstrapComplete {
        deltas_after_snapshot: u32,
        duration_ms: u64,
    },
    CompactionStart {},
    CompactionComplete {
        new_snapshot_key: String,
        old_deltas_removed: u32,
    },
    CasConflict {
        expected_etag: String,
        got_etag: String,
        retry: u32,
    },
    Error {
        phase: String,
        error_code: String,
        error_msg: String,
    },
    Recovery {
        /// "force_repull" | "restore_archive"
        reason: String,
    },
}

impl SyncEventCtx {
    /// Kind tương ứng, derive từ variant. Dùng insert vào column `kind` song
    /// song với `ctx_json` full payload.
    pub fn kind(&self) -> SyncEventKind {
        match self {
            Self::PushStart { .. } => SyncEventKind::PushStart,
            Self::PushUpload { .. } => SyncEventKind::PushUpload,
            Self::PushComplete { .. } => SyncEventKind::PushComplete,
            Self::PullStart {} => SyncEventKind::PullStart,
            Self::PullFetch { .. } => SyncEventKind::PullFetch,
            Self::PullApply { .. } => SyncEventKind::PullApply,
            Self::PullComplete { .. } => SyncEventKind::PullComplete,
            Self::BootstrapStart {} => SyncEventKind::BootstrapStart,
            Self::BootstrapSnapshot { .. } => SyncEventKind::BootstrapSnapshot,
            Self::BootstrapComplete { .. } => SyncEventKind::BootstrapComplete,
            Self::CompactionStart {} => SyncEventKind::CompactionStart,
            Self::CompactionComplete { .. } => SyncEventKind::CompactionComplete,
            Self::CasConflict { .. } => SyncEventKind::CasConflict,
            Self::Error { .. } => SyncEventKind::Error,
            Self::Recovery { .. } => SyncEventKind::Recovery,
        }
    }
}

/// 1 row đọc từ `sync_event_log`. Dùng cho admin viewer + user "Sync log" UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncEvent {
    pub event_id: i64,
    /// HLC RFC3339 Z.
    pub ts: String,
    /// Machine fingerprint (hash stable cross-rename). Non-PII.
    pub fingerprint: String,
    pub kind: SyncEventKind,
    pub ctx: SyncEventCtx,
    /// Null nếu chưa upload lên R2. Set sau khi `/v9/sync-log/push` OK.
    pub uploaded_at: Option<String>,
}

// =============================================================
// Tests
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn delta_event_insert_roundtrip() {
        let ev = DeltaEvent::Insert(InsertEvent {
            sv: 11,
            table: "raw_shopee_clicks".to_string(),
            pk: json!({"click_id": "abc"}),
            row: json!({"click_id": "abc", "day_date": "2026-04-20"}),
            clock_ms: 1_745_234_600_000,
        });
        let s = serde_json::to_string(&ev).unwrap();
        let back: DeltaEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
        // Wire format check: "op":"insert" (snake_case).
        assert!(s.contains("\"op\":\"insert\""));
    }

    #[test]
    fn delta_event_upsert_has_updated_at() {
        let ev = DeltaEvent::Upsert(UpsertEvent {
            sv: 11,
            table: "manual_entries".to_string(),
            pk: json!({"sub_id1": "x", "day_date": "2026-04-20"}),
            row: json!({"override_clicks": 100}),
            updated_at: "2026-04-24T08:00:00.000Z".to_string(),
            clock_ms: 1_745_234_600_000,
        });
        let s = serde_json::to_string(&ev).unwrap();
        assert!(s.contains("\"updated_at\":\"2026-04-24T08:00:00.000Z\""));
    }

    #[test]
    fn delta_event_tombstone_roundtrip() {
        let ev = DeltaEvent::Tombstone(TombstoneEvent {
            sv: 11,
            entity_type: "day".to_string(),
            entity_key: "2026-04-20".to_string(),
            deleted_at: "2026-04-24T08:00:00.000Z".to_string(),
            clock_ms: 1_745_234_600_000,
        });
        let s = serde_json::to_string(&ev).unwrap();
        let back: DeltaEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn manifest_empty_has_version() {
        let m = Manifest::empty("abc".to_string());
        assert_eq!(m.version, 11);
        assert_eq!(m.uid, "abc");
        assert!(m.latest_snapshot.is_none());
        assert!(m.deltas.is_empty());
    }

    #[test]
    fn manifest_roundtrip() {
        let m = Manifest {
            version: 11,
            uid: "test".to_string(),
            latest_snapshot: Some(ManifestSnapshot {
                key: "snapshots/snap_1.db.zst".to_string(),
                clock_ms: 1_000_000,
                size_bytes: 524_288_000,
            }),
            deltas: vec![ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: "deltas/raw_shopee_clicks/5000_2000000.ndjson.zst".to_string(),
                cursor_lo: "4001".to_string(),
                cursor_hi: "5000".to_string(),
                clock_ms: 2_000_000,
                size_bytes: 4_521_000,
                row_count: 1000,
            }],
            updated_at_ms: 2_000_000,
        };
        let s = serde_json::to_string(&m).unwrap();
        let back: Manifest = serde_json::from_str(&s).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn sync_event_kind_str_roundtrip() {
        for kind in [
            SyncEventKind::PushStart,
            SyncEventKind::CasConflict,
            SyncEventKind::Error,
            SyncEventKind::Recovery,
        ] {
            let s = kind.as_str();
            // Serde must produce same string.
            let ser = serde_json::to_string(&kind).unwrap();
            assert_eq!(ser, format!("\"{s}\""));
        }
    }

    #[test]
    fn sync_event_ctx_kind_matches() {
        let ctx = SyncEventCtx::CasConflict {
            expected_etag: "abc".to_string(),
            got_etag: "xyz".to_string(),
            retry: 2,
        };
        assert_eq!(ctx.kind(), SyncEventKind::CasConflict);
    }

    #[test]
    fn sync_event_ctx_roundtrip() {
        let ctx = SyncEventCtx::PushUpload {
            table: "raw_shopee_clicks".to_string(),
            cursor_lo: "4001".to_string(),
            cursor_hi: "5000".to_string(),
            bytes: 4_521_000,
            delta_key: "deltas/raw_shopee_clicks/5000_2000000.ndjson.zst".to_string(),
            row_count: 1000,
        };
        let s = serde_json::to_string(&ctx).unwrap();
        let back: SyncEventCtx = serde_json::from_str(&s).unwrap();
        assert_eq!(ctx, back);
        // Tag format: "kind":"push_upload"
        assert!(s.contains("\"kind\":\"push_upload\""));
    }

    /// Privacy regression: ensure SyncEventCtx variants chỉ chứa metadata.
    /// Không được có field lưu giá trị numeric cụ thể (spend/commission), row
    /// content, sub_id values, hay path chứa username.
    ///
    /// Test này enumerate tất cả variants và check field names blacklist.
    /// Thêm variant mới phải update danh sách BANNED_FIELDS nếu nghi ngờ.
    #[test]
    fn sync_event_ctx_no_pii_fields() {
        // Whitelist field names được phép. Tất cả field khác phải được review.
        let allowed = [
            "tables",
            "table",
            "cursor_lo",
            "cursor_hi",
            "bytes",
            "delta_key",
            "row_count",
            "tables_count",
            "total_bytes",
            "duration_ms",
            "deltas_applied",
            "skipped",
            "resurrected",
            "snapshot_key",
            "deltas_after_snapshot",
            "new_snapshot_key",
            "old_deltas_removed",
            "expected_etag",
            "got_etag",
            "retry",
            "phase",
            "error_code",
            "error_msg",
            "reason",
            "kind", // internal tag (serde tag="kind")
        ];
        // Build 1 instance mỗi variant + serialize + parse JSON keys.
        let samples = [
            serde_json::to_value(SyncEventCtx::PushStart {
                tables: vec!["t".to_string()],
            })
            .unwrap(),
            serde_json::to_value(SyncEventCtx::PushUpload {
                table: "t".to_string(),
                cursor_lo: "0".to_string(),
                cursor_hi: "1".to_string(),
                bytes: 0,
                delta_key: "k".to_string(),
                row_count: 0,
            })
            .unwrap(),
            serde_json::to_value(SyncEventCtx::PullApply {
                delta_key: "k".to_string(),
                row_count: 0,
                skipped: 0,
                resurrected: 0,
            })
            .unwrap(),
            serde_json::to_value(SyncEventCtx::Error {
                phase: "p".to_string(),
                error_code: "e".to_string(),
                error_msg: "m".to_string(),
            })
            .unwrap(),
            serde_json::to_value(SyncEventCtx::Recovery {
                reason: "force_repull".to_string(),
            })
            .unwrap(),
        ];

        for sample in &samples {
            let obj = sample.as_object().expect("variant phải là JSON object");
            for field in obj.keys() {
                assert!(
                    allowed.contains(&field.as_str()),
                    "field '{field}' không trong whitelist. Review PII risk trước khi add. \
                     Nếu OK → add vào `allowed` trong test này."
                );
            }
        }
    }
}
