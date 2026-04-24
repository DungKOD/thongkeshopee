//! In-memory cache cho manifest body + etag — process-lifetime (RAM only,
//! không persist DB).
//!
//! **Mục đích:** giảm GET manifest mỗi lần push CAS. Flow gốc:
//! ```
//! push CAS attempt 0:
//!   GET manifest               (1 op B)
//!   modify body + append entries
//!   PUT with etag              (1 op A)
//! ```
//!
//! Flow sau cache:
//! ```
//! push CAS attempt 0:
//!   if cache valid:
//!     use cached body + etag (skip GET)
//!     modify + PUT
//!     412 → invalidate, fallthrough attempt 1+
//!   else: GET fresh as before
//! ```
//!
//! **Saving**: ~50% giảm GET manifest cho user push liên tục
//! (push streak share cùng cached etag từ pull trước hoặc push trước).
//!
//! **Invalidation triggers:**
//! - 412 CAS conflict (etag stale)
//! - Bootstrap restore (manifest content thay đổi triệt để)
//! - User logout / UID change (cache user khác)
//! - TTL expire (60s) — defensive nếu RTDB notify lỡ
//!
//! **Thread-safety:** `Mutex<Option<ManifestCache>>`. Lock window ngắn (clone
//! + return), không HTTP holding.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::types::Manifest;

/// TTL cache — sau thời gian này, cache coi như stale.
/// Defensive: nếu RTDB notify lỡ, push không dùng manifest cũ quá lâu.
const CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct ManifestCache {
    manifest: Manifest,
    etag: String,
    fetched_at: Instant,
}

static CACHE: Mutex<Option<ManifestCache>> = Mutex::new(None);

/// Lưu manifest mới fetched (sau pull) hoặc đã PUT thành công (sau push).
/// Caller pass etag mới từ R2 response.
pub fn cache_put(manifest: Manifest, etag: String) {
    let mut guard = CACHE.lock().expect("manifest_cache mutex poisoned");
    *guard = Some(ManifestCache {
        manifest,
        etag,
        fetched_at: Instant::now(),
    });
}

/// Lấy cache nếu còn valid (chưa hết TTL). Trả `(manifest, etag)` clone.
/// None = cache empty hoặc expired → caller phải GET fresh.
pub fn cache_get() -> Option<(Manifest, String)> {
    let guard = CACHE.lock().expect("manifest_cache mutex poisoned");
    let cache = guard.as_ref()?;
    if cache.fetched_at.elapsed() >= CACHE_TTL {
        return None;
    }
    Some((cache.manifest.clone(), cache.etag.clone()))
}

/// Force invalidate. Gọi sau:
/// - 412 CAS conflict (etag stale)
/// - Bootstrap restore (manifest content rebuilt)
/// - User logout / UID change
pub fn cache_invalidate() {
    let mut guard = CACHE.lock().expect("manifest_cache mutex poisoned");
    *guard = None;
}

/// Test serializer — cache là static global, tests parallel sẽ race.
/// Tests cache phải lock `TEST_LOCK` đầu hàm để chạy tuần tự với nhau.
/// Other tests (không touch cache) vẫn parallel bình thường.
#[cfg(test)]
pub static TEST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_v9::types::{Manifest, ManifestDeltaEntry};

    fn mk_manifest(uid: &str, delta_count: usize) -> Manifest {
        let mut m = Manifest::empty(uid.to_string());
        for i in 0..delta_count {
            m.deltas.push(ManifestDeltaEntry {
                table: "raw_shopee_clicks".to_string(),
                key: format!("delta_{i}.ndjson.zst"),
                cursor_lo: format!("{}", i * 100),
                cursor_hi: format!("{}", (i + 1) * 100),
                clock_ms: 1000 + i as i64,
                size_bytes: 1024,
                row_count: 10,
            });
        }
        m
    }

    /// Reset state trước/sau test (static var shared cross-test).
    fn reset() {
        cache_invalidate();
    }

    #[test]
    fn cache_get_empty_returns_none() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        assert!(cache_get().is_none(), "fresh cache empty");
    }

    #[test]
    fn cache_put_then_get_returns_same_data() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        let m = mk_manifest("uid-test", 2);
        cache_put(m.clone(), "etag-123".to_string());

        let got = cache_get();
        assert!(got.is_some());
        let (got_m, got_etag) = got.unwrap();
        assert_eq!(got_etag, "etag-123");
        assert_eq!(got_m.deltas.len(), 2);
        assert_eq!(got_m.uid, "uid-test");
        reset();
    }

    #[test]
    fn cache_invalidate_clears() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        cache_put(mk_manifest("u", 1), "e1".to_string());
        assert!(cache_get().is_some());
        cache_invalidate();
        assert!(cache_get().is_none(), "invalidate clears cache");
    }

    #[test]
    fn cache_overwrite_updates_etag() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        cache_put(mk_manifest("u", 1), "e1".to_string());
        cache_put(mk_manifest("u", 5), "e2".to_string());
        let (m, etag) = cache_get().unwrap();
        assert_eq!(etag, "e2", "cache overwritten với etag mới");
        assert_eq!(m.deltas.len(), 5);
        reset();
    }

    #[test]
    fn cache_expires_after_ttl() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        // Không thể wait 60s in test → set fetched_at thẳng vào past.
        cache_put(mk_manifest("u", 1), "e1".to_string());
        {
            let mut guard = CACHE.lock().unwrap();
            if let Some(c) = guard.as_mut() {
                c.fetched_at = Instant::now() - CACHE_TTL - Duration::from_secs(1);
            }
        }
        assert!(
            cache_get().is_none(),
            "cache > TTL → return None (force GET fresh)"
        );
        reset();
    }

    #[test]
    fn cache_within_ttl_still_valid() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        cache_put(mk_manifest("u", 1), "e1".to_string());
        // Just-set cache, elapsed = ~0 → valid
        assert!(cache_get().is_some(), "vừa cache xong → valid");
        reset();
    }

    #[test]
    fn cache_clone_independent_from_internal() {
        let _g = TEST_LOCK.lock().unwrap();
        reset();
        let m = mk_manifest("u", 1);
        cache_put(m, "e1".to_string());
        let (mut got_m, _) = cache_get().unwrap();
        got_m.deltas.clear();
        // Cache internal không bị mutate qua got_m clone
        let (got_m2, _) = cache_get().unwrap();
        assert_eq!(got_m2.deltas.len(), 1, "clone không leak mutation");
        reset();
    }
}
