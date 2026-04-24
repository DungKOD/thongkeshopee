//! Deterministic content-based integer IDs cho các table có
//! `INTEGER PRIMARY KEY AUTOINCREMENT` VÀ được reference qua FK cross-machine.
//!
//! **Problem:** 2 máy fresh-install cùng import file → autoincrement trao cùng
//! `id=1` cho 2 rows khác file_hash. Sync → apply gặp PK collision → silent
//! IGNORE → data loss HOẶC data pollution (FK dangling tới row sai).
//!
//! **Solution:** id không còn là autoincrement counter — là hash(natural_key)
//! truncate về i63 positive. Cùng logical row trên mọi máy → cùng id.
//!
//! **Scope:** 4 tables với natural unique key ổn định:
//! - `imported_files` — natural: `file_hash` (SHA-256 content)
//! - `shopee_accounts` — natural: `name` (UNIQUE)
//! - `raw_shopee_order_items` — natural: `(checkout_id, item_id, model_id)`
//! - `raw_fb_ads` — natural: `(day_date, level, name)`
//!
//! Migration v13 rewrite existing rows + add `ON UPDATE CASCADE` cho FK
//! sao cho rewrite parent id tự cascade updated children (source_file_id,
//! order_item_id, fb_ad_id).
//!
//! Hash design: SHA-256 first 8 bytes → u64 → mask bit 63 → i64 positive.
//! Collision probability ~ 2^(-31.5) for 10^9 rows (birthday paradox). Đủ an
//! toàn cho dataset realistic (< 10M rows per user).

use sha2::{Digest, Sha256};

/// Separator giữa components để avoid hash collision ambiguity. `\x1F`
/// (Unit Separator) không xuất hiện trong any user string trong schema.
const SEP: &str = "\x1F";

/// Hash joined components → positive i64. Top bit cleared để đảm bảo > 0
/// (SQLite INTEGER signed, negative id hợp lệ nhưng lẫn SQLITE_ROWID_MAX).
fn hash_parts(parts: &[&str]) -> i64 {
    let joined = parts.join(SEP);
    let hash = Sha256::digest(joined.as_bytes());
    let bytes: [u8; 8] = hash[..8].try_into().expect("sha256 digest ≥ 8 bytes");
    let n = u64::from_be_bytes(bytes);
    (n & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

/// Content ID cho `imported_files`. Key = `file_hash` (SHA-256 raw CSV).
pub fn imported_file_id(file_hash: &str) -> i64 {
    hash_parts(&["imported_files", file_hash])
}

/// Content ID cho `shopee_accounts`. Key = `name` (UNIQUE trong DB).
/// "Mặc định" account (seeded id=1) → hash of "Mặc định".
pub fn shopee_account_id(name: &str) -> i64 {
    hash_parts(&["shopee_accounts", name])
}

/// Content ID cho `raw_shopee_order_items`. Key = natural unique
/// `(checkout_id, item_id, model_id)`.
pub fn order_item_id(checkout_id: &str, item_id: &str, model_id: &str) -> i64 {
    hash_parts(&[
        "raw_shopee_order_items",
        checkout_id,
        item_id,
        model_id,
    ])
}

/// Content ID cho `raw_fb_ads`. Key = `(day_date, level, name)` (UNIQUE).
pub fn fb_ad_id(day_date: &str, level: &str, name: &str) -> i64 {
    hash_parts(&["raw_fb_ads", day_date, level, name])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic() {
        let a = imported_file_id("hash_abc");
        let b = imported_file_id("hash_abc");
        assert_eq!(a, b, "cùng input → cùng id trên mọi máy");
    }

    #[test]
    fn hash_is_positive() {
        // Test nhiều input để spot check top bit luôn cleared.
        for s in ["", "a", "hash_x", "🚀", "a very long file hash string", "0"] {
            let id = imported_file_id(s);
            assert!(id >= 0, "id phải >= 0, got {id} cho input '{s}'");
        }
    }

    #[test]
    fn hash_differs_by_table_namespace() {
        // Cùng input nhưng namespace khác → khác id (tránh cross-table collision).
        let account = shopee_account_id("foo");
        let file = imported_file_id("foo");
        assert_ne!(account, file, "namespace 'foo' khác table phải khác id");
    }

    #[test]
    fn hash_differs_by_component_order() {
        // (a, b) vs (b, a) phải khác id — `SEP` separator đảm bảo.
        let x = order_item_id("a", "b", "c");
        let y = order_item_id("c", "b", "a");
        assert_ne!(x, y);
    }

    #[test]
    fn hash_handles_empty_model_id() {
        // model_id = "" hợp lệ trong schema (DEFAULT ''). Hash không panic.
        let id = order_item_id("checkout1", "item1", "");
        assert!(id > 0);
    }

    #[test]
    fn hash_handles_unicode() {
        let id = shopee_account_id("Tài khoản chính 🛒");
        assert!(id > 0);
    }

    #[test]
    fn fb_ad_same_name_different_day_different_id() {
        // FB data quirk: cùng ad name chạy nhiều ngày → 1 row/day. Hash phải
        // distinguish theo day_date.
        let d1 = fb_ad_id("2026-04-20", "ad_group", "My Campaign");
        let d2 = fb_ad_id("2026-04-21", "ad_group", "My Campaign");
        assert_ne!(d1, d2);
    }

    #[test]
    fn imported_file_id_stable_example() {
        // Concrete stability check — giá trị này KHÔNG được đổi giữa releases
        // (break cross-version sync). Sentinel test ngăn accident refactor.
        let id = imported_file_id("test_hash_sentinel");
        assert_eq!(
            id, 7619998408658863613_i64,
            "sentinel — refactor mà đổi hash function sẽ break cross-machine sync"
        );
    }
}
