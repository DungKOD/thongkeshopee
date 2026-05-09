//! Deterministic content-based integer IDs cho các table có
//! `INTEGER PRIMARY KEY`. Cùng logical row → cùng id qua mọi lần tạo.
//!
//! Hash design: SHA-256 first 8 bytes → u64 → mask bit 63 → i64 positive.

use sha2::{Digest, Sha256};

const SEP: &str = "\x1F";

fn hash_parts(parts: &[&str]) -> i64 {
    let joined = parts.join(SEP);
    let hash = Sha256::digest(joined.as_bytes());
    let bytes: [u8; 8] = hash[..8].try_into().expect("sha256 digest >= 8 bytes");
    let n = u64::from_be_bytes(bytes);
    (n & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

/// Content ID cho `imported_files`. Key = `file_hash` (SHA-256 raw CSV).
pub fn imported_file_id(file_hash: &str) -> i64 {
    hash_parts(&["imported_files", file_hash])
}

/// Content ID cho `shopee_accounts`. Key = `name` (UNIQUE trong DB).
pub fn shopee_account_id(name: &str) -> i64 {
    hash_parts(&["shopee_accounts", name])
}

/// Content ID cho `raw_shopee_order_items`. Key = `(checkout_id, item_id, model_id)`.
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
