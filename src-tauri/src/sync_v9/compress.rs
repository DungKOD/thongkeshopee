//! zstd compress/decompress + content hash helpers cho delta files.
//!
//! Pattern tái sử dụng từ `commands::sync::zstd_compress_mt` (v8 sync).
//! Level 3 multi-thread: fast compress, ratio OK. Workers = num_cpus/2.

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use std::io::Read;

/// Magic bytes của zstd frame. Validate để reject file sai format.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Compress input bằng zstd level 3, multi-thread.
///
/// Dùng cho delta NDJSON files + snapshots. Workers = `num_cpus / 2`
/// (min 1) để không độc chiếm CPU khi UI cần responsive.
pub fn zstd_compress(input: &[u8]) -> Result<Vec<u8>> {
    let workers = std::cmp::max(1, (num_cpus::get() / 2) as u32);
    let mut compressor = zstd::bulk::Compressor::new(3).context("zstd compressor init")?;
    let _ = compressor.set_parameter(zstd::stream::raw::CParameter::NbWorkers(workers));
    compressor.compress(input).context("zstd compress")
}

/// Decompress zstd payload, validate magic bytes trước.
///
/// Dùng stream decoder (grow buffer dynamic) để handle payload nén ratio cao.
/// Reject non-zstd input với error message chứa head hex để debug.
pub fn zstd_decompress(input: &[u8]) -> Result<Vec<u8>> {
    if input.len() < 4 || input[0..4] != ZSTD_MAGIC {
        let head_hex: String = input
            .iter()
            .take(16)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join(" ");
        anyhow::bail!(
            "payload không phải zstd frame — size={} bytes, head=[{}]",
            input.len(),
            head_hex
        );
    }
    let mut decoder = zstd::stream::read::Decoder::new(input).context("zstd decoder init")?;
    let mut out = Vec::with_capacity(input.len() * 5);
    decoder.read_to_end(&mut out).context("zstd decompress")?;
    Ok(out)
}

/// SHA-256 hex của bytes. Dùng cho skip-identical: cùng hash = không upload
/// lại. SHA-256 chọn cho consistency với `imported_files.file_hash`.
///
/// Collision practical ~0 trong scope per-user (~nghìn delta files). Đủ
/// strength không cần crypto-grade nhưng không để lỗi hash collision đè.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compress_decompress_roundtrip() {
        let input = b"hello world ".repeat(1000); // 12KB repetitive — nén ratio cao
        let compressed = zstd_compress(&input).unwrap();
        assert!(
            compressed.len() < input.len() / 5,
            "repetitive text phải nén ít nhất 5x"
        );
        let back = zstd_decompress(&compressed).unwrap();
        assert_eq!(back, input);
    }

    #[test]
    fn decompress_rejects_non_zstd() {
        let err = zstd_decompress(b"not a zstd payload").unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("không phải zstd frame"),
            "error message phải rõ: {msg}"
        );
    }

    #[test]
    fn decompress_rejects_empty() {
        let err = zstd_decompress(&[]).unwrap_err();
        assert!(format!("{err}").contains("không phải zstd frame"));
    }

    #[test]
    fn sha256_deterministic() {
        let a = sha256_hex(b"abc");
        let b = sha256_hex(b"abc");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64, "SHA-256 hex phải 64 char");
    }

    #[test]
    fn sha256_changes_with_input() {
        assert_ne!(sha256_hex(b"abc"), sha256_hex(b"abd"));
    }

    #[test]
    fn compress_empty_is_valid() {
        let compressed = zstd_compress(&[]).unwrap();
        let back = zstd_decompress(&compressed).unwrap();
        assert_eq!(back, Vec::<u8>::new());
    }
}
