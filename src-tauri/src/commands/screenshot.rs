//! Lưu ảnh PNG xuống PC.
//!
//! FE render DOM (DayBlock) thành PNG qua `html-to-image` → base64 string →
//! invoke lệnh này với path user đã chọn qua save dialog.

use super::{CmdError, CmdResult};
use base64::Engine as _;
use std::path::PathBuf;

/// Ghi PNG bytes (base64-encoded, có/không prefix `data:image/png;base64,`)
/// xuống `path`. Trả lại path tuyệt đối đã ghi để UI hiển thị.
#[tauri::command]
pub async fn save_png(path: String, base64_data: String) -> CmdResult<String> {
    let stripped = base64_data
        .strip_prefix("data:image/png;base64,")
        .unwrap_or(&base64_data);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(stripped)
        .map_err(|e| CmdError::msg(format!("base64 decode: {e}")))?;
    let path_buf = PathBuf::from(&path);
    tokio::fs::write(&path_buf, &bytes).await?;
    Ok(path)
}
