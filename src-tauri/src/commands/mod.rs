//! Tauri commands — interface giữa React UI và SQLite DB.
//!
//! Mỗi module tách theo nhóm chức năng để dễ mở rộng:
//! - `query`: đọc data từ DB (days, ui_rows, imported_files).
//! - `manual`: CRUD `manual_entries`.
//! - `imports`: INSERT raw tables + imported_files (validate single-date).
//! - `batch`: xóa batch (days, manual_rows) trong 1 transaction.

pub mod admin_view;
pub mod batch;
pub mod imports;
pub mod manual;
pub mod preview;
pub mod query;
pub mod screenshot;
pub mod sync;
pub mod sync_client;
pub mod video;

/// Wrapper lỗi cho Tauri command: serialize thành string để UI nhận được.
/// Rule: không panic, mọi lỗi đều qua đây.
#[derive(Debug, thiserror::Error)]
pub enum CmdError {
    #[error("lock poisoned: không lấy được DB connection")]
    LockPoisoned,
    #[error("db: {0}")]
    Db(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Msg(String),
}

impl CmdError {
    pub fn msg(s: impl Into<String>) -> Self {
        CmdError::Msg(s.into())
    }
}

/// Tauri yêu cầu error type phải Serialize — convert sang string.
impl serde::Serialize for CmdError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

pub type CmdResult<T> = std::result::Result<T, CmdError>;
