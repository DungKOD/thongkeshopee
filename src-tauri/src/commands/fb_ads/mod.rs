//! Bulk create Facebook Ad campaigns từ template — Phase 1.
//!
//! Phase 1 (đã làm): account/template/draft CRUD + Marketing API read endpoints.
//! Phase 2 (TODO): upload video ad + create campaign/adset/ad/creative + batch executor.
//!
//! Tách module riêng với `fb_reels` vì:
//! - Marketing API surface khác Graph API for Pages
//! - Object hierarchy 3 cấp (camp → adset → ad → creative)
//! - Status hierarchy phức tạp hơn (ACTIVE/PAUSED/DELETED ở 3 cấp)
//! - Rủi ro tài chính cao → safety net mạnh hơn

mod clone;
pub mod commands;
mod executor;
mod graph_api;
mod types;
mod upload;

pub use commands::*;
