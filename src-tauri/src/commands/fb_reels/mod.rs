//! Đăng video Reels lên Facebook Page qua Graph API.
//!
//! Flow:
//! 1. User paste long-lived Page Token vào Settings → app gọi
//!    `GET /me/accounts` lấy danh sách Pages → user chọn Pages để lưu.
//! 2. User chọn file video local (≤ 100MB) + caption + Page → enqueue post
//!    (status `pending`).
//! 3. Upload chạy 3 bước Graph API: `upload_phase=start` → stream binary
//!    với progress events → `upload_phase=finish` → fetch permalink.
//! 4. Có thể `scheduled_time_ms` để FB tự đăng sau (≥ 10 phút trong tương lai).
//!
//! Phiên bản v1: upload đơn giản, không resumable. File ≤ 100MB.
//! Sequential queue — frontend gọi `fb_upload_reel` từng post một.

pub mod commands;
mod graph_api;
mod types;
mod upload;

pub use commands::*;
