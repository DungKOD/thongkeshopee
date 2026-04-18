---
paths:
  - "src-tauri/**/*.rs"
---

# Rust Backend Rules

- Mọi function public phải có doc comment `///`
- Error types dùng `anyhow::Error` hoặc custom enum
- SQL queries phải parameterized (chống injection)
- Dùng `rusqlite::params![]` macro cho parameters
- Test helper functions đặt trong `#[cfg(test)]` block
- Khi thêm Tauri command mới: register trong `lib.rs`
