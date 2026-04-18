---
name: check
description: Quick check Rust code (cargo check + clippy)
user_invocable: true
---

1. Chạy `cd src-tauri && cargo check 2>&1`
2. Chạy `cd src-tauri && cargo clippy 2>&1`
3. Nếu có warning/error: liệt kê và fix từng cái
4. Nếu clean: báo "All good ✓"
