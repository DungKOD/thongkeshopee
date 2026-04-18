---
name: test
description: Chạy tất cả tests (Rust + React) và report kết quả
user_invocable: true
---

Chạy tests theo thứ tự:

1. Chạy `cd src-tauri && cargo test 2>&1` - báo cáo kết quả
2. Chạy `npm test -- --run 2>&1` - báo cáo kết quả
3. Tổng hợp: bao nhiêu test pass, bao nhiêu fail
4. Nếu có test fail: đọc error, phân tích nguyên nhân, đề xuất fix
