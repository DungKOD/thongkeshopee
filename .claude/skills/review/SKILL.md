---
name: review
description: Review code changes hiện tại
user_invocable: true
---

1. Chạy `git diff` xem changes
2. Review theo checklist:
   - [ ] Rust: có unwrap() trong production code?
   - [ ] Rust: SQL parameterized?
   - [ ] Rust: error handling đúng?
   - [ ] React: có any type?
   - [ ] React: có missing loading/empty states?
   - [ ] Tests: có test cho logic mới?
3. Liệt kê issues tìm thấy (nếu có)
4. Đề xuất fix
