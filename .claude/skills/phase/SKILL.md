---
name: phase
description: Bắt đầu triển khai 1 phase từ IMPLEMENTATION_GUIDE
user_invocable: true
args: phase_number
---

Đọc file `docs/IMPLEMENTATION_GUIDE.md` và tìm Phase {phase_number}.

1. Đọc mục tiêu của Phase
2. Liệt kê các bước cần làm
3. Tạo task list (TodoWrite) cho từng bước
4. Bắt đầu bước 1, làm tuần tự
5. Sau mỗi bước: chạy `cargo check` (nếu Rust) hoặc type check (nếu React)
6. Cuối Phase: chạy tests tương ứng từ `docs/TEST_GUIDE.md`
7. Tất cả test pass → báo Phase hoàn thành
