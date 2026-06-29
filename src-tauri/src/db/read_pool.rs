//! Read-only connection pool — N reader song song nhờ SQLite WAL mode.
//!
//! Trước đó: 1 `Mutex<Connection>` cho cả app (`DbState`) → mọi query queue
//! tuần tự dù WAL cho phép multi-reader concurrent. Pool này mở thêm N
//! connection read-only vào cùng DB file, mỗi cái có `Mutex<Connection>`
//! riêng. `acquire()` round-robin `try_lock` → connection busy thì thử tiếp,
//! hết cả N thì block trên 1 (FIFO-ish).
//!
//! Hiệu quả: 5 query Overview (`Promise.all` ở FE) chạy thật parallel trên
//! N core thay vì serial. Write vẫn đi qua `DbState` (1 writer trong WAL).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, RwLock};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

/// Số read connection mặc định. 8 cover được initial mount (3 query) +
/// Overview (5 query) chạy đồng thời không queue. Cost: ~256KB cache nội bộ
/// SQLite per-conn (cache_size = -32768 = 32MB nhưng SQLite lazy-grow theo
/// page touched, thực tế thường chỉ vài MB tổng).
pub const DEFAULT_READ_POOL_SIZE: usize = 8;

/// Pool gồm N `Mutex<Connection>` riêng + atomic round-robin counter để
/// spread acquire (tránh pile lên slot 0).
///
/// `db_path` nằm trong `RwLock` để hỗ trợ workspace switch: `swap_to()` thay
/// từng connection trong-place (giữ nguyên Mutex slot, lifetime guard không
/// đổi) rồi update path. Reader đang giữ guard cũ vẫn dùng conn cũ tới khi
/// drop; reader mới sẽ nhận conn workspace mới.
pub struct ReadPool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
    db_path: RwLock<PathBuf>,
}

/// RAII guard cho 1 read connection. Drop = trả về pool. Deref → Connection
/// nên consumer dùng giống `MutexGuard<Connection>` cũ.
pub struct ReadConn<'a> {
    guard: MutexGuard<'a, Connection>,
}

impl std::ops::Deref for ReadConn<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        &self.guard
    }
}

impl ReadPool {
    /// Mở pool với `size` read-only connection vào `path`. Cần `init_db_at`
    /// chạy trước (file tồn tại + WAL mode đã set bởi write connection).
    pub fn new(path: &Path, size: usize) -> Result<Self> {
        assert!(size > 0, "ReadPool size must be > 0");
        let mut conns = Vec::with_capacity(size);
        for i in 0..size {
            let conn = Connection::open_with_flags(
                path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .with_context(|| {
                format!("không mở read conn #{i} tại {}", path.display())
            })?;
            // query_only: defense-in-depth chặn UPDATE/DELETE vô tình.
            // Cache + mmap pragma mirror write connection để page cache đủ
            // lớn cho hot rows. journal_mode là file-level (đã WAL từ writer)
            // nên không cần set lại ở read conn.
            conn.execute_batch(
                "PRAGMA query_only = ON;
                 PRAGMA temp_store = MEMORY;
                 PRAGMA cache_size = -32768;
                 PRAGMA mmap_size = 268435456;",
            )
            .with_context(|| format!("không apply pragma cho read conn #{i}"))?;
            conns.push(Mutex::new(conn));
        }
        Ok(Self {
            conns,
            next: AtomicUsize::new(0),
            db_path: RwLock::new(path.to_path_buf()),
        })
    }

    /// Hot-swap pool sang DB file mới (workspace switch). Lock từng slot tuần
    /// tự để chờ reader hiện tại drop guard, mở conn mới trên path mới, thay
    /// inline. Reader đang giữ guard cũ vẫn tiếp tục dùng conn cũ — sau khi
    /// drop, lần acquire kế tiếp ở slot đó sẽ nhận conn mới. Frontend reload
    /// UI sau switch để mọi query mới chạy với pool đã swap xong.
    pub fn swap_to(&self, new_path: &Path) -> Result<()> {
        for (i, slot) in self.conns.iter().enumerate() {
            let new_conn = Connection::open_with_flags(
                new_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .with_context(|| {
                format!("không mở read conn #{i} tại {}", new_path.display())
            })?;
            new_conn
                .execute_batch(
                    "PRAGMA query_only = ON;
                     PRAGMA temp_store = MEMORY;
                     PRAGMA cache_size = -32768;
                     PRAGMA mmap_size = 268435456;",
                )
                .with_context(|| format!("không apply pragma cho read conn #{i}"))?;

            let mut guard = slot.lock().unwrap_or_else(|p| p.into_inner());
            // Replace với dummy in-memory trước để drop conn cũ giải phóng
            // file handle / mmap section trên DB workspace cũ, rồi mới gán
            // conn mới. Pattern này mirror `import_db` Windows mmap quirk.
            let dummy = Connection::open_in_memory()
                .context("không tạo được dummy conn khi swap pool")?;
            let old = std::mem::replace(&mut *guard, dummy);
            drop(old);
            *guard = new_conn;
        }
        let mut p = self.db_path.write().unwrap_or_else(|e| e.into_inner());
        *p = new_path.to_path_buf();
        Ok(())
    }

    /// Acquire connection: round-robin `try_lock` từ idx `next` → block trên
    /// idx start nếu tất cả busy. Round-robin spread giúp burst N query song
    /// song lấy N connection khác nhau, không pile lên 1.
    pub fn acquire(&self) -> ReadConn<'_> {
        let n = self.conns.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
        for i in 0..n {
            let idx = (start + i) % n;
            if let Ok(guard) = self.conns[idx].try_lock() {
                return ReadConn { guard };
            }
        }
        // Tất cả busy → block trên start. Recover poisoned mutex (1 reader
        // panic không nên crash toàn app).
        ReadConn {
            guard: self.conns[start]
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        }
    }

    /// Path DB pool đang điểm tới (clone vì path nằm sau RwLock để hỗ trợ
    /// workspace switch). Hiếm hit nên cost negligible.
    pub fn db_path(&self) -> PathBuf {
        self.db_path
            .read()
            .map(|p| p.clone())
            .unwrap_or_else(|e| e.into_inner().clone())
    }

    /// Số slot trong pool. Test/debug helper.
    pub fn size(&self) -> usize {
        self.conns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Tạo DB tại path với 1 bảng `marker(label)` chứa value đặt biệt.
    /// Dùng để verify pool đang đọc đúng workspace nào.
    fn make_marker_db(path: &Path, label: &str) {
        let conn = crate::db::init_db_at(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS marker (label TEXT PRIMARY KEY);",
        )
        .unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO marker(label) VALUES (?1)",
            rusqlite::params![label],
        )
        .unwrap();
    }

    fn read_marker(pool: &ReadPool) -> String {
        let conn = pool.acquire();
        conn.query_row("SELECT label FROM marker LIMIT 1", [], |r| {
            r.get::<_, String>(0)
        })
        .unwrap()
    }

    #[test]
    fn swap_to_changes_active_db_for_subsequent_reads() {
        let tmp = TempDir::new().unwrap();
        let ws_a = tmp.path().join("ws-a.db");
        let ws_b = tmp.path().join("ws-b.db");
        make_marker_db(&ws_a, "workspace-a");
        make_marker_db(&ws_b, "workspace-b");

        let pool = ReadPool::new(&ws_a, 4).unwrap();
        assert_eq!(read_marker(&pool), "workspace-a");

        pool.swap_to(&ws_b).unwrap();
        // Tất cả slot phải point tới ws-b — burst 8 acquire để cover N slot.
        for _ in 0..8 {
            assert_eq!(read_marker(&pool), "workspace-b");
        }
        assert_eq!(pool.db_path(), ws_b);
    }

    #[test]
    fn swap_to_then_back_keeps_data_intact() {
        let tmp = TempDir::new().unwrap();
        let ws_a = tmp.path().join("ws-a.db");
        let ws_b = tmp.path().join("ws-b.db");
        make_marker_db(&ws_a, "workspace-a");
        make_marker_db(&ws_b, "workspace-b");

        let pool = ReadPool::new(&ws_a, 2).unwrap();
        pool.swap_to(&ws_b).unwrap();
        pool.swap_to(&ws_a).unwrap();
        assert_eq!(read_marker(&pool), "workspace-a");
    }

    #[test]
    fn swap_to_invalid_path_returns_err_without_panic() {
        let tmp = TempDir::new().unwrap();
        let ws_a = tmp.path().join("ws-a.db");
        make_marker_db(&ws_a, "workspace-a");

        let pool = ReadPool::new(&ws_a, 2).unwrap();
        // Một đường dẫn chắc chắn không thể mở read-only được trên Windows
        // (folder root mà ko có file). open_with_flags trỏ tới folder fail.
        let bad = tmp.path().join("nonexistent").join("missing.db");
        let result = pool.swap_to(&bad);
        // SQLite tự tạo file rỗng khi open RW, nhưng READ_ONLY phải fail
        // khi file không tồn tại.
        assert!(result.is_err(), "swap sang path không tồn tại phải fail");
        // Pool gốc vẫn phải hoạt động — KHÔNG bị partial swap corrupt.
        assert_eq!(read_marker(&pool), "workspace-a");
    }
}
