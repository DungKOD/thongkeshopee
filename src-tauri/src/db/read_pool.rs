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
use std::sync::{Mutex, MutexGuard};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

/// Số read connection mặc định. 8 cover được initial mount (3 query) +
/// Overview (5 query) chạy đồng thời không queue. Cost: ~256KB cache nội bộ
/// SQLite per-conn (cache_size = -32768 = 32MB nhưng SQLite lazy-grow theo
/// page touched, thực tế thường chỉ vài MB tổng).
pub const DEFAULT_READ_POOL_SIZE: usize = 8;

/// Pool gồm N `Mutex<Connection>` riêng + atomic round-robin counter để
/// spread acquire (tránh pile lên slot 0).
pub struct ReadPool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
    db_path: PathBuf,
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
            db_path: path.to_path_buf(),
        })
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

    /// Path DB pool đang điểm tới. Dùng cho rebuild sau db_transfer (TODO).
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Số slot trong pool. Test/debug helper.
    pub fn size(&self) -> usize {
        self.conns.len()
    }
}
