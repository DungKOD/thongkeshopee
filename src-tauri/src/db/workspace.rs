//! Multi-workspace registry — mỗi workspace là 1 folder con của `app_data_dir`
//! chứa 4 DB file + folder `imports/` riêng biệt.
//!
//! - Registry JSON: `app_data_dir/workspaces.json`
//! - Workspace folder: `app_data_dir/workspaces/<id>/`
//! - Switch = hot-swap 4 DB connection + read pool in-process (xem
//!   `commands::workspace::switch_workspace`), KHÔNG restart app.
//!
//! Migration 1 lần: nếu registry chưa có nhưng `thongkeshopee.db` đang ở root
//! `app_data_dir/` (từ version trước) → move 4 DB + `imports/` vào
//! `workspaces/default/` rồi tạo registry.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Tên file registry ở root `app_data_dir`.
pub const REGISTRY_FILENAME: &str = "workspaces.json";

/// Subfolder gốc chứa các workspace folder.
pub const WORKSPACES_SUBDIR: &str = "workspaces";

/// Slug của workspace mặc định khi auto-migrate hoặc fresh install.
pub const DEFAULT_WORKSPACE_ID: &str = "default";

/// Tên hiển thị mặc định cho workspace `default`.
pub const DEFAULT_WORKSPACE_NAME: &str = "Mặc định";

/// Màu badge mặc định cho workspace `default`.
pub const DEFAULT_WORKSPACE_COLOR: &str = "#888888";

/// 4 DB file cần migrate khi nâng cấp từ layout cũ.
const DB_FILENAMES: &[&str] = &[
    "thongkeshopee.db",
    "video_logs.db",
    "fb_reels.db",
    "fb_ads.db",
];

/// 1 workspace = 1 folder chứa 4 DB + imports/. Người dùng chỉ thấy `name` +
/// `color`; `id` là slug random ổn định (không đổi khi rename) làm folder name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: String,
    pub name: String,
    pub color: String,
    pub created_at: String,
    #[serde(default)]
    pub last_opened_at: Option<String>,
}

/// File JSON lưu danh sách workspace + workspace đang active.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registry {
    pub active_id: String,
    pub workspaces: Vec<Workspace>,
}

impl Registry {
    /// Tìm workspace theo id.
    pub fn find(&self, id: &str) -> Option<&Workspace> {
        self.workspaces.iter().find(|w| w.id == id)
    }

    /// Mutable lookup theo id.
    pub fn find_mut(&mut self, id: &str) -> Option<&mut Workspace> {
        self.workspaces.iter_mut().find(|w| w.id == id)
    }

    /// Workspace đang active. Panic-safe: nếu `active_id` không trỏ tới WS
    /// nào (registry corrupt) thì fallback workspace đầu tiên.
    pub fn active(&self) -> &Workspace {
        self.find(&self.active_id)
            .unwrap_or_else(|| &self.workspaces[0])
    }
}

/// Path file registry trong `app_data_dir`.
pub fn registry_path(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join(REGISTRY_FILENAME)
}

/// Folder root chứa tất cả workspace (`app_data_dir/workspaces/`).
pub fn workspaces_root(app_data_dir: &Path) -> PathBuf {
    app_data_dir.join(WORKSPACES_SUBDIR)
}

/// Folder của 1 workspace cụ thể (`app_data_dir/workspaces/<id>/`).
pub fn workspace_dir(app_data_dir: &Path, id: &str) -> PathBuf {
    workspaces_root(app_data_dir).join(id)
}

/// Sinh workspace id mới — `ws-` + 8 hex char đầu của UUIDv4. Không dùng tên
/// user nhập làm folder name (tránh ký tự xấu trên Windows, va chạm rename).
pub fn new_workspace_id() -> String {
    let uuid = Uuid::new_v4().simple().to_string();
    format!("ws-{}", &uuid[..8])
}

/// RFC3339 với millisecond precision + suffix `Z`. Khớp convention DB.
fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Load registry từ disk. Trả `None` nếu file chưa tồn tại (caller sẽ chạy
/// migrate hoặc fresh-init).
pub fn load_registry(app_data_dir: &Path) -> Result<Option<Registry>> {
    let path = registry_path(app_data_dir);
    if !path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("không đọc được {}", path.display()))?;
    let reg: Registry = serde_json::from_str(&raw)
        .with_context(|| format!("registry JSON hỏng tại {}", path.display()))?;
    if reg.workspaces.is_empty() {
        anyhow::bail!("registry không có workspace nào");
    }
    Ok(Some(reg))
}

/// Atomic write registry: ghi vào file tmp rồi rename → không corrupt nếu
/// app crash giữa chừng.
pub fn save_registry(app_data_dir: &Path, registry: &Registry) -> Result<()> {
    fs::create_dir_all(app_data_dir).with_context(|| {
        format!("không tạo được app_data_dir: {}", app_data_dir.display())
    })?;
    let dest = registry_path(app_data_dir);
    let tmp = dest.with_extension("json.tmp");
    let json = serde_json::to_string_pretty(registry)
        .context("không serialize được registry")?;
    fs::write(&tmp, json)
        .with_context(|| format!("không ghi được {}", tmp.display()))?;
    fs::rename(&tmp, &dest)
        .with_context(|| format!("không rename {} -> {}", tmp.display(), dest.display()))?;
    Ok(())
}

/// Đảm bảo registry tồn tại + workspace active hợp lệ. Auto-migrate nếu cần.
///
/// Hậu điều kiện: file `workspaces.json` tồn tại, ít nhất 1 workspace, folder
/// của workspace active đã được tạo (sẵn sàng cho `init_db_at` chạy lên).
///
/// Trả về workspace active sau khi xử lý.
pub fn ensure_initialized(app_data_dir: &Path) -> Result<Workspace> {
    if let Some(mut reg) = load_registry(app_data_dir)? {
        // Validate active_id còn tồn tại trong list — fallback workspace đầu
        // tiên nếu registry bị edit tay.
        if reg.find(&reg.active_id).is_none() {
            reg.active_id = reg.workspaces[0].id.clone();
        }
        let active_id = reg.active_id.clone();
        let dir = workspace_dir(app_data_dir, &active_id);
        fs::create_dir_all(&dir).with_context(|| {
            format!("không tạo được workspace dir: {}", dir.display())
        })?;
        // Update last_opened_at — không fatal nếu save fail.
        if let Some(ws) = reg.find_mut(&active_id) {
            ws.last_opened_at = Some(now_iso());
        }
        let _ = save_registry(app_data_dir, &reg);
        return Ok(reg.find(&active_id).cloned().expect("active tồn tại"));
    }

    // Chưa có registry → migrate hoặc fresh init.
    let default_dir = workspace_dir(app_data_dir, DEFAULT_WORKSPACE_ID);
    fs::create_dir_all(&default_dir).with_context(|| {
        format!("không tạo được workspace mặc định: {}", default_dir.display())
    })?;

    migrate_legacy_root(app_data_dir, &default_dir)
        .context("auto-migrate DB cũ từ root vào workspace mặc định thất bại")?;

    let ws = Workspace {
        id: DEFAULT_WORKSPACE_ID.to_string(),
        name: DEFAULT_WORKSPACE_NAME.to_string(),
        color: DEFAULT_WORKSPACE_COLOR.to_string(),
        created_at: now_iso(),
        last_opened_at: Some(now_iso()),
    };
    let reg = Registry {
        active_id: DEFAULT_WORKSPACE_ID.to_string(),
        workspaces: vec![ws.clone()],
    };
    save_registry(app_data_dir, &reg)?;
    Ok(ws)
}

/// Move các DB file + folder `imports/` (nếu tồn tại) từ `app_data_dir` vào
/// `dest_dir`. Best-effort: file đã có ở dest thì giữ nguyên (không overwrite).
///
/// Bao gồm sidecar `*-wal` / `*-shm` để không mất uncommitted transaction
/// nếu app crash trước đó.
fn migrate_legacy_root(app_data_dir: &Path, dest_dir: &Path) -> Result<()> {
    for &name in DB_FILENAMES {
        for suffix in ["", "-wal", "-shm"] {
            let from = app_data_dir.join(format!("{name}{suffix}"));
            if !from.exists() {
                continue;
            }
            let to = dest_dir.join(format!("{name}{suffix}"));
            if to.exists() {
                // Workspace mới đã có sẵn — giữ workspace mới, xóa file cũ ở
                // root (đã thừa). Rare case: user chạy 2 process song song.
                let _ = fs::remove_file(&from);
                continue;
            }
            // rename = move on same filesystem; nếu fail (cross-device hoặc
            // file đang lock) thì fallback copy + delete.
            if fs::rename(&from, &to).is_err() {
                fs::copy(&from, &to)
                    .with_context(|| format!("copy {} -> {}", from.display(), to.display()))?;
                let _ = fs::remove_file(&from);
            }
        }
    }

    let from_imports = app_data_dir.join(crate::db::IMPORTS_SUBDIR);
    let to_imports = dest_dir.join(crate::db::IMPORTS_SUBDIR);
    if from_imports.exists()
        && !to_imports.exists()
        && fs::rename(&from_imports, &to_imports).is_err()
    {
        copy_dir_recursive(&from_imports, &to_imports)?;
        let _ = fs::remove_dir_all(&from_imports);
    }
    Ok(())
}

/// Đệ quy copy folder — fallback cho `fs::rename` khi cross-device (vd user
/// để app_data trên ổ khác imports).
fn copy_dir_recursive(from: &Path, to: &Path) -> Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let path = entry.path();
        let dest = to.join(entry.file_name());
        if path.is_dir() {
            copy_dir_recursive(&path, &dest)?;
        } else {
            fs::copy(&path, &dest)?;
        }
    }
    Ok(())
}

/// Tạo workspace mới: sinh id, mkdir folder, append vào registry. KHÔNG init
/// 4 DB file trong workspace — `init_db_at` sẽ chạy lúc app restart switch
/// sang workspace đó (tránh giữ stale connection).
pub fn create_workspace(
    app_data_dir: &Path,
    name: &str,
    color: &str,
) -> Result<Workspace> {
    let name = name.trim();
    if name.is_empty() {
        anyhow::bail!("tên workspace không được rỗng");
    }
    let mut reg = load_registry(app_data_dir)?
        .context("registry chưa init — gọi ensure_initialized trước")?;

    let id = new_workspace_id();
    let dir = workspace_dir(app_data_dir, &id);
    fs::create_dir_all(&dir)
        .with_context(|| format!("không tạo được workspace dir: {}", dir.display()))?;

    let ws = Workspace {
        id: id.clone(),
        name: name.to_string(),
        color: color.to_string(),
        created_at: now_iso(),
        last_opened_at: None,
    };
    reg.workspaces.push(ws.clone());
    save_registry(app_data_dir, &reg)?;
    Ok(ws)
}

/// Rename workspace. Folder name (id) không đổi để giữ DB path stable.
pub fn rename_workspace(app_data_dir: &Path, id: &str, new_name: &str) -> Result<()> {
    let new_name = new_name.trim();
    if new_name.is_empty() {
        anyhow::bail!("tên mới không được rỗng");
    }
    let mut reg = load_registry(app_data_dir)?
        .context("registry chưa init")?;
    let ws = reg
        .find_mut(id)
        .with_context(|| format!("không tìm thấy workspace id={id}"))?;
    ws.name = new_name.to_string();
    save_registry(app_data_dir, &reg)
}

/// Đổi màu workspace.
pub fn update_workspace_color(app_data_dir: &Path, id: &str, color: &str) -> Result<()> {
    let mut reg = load_registry(app_data_dir)?
        .context("registry chưa init")?;
    let ws = reg
        .find_mut(id)
        .with_context(|| format!("không tìm thấy workspace id={id}"))?;
    ws.color = color.to_string();
    save_registry(app_data_dir, &reg)
}

/// Set `active_id` = id mới trong registry trên disk. Caller (hiện tại là
/// `commands::workspace::switch_workspace`) phải hot-swap DB connection
/// trong Tauri state thì state Tauri + registry mới đồng bộ.
pub fn set_active_workspace(app_data_dir: &Path, id: &str) -> Result<()> {
    let mut reg = load_registry(app_data_dir)?
        .context("registry chưa init")?;
    if reg.find(id).is_none() {
        anyhow::bail!("không tìm thấy workspace id={id}");
    }
    reg.active_id = id.to_string();
    save_registry(app_data_dir, &reg)
}

/// Xóa workspace + folder của nó. Cấm xóa active hoặc workspace cuối cùng.
pub fn delete_workspace(app_data_dir: &Path, id: &str) -> Result<()> {
    let mut reg = load_registry(app_data_dir)?
        .context("registry chưa init")?;
    if reg.active_id == id {
        anyhow::bail!("không thể xóa workspace đang dùng — chuyển sang workspace khác trước");
    }
    if reg.workspaces.len() <= 1 {
        anyhow::bail!("không thể xóa workspace cuối cùng");
    }
    let pos = reg
        .workspaces
        .iter()
        .position(|w| w.id == id)
        .with_context(|| format!("không tìm thấy workspace id={id}"))?;
    reg.workspaces.remove(pos);
    save_registry(app_data_dir, &reg)?;

    let dir = workspace_dir(app_data_dir, id);
    if dir.exists() {
        fs::remove_dir_all(&dir)
            .with_context(|| format!("không xóa được folder {}", dir.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn ensure_initialized_creates_default_registry_for_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let active = ensure_initialized(tmp.path()).unwrap();
        assert_eq!(active.id, DEFAULT_WORKSPACE_ID);
        assert_eq!(active.name, DEFAULT_WORKSPACE_NAME);
        assert!(workspace_dir(tmp.path(), DEFAULT_WORKSPACE_ID).exists());
        assert!(registry_path(tmp.path()).exists());
    }

    #[test]
    fn ensure_initialized_migrates_legacy_db_files() {
        let tmp = TempDir::new().unwrap();
        // Giả lập layout cũ: file DB ở root.
        for name in DB_FILENAMES {
            fs::write(tmp.path().join(name), b"legacy").unwrap();
        }
        fs::create_dir_all(tmp.path().join(crate::db::IMPORTS_SUBDIR)).unwrap();
        fs::write(
            tmp.path().join(crate::db::IMPORTS_SUBDIR).join("a.csv"),
            b"x",
        )
        .unwrap();

        ensure_initialized(tmp.path()).unwrap();

        let default_dir = workspace_dir(tmp.path(), DEFAULT_WORKSPACE_ID);
        for name in DB_FILENAMES {
            assert!(default_dir.join(name).exists(), "{name} phải migrate");
            assert!(!tmp.path().join(name).exists(), "{name} phải biến mất ở root");
        }
        assert!(default_dir
            .join(crate::db::IMPORTS_SUBDIR)
            .join("a.csv")
            .exists());
    }

    #[test]
    fn ensure_initialized_idempotent_after_first_run() {
        let tmp = TempDir::new().unwrap();
        let first = ensure_initialized(tmp.path()).unwrap();
        let second = ensure_initialized(tmp.path()).unwrap();
        assert_eq!(first.id, second.id);
        let reg = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(reg.workspaces.len(), 1);
    }

    #[test]
    fn create_workspace_adds_to_registry_without_switching() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let ws = create_workspace(tmp.path(), "Bạn A", "#ff0000").unwrap();
        assert_ne!(ws.id, DEFAULT_WORKSPACE_ID);
        let reg = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(reg.workspaces.len(), 2);
        assert_eq!(reg.active_id, DEFAULT_WORKSPACE_ID, "create không switch");
        assert!(workspace_dir(tmp.path(), &ws.id).exists());
    }

    #[test]
    fn rename_keeps_id_and_folder_stable() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        rename_workspace(tmp.path(), DEFAULT_WORKSPACE_ID, "Bạn B").unwrap();
        let reg = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(reg.active().name, "Bạn B");
        assert!(workspace_dir(tmp.path(), DEFAULT_WORKSPACE_ID).exists());
    }

    #[test]
    fn delete_active_workspace_fails() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let other = create_workspace(tmp.path(), "Bạn A", "#fff").unwrap();
        let _ = other;
        let err = delete_workspace(tmp.path(), DEFAULT_WORKSPACE_ID).unwrap_err();
        assert!(err.to_string().contains("đang dùng"));
    }

    #[test]
    fn cannot_delete_default_when_active() {
        // Khi chỉ có 1 workspace, nó luôn là active → check "đang dùng" bắn
        // trước check "cuối cùng". Test cover đường active (đường phổ biến);
        // nhánh "cuối cùng" là defense-in-depth không reach được qua API
        // hợp lệ nhưng vẫn giữ để chặn registry tay-edit.
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let err = delete_workspace(tmp.path(), DEFAULT_WORKSPACE_ID).unwrap_err();
        assert!(err.to_string().contains("đang dùng"));
    }

    #[test]
    fn delete_non_active_workspace_removes_folder() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let ws = create_workspace(tmp.path(), "Tạm", "#000").unwrap();
        assert!(workspace_dir(tmp.path(), &ws.id).exists());
        delete_workspace(tmp.path(), &ws.id).unwrap();
        assert!(!workspace_dir(tmp.path(), &ws.id).exists());
        let reg = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(reg.workspaces.len(), 1);
    }

    #[test]
    fn set_active_workspace_validates_id() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        assert!(set_active_workspace(tmp.path(), "ws-doesnotexist").is_err());
        let ws = create_workspace(tmp.path(), "B", "#000").unwrap();
        set_active_workspace(tmp.path(), &ws.id).unwrap();
        let reg = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(reg.active_id, ws.id);
    }

    /// Data isolation contract: 2 workspace = 2 DB folder hoàn toàn riêng biệt.
    /// Insert row vào shopee_accounts của WS A → KHÔNG thấy ở WS B.
    /// Đây là invariant quan trọng nhất của multi-workspace.
    #[test]
    fn workspaces_have_isolated_databases() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let ws_b = create_workspace(tmp.path(), "B", "#000").unwrap();

        // Init main DB cho cả 2 workspace + insert account riêng cho mỗi WS.
        let dir_a = workspace_dir(tmp.path(), DEFAULT_WORKSPACE_ID);
        let dir_b = workspace_dir(tmp.path(), &ws_b.id);

        let path_a = crate::db::resolve_db_path_in(&dir_a);
        let path_b = crate::db::resolve_db_path_in(&dir_b);

        {
            let conn_a = crate::db::init_db_at(&path_a).unwrap();
            conn_a
                .execute(
                    "INSERT INTO shopee_accounts(id, name, color, created_at)
                     VALUES(?, 'AccountA', '#aaa', 'now')",
                    rusqlite::params![999_001_i64],
                )
                .unwrap();
        }
        {
            let conn_b = crate::db::init_db_at(&path_b).unwrap();
            conn_b
                .execute(
                    "INSERT INTO shopee_accounts(id, name, color, created_at)
                     VALUES(?, 'AccountB', '#bbb', 'now')",
                    rusqlite::params![999_002_i64],
                )
                .unwrap();
        }

        // Re-open mỗi DB và verify chỉ thấy data của workspace tương ứng.
        let conn_a = crate::db::open_existing_db(&path_a).unwrap();
        let conn_b = crate::db::open_existing_db(&path_b).unwrap();

        let a_has_a: i64 = conn_a
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE name='AccountA'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let a_has_b: i64 = conn_a
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE name='AccountB'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(a_has_a, 1, "WS A phải thấy AccountA");
        assert_eq!(a_has_b, 0, "WS A KHÔNG được thấy AccountB của WS B");

        let b_has_a: i64 = conn_b
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE name='AccountA'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let b_has_b: i64 = conn_b
            .query_row(
                "SELECT COUNT(*) FROM shopee_accounts WHERE name='AccountB'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(b_has_a, 0, "WS B KHÔNG được thấy AccountA của WS A");
        assert_eq!(b_has_b, 1, "WS B phải thấy AccountB");
    }

    /// `Registry::active()` fallback workspace[0] khi `active_id` không khớp.
    /// Case xảy ra: user edit tay registry hoặc workspace bị xóa ngoài registry.
    #[test]
    fn active_falls_back_to_first_when_active_id_invalid() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let ws_b = create_workspace(tmp.path(), "B", "#000").unwrap();

        // Edit tay registry: set active_id = id không tồn tại.
        let mut reg = load_registry(tmp.path()).unwrap().unwrap();
        reg.active_id = "ws-ghost".to_string();
        save_registry(tmp.path(), &reg).unwrap();

        // load_registry trả raw — Registry::active() phải fallback an toàn.
        let loaded = load_registry(tmp.path()).unwrap().unwrap();
        let active = loaded.active();
        assert_eq!(
            active.id, DEFAULT_WORKSPACE_ID,
            "fallback về workspace[0] (default tạo trước B)"
        );
        // ensure_initialized phải fix active_id.
        ensure_initialized(tmp.path()).unwrap();
        let fixed = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(
            fixed.active_id, DEFAULT_WORKSPACE_ID,
            "ensure_initialized phải reset active_id về workspace[0]"
        );
        let _ = ws_b;
    }

    /// Sau khi delete non-active workspace, registry KHÔNG được mất active_id.
    /// Đặc biệt nếu user delete xong rồi switch → app phải vẫn boot OK.
    #[test]
    fn delete_keeps_active_workspace_valid() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let ws_b = create_workspace(tmp.path(), "B", "#000").unwrap();
        let ws_c = create_workspace(tmp.path(), "C", "#fff").unwrap();

        // Active vẫn là default; xóa B (non-active).
        delete_workspace(tmp.path(), &ws_b.id).unwrap();
        let reg = load_registry(tmp.path()).unwrap().unwrap();
        assert_eq!(reg.active_id, DEFAULT_WORKSPACE_ID);
        assert_eq!(reg.workspaces.len(), 2);
        assert!(reg.find(&ws_c.id).is_some(), "C vẫn còn sau khi xóa B");
    }

    /// Rename workspace KHÔNG được đổi folder name (id) — DB path phải stable.
    #[test]
    fn rename_does_not_move_db_folder() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        let dir_before = workspace_dir(tmp.path(), DEFAULT_WORKSPACE_ID);
        fs::write(dir_before.join("marker.txt"), b"x").unwrap();

        rename_workspace(tmp.path(), DEFAULT_WORKSPACE_ID, "Tên Mới Toanh").unwrap();
        let dir_after = workspace_dir(tmp.path(), DEFAULT_WORKSPACE_ID);
        assert_eq!(dir_before, dir_after);
        assert!(dir_after.join("marker.txt").exists(), "file trong folder vẫn còn");
    }

    /// Tên rỗng / chỉ whitespace bị reject ở tầng business — không bao giờ
    /// để registry có workspace tên "" (UI sẽ hiển thị badge trống).
    #[test]
    fn create_and_rename_reject_blank_name() {
        let tmp = TempDir::new().unwrap();
        ensure_initialized(tmp.path()).unwrap();
        assert!(create_workspace(tmp.path(), "   ", "#000").is_err());
        assert!(create_workspace(tmp.path(), "", "#000").is_err());

        assert!(rename_workspace(tmp.path(), DEFAULT_WORKSPACE_ID, "  ").is_err());
        assert!(rename_workspace(tmp.path(), DEFAULT_WORKSPACE_ID, "").is_err());
    }
}
