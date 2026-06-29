//! Tauri commands quản lý nhiều workspace (DB profile cho từng người dùng).
//!
//! Workspace switch dùng hot-swap in-process: command lock 4 DB state + read
//! pool, đóng connection workspace cũ, mở connection workspace mới, swap
//! atomic dưới mutex. Frontend gọi `window.location.reload()` sau khi command
//! trả OK để React re-mount với data từ workspace mới.
//!
//! KHÔNG dùng `app.restart()` vì tauri-plugin-single-instance gây race
//! (process mới bị detect là "second instance" → tự exit), đặc biệt rõ
//! trong dev mode khi webview class chưa cleanup → window trắng. Pattern
//! hot-swap mirror `import_db::import_db`.

use serde::Serialize;
use tauri::{AppHandle, State};

use crate::db::workspace as ws;
use crate::db::{
    app_data_root, fb_ads_db::FbAdsDbState, fb_reels_db::FbReelsDbState,
    video_db::VideoDbState, DbState, ReadPool,
};

use super::{CmdError, CmdResult};

/// Snapshot 1 workspace để serialize sang frontend.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceDto {
    pub id: String,
    pub name: String,
    pub color: String,
    pub created_at: String,
    pub last_opened_at: Option<String>,
    pub is_active: bool,
}

impl WorkspaceDto {
    fn from(workspace: &ws::Workspace, active_id: &str) -> Self {
        Self {
            id: workspace.id.clone(),
            name: workspace.name.clone(),
            color: workspace.color.clone(),
            created_at: workspace.created_at.clone(),
            last_opened_at: workspace.last_opened_at.clone(),
            is_active: workspace.id == active_id,
        }
    }
}

/// List toàn bộ workspace + flag `is_active` cho workspace đang dùng.
#[tauri::command]
pub fn list_workspaces(app: AppHandle) -> CmdResult<Vec<WorkspaceDto>> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;
    let reg = ws::load_registry(&root)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .ok_or_else(|| CmdError::msg("registry chưa init"))?;
    let active = reg.active_id.clone();
    Ok(reg
        .workspaces
        .iter()
        .map(|w| WorkspaceDto::from(w, &active))
        .collect())
}

/// Trả workspace đang active. Tiện cho UI khi chỉ cần thông tin workspace
/// hiện tại (vd badge top bar) mà không cần load full list.
#[tauri::command]
pub fn get_active_workspace(app: AppHandle) -> CmdResult<WorkspaceDto> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;
    let reg = ws::load_registry(&root)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .ok_or_else(|| CmdError::msg("registry chưa init"))?;
    let active = reg.active().clone();
    Ok(WorkspaceDto::from(&active, &reg.active_id))
}

/// Tạo workspace mới (folder + entry registry). KHÔNG switch sang workspace
/// mới — user chốt switch riêng để chủ động về thời điểm restart.
///
/// `async fn`: pre-init 4 DB file qua `init_db_at` (chạy schema + ANALYZE) có
/// thể tốn ~500ms-2s → tránh block main thread (lý do giống `switch_workspace`).
#[tauri::command]
pub async fn create_workspace(
    app: AppHandle,
    name: String,
    color: String,
) -> CmdResult<WorkspaceDto> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;
    let created = ws::create_workspace(&root, &name, &color)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    // Pre-init 4 DB file ngay để các query metadata (file size, list_workspaces)
    // hoạt động trước khi switch. Open + drop connection xong file vẫn còn.
    let dir = ws::workspace_dir(&root, &created.id);
    let main_path = crate::db::resolve_db_path_in(&dir);
    crate::db::init_db_at(&main_path).map_err(|e| CmdError::msg(e.to_string()))?;
    let video_path = crate::db::video_db::resolve_video_db_path_in(&dir);
    crate::db::video_db::init_video_db_at(&video_path)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let reels_path = crate::db::fb_reels_db::resolve_fb_reels_db_path_in(&dir);
    crate::db::fb_reels_db::init_fb_reels_db_at(&reels_path)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    let ads_path = crate::db::fb_ads_db::resolve_fb_ads_db_path_in(&dir);
    crate::db::fb_ads_db::init_fb_ads_db_at(&ads_path)
        .map_err(|e| CmdError::msg(e.to_string()))?;

    Ok(WorkspaceDto::from(&created, ""))
}

/// Đổi tên workspace. ID (folder name) không đổi để giữ DB path stable.
#[tauri::command]
pub fn rename_workspace(app: AppHandle, id: String, name: String) -> CmdResult<()> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;
    ws::rename_workspace(&root, &id, &name).map_err(|e| CmdError::msg(e.to_string()))?;
    Ok(())
}

/// Đổi màu badge của workspace.
#[tauri::command]
pub fn update_workspace_color(
    app: AppHandle,
    id: String,
    color: String,
) -> CmdResult<()> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;
    ws::update_workspace_color(&root, &id, &color)
        .map_err(|e| CmdError::msg(e.to_string()))?;
    Ok(())
}

/// Hot-swap toàn bộ DB connection sang workspace mới. Quy trình atomic:
/// 1. Validate id + mkdir workspace folder.
/// 2. **Pre-open** 4 connection mới (cùng init nếu file chưa tồn tại, hoặc
///    `open_existing_db` nếu file đã có). Pre-open trước khi giữ mutex để mọi
///    fail thoát SỚM, KHÔNG để state nửa nạc nửa mỡ.
/// 3. Lock + swap inline 4 DbState + read pool (đã cố sẵn nguyên liệu, bước
///    này nhanh và không có khả năng fail giữa chừng).
/// 4. **SAU CÙNG** mới ghi `active_id` xuống registry. Nếu bất kỳ bước trên
///    fail, registry vẫn trỏ workspace cũ → restart app load đúng workspace
///    cũ (state Tauri + disk khớp nhau).
///
/// Tốc độ: dùng `open_existing_db` cho workspace đã có data (KHÔNG re-apply
/// schema, KHÔNG ANALYZE) → switch sang DB lớn (>500MB) từ vài chục giây
/// xuống <1s.
///
/// **`async fn`** (không phải `pub fn`): sync command trong Tauri 2 chạy trên
/// main event-loop thread → I/O nặng (open Connection × 12 + swap pool slot)
/// block main thread → Windows hiển thị "(Not Responding)", UI overlay đứng
/// yên. Async fn chạy trên tokio runtime worker → main thread tự do pump
/// message → animation overlay mượt. Không có `.await` bên trong nên không
/// có vấn đề giữ Mutex qua await point.
///
/// Frontend gọi `window.location.reload()` sau khi nhận Ok → React re-mount,
/// data từ workspace mới tự load qua query commands hiện có.
#[tauri::command]
pub async fn switch_workspace(
    app: AppHandle,
    id: String,
    db: State<'_, DbState>,
    video: State<'_, VideoDbState>,
    reels: State<'_, FbReelsDbState>,
    ads: State<'_, FbAdsDbState>,
    pool: State<'_, ReadPool>,
) -> CmdResult<()> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;

    // Validate workspace tồn tại trong registry trước khi đụng gì.
    let reg = ws::load_registry(&root)
        .map_err(|e| CmdError::msg(e.to_string()))?
        .ok_or_else(|| CmdError::msg("registry chưa init"))?;
    if reg.find(&id).is_none() {
        return Err(CmdError::msg(format!("không tìm thấy workspace id={id}")));
    }

    let new_dir = ws::workspace_dir(&root, &id);
    std::fs::create_dir_all(&new_dir).map_err(|e| CmdError::msg(e.to_string()))?;

    // ===== Pre-open conn — fail-fast, KHÔNG holding mutex nào =====
    // `open_existing_db` nếu file tồn tại (fast path: chỉ PRAGMA); fallback
    // `init_db_at` nếu file chưa có (vd workspace cũ chưa từng switch tới sau
    // khi feature multi-workspace ra đời, hoặc disk corrupt).
    let main_path = crate::db::resolve_db_path_in(&new_dir);
    let new_main = if main_path.exists() {
        crate::db::open_existing_db(&main_path)
    } else {
        crate::db::init_db_at(&main_path)
    }
    .map_err(|e| CmdError::msg(format!("mở main DB thất bại: {e}")))?;

    let video_path = crate::db::video_db::resolve_video_db_path_in(&new_dir);
    let new_video = if video_path.exists() {
        crate::db::video_db::open_existing_video_db(&video_path)
    } else {
        crate::db::video_db::init_video_db_at(&video_path)
    }
    .map_err(|e| CmdError::msg(format!("mở video DB thất bại: {e}")))?;

    let reels_path = crate::db::fb_reels_db::resolve_fb_reels_db_path_in(&new_dir);
    let new_reels = if reels_path.exists() {
        crate::db::fb_reels_db::open_existing_fb_reels_db(&reels_path)
    } else {
        crate::db::fb_reels_db::init_fb_reels_db_at(&reels_path)
    }
    .map_err(|e| CmdError::msg(format!("mở FB Reels DB thất bại: {e}")))?;

    let ads_path = crate::db::fb_ads_db::resolve_fb_ads_db_path_in(&new_dir);
    let new_ads = if ads_path.exists() {
        crate::db::fb_ads_db::open_existing_fb_ads_db(&ads_path)
    } else {
        crate::db::fb_ads_db::init_fb_ads_db_at(&ads_path)
    }
    .map_err(|e| CmdError::msg(format!("mở FB Ads DB thất bại: {e}")))?;

    // ===== Swap inline — đã có conn mới sẵn, bước này nhanh + không fail =====
    // Pattern mirror `import_db`: replace với dummy in-memory để drop conn cũ
    // giải phóng file handle / mmap section trên DB workspace cũ trước khi
    // gán conn mới (Windows quirk).
    {
        let mut guard = db.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let dummy = rusqlite::Connection::open_in_memory()
            .map_err(|e| CmdError::msg(format!("dummy conn fail: {e}")))?;
        let old = std::mem::replace(&mut *guard, dummy);
        drop(old);
        *guard = new_main;
    }
    {
        let mut guard = video.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let dummy = rusqlite::Connection::open_in_memory()
            .map_err(|e| CmdError::msg(format!("dummy conn fail: {e}")))?;
        let old = std::mem::replace(&mut *guard, dummy);
        drop(old);
        *guard = new_video;
    }
    {
        let mut guard = reels.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let dummy = rusqlite::Connection::open_in_memory()
            .map_err(|e| CmdError::msg(format!("dummy conn fail: {e}")))?;
        let old = std::mem::replace(&mut *guard, dummy);
        drop(old);
        *guard = new_reels;
    }
    {
        let mut guard = ads.0.lock().map_err(|_| CmdError::LockPoisoned)?;
        let dummy = rusqlite::Connection::open_in_memory()
            .map_err(|e| CmdError::msg(format!("dummy conn fail: {e}")))?;
        let old = std::mem::replace(&mut *guard, dummy);
        drop(old);
        *guard = new_ads;
    }

    // Read pool swap N slot sang main DB workspace mới.
    pool.swap_to(&main_path)
        .map_err(|e| CmdError::msg(e.to_string()))?;

    // Commit registry SAU CÙNG — nếu bước nào trên fail, registry vẫn trỏ
    // workspace cũ → restart app sẽ load đúng workspace cũ, khớp với state
    // đã thực sự được giữ trong Tauri (vì pre-open fail thì đã return Err).
    ws::set_active_workspace(&root, &id).map_err(|e| CmdError::msg(e.to_string()))?;

    Ok(())
}

/// Xóa workspace + folder của nó. Cấm xóa active hoặc workspace cuối cùng
/// (logic enforce ở `ws::delete_workspace`).
///
/// `async fn`: `remove_dir_all` trên workspace có hàng GB data có thể tốn vài
/// giây → tránh block main thread.
#[tauri::command]
pub async fn delete_workspace(
    app: AppHandle,
    _db: State<'_, DbState>,
    id: String,
) -> CmdResult<()> {
    let root = app_data_root(&app).map_err(|e| CmdError::msg(e.to_string()))?;
    ws::delete_workspace(&root, &id).map_err(|e| CmdError::msg(e.to_string()))?;
    Ok(())
}
