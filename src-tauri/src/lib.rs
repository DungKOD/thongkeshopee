mod commands;
mod db;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            // Nếu có file DB pending từ lần download Drive trước → apply trước khi mở DB.
            // Nếu không có pending → noop. Lỗi rename KHÔNG block app start (log + tiếp tục).
            if let Err(e) = commands::drive::apply_pending_sync(app.handle()) {
                eprintln!("apply_pending_sync warning: {e}");
            }
            // Init SQLite DB + manage state.
            db::setup(app.handle())?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::query::db_ping,
            commands::query::list_days,
            commands::query::list_days_with_rows,
            commands::query::list_imported_files,
            commands::query::list_click_referrers,
            commands::query::get_order_items_for_row,
            commands::imports::import_shopee_clicks,
            commands::imports::import_shopee_orders,
            commands::imports::import_fb_ad_groups,
            commands::imports::import_fb_campaigns,
            commands::preview::preview_import_shopee_clicks,
            commands::preview::preview_import_shopee_orders,
            commands::preview::preview_import_fb_ad_groups,
            commands::preview::preview_import_fb_campaigns,
            commands::manual::save_manual_entry,
            commands::manual::delete_manual_entry,
            commands::manual::has_manual_entry,
            commands::batch::batch_commit_deletes,
            commands::video::get_video_info,
            commands::video::download_video,
            commands::video::log_video_download,
            commands::video::list_video_downloads,
            commands::video::list_video_downloads_from_path,
            commands::drive::drive_check_or_create,
            commands::drive::drive_metadata,
            commands::drive::drive_upload_db,
            commands::drive::drive_download_db,
            commands::drive::drive_apply_pending,
            commands::drive::drive_pull_merge_push,
            commands::drive::drive_list_users,
            commands::drive::restart_app,
            commands::drive::machine_fingerprint,
            commands::drive::sync_state_get,
            commands::drive::sync_state_record_error,
            commands::drive::admin_download_user_db,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
