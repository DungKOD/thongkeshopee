mod commands;
mod db;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            // Nếu có file DB pending từ lần download R2 trước → apply trước khi mở DB.
            // Nếu không có pending → noop. Lỗi rename KHÔNG block app start (log + tiếp tục).
            if let Err(e) = commands::sync::apply_pending_sync(app.handle()) {
                eprintln!("apply_pending_sync warning: {e}");
            }
            // Init SQLite DB + manage state.
            db::setup(app.handle())?;
            // Admin view state — track user đang được admin xem (None = normal mode).
            app.manage(commands::admin_view::AdminViewState(
                std::sync::Mutex::new(None),
            ));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::accounts::list_shopee_accounts,
            commands::accounts::create_shopee_account,
            commands::accounts::rename_shopee_account,
            commands::accounts::update_shopee_account_color,
            commands::accounts::delete_shopee_account,
            commands::accounts::reassign_shopee_account_data,
            commands::query::db_ping,
            commands::query::list_days,
            commands::query::list_days_with_rows,
            commands::query::load_overview,
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
            commands::video::admin_fetch_user_log_sheet,
            commands::video::admin_read_user_log_cache,
            commands::video::admin_user_log_fetch_meta,
            commands::video::admin_delete_user_log_row,
            commands::video::admin_delete_user_log_sheet,
            commands::video::admin_read_user_list_cache,
            commands::video::admin_fetch_user_list,
            commands::sync::sync_metadata,
            commands::sync::sync_upload_db,
            commands::sync::sync_download_db,
            commands::sync::sync_apply_pending,
            commands::sync::sync_pull_merge_push,
            commands::sync::admin_list_users,
            commands::sync::restart_app,
            commands::sync::machine_fingerprint,
            commands::sync::sync_state_get,
            commands::sync::sync_state_record_error,
            commands::sync::sync_reset_for_new_user,
            commands::admin_view::admin_view_user_db,
            commands::admin_view::admin_exit_view_user_db,
            commands::admin_view::admin_view_state_get,
            commands::screenshot::save_png,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
