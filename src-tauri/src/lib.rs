mod commands;
mod db;
mod sync_v9;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let mut builder = tauri::Builder::default();

    // Single-instance: user click icon nhiều lần → focus window cũ thay vì
    // spawn process mới. Desktop-only (plugin không build trên mobile).
    #[cfg(desktop)]
    {
        builder = builder
            .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
                if let Some(win) = app.get_webview_window("main") {
                    let _ = win.unminimize();
                    let _ = win.show();
                    let _ = win.set_focus();
                }
            }))
            .plugin(tauri_plugin_updater::Builder::new().build())
            .plugin(tauri_plugin_process::init());
    }

    builder
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
            // Nếu có file DB pending từ lần download R2 trước → apply trước khi mở DB.
            // v7+ multi-tenant: pending DB nằm trong user folder (không biết UID
            // ở setup, chưa auth). Apply pending được dời vào `switch_db_to_user`
            // — chạy sau khi FE auth ready và gọi command. Không làm gì ở đây.
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
            commands::accounts::count_fb_linked_to_account,
            commands::accounts::reassign_shopee_account_data,
            commands::query::db_ping,
            commands::query::list_days,
            commands::query::list_days_with_rows,
            commands::query::load_overview,
            commands::query::list_imported_files,
            commands::query::load_hourly_orders,
            commands::query::load_hourly_clicks,
            commands::query::load_referrer_efficiency,
            commands::query::load_click_order_delays,
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
            commands::batch::revert_import,
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
            // App utilities (P8b: relocated từ v8 commands::sync).
            commands::app_util::restart_app,
            commands::app_util::machine_fingerprint,
            commands::app_util::switch_db_to_user,
            commands::app_util::admin_list_users,
            commands::app_util::get_app_data_paths,
            commands::app_util::app_log_request,
            commands::app_util::get_net_log_dir,
            commands::admin_view::admin_view_user_db,
            commands::admin_view::admin_exit_view_user_db,
            commands::admin_view::admin_view_state_get,
            commands::screenshot::save_png,
            // Sync v9 commands — per-table incremental delta sync.
            commands::sync_v9_cmds::sync_v9_get_state,
            commands::sync_v9_cmds::sync_v9_push_all,
            commands::sync_v9_cmds::sync_v9_pull_all,
            commands::sync_v9_cmds::sync_v9_sync_all,
            commands::sync_v9_cmds::sync_v9_log_flush,
            commands::sync_v9_cmds::sync_v9_log_list_local,
            commands::sync_v9_cmds::sync_v9_compact_if_needed,
            commands::sync_v9_cmds::admin_v9_sync_log_list,
            commands::sync_v9_cmds::admin_v9_sync_log_fetch_events,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
