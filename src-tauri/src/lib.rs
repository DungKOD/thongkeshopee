mod commands;
mod db;

use tauri::Manager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let mut builder = tauri::Builder::default();

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
            db::setup(app.handle())?;
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
            commands::query::load_cancellation_by_subid,
            commands::query::list_click_referrers,
            commands::query::get_order_items_for_row,
            commands::imports::import_shopee_clicks,
            commands::imports::import_shopee_orders,
            commands::imports::import_fb_ad_groups,
            commands::imports::import_fb_campaigns,
            commands::imports::import_fb_hierarchy,
            commands::preview::preview_import_shopee_clicks,
            commands::preview::preview_import_shopee_orders,
            commands::preview::preview_import_fb_ad_groups,
            commands::preview::preview_import_fb_campaigns,
            commands::preview::preview_import_fb_hierarchy,
            commands::manual::save_manual_entry,
            commands::manual::delete_manual_entry,
            commands::manual::has_manual_entry,
            commands::app_settings::get_app_setting,
            commands::app_settings::list_app_settings,
            commands::app_settings::set_app_setting,
            commands::app_settings::set_app_settings_bulk,
            commands::batch::batch_commit_deletes,
            commands::batch::revert_import,
            commands::batch::delete_import_history_entry,
            commands::batch::delete_all_reverted_history,
            commands::video::get_video_info,
            commands::video::download_video,
            commands::video::log_video_download,
            commands::video::list_video_downloads,
            commands::app_util::restart_app,
            commands::app_util::get_app_data_paths,
            commands::app_util::clear_app_data,
            commands::db_transfer::export_db,
            commands::db_transfer::import_db,
            commands::screenshot::save_png,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
