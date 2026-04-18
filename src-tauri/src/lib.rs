mod commands;
mod db;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .setup(|app| {
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
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
