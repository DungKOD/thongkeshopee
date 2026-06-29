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
            // FB Reels: register lock state + reset rows kẹt từ session trước +
            // spawn background poll cho scheduled/permalink. Lỗi recovery /
            // background không fatal — eprintln warn để debug.
            app.manage(std::sync::Arc::new(
                commands::fb_reels::UploadLocks::new(),
            ));
            {
                let state = app.state::<db::FbReelsDbState>();
                match commands::fb_reels::run_startup_recovery(&state) {
                    Ok(n) if n > 0 => {
                        eprintln!("[fb_reels] startup recovery: {n} stuck rows → failed");
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("[fb_reels] startup recovery error: {e}"),
                }
            }
            commands::fb_reels::spawn_background_maintenance(app.handle().clone());
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
            commands::query::days::list_days_with_rows,
            commands::query::overview::load_overview,
            commands::query::files::list_imported_files,
            commands::query::insights::load_hourly_orders,
            commands::query::insights::load_hourly_clicks,
            commands::query::insights::load_referrer_efficiency,
            commands::query::insights::load_click_order_delays,
            commands::query::insights::load_cancellation_by_subid,
            commands::query::insights::list_click_referrers,
            commands::query::items::get_order_items_for_row,
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
            commands::shopee_product::fetch_shopee_product,
            commands::shopee_affiliate::shopee_aff_open_login_window,
            commands::shopee_affiliate::shopee_aff_capture_cookies,
            commands::shopee_affiliate::shopee_aff_get_status,
            commands::shopee_affiliate::shopee_aff_clear_cookies,
            commands::shopee_affiliate::shopee_aff_close_login_window,
            commands::shopee_affiliate::shopee_aff_convert_links,
            commands::video::get_video_info,
            commands::video::download_video,
            commands::video::log_video_download,
            commands::video::list_video_downloads,
            commands::video_watermark::apply_video_watermark,
            commands::video_watermark::clear_page_logo_cache,
            commands::app_util::restart_app,
            commands::app_util::get_app_data_paths,
            commands::app_util::clear_app_data,
            commands::db_transfer::export_db,
            commands::db_transfer::import_db,
            commands::screenshot::save_png,
            commands::device::get_device_id,
            commands::fb_reels::fb_validate_token,
            commands::fb_reels::fb_save_pages,
            commands::fb_reels::fb_list_pages,
            commands::fb_reels::fb_get_page_token,
            commands::fb_reels::fb_delete_page,
            commands::fb_reels::fb_save_auth_token,
            commands::fb_reels::fb_list_auth_tokens,
            commands::fb_reels::fb_get_auth_token,
            commands::fb_reels::fb_update_auth_token_label,
            commands::fb_reels::fb_delete_auth_token,
            commands::fb_reels::fb_enqueue_reel,
            commands::fb_reels::fb_upload_reel,
            commands::fb_reels::fb_list_posts,
            commands::fb_reels::fb_delete_post,
            commands::fb_reels::fb_refetch_post_status,
            commands::fb_reels::fb_debug_video_info,
            commands::fb_ads::fb_ads_validate_token,
            commands::fb_ads::fb_ads_save_accounts,
            commands::fb_ads::fb_ads_list_accounts,
            commands::fb_ads::fb_ads_delete_account,
            commands::fb_ads::fb_ads_get_account_token,
            commands::fb_ads::fb_ads_save_auth_token,
            commands::fb_ads::fb_ads_list_auth_tokens,
            commands::fb_ads::fb_ads_get_auth_token,
            commands::fb_ads::fb_ads_update_auth_token_label,
            commands::fb_ads::fb_ads_delete_auth_token,
            commands::fb_ads::fb_ads_list_fb_campaigns,
            commands::fb_ads::fb_ads_save_template,
            commands::fb_ads::fb_ads_list_templates,
            commands::fb_ads::fb_ads_get_template_detail,
            commands::fb_ads::fb_ads_delete_template,
            commands::fb_ads::fb_ads_save_draft,
            commands::fb_ads::fb_ads_list_drafts,
            commands::fb_ads::fb_ads_get_draft,
            commands::fb_ads::fb_ads_delete_draft,
            commands::fb_ads::fb_ads_create_batch,
            commands::fb_ads::fb_ads_retry_job,
            commands::fb_ads::fb_ads_list_batches,
            commands::fb_ads::fb_ads_list_jobs,
            commands::workspace::list_workspaces,
            commands::workspace::get_active_workspace,
            commands::workspace::create_workspace,
            commands::workspace::rename_workspace,
            commands::workspace::update_workspace_color,
            commands::workspace::switch_workspace,
            commands::workspace::delete_workspace,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
