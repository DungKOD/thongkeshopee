// ==========================================================================
// Auto-update wrapper — gọi `@tauri-apps/plugin-updater` + `plugin-process`.
//
// Flow: check() query `latest.json` trên GitHub Release → nếu có version mới
// (so bằng semver) trả về object có `downloadAndInstall()` + `version` + `body`.
// Frontend nên catch error — no-network / offline / signing mismatch đều throw.
// ==========================================================================
import { check, type Update } from "@tauri-apps/plugin-updater";
import { relaunch } from "@tauri-apps/plugin-process";

export interface UpdateInfo {
  version: string;
  currentVersion: string;
  /** Release notes markdown — từ body của Release tag trên GitHub. */
  body: string | null;
  date: string | null;
  raw: Update;
}

export async function checkForUpdate(): Promise<UpdateInfo | null> {
  const u = await check();
  if (!u) return null;
  return {
    version: u.version,
    currentVersion: u.currentVersion,
    body: u.body ?? null,
    date: u.date ?? null,
    raw: u,
  };
}

export type DownloadEvent =
  | { kind: "Started"; contentLength: number }
  | { kind: "Progress"; downloaded: number; total: number }
  | { kind: "Finished" };

/** Tải bundle + cài đè, xong relaunch. Callback onEvent nhận progress để UI
 *  vẽ thanh %. Trên Windows NSIS silent-mode: không hiện installer wizard. */
export async function downloadAndInstall(
  info: UpdateInfo,
  onEvent?: (ev: DownloadEvent) => void,
): Promise<void> {
  let downloaded = 0;
  let total = 0;
  await info.raw.downloadAndInstall((event) => {
    if (event.event === "Started") {
      total = event.data.contentLength ?? 0;
      onEvent?.({ kind: "Started", contentLength: total });
    } else if (event.event === "Progress") {
      downloaded += event.data.chunkLength;
      onEvent?.({ kind: "Progress", downloaded, total });
    } else if (event.event === "Finished") {
      onEvent?.({ kind: "Finished" });
    }
  });
  await relaunch();
}
