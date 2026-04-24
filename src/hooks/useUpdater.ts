import { useCallback, useState } from "react";
import {
  checkForUpdate,
  downloadAndInstall,
  type DownloadEvent,
  type UpdateInfo,
} from "../lib/updater";

export type UpdaterState =
  | { phase: "idle" }
  | { phase: "checking" }
  | { phase: "up-to-date" }
  | { phase: "available"; info: UpdateInfo }
  | { phase: "downloading"; info: UpdateInfo; downloaded: number; total: number }
  | { phase: "installing"; info: UpdateInfo }
  | { phase: "error"; message: string };

export interface UseUpdaterResult {
  state: UpdaterState;
  check: () => Promise<void>;
  install: () => Promise<void>;
  reset: () => void;
}

export function useUpdater(): UseUpdaterResult {
  const [state, setState] = useState<UpdaterState>({ phase: "idle" });

  const check = useCallback(async () => {
    setState({ phase: "checking" });
    try {
      const info = await checkForUpdate();
      if (!info) setState({ phase: "up-to-date" });
      else setState({ phase: "available", info });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setState({ phase: "error", message });
    }
  }, []);

  const install = useCallback(async () => {
    setState((prev) => {
      if (prev.phase !== "available") return prev;
      return {
        phase: "downloading",
        info: prev.info,
        downloaded: 0,
        total: 0,
      };
    });
    try {
      // Lấy info từ state mới nhất — capture ngoài setState closure.
      const curr = await new Promise<UpdateInfo | null>((resolve) => {
        setState((s) => {
          if (s.phase === "downloading") resolve(s.info);
          else resolve(null);
          return s;
        });
      });
      if (!curr) return;
      const onEvent = (ev: DownloadEvent) => {
        if (ev.kind === "Progress") {
          setState((prev) => {
            if (prev.phase !== "downloading") return prev;
            return {
              ...prev,
              downloaded: ev.downloaded,
              total: ev.total || prev.total,
            };
          });
        } else if (ev.kind === "Finished") {
          setState((prev) => {
            if (prev.phase !== "downloading") return prev;
            return { phase: "installing", info: prev.info };
          });
        }
      };
      await downloadAndInstall(curr, onEvent);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setState({ phase: "error", message });
    }
  }, []);

  const reset = useCallback(() => setState({ phase: "idle" }), []);

  return { state, check, install, reset };
}
