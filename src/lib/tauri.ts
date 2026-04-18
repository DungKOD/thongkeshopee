import { invoke as tauriInvoke } from "@tauri-apps/api/core";

/**
 * Wrapper cho Tauri invoke. Dùng chỗ này để có thể thêm logging,
 * error handling tập trung hoặc mock cho test sau này.
 */
export function invoke<T = unknown>(
  cmd: string,
  args?: Record<string, unknown>,
): Promise<T> {
  return tauriInvoke<T>(cmd, args);
}
