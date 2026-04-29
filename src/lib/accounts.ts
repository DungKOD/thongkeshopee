import { invoke } from "./tauri";

/// 1 account Shopee affiliate. Row count = total rows Shopee clicks + orders
/// + manual_entries đang FK về account này.
///
/// **`id` là string** (không phải number) vì content_id hash sau v13 có thể
/// > 2^53 (Number.MAX_SAFE_INTEGER), JS number bị round → DELETE/rename sai
/// row. Rust serialize/deserialize as string ở Tauri boundary.
export interface ShopeeAccount {
  id: string;
  name: string;
  color: string | null;
  createdAt: string;
  rowCount: number;
}

export function listShopeeAccounts(): Promise<ShopeeAccount[]> {
  return invoke<ShopeeAccount[]>("list_shopee_accounts");
}

export function createShopeeAccount(
  name: string,
  color?: string | null,
): Promise<string> {
  return invoke<string>("create_shopee_account", { name, color: color ?? null });
}

/// Trả `true` nếu tên thực sự khác → caller báo cho sync layer. `false` =
/// trùng tên hiện có (no-op), caller skip mutation event.
export function renameShopeeAccount(
  id: string,
  newName: string,
): Promise<boolean> {
  return invoke<boolean>("rename_shopee_account", { id, newName });
}

/// Trả `true` nếu color thực sự khác (xem `renameShopeeAccount`).
export function updateShopeeAccountColor(
  id: string,
  color: string | null,
): Promise<boolean> {
  return invoke<boolean>("update_shopee_account_color", { id, color });
}

export function deleteShopeeAccount(
  id: string,
  alsoDeleteFb: boolean,
): Promise<void> {
  return invoke<void>("delete_shopee_account", { id, alsoDeleteFb });
}

/// Đếm FB ads sẽ bị "cuốn theo" khi xóa account — khớp sub_id prefix-
/// compatible với Shopee data của account, VÀ không dùng chung với account
/// khác (safeguard). Dùng cho preview dialog.
export function countFbLinkedToAccount(id: string): Promise<number> {
  return invoke<number>("count_fb_linked_to_account", { id });
}

/// Chuyển toàn bộ data từ account `fromId` sang `toId`. Trả về số row đã chuyển.
export function reassignShopeeAccountData(
  fromId: string,
  toId: string,
): Promise<number> {
  return invoke<number>("reassign_shopee_account_data", { fromId, toId });
}
