import { invoke } from "./tauri";

/// 1 account Shopee affiliate. Row count = total rows Shopee clicks + orders
/// + manual_entries đang FK về account này.
export interface ShopeeAccount {
  id: number;
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
): Promise<number> {
  return invoke<number>("create_shopee_account", { name, color: color ?? null });
}

export function renameShopeeAccount(id: number, newName: string): Promise<void> {
  return invoke<void>("rename_shopee_account", { id, newName });
}

export function updateShopeeAccountColor(
  id: number,
  color: string | null,
): Promise<void> {
  return invoke<void>("update_shopee_account_color", { id, color });
}

export function deleteShopeeAccount(id: number): Promise<void> {
  return invoke<void>("delete_shopee_account", { id });
}

/// Chuyển toàn bộ data từ account `fromId` sang `toId`. Trả về số row đã chuyển.
export function reassignShopeeAccountData(
  fromId: number,
  toId: number,
): Promise<number> {
  return invoke<number>("reassign_shopee_account_data", { fromId, toId });
}
