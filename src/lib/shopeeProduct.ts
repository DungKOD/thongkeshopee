import { invoke } from "./tauri";

export interface ShopeeProductInfo {
  itemId: number | null;
  productName: string;
  shopName: string;
  price: number;
  sales: number;
  imageUrl: string;
  productLink: string;
  /** API trả string ("4.50") hoặc number — keep loose. */
  rating: string | number | null;
  commission: number;
  sellerComFinal: number;
  shopeeComFinal: number;
  isXtra: boolean;
  lastUpdate: string;
  dataSource: string;
}

export function fetchShopeeProduct(url: string): Promise<ShopeeProductInfo> {
  return invoke<ShopeeProductInfo>("fetch_shopee_product", { url });
}
