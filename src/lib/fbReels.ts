import { invoke } from "./tauri";

export interface FbPage {
  pageId: string;
  name: string;
  /// Backend đã detect FB error 190 (Invalid OAuth) trên Page này — UI hiện
  /// badge "Token hết hạn" + chặn upload tới khi user save token mới.
  tokenExpired: boolean;
  /// 8 ký tự hex đầu của SHA-256(access_token). UI tô màu cùng nhóm cho các
  /// Page có cùng hash (cùng token). Không reversible nên không leak token.
  tokenHash: string;
}

export interface FbPageWithToken {
  pageId: string;
  name: string;
  accessToken: string;
}

/// User Token đã lưu — meta only, raw token fetch on-demand qua fbGetAuthToken.
export interface FbAuthToken {
  id: number;
  label: string;
  tokenHash: string;
  addedAtMs: number;
  expired: boolean;
}

export type FbReelStatus =
  | "pending"
  | "uploading"
  | "publishing"
  | "processing"
  | "scheduled"
  | "published"
  | "failed";

export interface FbReelPost {
  id: number;
  pageId: string;
  pageName: string;
  filePath: string;
  fileSize: number;
  caption: string | null;
  scheduledTimeMs: number | null;
  status: FbReelStatus;
  progress: number;
  fbVideoId: string | null;
  fbPermalink: string | null;
  errorMessage: string | null;
  createdAtMs: number;
  publishedAtMs: number | null;
}

export interface UploadProgressEvent {
  postId: number;
  status: FbReelStatus;
  progress: number;
  bytesUploaded: number;
  bytesTotal: number;
}

export interface ListPostsFilter {
  pageId?: string;
  status?: FbReelStatus;
  limit?: number;
}

export function fbValidateToken(token: string): Promise<FbPageWithToken[]> {
  return invoke<FbPageWithToken[]>("fb_validate_token", { token });
}

export function fbSavePages(pages: FbPageWithToken[]): Promise<void> {
  return invoke<void>("fb_save_pages", { pages });
}

export function fbListPages(): Promise<FbPage[]> {
  return invoke<FbPage[]>("fb_list_pages");
}

export function fbDeletePage(pageId: string): Promise<void> {
  return invoke<void>("fb_delete_page", { pageId });
}

/// Đọc access_token đã lưu — UI hiển thị/copy lại token. Token vẫn nằm trong
/// DB local (sqlite chưa encrypt at rest), backend chỉ trả khi UI request.
export function fbGetPageToken(pageId: string): Promise<string> {
  return invoke<string>("fb_get_page_token", { pageId });
}

/// Lưu/upsert 1 User Token. Trả id (mới hoặc cũ nếu dedupe). Auto-gen label
/// nếu không truyền.
export function fbSaveAuthToken(
  token: string,
  label?: string | null,
): Promise<number> {
  return invoke<number>("fb_save_auth_token", { token, label: label ?? null });
}

export function fbListAuthTokens(): Promise<FbAuthToken[]> {
  return invoke<FbAuthToken[]>("fb_list_auth_tokens");
}

export function fbGetAuthToken(id: number): Promise<string> {
  return invoke<string>("fb_get_auth_token", { id });
}

export function fbUpdateAuthTokenLabel(
  id: number,
  label: string,
): Promise<void> {
  return invoke<void>("fb_update_auth_token_label", { id, label });
}

export function fbDeleteAuthToken(id: number): Promise<void> {
  return invoke<void>("fb_delete_auth_token", { id });
}

export function fbEnqueueReel(args: {
  pageId: string;
  filePath: string;
  caption: string | null;
  scheduledTimeMs: number | null;
}): Promise<number> {
  return invoke<number>("fb_enqueue_reel", args);
}

export function fbUploadReel(postId: number): Promise<void> {
  return invoke<void>("fb_upload_reel", { postId });
}

export function fbListPosts(filter?: ListPostsFilter): Promise<FbReelPost[]> {
  return invoke<FbReelPost[]>("fb_list_posts", { filter: filter ?? null });
}

export function fbDeletePost(postId: number): Promise<void> {
  return invoke<void>("fb_delete_post", { postId });
}

/// Force re-fetch trạng thái 1 post từ FB (user-triggered). Background poll
/// chạy mỗi 60s tự động, nhưng UI button "Cập nhật" gọi command này để check
/// ngay không cần đợi.
export function fbRefetchPostStatus(postId: number): Promise<void> {
  return invoke<void>("fb_refetch_post_status", { postId });
}

/// Lấy raw JSON FB trả về cho video — UI hiển thị cho user copy debug khi
/// post kẹt `processing` hoặc fail mơ hồ. Backend pretty-print sẵn.
export function fbDebugVideoInfo(postId: number): Promise<string> {
  return invoke<string>("fb_debug_video_info", { postId });
}
