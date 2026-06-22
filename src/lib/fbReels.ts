import { invoke } from "./tauri";

export interface FbPage {
  pageId: string;
  name: string;
}

export interface FbPageWithToken {
  pageId: string;
  name: string;
  accessToken: string;
}

export type FbReelStatus =
  | "pending"
  | "uploading"
  | "publishing"
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
