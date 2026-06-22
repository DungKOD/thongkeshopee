import { invoke } from "./tauri";

export interface FbAdAccount {
  accountId: string;
  name: string;
  currency: string | null;
  timezoneName: string | null;
}

export interface FbAdAccountWithToken {
  accountId: string;
  name: string;
  currency: string | null;
  timezoneName: string | null;
  accessToken: string;
}

export interface FbCampaignSummary {
  id: string;
  name: string;
  objective: string | null;
  status: string | null;
  createdTime: string | null;
}

export interface FbCampTemplate {
  templateId: number;
  accountId: string;
  fbCampaignId: string;
  name: string;
  objective: string | null;
  cachedAtMs: number;
}

export interface FbCampTemplateDetail extends FbCampTemplate {
  snapshot: unknown;
}

export interface CampRow {
  campName: string;
  adsetName: string;
  adName: string;
  caption: string;
  videoPath: string;
  subId: string;
}

export function emptyCampRow(): CampRow {
  return {
    campName: "",
    adsetName: "",
    adName: "",
    caption: "",
    videoPath: "",
    subId: "",
  };
}

export interface FbCampDraft {
  draftId: number;
  templateId: number;
  name: string;
  rows: CampRow[];
  createdAtMs: number;
  updatedAtMs: number;
}

export interface FbCampDraftSummary {
  draftId: number;
  templateId: number;
  templateName: string | null;
  name: string;
  rowCount: number;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface FbCampBatch {
  batchId: number;
  draftId: number | null;
  templateId: number;
  templateName: string;
  accountId: string;
  totalRows: number;
  successCount: number;
  failedCount: number;
  status: string;
  startedAtMs: number;
  finishedAtMs: number | null;
}

export interface FbCampJob {
  jobId: number;
  batchId: number;
  rowIndex: number;
  campName: string;
  adsetName: string;
  adName: string;
  caption: string | null;
  videoPath: string;
  subId: string;
  status: string;
  progress: number;
  fbVideoId: string | null;
  fbCreativeId: string | null;
  fbCampaignId: string | null;
  fbAdsetId: string | null;
  fbAdId: string | null;
  errorMessage: string | null;
  startedAtMs: number | null;
  finishedAtMs: number | null;
}

// ===== Accounts =====

export function fbAdsValidateToken(
  token: string,
): Promise<FbAdAccountWithToken[]> {
  return invoke<FbAdAccountWithToken[]>("fb_ads_validate_token", { token });
}

export function fbAdsSaveAccounts(
  accounts: FbAdAccountWithToken[],
): Promise<void> {
  return invoke<void>("fb_ads_save_accounts", { accounts });
}

export function fbAdsListAccounts(): Promise<FbAdAccount[]> {
  return invoke<FbAdAccount[]>("fb_ads_list_accounts");
}

export function fbAdsDeleteAccount(accountId: string): Promise<void> {
  return invoke<void>("fb_ads_delete_account", { accountId });
}

// ===== Templates =====

export function fbAdsListFbCampaigns(
  accountId: string,
  limit?: number,
): Promise<FbCampaignSummary[]> {
  return invoke<FbCampaignSummary[]>("fb_ads_list_fb_campaigns", {
    accountId,
    limit: limit ?? null,
  });
}

export function fbAdsSaveTemplate(args: {
  accountId: string;
  fbCampaignId: string;
  templateName: string;
}): Promise<number> {
  return invoke<number>("fb_ads_save_template", args);
}

export function fbAdsListTemplates(
  accountId?: string,
): Promise<FbCampTemplate[]> {
  return invoke<FbCampTemplate[]>("fb_ads_list_templates", {
    accountId: accountId ?? null,
  });
}

export function fbAdsGetTemplateDetail(
  templateId: number,
): Promise<FbCampTemplateDetail> {
  return invoke<FbCampTemplateDetail>("fb_ads_get_template_detail", {
    templateId,
  });
}

export function fbAdsDeleteTemplate(templateId: number): Promise<void> {
  return invoke<void>("fb_ads_delete_template", { templateId });
}

// ===== Drafts =====

export function fbAdsSaveDraft(args: {
  draftId: number | null;
  templateId: number;
  name: string;
  rows: CampRow[];
}): Promise<number> {
  return invoke<number>("fb_ads_save_draft", args);
}

export function fbAdsListDrafts(): Promise<FbCampDraftSummary[]> {
  return invoke<FbCampDraftSummary[]>("fb_ads_list_drafts");
}

export function fbAdsGetDraft(draftId: number): Promise<FbCampDraft> {
  return invoke<FbCampDraft>("fb_ads_get_draft", { draftId });
}

export function fbAdsDeleteDraft(draftId: number): Promise<void> {
  return invoke<void>("fb_ads_delete_draft", { draftId });
}

// ===== Batches (Phase 2) =====

export function fbAdsCreateBatch(args: {
  templateId: number;
  draftId: number | null;
  rows: CampRow[];
}): Promise<number> {
  return invoke<number>("fb_ads_create_batch", args);
}

export function fbAdsRetryJob(jobId: number): Promise<void> {
  return invoke<void>("fb_ads_retry_job", { jobId });
}

export function fbAdsListBatches(limit?: number): Promise<FbCampBatch[]> {
  return invoke<FbCampBatch[]>("fb_ads_list_batches", {
    limit: limit ?? null,
  });
}

export function fbAdsListJobs(batchId: number): Promise<FbCampJob[]> {
  return invoke<FbCampJob[]>("fb_ads_list_jobs", { batchId });
}

export interface CampBatchProgressEvent {
  batchId: number;
  jobId: number;
  rowIndex: number;
  status: string;
  progress: number;
}
