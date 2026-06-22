import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useFbAdAccounts } from "../hooks/useFbAdAccounts";
import { useFbCampTemplates } from "../hooks/useFbCampTemplates";
import { useFbCampDrafts } from "../hooks/useFbCampDrafts";
import { useFbCampBatch } from "../hooks/useFbCampBatch";
import { FbAdAccountManagerDialog } from "./FbAdAccountManagerDialog";
import { FbCampTemplatePickerDialog } from "./FbCampTemplatePickerDialog";
import { FbCampSpreadsheet } from "./FbCampSpreadsheet";
import { FbCampBatchHistory } from "./FbCampBatchHistory";
import {
  emptyCampRow,
  fbAdsDeleteDraft,
  fbAdsDeleteTemplate,
  fbAdsGetDraft,
  fbAdsSaveDraft,
  type CampRow,
  type FbCampDraftSummary,
} from "../lib/fbAds";

const AUTO_SAVE_DEBOUNCE_MS = 2000;

export function CampaignBatchPage() {
  const {
    accounts,
    loading: accountsLoading,
    refresh: refreshAccounts,
  } = useFbAdAccounts();
  const {
    templates,
    loading: templatesLoading,
    refresh: refreshTemplates,
  } = useFbCampTemplates();
  const { drafts, refresh: refreshDrafts } = useFbCampDrafts();
  const {
    batches,
    currentJobs,
    currentBatchId,
    uploading,
    error: batchError,
    loadBatchJobs,
    startBatch,
    retryJob,
  } = useFbCampBatch();

  const [accountManagerOpen, setAccountManagerOpen] = useState(false);
  const [templatePickerOpen, setTemplatePickerOpen] = useState(false);

  const [selectedTemplateId, setSelectedTemplateId] = useState<number | null>(
    null,
  );
  const [draftId, setDraftId] = useState<number | null>(null);
  const [draftName, setDraftName] = useState("Batch mới");
  const [rows, setRows] = useState<CampRow[]>([emptyCampRow()]);

  const [savingDraft, setSavingDraft] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const creatingBatch = uploading;

  const saveTimerRef = useRef<number | null>(null);

  const validRows = useMemo(
    () =>
      rows.filter(
        (r) =>
          r.campName.trim() && r.adsetName.trim() && r.adName.trim() && r.videoPath.trim(),
      ),
    [rows],
  );

  const selectedTemplate = useMemo(
    () => templates.find((t) => t.templateId === selectedTemplateId) ?? null,
    [templates, selectedTemplateId],
  );

  // Auto-save draft khi rows/name/template thay đổi.
  useEffect(() => {
    if (!selectedTemplateId) return;
    if (rows.length === 0) return;
    if (saveTimerRef.current) window.clearTimeout(saveTimerRef.current);
    saveTimerRef.current = window.setTimeout(() => {
      void saveDraft();
    }, AUTO_SAVE_DEBOUNCE_MS);
    return () => {
      if (saveTimerRef.current) window.clearTimeout(saveTimerRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows, draftName, selectedTemplateId]);

  const saveDraft = useCallback(async () => {
    if (!selectedTemplateId) return;
    setSavingDraft(true);
    try {
      const id = await fbAdsSaveDraft({
        draftId,
        templateId: selectedTemplateId,
        name: draftName.trim() || "Batch không tên",
        rows,
      });
      if (draftId === null) setDraftId(id);
      void refreshDrafts();
    } catch (e) {
      console.error("save draft failed:", e);
    } finally {
      setSavingDraft(false);
    }
  }, [draftId, draftName, rows, selectedTemplateId, refreshDrafts]);

  const handleLoadDraft = useCallback(async (d: FbCampDraftSummary) => {
    try {
      const full = await fbAdsGetDraft(d.draftId);
      setDraftId(full.draftId);
      setDraftName(full.name);
      setSelectedTemplateId(full.templateId);
      setRows(full.rows.length > 0 ? full.rows : [emptyCampRow()]);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    }
  }, []);

  const handleNewDraft = useCallback(() => {
    setDraftId(null);
    setDraftName("Batch mới");
    setRows([emptyCampRow()]);
  }, []);

  const handleDeleteDraft = useCallback(
    async (id: number) => {
      if (!confirm("Xóa draft này?")) return;
      try {
        await fbAdsDeleteDraft(id);
        if (id === draftId) handleNewDraft();
        void refreshDrafts();
      } catch (e) {
        setError((e as Error).message ?? String(e));
      }
    },
    [draftId, handleNewDraft, refreshDrafts],
  );

  const handleDeleteTemplate = useCallback(
    async (id: number) => {
      if (!confirm("Xóa template này khỏi app?")) return;
      try {
        await fbAdsDeleteTemplate(id);
        if (id === selectedTemplateId) setSelectedTemplateId(null);
        void refreshTemplates();
      } catch (e) {
        setError((e as Error).message ?? String(e));
      }
    },
    [selectedTemplateId, refreshTemplates],
  );

  const handleCreateBatch = useCallback(async () => {
    if (!selectedTemplateId) {
      setError("Chọn template trước");
      return;
    }
    if (validRows.length === 0) {
      setError(
        "Bảng không có row hợp lệ nào (cần đủ camp_name, adset_name, ad_name, video_path)",
      );
      return;
    }
    if (
      !confirm(
        `Tạo ${validRows.length} camps trên FB (status PAUSED)?\n\n` +
          `App KHÔNG kích hoạt campaign. Sau khi tạo xong, vào Ads Manager ` +
          `review + tự active.\n\nThời gian dự kiến: ~${Math.ceil(
            (validRows.length * 30) / 60,
          )} phút (throttle 30s/job).`,
      )
    )
      return;

    setError(null);
    try {
      await saveDraft();
      await startBatch({
        templateId: selectedTemplateId,
        draftId,
        rows: validRows,
      });
    } catch (e) {
      setError((e as Error).message ?? String(e));
    }
  }, [draftId, saveDraft, selectedTemplateId, startBatch, validRows]);

  if (accountsLoading || templatesLoading) {
    return (
      <div className="mx-auto max-w-7xl">
        <div className="flex items-center justify-center gap-3 rounded-2xl border border-surface-8 bg-surface-1 py-16 text-white/55">
          <span className="material-symbols-rounded animate-spin text-2xl text-violet-400">
            sync
          </span>
          <span>Đang tải...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-7xl space-y-5">
      {/* Hero */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-violet-700 via-violet-600 to-violet-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">campaign</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">Bulk Camp Creator</h1>
            <p className="mt-0.5 text-xs text-white/75">
              Clone 1 camp mẫu thành nhiều camps cho Shopee Affiliate
              (OUTCOME_TRAFFIC) — status PAUSED, user tự active trên Ads Manager
            </p>
          </div>
          <button
            type="button"
            onClick={() => setAccountManagerOpen(true)}
            className="btn-ripple flex items-center gap-2 rounded-lg border border-white/40 px-3 py-1.5 text-sm font-medium text-white hover:bg-white/10"
          >
            <span className="material-symbols-rounded text-base">
              account_balance
            </span>
            {accounts.length > 0 ? `${accounts.length} Account` : "Thêm Account"}
          </button>
        </div>
      </section>

      {/* Status banner */}
      {uploading && (
        <div className="flex items-center gap-3 rounded-lg border border-violet-500/40 bg-violet-950/30 px-4 py-3 text-sm text-violet-100">
          <span className="material-symbols-rounded animate-spin text-2xl text-violet-300">
            sync
          </span>
          <div>
            <strong>Batch đang chạy...</strong>
            <p className="text-xs text-white/65">
              Mỗi job mất ~30s + thời gian upload video. Có thể đóng tab/app, batch
              vẫn tiếp tục — KHÔNG chạy được nếu app tắt.
            </p>
          </div>
        </div>
      )}

      {accounts.length === 0 ? (
        <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-8 text-center">
          <span className="material-symbols-rounded text-5xl text-violet-400">
            account_balance
          </span>
          <h3 className="mt-3 text-lg font-semibold text-white/90">
            Chưa có Ad Account
          </h3>
          <p className="mt-1 text-sm text-white/55">
            Paste Access Token với scope ads_management để bắt đầu
          </p>
          <button
            type="button"
            onClick={() => setAccountManagerOpen(true)}
            className="btn-ripple mx-auto mt-4 flex items-center gap-2 rounded-lg bg-violet-500 px-5 py-2.5 text-sm font-medium text-white hover:bg-violet-600"
          >
            <span className="material-symbols-rounded text-base">add</span>
            Thêm Ad Account
          </button>
        </section>
      ) : (
        <>
          {/* Toolbar — template + drafts + name */}
          <section className="rounded-2xl border border-surface-8 bg-surface-1 p-4">
            <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
              {/* Template selector */}
              <div>
                <label className="mb-1 block text-xs font-medium text-white/70">
                  Template (camp mẫu)
                </label>
                <div className="flex gap-2">
                  <select
                    value={selectedTemplateId ?? ""}
                    onChange={(e) =>
                      setSelectedTemplateId(
                        e.currentTarget.value
                          ? Number(e.currentTarget.value)
                          : null,
                      )
                    }
                    className="flex-1 rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90"
                  >
                    <option value="">-- Chọn template --</option>
                    {templates.map((t) => (
                      <option key={t.templateId} value={t.templateId}>
                        {t.name}
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    onClick={() => setTemplatePickerOpen(true)}
                    className="btn-ripple flex shrink-0 items-center gap-1 rounded-lg border border-violet-500/60 px-3 py-2 text-xs text-violet-200 hover:bg-violet-500/10"
                    title="Thêm template mới từ camp trên FB"
                  >
                    <span className="material-symbols-rounded text-sm">
                      add
                    </span>
                    Mới
                  </button>
                </div>
                {selectedTemplate && (
                  <div className="mt-1 flex items-center gap-2 text-[11px] text-white/45">
                    <span className="font-mono">
                      {selectedTemplate.fbCampaignId}
                    </span>
                    {selectedTemplate.objective && (
                      <span className="rounded bg-surface-4 px-1.5 py-0.5">
                        {selectedTemplate.objective}
                      </span>
                    )}
                    <button
                      type="button"
                      onClick={() =>
                        void handleDeleteTemplate(selectedTemplate.templateId)
                      }
                      className="ml-auto text-red-300 hover:underline"
                    >
                      Xóa
                    </button>
                  </div>
                )}
              </div>

              {/* Draft name + status */}
              <div>
                <label className="mb-1 block text-xs font-medium text-white/70">
                  Tên batch
                </label>
                <input
                  type="text"
                  value={draftName}
                  onChange={(e) => setDraftName(e.currentTarget.value)}
                  placeholder="Vd: Batch T6 - Mỹ phẩm"
                  className="w-full rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90 focus:border-violet-500 focus:outline-none focus:ring-1 focus:ring-violet-500"
                />
                <div className="mt-1 flex items-center gap-2 text-[11px] text-white/45">
                  {draftId !== null && (
                    <span className="font-mono">draft #{draftId}</span>
                  )}
                  {savingDraft ? (
                    <span className="flex items-center gap-1 text-amber-300">
                      <span className="material-symbols-rounded animate-spin text-xs">
                        sync
                      </span>
                      Đang lưu...
                    </span>
                  ) : draftId !== null ? (
                    <span className="text-emerald-300">✓ Đã lưu</span>
                  ) : null}
                </div>
              </div>

              {/* Draft list (top 5) */}
              <div>
                <label className="mb-1 block text-xs font-medium text-white/70">
                  Drafts gần đây
                </label>
                <div className="space-y-1">
                  <button
                    type="button"
                    onClick={handleNewDraft}
                    className="flex w-full items-center gap-2 rounded-md border border-surface-8 px-2 py-1.5 text-left text-xs text-white/70 hover:bg-white/5"
                  >
                    <span className="material-symbols-rounded text-sm">
                      add_box
                    </span>
                    Tạo batch mới
                  </button>
                  {drafts.slice(0, 5).map((d) => (
                    <div
                      key={d.draftId}
                      className={`flex items-center gap-1 rounded-md px-2 py-1 text-xs hover:bg-white/5 ${
                        d.draftId === draftId ? "bg-violet-500/20" : ""
                      }`}
                    >
                      <button
                        type="button"
                        onClick={() => void handleLoadDraft(d)}
                        className="min-w-0 flex-1 truncate text-left text-white/80"
                        title={d.name}
                      >
                        {d.name}{" "}
                        <span className="text-white/40">({d.rowCount})</span>
                      </button>
                      <button
                        type="button"
                        onClick={() => void handleDeleteDraft(d.draftId)}
                        className="flex h-5 w-5 shrink-0 items-center justify-center rounded text-red-300 hover:bg-red-500/20"
                      >
                        <span className="material-symbols-rounded text-xs">
                          delete
                        </span>
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </section>

          {/* Spreadsheet */}
          {selectedTemplateId ? (
            <FbCampSpreadsheet
              rows={rows}
              onChange={setRows}
              disabled={creatingBatch}
            />
          ) : (
            <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-8 text-center text-white/55">
              <span className="material-symbols-rounded text-5xl text-violet-400">
                content_copy
              </span>
              <p className="mt-3 text-sm">
                Chọn 1 template trước khi nhập rows
              </p>
              <button
                type="button"
                onClick={() => setTemplatePickerOpen(true)}
                className="btn-ripple mx-auto mt-3 flex items-center gap-2 rounded-lg bg-violet-500 px-5 py-2 text-sm font-medium text-white hover:bg-violet-600"
              >
                <span className="material-symbols-rounded text-base">
                  add
                </span>
                Thêm template
              </button>
            </section>
          )}

          {(error || batchError) && (
            <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
              {error ?? batchError}
            </div>
          )}

          <FbCampBatchHistory
            batches={batches}
            currentJobs={currentJobs}
            currentBatchId={currentBatchId}
            uploading={uploading}
            onSelectBatch={(id) => void loadBatchJobs(id)}
            onRetryJob={(id) => void retryJob(id)}
          />

          {/* Action bar */}
          {selectedTemplateId && (
            <section className="sticky bottom-0 z-10 rounded-2xl border border-surface-8 bg-surface-4/95 p-4 shadow-elev-16 backdrop-blur">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div className="text-xs text-white/65">
                  <strong className="text-white/90">{validRows.length}</strong>{" "}
                  / {rows.length} rows hợp lệ
                  {rows.length > validRows.length && (
                    <span className="ml-2 text-amber-300">
                      ({rows.length - validRows.length} rows thiếu field bắt
                      buộc)
                    </span>
                  )}
                </div>
                <div className="flex gap-2">
                  <button
                    type="button"
                    onClick={() => void saveDraft()}
                    disabled={savingDraft || creatingBatch}
                    className="flex items-center gap-2 rounded-lg border border-surface-8 px-4 py-2 text-sm font-medium text-white/80 hover:bg-white/5 disabled:opacity-50"
                  >
                    <span className="material-symbols-rounded text-base">
                      save
                    </span>
                    Lưu draft
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleCreateBatch()}
                    disabled={
                      creatingBatch ||
                      !selectedTemplateId ||
                      validRows.length === 0
                    }
                    className="btn-ripple flex items-center gap-2 rounded-lg bg-violet-500 px-5 py-2 text-sm font-semibold text-white shadow-elev-2 hover:bg-violet-600 disabled:opacity-50"
                  >
                    {creatingBatch ? (
                      <>
                        <span className="material-symbols-rounded animate-spin text-base">
                          sync
                        </span>
                        Đang tạo...
                      </>
                    ) : (
                      <>
                        <span className="material-symbols-rounded text-base">
                          rocket_launch
                        </span>
                        Tạo batch ({validRows.length})
                      </>
                    )}
                  </button>
                </div>
              </div>
            </section>
          )}
        </>
      )}

      <FbAdAccountManagerDialog
        isOpen={accountManagerOpen}
        savedAccounts={accounts}
        onClose={() => setAccountManagerOpen(false)}
        onChanged={() => void refreshAccounts()}
      />

      <FbCampTemplatePickerDialog
        isOpen={templatePickerOpen}
        accounts={accounts}
        defaultAccountId={accounts[0]?.accountId}
        onClose={() => setTemplatePickerOpen(false)}
        onSaved={(id) => {
          void refreshTemplates();
          setSelectedTemplateId(id);
        }}
      />
    </div>
  );
}
