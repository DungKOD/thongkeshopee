import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import {
  fbAdsListFbCampaigns,
  fbAdsSaveTemplate,
  type FbAdAccount,
  type FbCampaignSummary,
} from "../lib/fbAds";

interface FbCampTemplatePickerDialogProps {
  isOpen: boolean;
  accounts: FbAdAccount[];
  defaultAccountId?: string;
  onClose: () => void;
  onSaved: (templateId: number) => void;
}

export function FbCampTemplatePickerDialog({
  isOpen,
  accounts,
  defaultAccountId,
  onClose,
  onSaved,
}: FbCampTemplatePickerDialogProps) {
  const [accountId, setAccountId] = useState<string>(
    defaultAccountId ?? accounts[0]?.accountId ?? "",
  );
  const [campaigns, setCampaigns] = useState<FbCampaignSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [templateName, setTemplateName] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!isOpen) {
      setCampaigns([]);
      setSelectedId(null);
      setTemplateName("");
      setError(null);
      setSaving(false);
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen || !accountId) return;
    setLoading(true);
    setError(null);
    fbAdsListFbCampaigns(accountId, 100)
      .then((cs) => setCampaigns(cs))
      .catch((e) => setError((e as Error).message ?? String(e)))
      .finally(() => setLoading(false));
  }, [isOpen, accountId]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const handleSelect = (c: FbCampaignSummary) => {
    setSelectedId(c.id);
    if (!templateName) setTemplateName(c.name);
  };

  const handleSave = async () => {
    if (!selectedId) {
      setError("Chọn 1 campaign làm template");
      return;
    }
    if (!templateName.trim()) {
      setError("Đặt tên template");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const id = await fbAdsSaveTemplate({
        accountId,
        fbCampaignId: selectedId,
        templateName: templateName.trim(),
      });
      onSaved(id);
      onClose();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setSaving(false);
    }
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        className="w-full max-w-3xl overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
      >
        <header className="flex items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-2xl text-violet-400">
            content_copy
          </span>
          <h2 className="text-lg font-semibold text-white/90">
            Chọn Campaign làm Template
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="ml-auto flex h-8 w-8 items-center justify-center rounded-full text-white/60 hover:bg-white/10 hover:text-white"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <div className="max-h-[75vh] space-y-4 overflow-y-auto px-6 py-5">
          <div>
            <label className="mb-1 block text-xs font-medium text-white/70">
              Ad Account
            </label>
            <select
              value={accountId}
              onChange={(e) => setAccountId(e.currentTarget.value)}
              className="w-full rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90"
            >
              {accounts.map((a) => (
                <option key={a.accountId} value={a.accountId}>
                  {a.name} ({a.accountId})
                </option>
              ))}
            </select>
          </div>

          <div>
            <p className="mb-2 text-xs font-medium text-white/70">
              Campaigns ACTIVE/PAUSED của account này
            </p>
            {loading ? (
              <div className="flex items-center justify-center gap-2 rounded-lg border border-surface-8 bg-surface-1 py-8 text-white/60">
                <span className="material-symbols-rounded animate-spin text-base">
                  sync
                </span>
                Đang tải...
              </div>
            ) : campaigns.length === 0 ? (
              <div className="rounded-lg border border-dashed border-surface-8 bg-surface-1 py-6 text-center text-xs text-white/55">
                Không có campaign nào ở status ACTIVE/PAUSED.
                <br />
                Tạo 1 campaign mẫu trên Ads Manager trước.
              </div>
            ) : (
              <ul className="max-h-80 space-y-1 overflow-y-auto rounded-lg border border-surface-8 bg-surface-1 p-2">
                {campaigns.map((c) => (
                  <li key={c.id}>
                    <button
                      type="button"
                      onClick={() => handleSelect(c)}
                      className={`flex w-full items-center gap-3 rounded-md px-3 py-2 text-left hover:bg-white/5 ${
                        selectedId === c.id ? "bg-violet-500/20" : ""
                      }`}
                    >
                      <span
                        className={`material-symbols-rounded text-base ${
                          selectedId === c.id
                            ? "text-violet-300"
                            : "text-white/40"
                        }`}
                      >
                        {selectedId === c.id
                          ? "radio_button_checked"
                          : "radio_button_unchecked"}
                      </span>
                      <div className="min-w-0 flex-1">
                        <div className="truncate text-sm text-white/90">
                          {c.name}
                        </div>
                        <div className="font-mono text-[11px] text-white/40">
                          {c.id}
                          {c.objective && ` · ${c.objective}`}
                          {c.status && ` · ${c.status}`}
                        </div>
                      </div>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </div>

          {selectedId && (
            <div>
              <label className="mb-1 block text-xs font-medium text-white/70">
                Tên template (để dễ chọn sau này)
              </label>
              <input
                type="text"
                value={templateName}
                onChange={(e) => setTemplateName(e.currentTarget.value)}
                placeholder="Vd: Template Shopee Mỹ phẩm Traffic"
                className="w-full rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90 focus:border-violet-500 focus:outline-none focus:ring-1 focus:ring-violet-500"
              />
            </div>
          )}

          {error && (
            <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
              {error}
            </div>
          )}
        </div>

        <footer className="flex justify-end gap-2 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg px-4 py-2 text-sm font-medium text-white/80 hover:bg-white/5"
          >
            Hủy
          </button>
          <button
            type="button"
            onClick={() => void handleSave()}
            disabled={saving || !selectedId}
            className="btn-ripple flex items-center gap-2 rounded-lg bg-violet-500 px-5 py-2 text-sm font-medium text-white hover:bg-violet-600 disabled:opacity-50"
          >
            {saving ? (
              <>
                <span className="material-symbols-rounded animate-spin text-base">
                  sync
                </span>
                Đang fetch + lưu...
              </>
            ) : (
              <>
                <span className="material-symbols-rounded text-base">save</span>
                Lưu template
              </>
            )}
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
