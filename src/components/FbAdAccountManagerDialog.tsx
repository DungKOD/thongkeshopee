import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import {
  fbAdsDeleteAccount,
  fbAdsSaveAccounts,
  fbAdsValidateToken,
  type FbAdAccount,
  type FbAdAccountWithToken,
} from "../lib/fbAds";

interface FbAdAccountManagerDialogProps {
  isOpen: boolean;
  savedAccounts: FbAdAccount[];
  onClose: () => void;
  onChanged: () => void;
}

type Phase = "input" | "validating" | "pick";

export function FbAdAccountManagerDialog({
  isOpen,
  savedAccounts,
  onClose,
  onChanged,
}: FbAdAccountManagerDialogProps) {
  const [token, setToken] = useState("");
  const [phase, setPhase] = useState<Phase>("input");
  const [discovered, setDiscovered] = useState<FbAdAccountWithToken[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!isOpen) {
      setToken("");
      setPhase("input");
      setDiscovered([]);
      setSelected(new Set());
      setError(null);
      setSaving(false);
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const handleValidate = async () => {
    if (!token.trim()) {
      setError("Hãy paste Access Token");
      return;
    }
    setPhase("validating");
    setError(null);
    try {
      const accs = await fbAdsValidateToken(token.trim());
      if (accs.length === 0) {
        setError("Token hợp lệ nhưng không tìm thấy Ad Account nào");
        setPhase("input");
        return;
      }
      setDiscovered(accs);
      setSelected(new Set(accs.map((a) => a.accountId)));
      setPhase("pick");
    } catch (e) {
      setError((e as Error).message ?? String(e));
      setPhase("input");
    }
  };

  const handleSave = async () => {
    const toSave = discovered.filter((a) => selected.has(a.accountId));
    if (toSave.length === 0) {
      setError("Chọn ít nhất 1 Ad Account");
      return;
    }
    setSaving(true);
    try {
      await fbAdsSaveAccounts(toSave);
      onChanged();
      onClose();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (accountId: string) => {
    if (!confirm("Xóa Ad Account khỏi app? Templates + drafts vẫn được giữ."))
      return;
    try {
      await fbAdsDeleteAccount(accountId);
      onChanged();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    }
  };

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      onMouseDown={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        className="w-full max-w-2xl overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
      >
        <header className="flex items-center gap-3 border-b border-surface-8 px-6 py-4">
          <span className="material-symbols-rounded text-2xl text-violet-400">
            account_balance
          </span>
          <h2 className="text-lg font-semibold text-white/90">
            Quản lý Facebook Ad Account
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="ml-auto flex h-8 w-8 items-center justify-center rounded-full text-white/60 hover:bg-white/10 hover:text-white"
            aria-label="Đóng"
          >
            <span className="material-symbols-rounded">close</span>
          </button>
        </header>

        <div className="max-h-[70vh] space-y-5 overflow-y-auto px-6 py-5">
          {savedAccounts.length > 0 && (
            <section>
              <h3 className="mb-2 text-sm font-semibold text-white/80">
                Ad Account đã lưu ({savedAccounts.length})
              </h3>
              <ul className="space-y-1.5">
                {savedAccounts.map((a) => (
                  <li
                    key={a.accountId}
                    className="flex items-center gap-3 rounded-lg border border-surface-8 bg-surface-2 px-3 py-2"
                  >
                    <span className="material-symbols-rounded text-base text-violet-300">
                      campaign
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-sm font-medium text-white/90">
                        {a.name}
                      </div>
                      <div className="font-mono text-[11px] text-white/40">
                        {a.accountId}
                        {a.currency && ` · ${a.currency}`}
                        {a.timezoneName && ` · ${a.timezoneName}`}
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={() => void handleDelete(a.accountId)}
                      className="flex h-7 w-7 items-center justify-center rounded-full text-red-300 hover:bg-red-500/20"
                      title="Xóa khỏi app"
                    >
                      <span className="material-symbols-rounded text-base">
                        delete
                      </span>
                    </button>
                  </li>
                ))}
              </ul>
            </section>
          )}

          <section>
            <h3 className="mb-2 text-sm font-semibold text-white/80">
              {savedAccounts.length > 0
                ? "Thêm Ad Account mới"
                : "Thêm Ad Account lần đầu"}
            </h3>

            <details className="mb-3 rounded-lg border border-violet-500/30 bg-violet-950/30 px-3 py-2 text-xs text-white/75">
              <summary className="cursor-pointer font-medium text-violet-200">
                Lấy Access Token cho Marketing API
              </summary>
              <div className="mt-2 space-y-2 leading-relaxed">
                <p>
                  Khác với Page Token cho Reels, Ad Account cần scope{" "}
                  <code className="rounded bg-surface-1 px-1">
                    ads_management
                  </code>
                  . Quy trình:
                </p>
                <ol className="list-decimal space-y-0.5 pl-5">
                  <li>
                    Vào{" "}
                    <a
                      href="https://developers.facebook.com/tools/explorer/"
                      target="_blank"
                      rel="noreferrer"
                      className="text-violet-300 underline hover:text-violet-200"
                    >
                      Graph API Explorer
                    </a>
                  </li>
                  <li>
                    Permissions → add:{" "}
                    <code className="rounded bg-surface-1 px-1">
                      ads_management
                    </code>
                    ,{" "}
                    <code className="rounded bg-surface-1 px-1">ads_read</code>,{" "}
                    <code className="rounded bg-surface-1 px-1">
                      business_management
                    </code>
                  </li>
                  <li>Generate token → confirm → copy</li>
                  <li>
                    Đổi sang 60 ngày qua{" "}
                    <a
                      href="https://developers.facebook.com/tools/debug/accesstoken/"
                      target="_blank"
                      rel="noreferrer"
                      className="text-violet-300 underline hover:text-violet-200"
                    >
                      Access Token Debugger
                    </a>{" "}
                    → bấm "Extend Access Token"
                  </li>
                  <li>Paste token vào ô bên dưới</li>
                </ol>
                <p className="mt-2 text-[11px] text-amber-200">
                  ⚠️ Token User 60 ngày sẽ hết hạn. Dev mode chỉ dùng cho ad
                  account của chính mình — không cần App Review.
                </p>
              </div>
            </details>

            {phase !== "pick" && (
              <>
                <textarea
                  value={token}
                  onChange={(e) => setToken(e.currentTarget.value)}
                  placeholder="EAAxxx... (paste User Access Token với scope ads_management)"
                  rows={3}
                  className="w-full resize-y rounded-lg border border-surface-8 bg-surface-1 px-3 py-2 font-mono text-xs text-white/90 placeholder:text-white/30 focus:border-violet-500 focus:outline-none focus:ring-1 focus:ring-violet-500"
                />
                <button
                  type="button"
                  onClick={() => void handleValidate()}
                  disabled={phase === "validating" || !token.trim()}
                  className="btn-ripple mt-2 flex w-full items-center justify-center gap-2 rounded-lg bg-violet-500 px-4 py-2 text-sm font-medium text-white hover:bg-violet-600 disabled:opacity-50"
                >
                  {phase === "validating" ? (
                    <>
                      <span className="material-symbols-rounded animate-spin text-base">
                        sync
                      </span>
                      Đang kiểm tra...
                    </>
                  ) : (
                    <>
                      <span className="material-symbols-rounded text-base">
                        check_circle
                      </span>
                      Xác thực token
                    </>
                  )}
                </button>
              </>
            )}

            {phase === "pick" && (
              <>
                <p className="mb-2 text-xs text-white/65">
                  Tìm thấy {discovered.length} Ad Account — tick chọn để lưu:
                </p>
                <ul className="max-h-64 space-y-1 overflow-y-auto rounded-lg border border-surface-8 bg-surface-1 p-2">
                  {discovered.map((a) => (
                    <li key={a.accountId}>
                      <label className="flex cursor-pointer items-center gap-3 rounded-md px-2 py-1.5 hover:bg-white/5">
                        <input
                          type="checkbox"
                          checked={selected.has(a.accountId)}
                          onChange={() => {
                            setSelected((s) => {
                              const next = new Set(s);
                              if (next.has(a.accountId))
                                next.delete(a.accountId);
                              else next.add(a.accountId);
                              return next;
                            });
                          }}
                          className="h-4 w-4 accent-violet-500"
                        />
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-sm text-white/90">
                            {a.name}
                          </div>
                          <div className="font-mono text-[11px] text-white/40">
                            {a.accountId}
                            {a.currency && ` · ${a.currency}`}
                          </div>
                        </div>
                      </label>
                    </li>
                  ))}
                </ul>
                <div className="mt-3 flex gap-2">
                  <button
                    type="button"
                    onClick={() => setPhase("input")}
                    className="rounded-lg border border-surface-8 px-4 py-2 text-sm font-medium text-white/70 hover:bg-white/5"
                  >
                    Quay lại
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleSave()}
                    disabled={saving || selected.size === 0}
                    className="btn-ripple ml-auto flex items-center gap-2 rounded-lg bg-violet-500 px-5 py-2 text-sm font-medium text-white hover:bg-violet-600 disabled:opacity-50"
                  >
                    {saving ? (
                      <>
                        <span className="material-symbols-rounded animate-spin text-base">
                          sync
                        </span>
                        Đang lưu...
                      </>
                    ) : (
                      <>
                        <span className="material-symbols-rounded text-base">
                          save
                        </span>
                        Lưu {selected.size} Account
                      </>
                    )}
                  </button>
                </div>
              </>
            )}
          </section>

          {error && (
            <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
              {error}
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body,
  );
}
