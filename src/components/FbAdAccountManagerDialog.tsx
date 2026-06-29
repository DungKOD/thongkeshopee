import { useCallback, useEffect, useState } from "react";
import { createPortal } from "react-dom";
import {
  fbAdsDeleteAccount,
  fbAdsDeleteAuthToken,
  fbAdsGetAccountToken,
  fbAdsGetAuthToken,
  fbAdsListAuthTokens,
  fbAdsSaveAccounts,
  fbAdsSaveAuthToken,
  fbAdsUpdateAuthTokenLabel,
  fbAdsValidateToken,
  type FbAdAccount,
  type FbAdAccountWithToken,
  type FbAdsAuthToken,
} from "../lib/fbAds";

interface FbAdAccountManagerDialogProps {
  isOpen: boolean;
  savedAccounts: FbAdAccount[];
  onClose: () => void;
  onChanged: () => void;
}

type Phase = "input" | "validating" | "pick";

/// Mask token kiểu `EAAxxxx…xxxx` — giữ 4 đầu + 4 cuối.
function maskToken(token: string): string {
  if (token.length <= 12) return "•".repeat(token.length);
  return `${token.slice(0, 4)}${"•".repeat(16)}${token.slice(-4)}`;
}

/// Map hash → HSL color deterministic. Account/token cùng hash → cùng màu.
function colorFromHash(hash: string): {
  border: string;
  bg: string;
  dot: string;
} {
  const hue = parseInt(hash.slice(0, 4), 16) % 360;
  return {
    border: `hsla(${hue}, 65%, 55%, 0.55)`,
    bg: `hsla(${hue}, 65%, 50%, 0.10)`,
    dot: `hsl(${hue}, 70%, 60%)`,
  };
}

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
  const [authTokens, setAuthTokens] = useState<FbAdsAuthToken[]>([]);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);

  const refreshAuthTokens = useCallback(async () => {
    try {
      const list = await fbAdsListAuthTokens();
      setAuthTokens(list);
    } catch {
      // Bảng có thể chưa init (workspace mới chuyển) — ignore.
    }
  }, []);

  useEffect(() => {
    if (!isOpen) {
      setToken("");
      setPhase("input");
      setDiscovered([]);
      setSelected(new Set());
      setError(null);
      setSaving(false);
      setSuccessMsg(null);
    } else {
      void refreshAuthTokens();
    }
  }, [isOpen, refreshAuthTokens]);

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
      // Lưu auth token (User Token gốc) song song với accounts.
      await fbAdsSaveAuthToken(token.trim());
      await fbAdsSaveAccounts(toSave);
      await refreshAuthTokens();
      onChanged();
      // Giữ dialog mở — reset form để user paste token tiếp.
      setToken("");
      setDiscovered([]);
      setSelected(new Set());
      setPhase("input");
      setError(null);
      setSuccessMsg(`Đã lưu ${toSave.length} Ad Account + 1 token xác thực`);
      window.setTimeout(() => setSuccessMsg(null), 4000);
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

  const handleAuthChanged = async () => {
    await refreshAuthTokens();
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
          {authTokens.length > 0 && (
            <section>
              <h3 className="mb-2 flex items-center gap-2 text-sm font-semibold text-white/80">
                Token xác thực đã lưu ({authTokens.length})
                <span
                  className="text-[10px] font-normal text-white/40"
                  title="User Token (scope ads_management) user paste vào ô 'Xác thực'. 1 User Token quản nhiều Ad Account."
                >
                  · (User Token)
                </span>
              </h3>
              <ul className="space-y-1.5">
                {authTokens.map((t) => (
                  <SavedAdsAuthTokenRow
                    key={t.id}
                    auth={t}
                    onChanged={() => void handleAuthChanged()}
                  />
                ))}
              </ul>
            </section>
          )}

          {savedAccounts.length > 0 && (
            <section>
              <h3 className="mb-2 text-sm font-semibold text-white/80">
                Ad Account đã lưu ({savedAccounts.length})
              </h3>
              <ul className="space-y-1.5">
                {savedAccounts.map((a) => (
                  <SavedAdAccountRow
                    key={a.accountId}
                    account={a}
                    onDelete={() => void handleDelete(a.accountId)}
                  />
                ))}
              </ul>
            </section>
          )}

          {successMsg && (
            <div className="flex items-start gap-2 rounded-lg border border-emerald-500/40 bg-emerald-950/30 px-3 py-2 text-xs text-emerald-200">
              <span className="material-symbols-rounded shrink-0 text-base">
                check_circle
              </span>
              <span className="flex-1">{successMsg}</span>
              <button
                type="button"
                onClick={() => setSuccessMsg(null)}
                className="text-emerald-300/70 hover:text-emerald-200"
                aria-label="Đóng thông báo"
              >
                <span className="material-symbols-rounded text-sm">close</span>
              </button>
            </div>
          )}

          <section>
            <h3 className="mb-2 text-sm font-semibold text-white/80">
              {authTokens.length > 0 || savedAccounts.length > 0
                ? "Thêm token / Ad Account khác"
                : "Thêm Ad Account lần đầu"}
            </h3>
            {(authTokens.length > 0 || savedAccounts.length > 0) && (
              <p className="mb-3 text-[11px] text-white/55">
                Bạn có thể thêm nhiều token xác thực — paste token tiếp theo
                rồi xác thực để gộp thêm Ad Account vào danh sách trên.
              </p>
            )}

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

interface SavedAdAccountRowProps {
  account: FbAdAccount;
  onDelete: () => void;
}

/// 1 row Ad Account — có copy + reveal Account Token. Color theo `tokenHash`.
function SavedAdAccountRow({ account, onDelete }: SavedAdAccountRowProps) {
  const [token, setToken] = useState<string | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [rowError, setRowError] = useState<string | null>(null);

  const color = colorFromHash(account.tokenHash);

  const handleToggle = async () => {
    setRowError(null);
    if (revealed) {
      setRevealed(false);
      setToken(null);
      return;
    }
    if (token) {
      setRevealed(true);
      return;
    }
    setLoading(true);
    try {
      const t = await fbAdsGetAccountToken(account.accountId);
      setToken(t);
      setRevealed(true);
    } catch (e) {
      setRowError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = async () => {
    setRowError(null);
    try {
      const value = token ?? (await fbAdsGetAccountToken(account.accountId));
      if (!token) setToken(value);
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (e) {
      setRowError(`Không copy được: ${(e as Error).message ?? String(e)}`);
    }
  };

  return (
    <li
      className="rounded-lg border bg-surface-2 px-3 py-2"
      style={{
        borderColor: color.border,
        borderLeftWidth: 4,
        background: `linear-gradient(to right, ${color.bg}, transparent 60%)`,
      }}
    >
      <div className="flex items-center gap-3">
        <span className="material-symbols-rounded text-base text-violet-300">
          campaign
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="truncate text-sm font-medium text-white/90">
              {account.name}
            </span>
            <span
              className="shrink-0 rounded-full px-1.5 py-0.5 font-mono text-[10px] text-white/85"
              style={{ background: color.dot }}
              title={`Token group #${account.tokenHash} — account cùng màu = cùng token`}
            >
              #{account.tokenHash}
            </span>
          </div>
          <div className="font-mono text-[11px] text-white/40">
            {account.accountId}
            {account.currency && ` · ${account.currency}`}
            {account.timezoneName && ` · ${account.timezoneName}`}
          </div>
        </div>
        <button
          type="button"
          onClick={() => void handleCopy()}
          className={`flex shrink-0 items-center gap-1.5 rounded-lg border px-2.5 py-1 text-xs font-medium transition-colors ${
            copied
              ? "border-emerald-500/50 bg-emerald-500/15 text-emerald-200"
              : "border-violet-500/40 bg-violet-500/10 text-violet-200 hover:bg-violet-500/20"
          }`}
          title="Copy Account Token vào clipboard"
        >
          <span className="material-symbols-rounded text-sm">
            {copied ? "check" : "content_copy"}
          </span>
          {copied ? "Đã copy" : "Copy token"}
        </button>
        <button
          type="button"
          onClick={() => void handleToggle()}
          disabled={loading}
          className="flex h-7 w-7 items-center justify-center rounded-full text-violet-200 hover:bg-violet-500/20 disabled:opacity-50"
          title={revealed ? "Ẩn token" : "Hiện token"}
        >
          <span className="material-symbols-rounded text-base">
            {loading ? "hourglass_empty" : revealed ? "lock_open" : "lock"}
          </span>
        </button>
        <button
          type="button"
          onClick={onDelete}
          className="flex h-7 w-7 items-center justify-center rounded-full text-red-300 hover:bg-red-500/20"
          title="Xóa khỏi app"
        >
          <span className="material-symbols-rounded text-base">delete</span>
        </button>
      </div>

      {token && (
        <div className="mt-2 flex items-start gap-2 rounded-md border border-surface-8 bg-surface-1 px-2 py-1.5">
          <span
            className={`material-symbols-rounded shrink-0 text-sm ${
              revealed ? "text-amber-300" : "text-white/45"
            }`}
          >
            key
          </span>
          <code
            className={`min-w-0 flex-1 break-all font-mono text-[11px] ${
              revealed ? "text-amber-100" : "text-white/50"
            }`}
            style={{ userSelect: revealed ? "all" : "none" }}
          >
            {revealed ? token : maskToken(token)}
          </code>
          <button
            type="button"
            onClick={() => void handleCopy()}
            className={`flex shrink-0 items-center gap-1 rounded px-1.5 py-0.5 text-[11px] ${
              copied
                ? "bg-emerald-500/15 text-emerald-200"
                : "text-violet-200 hover:bg-violet-500/15"
            }`}
            title="Copy token"
          >
            <span className="material-symbols-rounded text-sm">
              {copied ? "check" : "content_copy"}
            </span>
            {copied ? "Đã copy" : "Copy"}
          </button>
        </div>
      )}

      {rowError && (
        <div className="mt-1.5 text-[11px] text-red-300">{rowError}</div>
      )}
    </li>
  );
}

interface SavedAdsAuthTokenRowProps {
  auth: FbAdsAuthToken;
  onChanged: () => void;
}

/// 1 row User Token đã lưu (FB Ads) — clone từ FB Reels SavedAuthTokenRow.
function SavedAdsAuthTokenRow({ auth, onChanged }: SavedAdsAuthTokenRowProps) {
  const [token, setToken] = useState<string | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [rowError, setRowError] = useState<string | null>(null);
  const [editing, setEditing] = useState(false);
  const [labelDraft, setLabelDraft] = useState(auth.label);

  const color = colorFromHash(auth.tokenHash);

  const handleToggle = async () => {
    setRowError(null);
    if (revealed) {
      setRevealed(false);
      setToken(null);
      return;
    }
    if (token) {
      setRevealed(true);
      return;
    }
    setLoading(true);
    try {
      const t = await fbAdsGetAuthToken(auth.id);
      setToken(t);
      setRevealed(true);
    } catch (e) {
      setRowError((e as Error).message ?? String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = async () => {
    setRowError(null);
    try {
      const value = token ?? (await fbAdsGetAuthToken(auth.id));
      if (!token) setToken(value);
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (e) {
      setRowError(`Không copy được: ${(e as Error).message ?? String(e)}`);
    }
  };

  const handleSaveLabel = async () => {
    const trimmed = labelDraft.trim();
    if (!trimmed || trimmed === auth.label) {
      setEditing(false);
      setLabelDraft(auth.label);
      return;
    }
    try {
      await fbAdsUpdateAuthTokenLabel(auth.id, trimmed);
      setEditing(false);
      onChanged();
    } catch (e) {
      setRowError((e as Error).message ?? String(e));
    }
  };

  const handleDelete = async () => {
    if (
      !confirm(
        `Xóa token "${auth.label}"? Ad Account đã lưu vẫn dùng được (mỗi account có token riêng).`,
      )
    )
      return;
    try {
      await fbAdsDeleteAuthToken(auth.id);
      onChanged();
    } catch (e) {
      setRowError((e as Error).message ?? String(e));
    }
  };

  return (
    <li
      className="rounded-lg border bg-surface-2 px-3 py-2"
      style={{
        borderColor: color.border,
        borderLeftWidth: 4,
        background: `linear-gradient(to right, ${color.bg}, transparent 60%)`,
      }}
    >
      <div className="flex items-center gap-3">
        <span className="material-symbols-rounded text-base text-amber-300">
          key
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            {editing ? (
              <input
                value={labelDraft}
                onChange={(e) => setLabelDraft(e.currentTarget.value)}
                onBlur={() => void handleSaveLabel()}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleSaveLabel();
                  else if (e.key === "Escape") {
                    setEditing(false);
                    setLabelDraft(auth.label);
                  }
                }}
                autoFocus
                className="min-w-0 flex-1 rounded border border-violet-500/40 bg-surface-1 px-1.5 py-0.5 text-sm text-white/90 focus:outline-none"
              />
            ) : (
              <button
                type="button"
                onClick={() => {
                  setLabelDraft(auth.label);
                  setEditing(true);
                }}
                className="truncate rounded px-1 text-left text-sm font-medium text-white/90 hover:bg-white/5"
                title="Bấm để đổi tên"
              >
                {auth.label}
              </button>
            )}
            <span
              className="shrink-0 rounded-full px-1.5 py-0.5 font-mono text-[10px] text-white/85"
              style={{ background: color.dot }}
              title={`Token group #${auth.tokenHash}`}
            >
              #{auth.tokenHash}
            </span>
            {auth.expired && (
              <span className="shrink-0 rounded-full border border-red-500/40 bg-red-500/15 px-1.5 py-0.5 text-[10px] text-red-200">
                Đã hết hạn
              </span>
            )}
          </div>
          <div className="text-[11px] text-white/40">
            Lưu lúc {new Date(auth.addedAtMs).toLocaleString()}
          </div>
        </div>
        <button
          type="button"
          onClick={() => void handleCopy()}
          className={`flex shrink-0 items-center gap-1.5 rounded-lg border px-2.5 py-1 text-xs font-medium transition-colors ${
            copied
              ? "border-emerald-500/50 bg-emerald-500/15 text-emerald-200"
              : "border-violet-500/40 bg-violet-500/10 text-violet-200 hover:bg-violet-500/20"
          }`}
          title="Copy User Token vào clipboard"
        >
          <span className="material-symbols-rounded text-sm">
            {copied ? "check" : "content_copy"}
          </span>
          {copied ? "Đã copy" : "Copy token"}
        </button>
        <button
          type="button"
          onClick={() => void handleToggle()}
          disabled={loading}
          className="flex h-7 w-7 items-center justify-center rounded-full text-violet-200 hover:bg-violet-500/20 disabled:opacity-50"
          title={revealed ? "Ẩn token" : "Hiện token"}
        >
          <span className="material-symbols-rounded text-base">
            {loading ? "hourglass_empty" : revealed ? "lock_open" : "lock"}
          </span>
        </button>
        <button
          type="button"
          onClick={() => void handleDelete()}
          className="flex h-7 w-7 items-center justify-center rounded-full text-red-300 hover:bg-red-500/20"
          title="Xóa token"
        >
          <span className="material-symbols-rounded text-base">delete</span>
        </button>
      </div>

      {token && (
        <div className="mt-2 flex items-start gap-2 rounded-md border border-surface-8 bg-surface-1 px-2 py-1.5">
          <span
            className={`material-symbols-rounded shrink-0 text-sm ${
              revealed ? "text-amber-300" : "text-white/45"
            }`}
          >
            key
          </span>
          <code
            className={`min-w-0 flex-1 break-all font-mono text-[11px] ${
              revealed ? "text-amber-100" : "text-white/50"
            }`}
            style={{ userSelect: revealed ? "all" : "none" }}
          >
            {revealed ? token : maskToken(token)}
          </code>
          <button
            type="button"
            onClick={() => void handleCopy()}
            className={`flex shrink-0 items-center gap-1 rounded px-1.5 py-0.5 text-[11px] ${
              copied
                ? "bg-emerald-500/15 text-emerald-200"
                : "text-violet-200 hover:bg-violet-500/15"
            }`}
            title="Copy token"
          >
            <span className="material-symbols-rounded text-sm">
              {copied ? "check" : "content_copy"}
            </span>
            {copied ? "Đã copy" : "Copy"}
          </button>
        </div>
      )}

      {rowError && (
        <div className="mt-1.5 text-[11px] text-red-300">{rowError}</div>
      )}
    </li>
  );
}
