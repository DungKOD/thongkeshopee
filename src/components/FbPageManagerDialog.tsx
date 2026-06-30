import { useCallback, useEffect, useState } from "react";
import { createPortal } from "react-dom";
import {
  fbDeleteAuthToken,
  fbDeletePage,
  fbGetAuthToken,
  fbGetPageToken,
  fbListAuthTokens,
  fbSaveAuthToken,
  fbSavePages,
  fbUpdateAuthTokenLabel,
  fbValidateToken,
  type FbAuthToken,
  type FbPage,
  type FbPageWithToken,
} from "../lib/fbReels";
import { emitTokensChanged } from "../lib/tokenEvents";

interface FbPageManagerDialogProps {
  isOpen: boolean;
  savedPages: FbPage[];
  onClose: () => void;
  onChanged: () => void;
}

type Phase = "input" | "validating" | "pick";

export function FbPageManagerDialog({
  isOpen,
  savedPages,
  onClose,
  onChanged,
}: FbPageManagerDialogProps) {
  const [token, setToken] = useState("");
  const [phase, setPhase] = useState<Phase>("input");
  const [discovered, setDiscovered] = useState<FbPageWithToken[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [authTokens, setAuthTokens] = useState<FbAuthToken[]>([]);
  const [successMsg, setSuccessMsg] = useState<string | null>(null);

  const refreshAuthTokens = useCallback(async () => {
    try {
      const list = await fbListAuthTokens();
      setAuthTokens(list);
    } catch {
      // Silently ignore — bảng có thể chưa init nếu workspace mới chuyển.
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
      const pages = await fbValidateToken(token.trim());
      if (pages.length === 0) {
        setError("Token hợp lệ nhưng không tìm thấy Page nào");
        setPhase("input");
        return;
      }
      setDiscovered(pages);
      setSelected(new Set(pages.map((p) => p.pageId)));
      setPhase("pick");
    } catch (e) {
      setError((e as Error).message ?? String(e));
      setPhase("input");
    }
  };

  const handleSave = async () => {
    const toSave = discovered.filter((p) => selected.has(p.pageId));
    if (toSave.length === 0) {
      setError("Chọn ít nhất 1 Page để lưu");
      return;
    }
    setSaving(true);
    try {
      // Lưu auth token (User Token gốc) song song với pages — dedupe theo
      // hash backend, paste lại cùng token không sinh row mới.
      await fbSaveAuthToken(token.trim());
      await fbSavePages(toSave);
      await refreshAuthTokens();
      emitTokensChanged("fb_page");
      emitTokensChanged("fb_user");
      onChanged();
      // KHÔNG đóng dialog — reset form để user paste token tiếp theo (vd 2
      // account FB Business + Personal). Hiện success message rồi auto-clear.
      setToken("");
      setDiscovered([]);
      setSelected(new Set());
      setPhase("input");
      setError(null);
      setSuccessMsg(`Đã lưu ${toSave.length} Page + 1 token xác thực`);
      window.setTimeout(() => setSuccessMsg(null), 4000);
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleAuthChanged = async () => {
    await refreshAuthTokens();
  };

  const handleDelete = async (pageId: string) => {
    if (!confirm("Xóa Page này khỏi app? Lịch sử đăng vẫn được giữ.")) return;
    try {
      await fbDeletePage(pageId);
      emitTokensChanged("fb_page");
      onChanged();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    }
  };

  const toggle = (pageId: string) => {
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(pageId)) next.delete(pageId);
      else next.add(pageId);
      return next;
    });
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
          <span className="material-symbols-rounded text-2xl text-blue-400">
            badge
          </span>
          <h2 className="text-lg font-semibold text-white/90">
            Quản lý Facebook Page
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
                  title="User Token user paste vào ô 'Xác thực token' — phân biệt với Page Token. 1 User Token quản nhiều Page."
                >
                  · (User Token)
                </span>
              </h3>
              <ul className="space-y-1.5">
                {authTokens.map((t) => (
                  <SavedAuthTokenRow
                    key={t.id}
                    auth={t}
                    onChanged={() => void handleAuthChanged()}
                  />
                ))}
              </ul>
            </section>
          )}

          {savedPages.length > 0 && (
            <section>
              <h3 className="mb-2 text-sm font-semibold text-white/80">
                Page đã lưu ({savedPages.length})
              </h3>
              <ul className="space-y-1.5">
                {savedPages.map((p) => (
                  <SavedPageRow
                    key={p.pageId}
                    page={p}
                    onDelete={() => void handleDelete(p.pageId)}
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
              {authTokens.length > 0 || savedPages.length > 0
                ? "Thêm token / Page khác"
                : "Thêm Page lần đầu"}
            </h3>
            {(authTokens.length > 0 || savedPages.length > 0) && (
              <p className="mb-3 text-[11px] text-white/55">
                Bạn có thể thêm nhiều token xác thực — paste token tiếp theo
                rồi xác thực để gộp thêm Page vào danh sách trên.
              </p>
            )}
            <details className="mb-3 rounded-lg border border-blue-500/30 bg-blue-950/30 px-3 py-2 text-xs text-white/75">
              <summary className="cursor-pointer font-medium text-blue-200">
                Hướng dẫn lấy Page Access Token (vĩnh viễn)
              </summary>

              <div className="mt-3 space-y-3">
                <div className="rounded-md border border-amber-500/30 bg-amber-950/20 px-2 py-1.5 text-[11px] text-amber-100">
                  <span className="font-semibold">Mục tiêu:</span> lấy Page
                  Token <em>không hết hạn</em>. Có 3 bước: tạo token ngắn hạn →
                  đổi User token 60 ngày → đổi Page Token vĩnh viễn.
                </div>

                <div>
                  <p className="font-semibold text-blue-200">
                    Bước 1 — Token ngắn hạn (~1-2h)
                  </p>
                  <ol className="mt-1 list-decimal space-y-0.5 pl-5 leading-relaxed">
                    <li>
                      Mở{" "}
                      <a
                        href="https://developers.facebook.com/tools/explorer/"
                        target="_blank"
                        rel="noreferrer"
                        className="text-blue-300 underline hover:text-blue-200"
                      >
                        developers.facebook.com/tools/explorer
                      </a>
                    </li>
                    <li>Góc phải chọn App của bạn (loại Business)</li>
                    <li>
                      Permissions → add scopes:{" "}
                      <code className="rounded bg-surface-1 px-1">
                        pages_show_list
                      </code>
                      ,{" "}
                      <code className="rounded bg-surface-1 px-1">
                        pages_manage_posts
                      </code>
                      ,{" "}
                      <code className="rounded bg-surface-1 px-1">
                        pages_read_engagement
                      </code>
                    </li>
                    <li>
                      Bấm <em>Generate Access Token</em> → login, confirm quyền →
                      copy token ngắn hạn
                    </li>
                  </ol>
                </div>

                <div>
                  <p className="font-semibold text-blue-200">
                    Bước 2 — Đổi sang User Token 60 ngày
                  </p>
                  <ol className="mt-1 list-decimal space-y-0.5 pl-5 leading-relaxed">
                    <li>
                      Mở{" "}
                      <a
                        href="https://developers.facebook.com/tools/debug/accesstoken/"
                        target="_blank"
                        rel="noreferrer"
                        className="text-blue-300 underline hover:text-blue-200"
                      >
                        developers.facebook.com/tools/debug/accesstoken
                      </a>
                    </li>
                    <li>Paste token ngắn hạn → bấm <em>Debug</em></li>
                    <li>
                      Kéo xuống cuối trang → bấm{" "}
                      <em>Extend Access Token</em> → login lại nếu được hỏi
                    </li>
                    <li>
                      Token mới hiện ra ở dòng dưới cùng — đó là User Token 60
                      ngày. Paste vào Debugger lần nữa: dòng <em>Expires</em>{" "}
                      hiển thị ~60 ngày là đúng
                    </li>
                  </ol>
                </div>

                <div>
                  <p className="font-semibold text-emerald-200">
                    Bước 3 — Đổi Page Token vĩnh viễn (KHUYẾN NGHỊ)
                  </p>
                  <p className="mt-1 leading-relaxed">
                    User Token 60 ngày vẫn hết hạn. Để đăng Reels lâu dài, đổi
                    sang <strong>Page Token không hết hạn</strong>:
                  </p>
                  <ol className="mt-1 list-decimal space-y-0.5 pl-5 leading-relaxed">
                    <li>Mở URL sau trong browser (thay token vào):</li>
                  </ol>
                  <pre className="mt-1 overflow-x-auto rounded bg-surface-1 px-2 py-1.5 font-mono text-[10px] text-emerald-100">
                    https://graph.facebook.com/v21.0/me/accounts?access_token=&#123;USER_TOKEN_60_NGAY&#125;
                  </pre>
                  <ol
                    start={2}
                    className="mt-1 list-decimal space-y-0.5 pl-5 leading-relaxed"
                  >
                    <li>
                      Mỗi Page trả về 1 field{" "}
                      <code className="rounded bg-surface-1 px-1">
                        access_token
                      </code>{" "}
                      — đó là Page Token vĩnh viễn
                    </li>
                    <li>
                      Paste <em>Page Token đó</em> (không phải User Token) vào
                      app — không bao giờ phải làm lại
                    </li>
                  </ol>
                  <p className="mt-1 text-[11px] text-white/55">
                    💡 Mẹo: bạn cũng có thể paste thẳng User Token 60 ngày vào
                    app — app tự gọi <code>/me/accounts</code> và lấy Page Token
                    vĩnh viễn từ response.
                  </p>
                </div>
              </div>
            </details>

            {phase !== "pick" && (
              <>
                <textarea
                  value={token}
                  onChange={(e) => setToken(e.currentTarget.value)}
                  placeholder="EAAxxx... (paste long-lived Page hoặc User Access Token)"
                  rows={3}
                  className="w-full resize-y rounded-lg border border-surface-8 bg-surface-1 px-3 py-2 font-mono text-xs text-white/90 placeholder:text-white/30 focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                />
                <button
                  type="button"
                  onClick={() => void handleValidate()}
                  disabled={phase === "validating" || !token.trim()}
                  className="btn-ripple mt-2 flex w-full items-center justify-center gap-2 rounded-lg bg-blue-500 px-4 py-2 text-sm font-medium text-white hover:bg-blue-600 disabled:opacity-50"
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
                  Tìm thấy {discovered.length} Page — tick chọn Page nào để lưu:
                </p>
                <ul className="max-h-64 space-y-1 overflow-y-auto rounded-lg border border-surface-8 bg-surface-1 p-2">
                  {discovered.map((p) => (
                    <li key={p.pageId}>
                      <label className="flex cursor-pointer items-center gap-3 rounded-md px-2 py-1.5 hover:bg-white/5">
                        <input
                          type="checkbox"
                          checked={selected.has(p.pageId)}
                          onChange={() => toggle(p.pageId)}
                          className="h-4 w-4 accent-blue-500"
                        />
                        <div className="min-w-0 flex-1">
                          <div className="truncate text-sm text-white/90">
                            {p.name}
                          </div>
                          <div className="font-mono text-[11px] text-white/40">
                            ID: {p.pageId}
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
                    className="btn-ripple ml-auto flex items-center gap-2 rounded-lg bg-blue-500 px-5 py-2 text-sm font-medium text-white hover:bg-blue-600 disabled:opacity-50"
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
                        Lưu {selected.size} Page
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

interface SavedPageRowProps {
  page: FbPage;
  onDelete: () => void;
}

/// Mask token kiểu `EAAxxxx…xxxx` — giữ 4 ký tự đầu + 4 cuối để user xác nhận
/// đúng token cần copy, phần giữa hide để tránh shoulder-surfing.
function maskToken(token: string): string {
  if (token.length <= 12) return "•".repeat(token.length);
  return `${token.slice(0, 4)}${"•".repeat(16)}${token.slice(-4)}`;
}

/// Map token hash (8 hex từ backend) → HSL color deterministic. Pages cùng token
/// → cùng hash → cùng color. Hue lấy 2 byte đầu (16 bit) cho dải mịn; saturation
/// + lightness fix để các color đều dễ nhìn trên nền tối surface-2.
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

/// 1 row Page đã lưu — có toggle ổ khóa để hiển thị Access Token + nút Copy.
/// Token fetch on-demand từ backend (không prefetch để giảm risk leak qua React
/// devtools / event log), giữ trong state local, ẩn lại khi user bấm khóa.
function SavedPageRow({ page, onDelete }: SavedPageRowProps) {
  const [token, setToken] = useState<string | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [rowError, setRowError] = useState<string | null>(null);

  const handleToggle = async () => {
    setRowError(null);
    if (revealed) {
      // Khóa lại — wipe token khỏi state để không còn snapshot trong memory
      // (best-effort; GC sẽ collect sau).
      setRevealed(false);
      setToken(null);
      return;
    }
    // Mở khóa — fetch nếu chưa có.
    if (token) {
      setRevealed(true);
      return;
    }
    setLoading(true);
    try {
      const t = await fbGetPageToken(page.pageId);
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
      // Fetch fresh nếu chưa có hoặc đang khóa — copy không cần reveal UI.
      const value = token ?? (await fbGetPageToken(page.pageId));
      if (!token) setToken(value);
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (e) {
      setRowError(
        `Không copy được: ${(e as Error).message ?? String(e)}`,
      );
    }
  };

  const color = colorFromHash(page.tokenHash);

  return (
    <li
      className="rounded-lg border bg-surface-2 px-3 py-2"
      // Inline style cho color động — Tailwind không generate được class từ
      // runtime hash. Border-l dày để emphasize grouping mà không phá layout.
      style={{
        borderColor: color.border,
        borderLeftWidth: 4,
        background: `linear-gradient(to right, ${color.bg}, transparent 60%)`,
      }}
    >
      <div className="flex items-center gap-3">
        <span className="material-symbols-rounded text-base text-blue-300">
          public
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2">
            <span className="truncate text-sm font-medium text-white/90">
              {page.name}
            </span>
            <span
              className="shrink-0 rounded-full px-1.5 py-0.5 font-mono text-[10px] text-white/85"
              style={{ background: color.dot }}
              title={`Token group #${page.tokenHash} — page cùng màu = cùng access token`}
            >
              #{page.tokenHash}
            </span>
            {page.tokenExpired && (
              <span className="shrink-0 rounded-full border border-red-500/40 bg-red-500/15 px-1.5 py-0.5 text-[10px] text-red-200">
                Token hết hạn
              </span>
            )}
          </div>
          <div className="font-mono text-[11px] text-white/40">
            ID: {page.pageId}
          </div>
        </div>
        <button
          type="button"
          onClick={() => void handleCopy()}
          className={`flex shrink-0 items-center gap-1.5 rounded-lg border px-2.5 py-1 text-xs font-medium transition-colors ${
            copied
              ? "border-emerald-500/50 bg-emerald-500/15 text-emerald-200"
              : "border-blue-500/40 bg-blue-500/10 text-blue-200 hover:bg-blue-500/20"
          }`}
          title="Copy Access Token vào clipboard"
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
          className="flex h-7 w-7 items-center justify-center rounded-full text-blue-200 hover:bg-blue-500/20 disabled:opacity-50"
          title={revealed ? "Ẩn token" : "Hiện token"}
        >
          <span className="material-symbols-rounded text-base">
            {loading
              ? "hourglass_empty"
              : revealed
                ? "lock_open"
                : "lock"}
          </span>
        </button>
        <button
          type="button"
          onClick={onDelete}
          className="flex h-7 w-7 items-center justify-center rounded-full text-red-300 hover:bg-red-500/20"
          title="Xóa Page khỏi app"
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
            // select-all để user dễ kéo-chọn toàn bộ chuỗi khi reveal.
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
                : "text-blue-200 hover:bg-blue-500/15"
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

interface SavedAuthTokenRowProps {
  auth: FbAuthToken;
  onChanged: () => void;
}

/// 1 row User Token đã lưu — copy/reveal/rename/delete. Color theo `tokenHash`
/// (giống SavedPageRow) để user nhận diện ngay 2 row cùng token.
function SavedAuthTokenRow({ auth, onChanged }: SavedAuthTokenRowProps) {
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
      const t = await fbGetAuthToken(auth.id);
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
      const value = token ?? (await fbGetAuthToken(auth.id));
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
      await fbUpdateAuthTokenLabel(auth.id, trimmed);
      setEditing(false);
      onChanged();
    } catch (e) {
      setRowError((e as Error).message ?? String(e));
    }
  };

  const handleDelete = async () => {
    if (
      !confirm(
        `Xóa token "${auth.label}"? Page đã lưu vẫn hoạt động độc lập (mỗi Page có Page Token riêng).`,
      )
    )
      return;
    try {
      await fbDeleteAuthToken(auth.id);
      emitTokensChanged("fb_user");
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
                className="min-w-0 flex-1 rounded border border-blue-500/40 bg-surface-1 px-1.5 py-0.5 text-sm text-white/90 focus:outline-none"
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
              : "border-blue-500/40 bg-blue-500/10 text-blue-200 hover:bg-blue-500/20"
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
          className="flex h-7 w-7 items-center justify-center rounded-full text-blue-200 hover:bg-blue-500/20 disabled:opacity-50"
          title={revealed ? "Ẩn token" : "Hiện token"}
        >
          <span className="material-symbols-rounded text-base">
            {loading
              ? "hourglass_empty"
              : revealed
                ? "lock_open"
                : "lock"}
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
                : "text-blue-200 hover:bg-blue-500/15"
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
