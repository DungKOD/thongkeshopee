import { useEffect, useState } from "react";
import { createPortal } from "react-dom";
import {
  fbDeletePage,
  fbSavePages,
  fbValidateToken,
  type FbPage,
  type FbPageWithToken,
} from "../lib/fbReels";

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
      await fbSavePages(toSave);
      onChanged();
      onClose();
    } catch (e) {
      setError((e as Error).message ?? String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (pageId: string) => {
    if (!confirm("Xóa Page này khỏi app? Lịch sử đăng vẫn được giữ.")) return;
    try {
      await fbDeletePage(pageId);
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
          {savedPages.length > 0 && (
            <section>
              <h3 className="mb-2 text-sm font-semibold text-white/80">
                Page đã lưu ({savedPages.length})
              </h3>
              <ul className="space-y-1.5">
                {savedPages.map((p) => (
                  <li
                    key={p.pageId}
                    className="flex items-center gap-3 rounded-lg border border-surface-8 bg-surface-2 px-3 py-2"
                  >
                    <span className="material-symbols-rounded text-base text-blue-300">
                      public
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-sm font-medium text-white/90">
                        {p.name}
                      </div>
                      <div className="font-mono text-[11px] text-white/40">
                        ID: {p.pageId}
                      </div>
                    </div>
                    <button
                      type="button"
                      onClick={() => void handleDelete(p.pageId)}
                      className="flex h-7 w-7 items-center justify-center rounded-full text-red-300 hover:bg-red-500/20"
                      title="Xóa Page khỏi app"
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
              {savedPages.length > 0 ? "Thêm Page mới" : "Thêm Page lần đầu"}
            </h3>
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
