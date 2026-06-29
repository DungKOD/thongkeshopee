import { useCallback, useEffect, useState } from "react";
import { createPortal } from "react-dom";
import {
  tokenListFbUserTokens,
  tokenSummary,
  type TokenSummary,
  type UnifiedFbToken,
} from "../lib/tokenManager";
import {
  fbDeleteAuthToken,
  fbGetAuthToken,
  fbListPages,
  type FbPage,
} from "../lib/fbReels";
import {
  fbAdsDeleteAuthToken,
  fbAdsGetAuthToken,
  fbAdsListAccounts,
  type FbAdAccount,
} from "../lib/fbAds";
import {
  shopeeAffGetStatus,
  type ShopeeAffStatus,
} from "../lib/shopeeAffiliate";
import { FbPageManagerDialog } from "./FbPageManagerDialog";
import { FbAdAccountManagerDialog } from "./FbAdAccountManagerDialog";

interface TokenManagerDialogProps {
  isOpen: boolean;
  onClose: () => void;
}

/// Token Manager — UI duy nhất gom tất cả token đang lưu trong app.
///
/// Layout:
///   ┌─────────────────────────────────────────┐
///   │ Header: Token Manager                   │
///   ├─────────────────────────────────────────┤
///   │ Section: FB User Tokens (unified list)  │
///   │   - 1 token cho cả Reels + Ads = 1 dòng │
///   │   - Badge nguồn (reels/ads/both)         │
///   ├─────────────────────────────────────────┤
///   │ Card grid: 3 feature cards               │
///   │   [FB Pages] [FB Ad Accounts] [Shopee]   │
///   │   Click → mở dialog chi tiết tương ứng   │
///   └─────────────────────────────────────────┘
///
/// Mỗi card click sẽ mở existing dialog (FbPageManagerDialog,
/// FbAdAccountManagerDialog, Shopee login flow) → giữ UX cũ đã polished
/// thay vì rebuild from scratch. TokenManager chỉ làm INDEX + entry point.
export function TokenManagerDialog({ isOpen, onClose }: TokenManagerDialogProps) {
  const [summary, setSummary] = useState<TokenSummary | null>(null);
  const [unifiedTokens, setUnifiedTokens] = useState<UnifiedFbToken[]>([]);
  const [shopeeStatus, setShopeeStatus] = useState<ShopeeAffStatus | null>(null);
  const [fbPages, setFbPages] = useState<FbPage[]>([]);
  const [fbAdAccounts, setFbAdAccounts] = useState<FbAdAccount[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  // Sub-dialog open flags — khi user click card, mở existing dialog.
  const [showPagesDialog, setShowPagesDialog] = useState(false);
  const [showAccountsDialog, setShowAccountsDialog] = useState(false);

  const refresh = useCallback(async () => {
    setRefreshing(true);
    setLoadError(null);
    try {
      // Parallel fetch — tất cả command đều idempotent, không race.
      const [s, tokens, pages, accounts, shopee] = await Promise.all([
        tokenSummary().catch(() => null),
        tokenListFbUserTokens().catch(() => []),
        fbListPages().catch(() => []),
        fbAdsListAccounts().catch(() => []),
        shopeeAffGetStatus().catch(() => null),
      ]);
      setSummary(s);
      setUnifiedTokens(tokens);
      setFbPages(pages);
      setFbAdAccounts(accounts);
      setShopeeStatus(shopee);
    } catch (e) {
      setLoadError((e as Error).message ?? String(e));
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    if (isOpen) void refresh();
  }, [isOpen, refresh]);

  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !showPagesDialog && !showAccountsDialog) {
        onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose, showPagesDialog, showAccountsDialog]);

  if (!isOpen) return null;

  return createPortal(
    <>
      <div
        className="fixed inset-0 z-40 flex items-center justify-center bg-black/70 p-4"
        onMouseDown={(e) => e.target === e.currentTarget && onClose()}
      >
        <div
          className="w-full max-w-3xl overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
          role="dialog"
          aria-modal="true"
        >
          <header className="flex items-center gap-3 border-b border-surface-8 px-6 py-4">
            <span className="material-symbols-rounded text-2xl text-amber-300">
              key
            </span>
            <h2 className="text-lg font-semibold text-white/90">
              Token Manager
            </h2>
            <p className="hidden text-xs text-white/45 sm:block">
              Quản lý tất cả token & thông tin đăng nhập tại 1 chỗ
            </p>
            <button
              type="button"
              onClick={() => void refresh()}
              disabled={refreshing}
              className="ml-auto flex h-8 w-8 items-center justify-center rounded-full text-white/60 hover:bg-white/10 hover:text-white disabled:opacity-50"
              title="Tải lại"
              aria-label="Tải lại"
            >
              <span
                className={`material-symbols-rounded ${refreshing ? "animate-spin" : ""}`}
              >
                refresh
              </span>
            </button>
            <button
              type="button"
              onClick={onClose}
              className="flex h-8 w-8 items-center justify-center rounded-full text-white/60 hover:bg-white/10 hover:text-white"
              aria-label="Đóng"
            >
              <span className="material-symbols-rounded">close</span>
            </button>
          </header>

          <div className="max-h-[75vh] space-y-5 overflow-y-auto px-6 py-5">
            {loadError && (
              <div className="rounded-lg border border-red-500/40 bg-red-950/30 px-3 py-2 text-xs text-red-200">
                {loadError}
              </div>
            )}

            {/* Section: FB User Tokens unified */}
            <section>
              <div className="mb-2 flex items-center gap-2">
                <h3 className="text-sm font-semibold text-white/85">
                  FB User Tokens
                </h3>
                <span className="text-[11px] text-white/45">
                  ({unifiedTokens.length} token unique — dùng cho Reels + Ads)
                </span>
              </div>

              {unifiedTokens.length === 0 ? (
                <div className="rounded-lg border border-dashed border-surface-8 bg-surface-1 px-4 py-6 text-center text-xs text-white/55">
                  Chưa có User Token nào. Mở "Quản lý FB Page" hoặc "Quản lý
                  FB Ad Account" bên dưới để paste token đầu tiên.
                </div>
              ) : (
                <ul className="space-y-1.5">
                  {unifiedTokens.map((t) => (
                    <UnifiedTokenRow
                      key={`${t.reelsId ?? "x"}-${t.adsId ?? "x"}-${t.tokenHash}`}
                      token={t}
                      onChanged={() => void refresh()}
                    />
                  ))}
                </ul>
              )}
            </section>

            <hr className="border-surface-8" />

            {/* Card grid: 3 feature entry points */}
            <section>
              <h3 className="mb-2 text-sm font-semibold text-white/85">
                Quản lý chi tiết theo feature
              </h3>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                <FeatureCard
                  icon="public"
                  iconColor="text-blue-300"
                  title="FB Pages"
                  countLabel={`${fbPages.length} Page`}
                  warning={
                    summary && summary.fbPagesExpired > 0
                      ? `${summary.fbPagesExpired} hết hạn`
                      : null
                  }
                  description="Page token vĩnh viễn, dùng để đăng Reels"
                  onClick={() => setShowPagesDialog(true)}
                />
                <FeatureCard
                  icon="ads_click"
                  iconColor="text-orange-300"
                  title="FB Ad Accounts"
                  countLabel={`${fbAdAccounts.length} account`}
                  warning={null}
                  description="Ad account token cho Bulk Camp"
                  onClick={() => setShowAccountsDialog(true)}
                />
                <FeatureCard
                  icon="shopping_bag"
                  iconColor="text-emerald-300"
                  title="Shopee Affiliate"
                  countLabel={
                    shopeeStatus?.hasCookies
                      ? `${shopeeStatus.cookieCount} cookies`
                      : "Chưa đăng nhập"
                  }
                  warning={
                    !shopeeStatus?.hasCookies ? "Cần đăng nhập" : null
                  }
                  description="Cookies session để convert affiliate link"
                  onClick={() => {
                    // Shopee không có dialog riêng — chuyển tab về Shopee Affiliate
                    // page. User dùng nút "Đăng nhập" ở đó.
                    alert(
                      "Đăng nhập Shopee tại tab 'Sản phẩm Shopee' → button 'Đăng nhập Shopee Affiliate'.",
                    );
                  }}
                />
              </div>
            </section>
          </div>
        </div>
      </div>

      {/* Sub-dialogs — mở khi user click card. Reuse existing dialogs để
          giữ UX paste/validate/pick đã polished kỹ trong v0.13. */}
      <FbPageManagerDialog
        isOpen={showPagesDialog}
        savedPages={fbPages}
        onClose={() => {
          setShowPagesDialog(false);
          void refresh();
        }}
        onChanged={() => void refresh()}
      />
      <FbAdAccountManagerDialog
        isOpen={showAccountsDialog}
        savedAccounts={fbAdAccounts}
        onClose={() => {
          setShowAccountsDialog(false);
          void refresh();
        }}
        onChanged={() => void refresh()}
      />
    </>,
    document.body,
  );
}

interface FeatureCardProps {
  icon: string;
  iconColor: string;
  title: string;
  countLabel: string;
  warning: string | null;
  description: string;
  onClick: () => void;
}

function FeatureCard({
  icon,
  iconColor,
  title,
  countLabel,
  warning,
  description,
  onClick,
}: FeatureCardProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="group relative flex flex-col gap-1 rounded-xl border border-surface-8 bg-surface-2 px-4 py-3 text-left transition-colors hover:border-blue-500/40 hover:bg-surface-1"
    >
      <div className="flex items-center gap-2">
        <span
          className={`material-symbols-rounded text-2xl ${iconColor}`}
          aria-hidden
        >
          {icon}
        </span>
        <span className="text-sm font-semibold text-white/90">{title}</span>
      </div>
      <div className="text-xs text-white/65">{countLabel}</div>
      {warning && (
        <div className="rounded-full bg-red-500/15 px-2 py-0.5 text-[10px] font-medium text-red-200">
          ⚠ {warning}
        </div>
      )}
      <div className="text-[11px] text-white/40">{description}</div>
      <span
        className="absolute right-3 top-3 text-white/30 transition-colors group-hover:text-white/70"
        aria-hidden
      >
        <span className="material-symbols-rounded text-base">
          chevron_right
        </span>
      </span>
    </button>
  );
}

interface UnifiedTokenRowProps {
  token: UnifiedFbToken;
  onChanged: () => void;
}

/// Mask token kiểu `EAAxxxx…xxxx` cho hiển thị.
function maskToken(token: string): string {
  if (token.length <= 12) return "•".repeat(token.length);
  return `${token.slice(0, 4)}${"•".repeat(16)}${token.slice(-4)}`;
}

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

function UnifiedTokenRow({ token, onChanged }: UnifiedTokenRowProps) {
  const [revealed, setRevealed] = useState(false);
  const [rawToken, setRawToken] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [rowError, setRowError] = useState<string | null>(null);

  const color = colorFromHash(token.tokenHash);

  /// Fetch raw token — ưu tiên Reels DB (cùng token cả 2 bảng nên không quan
  /// trọng). Nếu chỉ ở Ads, fallback Ads.
  const fetchRaw = useCallback(async (): Promise<string> => {
    if (token.reelsId !== null) {
      return await fbGetAuthToken(token.reelsId);
    }
    if (token.adsId !== null) {
      return await fbAdsGetAuthToken(token.adsId);
    }
    throw new Error("Token không có ID hợp lệ ở cả 2 bảng");
  }, [token.adsId, token.reelsId]);

  const handleToggle = async () => {
    setRowError(null);
    if (revealed) {
      setRevealed(false);
      setRawToken(null);
      return;
    }
    if (rawToken) {
      setRevealed(true);
      return;
    }
    setLoading(true);
    try {
      const t = await fetchRaw();
      setRawToken(t);
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
      const value = rawToken ?? (await fetchRaw());
      if (!rawToken) setRawToken(value);
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch (e) {
      setRowError(`Không copy được: ${(e as Error).message ?? String(e)}`);
    }
  };

  /// Delete: xóa khỏi CẢ 2 bảng nếu source = "both", còn lại chỉ xóa 1 bảng.
  /// Khi xóa, Page/Ad Account đã save vẫn còn (Page Token / Account Token độc
  /// lập với User Token).
  const handleDelete = async () => {
    if (
      !confirm(
        `Xóa token "${token.label}"?\n\nPage / Ad Account đã lưu vẫn dùng được vì mỗi cái có token riêng — chỉ ảnh hưởng khi cần discover thêm.`,
      )
    )
      return;
    setRowError(null);
    try {
      if (token.reelsId !== null) {
        await fbDeleteAuthToken(token.reelsId);
      }
      if (token.adsId !== null) {
        await fbAdsDeleteAuthToken(token.adsId);
      }
      onChanged();
    } catch (e) {
      setRowError((e as Error).message ?? String(e));
    }
  };

  /// Badge nguồn — "Reels" / "Ads" / "Cả 2".
  const sourceBadge = (() => {
    if (token.source === "both") {
      return {
        label: "Cả 2",
        cls: "border-emerald-500/40 bg-emerald-500/15 text-emerald-200",
        title: "Token này có scope dùng được cho cả Reels (Page) lẫn Ads (Ad Account)",
      };
    }
    if (token.source === "reels") {
      return {
        label: "Reels",
        cls: "border-blue-500/40 bg-blue-500/15 text-blue-200",
        title: "Token chỉ dùng cho FB Reels / quản lý Page",
      };
    }
    return {
      label: "Ads",
      cls: "border-orange-500/40 bg-orange-500/15 text-orange-200",
      title: "Token chỉ dùng cho FB Ads / Bulk Camp",
    };
  })();

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
            <span className="truncate text-sm font-medium text-white/90">
              {token.label}
            </span>
            <span
              className={`shrink-0 rounded-full border px-1.5 py-0.5 text-[10px] font-medium ${sourceBadge.cls}`}
              title={sourceBadge.title}
            >
              {sourceBadge.label}
            </span>
            <span
              className="shrink-0 rounded-full px-1.5 py-0.5 font-mono text-[10px] text-white/85"
              style={{ background: color.dot }}
              title={`Token group #${token.tokenHash}`}
            >
              #{token.tokenHash}
            </span>
            {token.expired && (
              <span className="shrink-0 rounded-full border border-red-500/40 bg-red-500/15 px-1.5 py-0.5 text-[10px] text-red-200">
                Hết hạn
              </span>
            )}
          </div>
          <div className="text-[11px] text-white/40">
            Lưu lúc {new Date(token.addedAtMs).toLocaleString()}
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
          {copied ? "Đã copy" : "Copy"}
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

      {rawToken && (
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
            {revealed ? rawToken : maskToken(rawToken)}
          </code>
        </div>
      )}

      {rowError && (
        <div className="mt-1.5 text-[11px] text-red-300">{rowError}</div>
      )}
    </li>
  );
}
