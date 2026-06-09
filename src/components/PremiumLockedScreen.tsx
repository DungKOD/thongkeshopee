import { useState } from "react";
import { useAuth } from "../contexts/AuthContext";

/**
 * Splash full-screen khi user đã login nhưng premium chưa active (premium=false
 * hoặc expiredAt đã qua). Hiển thị email + UID để user gửi cho admin upgrade,
 * kèm nút Đăng xuất.
 */
export function PremiumLockedScreen() {
  const { user, userProfile, signOut } = useAuth();
  const [copied, setCopied] = useState(false);
  const [signingOut, setSigningOut] = useState(false);

  // Phân biệt 2 reason để text rõ ràng hơn:
  // - expired: doc tồn tại + premium=true nhưng expiredAt < now
  // - notActivated: mọi case còn lại (doc null, premium=false, ...)
  const isExpired =
    userProfile?.premium === true &&
    userProfile.expiredAt !== null &&
    userProfile.expiredAt.toMillis() <= Date.now();

  const uid = user?.uid ?? "";
  const email = user?.email ?? userProfile?.email ?? "";

  const handleCopyUid = async () => {
    if (!uid) return;
    try {
      await navigator.clipboard.writeText(uid);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch (e) {
      console.warn("[PremiumLockedScreen] copy UID failed:", e);
    }
  };

  const handleSignOut = async () => {
    setSigningOut(true);
    try {
      await signOut();
    } catch (e) {
      console.warn("[PremiumLockedScreen] signOut failed:", e);
      setSigningOut(false);
    }
  };

  return (
    <main className="min-h-screen bg-surface-0 px-6 text-white">
      <div className="mx-auto flex max-w-lg flex-col items-center pt-[18vh]">
        <span className="material-symbols-rounded text-7xl text-amber-400">
          {isExpired ? "schedule" : "lock"}
        </span>

        <h1 className="mt-5 text-center text-2xl font-semibold text-white/95">
          {isExpired
            ? "Phiên premium đã hết hạn"
            : "Tài khoản chưa được kích hoạt"}
        </h1>

        <p className="mt-3 max-w-md text-center text-base leading-relaxed text-white/70">
          {isExpired ? (
            <>
              Gói premium của bạn đã kết thúc. Liên hệ admin để gia hạn và tiếp
              tục sử dụng app.
            </>
          ) : (
            <>
              Tài khoản này chưa được cấp quyền premium. Vui lòng liên hệ admin,
              gửi <span className="font-semibold text-amber-300">UID</span> bên
              dưới để được kích hoạt.
            </>
          )}
        </p>

        <div className="mt-7 w-full max-w-md rounded-2xl border border-white/10 bg-surface-1 p-5 shadow-elev-2">
          <div className="space-y-3 text-sm">
            <div>
              <div className="text-xs uppercase tracking-wide text-white/40">
                Email
              </div>
              <div className="mt-1 break-all text-white/90">
                {email || "—"}
              </div>
            </div>
            <div>
              <div className="text-xs uppercase tracking-wide text-white/40">
                UID
              </div>
              <div className="mt-1 flex items-center gap-2">
                <code className="flex-1 break-all rounded bg-surface-4 px-2 py-1 font-mono text-xs text-white/85">
                  {uid || "—"}
                </code>
                <button
                  type="button"
                  onClick={handleCopyUid}
                  className="btn-ripple flex shrink-0 items-center gap-1 rounded-lg border border-white/15 bg-white/5 px-3 py-1.5 text-xs font-medium text-white/85 hover:bg-white/10"
                  disabled={!uid}
                >
                  <span className="material-symbols-rounded text-base">
                    {copied ? "check" : "content_copy"}
                  </span>
                  {copied ? "Đã chép" : "Sao chép"}
                </button>
              </div>
            </div>
          </div>
        </div>

        <button
          type="button"
          onClick={handleSignOut}
          disabled={signingOut}
          className="btn-ripple mt-6 flex items-center gap-2 rounded-lg border border-white/20 bg-white/5 px-5 py-2.5 text-sm font-medium text-white/90 hover:bg-white/10 disabled:opacity-50"
        >
          <span className="material-symbols-rounded text-base">logout</span>
          {signingOut ? "Đang đăng xuất..." : "Đăng xuất"}
        </button>
      </div>
    </main>
  );
}
