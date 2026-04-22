import { useState } from "react";
import { useAuth } from "../contexts/AuthContext";
import { useIsAdmin } from "../hooks/usePremium";

interface UserMenuProps {
  /// Override signOut flow — App.tsx pass callback để check dirty DB + hỏi
  /// sync trước khi signOut. Nếu bỏ, dùng `auth.signOut` trực tiếp (không check).
  onRequestSignOut?: () => Promise<void> | void;
}

export function UserMenu({ onRequestSignOut }: UserMenuProps = {}) {
  const { user, signOut } = useAuth();
  const isAdmin = useIsAdmin();
  const [open, setOpen] = useState(false);
  const [signingOut, setSigningOut] = useState(false);

  if (!user) return null;

  const email = user.email ?? "";
  const initial = (email[0] ?? "?").toUpperCase();

  const handleSignOut = async () => {
    if (signingOut) return;
    setSigningOut(true);
    try {
      if (onRequestSignOut) {
        await onRequestSignOut();
      } else {
        await signOut();
      }
    } finally {
      setSigningOut(false);
      setOpen(false);
    }
  };

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full bg-white/15 text-sm font-semibold text-white hover:bg-white/25"
        title={email}
        aria-label="Tài khoản"
      >
        {initial}
      </button>

      {open && (
        <>
          <div
            className="fixed inset-0 z-30"
            onClick={() => setOpen(false)}
          />
          <div className="absolute right-0 top-full z-40 mt-1 w-64 overflow-hidden rounded-lg border border-surface-8 bg-surface-2 shadow-elev-16">
            <div className="border-b border-surface-8 px-4 py-3">
              <div className="mb-1 flex items-center gap-2">
                <span className="material-symbols-rounded text-base text-white/50">
                  account_circle
                </span>
                <span className="text-xs font-medium text-white/50">
                  Đăng nhập với
                </span>
              </div>
              <div
                className="truncate text-sm text-white/90"
                title={email}
              >
                {email}
              </div>
              {isAdmin && (
                <span className="mt-2 inline-block rounded-full bg-shopee-900/60 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-shopee-300">
                  Admin
                </span>
              )}
            </div>
            <button
              type="button"
              onClick={() => void handleSignOut()}
              disabled={signingOut}
              className="btn-ripple flex w-full items-center gap-2 px-4 py-2.5 text-left text-sm text-red-300 hover:bg-red-900/30 disabled:opacity-50"
            >
              <span className="material-symbols-rounded text-base">logout</span>
              {signingOut ? "Đang đăng xuất..." : "Đăng xuất"}
            </button>
          </div>
        </>
      )}
    </div>
  );
}
