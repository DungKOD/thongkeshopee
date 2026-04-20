import { useState } from "react";
import { useAuth } from "../contexts/AuthContext";
import { DevCredit } from "./DevCredit";

type Mode = "signin" | "signup";

function parseAuthError(err: unknown): string {
  const msg = err instanceof Error ? err.message : String(err);
  if (msg.includes("auth/invalid-credential") || msg.includes("auth/wrong-password"))
    return "Email hoặc mật khẩu không đúng";
  if (msg.includes("auth/user-not-found")) return "Tài khoản không tồn tại";
  if (msg.includes("auth/email-already-in-use")) return "Email đã đăng ký";
  if (msg.includes("auth/weak-password")) return "Mật khẩu tối thiểu 6 ký tự";
  if (msg.includes("auth/invalid-email")) return "Email không hợp lệ";
  if (msg.includes("auth/popup-closed-by-user")) return "Đã hủy đăng nhập Google";
  if (msg.includes("auth/network-request-failed"))
    return "Mất kết nối mạng — kiểm tra lại internet";
  return msg;
}

export function LoginScreen() {
  const { signInWithGoogle, signInWithEmail, signUpWithEmail } = useAuth();
  const [mode, setMode] = useState<Mode>("signin");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleGoogle = async () => {
    setError(null);
    setSubmitting(true);
    try {
      await signInWithGoogle();
    } catch (e) {
      setError(parseAuthError(e));
    } finally {
      setSubmitting(false);
    }
  };

  const handleEmailSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      if (mode === "signin") await signInWithEmail(email, password);
      else await signUpWithEmail(email, password);
    } catch (err) {
      setError(parseAuthError(err));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <main className="flex min-h-screen flex-col items-center justify-center bg-gradient-to-br from-surface-0 via-surface-1 to-shopee-900/30 p-6">
      <div className="w-full max-w-sm rounded-2xl border border-surface-8 bg-surface-1 p-8 shadow-elev-16">
        <div className="mb-6 text-center">
          <img
            src="/shopee.png"
            alt="Shopee"
            className="mx-auto h-16 w-16 rounded-xl object-contain shadow-elev-4"
            draggable={false}
          />
          <h1 className="mt-3 text-xl font-semibold text-white/90">
            Thống kê Shopee Affiliate
          </h1>
          <p className="mt-1 text-sm text-white/60">
            {mode === "signin" ? "Đăng nhập để tiếp tục" : "Tạo tài khoản mới"}
          </p>
        </div>

        <button
          type="button"
          onClick={handleGoogle}
          disabled={submitting}
          className="btn-ripple mb-4 flex w-full items-center justify-center gap-2 rounded-lg border border-surface-8 bg-surface-2 px-4 py-2.5 text-sm font-medium text-white/90 hover:bg-surface-4 disabled:opacity-50"
        >
          <GoogleIcon />
          Tiếp tục với Google
        </button>

        <div className="my-4 flex items-center gap-3 text-xs text-white/40">
          <span className="h-px flex-1 bg-surface-8" />
          hoặc
          <span className="h-px flex-1 bg-surface-8" />
        </div>

        <form onSubmit={handleEmailSubmit} className="flex flex-col gap-3">
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.currentTarget.value)}
            placeholder="Email"
            autoComplete="email"
            required
            disabled={submitting}
            className="rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90 placeholder:text-white/30 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500 disabled:opacity-50"
          />
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.currentTarget.value)}
            placeholder="Mật khẩu"
            autoComplete={mode === "signin" ? "current-password" : "new-password"}
            required
            minLength={6}
            disabled={submitting}
            className="rounded-lg border border-surface-8 bg-surface-2 px-3 py-2 text-sm text-white/90 placeholder:text-white/30 focus:border-shopee-500 focus:outline-none focus:ring-1 focus:ring-shopee-500 disabled:opacity-50"
          />
          {error && (
            <div className="rounded-md border border-red-500/40 bg-red-900/30 px-3 py-2 text-xs text-red-200">
              {error}
            </div>
          )}
          <button
            type="submit"
            disabled={submitting}
            className="btn-ripple rounded-lg bg-shopee-500 px-4 py-2.5 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4 disabled:opacity-50"
          >
            {submitting
              ? "Đang xử lý..."
              : mode === "signin"
                ? "Đăng nhập"
                : "Đăng ký"}
          </button>
        </form>

        <button
          type="button"
          onClick={() => {
            setMode(mode === "signin" ? "signup" : "signin");
            setError(null);
          }}
          className="mt-4 block w-full text-center text-xs text-white/60 hover:text-shopee-300"
        >
          {mode === "signin"
            ? "Chưa có tài khoản? Đăng ký"
            : "Đã có tài khoản? Đăng nhập"}
        </button>

        <DevCredit />
      </div>
    </main>
  );
}

function GoogleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" aria-hidden="true">
      <path
        fill="#4285F4"
        d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 01-2.2 3.32v2.75h3.57c2.08-1.92 3.28-4.74 3.28-8.08z"
      />
      <path
        fill="#34A853"
        d="M12 23c2.97 0 5.46-.98 7.28-2.67l-3.57-2.75c-.99.66-2.25 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84A11 11 0 0012 23z"
      />
      <path
        fill="#FBBC05"
        d="M5.84 14.11a6.6 6.6 0 010-4.22V7.05H2.18a11 11 0 000 9.9l3.66-2.84z"
      />
      <path
        fill="#EA4335"
        d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1a11 11 0 00-9.82 6.05l3.66 2.84C6.71 7.31 9.14 5.38 12 5.38z"
      />
    </svg>
  );
}
