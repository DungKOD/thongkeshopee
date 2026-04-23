import { useState } from "react";
import { useAuth } from "../contexts/AuthContext";

interface PaywallScreenProps {
  expiredAt: Date | null;
  reason: "inactive" | "expired";
}

export function PaywallScreen({ expiredAt, reason }: PaywallScreenProps) {
  const { user, signOut } = useAuth();
  const [copied, setCopied] = useState(false);

  const email = user?.email ?? "";
  const uid = user?.uid ?? "";
  const zalo = import.meta.env.VITE_ADMIN_ZALO || "";
  const zaloLink = zalo ? `https://zalo.me/${zalo.replace(/\D/g, "")}` : "";

  const copyText = `Email: ${email}\nUID: ${uid}`;

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(copyText);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // ignore clipboard errors
    }
  };

  return (
    <main className="flex min-h-screen items-center justify-center bg-gradient-to-br from-surface-0 via-surface-1 to-shopee-900/30 p-6">
      <div className="w-full max-w-md rounded-2xl border border-surface-8 bg-surface-1 p-8 shadow-elev-16">
        <div className="mb-6 text-center">
          <span className="material-symbols-rounded text-5xl text-shopee-400">
            workspace_premium
          </span>
          <h1 className="mt-2 text-xl font-semibold text-white/90">
            {reason === "expired"
              ? "Gói của bạn đã hết hạn"
              : "Tài khoản chưa được kích hoạt"}
          </h1>
          <p className="mt-1 text-sm text-white/60">
            Liên hệ admin qua Zalo để kích hoạt premium
          </p>
        </div>

        {zalo && (
          <a
            href={zaloLink}
            target="_blank"
            rel="noopener noreferrer"
            className="btn-ripple mb-4 flex items-center justify-center gap-2 rounded-lg border border-shopee-500/40 bg-shopee-900/20 p-4 text-shopee-200 transition hover:bg-shopee-900/40"
            title={`Mở Zalo ${zalo}`}
          >
            <span className="material-symbols-rounded text-xl">chat</span>
            <span className="text-sm font-medium">
              Liên hệ admin qua Zalo
            </span>
          </a>
        )}

        <div className="mb-4 rounded-lg border border-surface-8 bg-surface-2 p-4">
          <div className="mb-3 text-xs font-medium text-white/60">
            Gửi thông tin sau cho admin:
          </div>
          <InfoRow label="Email" value={email} />
          <InfoRow label="UID" value={uid} mono />
          {expiredAt && reason === "expired" && (
            <InfoRow
              label="Hết hạn"
              value={expiredAt.toLocaleString("vi-VN")}
            />
          )}
          <button
            type="button"
            onClick={handleCopy}
            disabled={!email}
            className="btn-ripple mt-3 flex w-full items-center justify-center gap-2 rounded-lg bg-shopee-500 px-4 py-2 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4 disabled:opacity-50"
          >
            <span className="material-symbols-rounded text-base">
              {copied ? "check" : "content_copy"}
            </span>
            {copied ? "Đã copy" : "Copy email + UID"}
          </button>
        </div>

        <div className="mb-4 flex items-center justify-center gap-2 text-xs text-white/40">
          <span className="material-symbols-rounded animate-pulse text-sm">
            sync
          </span>
          Đang chờ admin kích hoạt — app sẽ tự mở khi xong
        </div>

        <button
          type="button"
          onClick={() => void signOut()}
          className="btn-ripple block w-full rounded-lg border border-surface-8 bg-surface-2 px-4 py-2 text-sm text-white/70 hover:bg-surface-4"
        >
          Đăng xuất
        </button>
      </div>
    </main>
  );
}

interface InfoRowProps {
  label: string;
  value: string;
  mono?: boolean;
}

function InfoRow({ label, value, mono }: InfoRowProps) {
  return (
    <div className="mb-2 last:mb-0">
      <div className="text-[11px] uppercase tracking-wide text-white/40">
        {label}
      </div>
      <div
        className={`truncate text-sm text-white/90 ${mono ? "font-mono" : ""}`}
        title={value}
      >
        {value || "—"}
      </div>
    </div>
  );
}
