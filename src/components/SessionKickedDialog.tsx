import { createPortal } from "react-dom";
import type { SessionKickInfo } from "../contexts/AuthContext";

interface SessionKickedDialogProps {
  info: SessionKickInfo;
  onAcknowledge: () => void;
}

export function SessionKickedDialog({
  info,
  onAcknowledge,
}: SessionKickedDialogProps) {
  const isVerifyFailed = info.reason === "verify-failed";
  const deviceLabel = info.deviceName?.trim() || "máy khác";
  const platformLabel = info.platform?.trim();

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/70 backdrop-blur-sm">
      <div className="mx-4 w-full max-w-md rounded-2xl border border-amber-500/40 bg-surface-1 p-6 shadow-elev-16">
        <div className="flex flex-col items-center text-center">
          <span className="material-symbols-rounded text-6xl text-amber-400">
            devices_off
          </span>
          <h2 className="mt-3 text-xl font-semibold text-white/95">
            Tài khoản đang dùng ở máy khác
          </h2>
          <p className="mt-3 text-sm leading-relaxed text-white/70">
            {isVerifyFailed ? (
              <>
                Tài khoản này đã được dùng trên một máy khác trước khi bạn mở
                app. Mỗi tài khoản chỉ được đăng nhập trên 1 máy tại một thời
                điểm.
              </>
            ) : (
              <>
                Tài khoản vừa được đăng nhập trên{" "}
                <span className="font-semibold text-amber-300">
                  {deviceLabel}
                </span>
                {platformLabel ? (
                  <span className="text-white/50"> ({platformLabel})</span>
                ) : null}
                . Phiên của bạn đã bị đăng xuất.
              </>
            )}
          </p>
          <p className="mt-3 rounded-lg bg-surface-4 px-3 py-2 text-xs text-white/55">
            Đăng nhập lại trên máy này sẽ tự động đẩy máy kia ra.
          </p>
          <button
            type="button"
            onClick={onAcknowledge}
            className="btn-ripple mt-5 flex items-center gap-2 rounded-lg bg-shopee-500 px-5 py-2.5 text-sm font-semibold text-white shadow-elev-2 hover:bg-shopee-600"
            autoFocus
          >
            <span className="material-symbols-rounded text-base">login</span>
            Đăng nhập lại
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
