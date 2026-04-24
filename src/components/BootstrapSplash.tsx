import type { SyncPhase } from "../hooks/useCloudSync";

interface BootstrapSplashProps {
  /// Current phase từ useCloudSync — null khi chưa fire sync call.
  phase: SyncPhase;
  /// Nếu user đã sync trên 1 máy khác gần đây, hiển thị thời điểm để
  /// user yên tâm là dữ liệu gần nhất sẽ về.
  lastSyncAt?: Date | null;
  /// Error message từ doSync — hiển thị dưới progress, splash vẫn giữ
  /// layout bootstrap (không đổi sang error splash thường).
  error?: string | null;
}

interface Step {
  key: "pull" | "apply" | "push";
  label: string;
  hint: string;
}

const STEPS: Step[] = [
  {
    key: "pull",
    label: "Nhận dữ liệu",
    hint: "Kéo manifest + delta files từ R2.",
  },
  {
    key: "apply",
    label: "Áp dụng",
    hint: "Insert / upsert / tombstone vào DB local.",
  },
  {
    key: "push",
    label: "Đồng bộ máy này",
    hint: "Ghi nhận máy này đã có dữ liệu.",
  },
];

/// Fullscreen splash cho lần khởi tạo đầu tiên (freshInstallPending=true
/// hoặc local empty + remote có data). UX khác SplashScreen thường ở chỗ:
/// - Tiêu đề rõ "Khởi tạo lần đầu" (user biết app đang làm gì quan trọng)
/// - Progress indicator 3 bước (pull → apply → push) với step active highlighted
/// - Subtitle nhắc KHÔNG tắt app giữa chừng (rule giữ data C2)
///
/// Component stateless — phase đến từ `useCloudSync.syncPhase`.
export function BootstrapSplash({
  phase,
  lastSyncAt,
  error,
}: BootstrapSplashProps) {
  const activeIdx = phaseToIdx(phase);
  return (
    <main className="min-h-screen bg-surface-0 px-6 text-white">
      <div className="mx-auto flex max-w-xl flex-col items-center pt-[18vh]">
        <div className="flex h-20 w-20 items-center justify-center rounded-full bg-shopee-500/15 ring-4 ring-shopee-500/30">
          <span className="material-symbols-rounded animate-pulse text-5xl text-shopee-300">
            cloud_download
          </span>
        </div>
        <h1 className="mt-6 text-center text-2xl font-semibold text-white/95">
          Khởi tạo dữ liệu lần đầu
        </h1>
        <p className="mt-2 max-w-md text-center text-sm text-white/70">
          Đang tải dữ liệu của bạn từ R2. Có thể mất vài phút tùy dung
          lượng. <span className="text-amber-300">Vui lòng không tắt app</span>{" "}
          cho đến khi hoàn tất.
        </p>
        {lastSyncAt && (
          <p className="mt-1 text-xs text-white/40">
            Sync gần nhất trên máy khác:{" "}
            {lastSyncAt.toLocaleString("vi-VN")}
          </p>
        )}

        <ul className="mt-10 w-full space-y-3">
          {STEPS.map((step, idx) => (
            <StepRow
              key={step.key}
              step={step}
              state={
                error && idx === activeIdx
                  ? "error"
                  : idx < activeIdx
                    ? "done"
                    : idx === activeIdx
                      ? "active"
                      : "pending"
              }
            />
          ))}
        </ul>

        {error && (
          <div className="mt-6 w-full rounded-lg border border-red-500/40 bg-red-900/20 p-3 text-sm text-red-200">
            <div className="font-medium">Lỗi giữa chừng</div>
            <div className="mt-1 break-words text-xs text-red-300/90">
              {error}
            </div>
            <div className="mt-2 text-xs text-white/60">
              Hệ thống đang retry với backoff exponential. Nếu không tự hồi
              phục, hãy kiểm tra kết nối rồi mở lại app.
            </div>
          </div>
        )}
      </div>
    </main>
  );
}

function phaseToIdx(phase: SyncPhase): number {
  // Bootstrap chạy pull → (apply xảy ra trong pull trong Rust) → push xác nhận
  // máy này có data. syncPhase trong useCloudSync chỉ có "pulling"/"pushing"
  // — map:
  // - null (chưa fire): 0 (pull chuẩn bị)
  // - "pulling": 1 (đang apply — pull_all đã fetch + apply trong Rust)
  // - "pushing": 2
  switch (phase) {
    case "pulling":
      return 1;
    case "pushing":
      return 2;
    default:
      return 0;
  }
}

interface StepRowProps {
  step: Step;
  state: "pending" | "active" | "done" | "error";
}

function StepRow({ step, state }: StepRowProps) {
  const { icon, color, bg, ring, spin } = stepVisual(state);
  return (
    <li
      className={`flex items-start gap-3 rounded-xl border border-surface-8 bg-surface-1 px-4 py-3 ${ring}`}
    >
      <div className={`flex h-9 w-9 items-center justify-center rounded-full ${bg}`}>
        <span
          className={`material-symbols-rounded text-lg ${color} ${spin ? "animate-spin" : ""}`}
        >
          {icon}
        </span>
      </div>
      <div className="flex-1 min-w-0">
        <div className={`text-sm font-medium ${state === "pending" ? "text-white/50" : "text-white/90"}`}>
          {step.label}
        </div>
        <div className="text-xs text-white/50">{step.hint}</div>
      </div>
    </li>
  );
}

function stepVisual(state: StepRowProps["state"]): {
  icon: string;
  color: string;
  bg: string;
  ring: string;
  spin: boolean;
} {
  switch (state) {
    case "done":
      return {
        icon: "check",
        color: "text-green-300",
        bg: "bg-green-500/20",
        ring: "",
        spin: false,
      };
    case "active":
      return {
        icon: "sync",
        color: "text-shopee-300",
        bg: "bg-shopee-500/20",
        ring: "ring-1 ring-shopee-500/50",
        spin: true,
      };
    case "error":
      return {
        icon: "error",
        color: "text-red-300",
        bg: "bg-red-500/20",
        ring: "ring-1 ring-red-500/50",
        spin: false,
      };
    case "pending":
    default:
      return {
        icon: "radio_button_unchecked",
        color: "text-white/40",
        bg: "bg-surface-4",
        ring: "",
        spin: false,
      };
  }
}
