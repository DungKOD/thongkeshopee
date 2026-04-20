/**
 * Developer credit — hiển thị info dev ở các màn hình chính để user biết liên hệ.
 * 2 variant: `inline` (dưới card/footer layout) và `floating` (fixed bottom-right).
 */

interface DevCreditProps {
  variant?: "inline" | "floating";
}

const DEV_NAME = "Nguyễn Văn Dũng";
const DEV_ZALO = "0868852102";
const DEV_ZALO_URL = `https://zalo.me/${DEV_ZALO}`;

export function DevCredit({ variant = "inline" }: DevCreditProps) {
  if (variant === "floating") {
    return (
      <div className="pointer-events-none fixed bottom-4 right-4 z-20">
        <a
          href={DEV_ZALO_URL}
          target="_blank"
          rel="noreferrer"
          className="pointer-events-auto flex items-center gap-2.5 rounded-full border border-surface-8 bg-surface-1/95 px-4 py-2.5 text-sm font-medium text-white/90 opacity-40 shadow-elev-4 backdrop-blur-md transition-all duration-200 hover:scale-105 hover:border-shopee-500/60 hover:bg-surface-2 hover:opacity-100 hover:shadow-elev-8"
          title={`Liên hệ Zalo ${DEV_ZALO}`}
        >
          <span className="material-symbols-rounded text-lg text-shopee-400">
            code
          </span>
          <span className="whitespace-nowrap">
            Dev:{" "}
            <span className="font-bold text-white">{DEV_NAME}</span>
          </span>
          <span className="text-white/30">·</span>
          <span className="flex items-center gap-1 whitespace-nowrap font-semibold text-shopee-300">
            <span className="material-symbols-rounded text-lg">chat</span>
            Zalo {DEV_ZALO}
          </span>
        </a>
      </div>
    );
  }

  return (
    <div className="mt-6 flex flex-col items-center gap-1 text-center text-xs text-white/50">
      <span className="flex items-center gap-1.5">
        <span className="material-symbols-rounded text-sm text-shopee-400">
          code
        </span>
        Developed by{" "}
        <span className="font-semibold text-white/80">{DEV_NAME}</span>
      </span>
      <a
        href={DEV_ZALO_URL}
        target="_blank"
        rel="noreferrer"
        className="inline-flex items-center gap-1 text-shopee-300 hover:text-shopee-200 hover:underline"
      >
        <span className="material-symbols-rounded text-sm">chat</span>
        Zalo: {DEV_ZALO}
      </a>
    </div>
  );
}
