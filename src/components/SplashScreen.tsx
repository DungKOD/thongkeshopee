interface SplashScreenProps {
  title: string;
  subtitle?: string;
  error?: boolean;
  onRetry?: () => void;
}

export function SplashScreen({ title, subtitle, error, onRetry }: SplashScreenProps) {
  return (
    <main className="min-h-screen bg-surface-0 px-6 text-white">
      <div className="flex flex-col items-center pt-[32vh]">
        <span
          className={`material-symbols-rounded text-7xl ${
            error ? "text-red-400" : "animate-spin text-shopee-400"
          }`}
        >
          {error ? "error" : "cloud_sync"}
        </span>
        <div className="mt-6 flex flex-col items-center gap-3">
          <h1 className="text-center text-2xl font-semibold text-white/95">
            {title}
          </h1>
          {subtitle && (
            <p className="max-w-md break-words text-center text-base text-white/70">
              {subtitle}
            </p>
          )}
          {onRetry && (
            <button
              onClick={onRetry}
              className="btn-ripple mt-2 flex items-center gap-2 rounded-lg border border-white/40 bg-white/10 px-4 py-2 text-sm font-medium text-white hover:bg-white/20"
            >
              <span className="material-symbols-rounded text-base">refresh</span>
              Tải lại
            </button>
          )}
        </div>
      </div>
    </main>
  );
}
