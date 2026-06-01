export function UploadVideoPage() {
  return (
    <div className="mx-auto max-w-3xl space-y-5">
      {/* ===== Hero ===== */}
      <section className="overflow-hidden rounded-2xl bg-gradient-to-br from-blue-700 via-blue-600 to-blue-500 shadow-elev-4">
        <div className="flex items-center gap-4 px-6 py-5">
          <span className="flex h-12 w-12 shrink-0 items-center justify-center rounded-xl bg-white/15 text-white shadow-inner">
            <span className="material-symbols-rounded text-2xl">upload</span>
          </span>
          <div className="min-w-0 flex-1">
            <h1 className="text-xl font-bold text-white">Đăng video lên Page</h1>
            <p className="mt-0.5 text-xs text-white/75">
              Tự động đăng video lên Facebook Page qua Graph API
            </p>
          </div>
        </div>
      </section>

      {/* ===== Placeholder ===== */}
      <section className="rounded-2xl border border-dashed border-surface-8 bg-surface-1 p-10">
        <div className="flex flex-col items-center gap-4 text-center">
          <span className="material-symbols-rounded text-5xl text-blue-400">
            construction
          </span>
          <div>
            <h2 className="text-lg font-semibold text-white/85">Đang phát triển</h2>
            <p className="mt-1 text-sm text-white/55">
              Tính năng đăng video lên Facebook Page sẽ được thêm vào đây
            </p>
          </div>
        </div>
      </section>
    </div>
  );
}
