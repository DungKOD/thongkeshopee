import { useRef, useState } from "react";
import { DayBlock } from "./components/DayBlock";
import { NewDayDialog } from "./components/NewDayDialog";
import { SettingsDialog } from "./components/SettingsDialog";
import { useStats } from "./hooks/useStats";
import { SettingsProvider, useSettings } from "./hooks/useSettings";
import { useToast } from "./components/ToastProvider";
import { fmtDate } from "./formulas";
import { todayIso } from "./hooks/useStats";
import { aggregate, parseCsvFile } from "./lib/import";
import type { VideoInput } from "./hooks/useStats";
import type { Day } from "./types";
import "./App.css";

function AppInner() {
  const {
    days,
    addDay,
    replaceDay,
    removeDay,
    restoreDay,
    removeVideo,
    restoreVideo,
  } = useStats();

  const { showToast } = useToast();
  const { settings, setClickSource, registerSources, setProfitFee } =
    useSettings();

  const [addDayOpen, setAddDayOpen] = useState(false);
  const [editDayId, setEditDayId] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [importData, setImportData] = useState<{
    date: string;
    videos: VideoInput[];
  } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const editingDay = editDayId ? days.find((d) => d.id === editDayId) ?? null : null;
  const dialogOpen = addDayOpen || !!editingDay || !!importData;

  const closeDialog = () => {
    setAddDayOpen(false);
    setEditDayId(null);
    setImportData(null);
  };

  const handleRemoveDay = (day: Day) => {
    const index = days.findIndex((d) => d.id === day.id);
    if (index < 0) return;
    removeDay(day.id);
    showToast({
      message: `Đã xóa ngày ${fmtDate(day.date)}`,
      undo: () => restoreDay(day, index),
    });
  };

  const handleRemoveVideo = (day: Day, videoId: string) => {
    const index = day.videos.findIndex((v) => v.id === videoId);
    if (index < 0) return;
    const video = day.videos[index];

    if (day.videos.length === 1) {
      const dayIndex = days.findIndex((d) => d.id === day.id);
      removeDay(day.id);
      showToast({
        message: `Đã xóa ngày ${fmtDate(day.date)} (rỗng)`,
        undo: () => restoreDay(day, dayIndex),
      });
      return;
    }

    removeVideo(day.id, videoId);
    showToast({
      message: `Đã xóa sản phẩm "${video.name || "Chưa đặt tên"}"`,
      undo: () => restoreVideo(day.id, video, index),
    });
  };

  const handleImportClick = () => fileInputRef.current?.click();

  const handleFilesSelected = async (
    e: React.ChangeEvent<HTMLInputElement>,
  ) => {
    const files = Array.from(e.target.files ?? []);
    e.target.value = "";
    if (files.length === 0) return;
    console.log("[Import] files selected:", files.map((f) => f.name));
    try {
      const parsed = await Promise.all(files.map((f) => parseCsvFile(f)));
      const result = aggregate(parsed);
      const newSources = Object.keys(result.discoveredClickSources);
      if (newSources.length > 0) registerSources(newSources);
      const summary = result.files
        .map((f) => `${f.name}: ${f.kind} (${f.rowCount} dòng)`)
        .join("; ");
      console.log("[Import]", summary, result);

      if (result.products.length === 0) {
        showToast({
          message:
            "Không trích xuất được sản phẩm nào. Kiểm tra lại file có đúng định dạng không.",
          duration: 7000,
        });
        return;
      }
      result.warnings.forEach((w) => showToast({ message: w, duration: 6000 }));
      setImportData({
        date: result.date ?? todayIso(),
        videos: result.products,
      });
    } catch (err) {
      console.error("[Import]", err);
      showToast({
        message: `Lỗi đọc file: ${(err as Error).message}`,
        duration: 7000,
      });
    }
  };

  return (
    <main className="min-h-full bg-surface-0">
      <header className="sticky top-0 z-30 bg-gradient-to-r from-shopee-600 to-shopee-500 shadow-elev-4">
        <div className="flex items-center justify-between px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="material-symbols-rounded text-3xl text-white">
              analytics
            </span>
            <div>
              <h1 className="text-xl font-semibold tracking-tight text-white">
                Thống kê chạy Ads FB hàng ngày
              </h1>
              <p className="text-xs text-white/70">
                Các cột xám tính tự động từ dữ liệu nhập tay
              </p>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setSettingsOpen(true)}
              className="btn-ripple flex h-10 w-10 items-center justify-center rounded-full text-white hover:bg-white/10 active:bg-white/20"
              title="Cài đặt"
              aria-label="Cài đặt"
            >
              <span className="material-symbols-rounded">settings</span>
            </button>
            <button
              onClick={handleImportClick}
              className="btn-ripple flex items-center gap-2 rounded-lg border border-white/50 px-4 py-2 text-sm font-medium text-white hover:bg-white/10 active:bg-white/20"
            >
              <span className="material-symbols-rounded text-base">
                upload_file
              </span>
              Import CSV
            </button>
            <button
              onClick={() => setAddDayOpen(true)}
              className="btn-ripple flex items-center gap-2 rounded-lg bg-white px-4 py-2 text-sm font-medium text-shopee-600 shadow-elev-2 hover:shadow-elev-4"
            >
              <span className="material-symbols-rounded text-base">add</span>
              Thêm ngày
            </button>
          </div>
        </div>
      </header>

      <input
        ref={fileInputRef}
        type="file"
        accept=".csv,text/csv"
        multiple
        className="hidden"
        onChange={handleFilesSelected}
      />

      <div className="p-6">
        {days.length === 0 ? (
          <div className="mx-auto flex max-w-xl flex-col items-center gap-4 rounded-2xl border border-surface-8 bg-surface-1 p-12 text-center shadow-elev-1">
            <span className="material-symbols-rounded text-6xl text-shopee-400">
              calendar_month
            </span>
            <div>
              <h2 className="text-lg font-medium text-white/90">
                Chưa có ngày nào
              </h2>
              <p className="mt-1 text-sm text-white/60">
                Bắt đầu bằng cách thêm ngày mới hoặc import từ CSV
              </p>
            </div>
            <button
              onClick={() => setAddDayOpen(true)}
              className="btn-ripple mt-2 flex items-center gap-2 rounded-lg bg-shopee-500 px-5 py-2.5 text-sm font-medium text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4"
            >
              <span className="material-symbols-rounded text-base">add</span>
              Thêm ngày đầu tiên
            </button>
          </div>
        ) : (
          days.map((day) => (
            <DayBlock
              key={day.id}
              day={day}
              onRemoveDay={() => handleRemoveDay(day)}
              onRemoveVideo={(vid) => handleRemoveVideo(day, vid)}
              onEditDay={() => setEditDayId(day.id)}
            />
          ))
        )}
      </div>

      <SettingsDialog
        isOpen={settingsOpen}
        settings={settings}
        daysCount={days.length}
        productsCount={days.reduce((a, d) => a + d.videos.length, 0)}
        onToggleClickSource={setClickSource}
        onSetProfitFee={setProfitFee}
        onClose={() => setSettingsOpen(false)}
      />

      <NewDayDialog
        isOpen={dialogOpen}
        existingDays={days}
        initialDay={editingDay ?? undefined}
        initialData={importData ?? undefined}
        onSave={(date, videos, replaceDayId) => {
          if (replaceDayId) {
            replaceDay(replaceDayId, date, videos);
          } else {
            addDay(date, videos);
          }
          closeDialog();
        }}
        onClose={closeDialog}
      />
    </main>
  );
}

function App() {
  return (
    <SettingsProvider>
      <AppInner />
    </SettingsProvider>
  );
}

export default App;
