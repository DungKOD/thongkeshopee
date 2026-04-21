import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { save } from "@tauri-apps/plugin-dialog";
import { invoke } from "../lib/tauri";

interface DayScreenshotDialogProps {
  isOpen: boolean;
  /** PNG blob chụp từ DayBlock / ProductDetailDialog. Dialog tự tạo + revoke object URL. */
  blob: Blob | null;
  /** Định dạng ISO `YYYY-MM-DD` — dùng làm phần default filename. */
  date: string;
  /** Text đã format dd/mm/yyyy — hiển thị trên title. */
  dateLabel: string;
  /** Override default filename. Nếu không truyền: `thongkee-${date}.png`. */
  defaultFileName?: string;
  /** Override title. Nếu không truyền: `Ảnh ngày ${dateLabel}`. */
  title?: string;
  onClose: () => void;
}

type Status =
  | { kind: "idle" }
  | { kind: "busy"; text: string }
  | { kind: "ok"; text: string }
  | { kind: "err"; text: string };

/** 1 stroke freehand — list điểm theo tọa độ pixel của canvas (native image size). */
interface Stroke {
  points: Array<[number, number]>;
  width: number;
  color: string;
}

const PEN_SIZES: Array<{ label: string; scale: number }> = [
  { label: "Nhỏ", scale: 0.003 },
  { label: "Vừa", scale: 0.006 },
  { label: "Lớn", scale: 0.01 },
];
// 10 màu bút — mặc định đỏ (index 0). Chọn các màu contrast tốt trên nền dark/light.
const PEN_COLORS: Array<{ label: string; value: string }> = [
  { label: "Đỏ", value: "#ef4444" },
  { label: "Cam", value: "#f97316" },
  { label: "Vàng", value: "#fbbf24" },
  { label: "Xanh lá", value: "#22c55e" },
  { label: "Teal", value: "#14b8a6" },
  { label: "Cyan", value: "#06b6d4" },
  { label: "Xanh dương", value: "#3b82f6" },
  { label: "Tím", value: "#a855f7" },
  { label: "Hồng", value: "#ec4899" },
  { label: "Trắng", value: "#ffffff" },
];

export function DayScreenshotDialog({
  isOpen,
  blob,
  date,
  dateLabel,
  defaultFileName,
  title,
  onClose,
}: DayScreenshotDialogProps) {
  const [status, setStatus] = useState<Status>({ kind: "idle" });
  const [strokes, setStrokes] = useState<Stroke[]>([]);
  const [penSizeIdx, setPenSizeIdx] = useState(1);
  const [penColorIdx, setPenColorIdx] = useState(0);
  const [naturalWidth, setNaturalWidth] = useState(0);
  const [objectUrl, setObjectUrl] = useState<string | null>(null);

  const imgRef = useRef<HTMLImageElement>(null);
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const currentStrokeRef = useRef<Stroke | null>(null);

  // Tạo object URL từ blob + revoke khi blob đổi/unmount → giải phóng bộ nhớ.
  useEffect(() => {
    if (!blob) {
      setObjectUrl(null);
      return;
    }
    const url = URL.createObjectURL(blob);
    setObjectUrl(url);
    return () => URL.revokeObjectURL(url);
  }, [blob]);

  // Khóa scroll của body khi dialog mở để wheel trong ảnh preview không
  // lan (scroll-chain) ra bảng thống kê phía sau.
  useEffect(() => {
    if (!isOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [isOpen]);

  // Reset state mỗi lần mở dialog mới + clear hết khi đóng (strokes, status,
  // naturalWidth) để lần mở kế tiếp không kéo theo state cũ.
  useEffect(() => {
    if (!isOpen) {
      setStatus({ kind: "idle" });
      setStrokes([]);
      setNaturalWidth(0);
      currentStrokeRef.current = null;
      return;
    }
    setStatus({ kind: "idle" });
    setStrokes([]);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
        return;
      }
      // Ctrl+Z (Windows/Linux) hoặc Cmd+Z (Mac) → undo nét cuối.
      if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === "z") {
        e.preventDefault();
        setStrokes((prev) => prev.slice(0, -1));
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose]);

  const redraw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    const draw = (s: Stroke) => {
      ctx.strokeStyle = s.color;
      ctx.lineWidth = s.width;
      ctx.beginPath();
      s.points.forEach(([x, y], i) => {
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.stroke();
    };
    strokes.forEach(draw);
    if (currentStrokeRef.current) draw(currentStrokeRef.current);
  }, [strokes]);

  // Set canvas intrinsic size = image natural size khi ảnh đã load.
  const handleImgLoad = () => {
    const img = imgRef.current;
    const canvas = canvasRef.current;
    if (!img || !canvas) return;
    canvas.width = img.naturalWidth;
    canvas.height = img.naturalHeight;
    setNaturalWidth(img.naturalWidth);
    redraw();
  };

  useEffect(() => {
    redraw();
  }, [redraw]);

  if (!isOpen || !objectUrl) return null;

  const currentWidth = () => {
    const canvas = canvasRef.current;
    if (!canvas) return 8;
    const base = Math.min(canvas.width, canvas.height);
    return Math.max(3, Math.round(base * PEN_SIZES[penSizeIdx].scale));
  };

  const getPoint = (e: React.PointerEvent<HTMLCanvasElement>): [number, number] => {
    const canvas = canvasRef.current!;
    const rect = canvas.getBoundingClientRect();
    const scaleX = canvas.width / rect.width;
    const scaleY = canvas.height / rect.height;
    return [(e.clientX - rect.left) * scaleX, (e.clientY - rect.top) * scaleY];
  };

  const onPointerDown = (e: React.PointerEvent<HTMLCanvasElement>) => {
    if (e.button !== 0) return;
    e.currentTarget.setPointerCapture(e.pointerId);
    currentStrokeRef.current = {
      points: [getPoint(e)],
      width: currentWidth(),
      color: PEN_COLORS[penColorIdx].value,
    };
    redraw();
  };
  const onPointerMove = (e: React.PointerEvent<HTMLCanvasElement>) => {
    if (!currentStrokeRef.current) return;
    currentStrokeRef.current.points.push(getPoint(e));
    redraw();
  };
  const onPointerUp = (e: React.PointerEvent<HTMLCanvasElement>) => {
    const stroke = currentStrokeRef.current;
    currentStrokeRef.current = null;
    try {
      e.currentTarget.releasePointerCapture(e.pointerId);
    } catch {
      /* no-op */
    }
    if (stroke && stroke.points.length > 0) {
      setStrokes((prev) => [...prev, stroke]);
    } else {
      redraw();
    }
  };

  const handleUndo = () => setStrokes((prev) => prev.slice(0, -1));
  const handleClear = () => setStrokes([]);

  /** Merge ảnh gốc + canvas vẽ → Blob PNG. Không có stroke thì trả blob gốc. */
  const exportMerged = async (): Promise<Blob | null> => {
    if (!blob) return null;
    if (strokes.length === 0) return blob;
    const img = imgRef.current;
    if (!img) return blob;
    const tmp = document.createElement("canvas");
    tmp.width = img.naturalWidth;
    tmp.height = img.naturalHeight;
    const ctx = tmp.getContext("2d");
    if (!ctx) return blob;
    ctx.drawImage(img, 0, 0);
    if (canvasRef.current) ctx.drawImage(canvasRef.current, 0, 0);
    return await new Promise<Blob | null>((resolve) =>
      tmp.toBlob((b) => resolve(b), "image/png"),
    );
  };

  const blobToBase64 = (b: Blob): Promise<string> =>
    new Promise((resolve, reject) => {
      const r = new FileReader();
      r.onload = () => {
        const result = String(r.result);
        resolve(result.split(",")[1] ?? result);
      };
      r.onerror = () => reject(r.error ?? new Error("FileReader error"));
      r.readAsDataURL(b);
    });

  const handleCopy = async () => {
    setStatus({ kind: "busy", text: "Đang copy..." });
    try {
      const merged = await exportMerged();
      if (!merged) throw new Error("Không có ảnh để copy");
      await navigator.clipboard.write([
        new ClipboardItem({ "image/png": merged }),
      ]);
      setStatus({ kind: "ok", text: "Đã copy vào clipboard" });
    } catch (e) {
      setStatus({ kind: "err", text: `Copy thất bại: ${String(e)}` });
    }
  };

  const handleSave = async () => {
    setStatus({ kind: "busy", text: "Chọn nơi lưu..." });
    try {
      const defaultName = defaultFileName ?? `thongkee-${date}.png`;
      const path = await save({
        defaultPath: defaultName,
        filters: [{ name: "PNG Image", extensions: ["png"] }],
      });
      if (!path) {
        setStatus({ kind: "idle" });
        return;
      }
      setStatus({ kind: "busy", text: "Đang lưu..." });
      const merged = await exportMerged();
      if (!merged) throw new Error("Không có ảnh để lưu");
      const base64 = await blobToBase64(merged);
      const saved = await invoke<string>("save_png", {
        path,
        base64Data: base64,
      });
      setStatus({ kind: "ok", text: `Đã lưu: ${saved}` });
    } catch (e) {
      setStatus({ kind: "err", text: `Lưu thất bại: ${String(e)}` });
    }
  };

  const handleBackdropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    if (e.target === e.currentTarget) onClose();
  };

  const statusCls =
    status.kind === "ok"
      ? "text-green-400"
      : status.kind === "err"
      ? "text-red-400"
      : "text-white/60";

  return createPortal(
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4"
      onMouseDown={handleBackdropMouseDown}
    >
      <div
        className="flex max-h-[76vh] w-full max-w-[76vw] select-none flex-col overflow-hidden rounded-2xl bg-surface-4 shadow-elev-24"
        role="dialog"
        aria-modal="true"
        aria-labelledby="screenshot-dialog-title"
      >
        <header className="flex items-center justify-between border-b border-surface-8 px-6 py-3">
          <div className="flex items-center gap-3">
            <span className="material-symbols-rounded text-2xl text-shopee-400">
              photo_camera
            </span>
            <h2
              id="screenshot-dialog-title"
              className="text-lg font-semibold text-white/90"
            >
              {title ?? `Ảnh ngày ${dateLabel}`}
            </h2>
          </div>

          <div className="flex flex-wrap items-center gap-1.5">
            <span
              className="mr-1 flex items-center gap-1 rounded-md bg-surface-8 px-2 py-1 text-xs font-medium text-white/70"
              title="Kéo chuột trên ảnh để vẽ khoanh tròn"
            >
              <span className="material-symbols-rounded text-sm">edit</span>
              Bút
            </span>

            {PEN_COLORS.map((c, i) => (
              <button
                key={c.value}
                type="button"
                onClick={() => setPenColorIdx(i)}
                className={`h-6 w-6 rounded-full border transition-transform ${
                  penColorIdx === i
                    ? "scale-110 border-white ring-2 ring-white/60"
                    : "border-white/20 hover:scale-105"
                }`}
                style={{ backgroundColor: c.value }}
                title={c.label}
                aria-label={`Màu ${c.label}`}
              />
            ))}

            <div className="mx-1 h-6 w-px bg-surface-8" />

            {PEN_SIZES.map((s, i) => (
              <button
                key={s.label}
                type="button"
                onClick={() => setPenSizeIdx(i)}
                className={`flex h-8 w-8 items-center justify-center rounded-full transition-colors ${
                  penSizeIdx === i
                    ? "bg-white/15 ring-2 ring-white/70"
                    : "bg-surface-6 hover:bg-surface-8"
                }`}
                title={`Cỡ ${s.label}`}
                aria-label={`Cỡ ${s.label}`}
              >
                <span
                  className="block rounded-full"
                  style={{
                    width: `${4 + i * 4}px`,
                    height: `${4 + i * 4}px`,
                    backgroundColor: PEN_COLORS[penColorIdx].value,
                  }}
                />
              </button>
            ))}

            <button
              type="button"
              onClick={handleUndo}
              disabled={strokes.length === 0}
              className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/80 hover:bg-white/5 disabled:cursor-not-allowed disabled:opacity-30"
              title="Hoàn tác nét vẽ (Ctrl+Z)"
              aria-label="Hoàn tác"
            >
              <span className="material-symbols-rounded">undo</span>
            </button>
            <button
              type="button"
              onClick={handleClear}
              disabled={strokes.length === 0}
              className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/80 hover:bg-red-500/10 hover:text-red-400 disabled:cursor-not-allowed disabled:opacity-30"
              title="Xóa hết nét vẽ"
              aria-label="Xóa hết"
            >
              <span className="material-symbols-rounded">ink_eraser</span>
            </button>

            <div className="mx-1 h-6 w-px bg-surface-8" />

            <button
              type="button"
              onClick={onClose}
              className="btn-ripple flex h-9 w-9 items-center justify-center rounded-full text-white/60 hover:bg-white/5 hover:text-white/90"
              aria-label="Đóng"
              title="Đóng (Esc)"
            >
              <span className="material-symbols-rounded">close</span>
            </button>
          </div>
        </header>

        <div className="flex-1 overflow-auto overscroll-contain bg-surface-0 p-4">
          {/* Flex wrapper min-h/w-full để ảnh center cả 2 trục; khi ảnh lớn
           * hơn container sẽ tự scroll từ vị trí center (inner div expand). */}
          <div className="flex min-h-full min-w-full items-center justify-center">
            <div className="relative">
              <img
                ref={imgRef}
                src={objectUrl}
                alt={`Ảnh ngày ${dateLabel}`}
                onLoad={handleImgLoad}
                // Hiển thị ở 64% native size (0.8 × 0.8). html-to-image chụp
                // pixelRatio 2 nên 64% native ≈ 1.28× DOM gốc — vẫn sắc nét.
                style={{ width: naturalWidth ? `${naturalWidth * 0.64}px` : undefined }}
                className="block max-w-none rounded-lg shadow-elev-8"
                draggable={false}
              />
              <canvas
                ref={canvasRef}
                onPointerDown={onPointerDown}
                onPointerMove={onPointerMove}
                onPointerUp={onPointerUp}
                onPointerCancel={onPointerUp}
                className="absolute inset-0 h-full w-full touch-none"
                style={{ cursor: "crosshair" }}
              />
            </div>
          </div>
        </div>

        <footer className="flex items-center justify-between gap-3 border-t border-surface-8 bg-surface-1 px-6 py-3">
          <span
            className={`truncate text-sm ${statusCls}`}
            title={status.kind !== "idle" ? status.text : undefined}
          >
            {status.kind !== "idle" ? status.text : ""}
          </span>
          <div className="flex gap-2">
            <button
              type="button"
              onClick={handleCopy}
              disabled={status.kind === "busy"}
              className="btn-ripple flex items-center gap-2 rounded-lg bg-surface-6 px-4 py-2 text-sm font-medium text-white/90 hover:bg-surface-8 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="material-symbols-rounded text-base">
                content_copy
              </span>
              Copy clipboard
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={status.kind === "busy"}
              className="btn-ripple flex items-center gap-2 rounded-lg bg-shopee-500 px-4 py-2 text-sm font-semibold text-white shadow-elev-2 hover:bg-shopee-600 hover:shadow-elev-4 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span className="material-symbols-rounded text-base">
                download
              </span>
              Lưu xuống PC
            </button>
          </div>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
