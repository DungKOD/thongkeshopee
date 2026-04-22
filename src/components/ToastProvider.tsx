import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";

interface Toast {
  id: string;
  message: string;
  undo?: () => void;
  duration: number;
}

interface ShowToastInput {
  message: string;
  undo?: () => void;
  duration?: number;
}

interface ToastContextValue {
  showToast: (input: ShowToastInput) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

const uid = () =>
  typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : Math.random().toString(36).slice(2);

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const timers = useRef<Map<string, number>>(new Map());

  const dismiss = useCallback((id: string) => {
    const t = timers.current.get(id);
    if (t) {
      clearTimeout(t);
      timers.current.delete(id);
    }
    setToasts((prev) => prev.filter((x) => x.id !== id));
  }, []);

  const showToast = useCallback(
    ({ message, undo, duration = 5000 }: ShowToastInput) => {
      const id = uid();
      setToasts((prev) => [...prev, { id, message, undo, duration }]);
      const handle = window.setTimeout(() => {
        dismiss(id);
      }, duration);
      timers.current.set(id, handle);
    },
    [dismiss],
  );

  const handleUndo = (toast: Toast) => {
    toast.undo?.();
    dismiss(toast.id);
  };

  useEffect(() => {
    const map = timers.current;
    return () => {
      map.forEach((h) => clearTimeout(h));
      map.clear();
    };
  }, []);

  return (
    <ToastContext.Provider value={{ showToast }}>
      {children}
      {createPortal(
        <div className="pointer-events-none fixed bottom-4 right-4 z-[60] flex flex-col gap-2">
          {toasts.map((t) => (
            <div
              key={t.id}
              className="pointer-events-auto flex min-w-[320px] max-w-md items-center gap-3 rounded-xl bg-surface-12 px-4 py-3 shadow-elev-8"
            >
              <span
                className={`material-symbols-rounded ${t.undo ? "text-shopee-400" : "text-green-400"}`}
              >
                {t.undo ? "undo" : "check_circle"}
              </span>
              <span className="flex-1 text-sm text-white/90">{t.message}</span>
              {t.undo && (
                <button
                  type="button"
                  onClick={() => handleUndo(t)}
                  className="btn-ripple shrink-0 rounded-lg px-3 py-1 text-sm font-semibold uppercase tracking-wider text-shopee-400 hover:bg-shopee-500/10"
                >
                  Hoàn tác
                </button>
              )}
              <button
                type="button"
                onClick={() => dismiss(t.id)}
                className="btn-ripple flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-white/50 hover:bg-white/10 hover:text-white/80"
                aria-label="Đóng"
              >
                <span className="material-symbols-rounded text-base">
                  close
                </span>
              </button>
            </div>
          ))}
        </div>,
        document.body,
      )}
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextValue {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}
