import { useCallback, useEffect, useRef, useState } from "react";
import { ConfirmDialog } from "./ConfirmDialog";

/**
 * Máy tính thông minh — floating draggable window.
 * - Open/close bởi button ở header app (prop `isOpen` từ parent).
 * - Fade in/out opacity transition (200ms) khi toggle.
 * - Drag bằng cách grab header panel. Vị trí persist localStorage.
 * - Semi-transparent (opacity 95) để không chặn toàn bộ content phía sau.
 * - Evaluate live, chain calculation, history 50 items (persist localStorage).
 * - Hỗ trợ +, -, *, /, %, (). Percent = `/100` (50% = 0.5, 100*10% = 10).
 */

interface SmartCalculatorProps {
  isOpen: boolean;
  onClose: () => void;
}

interface HistoryItem {
  expression: string;
  result: string;
  timestamp: number;
}

interface Position {
  x: number;
  y: number;
}

const LS_POS = "smartcalc:pos";
const LS_EXPR = "smartcalc:expression";
const LS_HISTORY = "smartcalc:history";
const HISTORY_MAX = 50;
const PANEL_W = 320;
const PANEL_H_MAX = 640;
const HISTORY_H = 240;

function loadLS<T>(key: string, fallback: T): T {
  try {
    const v = localStorage.getItem(key);
    return v != null ? (JSON.parse(v) as T) : fallback;
  } catch {
    return fallback;
  }
}

function saveLS(key: string, value: unknown) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* quota */
  }
}

function clampPosition(p: Position): Position {
  const maxX = Math.max(0, window.innerWidth - PANEL_W);
  const maxY = Math.max(0, window.innerHeight - 100); // chừa ít nhất header visible
  return {
    x: Math.min(Math.max(0, p.x), maxX),
    y: Math.min(Math.max(0, p.y), maxY),
  };
}

function initialPosition(): Position {
  // Mặc định góc phải trên, cách mép 24px. Clamp defensive.
  return clampPosition({
    x: window.innerWidth - PANEL_W - 24,
    y: 120,
  });
}

type EvalResult =
  | { ok: true; value: number }
  | { ok: false; error: string };

function tryEvaluate(expr: string): EvalResult {
  const trimmed = expr.trim();
  if (!trimmed) return { ok: false, error: "" };
  if (!/^[\d+\-*/().%\s]+$/.test(trimmed))
    return { ok: false, error: "Ký tự không hỗ trợ" };
  const normalized = trimmed.replace(/%/g, "/100");
  try {
    const fn = new Function(`"use strict"; return (${normalized})`);
    const result = fn() as unknown;
    if (typeof result !== "number" || !isFinite(result))
      return { ok: false, error: "Kết quả không hợp lệ" };
    return { ok: true, value: result };
  } catch {
    return { ok: false, error: "Lỗi cú pháp" };
  }
}

function fmt(n: number): string {
  if (Number.isInteger(n)) return n.toLocaleString("en");
  const rounded = Math.round(n * 1e8) / 1e8;
  return rounded.toLocaleString("en", { maximumFractionDigits: 8 });
}

/**
 * Lọc input — chỉ giữ digit + operators + parens + dot + percent + space.
 * Dùng cho onChange để chặn paste text chứa chữ.
 */
function sanitizeInput(s: string): string {
  return s.replace(/[^\d+\-*/().%\s]/g, "");
}

/**
 * Extract số từ text (của element user ctrl+click). Xử lý cả locale VN (1.234,56)
 * và EN (1,234.56) + thousand separator. Trả về chuỗi số hợp lệ cho calculator,
 * hoặc null nếu text không chứa số.
 */
function extractNumberFromText(text: string): string | null {
  if (!text) return null;
  // Strip currency/whitespace.
  const stripped = text.replace(/[₫$€£¥\s\u00A0]/g, "");
  const match = stripped.match(/-?[\d.,]+/);
  if (!match) return null;
  const raw = match[0];
  if (!/\d/.test(raw)) return null;

  // Có cả . và , → cái nằm sau là decimal, cái còn lại là thousand separator.
  if (raw.includes(".") && raw.includes(",")) {
    const lastDot = raw.lastIndexOf(".");
    const lastComma = raw.lastIndexOf(",");
    if (lastDot > lastComma) return raw.replace(/,/g, "");
    return raw.replace(/\./g, "").replace(",", ".");
  }
  // Chỉ có , — check pattern thousand `1,234,567`.
  if (raw.includes(",")) {
    if (/^-?\d{1,3}(,\d{3})+$/.test(raw)) return raw.replace(/,/g, "");
    return raw.replace(",", ".");
  }
  // Chỉ có . — check pattern thousand `1.234.567` (VN).
  if (raw.includes(".")) {
    if (/^-?\d{1,3}(\.\d{3})+$/.test(raw)) return raw.replace(/\./g, "");
    return raw;
  }
  return raw;
}

export function SmartCalculator({ isOpen, onClose }: SmartCalculatorProps) {
  const [position, setPosition] = useState<Position>(() =>
    loadLS(LS_POS, initialPosition()),
  );
  const [expression, setExpression] = useState<string>(() =>
    loadLS(LS_EXPR, ""),
  );
  const [history, setHistory] = useState<HistoryItem[]>(() =>
    loadLS(LS_HISTORY, []),
  );
  const [dragState, setDragState] = useState<{
    offsetX: number;
    offsetY: number;
  } | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => saveLS(LS_POS, position), [position]);
  useEffect(() => saveLS(LS_EXPR, expression), [expression]);
  useEffect(() => saveLS(LS_HISTORY, history), [history]);

  // Focus input sau khi fade-in xong (tránh scroll lung tung).
  useEffect(() => {
    if (isOpen) {
      const t = setTimeout(() => inputRef.current?.focus(), 220);
      return () => clearTimeout(t);
    }
  }, [isOpen]);

  // Global mouse tracking cho drag. Chỉ active khi dragState set.
  useEffect(() => {
    if (!dragState) return;
    const onMove = (e: MouseEvent) => {
      setPosition(
        clampPosition({
          x: e.clientX - dragState.offsetX,
          y: e.clientY - dragState.offsetY,
        }),
      );
    };
    const onUp = () => setDragState(null);
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
    return () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
  }, [dragState]);

  // Resize window → clamp vị trí để panel không ra ngoài viewport.
  useEffect(() => {
    const onResize = () => setPosition((p) => clampPosition(p));
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // Ctrl/Cmd + click vào số bất kỳ trong app → auto append vào expression.
  // Chỉ active khi calculator open. Capture phase để chặn handler dưới.
  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: MouseEvent) => {
      if (!(e.ctrlKey || e.metaKey)) return;
      const target = e.target as HTMLElement | null;
      if (!target) return;
      // Bỏ qua click trong chính panel máy tính (tránh self-loop).
      if (target.closest("[data-smartcalc-panel]")) return;
      // Bỏ qua click vào <input>/<textarea> (user đang edit field khác).
      const tag = target.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;

      const text = target.textContent ?? "";
      const num = extractNumberFromText(text);
      if (num == null) return;

      e.preventDefault();
      e.stopPropagation();
      setExpression((prev) => prev + num);
      // Visual pulse: focus input để user thấy ngay giá trị đã chèn.
      inputRef.current?.focus();
    };
    window.addEventListener("click", handler, true);
    return () => window.removeEventListener("click", handler, true);
  }, [isOpen]);

  const startDrag = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      // Chỉ drag khi click vào header (không phải button bên trong header).
      if ((e.target as HTMLElement).closest("button")) return;
      e.preventDefault();
      setDragState({
        offsetX: e.clientX - position.x,
        offsetY: e.clientY - position.y,
      });
    },
    [position],
  );

  const evalResult = tryEvaluate(expression);
  const liveResult = evalResult.ok ? fmt(evalResult.value) : "";
  const liveError = !evalResult.ok && evalResult.error ? evalResult.error : "";

  const commit = () => {
    if (!evalResult.ok) return;
    const item: HistoryItem = {
      expression: expression.trim(),
      result: fmt(evalResult.value),
      timestamp: Date.now(),
    };
    setHistory((prev) => [item, ...prev].slice(0, HISTORY_MAX));
    setExpression(String(evalResult.value));
  };

  const append = (s: string) => {
    setExpression((e) => e + s);
    inputRef.current?.focus();
  };
  const clear = () => {
    setExpression("");
    inputRef.current?.focus();
  };
  const backspace = () => {
    setExpression((e) => e.slice(0, -1));
    inputRef.current?.focus();
  };
  const recall = (item: HistoryItem) => {
    setExpression(item.expression);
    inputRef.current?.focus();
  };
  const [confirmClearHistory, setConfirmClearHistory] = useState(false);
  const clearHistory = () => {
    if (history.length === 0) return;
    setConfirmClearHistory(true);
  };

  return (
    <div
      className={`fixed z-40 transition-opacity duration-200 ease-out ${
        isOpen ? "opacity-80 hover:opacity-100" : "pointer-events-none opacity-0"
      } ${dragState ? "opacity-100 select-none" : ""}`}
      style={{
        left: `${position.x}px`,
        top: `${position.y}px`,
        width: `${PANEL_W}px`,
        maxHeight: `${PANEL_H_MAX}px`,
      }}
      aria-hidden={!isOpen}
      data-smartcalc-panel
    >
      <div className="flex flex-col overflow-hidden rounded-xl border border-surface-8 bg-surface-1/95 shadow-elev-16 backdrop-blur-md">
        {/* Draggable header */}
        <div
          onMouseDown={startDrag}
          className={`flex items-center gap-2 bg-gradient-to-r from-shopee-600 to-shopee-500 px-3 py-2 ${
            dragState ? "cursor-grabbing" : "cursor-grab"
          }`}
        >
          <span className="material-symbols-rounded text-white/90">
            drag_indicator
          </span>
          <span className="flex-1 select-none text-sm font-semibold text-white">
            Máy tính
          </span>
          <button
            type="button"
            onClick={clearHistory}
            disabled={history.length === 0}
            className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/70 hover:bg-white/15 disabled:opacity-30"
            title="Xóa lịch sử"
            aria-label="Xóa lịch sử"
          >
            <span className="material-symbols-rounded text-base">
              history_off
            </span>
          </button>
          <button
            type="button"
            onClick={onClose}
            className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/90 hover:bg-white/15"
            title="Đóng"
            aria-label="Đóng máy tính"
          >
            <span className="material-symbols-rounded text-base">close</span>
          </button>
        </div>

        {/* History — fixed height, scroll khi nhiều. */}
        <div
          className="overflow-auto bg-surface-0/40 px-2 py-1.5"
          style={{ height: `${HISTORY_H}px` }}
        >
          {history.length === 0 ? (
            <div className="flex h-full items-center justify-center text-center text-xs text-white/30">
              Chưa có phép tính nào
            </div>
          ) : (
            <ul className="space-y-0.5">
              {history.map((h, idx) => (
                <li key={`${h.timestamp}-${idx}`}>
                  <button
                    type="button"
                    onClick={() => recall(h)}
                    className="group w-full rounded-md px-2 py-1 text-right transition-colors hover:bg-surface-2/60"
                    title="Click để tính lại"
                  >
                    <div className="truncate text-[11px] text-white/40 group-hover:text-white/60">
                      {h.expression}
                    </div>
                    <div className="truncate text-sm font-semibold text-shopee-300">
                      = {h.result}
                    </div>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Input + live result */}
        <div className="border-t border-surface-8 bg-surface-2 px-3 py-2">
          <input
            ref={inputRef}
            type="text"
            value={expression}
            onChange={(e) =>
              setExpression(sanitizeInput(e.currentTarget.value))
            }
            onPaste={(e) => {
              // Filter clipboard — chỉ lấy phần số/toán tử.
              e.preventDefault();
              const raw = e.clipboardData.getData("text");
              const clean = sanitizeInput(raw);
              if (clean) setExpression((prev) => prev + clean);
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                commit();
              }
            }}
            placeholder="Chỉ số + − × ÷ ( ) . %"
            className="w-full rounded-md bg-surface-1 px-3 py-2 text-right text-lg font-medium tabular-nums text-white/95 outline-none transition-all focus:ring-2 focus:ring-shopee-500/50"
            spellCheck={false}
            autoComplete="off"
            inputMode="decimal"
          />
          <div className="mt-1 flex min-h-[20px] items-center justify-end px-2 text-right">
            {liveResult && (
              <span className="text-sm font-semibold text-shopee-300 tabular-nums">
                = {liveResult}
              </span>
            )}
            {liveError && (
              <span className="text-xs text-red-400/80">{liveError}</span>
            )}
          </div>
        </div>

        {/* Keypad */}
        <div className="grid grid-cols-4 gap-1.5 p-2">
          <KeyBtn onClick={clear} variant="danger">
            C
          </KeyBtn>
          <KeyBtn onClick={backspace} variant="ghost">
            ⌫
          </KeyBtn>
          <KeyBtn onClick={() => append("(")} variant="ghost">
            (
          </KeyBtn>
          <KeyBtn onClick={() => append(")")} variant="ghost">
            )
          </KeyBtn>

          <KeyBtn onClick={() => append("7")}>7</KeyBtn>
          <KeyBtn onClick={() => append("8")}>8</KeyBtn>
          <KeyBtn onClick={() => append("9")}>9</KeyBtn>
          <KeyBtn onClick={() => append("/")} variant="op">
            ÷
          </KeyBtn>

          <KeyBtn onClick={() => append("4")}>4</KeyBtn>
          <KeyBtn onClick={() => append("5")}>5</KeyBtn>
          <KeyBtn onClick={() => append("6")}>6</KeyBtn>
          <KeyBtn onClick={() => append("*")} variant="op">
            ×
          </KeyBtn>

          <KeyBtn onClick={() => append("1")}>1</KeyBtn>
          <KeyBtn onClick={() => append("2")}>2</KeyBtn>
          <KeyBtn onClick={() => append("3")}>3</KeyBtn>
          <KeyBtn onClick={() => append("-")} variant="op">
            −
          </KeyBtn>

          <KeyBtn onClick={() => append("0")}>0</KeyBtn>
          <KeyBtn onClick={() => append(".")}>.</KeyBtn>
          <KeyBtn onClick={() => append("%")} variant="op">
            %
          </KeyBtn>
          <KeyBtn onClick={() => append("+")} variant="op">
            +
          </KeyBtn>

          <div className="col-span-4">
            <KeyBtn onClick={commit} variant="primary" full>
              = Tính
            </KeyBtn>
          </div>
        </div>
      </div>

      <ConfirmDialog
        isOpen={confirmClearHistory}
        title="Xóa toàn bộ lịch sử?"
        message={`Xóa ${history.length} phép tính đã lưu. Không hoàn tác được.`}
        confirmLabel="Xóa lịch sử"
        danger
        onConfirm={() => {
          setHistory([]);
          setConfirmClearHistory(false);
        }}
        onClose={() => setConfirmClearHistory(false)}
      />
    </div>
  );
}

interface KeyBtnProps {
  onClick: () => void;
  children: React.ReactNode;
  variant?: "default" | "op" | "primary" | "danger" | "ghost";
  full?: boolean;
}

function KeyBtn({
  onClick,
  children,
  variant = "default",
  full = false,
}: KeyBtnProps) {
  const styles: Record<string, string> = {
    default:
      "bg-surface-4 text-white/90 hover:bg-surface-6 active:bg-surface-8",
    op: "bg-shopee-900/40 text-shopee-300 hover:bg-shopee-900/60 active:bg-shopee-900/80",
    primary:
      "bg-gradient-to-br from-shopee-500 to-shopee-600 text-white font-bold hover:from-shopee-400 hover:to-shopee-500 shadow-elev-2 hover:shadow-elev-4",
    danger: "bg-red-900/30 text-red-300 hover:bg-red-900/50",
    ghost: "bg-surface-2 text-white/70 hover:bg-surface-4",
  };
  const fullCls = full ? "w-full" : "";
  return (
    <button
      type="button"
      onClick={onClick}
      className={`btn-ripple h-10 rounded-md text-sm font-medium transition-all ${styles[variant]} ${fullCls}`}
    >
      {children}
    </button>
  );
}
