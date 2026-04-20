import { useEffect, useRef, useState } from "react";

/**
 * Máy tính thông minh nổi góc phải màn hình.
 * - Mũi tên toggle show/hide có slide animation (translate-x + duration-300).
 * - Eval biểu thức live (`2+3*4` hiển thị `14` ngay khi gõ).
 * - History 50 phép tính gần nhất, persist localStorage (`smartcalc:*`).
 * - Click 1 history item → recall vào input (chain calculation).
 * - Keyboard: Enter = commit, digits/operators gõ trực tiếp.
 *
 * Safety: `new Function()` eval — OK vì local Tauri app, user tự nhập, không
 * có external input. Sanitize regex chỉ allow `0-9 + - * / ( ) . space`.
 */

interface HistoryItem {
  expression: string;
  result: string;
  timestamp: number;
}

const LS_OPEN = "smartcalc:open";
const LS_EXPR = "smartcalc:expression";
const LS_HISTORY = "smartcalc:history";
const HISTORY_MAX = 50;

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
    /* quota exceeded — silently drop */
  }
}

type EvalResult =
  | { ok: true; value: number }
  | { ok: false; error: string };

function tryEvaluate(expr: string): EvalResult {
  const trimmed = expr.trim();
  if (!trimmed) return { ok: false, error: "" };
  // Whitelist ký tự — chặn bất kỳ JS identifier / keyword nào.
  if (!/^[\d+\-*/().%\s]+$/.test(trimmed))
    return { ok: false, error: "Ký tự không hỗ trợ" };
  // `%` trong toán học = chia 100 (không phải modulo JS). Replace trước khi eval.
  // `50%` → `50/100` = 0.5. `100*10%` → `100*10/100` = 10.
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

export function SmartCalculator() {
  const [isOpen, setIsOpen] = useState<boolean>(() => loadLS(LS_OPEN, false));
  const [expression, setExpression] = useState<string>(() =>
    loadLS(LS_EXPR, ""),
  );
  const [history, setHistory] = useState<HistoryItem[]>(() =>
    loadLS(LS_HISTORY, []),
  );
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => saveLS(LS_OPEN, isOpen), [isOpen]);
  useEffect(() => saveLS(LS_EXPR, expression), [expression]);
  useEffect(() => saveLS(LS_HISTORY, history), [history]);

  useEffect(() => {
    if (isOpen) {
      // Delay focus đến khi slide-in xong để tránh scroll lung tung.
      const t = setTimeout(() => inputRef.current?.focus(), 300);
      return () => clearTimeout(t);
    }
  }, [isOpen]);

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
  const clearHistory = () => {
    if (history.length === 0) return;
    if (!confirm("Xóa toàn bộ lịch sử?")) return;
    setHistory([]);
  };

  return (
    <div
      className="fixed right-0 top-1/2 z-40 flex -translate-y-1/2 items-center"
      aria-label="Máy tính thông minh"
    >
      {/* Toggle arrow — luôn hiển thị, mép trái panel */}
      <button
        onClick={() => setIsOpen((o) => !o)}
        className="btn-ripple z-10 flex h-16 w-8 items-center justify-center rounded-l-lg bg-gradient-to-br from-shopee-500 to-shopee-600 text-white shadow-elev-4 transition-all hover:from-shopee-400 hover:to-shopee-500 hover:w-9"
        title={isOpen ? "Thu gọn máy tính" : "Mở máy tính"}
        aria-label={isOpen ? "Thu gọn máy tính" : "Mở máy tính"}
        aria-expanded={isOpen}
      >
        <span
          className="material-symbols-rounded transition-transform duration-300 ease-out"
          style={{ transform: isOpen ? "rotate(0deg)" : "rotate(180deg)" }}
        >
          chevron_right
        </span>
      </button>

      {/* Panel — slide từ phải vào */}
      <div
        className={`flex w-80 flex-col overflow-hidden border-y border-r-0 border-l border-surface-8 bg-surface-1 shadow-elev-16 transition-transform duration-300 ease-out ${
          isOpen ? "translate-x-0" : "translate-x-full"
        }`}
        aria-hidden={!isOpen}
      >
        {/* Header */}
        <div className="flex items-center gap-2 bg-gradient-to-r from-shopee-600 to-shopee-500 px-4 py-2.5">
          <span className="material-symbols-rounded text-white">calculate</span>
          <span className="flex-1 text-sm font-semibold text-white">
            Máy tính
          </span>
          <button
            onClick={clearHistory}
            disabled={history.length === 0}
            className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/70 hover:bg-white/15 disabled:opacity-30"
            title="Xóa toàn bộ lịch sử"
            aria-label="Xóa lịch sử"
          >
            <span className="material-symbols-rounded text-base">
              history_off
            </span>
          </button>
        </div>

        {/* History */}
        <div className="min-h-[120px] max-h-[200px] flex-1 overflow-auto bg-surface-0/40 px-2 py-1.5">
          {history.length === 0 ? (
            <div className="flex h-full min-h-[100px] items-center justify-center text-center text-xs text-white/30">
              Chưa có phép tính nào
            </div>
          ) : (
            <ul className="space-y-0.5">
              {history.map((h, idx) => (
                <li key={`${h.timestamp}-${idx}`}>
                  <button
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
            onChange={(e) => setExpression(e.currentTarget.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                commit();
              }
            }}
            placeholder="Nhập phép tính..."
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
