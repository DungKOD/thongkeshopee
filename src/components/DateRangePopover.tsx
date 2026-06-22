import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { FilterMode } from "../hooks/useFilterMode";

interface DateRangePopoverProps {
  mode: FilterMode;
  isPrevMonthActive: boolean;
  isCurrentMonthActive: boolean;
  setRecent: (n: number) => void;
  setPrevMonth: () => void;
  setCurrentMonth: () => void;
  setAllTime: () => void;
  setRange: (from: string, to: string) => void;
}

const DAY_NAMES = ["T2", "T3", "T4", "T5", "T6", "T7", "CN"] as const;
const MONTH_NAMES = [
  "Tháng 1", "Tháng 2", "Tháng 3", "Tháng 4", "Tháng 5", "Tháng 6",
  "Tháng 7", "Tháng 8", "Tháng 9", "Tháng 10", "Tháng 11", "Tháng 12",
];

function fmtIso(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function parseIso(s: string): Date | null {
  if (!s) return null;
  const [y, m, d] = s.split("-").map(Number);
  if (!y || !m || !d) return null;
  return new Date(y, m - 1, d);
}

function fmtDDMM(iso: string): string {
  const d = parseIso(iso);
  if (!d) return iso;
  return `${String(d.getDate()).padStart(2, "0")}/${String(d.getMonth() + 1).padStart(2, "0")}`;
}

function fmtFull(iso: string): string {
  const d = parseIso(iso);
  if (!d) return iso;
  return `${String(d.getDate()).padStart(2, "0")}/${String(d.getMonth() + 1).padStart(2, "0")}/${d.getFullYear()}`;
}

function buttonLabel(mode: FilterMode): string {
  if (mode.type === "all") return "Từ trước đến nay";
  if (mode.type === "recent") {
    if (mode.count === 1 && !mode.canExpand) return "Ngày gần nhất";
    return `${mode.count} ngày gần nhất`;
  }
  if (!mode.from || !mode.to) return "Chọn khoảng ngày";
  if (mode.from === mode.to) return fmtFull(mode.from);
  return `${fmtDDMM(mode.from)} – ${fmtFull(mode.to)}`;
}

export function DateRangePopover({
  mode,
  isPrevMonthActive,
  isCurrentMonthActive,
  setRecent,
  setPrevMonth,
  setCurrentMonth,
  setAllTime,
  setRange,
}: DateRangePopoverProps) {
  const [open, setOpen] = useState(false);
  const [pendingFrom, setPendingFrom] = useState<string | null>(null);
  const [viewMonth, setViewMonth] = useState<Date>(() => {
    if (mode.type === "range" && mode.to) {
      const d = parseIso(mode.to);
      if (d) return new Date(d.getFullYear(), d.getMonth(), 1);
    }
    const now = new Date();
    return new Date(now.getFullYear(), now.getMonth(), 1);
  });
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const popoverRef = useRef<HTMLDivElement | null>(null);
  const [triggerRect, setTriggerRect] = useState<DOMRect | null>(null);

  // Mỗi lần mở popover → nhảy view sang tháng chứa endpoint hiện tại
  // (UX: user vừa thấy ngay khoảng đang dùng, không phải bấm mũi tên tìm).
  useEffect(() => {
    if (!open) {
      setPendingFrom(null);
      return;
    }
    let target: Date | null = null;
    if (mode.type === "range" && mode.to) {
      target = parseIso(mode.to);
    } else if (mode.type === "recent") {
      target = new Date();
    }
    if (target) {
      setViewMonth(new Date(target.getFullYear(), target.getMonth(), 1));
    }
  }, [open, mode]);

  // Click outside + Esc — phải check cả trigger lẫn popover vì 2 cái
  // ở khác stacking context (portal).
  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (triggerRef.current?.contains(t)) return;
      if (popoverRef.current?.contains(t)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  // Update trigger rect mỗi khi mở / scroll / resize (popover dùng position
  // fixed bám theo trigger).
  useEffect(() => {
    if (!open) return;
    const update = () => {
      if (triggerRef.current) {
        setTriggerRect(triggerRef.current.getBoundingClientRect());
      }
    };
    update();
    window.addEventListener("scroll", update, true);
    window.addEventListener("resize", update);
    return () => {
      window.removeEventListener("scroll", update, true);
      window.removeEventListener("resize", update);
    };
  }, [open]);

  // 6×7 grid bắt đầu từ Thứ 2 của tuần chứa ngày 1.
  const monthCells = useMemo(() => {
    const first = new Date(viewMonth.getFullYear(), viewMonth.getMonth(), 1);
    const offset = (first.getDay() + 6) % 7; // 0=Sun..6=Sat → Mon=0..Sun=6
    const start = new Date(first.getFullYear(), first.getMonth(), 1 - offset);
    const cells: Array<{ date: Date; iso: string; inMonth: boolean }> = [];
    for (let i = 0; i < 42; i++) {
      const d = new Date(start.getFullYear(), start.getMonth(), start.getDate() + i);
      cells.push({
        date: d,
        iso: fmtIso(d),
        inMonth: d.getMonth() === viewMonth.getMonth(),
      });
    }
    return cells;
  }, [viewMonth]);

  const todayIso = fmtIso(new Date());

  // Range hiện tại để highlight trên lịch.
  let activeFrom: string | null = null;
  let activeTo: string | null = null;
  if (mode.type === "range") {
    activeFrom = mode.from || null;
    activeTo = mode.to || null;
  } else if (mode.type === "recent") {
    const today = new Date();
    const start = new Date(today.getFullYear(), today.getMonth(), today.getDate());
    start.setDate(start.getDate() - mode.count + 1);
    activeFrom = fmtIso(start);
    activeTo = fmtIso(today);
  }

  const close = () => {
    setOpen(false);
    setPendingFrom(null);
  };

  const handleDayClick = (iso: string) => {
    if (pendingFrom === null) {
      setPendingFrom(iso);
      return;
    }
    const a = pendingFrom < iso ? pendingFrom : iso;
    const b = pendingFrom < iso ? iso : pendingFrom;
    setRange(a, b);
    close();
  };

  const isEndpoint = (iso: string): boolean => {
    if (pendingFrom) return iso === pendingFrom;
    return iso === activeFrom || iso === activeTo;
  };

  const isInRange = (iso: string): boolean => {
    if (pendingFrom) return false; // chưa có ngày 2 → chỉ highlight pending
    if (!activeFrom || !activeTo) return false;
    return iso > activeFrom && iso < activeTo;
  };

  // Popover dimension cố định để compute clamp:
  const POPOVER_W = 148 + 296; // sidebar + calendar
  const POPOVER_H_EST = 360;
  let popoverTop = 0;
  let popoverLeft = 0;
  if (triggerRect) {
    const margin = 6;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    popoverTop = triggerRect.bottom + margin;
    popoverLeft = triggerRect.left;
    // Flip lên trên nếu thiếu chỗ phía dưới.
    if (popoverTop + POPOVER_H_EST > vh && triggerRect.top > POPOVER_H_EST) {
      popoverTop = triggerRect.top - POPOVER_H_EST - margin;
    }
    // Clamp ngang để không tràn phải.
    if (popoverLeft + POPOVER_W > vw - 8) {
      popoverLeft = Math.max(8, vw - POPOVER_W - 8);
    }
  }

  return (
    <div className="relative shrink-0">
      <button
        ref={triggerRef}
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={`flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-sm transition-colors ${
          open
            ? "border-shopee-500 bg-shopee-500/10 text-white"
            : "border-surface-8 bg-surface-1 text-white/90 hover:border-shopee-500/60"
        }`}
        title="Chọn ngày / khoảng ngày"
      >
        <span className="material-symbols-rounded text-base text-shopee-400">
          calendar_month
        </span>
        <span className="font-medium">{buttonLabel(mode)}</span>
        <span
          className={`material-symbols-rounded text-base text-white/40 transition-transform ${
            open ? "rotate-180" : ""
          }`}
        >
          expand_more
        </span>
      </button>

      {open && triggerRect && createPortal(
        <div
          ref={popoverRef}
          style={{
            position: "fixed",
            top: popoverTop,
            left: popoverLeft,
            zIndex: 1000,
          }}
          className="flex rounded-xl border border-surface-8 bg-surface-1 shadow-elev-24"
        >
          {/* Preset sidebar */}
          <div className="flex w-[148px] flex-col gap-0.5 border-r border-surface-8 p-2">
            <PresetButton
              active={
                mode.type === "recent" && !mode.canExpand && mode.count === 1
              }
              onClick={() => {
                setRecent(1);
                close();
              }}
            >
              Ngày gần nhất
            </PresetButton>
            <PresetButton
              active={
                mode.type === "recent" && !mode.canExpand && mode.count === 7
              }
              onClick={() => {
                setRecent(7);
                close();
              }}
            >
              7 ngày
            </PresetButton>
            <PresetButton
              active={
                mode.type === "recent" && !mode.canExpand && mode.count === 14
              }
              onClick={() => {
                setRecent(14);
                close();
              }}
            >
              14 ngày
            </PresetButton>
            <PresetButton
              active={
                mode.type === "recent" && !mode.canExpand && mode.count === 30
              }
              onClick={() => {
                setRecent(30);
                close();
              }}
            >
              30 ngày
            </PresetButton>
            <div className="my-1 h-px bg-surface-8" />
            <PresetButton
              active={isCurrentMonthActive}
              onClick={() => {
                setCurrentMonth();
                close();
              }}
            >
              Tháng này
            </PresetButton>
            <PresetButton
              active={isPrevMonthActive}
              onClick={() => {
                setPrevMonth();
                close();
              }}
            >
              Tháng trước
            </PresetButton>
            <div className="my-1 h-px bg-surface-8" />
            <PresetButton
              active={mode.type === "all"}
              onClick={() => {
                setAllTime();
                close();
              }}
            >
              Từ trước đến nay
            </PresetButton>
          </div>

          {/* Calendar */}
          <div className="w-[296px] p-3">
            <div className="mb-2 flex items-center justify-between">
              <button
                type="button"
                onClick={() =>
                  setViewMonth(
                    (d) => new Date(d.getFullYear(), d.getMonth() - 1, 1),
                  )
                }
                className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/70 hover:bg-white/10"
                aria-label="Tháng trước"
              >
                <span className="material-symbols-rounded text-base">
                  chevron_left
                </span>
              </button>
              <span className="text-sm font-semibold text-white/90">
                {MONTH_NAMES[viewMonth.getMonth()]} / {viewMonth.getFullYear()}
              </span>
              <button
                type="button"
                onClick={() =>
                  setViewMonth(
                    (d) => new Date(d.getFullYear(), d.getMonth() + 1, 1),
                  )
                }
                className="btn-ripple flex h-7 w-7 items-center justify-center rounded-full text-white/70 hover:bg-white/10"
                aria-label="Tháng sau"
              >
                <span className="material-symbols-rounded text-base">
                  chevron_right
                </span>
              </button>
            </div>

            <div className="mb-1 grid grid-cols-7 gap-0.5">
              {DAY_NAMES.map((d) => (
                <div
                  key={d}
                  className="py-1 text-center text-[10px] font-semibold uppercase tracking-wide text-white/40"
                >
                  {d}
                </div>
              ))}
            </div>

            <div className="grid grid-cols-7 gap-0.5">
              {monthCells.map((c, i) => {
                const endpoint = isEndpoint(c.iso) && c.inMonth;
                const inRange = isInRange(c.iso) && c.inMonth;
                const isToday = c.iso === todayIso;
                return (
                  <button
                    key={i}
                    type="button"
                    onClick={() => handleDayClick(c.iso)}
                    className={`relative flex h-8 items-center justify-center rounded text-sm transition-colors ${
                      !c.inMonth
                        ? "text-white/20 hover:bg-white/5"
                        : endpoint
                        ? "bg-shopee-500 font-bold text-white shadow-elev-2"
                        : inRange
                        ? "bg-shopee-500/25 text-shopee-100"
                        : "text-white/85 hover:bg-shopee-500/20"
                    } ${
                      isToday && !endpoint
                        ? "ring-1 ring-shopee-400/70"
                        : ""
                    }`}
                  >
                    {c.date.getDate()}
                  </button>
                );
              })}
            </div>

            <div className="mt-2.5 flex items-center justify-between gap-2 text-[11px] text-white/60">
              {pendingFrom ? (
                <>
                  <span className="truncate">
                    Bắt đầu: <b className="text-shopee-300">{fmtFull(pendingFrom)}</b> — chọn ngày kết thúc
                  </span>
                  <button
                    type="button"
                    onClick={() => {
                      setRange(pendingFrom, pendingFrom);
                      close();
                    }}
                    className="shrink-0 rounded-md bg-shopee-500 px-2.5 py-1 text-xs font-semibold text-white hover:bg-shopee-600"
                  >
                    Chỉ 1 ngày
                  </button>
                </>
              ) : (
                <span>
                  <b>1 click</b> ngày → <b>click ngày 2</b> = range. Chọn cùng ngày = 1 ngày.
                </span>
              )}
            </div>
          </div>
        </div>,
        document.body,
      )}
    </div>
  );
}

function PresetButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`btn-ripple rounded-md px-2.5 py-1.5 text-left text-xs font-medium transition-colors ${
        active
          ? "bg-shopee-500 text-white shadow-elev-1"
          : "text-white/75 hover:bg-shopee-500/15 hover:text-white"
      }`}
    >
      {children}
    </button>
  );
}
