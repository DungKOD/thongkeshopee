import { ShortcutButton } from "./AppShellButtons";
import type { FilterMode } from "../hooks/useFilterMode";

interface AppFilterShortcutsProps {
  filterMode: FilterMode;
  isPrevMonthActive: boolean;
  isCurrentMonthActive: boolean;
  setRecentDays: (n: number) => void;
  setPrevMonth: () => void;
  setCurrentMonth: () => void;
  setAllTime: () => void;
}

/** Strip "recent days" shortcut chips + tháng trước/này + all-time. */
export function AppFilterShortcuts({
  filterMode,
  isPrevMonthActive,
  isCurrentMonthActive,
  setRecentDays,
  setPrevMonth,
  setCurrentMonth,
  setAllTime,
}: AppFilterShortcutsProps) {
  const isRecent = (n: number) =>
    filterMode.type === "recent" &&
    !filterMode.canExpand &&
    filterMode.count === n;

  return (
    <div className="flex shrink-0 flex-wrap items-center gap-1">
      <ShortcutButton active={isRecent(1)} onClick={() => setRecentDays(1)}>
        Ngày gần nhất
      </ShortcutButton>
      <ShortcutButton active={isRecent(7)} onClick={() => setRecentDays(7)}>
        7 ngày
      </ShortcutButton>
      <ShortcutButton active={isRecent(14)} onClick={() => setRecentDays(14)}>
        14 ngày
      </ShortcutButton>
      <ShortcutButton active={isRecent(30)} onClick={() => setRecentDays(30)}>
        30 ngày
      </ShortcutButton>
      <ShortcutButton active={isPrevMonthActive} onClick={setPrevMonth}>
        Tháng trước
      </ShortcutButton>
      <ShortcutButton active={isCurrentMonthActive} onClick={setCurrentMonth}>
        Tháng này
      </ShortcutButton>
      <ShortcutButton active={filterMode.type === "all"} onClick={setAllTime}>
        Từ trước đến nay
      </ShortcutButton>
    </div>
  );
}
