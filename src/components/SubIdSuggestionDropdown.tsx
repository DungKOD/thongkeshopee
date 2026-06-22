import { useEffect, useState, type RefObject } from "react";
import { createPortal } from "react-dom";

interface SubIdSuggestionDropdownProps {
  /** Element bám theo (thường là input). Dropdown align dưới và cùng width. */
  anchorRef: RefObject<HTMLElement | null>;
  open: boolean;
  suggestions: string[];
  onPick: (sub: string) => void;
}

/// Portal vào `document.body` để thoát stacking context của sticky bar +
/// sticky table headers (z-index nội bộ không đủ để đè lên).
export function SubIdSuggestionDropdown({
  anchorRef,
  open,
  suggestions,
  onPick,
}: SubIdSuggestionDropdownProps) {
  const [rect, setRect] = useState<DOMRect | null>(null);

  useEffect(() => {
    if (!open) return;
    const update = () => {
      if (anchorRef.current) {
        setRect(anchorRef.current.getBoundingClientRect());
      }
    };
    update();
    window.addEventListener("scroll", update, true);
    window.addEventListener("resize", update);
    return () => {
      window.removeEventListener("scroll", update, true);
      window.removeEventListener("resize", update);
    };
  }, [open, anchorRef]);

  if (!open || suggestions.length === 0 || !rect) return null;

  return createPortal(
    <ul
      style={{
        position: "fixed",
        top: rect.bottom + 4,
        left: rect.left,
        width: rect.width,
        zIndex: 1000,
      }}
      className="max-h-[400px] overflow-y-auto rounded-lg border border-surface-8 bg-surface-4 shadow-elev-16"
    >
      {suggestions.map((s) => (
        <li key={s}>
          <button
            type="button"
            onMouseDown={(e) => {
              e.preventDefault();
              onPick(s);
            }}
            className="block w-full truncate px-3 py-1.5 text-left font-mono text-xs text-white/85 hover:bg-shopee-900/30 hover:text-shopee-200"
            title={s}
          >
            {s}
          </button>
        </li>
      ))}
    </ul>,
    document.body,
  );
}
