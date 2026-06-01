import { useCallback, useEffect, useState } from "react";

const LS_KEY = "bookmark:row";
const EV = "bookmark-changed";

export function useBookmark(rowKey: string) {
  const [isBookmarked, setIsBookmarked] = useState(() => {
    try { return localStorage.getItem(LS_KEY) === rowKey; }
    catch { return false; }
  });

  useEffect(() => {
    const sync = () => {
      try { setIsBookmarked(localStorage.getItem(LS_KEY) === rowKey); }
      catch { setIsBookmarked(false); }
    };
    window.addEventListener(EV, sync);
    window.addEventListener("storage", sync);
    return () => {
      window.removeEventListener(EV, sync);
      window.removeEventListener("storage", sync);
    };
  }, [rowKey]);

  const toggle = useCallback(() => {
    try {
      if (localStorage.getItem(LS_KEY) === rowKey) {
        localStorage.removeItem(LS_KEY);
      } else {
        localStorage.setItem(LS_KEY, rowKey);
      }
      window.dispatchEvent(new Event(EV));
    } catch { /* quota */ }
  }, [rowKey]);

  return { isBookmarked, toggle };
}
