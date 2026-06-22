import { useCallback, useSyncExternalStore } from "react";

const LS_KEY = "bookmark:row";

// Module-level store: 1 nguồn duy nhất, không tạo N listeners per row.
// Trước đây mỗi useBookmark(rowKey) đăng ký 2 window listener + 1 localStorage
// read sync trên mount. Với 30 ngày × 30 rows = 900 rows → 1800 window
// listeners + 900 localStorage reads chiếm main thread khi load. Mỗi toggle
// → 900 listeners fire → 900× setState cascade.
//
// Pattern mới: 1 listeners Set ở module scope, mỗi useBookmark dùng selector
// trả `current === rowKey` (boolean) qua useSyncExternalStore. React chỉ
// re-render row khi boolean thay đổi → 2 rows tối đa (row mất bookmark + row
// nhận bookmark), tất cả row khác bail out qua snapshot equality check.
let current: string | null = (() => {
  try {
    return localStorage.getItem(LS_KEY);
  } catch {
    return null;
  }
})();

const listeners = new Set<() => void>();

function subscribe(cb: () => void): () => void {
  listeners.add(cb);
  return () => {
    listeners.delete(cb);
  };
}

function emit() {
  for (const l of listeners) l();
}

function setCurrent(next: string | null) {
  if (current === next) return;
  current = next;
  try {
    if (next === null) localStorage.removeItem(LS_KEY);
    else localStorage.setItem(LS_KEY, next);
  } catch {
    /* quota */
  }
  emit();
}

export function useBookmark(rowKey: string) {
  // Per-row selector: chỉ re-render khi boolean cho row NÀY đổi. Các row khác
  // nhận notify nhưng snapshot không đổi → React skip re-render.
  const getSnapshot = useCallback(() => current === rowKey, [rowKey]);
  const isBookmarked = useSyncExternalStore(subscribe, getSnapshot);

  const toggle = useCallback(() => {
    setCurrent(current === rowKey ? null : rowKey);
  }, [rowKey]);

  const set = useCallback(
    (value: boolean) => {
      if (value) {
        if (current !== rowKey) setCurrent(rowKey);
      } else {
        if (current === rowKey) setCurrent(null);
      }
    },
    [rowKey],
  );

  return { isBookmarked, toggle, set };
}
