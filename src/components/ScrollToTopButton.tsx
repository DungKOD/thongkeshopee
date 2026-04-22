import { useEffect, useState, type RefObject } from "react";

interface ScrollToTopButtonProps {
  /// Container có scroll. Null/undefined = window (page-level scroll).
  targetRef?: RefObject<HTMLElement | null>;
  /// ScrollTop threshold (px) để hiện nút. Default 400.
  threshold?: number;
  /// CSS class tuỳ biến vị trí. Default `fixed bottom-6 right-6` cho window;
  /// dialog nên pass `absolute bottom-4 right-4` để chỉ hiện trong dialog scope.
  className?: string;
}

/// Nút "Về đầu" auto-hiện khi scroll sâu quá threshold. Click → smooth scroll
/// về top. Dùng cho page-level scroll (window) hoặc container riêng (dialog).
export function ScrollToTopButton({
  targetRef,
  threshold = 400,
  className,
}: ScrollToTopButtonProps) {
  const [show, setShow] = useState(false);

  useEffect(() => {
    const target: HTMLElement | Window = targetRef?.current ?? window;
    const getScrollTop = (): number => {
      if (targetRef?.current) return targetRef.current.scrollTop;
      return window.scrollY || document.documentElement.scrollTop;
    };
    const onScroll = () => setShow(getScrollTop() > threshold);
    target.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
    return () => target.removeEventListener("scroll", onScroll);
  }, [targetRef, threshold]);

  if (!show) return null;

  const scrollToTop = () => {
    const el = targetRef?.current;
    if (el) {
      el.scrollTo({ top: 0, behavior: "smooth" });
    } else {
      window.scrollTo({ top: 0, behavior: "smooth" });
    }
  };

  // Default position: bottom-24 để không đè lên DevCredit floating (bottom-4).
  // Caller có thể override qua className cho dialog-scope (vd absolute).
  const posCls = className ?? "fixed bottom-24 right-6";
  return (
    <button
      type="button"
      onClick={scrollToTop}
      aria-label="Về đầu"
      title="Về đầu trang"
      className={`${posCls} z-40 flex h-11 w-11 items-center justify-center rounded-full bg-shopee-500 text-white shadow-elev-4 transition-transform hover:scale-110 hover:bg-shopee-600 focus:outline-none focus:ring-2 focus:ring-shopee-300`}
    >
      <span className="material-symbols-rounded">keyboard_arrow_up</span>
    </button>
  );
}
