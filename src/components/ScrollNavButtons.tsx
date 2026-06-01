import { useEffect, useState } from "react";

export function ScrollNavButtons() {
  const [show, setShow] = useState(false);

  useEffect(() => {
    const onScroll = () => {
      setShow(document.documentElement.scrollHeight > window.innerHeight + 100);
    };
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll, { passive: true });
    return () => {
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
    };
  }, []);

  if (!show) return null;

  return (
    <div className="fixed right-4 z-40 flex flex-col gap-1.5" style={{ top: "50%", transform: "translateY(-50%)" }}>
      <button
        type="button"
        onClick={() => window.scrollTo({ top: 0, behavior: "smooth" })}
        aria-label="Lên đầu danh sách"
        title="Lên đầu danh sách"
        className="flex h-9 w-9 items-center justify-center rounded-full bg-surface-4/90 text-white/70 shadow-elev-2 backdrop-blur transition-all hover:scale-110 hover:bg-shopee-500 hover:text-white hover:shadow-elev-4"
      >
        <span className="material-symbols-rounded text-lg">keyboard_arrow_up</span>
      </button>
      <button
        type="button"
        onClick={() =>
          window.scrollTo({
            top: document.documentElement.scrollHeight,
            behavior: "smooth",
          })
        }
        aria-label="Xuống cuối danh sách"
        title="Xuống cuối danh sách"
        className="flex h-9 w-9 items-center justify-center rounded-full bg-surface-4/90 text-white/70 shadow-elev-2 backdrop-blur transition-all hover:scale-110 hover:bg-shopee-500 hover:text-white hover:shadow-elev-4"
      >
        <span className="material-symbols-rounded text-lg">keyboard_arrow_down</span>
      </button>
    </div>
  );
}
