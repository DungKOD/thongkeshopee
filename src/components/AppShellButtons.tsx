import type { ReactNode } from "react";

interface ShortcutButtonProps {
  active?: boolean;
  onClick: () => void;
  children: ReactNode;
}

export function ShortcutButton({ active, onClick, children }: ShortcutButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`btn-ripple rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
        active
          ? "border border-shopee-500 bg-shopee-500/20 text-shopee-200"
          : "border border-surface-8 bg-surface-1 text-white/80 hover:border-shopee-500/50 hover:bg-shopee-900/20 hover:text-shopee-200"
      }`}
    >
      {children}
    </button>
  );
}

interface TabButtonProps {
  active: boolean;
  onClick: () => void;
  icon: string;
  label: string;
}

export function TabButton({ active, onClick, icon, label }: TabButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`btn-ripple flex items-center gap-1.5 rounded-t-lg px-4 py-2 text-sm font-medium transition-colors ${
        active
          ? "bg-surface-0 text-shopee-300"
          : "text-white/70 hover:bg-white/10 hover:text-white"
      }`}
    >
      <span className="material-symbols-rounded text-base">{icon}</span>
      {label}
    </button>
  );
}
