import { useEffect, useRef } from "react";
import { useThemeStore } from "../store/themeStore";

export interface ContextMenuItem {
  label: string;
  icon?: string;
  danger?: boolean;
  disabled?: boolean;
  separator?: boolean;
  onClick: () => void;
}

interface Props {
  x: number;
  y: number;
  items: ContextMenuItem[];
  onClose: () => void;
}

export function ContextMenu({ x, y, items, onClose }: Props) {
  const ref = useRef<HTMLDivElement>(null);
  const isBright = useThemeStore((s) => s.theme === "bright");

  // Close on outside click or escape
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as HTMLElement)) onClose();
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("mousedown", handleClick);
    document.addEventListener("keydown", handleKey);
    return () => {
      document.removeEventListener("mousedown", handleClick);
      document.removeEventListener("keydown", handleKey);
    };
  }, [onClose]);

  // Adjust position to stay within viewport
  useEffect(() => {
    if (!ref.current) return;
    const rect = ref.current.getBoundingClientRect();
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    if (rect.right > vw) ref.current.style.left = `${x - rect.width}px`;
    if (rect.bottom > vh) ref.current.style.top = `${y - rect.height}px`;
  }, [x, y]);

  return (
    <div
      ref={ref}
      className={`fixed z-[100] min-w-[180px] rounded-xl border shadow-2xl backdrop-blur-xl py-1.5 animate-in fade-in zoom-in-95 duration-100 ${
        isBright
          ? "bg-white/95 border-slate-200/80 shadow-black/10"
          : "bg-slate-900/95 border-slate-700/60 shadow-black/50"
      }`}
      style={{ left: x, top: y }}
    >
      {items.map((item, i) => {
        if (item.separator) {
          return (
            <div
              key={`sep-${i}`}
              className={`my-1 mx-2 border-t ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}
            />
          );
        }
        return (
          <button
            key={i}
            onClick={() => {
              if (!item.disabled) {
                item.onClick();
                onClose();
              }
            }}
            disabled={item.disabled}
            className={`w-full flex items-center gap-2.5 px-3.5 py-2 text-[12px] font-medium transition-colors cursor-pointer text-left ${
              item.disabled
                ? isBright ? "text-slate-300 cursor-not-allowed" : "text-slate-600 cursor-not-allowed"
                : item.danger
                  ? isBright
                    ? "text-red-600 hover:bg-red-50"
                    : "text-red-400 hover:bg-red-500/10"
                  : isBright
                    ? "text-slate-700 hover:bg-slate-100"
                    : "text-slate-300 hover:bg-slate-800/60"
            }`}
          >
            {item.icon && <span className="text-sm w-4 text-center">{item.icon}</span>}
            <span>{item.label}</span>
          </button>
        );
      })}
    </div>
  );
}
