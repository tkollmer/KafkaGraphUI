import { useEffect, useRef } from "react";
import { useThemeStore } from "../store/themeStore";

interface Props {
  title: string;
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
}

export function Modal({ title, open, onClose, children }: Props) {
  const backdropRef = useRef<HTMLDivElement>(null);
  const { theme } = useThemeStore();
  const isBright = theme === "bright";

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      ref={backdropRef}
      className="fixed inset-0 z-[60] flex items-center justify-center"
      onClick={(e) => {
        if (e.target === backdropRef.current) onClose();
      }}
    >
      <div className={`absolute inset-0 backdrop-blur-sm ${isBright ? "bg-black/20" : "bg-black/60"}`} />
      <div className={`relative w-full max-w-lg mx-4 rounded-2xl backdrop-blur-xl border shadow-2xl transition-colors ${
        isBright
          ? "bg-white/98 border-slate-200/80"
          : "bg-slate-900/95 border-slate-700/50"
      }`}>
        {/* Header */}
        <div className={`flex items-center justify-between px-6 py-4 border-b ${isBright ? "border-slate-200/50" : "border-slate-700/50"}`}>
          <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>{title}</h3>
          <button
            onClick={onClose}
            className={`w-8 h-8 rounded-lg flex items-center justify-center transition-colors cursor-pointer ${
              isBright ? "text-slate-400 hover:text-slate-700 hover:bg-slate-100" : "text-slate-400 hover:text-white hover:bg-slate-800"
            }`}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="px-6 py-4">
          {children}
        </div>
      </div>
    </div>
  );
}
