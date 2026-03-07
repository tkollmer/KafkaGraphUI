import { useEffect, useRef } from "react";

interface Props {
  title: string;
  open: boolean;
  onClose: () => void;
  children: React.ReactNode;
}

export function Modal({ title, open, onClose, children }: Props) {
  const backdropRef = useRef<HTMLDivElement>(null);

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
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
      <div className="relative w-full max-w-lg mx-4 rounded-2xl bg-slate-900/95 backdrop-blur-xl border border-slate-700/50 shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-slate-700/50">
          <h3 className="text-sm font-semibold text-white">{title}</h3>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-lg flex items-center justify-center text-slate-400 hover:text-white hover:bg-slate-800 transition-colors cursor-pointer"
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
