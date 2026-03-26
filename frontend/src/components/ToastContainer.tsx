import { useEffect, useState } from "react";
import { useToastStore, type Toast } from "../store/toastStore";
import { useThemeStore } from "../store/themeStore";

function ToastIcon({ type }: { type: Toast["type"] }) {
  if (type === "success") {
    return (
      <svg className="w-4 h-4 text-emerald-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
      </svg>
    );
  }
  if (type === "error") {
    return (
      <svg className="w-4 h-4 text-red-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
      </svg>
    );
  }
  return (
    <svg className="w-4 h-4 text-blue-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M12 2a10 10 0 100 20 10 10 0 000-20z" />
    </svg>
  );
}

function ToastItem({ toast }: { toast: Toast }) {
  const { theme } = useThemeStore();
  const removeToast = useToastStore((s) => s.removeToast);
  const isBright = theme === "bright";
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    // Trigger slide-in on mount
    const frame = requestAnimationFrame(() => setVisible(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  const borderColor =
    toast.type === "success"
      ? isBright ? "border-emerald-300/60" : "border-emerald-500/30"
      : toast.type === "error"
        ? isBright ? "border-red-300/60" : "border-red-500/30"
        : isBright ? "border-blue-300/60" : "border-blue-500/30";

  const bgColor =
    toast.type === "success"
      ? isBright ? "bg-emerald-50/95" : "bg-emerald-950/80"
      : toast.type === "error"
        ? isBright ? "bg-red-50/95" : "bg-red-950/80"
        : isBright ? "bg-blue-50/95" : "bg-blue-950/80";

  const textColor = isBright ? "text-slate-700" : "text-slate-200";
  const closeColor = isBright
    ? "text-slate-400 hover:text-slate-600 hover:bg-slate-200/50"
    : "text-slate-500 hover:text-slate-300 hover:bg-slate-700/50";

  return (
    <div
      className={`flex items-start gap-2.5 px-4 py-3 rounded-xl border shadow-lg backdrop-blur-xl transition-all duration-300 ease-out ${borderColor} ${bgColor} ${
        visible ? "translate-x-0 opacity-100" : "translate-x-full opacity-0"
      }`}
      style={{ maxWidth: 380, minWidth: 280 }}
    >
      <ToastIcon type={toast.type} />
      <span className={`text-sm flex-1 ${textColor}`}>{toast.message}</span>
      <button
        onClick={() => removeToast(toast.id)}
        className={`w-5 h-5 rounded-md flex items-center justify-center transition-colors shrink-0 cursor-pointer ${closeColor}`}
      >
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
          <path strokeLinecap="round" d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
}

export function ToastContainer() {
  const toasts = useToastStore((s) => s.toasts);

  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-6 right-6 z-[100] flex flex-col gap-2.5">
      {toasts.map((toast) => (
        <ToastItem key={toast.id} toast={toast} />
      ))}
    </div>
  );
}
