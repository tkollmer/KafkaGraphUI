import { useState, useEffect } from "react";
import { useThemeStore } from "../store/themeStore";

const shortcuts = [
  { keys: [navigator.platform.includes("Mac") ? "\u2318" : "Ctrl", "K"], desc: "Open command palette", global: true },
  { keys: ["1"], desc: "Dashboard", global: true },
  { keys: ["2"], desc: "Pipeline view", global: true },
  { keys: ["3"], desc: "Applications view", global: true },
  { keys: ["4"], desc: "Topics view", global: true },
  { keys: ["5"], desc: "Consumer Groups", global: true },
  { keys: ["6"], desc: "Brokers view", global: true },
  { keys: ["7"], desc: "Schema Registry", global: true },
  { keys: ["8"], desc: "Connectors", global: true },
  { keys: ["9"], desc: "Settings", global: true },
  { keys: ["R"], desc: "Refresh current view", global: true },
  { keys: ["/"], desc: "Focus pipeline search", global: false },
  { keys: ["F"], desc: "Fit view (pipeline)", global: false },
  { keys: ["L"], desc: "Re-layout nodes", global: false },
  { keys: ["T"], desc: "Toggle dark/bright theme", global: true },
  { keys: ["Z"], desc: "Toggle fullscreen mode", global: true },
  { keys: ["Esc"], desc: "Close panel / go back", global: true },
  { keys: ["?"], desc: "Show this dialog", global: true },
];

export function KeyboardShortcuts() {
  const [open, setOpen] = useState(false);
  const { theme } = useThemeStore();
  const isBright = theme === "bright";

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (e.key === "?" && !e.metaKey && !e.ctrlKey) {
        e.preventDefault();
        setOpen((prev) => !prev);
      }
      if (e.key === "Escape" && open) {
        setOpen(false);
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open]);

  if (!open) return null;

  return (
    <>
      <div className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm" onClick={() => setOpen(false)} />
      <div className={`fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 z-50 w-full max-w-md rounded-2xl border shadow-2xl overflow-hidden ${
        isBright ? "bg-white border-slate-200" : "bg-slate-900 border-slate-700/60"
      }`}>
        <div className={`flex items-center justify-between px-5 py-4 border-b ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
          <h2 className={`text-sm font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Keyboard Shortcuts</h2>
          <button
            onClick={() => setOpen(false)}
            className={`w-7 h-7 rounded-md flex items-center justify-center transition-colors cursor-pointer ${
              isBright ? "text-slate-400 hover:text-slate-700 hover:bg-slate-100" : "text-slate-500 hover:text-white hover:bg-slate-800"
            }`}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </div>
        <div className="px-5 py-4 space-y-2">
          {shortcuts.map((s) => (
            <div key={s.desc} className={`flex items-center justify-between py-2 px-3 rounded-xl ${
              isBright ? "hover:bg-slate-50" : "hover:bg-slate-800/30"
            }`}>
              <span className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>{s.desc}</span>
              <div className="flex items-center gap-1">
                {s.keys.map((k) => (
                  <kbd
                    key={k}
                    className={`min-w-[24px] h-6 px-1.5 rounded-md border text-[11px] font-mono font-medium flex items-center justify-center ${
                      isBright
                        ? "bg-slate-100 border-slate-200 text-slate-500 shadow-sm"
                        : "bg-slate-800 border-slate-700 text-slate-400"
                    }`}
                  >
                    {k}
                  </kbd>
                ))}
              </div>
            </div>
          ))}
        </div>
        <div className={`px-5 py-3 border-t text-[11px] ${isBright ? "border-slate-200/60 text-slate-400" : "border-slate-700/40 text-slate-500"}`}>
          Press <kbd className={`px-1 py-0.5 rounded border font-mono ${isBright ? "bg-slate-100 border-slate-200" : "bg-slate-800 border-slate-700"}`}>?</kbd> to toggle this dialog
        </div>
      </div>
    </>
  );
}
