import { useThemeStore } from "../store/themeStore";

export function SkeletonRow({ cols = 4 }: { cols?: number }) {
  const isBright = useThemeStore((s) => s.theme === "bright");
  return (
    <div className="flex items-center gap-4 py-3">
      {Array.from({ length: cols }).map((_, i) => (
        <div
          key={i}
          className={`h-4 rounded-lg animate-pulse ${
            i === 0 ? "w-40" : "w-20"
          } ${isBright ? "bg-slate-200/80" : "bg-slate-800/60"}`}
        />
      ))}
    </div>
  );
}

export function SkeletonCard() {
  const isBright = useThemeStore((s) => s.theme === "bright");
  return (
    <div className={`rounded-2xl border p-6 space-y-4 animate-pulse ${
      isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"
    }`}>
      <div className={`h-4 w-32 rounded-lg ${isBright ? "bg-slate-200/80" : "bg-slate-800/60"}`} />
      <div className="space-y-3">
        <div className={`h-3 w-full rounded-lg ${isBright ? "bg-slate-200/60" : "bg-slate-800/40"}`} />
        <div className={`h-3 w-3/4 rounded-lg ${isBright ? "bg-slate-200/60" : "bg-slate-800/40"}`} />
        <div className={`h-3 w-1/2 rounded-lg ${isBright ? "bg-slate-200/60" : "bg-slate-800/40"}`} />
      </div>
    </div>
  );
}

export function SkeletonTable({ rows = 5, cols = 4 }: { rows?: number; cols?: number }) {
  const isBright = useThemeStore((s) => s.theme === "bright");
  return (
    <div className={`rounded-2xl border overflow-hidden ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
      {/* Header */}
      <div className={`flex items-center gap-4 px-5 py-3 border-b ${isBright ? "border-slate-200/60 bg-slate-50/50" : "border-slate-700/30 bg-slate-800/20"}`}>
        {Array.from({ length: cols }).map((_, i) => (
          <div
            key={i}
            className={`h-3 rounded animate-pulse ${
              i === 0 ? "w-28" : "w-16"
            } ${isBright ? "bg-slate-200/80" : "bg-slate-700/60"}`}
          />
        ))}
      </div>
      {/* Rows */}
      {Array.from({ length: rows }).map((_, r) => (
        <div
          key={r}
          className={`flex items-center gap-4 px-5 py-3 border-b last:border-0 ${
            isBright ? "border-slate-100" : "border-slate-800/30"
          }`}
        >
          {Array.from({ length: cols }).map((_, i) => (
            <div
              key={i}
              className={`h-3 rounded animate-pulse ${
                i === 0 ? "w-40" : "w-20"
              } ${isBright ? "bg-slate-200/60" : "bg-slate-800/40"}`}
              style={{ animationDelay: `${r * 100 + i * 50}ms` }}
            />
          ))}
        </div>
      ))}
    </div>
  );
}
