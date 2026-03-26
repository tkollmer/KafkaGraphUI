import { useState, useEffect } from "react";
import { useThemeStore } from "../store/themeStore";

function relativeTime(ts: number): string {
  const diff = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (diff < 5) return "just now";
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

export function FreshnessIndicator({ timestamp }: { timestamp: number | null }) {
  const isBright = useThemeStore((s) => s.theme === "bright");
  const [, tick] = useState(0);

  useEffect(() => {
    const id = setInterval(() => tick((n) => n + 1), 5000);
    return () => clearInterval(id);
  }, []);

  if (!timestamp) return null;

  const age = Date.now() - timestamp;
  const isStale = age > 30_000;

  return (
    <span
      className={`text-[11px] tabular-nums ${
        isStale
          ? "text-amber-500"
          : isBright
            ? "text-slate-400"
            : "text-slate-500"
      }`}
      title={new Date(timestamp).toLocaleTimeString()}
    >
      Updated {relativeTime(timestamp)}
    </span>
  );
}
