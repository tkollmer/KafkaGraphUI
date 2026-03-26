import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useThemeStore } from "../store/themeStore";

function ProducerNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const msgPerSec = Number(d.msgPerSec || 0);
  const topicCount = Number(d.topicCount || 0);
  const isActive = msgPerSec > 0;
  const isInactive = d.status === "inactive";
  const isDimmed = Boolean(d._dimmed);
  const isSearchMatch = Boolean(d._searchMatch);
  const isBright = useThemeStore((s) => s.theme === "bright");

  return (
    <div
      className={`
        relative rounded-2xl border px-5 py-4 min-w-[220px] max-w-[260px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isInactive ? "opacity-40 grayscale" : ""}
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${isSearchMatch ? "ring-2 ring-yellow-400/70 ring-offset-2 ring-offset-transparent" : ""}
        ${selected
          ? isBright
            ? "border-emerald-500 bg-white text-slate-800 ring-2 ring-emerald-400/50 ring-offset-2 ring-offset-slate-100"
            : "border-emerald-400 bg-gradient-to-br from-emerald-950/90 to-slate-900/90 text-white ring-2 ring-emerald-400/50 ring-offset-2 ring-offset-slate-950"
          : isBright
            ? "border-emerald-200/80 bg-white/95 text-slate-800 hover:border-emerald-400 hover:shadow-lg"
            : "border-emerald-500/40 bg-gradient-to-br from-emerald-950/80 to-slate-900/80 text-white hover:border-emerald-400/70"
        }
      `}
    >
      <Handle type="source" position={Position.Right} className={`!w-3 !h-3 !border-2 !-right-1.5 ${isBright ? "!bg-emerald-500 !border-emerald-300" : "!bg-emerald-400 !border-emerald-600"}`} />

      {/* Activity */}
      {isActive && (
        <div className="absolute -top-1 -right-1 w-3 h-3">
          <span className="absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75 animate-ping" />
          <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500" />
        </div>
      )}

      <div className="flex items-center gap-2 mb-3">
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${isBright ? "bg-emerald-100 text-emerald-600" : "bg-emerald-500/30 text-emerald-300"}`}>
          P
        </div>
        <div className="min-w-0">
          <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-emerald-500/70" : "text-emerald-400/70"}`}>Producer</div>
          <div className="font-semibold text-[14px] truncate" title={label}>{label}</div>
        </div>
      </div>

      <div className={`grid ${topicCount > 0 ? "grid-cols-2" : "grid-cols-1"} gap-2 text-center`}>
        <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
          <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Rate</div>
          <div className={`text-[12px] font-bold font-mono ${isActive ? "text-green-500" : isBright ? "text-slate-400" : "text-slate-500"}`}>
            {msgPerSec > 0 ? `${msgPerSec}/s` : "idle"}
          </div>
        </div>
        {topicCount > 0 && (
          <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
            <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Topics</div>
            <div className={`text-[12px] font-bold ${isBright ? "text-emerald-600" : "text-emerald-300"}`}>{topicCount}</div>
          </div>
        )}
      </div>

      {/* Throughput bar */}
      {isActive && (
        <div className={`mt-2 rounded-full overflow-hidden h-1 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
          <div
            className="h-full rounded-full bg-emerald-500 transition-all duration-700"
            style={{ width: `${Math.min(100, msgPerSec > 0 ? Math.log10(msgPerSec + 1) * 33 : 0)}%` }}
          />
        </div>
      )}

      {Boolean(d.inferred) && (
        <div className={`text-[10px] text-center mt-2 italic ${isBright ? "text-emerald-400" : "text-emerald-500/60"}`}>inferred from offsets</div>
      )}
    </div>
  );
}

export const ProducerNode = memo(ProducerNodeComponent);
