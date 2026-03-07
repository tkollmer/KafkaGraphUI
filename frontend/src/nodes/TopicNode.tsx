import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";

function TopicNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const partitions = Number(d.partitions || 0);
  const msgPerSec = Number(d.msgPerSec || 0);
  const totalMessages = Number(d.totalMessages || 0);
  const isDenied = d.status === "access_denied";
  const isInactive = d.status === "inactive";
  const isDimmed = Boolean(d._dimmed);
  const isActive = msgPerSec > 0;

  return (
    <div
      className={`
        relative group rounded-2xl border px-5 py-4 min-w-[220px] max-w-[260px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isInactive ? "opacity-40 grayscale" : ""}
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${isDenied
          ? "border-slate-600/50 bg-slate-800/40 text-slate-500"
          : selected
            ? "border-indigo-400 bg-gradient-to-br from-indigo-950/90 to-slate-900/90 text-white ring-2 ring-indigo-400/50 ring-offset-2 ring-offset-slate-950 shadow-indigo-500/20 shadow-2xl"
            : "border-indigo-500/40 bg-gradient-to-br from-indigo-950/80 to-slate-900/80 text-white hover:border-indigo-400/70 hover:shadow-indigo-500/10 hover:shadow-2xl"
        }
      `}
    >
      <Handle type="target" position={Position.Left} className="!w-3 !h-3 !bg-indigo-400 !border-2 !border-indigo-600 !-left-1.5" />
      <Handle type="source" position={Position.Right} className="!w-3 !h-3 !bg-indigo-400 !border-2 !border-indigo-600 !-right-1.5" />

      {/* Activity indicator */}
      {isActive && !isDenied && (
        <div className="absolute -top-1 -right-1 w-3 h-3">
          <span className="absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75 animate-ping" />
          <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500" />
        </div>
      )}

      {/* Header */}
      <div className="flex items-center gap-2 mb-3">
        <div className={`w-7 h-7 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${isDenied ? "bg-slate-700 text-slate-500" : "bg-indigo-500/30 text-indigo-300"}`}>
          T
        </div>
        <div className="min-w-0">
          <div className="text-[10px] uppercase tracking-wider text-indigo-400/70 font-medium">Topic</div>
          <div className="font-semibold text-sm truncate" title={label}>{label}</div>
        </div>
        {isDenied && <span className="text-slate-500 ml-auto text-sm" title="Access Denied">&#x1F512;</span>}
      </div>

      {/* Metrics */}
      {!isDenied && (
        <div className="grid grid-cols-3 gap-2 text-center">
          <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
            <div className="text-[10px] text-slate-400">Partitions</div>
            <div className="text-xs font-bold text-indigo-300">{partitions}</div>
          </div>
          <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
            <div className="text-[10px] text-slate-400">Rate</div>
            <div className={`text-xs font-bold font-mono ${isActive ? "text-green-400" : "text-slate-500"}`}>
              {msgPerSec > 0 ? `${msgPerSec}/s` : "idle"}
            </div>
          </div>
          <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
            <div className="text-[10px] text-slate-400">Total</div>
            <div className="text-xs font-bold text-slate-300">{fmt(totalMessages)}</div>
          </div>
        </div>
      )}
    </div>
  );
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export const TopicNode = memo(TopicNodeComponent);
