import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";

function ProducerNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const msgPerSec = Number(d.msgPerSec || 0);
  const isActive = msgPerSec > 0;
  const isInactive = d.status === "inactive";
  const isDimmed = Boolean(d._dimmed);

  return (
    <div
      className={`
        relative rounded-2xl border px-5 py-4 min-w-[200px] max-w-[240px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isInactive ? "opacity-40 grayscale" : ""}
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${selected
          ? "border-emerald-400 bg-gradient-to-br from-emerald-950/90 to-slate-900/90 text-white ring-2 ring-emerald-400/50 ring-offset-2 ring-offset-slate-950"
          : "border-emerald-500/40 bg-gradient-to-br from-emerald-950/80 to-slate-900/80 text-white hover:border-emerald-400/70"
        }
      `}
    >
      <Handle type="source" position={Position.Right} className="!w-3 !h-3 !bg-emerald-400 !border-2 !border-emerald-600 !-right-1.5" />

      {/* Activity */}
      {isActive && (
        <div className="absolute -top-1 -right-1 w-3 h-3">
          <span className="absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75 animate-ping" />
          <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500" />
        </div>
      )}

      <div className="flex items-center gap-2 mb-3">
        <div className="w-7 h-7 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 bg-emerald-500/30 text-emerald-300">
          P
        </div>
        <div className="min-w-0">
          <div className="text-[10px] uppercase tracking-wider text-emerald-400/70 font-medium">Producer</div>
          <div className="font-semibold text-sm truncate" title={label}>{label}</div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-2 text-center">
        <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
          <div className="text-[10px] text-slate-400">Rate</div>
          <div className={`text-xs font-bold font-mono ${isActive ? "text-green-400" : "text-slate-500"}`}>
            {msgPerSec > 0 ? `${msgPerSec} msg/s` : "idle"}
          </div>
        </div>
      </div>

      {Boolean(d.inferred) && (
        <div className="text-[9px] text-emerald-500/60 text-center mt-2 italic">inferred from offsets</div>
      )}
    </div>
  );
}

export const ProducerNode = memo(ProducerNodeComponent);
