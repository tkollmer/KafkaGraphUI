import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";

function ServiceNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const members = Number(d.members || 0);
  const totalLag = Number(d.totalLag || 0);
  const lagWarning = Boolean(d.lagWarning);
  const consumes = (d.consumes as string[]) || [];
  const produces = (d.produces as string[]) || [];
  const isDenied = d.status === "access_denied";
  const isInactive = d.status === "inactive";
  const isDimmed = Boolean(d._dimmed);

  return (
    <div
      className={`
        relative rounded-2xl border px-5 py-4 min-w-[220px] max-w-[260px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isInactive ? "opacity-40 grayscale" : ""}
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${isDenied
          ? "border-slate-600/50 bg-slate-800/40 text-slate-500"
          : lagWarning
            ? "border-red-500/60 bg-gradient-to-br from-red-950/80 to-slate-900/90 text-white lag-pulse"
            : selected
              ? "border-cyan-400 bg-gradient-to-br from-cyan-950/90 to-slate-900/90 text-white ring-2 ring-cyan-400/50 ring-offset-2 ring-offset-slate-950 shadow-cyan-500/20 shadow-2xl"
              : "border-cyan-500/40 bg-gradient-to-br from-cyan-950/80 to-slate-900/80 text-white hover:border-cyan-400/70 hover:shadow-cyan-500/10 hover:shadow-2xl"
        }
      `}
    >
      <Handle type="target" position={Position.Left} className="!w-3 !h-3 !bg-cyan-400 !border-2 !border-cyan-600 !-left-1.5" />
      <Handle type="source" position={Position.Right} className="!w-3 !h-3 !bg-cyan-400 !border-2 !border-cyan-600 !-right-1.5" />

      {/* Header */}
      <div className="flex items-center gap-2 mb-3">
        <div className={`w-7 h-7 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${isDenied ? "bg-slate-700 text-slate-500" : "bg-cyan-500/30 text-cyan-300"}`}>
          S
        </div>
        <div className="min-w-0">
          <div className="text-[10px] uppercase tracking-wider text-cyan-400/70 font-medium">Service</div>
          <div className="font-semibold text-sm truncate" title={label}>{label}</div>
        </div>
        {isDenied && <span className="text-slate-500 ml-auto text-sm" title="Access Denied">&#x1F512;</span>}
      </div>

      {/* Metrics */}
      {!isDenied && (
        <>
          <div className="grid grid-cols-3 gap-2 text-center mb-2">
            <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
              <div className="text-[10px] text-slate-400">Members</div>
              <div className="text-xs font-bold text-cyan-300">{members}</div>
            </div>
            <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
              <div className="text-[10px] text-slate-400">Lag</div>
              <div className={`text-xs font-bold font-mono ${lagWarning ? "text-red-400" : totalLag > 0 ? "text-amber-400" : "text-green-400"}`}>
                {fmt(totalLag)}
              </div>
            </div>
            <div className="bg-slate-800/60 rounded-lg px-2 py-1.5">
              <div className="text-[10px] text-slate-400">Flow</div>
              <div className="text-xs font-bold text-slate-300">{consumes.length}&#x2192;{produces.length}</div>
            </div>
          </div>

          {/* Pipeline indicators */}
          <div className="flex gap-1 flex-wrap">
            {consumes.map((t) => (
              <span key={`c-${t}`} className="text-[9px] bg-indigo-500/20 text-indigo-300 rounded px-1.5 py-0.5 truncate max-w-[100px]" title={`consumes: ${t}`}>
                &#x2190; {t.split('.').pop()}
              </span>
            ))}
            {produces.map((t) => (
              <span key={`p-${t}`} className="text-[9px] bg-green-500/20 text-green-300 rounded px-1.5 py-0.5 truncate max-w-[100px]" title={`produces: ${t}`}>
                &#x2192; {t.split('.').pop()}
              </span>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export const ServiceNode = memo(ServiceNodeComponent);
