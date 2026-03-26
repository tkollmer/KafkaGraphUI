import { memo, useState } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useThemeStore } from "../store/themeStore";

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
  const isSearchMatch = Boolean(d._searchMatch);
  const groupState = String(d.state || "").toLowerCase();
  const isBright = useThemeStore((s) => s.theme === "bright");
  const [hovered, setHovered] = useState(false);

  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      className={`
        relative rounded-2xl border px-5 py-4 min-w-[240px] max-w-[280px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isInactive ? "opacity-40 grayscale" : ""}
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${isSearchMatch ? "ring-2 ring-yellow-400/70 ring-offset-2 ring-offset-transparent" : ""}
        ${isDenied
          ? isBright ? "border-slate-300 bg-slate-100/80 text-slate-400" : "border-slate-600/50 bg-slate-800/40 text-slate-500"
          : lagWarning
            ? "border-red-500/60 bg-gradient-to-br from-red-950/80 to-slate-900/90 text-white lag-pulse"
            : selected
              ? isBright
                ? "border-cyan-500 bg-white text-slate-800 ring-2 ring-cyan-400/50 ring-offset-2 ring-offset-slate-100 shadow-cyan-200/50 shadow-2xl"
                : "border-cyan-400 bg-gradient-to-br from-cyan-950/90 to-slate-900/90 text-white ring-2 ring-cyan-400/50 ring-offset-2 ring-offset-slate-950 shadow-cyan-500/20 shadow-2xl"
              : isBright
                ? "border-cyan-200/80 bg-white/95 text-slate-800 hover:border-cyan-400 hover:shadow-lg hover:shadow-cyan-100/50"
                : "border-cyan-500/40 bg-gradient-to-br from-cyan-950/80 to-slate-900/80 text-white hover:border-cyan-400/70 hover:shadow-cyan-500/10 hover:shadow-2xl"
        }
      `}
    >
      <Handle type="target" position={Position.Left} className={`!w-3 !h-3 !border-2 !-left-1.5 ${isBright ? "!bg-cyan-500 !border-cyan-300" : "!bg-cyan-400 !border-cyan-600"}`} />
      <Handle type="source" position={Position.Right} className={`!w-3 !h-3 !border-2 !-right-1.5 ${isBright ? "!bg-cyan-500 !border-cyan-300" : "!bg-cyan-400 !border-cyan-600"}`} />

      {/* Header */}
      <div className="flex items-center gap-2 mb-3">
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${
          isDenied
            ? isBright ? "bg-slate-200 text-slate-400" : "bg-slate-700 text-slate-500"
            : isBright ? "bg-cyan-100 text-cyan-600" : "bg-cyan-500/30 text-cyan-300"
        }`}>
          S
        </div>
        <div className="min-w-0 flex-1">
          <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-cyan-500/70" : "text-cyan-400/70"}`}>Service</div>
          <div className="font-semibold text-[14px] truncate" title={label}>{label}</div>
        </div>
        {groupState && groupState !== "stable" && groupState !== "ok" && (
          <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded-md shrink-0 ${
            groupState === "rebalancing" || groupState === "preparingrebalance"
              ? isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/20 text-amber-300"
              : groupState === "empty"
                ? isBright ? "bg-slate-100 text-slate-500" : "bg-slate-700/50 text-slate-400"
                : groupState === "dead"
                  ? isBright ? "bg-red-100 text-red-600" : "bg-red-500/20 text-red-400"
                  : isBright ? "bg-slate-100 text-slate-500" : "bg-slate-700/50 text-slate-400"
          }`}>
            {groupState === "preparingrebalance" ? "rebal" : groupState}
          </span>
        )}
        {isDenied && <span className={`text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`} title="Access Denied">&#x1F512;</span>}
      </div>

      {/* Metrics */}
      {!isDenied && (
        <>
          <div className="grid grid-cols-3 gap-2 text-center mb-2">
            <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Members</div>
              <div className={`text-[12px] font-bold ${isBright ? "text-cyan-600" : "text-cyan-300"}`}>{members}</div>
            </div>
            <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Lag</div>
              <div className={`text-[12px] font-bold font-mono ${lagWarning ? "text-red-500" : totalLag > 0 ? "text-amber-500" : "text-green-500"}`}>
                {fmt(totalLag)}
              </div>
            </div>
            <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Flow</div>
              <div className={`text-[12px] font-bold ${isBright ? "text-slate-600" : "text-slate-300"}`}>{consumes.length}&#x2192;{produces.length}</div>
            </div>
          </div>

          {/* Lag bar */}
          {totalLag > 0 && (
            <div className={`rounded-full overflow-hidden h-1.5 mb-2 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
              <div
                className={`h-full rounded-full transition-all duration-500 ${
                  lagWarning ? "bg-red-500" : totalLag > 100 ? "bg-amber-500" : "bg-emerald-500"
                }`}
                style={{ width: `${Math.min(100, Math.log10(totalLag + 1) * 25)}%` }}
              />
            </div>
          )}

          {/* Pipeline indicators */}
          <div className="flex gap-1 flex-wrap">
            {consumes.slice(0, 4).map((t) => (
              <span key={`c-${t}`} className={`text-[9px] rounded px-1.5 py-0.5 truncate max-w-[100px] ${
                isBright ? "bg-indigo-50 text-indigo-600" : "bg-indigo-500/20 text-indigo-300"
              }`} title={`consumes: ${t}`}>
                &#x2190; {t.split('.').pop()}
              </span>
            ))}
            {produces.slice(0, 4).map((t) => (
              <span key={`p-${t}`} className={`text-[9px] rounded px-1.5 py-0.5 truncate max-w-[100px] ${
                isBright ? "bg-green-50 text-green-600" : "bg-green-500/20 text-green-300"
              }`} title={`produces: ${t}`}>
                &#x2192; {t.split('.').pop()}
              </span>
            ))}
            {(consumes.length + produces.length > 8) && (
              <span className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>+{consumes.length + produces.length - 8} more</span>
            )}
          </div>
        </>
      )}

      {/* Hover tooltip with full topic lists */}
      {hovered && !isDimmed && !isDenied && (consumes.length > 4 || produces.length > 4) && (
        <div className={`absolute left-1/2 -translate-x-1/2 top-full mt-2 z-50 rounded-xl border shadow-xl backdrop-blur-xl px-3 py-2.5 min-w-[200px] max-w-[280px] ${
          isBright ? "bg-white/95 border-slate-200 text-slate-700" : "bg-slate-900/95 border-slate-700/60 text-slate-200"
        }`}>
          <div className="space-y-1.5 text-[10px]">
            {consumes.length > 0 && (
              <div>
                <span className={`block mb-0.5 font-medium ${isBright ? "text-indigo-500" : "text-indigo-400"}`}>Consumes ({consumes.length})</span>
                <div className="flex flex-col gap-0.5">
                  {consumes.map((t) => (
                    <span key={t} className="font-mono truncate">{t}</span>
                  ))}
                </div>
              </div>
            )}
            {produces.length > 0 && (
              <div>
                <span className={`block mb-0.5 font-medium ${isBright ? "text-emerald-500" : "text-emerald-400"}`}>Produces ({produces.length})</span>
                <div className="flex flex-col gap-0.5">
                  {produces.map((t) => (
                    <span key={t} className="font-mono truncate">{t}</span>
                  ))}
                </div>
              </div>
            )}
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

export const ServiceNode = memo(ServiceNodeComponent);
