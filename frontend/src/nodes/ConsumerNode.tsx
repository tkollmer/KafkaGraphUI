import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useThemeStore } from "../store/themeStore";

function ConsumerNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const members = Number(d.members || 0);
  const totalLag = Number(d.totalLag || 0);
  const lagWarning = Boolean(d.lagWarning);
  const isDenied = d.status === "access_denied";
  const isInactive = d.status === "inactive";
  const isDimmed = Boolean(d._dimmed);
  const isSearchMatch = Boolean(d._searchMatch);
  const consumes = Array.isArray(d.consumes) ? d.consumes : [];
  const clientIds = Array.isArray(d.clientIds) ? d.clientIds : [];
  const groupState = String(d.state || "").toLowerCase();
  const isBright = useThemeStore((s) => s.theme === "bright");

  const tooltipParts = [];
  if (clientIds.length > 0) tooltipParts.push(`Clients: ${clientIds.join(", ")}`);
  if (consumes.length > 0) tooltipParts.push(`Topics: ${consumes.join(", ")}`);

  return (
    <div
      title={tooltipParts.join("\n")}
      className={`
        relative rounded-2xl border px-5 py-4 min-w-[220px] max-w-[260px]
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
                ? "border-amber-500 bg-white text-slate-800 ring-2 ring-amber-400/50 ring-offset-2 ring-offset-slate-100"
                : "border-amber-400 bg-gradient-to-br from-amber-950/90 to-slate-900/90 text-white ring-2 ring-amber-400/50 ring-offset-2 ring-offset-slate-950"
              : isBright
                ? "border-amber-200/80 bg-white/95 text-slate-800 hover:border-amber-400 hover:shadow-lg"
                : "border-amber-500/40 bg-gradient-to-br from-amber-950/80 to-slate-900/80 text-white hover:border-amber-400/70"
        }
      `}
    >
      <Handle type="target" position={Position.Left} className={`!w-3 !h-3 !border-2 !-left-1.5 ${isBright ? "!bg-amber-500 !border-amber-300" : "!bg-amber-400 !border-amber-600"}`} />

      {/* Header */}
      <div className="flex items-center gap-2 mb-3">
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${
          isDenied
            ? isBright ? "bg-slate-200 text-slate-400" : "bg-slate-700 text-slate-500"
            : isBright ? "bg-amber-100 text-amber-600" : "bg-amber-500/30 text-amber-300"
        }`}>
          C
        </div>
        <div className="min-w-0 flex-1">
          <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-amber-500/70" : "text-amber-400/70"}`}>Consumer</div>
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
      </div>

      {/* Metrics */}
      {!isDenied && (
        <div className="grid grid-cols-2 gap-2 text-center">
          <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
            <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Members</div>
            <div className={`text-[12px] font-bold ${isBright ? "text-amber-600" : "text-amber-300"}`}>{members}</div>
          </div>
          <div className={`rounded-lg px-2 py-1.5 ${isBright ? "bg-slate-50" : "bg-slate-800/60"}`}>
            <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Lag</div>
            <div className={`text-[12px] font-bold font-mono ${lagWarning ? "text-red-500" : totalLag > 0 ? "text-amber-500" : "text-green-500"}`}>
              {fmt(totalLag)}
            </div>
          </div>
        </div>
      )}

      {/* Lag bar indicator */}
      {!isDenied && totalLag > 0 && (
        <div className={`mt-2 rounded-full overflow-hidden h-1.5 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
          <div
            className={`h-full rounded-full transition-all duration-500 ${
              lagWarning ? "bg-red-500" : totalLag > 100 ? "bg-amber-500" : "bg-emerald-500"
            }`}
            style={{ width: `${Math.min(100, Math.log10(totalLag + 1) * 25)}%` }}
          />
        </div>
      )}

      {/* Subscribed topics count */}
      {!isDenied && consumes.length > 0 && (
        <div className={`mt-1.5 text-[10px] text-center ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          {consumes.length} topic{consumes.length !== 1 ? "s" : ""}
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

export const ConsumerNode = memo(ConsumerNodeComponent);
