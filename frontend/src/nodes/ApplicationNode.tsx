import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useThemeStore } from "../store/themeStore";

function ApplicationNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const serviceCount = Number(d.serviceCount || 0);
  const consumerCount = Number(d.consumerCount || 0);
  const producerCount = Number(d.producerCount || 0);
  const topicCount = Number(d.topicCount || 0);
  const totalLag = Number(d.totalLag || 0);
  const totalMsgPerSec = Number(d.totalMsgPerSec || 0);
  const lagWarning = Boolean(d.lagWarning);
  const isDimmed = Boolean(d._dimmed);
  const isBright = useThemeStore((s) => s.theme === "bright");

  return (
    <div
      className={`
        relative rounded-2xl border px-6 py-5 min-w-[280px] max-w-[340px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${lagWarning
          ? "border-red-500/60 bg-gradient-to-br from-red-950/80 to-slate-900/90 text-white lag-pulse"
          : selected
            ? isBright
              ? "border-violet-500 bg-white text-slate-800 ring-2 ring-violet-400/50 ring-offset-2 ring-offset-slate-100 shadow-violet-200/50 shadow-2xl"
              : "border-violet-400 bg-gradient-to-br from-violet-950/90 to-slate-900/90 text-white ring-2 ring-violet-400/50 ring-offset-2 ring-offset-slate-950 shadow-violet-500/20 shadow-2xl"
            : isBright
              ? "border-violet-200/80 bg-white/95 text-slate-800 hover:border-violet-400 hover:shadow-lg hover:shadow-violet-100/50"
              : "border-violet-500/40 bg-gradient-to-br from-violet-950/80 to-slate-900/80 text-white hover:border-violet-400/70 hover:shadow-violet-500/10 hover:shadow-2xl"
        }
      `}
    >
      <Handle type="target" position={Position.Left} className={`!w-4 !h-4 !border-2 !-left-2 ${isBright ? "!bg-violet-500 !border-violet-300" : "!bg-violet-400 !border-violet-600"}`} />
      <Handle type="source" position={Position.Right} className={`!w-4 !h-4 !border-2 !-right-2 ${isBright ? "!bg-violet-500 !border-violet-300" : "!bg-violet-400 !border-violet-600"}`} />

      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <div className={`w-10 h-10 rounded-xl flex items-center justify-center text-sm font-bold shrink-0 ${
          isBright ? "bg-violet-100 text-violet-600" : "bg-violet-500/30 text-violet-300"
        }`}>
          A
        </div>
        <div className="min-w-0">
          <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-violet-500/70" : "text-violet-400/70"}`}>Application</div>
          <div className="font-bold text-[16px] truncate" title={label}>{label}</div>
        </div>
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-3 gap-2 text-center mb-3">
        <MetricBox label="Services" value={String(serviceCount + consumerCount)} bright={isBright} />
        <MetricBox label="Topics" value={String(topicCount)} bright={isBright} />
        <MetricBox label="Rate" value={totalMsgPerSec > 0 ? `${totalMsgPerSec.toFixed(0)}/s` : "idle"} accent={totalMsgPerSec > 0} bright={isBright} />
      </div>

      {/* Second row */}
      <div className="grid grid-cols-2 gap-2 text-center">
        <MetricBox label="Producers" value={String(producerCount)} bright={isBright} />
        <MetricBox
          label="Total Lag"
          value={fmt(totalLag)}
          warn={lagWarning}
          bright={isBright}
        />
      </div>

      {/* Lag indicator */}
      {totalLag > 0 && (
        <div className={`mt-2 rounded-full overflow-hidden h-1.5 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
          <div
            className={`h-full rounded-full transition-all duration-500 ${
              lagWarning ? "bg-red-500" : totalLag > 100 ? "bg-amber-500" : "bg-emerald-500"
            }`}
            style={{ width: `${Math.min(100, Math.log10(totalLag + 1) * 25)}%` }}
          />
        </div>
      )}

      {/* Click hint */}
      <div className={`text-[10px] text-center mt-2 italic ${isBright ? "text-violet-400" : "text-violet-400/60"}`}>
        Click to drill down
      </div>
    </div>
  );
}

function MetricBox({ label, value, accent, warn, bright }: { label: string; value: string; accent?: boolean; warn?: boolean; bright: boolean }) {
  return (
    <div className={`rounded-lg px-2 py-1.5 ${bright ? "bg-slate-50" : "bg-slate-800/60"}`}>
      <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-400"}`}>{label}</div>
      <div className={`text-[12px] font-bold font-mono ${
        warn ? "text-red-500" : accent ? "text-green-500" : bright ? "text-slate-700" : "text-slate-300"
      }`}>
        {value}
      </div>
    </div>
  );
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export const ApplicationNode = memo(ApplicationNodeComponent);
