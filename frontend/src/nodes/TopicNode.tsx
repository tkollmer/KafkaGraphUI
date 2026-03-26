import { memo, useState } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useThemeStore } from "../store/themeStore";

function TopicNodeComponent({ data, selected }: NodeProps) {
  const d = data as Record<string, unknown>;
  const label = String(d.label || "");
  const partitions = Number(d.partitions || 0);
  const msgPerSec = Number(d.msgPerSec || 0);
  const totalMessages = Number(d.totalMessages || 0);
  const avgMsgSize = Number(d.avgMsgSize || 0);
  const replicationFactor = Number(d.replicationFactor || 0);
  const retentionMs = d.retentionMs as string | undefined;
  const consumers = (d.consumers as string[]) || [];
  const producers = (d.producers as string[]) || [];
  const isDenied = d.status === "access_denied";
  const isInactive = d.status === "inactive";
  const isDimmed = Boolean(d._dimmed);
  const isSearchMatch = Boolean(d._searchMatch);
  const isActive = msgPerSec > 0;
  const isBright = useThemeStore((s) => s.theme === "bright");
  const [hovered, setHovered] = useState(false);

  return (
    <div
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      className={`
        relative group rounded-2xl border px-5 py-4 min-w-[240px] max-w-[280px]
        shadow-xl backdrop-blur-xl transition-all duration-300
        ${isInactive ? "opacity-40 grayscale" : ""}
        ${isDimmed ? "opacity-20 scale-[0.97]" : ""}
        ${isSearchMatch ? "ring-2 ring-yellow-400/70 ring-offset-2 ring-offset-transparent" : ""}
        ${isDenied
          ? isBright
            ? "border-slate-300 bg-slate-100/80 text-slate-400"
            : "border-slate-600/50 bg-slate-800/40 text-slate-500"
          : selected
            ? isBright
              ? "border-indigo-500 bg-white text-slate-800 ring-2 ring-indigo-400/50 ring-offset-2 ring-offset-slate-100 shadow-indigo-200/50 shadow-2xl"
              : "border-indigo-400 bg-gradient-to-br from-indigo-950/90 to-slate-900/90 text-white ring-2 ring-indigo-400/50 ring-offset-2 ring-offset-slate-950 shadow-indigo-500/20 shadow-2xl"
            : isBright
              ? "border-indigo-200/80 bg-white/95 text-slate-800 hover:border-indigo-400 hover:shadow-lg hover:shadow-indigo-100/50"
              : "border-indigo-500/40 bg-gradient-to-br from-indigo-950/80 to-slate-900/80 text-white hover:border-indigo-400/70 hover:shadow-indigo-500/10 hover:shadow-2xl"
        }
      `}
    >
      <Handle type="target" position={Position.Left} className={`!w-3 !h-3 !border-2 !-left-1.5 ${isBright ? "!bg-indigo-500 !border-indigo-300" : "!bg-indigo-400 !border-indigo-600"}`} />
      <Handle type="source" position={Position.Right} className={`!w-3 !h-3 !border-2 !-right-1.5 ${isBright ? "!bg-indigo-500 !border-indigo-300" : "!bg-indigo-400 !border-indigo-600"}`} />

      {/* Activity indicator */}
      {isActive && !isDenied && (
        <div className="absolute -top-1 -right-1 w-3 h-3">
          <span className="absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75 animate-ping" />
          <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500" />
        </div>
      )}

      {/* Header */}
      <div className="flex items-center gap-2 mb-3">
        <div className={`w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${
          isDenied
            ? isBright ? "bg-slate-200 text-slate-400" : "bg-slate-700 text-slate-500"
            : isBright ? "bg-indigo-100 text-indigo-600" : "bg-indigo-500/30 text-indigo-300"
        }`}>
          T
        </div>
        <div className="min-w-0">
          <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-indigo-500/70" : "text-indigo-400/70"}`}>Topic</div>
          <div className="font-semibold text-[14px] truncate" title={label}>{label}</div>
        </div>
        {isDenied && <span className={`ml-auto text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`} title="Access Denied">&#x1F512;</span>}
      </div>

      {/* Metrics */}
      {!isDenied && (
        <>
          <div className={`grid ${avgMsgSize > 0 ? "grid-cols-4" : "grid-cols-3"} gap-2 text-center`}>
            <MetricBox label="Partitions" value={String(partitions)} bright={isBright} />
            <MetricBox label="Rate" value={msgPerSec > 0 ? `${msgPerSec}/s` : "idle"} accent={isActive} bright={isBright} />
            <MetricBox label="Total" value={fmt(totalMessages)} bright={isBright} />
            {avgMsgSize > 0 && <MetricBox label="Avg Size" value={fmtBytes(avgMsgSize)} bright={isBright} />}
          </div>

          {/* Activity bar */}
          {isActive && (
            <div className={`mt-2 rounded-full overflow-hidden h-1 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
              <div
                className="h-full rounded-full bg-emerald-500 transition-all duration-700"
                style={{ width: `${Math.min(100, msgPerSec > 0 ? Math.log10(msgPerSec + 1) * 33 : 0)}%` }}
              />
            </div>
          )}
        </>
      )}

      {/* Hover tooltip with extended info */}
      {hovered && !isDimmed && !isDenied && (consumers.length > 0 || producers.length > 0 || replicationFactor > 0) && (
        <div className={`absolute left-1/2 -translate-x-1/2 top-full mt-2 z-50 rounded-xl border shadow-xl backdrop-blur-xl px-3 py-2.5 min-w-[200px] max-w-[260px] ${
          isBright ? "bg-white/95 border-slate-200 text-slate-700" : "bg-slate-900/95 border-slate-700/60 text-slate-200"
        }`}>
          <div className="space-y-1.5 text-[10px]">
            {replicationFactor > 0 && (
              <div className="flex justify-between gap-3">
                <span className={isBright ? "text-slate-400" : "text-slate-500"}>Replication</span>
                <span className="font-mono font-medium">{replicationFactor}</span>
              </div>
            )}
            {retentionMs && (
              <div className="flex justify-between gap-3">
                <span className={isBright ? "text-slate-400" : "text-slate-500"}>Retention</span>
                <span className="font-mono font-medium">{fmtRetention(retentionMs)}</span>
              </div>
            )}
            {consumers.length > 0 && (
              <div>
                <span className={`block mb-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Consumers ({consumers.length})</span>
                <div className="flex flex-wrap gap-0.5">
                  {consumers.slice(0, 4).map((c) => (
                    <span key={c} className={`px-1 py-0.5 rounded font-mono truncate max-w-[120px] ${isBright ? "bg-cyan-50 text-cyan-600" : "bg-cyan-500/15 text-cyan-300"}`}>{c}</span>
                  ))}
                  {consumers.length > 4 && <span className={isBright ? "text-slate-400" : "text-slate-500"}>+{consumers.length - 4}</span>}
                </div>
              </div>
            )}
            {producers.length > 0 && (
              <div>
                <span className={`block mb-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Producers ({producers.length})</span>
                <div className="flex flex-wrap gap-0.5">
                  {producers.slice(0, 4).map((p) => (
                    <span key={p} className={`px-1 py-0.5 rounded font-mono truncate max-w-[120px] ${isBright ? "bg-emerald-50 text-emerald-600" : "bg-emerald-500/15 text-emerald-300"}`}>{p}</span>
                  ))}
                  {producers.length > 4 && <span className={isBright ? "text-slate-400" : "text-slate-500"}>+{producers.length - 4}</span>}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function MetricBox({ label, value, accent, bright }: { label: string; value: string; accent?: boolean; bright: boolean }) {
  return (
    <div className={`rounded-lg px-2 py-1.5 ${bright ? "bg-slate-50" : "bg-slate-800/60"}`}>
      <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-400"}`}>{label}</div>
      <div className={`text-[12px] font-bold font-mono ${
        accent ? "text-green-500" : bright ? "text-slate-700" : "text-slate-300"
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

function fmtRetention(ms: string): string {
  if (!ms || ms === "-1") return "Forever";
  const n = parseInt(ms, 10);
  if (isNaN(n) || n < 0) return "Forever";
  if (n < 60000) return `${(n / 1000).toFixed(0)}s`;
  if (n < 3600000) return `${(n / 60000).toFixed(0)}m`;
  if (n < 86400000) return `${(n / 3600000).toFixed(1)}h`;
  return `${(n / 86400000).toFixed(1)}d`;
}

function fmtBytes(b: number): string {
  if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)}MB`;
  if (b >= 1_024) return `${(b / 1_024).toFixed(1)}KB`;
  return `${b}B`;
}

export const TopicNode = memo(TopicNodeComponent);
