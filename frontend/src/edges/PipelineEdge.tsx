import { memo, useState } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getSmoothStepPath,
  type EdgeProps,
} from "@xyflow/react";
import { useThemeStore } from "../store/themeStore";

function PipelineEdgeComponent({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
}: EdgeProps) {
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
    borderRadius: 16,
  });

  const isBright = useThemeStore((s) => s.theme === "bright");
  const edgeType = (data?.type as string) || "consumes";
  const isActive = Boolean(data?.active);
  const isInactive = Boolean(data?.inactive);
  const isDimmed = Boolean(data?._dimmed);
  const lagWarning = Boolean(data?.lagWarning);
  const label = String(data?.label || "");

  const lag = Number(data?.lag || 0);
  const msgPerSec = Number(data?.msgPerSec || 0);
  const [hovered, setHovered] = useState(false);
  const [pinned, setPinned] = useState(false);
  const totalMessages = Number(data?.totalMessages || 0);
  const topicName = String(data?.topic || "");
  const partitionCount = Number(data?.partitions || 0);
  const consumerGroup = String(data?.consumerGroup || "");

  const isProduces = edgeType === "produces";
  const baseColor = lagWarning
    ? "#ef4444"
    : isProduces
      ? "#22c55e"
      : isBright ? "#6366f1" : "#6366f1";
  const glowColor = lagWarning
    ? "rgba(239,68,68,0.3)"
    : isProduces
      ? isBright ? "rgba(34,197,94,0.15)" : "rgba(34,197,94,0.2)"
      : isBright ? "rgba(99,102,241,0.15)" : "rgba(99,102,241,0.2)";

  // Auto-generate label from lag/rate if no explicit label
  const autoLabel = !label && lag > 100
    ? `lag: ${lag > 1000 ? `${(lag / 1000).toFixed(1)}K` : String(lag)}`
    : !label && msgPerSec > 0
      ? `${msgPerSec.toFixed(0)}/s`
      : "";
  const displayLabel = label || autoLabel;

  return (
    <>
      {/* Invisible wide hit area */}
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={20}
        className="cursor-pointer"
      />

      {/* Glow layer */}
      <path
        d={edgePath}
        fill="none"
        stroke={glowColor}
        strokeWidth={isActive && !isInactive ? 12 : 6}
        className="transition-all duration-500 pointer-events-none"
        style={{ opacity: isDimmed ? 0.08 : isInactive ? 0.15 : 1 }}
      />

      {/* Base edge */}
      <BaseEdge
        id={id}
        path={edgePath}
        style={{
          stroke: baseColor,
          strokeWidth: selected ? 3 : 2,
          opacity: isDimmed ? 0.1 : isInactive ? 0.15 : isActive ? 1 : 0.4,
          transition: "all 0.3s ease",
        }}
      />

      {/* Animated flow particles - speed based on throughput */}
      {isActive && !isInactive && !isDimmed && (
        <path
          d={edgePath}
          fill="none"
          stroke={isProduces ? "#4ade80" : "#818cf8"}
          strokeWidth={2.5}
          strokeDasharray="6 8"
          className={isProduces ? "edge-flow-produce" : "edge-flow-consume"}
          style={{
            opacity: 0.8,
            animationDuration: msgPerSec > 100 ? "0.3s" : msgPerSec > 10 ? "0.6s" : msgPerSec > 0 ? "1s" : "2s",
          }}
        />
      )}

      {/* Arrow marker */}
      <defs>
        <marker
          id={`arrow-${id}`}
          viewBox="0 0 10 10"
          refX="8"
          refY="5"
          markerWidth="6"
          markerHeight="6"
          orient="auto-start-reverse"
        >
          <path d="M 0 0 L 10 5 L 0 10 z" fill={baseColor} />
        </marker>
      </defs>
      <path
        d={edgePath}
        fill="none"
        stroke="transparent"
        strokeWidth={1}
        markerEnd={`url(#arrow-${id})`}
      />

      {/* Edge label */}
      {displayLabel && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan pointer-events-auto cursor-pointer"
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            }}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={() => { if (!pinned) setHovered(false); }}
            onClick={(e) => { e.stopPropagation(); setPinned((p) => !p); if (!pinned) setHovered(true); else setHovered(false); }}
          >
            <div
              className={`
                text-[10px] font-mono font-medium px-2 py-0.5 rounded-full
                backdrop-blur-sm border shadow-lg transition-all
                ${lagWarning
                  ? isBright
                    ? "bg-red-50/95 border-red-300/60 text-red-700"
                    : "bg-red-950/90 border-red-500/50 text-red-300"
                  : isProduces
                    ? isBright
                      ? "bg-emerald-50/95 border-emerald-300/50 text-emerald-700"
                      : "bg-emerald-950/90 border-emerald-500/30 text-emerald-300"
                    : isBright
                      ? "bg-indigo-50/95 border-indigo-300/50 text-indigo-700"
                      : "bg-indigo-950/90 border-indigo-500/30 text-indigo-300"
                }
              `}
            >
              {displayLabel}
            </div>
            {/* Hover/pinned tooltip with extended details */}
            {(hovered || pinned) && (lag > 0 || msgPerSec > 0 || topicName) && (
              <div
                className={`absolute left-1/2 -translate-x-1/2 top-full mt-1.5 z-50 rounded-xl border shadow-xl backdrop-blur-xl px-3 py-2 min-w-[180px] ${
                  pinned ? "ring-1 " : ""
                }${isBright
                  ? `bg-white/95 border-slate-200 text-slate-700 ${pinned ? "ring-amber-300" : ""}`
                  : `bg-slate-900/95 border-slate-700/60 text-slate-200 ${pinned ? "ring-amber-500/50" : ""}`
                }`}
              >
                {pinned && (
                  <button
                    onClick={(e) => { e.stopPropagation(); setPinned(false); setHovered(false); }}
                    className={`absolute top-1 right-1.5 text-[10px] leading-none cursor-pointer ${isBright ? "text-slate-400 hover:text-slate-600" : "text-slate-500 hover:text-slate-300"}`}
                  >
                    ✕
                  </button>
                )}
                {topicName && (
                  <div className="text-[10px] font-mono font-medium truncate mb-1.5 pr-4">{topicName}</div>
                )}
                <div className="space-y-0.5 text-[10px]">
                  <div className="flex justify-between gap-4">
                    <span className={isBright ? "text-slate-400" : "text-slate-500"}>Type</span>
                    <span className="font-medium">{isProduces ? "produces" : "consumes"}</span>
                  </div>
                  {msgPerSec > 0 && (
                    <div className="flex justify-between gap-4">
                      <span className={isBright ? "text-slate-400" : "text-slate-500"}>Rate</span>
                      <span className="font-mono font-medium text-emerald-500">{msgPerSec.toFixed(1)}/s</span>
                    </div>
                  )}
                  {lag > 0 && (
                    <div className="flex justify-between gap-4">
                      <span className={isBright ? "text-slate-400" : "text-slate-500"}>Lag</span>
                      <span className={`font-mono font-medium ${lag > 1000 ? "text-red-500" : "text-amber-500"}`}>{lag.toLocaleString()}</span>
                    </div>
                  )}
                  {totalMessages > 0 && (
                    <div className="flex justify-between gap-4">
                      <span className={isBright ? "text-slate-400" : "text-slate-500"}>Messages</span>
                      <span className="font-mono">{totalMessages.toLocaleString()}</span>
                    </div>
                  )}
                  {partitionCount > 0 && (
                    <div className="flex justify-between gap-4">
                      <span className={isBright ? "text-slate-400" : "text-slate-500"}>Partitions</span>
                      <span className="font-mono">{partitionCount}</span>
                    </div>
                  )}
                  {consumerGroup && (
                    <div className="flex justify-between gap-4">
                      <span className={isBright ? "text-slate-400" : "text-slate-500"}>Group</span>
                      <span className="font-mono truncate max-w-[100px]">{consumerGroup}</span>
                    </div>
                  )}
                  {lag > 0 && msgPerSec > 0 && (
                    <div className={`flex justify-between gap-4 pt-1 mt-1 border-t ${isBright ? "border-slate-200" : "border-slate-700/50"}`}>
                      <span className={isBright ? "text-slate-400" : "text-slate-500"}>Catch-up</span>
                      <span className="font-mono font-medium text-cyan-500">
                        {lag / msgPerSec < 60
                          ? `${Math.round(lag / msgPerSec)}s`
                          : lag / msgPerSec < 3600
                            ? `${Math.round(lag / msgPerSec / 60)}m`
                            : `${(lag / msgPerSec / 3600).toFixed(1)}h`}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

export const PipelineEdge = memo(PipelineEdgeComponent);
