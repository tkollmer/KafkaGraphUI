import { memo } from "react";
import {
  BaseEdge,
  EdgeLabelRenderer,
  getSmoothStepPath,
  type EdgeProps,
} from "@xyflow/react";

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

  const edgeType = (data?.type as string) || "consumes";
  const isActive = Boolean(data?.active);
  const isInactive = Boolean(data?.inactive);
  const isDimmed = Boolean(data?._dimmed);
  const lagWarning = Boolean(data?.lagWarning);
  const label = String(data?.label || "");

  // Color based on edge type
  const isProduces = edgeType === "produces";
  const baseColor = lagWarning
    ? "#ef4444"
    : isProduces
      ? "#22c55e"
      : "#6366f1";
  const glowColor = lagWarning
    ? "rgba(239,68,68,0.3)"
    : isProduces
      ? "rgba(34,197,94,0.2)"
      : "rgba(99,102,241,0.2)";

  return (
    <>
      {/* Invisible wide hit area for easier clicking */}
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

      {/* Animated flow particles */}
      {isActive && !isInactive && !isDimmed && (
        <path
          d={edgePath}
          fill="none"
          stroke={isProduces ? "#4ade80" : "#818cf8"}
          strokeWidth={2.5}
          strokeDasharray="6 8"
          className={isProduces ? "edge-flow-produce" : "edge-flow-consume"}
          style={{ opacity: 0.8 }}
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
      {label && (
        <EdgeLabelRenderer>
          <div
            className="nodrag nopan pointer-events-auto cursor-pointer"
            style={{
              position: "absolute",
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
            }}
          >
            <div
              className={`
                text-[10px] font-mono font-medium px-2 py-0.5 rounded-full
                backdrop-blur-sm border shadow-lg
                ${lagWarning
                  ? "bg-red-950/90 border-red-500/50 text-red-300"
                  : isProduces
                    ? "bg-emerald-950/90 border-emerald-500/30 text-emerald-300"
                    : "bg-indigo-950/90 border-indigo-500/30 text-indigo-300"
                }
              `}
            >
              {label}
            </div>
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

export const PipelineEdge = memo(PipelineEdgeComponent);
