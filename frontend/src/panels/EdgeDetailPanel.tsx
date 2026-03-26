import { useGraphStore } from "../store/graphStore";
import { useThemeStore } from "../store/themeStore";
import { useNavigationStore } from "../store/navigationStore";

interface Props {
  edgeId: string;
  onClose: () => void;
  onInspectTopic?: (topic: string) => void;
}

export function EdgeDetailPanel({ edgeId, onClose, onInspectTopic }: Props) {
  const edge = useGraphStore((s) => s.edges.find((e) => e.id === edgeId));
  const sourceNode = useGraphStore((s) => s.nodes.find((n) => n.id === edge?.source));
  const targetNode = useGraphStore((s) => s.nodes.find((n) => n.id === edge?.target));
  const { theme } = useThemeStore();
  const isBright = theme === "bright";

  if (!edge || !sourceNode || !targetNode) return null;

  const data = (edge.data || {}) as Record<string, unknown>;
  const isProduces = data.type === "produces";
  const lag = Number(data.lag || 0);
  const msgPerSec = Number(data.msgPerSec || 0);
  const isActive = Boolean(data.active);
  const lagWarning = Boolean(data.lagWarning);

  const sourceLabel = String(sourceNode.data?.label || sourceNode.id);
  const targetLabel = String(targetNode.data?.label || targetNode.id);

  // Find the topic in this connection
  const topicLabel = sourceNode.type === "topicNode"
    ? sourceLabel
    : targetNode.type === "topicNode"
      ? targetLabel
      : null;

  const accentColor = lagWarning
    ? "red"
    : isProduces
      ? "emerald"
      : "indigo";

  const darkAccent: Record<string, string> = {
    red: "from-red-500/20 border-red-500/30",
    emerald: "from-emerald-500/20 border-emerald-500/30",
    indigo: "from-indigo-500/20 border-indigo-500/30",
  };
  const brightAccent: Record<string, string> = {
    red: "from-red-50 border-red-200/60",
    emerald: "from-emerald-50 border-emerald-200/60",
    indigo: "from-indigo-50 border-indigo-200/60",
  };
  const accent = isBright ? brightAccent[accentColor] : darkAccent[accentColor];

  return (
    <div className="absolute right-4 bottom-4 w-[340px] z-50">
      <div className={`rounded-2xl border shadow-2xl bg-gradient-to-b backdrop-blur-2xl transition-colors ${accent} ${
        isBright ? "to-white/95 shadow-black/10" : "to-slate-950/95 shadow-black/50"
      }`}>
        {/* Header */}
        <div className={`flex items-center justify-between px-5 py-3 border-b ${isBright ? "border-slate-200/50" : "border-slate-700/30"}`}>
          <div className={`text-[11px] uppercase tracking-wider font-semibold ${
            isProduces ? (isBright ? "text-emerald-600" : "text-emerald-400") : (isBright ? "text-indigo-600" : "text-indigo-400")
          }`}>
            {isProduces ? "Produces" : "Consumes"} Connection
          </div>
          <button onClick={(e) => { e.stopPropagation(); onClose(); }} className={`w-7 h-7 rounded-md flex items-center justify-center transition-colors cursor-pointer ${
            isBright ? "text-slate-400 hover:text-slate-700 hover:bg-slate-100" : "text-slate-500 hover:text-white hover:bg-slate-800"
          }`}>
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        <div className="px-5 py-4 space-y-4">
          {/* Flow direction */}
          <div className="flex items-center gap-3">
            <div className={`flex-1 text-center rounded-xl px-3 py-2.5 border ${
              isBright ? "bg-slate-50 border-slate-200/50" : "bg-slate-800/50 border-slate-700/30"
            }`}>
              <div className={`text-[10px] mb-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Source</div>
              <div className={`text-[13px] font-mono font-bold truncate ${isBright ? "text-slate-800" : "text-white"}`} title={sourceLabel}>
                {sourceLabel}
              </div>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{sourceNode.type?.replace("Node", "")}</div>
            </div>
            <div className={`text-lg ${isProduces ? "text-emerald-500" : "text-indigo-500"}`}>
              {isProduces ? "\u2192" : "\u2192"}
            </div>
            <div className={`flex-1 text-center rounded-xl px-3 py-2.5 border ${
              isBright ? "bg-slate-50 border-slate-200/50" : "bg-slate-800/50 border-slate-700/30"
            }`}>
              <div className={`text-[10px] mb-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Target</div>
              <div className={`text-[13px] font-mono font-bold truncate ${isBright ? "text-slate-800" : "text-white"}`} title={targetLabel}>
                {targetLabel}
              </div>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{targetNode.type?.replace("Node", "")}</div>
            </div>
          </div>

          {/* Metrics */}
          <div className="grid grid-cols-2 gap-2">
            <div className={`rounded-xl px-3 py-2.5 border ${
              isBright ? "bg-slate-50/80 border-slate-200/30" : "bg-slate-800/40 border-slate-700/20"
            }`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Status</div>
              <div className={`text-[13px] font-bold ${
                isActive ? "text-emerald-500" : isBright ? "text-slate-400" : "text-slate-500"
              }`}>
                {isActive ? "Active" : "Idle"}
              </div>
            </div>
            {msgPerSec > 0 && (
              <div className={`rounded-xl px-3 py-2.5 border ${
                isBright ? "bg-slate-50/80 border-slate-200/30" : "bg-slate-800/40 border-slate-700/20"
              }`}>
                <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Rate</div>
                <div className="text-[13px] font-bold font-mono text-emerald-500">
                  {msgPerSec.toFixed(1)} msg/s
                </div>
              </div>
            )}
          </div>

          {/* Lag section for consumer edges */}
          {!isProduces && lag > 0 && (
            <div className={`rounded-xl px-3 py-3 border space-y-2 ${
              isBright ? "bg-slate-50/80 border-slate-200/30" : "bg-slate-800/40 border-slate-700/20"
            }`}>
              <div className="flex items-center justify-between">
                <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Consumer Lag</div>
                <div className={`text-[14px] font-bold font-mono ${
                  lagWarning ? "text-red-500" : lag > 100 ? "text-amber-500" : "text-emerald-500"
                }`}>
                  {lag.toLocaleString()}
                </div>
              </div>
              <div className={`rounded-full overflow-hidden h-2 ${isBright ? "bg-slate-200" : "bg-slate-700/50"}`}>
                <div
                  className={`h-full rounded-full transition-all duration-500 ${
                    lagWarning ? "bg-red-500" : lag > 100 ? "bg-amber-500" : "bg-emerald-500"
                  }`}
                  style={{ width: `${Math.min(100, lag > 0 ? Math.log10(lag + 1) * 25 : 0)}%` }}
                />
              </div>
            </div>
          )}

          {/* Action buttons */}
          <div className="space-y-1.5">
            {topicLabel && onInspectTopic && (
              <button
                onClick={(e) => { e.stopPropagation(); onInspectTopic(topicLabel); }}
                className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                  isBright
                    ? "bg-indigo-50 border border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                    : "bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                }`}
              >
                Inspect Messages: {topicLabel}
              </button>
            )}
            {topicLabel && (
              <button
                onClick={(e) => { e.stopPropagation(); useNavigationStore.getState().navigateToTopic(topicLabel); }}
                className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                  isBright
                    ? "bg-slate-50 border border-slate-200/60 text-slate-600 hover:bg-slate-100"
                    : "bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
                }`}
              >
                Go to Topic: {topicLabel}
              </button>
            )}
            {/* Navigate to consumer group if consumer node */}
            {sourceNode.type === "consumerNode" && (
              <button
                onClick={(e) => { e.stopPropagation(); useNavigationStore.getState().navigateToConsumerGroup(sourceLabel); }}
                className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                  isBright
                    ? "bg-slate-50 border border-slate-200/60 text-slate-600 hover:bg-slate-100"
                    : "bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
                }`}
              >
                Go to Consumer Group: {sourceLabel}
              </button>
            )}
            {targetNode.type === "consumerNode" && (
              <button
                onClick={(e) => { e.stopPropagation(); useNavigationStore.getState().navigateToConsumerGroup(targetLabel); }}
                className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                  isBright
                    ? "bg-slate-50 border border-slate-200/60 text-slate-600 hover:bg-slate-100"
                    : "bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
                }`}
              >
                Go to Consumer Group: {targetLabel}
              </button>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
