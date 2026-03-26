import { useState } from "react";
import { useReactFlow } from "@xyflow/react";
import { useGraphStore } from "../store/graphStore";
import { useThemeStore } from "../store/themeStore";
import { useNavigationStore } from "../store/navigationStore";

interface Props {
  nodeId: string;
  onClose: () => void;
  onInspect?: (topic: string) => void;
}

export function MetricsPanel({ nodeId, onClose, onInspect }: Props) {
  const node = useGraphStore((s) => s.nodes.find((n) => n.id === nodeId));
  const edges = useGraphStore((s) => s.edges.filter((e) => e.source === nodeId || e.target === nodeId));
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const { fitView } = useReactFlow();
  const [copied, setCopied] = useState(false);

  if (!node || !node.data) return null;

  let d: Record<string, unknown>;
  try {
    d = node.data as Record<string, unknown>;
  } catch {
    return null;
  }

  const isTopic = node.type === "topicNode";
  const isService = node.type === "serviceNode";
  const isConsumer = node.type === "consumerNode";
  const isProducer = node.type === "producerNode";

  const typeLabel = isTopic ? "Topic" : isService ? "Service" : isConsumer ? "Consumer Group" : "Producer";

  const darkColorClasses: Record<string, string> = {
    indigo: "from-indigo-500/20 border-indigo-500/30",
    cyan: "from-cyan-500/20 border-cyan-500/30",
    amber: "from-amber-500/20 border-amber-500/30",
    emerald: "from-emerald-500/20 border-emerald-500/30",
  };
  const brightColorClasses: Record<string, string> = {
    indigo: "from-indigo-50 border-indigo-200/60",
    cyan: "from-cyan-50 border-cyan-200/60",
    amber: "from-amber-50 border-amber-200/60",
    emerald: "from-emerald-50 border-emerald-200/60",
  };
  const typeColor = isTopic ? "indigo" : isService ? "cyan" : isConsumer ? "amber" : "emerald";
  const colorClasses = isBright ? brightColorClasses : darkColorClasses;

  return (
    <div className="absolute left-4 bottom-4 w-[380px] z-50">
      <div className={`rounded-2xl border shadow-2xl bg-gradient-to-b backdrop-blur-2xl transition-colors ${colorClasses[typeColor]} ${
        isBright ? "to-white/95 shadow-black/10" : "to-slate-950/95 shadow-black/50"
      }`}>
        {/* Header */}
        <div className={`flex items-center justify-between px-5 py-3 border-b ${isBright ? "border-slate-200/50" : "border-slate-700/30"}`}>
          <div className="flex items-center gap-2.5">
            <div className={`text-[11px] uppercase tracking-wider font-semibold ${
              isTopic ? (isBright ? "text-indigo-600" : "text-indigo-400") :
              isService ? (isBright ? "text-cyan-600" : "text-cyan-400") :
              isConsumer ? (isBright ? "text-amber-600" : "text-amber-400") :
              (isBright ? "text-emerald-600" : "text-emerald-400")
            }`}>
              {typeLabel}
            </div>
          </div>
          <button onClick={(e) => { e.stopPropagation(); e.preventDefault(); onClose(); }} className={`w-7 h-7 rounded-md flex items-center justify-center transition-colors cursor-pointer ${
            isBright ? "text-slate-400 hover:text-slate-700 hover:bg-slate-100" : "text-slate-500 hover:text-white hover:bg-slate-800"
          }`}>
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        {/* Name */}
        <div className="px-5 pt-3 pb-2 flex items-center gap-2">
          <div className={`text-[16px] font-bold truncate flex-1 ${isBright ? "text-slate-800" : "text-white"}`} title={String(d.label || nodeId)}>
            {String(d.label || nodeId)}
          </div>
          <button
            onClick={() => fitView({ nodes: [{ id: nodeId }], padding: 0.5, duration: 400 })}
            className={`shrink-0 px-2 py-1 rounded-lg text-[10px] font-medium transition-all cursor-pointer ${
              isBright ? "text-slate-400 hover:text-slate-600 hover:bg-slate-100" : "text-slate-500 hover:text-slate-300 hover:bg-slate-800"
            }`}
            title="Zoom to node"
          >
            Zoom
          </button>
          <button
            onClick={() => {
              navigator.clipboard.writeText(String(d.label || nodeId));
              setCopied(true);
              setTimeout(() => setCopied(false), 1500);
            }}
            className={`shrink-0 px-2 py-1 rounded-lg text-[10px] font-medium transition-all cursor-pointer ${
              copied
                ? isBright ? "bg-emerald-50 text-emerald-600" : "bg-emerald-500/20 text-emerald-300"
                : isBright ? "text-slate-400 hover:text-slate-600 hover:bg-slate-100" : "text-slate-500 hover:text-slate-300 hover:bg-slate-800"
            }`}
            title="Copy name"
          >
            {copied ? "Copied" : "Copy"}
          </button>
        </div>

        {/* Metrics grid */}
        <div className="px-5 pb-4 space-y-3">
          {isTopic && (
            <>
              <MetricsGrid>
                <MetricCell label="Partitions" value={String(d.partitions ?? 0)} bright={isBright} />
                <MetricCell label="Messages/sec" value={`${d.msgPerSec ?? 0}`} accent={Number(d.msgPerSec || 0) > 0} bright={isBright} />
                <MetricCell label="Total" value={fmt(Number(d.totalMessages || 0))} bright={isBright} />
                <MetricCell label="Consumers" value={String(edges.filter((e) => e.source === nodeId).length)} bright={isBright} />
              </MetricsGrid>

              {onInspect && d.label && (
                <button
                  onClick={(e) => { e.stopPropagation(); e.preventDefault(); onInspect(String(d.label)); }}
                  className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                    isBright
                      ? "bg-indigo-50 border border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                      : "bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                  }`}
                >
                  Inspect Messages
                </button>
              )}
              {d.label && (
                <button
                  onClick={(e) => { e.stopPropagation(); useNavigationStore.getState().navigateToTopic(String(d.label)); }}
                  className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                    isBright
                      ? "bg-slate-50 border border-slate-200/60 text-slate-600 hover:bg-slate-100"
                      : "bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
                  }`}
                >
                  Go to Topic Detail
                </button>
              )}
            </>
          )}

          {(isService || isConsumer) && (
            <>
              <MetricsGrid>
                <MetricCell label="Members" value={String(d.members ?? 0)} bright={isBright} />
                <MetricCell label="Total Lag" value={fmt(Number(d.totalLag || 0))} warn={Boolean(d.lagWarning)} bright={isBright} />
                {isService && (
                  <>
                    <MetricCell label="Consumes" value={String((d.consumes as string[])?.length || 0)} bright={isBright} />
                    <MetricCell label="Produces" value={String((d.produces as string[])?.length || 0)} bright={isBright} />
                  </>
                )}
              </MetricsGrid>

              {/* Per-partition lag */}
              {d.perPartitionLag && typeof d.perPartitionLag === "object" && !Array.isArray(d.perPartitionLag) && (() => {
                try {
                  const entries = Object.entries(d.perPartitionLag as Record<string, number>);
                  if (entries.length === 0) return null;
                  return (
                    <div>
                      <div className={`text-[11px] uppercase tracking-wider font-medium mb-1.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Per-partition lag</div>
                      <div className={`max-h-32 overflow-y-auto rounded-lg border ${
                        isBright ? "bg-slate-50 border-slate-200/50" : "bg-slate-900/50 border-slate-800/50"
                      }`}>
                        {entries.map(([k, v]) => (
                          <div key={k} className={`flex justify-between px-3 py-1.5 border-b last:border-0 ${isBright ? "border-slate-100" : "border-slate-800/30"}`}>
                            <span className={`text-[11px] truncate mr-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>{k}</span>
                            <span className={`text-[11px] font-mono font-bold ${Number(v) > 1000 ? "text-red-500" : Number(v) > 0 ? "text-amber-500" : "text-green-500"}`}>
                              {v}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                } catch { return null; }
              })()}

              {/* Connected topics */}
              {isService && (
                <div>
                  <div className={`text-[11px] uppercase tracking-wider font-medium mb-1.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Data flow</div>
                  <div className="flex flex-wrap gap-1">
                    {(Array.isArray(d.consumes) ? d.consumes : []).map((t) => (
                      <button
                        key={`c-${t}`}
                        onClick={(e) => { e.stopPropagation(); onInspect?.(String(t)); }}
                        className={`text-[10px] rounded-md px-2 py-0.5 border cursor-pointer transition-colors ${
                          isBright ? "bg-indigo-50 text-indigo-600 border-indigo-200/50 hover:bg-indigo-100" : "bg-indigo-500/15 text-indigo-300 border-indigo-500/20 hover:bg-indigo-500/25"
                        }`}
                      >
                        &#x2190; {String(t)}
                      </button>
                    ))}
                    {(Array.isArray(d.produces) ? d.produces : []).map((t) => (
                      <button
                        key={`p-${t}`}
                        onClick={(e) => { e.stopPropagation(); onInspect?.(String(t)); }}
                        className={`text-[10px] rounded-md px-2 py-0.5 border cursor-pointer transition-colors ${
                          isBright ? "bg-emerald-50 text-emerald-600 border-emerald-200/50 hover:bg-emerald-100" : "bg-emerald-500/15 text-emerald-300 border-emerald-500/20 hover:bg-emerald-500/25"
                        }`}
                      >
                        &#x2192; {String(t)}
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {/* Navigate to consumer group detail */}
              {isConsumer && d.label && (
                <button
                  onClick={(e) => { e.stopPropagation(); useNavigationStore.getState().navigateToConsumerGroup(String(d.label)); }}
                  className={`w-full py-2.5 rounded-xl text-xs font-medium transition-colors cursor-pointer ${
                    isBright
                      ? "bg-amber-50 border border-amber-200/60 text-amber-700 hover:bg-amber-100"
                      : "bg-amber-500/15 border border-amber-500/30 text-amber-300 hover:bg-amber-500/25"
                  }`}
                >
                  Go to Consumer Group Detail
                </button>
              )}

              {/* Inspect button for consumer group topics */}
              {isConsumer && onInspect && (
                <div>
                  <div className={`text-[11px] uppercase tracking-wider font-medium mb-1.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Subscribed topics</div>
                  <div className="flex flex-wrap gap-1">
                    {(Array.isArray(d.consumes) ? d.consumes : []).map((t) => (
                      <button
                        key={`inspect-${t}`}
                        onClick={(e) => { e.stopPropagation(); onInspect(String(t)); }}
                        className={`text-[10px] rounded-md px-2 py-0.5 border cursor-pointer transition-colors ${
                          isBright ? "bg-indigo-50 text-indigo-600 border-indigo-200/50 hover:bg-indigo-100" : "bg-indigo-500/15 text-indigo-300 border-indigo-500/20 hover:bg-indigo-500/25"
                        }`}
                      >
                        {String(t)}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {isProducer && (
            <MetricsGrid>
              <MetricCell label="Rate" value={`${d.msgPerSec} msg/s`} accent={Number(d.msgPerSec) > 0} bright={isBright} />
              <MetricCell label="Type" value={d.inferred ? "Inferred" : "Known"} bright={isBright} />
            </MetricsGrid>
          )}

          {/* Edges info */}
          <div className={`text-[11px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
            {edges.length} connection{edges.length !== 1 ? "s" : ""}
          </div>
        </div>
      </div>
    </div>
  );
}

function MetricsGrid({ children }: { children: React.ReactNode }) {
  return <div className="grid grid-cols-2 gap-2">{children}</div>;
}

function MetricCell({ label, value, accent, warn, bright }: { label: string; value: string; accent?: boolean; warn?: boolean; bright: boolean }) {
  return (
    <div className={`rounded-xl px-3 py-2.5 border ${
      bright ? "bg-slate-50/80 border-slate-200/30" : "bg-slate-800/40 border-slate-700/20"
    }`}>
      <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>{label}</div>
      <div className={`text-[14px] font-bold font-mono ${
        warn ? "text-red-500" : accent ? "text-emerald-500" : bright ? "text-slate-800" : "text-white"
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
