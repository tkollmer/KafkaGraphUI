import { useGraphStore } from "../store/graphStore";

interface Props {
  nodeId: string;
  onClose: () => void;
  onInspect?: (topic: string) => void;
}

export function MetricsPanel({ nodeId, onClose, onInspect }: Props) {
  const node = useGraphStore((s) => s.nodes.find((n) => n.id === nodeId));
  const edges = useGraphStore((s) => s.edges.filter((e) => e.source === nodeId || e.target === nodeId));

  if (!node) return null;

  const d = node.data as Record<string, unknown>;
  const isTopic = node.type === "topicNode";
  const isService = node.type === "serviceNode";
  const isConsumer = node.type === "consumerNode";
  const isProducer = node.type === "producerNode";

  const typeColor = isTopic ? "indigo" : isService ? "cyan" : isConsumer ? "amber" : "emerald";
  const typeLabel = isTopic ? "Topic" : isService ? "Service" : isConsumer ? "Consumer Group" : "Producer";

  const colorClasses: Record<string, string> = {
    indigo: "from-indigo-500/20 border-indigo-500/30",
    cyan: "from-cyan-500/20 border-cyan-500/30",
    amber: "from-amber-500/20 border-amber-500/30",
    emerald: "from-emerald-500/20 border-emerald-500/30",
  };

  return (
    <div className="absolute left-4 bottom-4 w-[360px] z-50">
      <div className={`rounded-2xl border shadow-2xl shadow-black/50 bg-gradient-to-b ${colorClasses[typeColor]} to-slate-950/95 backdrop-blur-2xl`}>
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-slate-700/30">
          <div className="flex items-center gap-2.5">
            <div className={`text-[10px] uppercase tracking-wider font-semibold text-${typeColor}-400`}>
              {typeLabel}
            </div>
          </div>
          <button onClick={onClose} className="w-6 h-6 rounded-md flex items-center justify-center text-slate-500 hover:text-white hover:bg-slate-800 transition-colors cursor-pointer">
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </div>

        {/* Name */}
        <div className="px-5 pt-3 pb-2">
          <div className="text-base font-bold text-white truncate" title={String(d.label || nodeId)}>
            {String(d.label || nodeId)}
          </div>
        </div>

        {/* Metrics grid */}
        <div className="px-5 pb-4 space-y-3">
          {isTopic && (
            <>
              <MetricsGrid>
                <MetricCell label="Partitions" value={String(d.partitions)} />
                <MetricCell label="Messages/sec" value={`${d.msgPerSec}`} accent={Number(d.msgPerSec) > 0} />
                <MetricCell label="Total" value={fmt(Number(d.totalMessages))} />
                <MetricCell label="Consumers" value={String(edges.filter((e) => e.source === nodeId).length)} />
              </MetricsGrid>

              {/* Inspect messages button */}
              {onInspect && (
                <button
                  onClick={() => onInspect(String(d.label))}
                  className="w-full py-2 rounded-xl bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 text-xs font-medium hover:bg-indigo-500/25 transition-colors cursor-pointer"
                >
                  Inspect Messages
                </button>
              )}
            </>
          )}

          {(isService || isConsumer) && (
            <>
              <MetricsGrid>
                <MetricCell label="Members" value={String(d.members)} />
                <MetricCell label="Total Lag" value={fmt(Number(d.totalLag))} warn={Boolean(d.lagWarning)} />
                {isService && (
                  <>
                    <MetricCell label="Consumes" value={String((d.consumes as string[])?.length || 0)} />
                    <MetricCell label="Produces" value={String((d.produces as string[])?.length || 0)} />
                  </>
                )}
              </MetricsGrid>

              {/* Per-partition lag */}
              {d.perPartitionLag && typeof d.perPartitionLag === "object" && Object.keys(d.perPartitionLag as object).length > 0 && (
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5">Per-partition lag</div>
                  <div className="max-h-28 overflow-y-auto rounded-lg bg-slate-900/50 border border-slate-800/50">
                    {Object.entries(d.perPartitionLag as Record<string, number>).map(([k, v]) => (
                      <div key={k} className="flex justify-between px-3 py-1 border-b border-slate-800/30 last:border-0">
                        <span className="text-[10px] text-slate-400 truncate mr-2">{k}</span>
                        <span className={`text-[10px] font-mono font-bold ${v > 1000 ? "text-red-400" : v > 0 ? "text-amber-400" : "text-green-400"}`}>
                          {v}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Connected topics */}
              {isService && (
                <div>
                  <div className="text-[10px] uppercase tracking-wider text-slate-500 font-medium mb-1.5">Data flow</div>
                  <div className="flex flex-wrap gap-1">
                    {((d.consumes as string[]) || []).map((t) => (
                      <span key={`c-${t}`} className="text-[9px] bg-indigo-500/15 text-indigo-300 rounded-md px-2 py-0.5 border border-indigo-500/20">
                        &#x2190; {t}
                      </span>
                    ))}
                    {((d.produces as string[]) || []).map((t) => (
                      <span key={`p-${t}`} className="text-[9px] bg-emerald-500/15 text-emerald-300 rounded-md px-2 py-0.5 border border-emerald-500/20">
                        &#x2192; {t}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}

          {isProducer && (
            <MetricsGrid>
              <MetricCell label="Rate" value={`${d.msgPerSec} msg/s`} accent={Number(d.msgPerSec) > 0} />
              <MetricCell label="Type" value={d.inferred ? "Inferred" : "Known"} />
            </MetricsGrid>
          )}

          {/* Edges info */}
          <div className="text-[10px] text-slate-500">
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

function MetricCell({ label, value, accent, warn }: { label: string; value: string; accent?: boolean; warn?: boolean }) {
  return (
    <div className="bg-slate-800/40 rounded-xl px-3 py-2 border border-slate-700/20">
      <div className="text-[10px] text-slate-500">{label}</div>
      <div className={`text-sm font-bold font-mono ${warn ? "text-red-400" : accent ? "text-emerald-400" : "text-white"}`}>
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
