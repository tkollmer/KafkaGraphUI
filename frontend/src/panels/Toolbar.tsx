import { useEffect, useRef, useState } from "react";
import { useGraphStore } from "../store/graphStore";
import { useNavigationStore } from "../store/navigationStore";
import { useThemeStore } from "../store/themeStore";
import { useToastStore } from "../store/toastStore";

interface Props {
  onAutoLayout: () => void;
  onFitView: () => void;
}

function exportGraph() {
  const state = useGraphStore.getState();
  const data = {
    nodes: state.nodes.map((n) => ({ id: n.id, type: n.type, data: n.data })),
    edges: state.edges.map((e) => ({ id: e.id, source: e.source, target: e.target, data: e.data })),
    metrics: state.metrics,
    exportedAt: new Date().toISOString(),
  };
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `kafka-graph-${new Date().toISOString().slice(0, 19).replace(/:/g, "-")}.json`;
  a.click();
  URL.revokeObjectURL(url);
  useToastStore.getState().addToast("Graph exported as JSON", "success", 2000);
}

export function Toolbar({ onAutoLayout, onFitView }: Props) {
  const connectionStatus = useGraphStore((s) => s.connectionStatus);
  const searchQuery = useGraphStore((s) => s.searchQuery);
  const setSearchQuery = useGraphStore((s) => s.setSearchQuery);
  const hideSystemTopics = useGraphStore((s) => s.hideSystemTopics);
  const setHideSystemTopics = useGraphStore((s) => s.setHideSystemTopics);
  const nodes = useGraphStore((s) => s.nodes);

  const { activeView } = useNavigationStore();
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const isPipeline = activeView === "pipeline";
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [uptime, setUptime] = useState<string | null>(null);

  // Slash to focus pipeline search
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (e.key === "/" && isPipeline) {
        e.preventDefault();
        searchInputRef.current?.focus();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [isPipeline]);

  // Fetch uptime periodically
  useEffect(() => {
    let mounted = true;
    async function fetchHealth() {
      try {
        const res = await fetch("/api/health");
        if (!res.ok) return;
        const data = await res.json();
        if (!mounted) return;
        const secs = Math.round(data.uptime || 0);
        if (secs < 60) setUptime(`${secs}s`);
        else if (secs < 3600) setUptime(`${Math.floor(secs / 60)}m`);
        else if (secs < 86400) setUptime(`${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`);
        else setUptime(`${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`);
      } catch { /* ignore */ }
    }
    fetchHealth();
    const iv = setInterval(fetchHealth, 30000);
    return () => { mounted = false; clearInterval(iv); };
  }, []);

  const statusColor = {
    connected: "bg-emerald-500",
    reconnecting: "bg-amber-500 animate-pulse",
    disconnected: "bg-red-500",
  }[connectionStatus];

  const topicCount = nodes.filter((n) => n.type === "topicNode").length;
  const serviceCount = nodes.filter((n) => n.type === "serviceNode").length;
  const consumerCount = nodes.filter((n) => n.type === "consumerNode").length;
  const producerCount = nodes.filter((n) => n.type === "producerNode").length;

  return (
    <div className="relative z-30 shrink-0">
      <div className={`mx-4 mt-4 flex items-center gap-2 p-2 rounded-2xl backdrop-blur-xl border shadow-2xl transition-colors duration-300 ${
        isBright
          ? "bg-white/90 border-slate-200/80"
          : "bg-slate-900/80 border-slate-700/50"
      }`}>
        {/* Status */}
        <div className={`flex items-center gap-2.5 pl-2 pr-3 border-r ${isBright ? "border-slate-200/60" : "border-slate-700/50"}`}>
          <div className="flex items-center gap-1.5">
            <div className={`w-2.5 h-2.5 rounded-full ${statusColor}`} />
            <span className={`text-[11px] capitalize ${isBright ? "text-slate-500" : "text-slate-400"}`}>{connectionStatus}</span>
            {uptime && (
              <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                {uptime}
              </span>
            )}
          </div>
        </div>

        {/* Mini sparkline */}
        {isPipeline && <MiniSparkline bright={isBright} />}

        {/* Stats pills — only on pipeline */}
        {isPipeline && (
          <div className={`flex items-center gap-1.5 px-2 border-r ${isBright ? "border-slate-200/60" : "border-slate-700/50"}`}>
            <StatPill label="Topics" count={topicCount} color="indigo" bright={isBright} />
            <StatPill label="Services" count={serviceCount} color="cyan" bright={isBright} />
            <StatPill label="Consumers" count={consumerCount} color="amber" bright={isBright} />
            {producerCount > 0 && <StatPill label="Producers" count={producerCount} color="emerald" bright={isBright} />}
          </div>
        )}

        {/* Search — only on pipeline */}
        {isPipeline && (
          <div className="relative flex-1 max-w-xs">
            <svg className={`absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 ${isBright ? "text-slate-400" : "text-slate-500"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <circle cx="11" cy="11" r="8" />
              <path d="m21 21-4.35-4.35" strokeLinecap="round" />
            </svg>
            <input
              ref={searchInputRef}
              type="text"
              placeholder="Filter nodes... (/)"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className={`w-full rounded-xl pl-9 pr-3 py-2 border text-[13px] placeholder-slate-400 focus:outline-none focus:border-indigo-500/50 focus:ring-1 focus:ring-indigo-500/30 transition-all ${
                isBright
                  ? "bg-slate-100/80 border-slate-200/80 text-slate-800"
                  : "bg-slate-800/80 border-slate-700/50 text-white"
              }`}
            />
          </div>
        )}

        {/* Spacer */}
        <div className="flex-1" />

        {/* Pipeline-specific buttons */}
        {isPipeline && (
          <>
            <ToolbarBtn
              active={!hideSystemTopics}
              onClick={() => setHideSystemTopics(!hideSystemTopics)}
              label="System"
              title="Toggle system topics (__consumer_offsets, etc.)"
              bright={isBright}
            />
            <ToolbarBtn onClick={onAutoLayout} label="Re-layout" title="Re-calculate node positions (L)" bright={isBright} />
            <ToolbarBtn onClick={onFitView} label="Fit" title="Fit all nodes in view (F)" bright={isBright} />
            <ToolbarBtn onClick={exportGraph} label="Export" title="Export graph as JSON" bright={isBright} />
            <ToolbarBtn onClick={() => useNavigationStore.getState().toggleFullscreen()} label="Zen" title="Toggle fullscreen mode (Z)" bright={isBright} />
          </>
        )}
      </div>
    </div>
  );
}

function StatPill({ label, count, color, bright }: { label: string; count: number; color: string; bright: boolean }) {
  const darkColors: Record<string, string> = {
    indigo: "bg-indigo-500/15 text-indigo-300 border-indigo-500/20",
    cyan: "bg-cyan-500/15 text-cyan-300 border-cyan-500/20",
    amber: "bg-amber-500/15 text-amber-300 border-amber-500/20",
    emerald: "bg-emerald-500/15 text-emerald-300 border-emerald-500/20",
  };
  const brightColors: Record<string, string> = {
    indigo: "bg-indigo-50 text-indigo-700 border-indigo-200/60",
    cyan: "bg-cyan-50 text-cyan-700 border-cyan-200/60",
    amber: "bg-amber-50 text-amber-700 border-amber-200/60",
    emerald: "bg-emerald-50 text-emerald-700 border-emerald-200/60",
  };
  const colors = bright ? brightColors : darkColors;
  return (
    <div className={`flex items-center gap-1.5 rounded-lg px-2 py-1 border text-[11px] font-medium ${colors[color] || colors.indigo}`}>
      <span className="font-bold text-xs">{count}</span>
      <span className="opacity-70">{label}</span>
    </div>
  );
}

function MiniSparkline({ bright }: { bright: boolean }) {
  const rateHistory = useGraphStore((s) => s.rateHistory);
  if (rateHistory.length < 2) return null;

  const max = Math.max(...rateHistory, 1);
  const h = 24;
  const w = 60;
  const points = rateHistory.map((v, i) => {
    const x = (i / (rateHistory.length - 1)) * w;
    const y = h - (v / max) * (h - 4) - 2;
    return `${x},${y}`;
  }).join(" ");

  const currentRate = rateHistory[rateHistory.length - 1] || 0;

  return (
    <div className={`flex items-center gap-1.5 px-2 border-r ${bright ? "border-slate-200/60" : "border-slate-700/50"}`} title={`Throughput: ${currentRate.toFixed(1)} msg/s`}>
      <svg width={w} height={h} className="shrink-0">
        <polyline
          points={points}
          fill="none"
          stroke={bright ? "#6366f1" : "#818cf8"}
          strokeWidth={1.5}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </svg>
      <span className={`text-[10px] font-mono tabular-nums ${bright ? "text-slate-500" : "text-slate-400"}`}>
        {currentRate.toFixed(0)}/s
      </span>
    </div>
  );
}

function ToolbarBtn({ label, onClick, active, bright, title }: { label: string; onClick: () => void; active?: boolean; bright: boolean; title?: string }) {
  return (
    <button
      onClick={onClick}
      title={title}
      className={`px-3 py-1.5 rounded-xl text-xs font-medium border transition-all duration-200 cursor-pointer
        ${active
          ? bright
            ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
            : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
          : bright
            ? "bg-slate-100/80 border-slate-200/60 text-slate-500 hover:bg-slate-200/60 hover:text-slate-700"
            : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
        }
      `}
    >
      {label}
    </button>
  );
}
