import { useGraphStore } from "../store/graphStore";
import { useNavigationStore } from "../store/navigationStore";

interface Props {
  onAutoLayout: () => void;
  onFitView: () => void;
}

export function Toolbar({ onAutoLayout, onFitView }: Props) {
  const connectionStatus = useGraphStore((s) => s.connectionStatus);
  const searchQuery = useGraphStore((s) => s.searchQuery);
  const setSearchQuery = useGraphStore((s) => s.setSearchQuery);
  const hideSystemTopics = useGraphStore((s) => s.hideSystemTopics);
  const setHideSystemTopics = useGraphStore((s) => s.setHideSystemTopics);
  const darkMode = useGraphStore((s) => s.darkMode);
  const setDarkMode = useGraphStore((s) => s.setDarkMode);
  const nodes = useGraphStore((s) => s.nodes);

  const { activeView } = useNavigationStore();
  const isPipeline = activeView === "pipeline";

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
      <div className="mx-4 mt-4 flex items-center gap-2 p-2 rounded-2xl bg-slate-900/80 backdrop-blur-xl border border-slate-700/50 shadow-2xl">
        {/* Status */}
        <div className="flex items-center gap-2.5 pl-2 pr-3 border-r border-slate-700/50">
          <div className="flex items-center gap-1.5">
            <div className={`w-2 h-2 rounded-full ${statusColor}`} />
            <span className="text-[10px] text-slate-400 capitalize">{connectionStatus}</span>
          </div>
        </div>

        {/* Stats pills — only on pipeline */}
        {isPipeline && (
          <div className="flex items-center gap-1.5 px-2 border-r border-slate-700/50">
            <StatPill label="Topics" count={topicCount} color="indigo" />
            <StatPill label="Services" count={serviceCount} color="cyan" />
            <StatPill label="Consumers" count={consumerCount} color="amber" />
            {producerCount > 0 && <StatPill label="Producers" count={producerCount} color="emerald" />}
          </div>
        )}

        {/* Search — only on pipeline */}
        {isPipeline && (
          <div className="relative flex-1 max-w-xs">
            <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <circle cx="11" cy="11" r="8" />
              <path d="m21 21-4.35-4.35" strokeLinecap="round" />
            </svg>
            <input
              type="text"
              placeholder="Filter..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full bg-slate-800/80 rounded-xl pl-9 pr-3 py-2 border border-slate-700/50 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500/50 focus:ring-1 focus:ring-indigo-500/30 transition-all"
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
            />
            <ToolbarBtn onClick={onAutoLayout} label="Re-layout" />
            <ToolbarBtn onClick={onFitView} label="Fit" />
          </>
        )}
        <ToolbarBtn
          onClick={() => setDarkMode(!darkMode)}
          label={darkMode ? "Light" : "Dark"}
        />
      </div>
    </div>
  );
}

function StatPill({ label, count, color }: { label: string; count: number; color: string }) {
  const colors: Record<string, string> = {
    indigo: "bg-indigo-500/15 text-indigo-300 border-indigo-500/20",
    cyan: "bg-cyan-500/15 text-cyan-300 border-cyan-500/20",
    amber: "bg-amber-500/15 text-amber-300 border-amber-500/20",
    emerald: "bg-emerald-500/15 text-emerald-300 border-emerald-500/20",
  };
  return (
    <div className={`flex items-center gap-1.5 rounded-lg px-2 py-1 border text-[10px] font-medium ${colors[color] || colors.indigo}`}>
      <span className="font-bold text-xs">{count}</span>
      <span className="opacity-70">{label}</span>
    </div>
  );
}

function ToolbarBtn({ label, onClick, active }: { label: string; onClick: () => void; active?: boolean }) {
  return (
    <button
      onClick={onClick}
      className={`px-3 py-1.5 rounded-xl text-xs font-medium border transition-all duration-200 cursor-pointer
        ${active
          ? "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
          : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
        }
      `}
    >
      {label}
    </button>
  );
}
