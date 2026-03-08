import { useNavigationStore, type ActiveView } from "../store/navigationStore";

function PipelineIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <circle cx="5" cy="12" r="2.5" />
      <circle cx="19" cy="6" r="2.5" />
      <circle cx="19" cy="18" r="2.5" />
      <path d="M7.5 11L16.5 7M7.5 13L16.5 17" strokeLinecap="round" />
    </svg>
  );
}

function TopicsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <rect x="3" y="3" width="18" height="5" rx="1.5" />
      <rect x="3" y="10" width="18" height="5" rx="1.5" />
      <rect x="3" y="17" width="12" height="4" rx="1.5" strokeDasharray="3 2" />
    </svg>
  );
}

function ConsumersIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2" />
      <circle cx="9" cy="7" r="4" />
      <path d="M23 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75" />
    </svg>
  );
}

function BrokersIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <rect x="2" y="2" width="20" height="8" rx="2" />
      <rect x="2" y="14" width="20" height="8" rx="2" />
      <circle cx="6" cy="6" r="1" fill="currentColor" />
      <circle cx="6" cy="18" r="1" fill="currentColor" />
    </svg>
  );
}

const navItems: { id: ActiveView; label: string; Icon: React.FC<{ className?: string }> }[] = [
  { id: "pipeline", label: "Pipeline", Icon: PipelineIcon },
  { id: "topics", label: "Topics", Icon: TopicsIcon },
  { id: "consumers", label: "Consumers", Icon: ConsumersIcon },
  { id: "brokers", label: "Brokers", Icon: BrokersIcon },
];

export function Sidebar() {
  const { activeView, setActiveView, sidebarCollapsed, toggleSidebar } = useNavigationStore();

  return (
    <div
      className={`shrink-0 h-full flex flex-col transition-all duration-300 ${
        sidebarCollapsed ? "w-[68px]" : "w-[220px]"
      }`}
    >
      <div className="flex flex-col h-full m-2 mr-0 rounded-2xl bg-slate-900/80 backdrop-blur-xl border border-slate-700/50 shadow-2xl overflow-hidden">
        {/* Header */}
        <div className="flex items-center gap-2.5 px-3.5 py-4 border-b border-slate-700/40">
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/20 shrink-0">
            <span className="text-white font-black text-sm">K</span>
          </div>
          {!sidebarCollapsed && (
            <div className="min-w-0">
              <div className="text-xs font-bold text-white leading-none">Kafka Debug</div>
              <div className="text-[10px] text-slate-400 leading-tight mt-0.5">Flow</div>
            </div>
          )}
        </div>

        {/* Nav items */}
        <nav className="flex-1 py-3 space-y-1 px-2">
          {navItems.map((item) => {
            const isActive = activeView === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveView(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 cursor-pointer ${
                  isActive
                    ? "bg-indigo-500/15 text-indigo-300 shadow-sm shadow-indigo-500/10"
                    : "text-slate-400 hover:bg-slate-800/50 hover:text-slate-200"
                }`}
                title={sidebarCollapsed ? item.label : undefined}
              >
                <item.Icon className={`w-5 h-5 shrink-0 ${isActive ? "text-indigo-400" : "text-slate-500"}`} />
                {!sidebarCollapsed && <span>{item.label}</span>}
              </button>
            );
          })}
        </nav>

        {/* Footer */}
        <div className="px-2 pb-3 space-y-2">
          {!sidebarCollapsed && (
            <div className="px-3 py-2 text-[10px] text-slate-600">v1.0.0</div>
          )}
          <button
            onClick={toggleSidebar}
            className="w-full py-2 rounded-xl text-xs text-slate-500 hover:text-slate-300 hover:bg-slate-800/50 transition-all cursor-pointer flex items-center justify-center gap-1"
          >
            <svg className={`w-4 h-4 transition-transform ${sidebarCollapsed ? "" : "rotate-180"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M9 5l7 7-7 7" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
            {!sidebarCollapsed && <span>Collapse</span>}
          </button>
        </div>
      </div>
    </div>
  );
}
