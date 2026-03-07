import { useNavigationStore, type ActiveView } from "../store/navigationStore";

const navItems: { id: ActiveView; label: string; icon: string }[] = [
  { id: "pipeline", label: "Pipeline", icon: "P" },
  { id: "topics", label: "Topics", icon: "T" },
  { id: "consumers", label: "Consumers", icon: "C" },
  { id: "brokers", label: "Brokers", icon: "B" },
];

export function Sidebar() {
  const { activeView, setActiveView, sidebarCollapsed, toggleSidebar } = useNavigationStore();

  return (
    <div
      className={`shrink-0 h-full flex flex-col transition-all duration-300 ${
        sidebarCollapsed ? "w-[72px]" : "w-[216px]"
      }`}
    >
      <div className="flex flex-col h-full m-2 mr-0 rounded-2xl bg-slate-900/80 backdrop-blur-xl border border-slate-700/50 shadow-2xl overflow-hidden">
        {/* Header */}
        <div className="flex items-center gap-2 px-3 py-4 border-b border-slate-700/50">
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/20 shrink-0">
            <span className="text-white font-black text-sm">K</span>
          </div>
          {!sidebarCollapsed && (
            <div className="min-w-0">
              <div className="text-xs font-bold text-white leading-none">Kafka Debug</div>
              <div className="text-[10px] text-slate-400 leading-tight">Management</div>
            </div>
          )}
        </div>

        {/* Nav items */}
        <nav className="flex-1 py-2 space-y-1 px-2">
          {navItems.map((item) => {
            const isActive = activeView === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveView(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-all duration-200 cursor-pointer ${
                  isActive
                    ? "bg-indigo-500/20 border border-indigo-500/40 text-indigo-300"
                    : "border border-transparent text-slate-400 hover:bg-slate-800/50 hover:text-slate-300"
                }`}
                title={sidebarCollapsed ? item.label : undefined}
              >
                <span
                  className={`w-7 h-7 rounded-lg flex items-center justify-center text-xs font-bold shrink-0 ${
                    isActive ? "bg-indigo-500/30 text-indigo-300" : "bg-slate-800 text-slate-500"
                  }`}
                >
                  {item.icon}
                </span>
                {!sidebarCollapsed && <span>{item.label}</span>}
              </button>
            );
          })}
        </nav>

        {/* Collapse toggle */}
        <button
          onClick={toggleSidebar}
          className="mx-2 mb-3 py-2 rounded-xl text-xs text-slate-500 hover:text-slate-300 hover:bg-slate-800/50 transition-all cursor-pointer border border-transparent"
        >
          {sidebarCollapsed ? ">>" : "<< Collapse"}
        </button>
      </div>
    </div>
  );
}
