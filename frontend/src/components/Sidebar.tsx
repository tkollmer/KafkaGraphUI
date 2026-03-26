import { useState } from "react";
import { useNavigationStore, type ActiveView } from "../store/navigationStore";
import { useThemeStore } from "../store/themeStore";
import { useGraphStore } from "../store/graphStore";
import { useKafkaStore } from "../store/kafkaStore";
import { useToastStore } from "../store/toastStore";
import { useFavoritesStore } from "../store/favoritesStore";
import { useClusterStore } from "../store/clusterStore";

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

function ApplicationsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <rect x="3" y="3" width="7" height="7" rx="1.5" />
      <rect x="14" y="3" width="7" height="7" rx="1.5" />
      <rect x="3" y="14" width="7" height="7" rx="1.5" />
      <rect x="14" y="14" width="7" height="7" rx="1.5" />
      <path d="M10 6.5h4M6.5 10v4M17.5 10v4M10 17.5h4" strokeLinecap="round" />
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

function SettingsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <circle cx="12" cy="12" r="3" />
      <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
    </svg>
  );
}

function SunIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <circle cx="12" cy="12" r="5" />
      <path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42" strokeLinecap="round" />
    </svg>
  );
}

function MoonIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
    </svg>
  );
}

function ACLsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
      <path d="M9 12l2 2 4-4" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function ConnectorsIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83" strokeLinecap="round" />
      <circle cx="12" cy="12" r="4" />
    </svg>
  );
}

function SchemasIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <path d="M14 2v6h6M8 13h8M8 17h4" strokeLinecap="round" />
    </svg>
  );
}

function QuotasIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <path d="M13 10V3L4 14h7v7l9-11h-7z" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function DashboardIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
      <rect x="3" y="3" width="7" height="9" rx="1.5" />
      <rect x="14" y="3" width="7" height="5" rx="1.5" />
      <rect x="14" y="12" width="7" height="9" rx="1.5" />
      <rect x="3" y="16" width="7" height="5" rx="1.5" />
    </svg>
  );
}

const navItems: { id: ActiveView; label: string; Icon: React.FC<{ className?: string }> }[] = [
  { id: "dashboard", label: "Dashboard", Icon: DashboardIcon },
  { id: "pipeline", label: "Pipeline", Icon: PipelineIcon },
  { id: "applications", label: "Applications", Icon: ApplicationsIcon },
  { id: "topics", label: "Topics", Icon: TopicsIcon },
  { id: "consumers", label: "Consumers", Icon: ConsumersIcon },
  { id: "brokers", label: "Brokers", Icon: BrokersIcon },
  { id: "schemas", label: "Schemas", Icon: SchemasIcon },
  { id: "connectors", label: "Connectors", Icon: ConnectorsIcon },
  { id: "acls", label: "ACLs", Icon: ACLsIcon },
  { id: "quotas", label: "Quotas", Icon: QuotasIcon },
  { id: "settings", label: "Settings", Icon: SettingsIcon },
];

export function Sidebar() {
  const { activeView, setActiveView, sidebarCollapsed, toggleSidebar } = useNavigationStore();
  const { theme, toggleTheme } = useThemeStore();
  const nodes = useGraphStore((s) => s.nodes);
  const connectionStatus = useGraphStore((s) => s.connectionStatus);
  const isBright = theme === "bright";
  const eventLog = useToastStore((s) => s.eventLog);
  const unreadCount = useToastStore((s) => s.unreadCount);
  const [showEventLog, setShowEventLog] = useState(false);
  const [eventFilter, setEventFilter] = useState<"all" | "success" | "error" | "info">("all");

  const favoriteTopics = useFavoritesStore((s) => s.favoriteTopics);
  const favoriteGroups = useFavoritesStore((s) => s.favoriteGroups);
  const { navigateToTopic, navigateToConsumerGroup } = useNavigationStore();
  const clusters = useClusterStore((s) => s.clusters);
  const activeClusterId = useClusterStore((s) => s.activeClusterId);
  const activeCluster = useClusterStore((s) => s.getActiveCluster());
  const [showClusterSwitch, setShowClusterSwitch] = useState(false);

  const kafkaTopics = useKafkaStore((s) => s.topics);
  const kafkaGroups = useKafkaStore((s) => s.consumerGroups);
  const kafkaBrokers = useKafkaStore((s) => s.brokers);

  const topicCount = kafkaTopics.length || nodes.filter((n) => n.type === "topicNode").length;
  const consumerCount = kafkaGroups.length || nodes.filter((n) => n.type === "consumerNode").length;
  const serviceCount = nodes.filter((n) => n.type === "serviceNode").length;
  const brokerCount = kafkaBrokers.length;
  const totalLag = kafkaGroups.reduce((s, g) => s + (g.totalLag || 0), 0);

  const countMap: Record<string, number> = {
    topics: topicCount,
    consumers: consumerCount,
    applications: serviceCount,
    brokers: brokerCount,
  };

  const alertMap: Record<string, boolean> = {
    consumers: totalLag > 1000,
  };

  return (
    <div
      className={`shrink-0 h-full flex flex-col transition-all duration-300 ${
        sidebarCollapsed ? "w-[68px]" : "w-[230px]"
      }`}
    >
      <div className={`flex flex-col h-full m-2 mr-0 rounded-2xl backdrop-blur-xl border shadow-2xl overflow-hidden transition-colors duration-300 ${
        isBright
          ? "bg-white/90 border-slate-200/80"
          : "bg-slate-900/80 border-slate-700/50"
      }`}>
        {/* Header */}
        <div className={`flex items-center gap-2.5 px-3.5 py-4 border-b ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
          <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center shadow-lg shadow-indigo-500/20 shrink-0">
            <span className="text-white font-black text-sm">K</span>
          </div>
          {!sidebarCollapsed && (
            <div className="min-w-0 flex-1">
              <div className={`text-sm font-bold leading-none ${isBright ? "text-slate-800" : "text-white"}`}>Kafka Debug</div>
              <button
                onClick={() => setShowClusterSwitch(!showClusterSwitch)}
                className={`text-[11px] leading-tight mt-0.5 cursor-pointer hover:underline flex items-center gap-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}
                title="Switch cluster"
              >
                <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{ background: activeCluster.color }} />
                {activeCluster.name}
              </button>
            </div>
          )}
        </div>

        {/* Cluster switcher */}
        {showClusterSwitch && !sidebarCollapsed && (
          <div className={`mx-2 mb-1 p-2 rounded-xl border ${isBright ? "bg-slate-50 border-slate-200/60" : "bg-slate-800/50 border-slate-700/30"}`}>
            <div className={`text-[9px] uppercase tracking-wider font-semibold px-1 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Clusters</div>
            {clusters.map((c) => (
              <button
                key={c.id}
                onClick={() => { if (c.id !== activeClusterId) useClusterStore.getState().setActiveCluster(c.id); setShowClusterSwitch(false); }}
                className={`w-full flex items-center gap-2 px-2 py-1.5 rounded-lg text-[11px] transition-colors cursor-pointer ${
                  c.id === activeClusterId
                    ? isBright ? "bg-indigo-50 text-indigo-700" : "bg-indigo-500/10 text-indigo-300"
                    : isBright ? "text-slate-600 hover:bg-slate-100" : "text-slate-400 hover:bg-slate-700/50"
                }`}
              >
                <span className="w-2 h-2 rounded-full shrink-0" style={{ background: c.color }} />
                <span className="truncate">{c.name}</span>
                {c.url && <span className={`text-[9px] ml-auto truncate max-w-[80px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{c.url.replace(/^https?:\/\//, "")}</span>}
              </button>
            ))}
          </div>
        )}

        {/* Nav items */}
        <nav className="flex-1 py-3 space-y-1 px-2">
          {navItems.map((item) => {
            const isActive = activeView === item.id;
            const count = countMap[item.id];
            const hasAlert = alertMap[item.id];
            return (
              <button
                key={item.id}
                onClick={() => setActiveView(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-[13px] font-medium transition-all duration-200 cursor-pointer ${
                  isActive
                    ? isBright
                      ? "bg-indigo-50 text-indigo-700 shadow-sm"
                      : "bg-indigo-500/15 text-indigo-300 shadow-sm shadow-indigo-500/10"
                    : isBright
                      ? "text-slate-600 hover:bg-slate-100 hover:text-slate-800"
                      : "text-slate-400 hover:bg-slate-800/50 hover:text-slate-200"
                }`}
                title={sidebarCollapsed ? item.label : undefined}
              >
                <div className="relative shrink-0">
                  <item.Icon className={`w-5 h-5 ${
                    isActive
                      ? isBright ? "text-indigo-600" : "text-indigo-400"
                      : isBright ? "text-slate-400" : "text-slate-500"
                  }`} />
                  {hasAlert && (
                    <span className="absolute -top-0.5 -right-0.5 w-2 h-2 rounded-full bg-red-500" />
                  )}
                </div>
                {!sidebarCollapsed && (
                  <>
                    <span>{item.label}</span>
                    {count !== undefined && count > 0 && (
                      <span className={`ml-auto text-[10px] font-bold px-1.5 py-0.5 rounded-md ${
                        hasAlert
                          ? "bg-red-500/15 text-red-400"
                          : isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"
                      }`}>
                        {count}
                      </span>
                    )}
                  </>
                )}
              </button>
            );
          })}
        </nav>

        {/* Favorites */}
        {!sidebarCollapsed && (favoriteTopics.length > 0 || favoriteGroups.length > 0) && (
          <div className={`mx-2 mb-1 border-t pt-2 ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
            <div className={`text-[9px] uppercase tracking-wider font-semibold px-2 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Favorites</div>
            <div className="space-y-0.5 max-h-32 overflow-y-auto">
              {favoriteTopics.map((name) => (
                <button
                  key={`t-${name}`}
                  onClick={() => navigateToTopic(name)}
                  className={`w-full flex items-center gap-2 px-2 py-1 rounded-lg text-[11px] transition-colors cursor-pointer truncate ${
                    isBright ? "text-slate-600 hover:bg-indigo-50 hover:text-indigo-700" : "text-slate-400 hover:bg-indigo-500/10 hover:text-indigo-300"
                  }`}
                  title={name}
                >
                  <svg className="w-3 h-3 text-amber-400 shrink-0" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth={1}>
                    <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
                  </svg>
                  <span className="truncate font-mono">{name}</span>
                </button>
              ))}
              {favoriteGroups.map((gid) => (
                <button
                  key={`g-${gid}`}
                  onClick={() => navigateToConsumerGroup(gid)}
                  className={`w-full flex items-center gap-2 px-2 py-1 rounded-lg text-[11px] transition-colors cursor-pointer truncate ${
                    isBright ? "text-slate-600 hover:bg-purple-50 hover:text-purple-700" : "text-slate-400 hover:bg-purple-500/10 hover:text-purple-300"
                  }`}
                  title={gid}
                >
                  <svg className="w-3 h-3 text-amber-400 shrink-0" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" strokeWidth={1}>
                    <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
                  </svg>
                  <span className="truncate font-mono">{gid}</span>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Search shortcut */}
        {!sidebarCollapsed && (
          <div className={`mx-2 mt-1`}>
            <button
              onClick={() => document.dispatchEvent(new KeyboardEvent("keydown", { key: "k", metaKey: true }))}
              className={`w-full flex items-center gap-2 px-3 py-2 rounded-xl text-[12px] transition-all cursor-pointer ${
                isBright
                  ? "bg-slate-50 text-slate-400 hover:bg-slate-100 hover:text-slate-600 border border-slate-200/60"
                  : "bg-slate-800/40 text-slate-500 hover:bg-slate-800/80 hover:text-slate-300 border border-slate-700/30"
              }`}
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <circle cx="11" cy="11" r="8" />
                <path d="m21 21-4.35-4.35" strokeLinecap="round" />
              </svg>
              <span>Search...</span>
              <kbd className={`ml-auto text-[10px] px-1.5 py-0.5 rounded border font-mono ${
                isBright ? "bg-white border-slate-200 text-slate-400" : "bg-slate-900 border-slate-700 text-slate-500"
              }`}>{navigator.platform.includes("Mac") ? "\u2318" : "Ctrl+"}K</kbd>
            </button>
          </div>
        )}

        {/* Notification bell */}
        <div className="mx-2 relative">
          <button
            onClick={() => {
              setShowEventLog(!showEventLog);
              if (!showEventLog) useToastStore.getState().markAllRead();
            }}
            className={`w-full flex items-center justify-center gap-2 px-3 py-2 rounded-xl text-[12px] transition-all cursor-pointer ${
              isBright
                ? "text-slate-400 hover:bg-slate-100 hover:text-slate-600"
                : "text-slate-500 hover:bg-slate-800/50 hover:text-slate-300"
            }`}
            title="Event log"
          >
            <div className="relative">
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9M13.73 21a2 2 0 0 1-3.46 0" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              {unreadCount > 0 && (
                <span className="absolute -top-1 -right-1.5 w-3.5 h-3.5 rounded-full bg-red-500 text-white text-[8px] font-bold flex items-center justify-center">
                  {unreadCount > 9 ? "9+" : unreadCount}
                </span>
              )}
            </div>
            {!sidebarCollapsed && <span>Events{unreadCount > 0 ? ` (${unreadCount})` : ""}</span>}
          </button>

          {showEventLog && (
            <div className={`absolute bottom-full left-0 mb-2 w-72 max-h-80 rounded-xl border shadow-2xl backdrop-blur-xl overflow-hidden z-50 ${
              isBright ? "bg-white/95 border-slate-200/80" : "bg-slate-900/95 border-slate-700/60"
            }`}>
              <div className={`flex items-center justify-between px-3 py-2 border-b ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
                <span className={`text-[11px] font-semibold uppercase tracking-wider ${isBright ? "text-slate-500" : "text-slate-400"}`}>Event Log</span>
                {eventLog.length > 0 && (
                  <button
                    onClick={() => useToastStore.getState().clearEventLog()}
                    className={`text-[10px] cursor-pointer ${isBright ? "text-slate-400 hover:text-slate-600" : "text-slate-500 hover:text-slate-300"}`}
                  >
                    Clear
                  </button>
                )}
              </div>
              <div className={`flex gap-1 px-3 py-1.5 border-b ${isBright ? "border-slate-100" : "border-slate-800/30"}`}>
                {(["all", "success", "error", "info"] as const).map((f) => (
                  <button
                    key={f}
                    onClick={() => setEventFilter(f)}
                    className={`px-1.5 py-0.5 rounded text-[9px] font-medium transition-colors cursor-pointer ${
                      eventFilter === f
                        ? isBright ? "bg-slate-200 text-slate-700" : "bg-slate-700 text-white"
                        : isBright ? "text-slate-400 hover:text-slate-600" : "text-slate-500 hover:text-slate-300"
                    }`}
                  >{f === "all" ? "All" : f.charAt(0).toUpperCase() + f.slice(1)}</button>
                ))}
              </div>
              <div className="overflow-y-auto max-h-56">
                {(eventFilter === "all" ? eventLog : eventLog.filter((e) => e.type === eventFilter)).length === 0 ? (
                  <div className={`px-3 py-6 text-center text-[11px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                    No events yet
                  </div>
                ) : (
                  (eventFilter === "all" ? eventLog : eventLog.filter((e) => e.type === eventFilter)).map((evt) => (
                    <div key={evt.id} className={`flex items-start gap-2 px-3 py-2 border-b last:border-0 ${
                      isBright ? "border-slate-100" : "border-slate-800/30"
                    }`}>
                      <span className={`mt-0.5 w-1.5 h-1.5 rounded-full shrink-0 ${
                        evt.type === "success" ? "bg-emerald-500" : evt.type === "error" ? "bg-red-500" : "bg-blue-500"
                      }`} />
                      <div className="min-w-0 flex-1">
                        <div className={`text-[11px] leading-tight ${isBright ? "text-slate-700" : "text-slate-300"}`}>{evt.message}</div>
                        <div className={`text-[9px] mt-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                          {formatTimeAgo(evt.timestamp)}
                        </div>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className={`px-2 pb-3 space-y-2 border-t ${isBright ? "border-slate-200/60" : "border-slate-700/40"} pt-2`}>
          {/* Connection status */}
          <div className={`flex items-center gap-2 px-3 py-1.5 rounded-lg ${
            connectionStatus === "connected"
              ? isBright ? "bg-emerald-50/50" : "bg-emerald-500/5"
              : connectionStatus === "reconnecting"
                ? isBright ? "bg-amber-50/50" : "bg-amber-500/5"
                : isBright ? "bg-red-50/50" : "bg-red-500/5"
          }`}>
            <div className={`w-2 h-2 rounded-full shrink-0 ${
              connectionStatus === "connected" ? "bg-emerald-500"
                : connectionStatus === "reconnecting" ? "bg-amber-500 animate-pulse"
                : "bg-red-500"
            }`} />
            {!sidebarCollapsed && (
              <span className={`text-[11px] capitalize ${isBright ? "text-slate-500" : "text-slate-400"}`}>{connectionStatus}</span>
            )}
          </div>

          {/* Theme toggle */}
          <button
            onClick={toggleTheme}
            className={`w-full py-2.5 rounded-xl text-xs font-medium transition-all cursor-pointer flex items-center justify-center gap-2 ${
              isBright
                ? "text-slate-500 hover:text-slate-700 hover:bg-slate-100"
                : "text-slate-500 hover:text-slate-300 hover:bg-slate-800/50"
            }`}
          >
            {isBright ? (
              <MoonIcon className="w-4 h-4" />
            ) : (
              <SunIcon className="w-4 h-4" />
            )}
            {!sidebarCollapsed && <span>{isBright ? "Dark Mode" : "Bright Mode"}</span>}
          </button>

          {!sidebarCollapsed && (
            <div className={`px-3 py-1 text-[10px] ${isBright ? "text-slate-400" : "text-slate-600"}`}>v2.0.0</div>
          )}
          <button
            onClick={toggleSidebar}
            className={`w-full py-2 rounded-xl text-xs transition-all cursor-pointer flex items-center justify-center gap-1 ${
              isBright
                ? "text-slate-400 hover:text-slate-600 hover:bg-slate-100"
                : "text-slate-500 hover:text-slate-300 hover:bg-slate-800/50"
            }`}
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

function formatTimeAgo(ts: number): string {
  const diff = Math.floor((Date.now() - ts) / 1000);
  if (diff < 5) return "just now";
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return `${Math.floor(diff / 86400)}d ago`;
}
