import { useEffect, useState, useRef, useMemo } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { useThemeStore } from "../store/themeStore";
import { useGraphStore } from "../store/graphStore";
import { DataTable } from "../components/DataTable";
import { SkeletonTable } from "../components/Skeleton";
import { FreshnessIndicator } from "../components/FreshnessIndicator";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function fmtSize(b: number): string {
  if (b >= 1_099_511_627_776) return `${(b / 1_099_511_627_776).toFixed(1)} TB`;
  if (b >= 1_073_741_824) return `${(b / 1_073_741_824).toFixed(1)} GB`;
  if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)} MB`;
  if (b >= 1_024) return `${(b / 1_024).toFixed(1)} KB`;
  return `${b} B`;
}

export function BrokersView() {
  const { brokers, brokersLoading, clusterInfo, fetchBrokers, fetchClusterInfo, topics, fetchTopics, brokersLastFetched, clusterHealth, fetchClusterHealth } = useKafkaStore();
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [selectedBroker, setSelectedBroker] = useState<number | null>(null);
  const [compareBrokers, setCompareBrokers] = useState<[number, number] | null>(null);
  const [brokerConfig, setBrokerConfig] = useState<{ brokerId: number; configs: { name: string; value: string; source: string; isReadOnly: boolean; isSensitive: boolean }[] } | null>(null);
  const [configFilter, setConfigFilter] = useState("");
  const [configEdits, setConfigEdits] = useState<Record<string, string>>({});
  const [savingConfig, setSavingConfig] = useState(false);

  const [logDirs, setLogDirs] = useState<{ brokerId: number; logDir: string; size: number; partitionCount: number; topicCount: number; estimated?: boolean }[]>([]);
  const [, setLogDirsLoading] = useState(false);

  const fetchLogDirs = async () => {
    setLogDirsLoading(true);
    try {
      const resp = await fetch("/api/cluster/log-dirs");
      if (resp.ok) {
        const data = await resp.json();
        setLogDirs(data);
      }
    } catch { /* ignore */ }
    setLogDirsLoading(false);
  };

  const fetchBrokerConfig = async (brokerId: number) => {
    try {
      const resp = await fetch(`/api/brokers/${brokerId}/config`);
      if (resp.ok) {
        const data = await resp.json();
        setBrokerConfig(data);
        setConfigEdits({});
        setConfigFilter("");
      }
    } catch { /* ignore */ }
  };

  const saveBrokerConfig = async () => {
    if (!brokerConfig || Object.keys(configEdits).length === 0) return;
    setSavingConfig(true);
    try {
      const resp = await fetch(`/api/brokers/${brokerConfig.brokerId}/config`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ configs: configEdits }),
      });
      if (resp.ok) {
        fetchBrokerConfig(brokerConfig.brokerId);
      }
    } catch { /* ignore */ }
    setSavingConfig(false);
  };
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  useEffect(() => {
    fetchBrokers();
    fetchClusterInfo();
    fetchTopics();
    fetchClusterHealth();
    fetchLogDirs();
  }, [fetchBrokers, fetchClusterInfo, fetchTopics, fetchClusterHealth]);

  const graphMetrics = useGraphStore((s) => s.metrics);

  // Cluster health metrics
  const healthMetrics = useMemo(() => {
    const totalPartitions = topics.reduce((s, t) => s + (t.partitions || 0), 0);
    const totalMessages = topics.reduce((s, t) => s + (t.totalMessages || 0), 0);
    const avgPartitionsPerBroker = brokers.length > 0 ? Math.round(totalPartitions / brokers.length) : 0;
    const totalThroughput = Object.values(graphMetrics).reduce((s, m) => s + (m.msgPerSec || 0), 0);
    const topicsAtRisk = topics.filter((t) => (t.replicationFactor || 1) <= 1).length;
    const avgReplication = topics.length > 0 ? topics.reduce((s, t) => s + (t.replicationFactor || 1), 0) / topics.length : 0;
    return { totalPartitions, totalMessages, avgPartitionsPerBroker, totalThroughput, topicsAtRisk, avgReplication };
  }, [topics, brokers, graphMetrics]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(() => { fetchBrokers(); fetchClusterInfo(); }, 5000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, fetchBrokers, fetchClusterInfo]);

  const broker = selectedBroker !== null ? brokers.find(b => b.id === selectedBroker) : null;

  if (broker) {
    return (
      <div className="p-6 flex-1 overflow-y-auto space-y-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => setSelectedBroker(null)}
            className={`px-3 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
            }`}
          >
            &larr; Back
          </button>
          <div className="flex-1">
            <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-cyan-500" : "text-cyan-400/70"}`}>Broker</div>
            <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>
              Broker {broker.id}
              {broker.isController && (
                <span className={`ml-3 text-[10px] font-semibold uppercase px-2.5 py-1 rounded-lg border align-middle ${
                  isBright ? "bg-cyan-50 text-cyan-700 border-cyan-200" : "bg-cyan-500/15 text-cyan-300 border-cyan-500/25"
                }`}>Controller</span>
              )}
            </h1>
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          <InfoCard label="Host" value={broker.host} mono bright={isBright} />
          <InfoCard label="Port" value={String(broker.port)} mono bright={isBright} />
          <InfoCard label="Rack" value={broker.rack || "N/A"} bright={isBright} />
          <InfoCard label="Role" value={broker.isController ? "Controller" : "Follower"} color="cyan" bright={isBright} />
        </div>

        {/* Broker Config */}
        <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className="flex items-center justify-between mb-3">
            <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Broker Configuration
            </div>
            <div className="flex items-center gap-2">
              {brokerConfig?.brokerId === broker.id && Object.keys(configEdits).length > 0 && (
                <button
                  onClick={saveBrokerConfig}
                  disabled={savingConfig}
                  className={`px-2.5 py-1 rounded-lg text-[10px] font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                    isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700 hover:bg-emerald-100" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300 hover:bg-emerald-500/25"
                  }`}
                >
                  {savingConfig ? "Saving..." : `Save ${Object.keys(configEdits).length} changes`}
                </button>
              )}
              <button
                onClick={() => fetchBrokerConfig(broker.id)}
                className={`px-2.5 py-1 rounded-lg text-[10px] font-medium border transition-colors cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-400 hover:bg-slate-700"
                }`}
              >
                {brokerConfig?.brokerId === broker.id ? "Refresh Config" : "Load Config"}
              </button>
            </div>
          </div>
          {brokerConfig?.brokerId === broker.id ? (
            <>
              <input
                type="text"
                value={configFilter}
                onChange={(e) => setConfigFilter(e.target.value)}
                placeholder="Filter config keys..."
                className={`w-full mb-3 px-3 py-1.5 rounded-lg text-xs border focus:outline-none ${
                  isBright ? "bg-slate-50 border-slate-200 text-slate-700 placeholder-slate-400" : "bg-slate-800/60 border-slate-700/40 text-slate-300 placeholder-slate-500"
                }`}
              />
              <div className="max-h-64 overflow-y-auto space-y-1">
                {brokerConfig.configs
                  .filter((c) => !configFilter || c.name.toLowerCase().includes(configFilter.toLowerCase()))
                  .sort((a, b) => a.name.localeCompare(b.name))
                  .map((c) => (
                    <div key={c.name} className="flex items-center gap-2 py-0.5">
                      <span className={`text-[10px] font-mono w-64 shrink-0 truncate ${
                        c.source === "DYNAMIC_BROKER_CONFIG" ? (isBright ? "text-indigo-600" : "text-indigo-300")
                          : isBright ? "text-slate-500" : "text-slate-400"
                      }`} title={`${c.name} (${c.source})`}>{c.name}</span>
                      {c.isReadOnly || c.isSensitive ? (
                        <span className={`text-[10px] font-mono truncate ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                          {c.isSensitive ? "********" : c.value || "—"}
                        </span>
                      ) : (
                        <input
                          type="text"
                          value={configEdits[c.name] ?? c.value ?? ""}
                          onChange={(e) => setConfigEdits({ ...configEdits, [c.name]: e.target.value })}
                          className={`flex-1 px-2 py-0.5 rounded text-[10px] font-mono border focus:outline-none ${
                            configEdits[c.name] !== undefined && configEdits[c.name] !== c.value
                              ? isBright ? "bg-amber-50 border-amber-200 text-amber-700" : "bg-amber-500/10 border-amber-500/30 text-amber-300"
                              : isBright ? "bg-white border-slate-200 text-slate-700" : "bg-slate-800/60 border-slate-700/40 text-slate-300"
                          }`}
                        />
                      )}
                      <span className={`text-[8px] px-1 py-0.5 rounded shrink-0 ${
                        c.source === "DYNAMIC_BROKER_CONFIG" ? (isBright ? "bg-indigo-50 text-indigo-500" : "bg-indigo-500/10 text-indigo-400")
                          : c.source === "STATIC_BROKER_CONFIG" ? (isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400")
                          : isBright ? "bg-slate-50 text-slate-400" : "bg-slate-800/50 text-slate-500"
                      }`}>{c.source.replace(/_CONFIG$/, "").replace(/_/g, " ").toLowerCase()}</span>
                    </div>
                  ))}
              </div>
            </>
          ) : (
            <div className={`text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              Click "Load Config" to view and edit broker configuration
            </div>
          )}
        </div>

        {/* Estimated data size */}
        {topics.length > 0 && (() => {
          const totalPartitions = topics.reduce((s, t) => s + (t.partitions || 0), 0);
          const totalMessages = topics.reduce((s, t) => s + (t.totalMessages || 0), 0);
          const avgReplication = topics.length > 0 ? topics.reduce((s, t) => s + (t.replicationFactor || 1), 0) / topics.length : 1;
          const estBytesPerBroker = (totalMessages * 1024 * avgReplication) / brokers.length; // ~1KB avg msg
          const topicsPerBroker = Math.round(totalPartitions / brokers.length);
          return (
            <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Estimated Storage
              </div>
              <div className="grid grid-cols-3 gap-3">
                <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-cyan-50/50 border-cyan-200/40" : "bg-cyan-500/[0.06] border-cyan-500/15"}`}>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Est. Data Size</div>
                  <div className={`text-lg font-bold tabular-nums font-mono ${isBright ? "text-slate-800" : "text-white"}`}>{fmtSize(estBytesPerBroker)}</div>
                </div>
                <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Partitions</div>
                  <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>~{topicsPerBroker}</div>
                </div>
                <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Avg Replication</div>
                  <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{avgReplication.toFixed(1)}</div>
                </div>
              </div>
            </div>
          );
        })()}

        <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
          <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Connection Details</h3>
          <div className="grid grid-cols-1 gap-3">
            <DetailRow label="Address" value={`${broker.host}:${broker.port}`} mono isBright={isBright} />
            <DetailRow label="Node ID" value={String(broker.id)} isBright={isBright} />
            <DetailRow label="Rack ID" value={broker.rack || "Not configured"} isBright={isBright} />
            <DetailRow label="Is Controller" value={broker.isController ? "Yes" : "No"} isBright={isBright} />
            {clusterInfo && (
              <>
                <DetailRow label="Cluster ID" value={clusterInfo.clusterId || "Unknown"} mono isBright={isBright} />
                <DetailRow label="Total Brokers" value={String(clusterInfo.brokerCount)} isBright={isBright} />
              </>
            )}
          </div>
        </div>
      </div>
    );
  }

  const columns = [
    { key: "id", label: "ID", className: "w-20 text-center", render: (r: Record<string, unknown>) => (
      <span className={`font-mono font-bold ${isBright ? "text-cyan-600" : "text-cyan-300"}`}>{String(r.id)}</span>
    )},
    { key: "host", label: "Host", render: (r: Record<string, unknown>) => (
      <span className="font-mono">{String(r.host)}</span>
    )},
    { key: "port", label: "Port", className: "w-24 text-center", render: (r: Record<string, unknown>) => (
      <span className={`font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>{String(r.port)}</span>
    )},
    { key: "rack", label: "Rack", className: "w-28", render: (r: Record<string, unknown>) => (
      <span className={isBright ? "text-slate-500" : "text-slate-400"}>{r.rack ? String(r.rack) : "-"}</span>
    )},
    { key: "isController", label: "Role", className: "w-28", render: (r: Record<string, unknown>) => (
      r.isController
        ? <span className={`text-[10px] font-semibold uppercase px-2 py-1 rounded-lg border ${
            isBright ? "bg-cyan-50 text-cyan-700 border-cyan-200" : "bg-cyan-500/15 text-cyan-300 border-cyan-500/25"
          }`}>Controller</span>
        : <span className={`text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>Follower</span>
    )},
    { key: "address", label: "Address", className: "w-48", render: (r: Record<string, unknown>) => (
      <span className={`font-mono text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>{String(r.host)}:{String(r.port)}</span>
    )},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Brokers</h1>
          <div className="flex items-center gap-3 mt-0.5">
            <p className={`text-sm ${isBright ? "text-slate-500" : "text-slate-500"}`}>Cluster overview, broker health, and configuration</p>
            <FreshnessIndicator timestamp={brokersLastFetched} />
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              autoRefresh
                ? isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300"
                : isBright ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
            }`}
            title={autoRefresh ? "Stop auto-refresh (5s)" : "Auto-refresh every 5s"}
          >
            {autoRefresh ? "Auto (5s)" : "Auto"}
          </button>
          <button
            onClick={() => { fetchBrokers(); fetchClusterInfo(); }}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
            }`}
          >
            Refresh
          </button>
          {brokers.length > 0 && (
            <button
              onClick={() => {
                const data = { brokers, clusterInfo };
                const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url; a.download = "kafka-brokers.json"; a.click();
                URL.revokeObjectURL(url);
              }}
              className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                isBright
                  ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                  : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
              }`}
            >
              Export
            </button>
          )}
        </div>
      </div>

      {clusterInfo && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <InfoCard label="Cluster ID" value={clusterInfo.clusterId || "-"} mono bright={isBright} />
          <InfoCard label="Controller" value={`Broker ${clusterInfo.controllerId}`} color="cyan" bright={isBright} />
          <InfoCard label="Brokers" value={String(clusterInfo.brokerCount)} bright={isBright} />
          <InfoCard label="Topics" value={String(clusterInfo.topicCount)} color="indigo" bright={isBright} />
          <InfoCard label="Consumer Groups" value={String(clusterInfo.consumerGroupCount)} color="amber" bright={isBright} />
        </div>
      )}

      {/* Cluster Health */}
      {brokers.length > 0 && topics.length > 0 && (
        <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
            Cluster Health
          </div>
          <div className="grid grid-cols-3 md:grid-cols-6 gap-3">
            <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-emerald-50/50 border-emerald-200/40" : "bg-emerald-500/[0.06] border-emerald-500/15"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Total Partitions</div>
              <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{healthMetrics.totalPartitions.toLocaleString()}</div>
            </div>
            <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Avg/Broker</div>
              <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{healthMetrics.avgPartitionsPerBroker}</div>
            </div>
            <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-indigo-50/50 border-indigo-200/40" : "bg-indigo-500/[0.06] border-indigo-500/15"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Total Messages</div>
              <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{fmt(healthMetrics.totalMessages)}</div>
            </div>
            <div className={`rounded-xl px-3 py-2.5 border ${
              healthMetrics.totalThroughput > 0
                ? isBright ? "bg-emerald-50/50 border-emerald-200/40" : "bg-emerald-500/[0.06] border-emerald-500/15"
                : isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"
            }`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Throughput</div>
              <div className={`text-lg font-bold tabular-nums font-mono ${healthMetrics.totalThroughput > 0 ? "text-emerald-500" : isBright ? "text-slate-400" : "text-slate-500"}`}>
                {healthMetrics.totalThroughput > 0 ? `${healthMetrics.totalThroughput.toFixed(0)} msg/s` : "idle"}
              </div>
            </div>
            <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Avg Replication</div>
              <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{healthMetrics.avgReplication.toFixed(1)}</div>
            </div>
            <div className={`rounded-xl px-3 py-2.5 border ${
              healthMetrics.topicsAtRisk > 0
                ? isBright ? "bg-red-50/50 border-red-200/40" : "bg-red-500/[0.06] border-red-500/15"
                : isBright ? "bg-emerald-50/50 border-emerald-200/40" : "bg-emerald-500/[0.06] border-emerald-500/15"
            }`}>
              <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>At Risk (RF=1)</div>
              <div className={`text-lg font-bold tabular-nums ${
                healthMetrics.topicsAtRisk > 0 ? "text-red-500" : "text-emerald-500"
              }`}>{healthMetrics.topicsAtRisk}</div>
            </div>
          </div>
        </div>
      )}

      {/* Cluster health score and leader distribution */}
      {clusterHealth && brokers.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {/* Health Score */}
          <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Health Score
            </div>
            {(() => {
              let score = 100;
              const issues: string[] = [];
              if (clusterHealth.offlinePartitionCount > 0) {
                score -= 40;
                issues.push(`${clusterHealth.offlinePartitionCount} offline partitions`);
              }
              if (clusterHealth.underReplicatedCount > 0) {
                score -= Math.min(30, clusterHealth.underReplicatedCount * 5);
                issues.push(`${clusterHealth.underReplicatedCount} under-replicated`);
              }
              if (healthMetrics.topicsAtRisk > 0) {
                score -= Math.min(15, healthMetrics.topicsAtRisk * 3);
                issues.push(`${healthMetrics.topicsAtRisk} topics with RF=1`);
              }
              if (brokers.length < 3) {
                score -= 10;
                issues.push("Fewer than 3 brokers");
              }
              score = Math.max(0, score);
              const color = score >= 90 ? "emerald" : score >= 70 ? "amber" : "red";
              const colorCls = color === "emerald" ? "text-emerald-500" : color === "amber" ? "text-amber-500" : "text-red-500";
              const bgCls = color === "emerald"
                ? isBright ? "bg-emerald-500" : "bg-emerald-500"
                : color === "amber" ? "bg-amber-500" : "bg-red-500";
              return (
                <div>
                  <div className="flex items-end gap-3 mb-3">
                    <span className={`text-4xl font-bold tabular-nums ${colorCls}`}>{score}</span>
                    <span className={`text-sm font-medium mb-1 ${colorCls}`}>
                      {score >= 90 ? "Healthy" : score >= 70 ? "Degraded" : "Critical"}
                    </span>
                  </div>
                  <div className={`w-full h-2 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                    <div className={`h-full rounded-full transition-all duration-700 ${bgCls}`} style={{ width: `${score}%` }} />
                  </div>
                  {issues.length > 0 && (
                    <div className="mt-3 space-y-1">
                      {issues.map((issue) => (
                        <div key={issue} className={`text-[10px] flex items-center gap-1.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                          <span className="w-1.5 h-1.5 rounded-full bg-amber-500 shrink-0" />
                          {issue}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              );
            })()}
          </div>

          {/* Leader Distribution */}
          {clusterHealth.leaderDistribution && Object.keys(clusterHealth.leaderDistribution).length > 0 && (
            <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Leader Distribution
              </div>
              {(() => {
                const entries = Object.entries(clusterHealth.leaderDistribution).sort((a, b) => Number(a[0]) - Number(b[0]));
                const maxVal = Math.max(...entries.map(([, v]) => v), 1);
                const total = entries.reduce((s, [, v]) => s + v, 0);
                return (
                  <div className="space-y-2">
                    {entries.map(([brokerId, count]) => {
                      const pct = total > 0 ? ((count / total) * 100).toFixed(0) : "0";
                      return (
                        <div key={brokerId} className="flex items-center gap-3">
                          <span className={`text-[11px] font-mono w-16 text-right shrink-0 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                            Broker {brokerId}
                          </span>
                          <div className={`flex-1 rounded-full overflow-hidden h-3 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                            <div
                              className={`h-full rounded-full transition-all duration-500 ${isBright ? "bg-cyan-400" : "bg-cyan-500/70"}`}
                              style={{ width: `${(count / maxVal) * 100}%` }}
                            />
                          </div>
                          <span className={`text-[11px] font-mono font-bold w-16 text-right ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                            {count} ({pct}%)
                          </span>
                        </div>
                      );
                    })}
                  </div>
                );
              })()}
            </div>
          )}
        </div>
      )}

      {/* Under-replicated partitions warning */}
      {clusterHealth && (clusterHealth.underReplicatedCount > 0 || clusterHealth.offlinePartitionCount > 0) && (
        <div className="space-y-3">
          {clusterHealth.offlinePartitionCount > 0 && (
            <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-red-200/60 bg-red-50/50" : "border-red-500/20 bg-red-950/20"}`}>
              <div className="flex items-center gap-2 mb-2">
                <span className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded bg-red-500/20 text-red-500`}>Critical</span>
                <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-red-600" : "text-red-400"}`}>
                  {clusterHealth.offlinePartitionCount} Offline Partition{clusterHealth.offlinePartitionCount > 1 ? "s" : ""}
                </span>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {clusterHealth.offlinePartitions.slice(0, 20).map((p) => (
                  <span key={`${p.topic}-${p.partition}`} className={`text-[10px] font-mono px-2 py-1 rounded-lg border ${
                    isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
                  }`}>
                    {p.topic}:{p.partition}
                  </span>
                ))}
              </div>
            </div>
          )}
          {clusterHealth.underReplicatedCount > 0 && (
            <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-amber-200/60 bg-amber-50/50" : "border-amber-500/20 bg-amber-950/20"}`}>
              <div className="flex items-center gap-2 mb-2">
                <span className={`text-[10px] font-bold uppercase px-2 py-0.5 rounded bg-amber-500/20 text-amber-500`}>Warning</span>
                <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-amber-600" : "text-amber-400"}`}>
                  {clusterHealth.underReplicatedCount} Under-Replicated Partition{clusterHealth.underReplicatedCount > 1 ? "s" : ""}
                </span>
              </div>
              <div className="flex flex-wrap gap-1.5">
                {clusterHealth.underReplicated.slice(0, 20).map((p) => (
                  <span key={`${p.topic}-${p.partition}`} className={`text-[10px] font-mono px-2 py-1 rounded-lg border ${
                    isBright ? "bg-amber-50 border-amber-200 text-amber-700" : "bg-amber-950/50 border-amber-500/30 text-amber-300"
                  }`} title={`ISR: ${p.isr}/${p.replicas}`}>
                    {p.topic}:{p.partition} ({p.isr}/{p.replicas})
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Log Dirs / Disk Usage */}
      {logDirs.length > 0 && (
        <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className="flex items-center justify-between mb-3">
            <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Disk Usage by Broker
            </span>
            <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              {logDirs.some((d) => d.estimated) ? "estimated" : "actual"}
            </span>
          </div>
          {(() => {
            const maxSize = Math.max(...logDirs.map((d) => d.size), 1);
            const totalSize = logDirs.reduce((s, d) => s + d.size, 0);
            return (
              <div className="space-y-2">
                {logDirs.map((d) => {
                  const pct = d.size > 0 ? (d.size / maxSize) * 100 : 0;
                  return (
                    <div key={`${d.brokerId}-${d.logDir}`} className="flex items-center gap-3">
                      <span className={`text-[11px] font-mono w-20 text-right shrink-0 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                        Broker {d.brokerId}
                      </span>
                      <div className={`flex-1 rounded-full overflow-hidden h-3 ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                        <div
                          className={`h-full rounded-full transition-all duration-500 ${isBright ? "bg-violet-400" : "bg-violet-500/70"}`}
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className={`text-[11px] font-mono font-bold w-20 text-right ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                        {fmtSize(d.size)}
                      </span>
                      <span className={`text-[10px] font-mono w-16 text-right ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                        {d.partitionCount}p
                      </span>
                    </div>
                  );
                })}
                <div className={`flex justify-end pt-1 border-t ${isBright ? "border-slate-200/40" : "border-slate-700/20"}`}>
                  <span className={`text-[11px] font-mono font-bold ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                    Total: {fmtSize(totalSize)}
                  </span>
                </div>
              </div>
            );
          })()}
        </div>
      )}

      {/* Partition distribution across brokers */}
      {brokers.length > 1 && topics.length > 0 && (
        <BrokerPartitionChart brokers={brokers} topics={topics} bright={isBright} />
      )}

      {/* Broker topology with rack awareness */}
      {brokers.length > 0 && (() => {
        const rackMap = new Map<string, typeof brokers>();
        for (const b of brokers) {
          const rack = b.rack || "default";
          const list = rackMap.get(rack) || [];
          list.push(b);
          rackMap.set(rack, list);
        }
        const racks = [...rackMap.entries()].sort((a, b) => a[0].localeCompare(b[0]));
        const hasRacks = racks.length > 1 || (racks.length === 1 && racks[0][0] !== "default");
        const rackColors = ["cyan", "violet", "amber", "emerald", "rose", "blue"];

        return (
          <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className="flex items-center justify-between mb-3">
              <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Broker Topology
              </span>
              {hasRacks && (
                <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                  {racks.length} racks
                </span>
              )}
            </div>
            <div className={`flex ${hasRacks ? "gap-4" : "gap-3 justify-center"} flex-wrap`}>
              {racks.map(([rack, rackBrokers], rackIdx) => (
                <div key={rack} className={hasRacks ? `flex-1 min-w-[180px] rounded-xl border p-3 ${
                  isBright ? "border-slate-200/40 bg-slate-50/50" : "border-slate-700/20 bg-slate-800/20"
                }` : ""}>
                  {hasRacks && (
                    <div className={`text-[10px] font-bold uppercase mb-2 ${
                      isBright ? `text-${rackColors[rackIdx % rackColors.length]}-600` : `text-${rackColors[rackIdx % rackColors.length]}-400`
                    }`}>
                      Rack: {rack}
                    </div>
                  )}
                  <div className="flex items-end gap-2 justify-center">
                    {rackBrokers.map((b) => (
                      <button
                        key={b.id}
                        onClick={() => setSelectedBroker(b.id)}
                        className={`flex flex-col items-center gap-1.5 px-4 py-3 rounded-xl border transition-all cursor-pointer ${
                          b.isController
                            ? isBright
                              ? "bg-cyan-50 border-cyan-200/60 hover:border-cyan-400"
                              : "bg-cyan-500/10 border-cyan-500/30 hover:border-cyan-400"
                            : isBright
                              ? "bg-white border-slate-200/60 hover:border-slate-400"
                              : "bg-slate-800/40 border-slate-700/30 hover:border-slate-500"
                        }`}
                      >
                        <div className={`w-10 h-10 rounded-xl flex items-center justify-center text-sm font-bold ${
                          b.isController
                            ? isBright ? "bg-cyan-100 text-cyan-700" : "bg-cyan-500/20 text-cyan-300"
                            : isBright ? "bg-slate-100 text-slate-600" : "bg-slate-700 text-slate-300"
                        }`}>
                          {b.id}
                        </div>
                        <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                          {b.host}:{b.port}
                        </span>
                        {b.isController && (
                          <span className={`text-[9px] font-semibold uppercase ${isBright ? "text-cyan-600" : "text-cyan-400"}`}>
                            Controller
                          </span>
                        )}
                        {hasRacks && b.rack && (
                          <span className={`text-[8px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                            {b.rack}
                          </span>
                        )}
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        );
      })()}

      {/* Broker comparison panel */}
      {compareBrokers && (() => {
        const a = brokers.find((b) => b.id === compareBrokers[0]);
        const b2 = brokers.find((b) => b.id === compareBrokers[1]);
        if (!a || !b2) return null;
        const leaderDist = clusterHealth?.leaderDistribution || {};
        const aLeaders = leaderDist[String(a.id)] || 0;
        const bLeaders = leaderDist[String(b2.id)] || 0;
        const totalParts = topics.reduce((s, t) => s + (t.partitions || 0), 0);
        const avgRepl = topics.length > 0 ? topics.reduce((s, t) => s + (t.replicationFactor || 1), 0) / topics.length : 1;
        const replPerBroker = Math.round((totalParts * avgRepl) / brokers.length);
        const rows: { label: string; va: string; vb: string; diff: boolean }[] = [
          { label: "Host", va: a.host, vb: b2.host, diff: a.host !== b2.host },
          { label: "Port", va: String(a.port), vb: String(b2.port), diff: a.port !== b2.port },
          { label: "Rack", va: a.rack || "N/A", vb: b2.rack || "N/A", diff: a.rack !== b2.rack },
          { label: "Role", va: a.isController ? "Controller" : "Follower", vb: b2.isController ? "Controller" : "Follower", diff: a.isController !== b2.isController },
          { label: "Leader Partitions", va: String(aLeaders), vb: String(bLeaders), diff: aLeaders !== bLeaders },
          { label: "Est. Replicas", va: `~${replPerBroker}`, vb: `~${replPerBroker}`, diff: false },
        ];
        return (
          <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className="flex items-center justify-between mb-3">
              <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Broker Comparison
              </span>
              <button onClick={() => setCompareBrokers(null)} className={`text-[10px] px-2 py-0.5 rounded-md border cursor-pointer ${isBright ? "border-slate-200 text-slate-400 hover:text-slate-600" : "border-slate-700/40 text-slate-500 hover:text-slate-300"}`}>
                Close
              </button>
            </div>
            <div className={`grid grid-cols-3 gap-2 text-[11px] pb-2 mb-2 border-b ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
              <span />
              <span className={`font-mono font-bold text-center ${isBright ? "text-cyan-600" : "text-cyan-400"}`}>Broker {a.id}</span>
              <span className={`font-mono font-bold text-center ${isBright ? "text-cyan-600" : "text-cyan-400"}`}>Broker {b2.id}</span>
            </div>
            <div className="space-y-1">
              {rows.map((r) => (
                <div key={r.label} className={`grid grid-cols-3 gap-2 py-1.5 px-2 rounded-lg text-[11px] ${
                  r.diff
                    ? isBright ? "bg-amber-50/50" : "bg-amber-500/[0.04]"
                    : ""
                }`}>
                  <span className={isBright ? "text-slate-500" : "text-slate-400"}>{r.label}</span>
                  <span className={`font-mono text-center ${isBright ? "text-slate-700" : "text-slate-200"}`}>{r.va}</span>
                  <span className={`font-mono text-center ${isBright ? "text-slate-700" : "text-slate-200"}`}>{r.vb}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })()}

      {/* Compare broker selection */}
      {brokers.length >= 2 && !compareBrokers && (
        <div className="flex gap-2 flex-wrap">
          {brokers.length >= 2 && brokers.length <= 6 && (
            <div className="flex gap-1.5 items-center">
              <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Compare:</span>
              {brokers.map((b1, i) =>
                brokers.slice(i + 1).map((b2) => (
                  <button
                    key={`${b1.id}-${b2.id}`}
                    onClick={() => setCompareBrokers([b1.id, b2.id])}
                    className={`px-2 py-0.5 rounded-md text-[10px] font-mono border cursor-pointer transition-colors ${
                      isBright ? "border-slate-200/60 text-slate-400 hover:bg-slate-50 hover:text-slate-600" : "border-slate-700/40 text-slate-500 hover:bg-slate-800 hover:text-slate-300"
                    }`}
                  >
                    {b1.id} vs {b2.id}
                  </button>
                ))
              )}
            </div>
          )}
        </div>
      )}

      {brokersLoading && brokers.length === 0 ? (
        <SkeletonTable rows={3} cols={4} />
      ) : (
        <DataTable
          columns={columns}
          data={brokers as unknown as Record<string, unknown>[]}
          onRowClick={(row) => setSelectedBroker(Number(row.id))}
          searchPlaceholder="Filter brokers..."
          searchKeys={["host", "id"]}
          emptyMessage="No brokers found"
        />
      )}
    </div>
  );
}

function BrokerPartitionChart({ brokers, topics, bright }: { brokers: { id: number }[]; topics: { partitions?: number; replicationFactor?: number }[]; bright: boolean }) {
  // Estimate partitions per broker: totalPartitions * replicationFactor / brokerCount
  const brokerCount = brokers.length;
  const totalPartitions = topics.reduce((s, t) => s + (t.partitions || 0), 0);
  const avgReplication = topics.length > 0
    ? topics.reduce((s, t) => s + (t.replicationFactor || 1), 0) / topics.length
    : 1;
  const totalReplicas = Math.round(totalPartitions * avgReplication);
  const perBroker = Math.round(totalReplicas / brokerCount);
  const maxVal = Math.max(perBroker + 10, 1);

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-3">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Estimated Partition Replicas per Broker
        </span>
        <span className={`text-[11px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>
          ~{totalReplicas} total replicas
        </span>
      </div>
      <div className="space-y-2">
        {brokers.map((b) => (
          <div key={b.id} className="flex items-center gap-3">
            <span className={`text-[11px] font-mono w-16 text-right shrink-0 ${bright ? "text-slate-500" : "text-slate-400"}`}>
              Broker {b.id}
            </span>
            <div className={`flex-1 rounded-full overflow-hidden h-3 ${bright ? "bg-slate-100" : "bg-slate-800/60"}`}>
              <div
                className={`h-full rounded-full transition-all duration-500 ${bright ? "bg-cyan-400" : "bg-cyan-500/70"}`}
                style={{ width: `${(perBroker / maxVal) * 100}%` }}
              />
            </div>
            <span className={`text-[11px] font-mono font-bold w-10 ${bright ? "text-slate-600" : "text-slate-300"}`}>
              ~{perBroker}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DetailRow({ label, value, mono, isBright }: { label: string; value: string; mono?: boolean; isBright: boolean }) {
  return (
    <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
      <span className={`text-xs font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>{label}</span>
      <span className={`text-sm font-medium ${mono ? "font-mono" : ""} ${isBright ? "text-slate-800" : "text-white"}`}>{value}</span>
    </div>
  );
}

function InfoCard({ label, value, mono, color, bright }: { label: string; value: string; mono?: boolean; color?: string; bright: boolean }) {
  const darkColorMap: Record<string, string> = {
    cyan: "border-cyan-500/20 from-cyan-500/[0.06]",
    indigo: "border-indigo-500/20 from-indigo-500/[0.06]",
    amber: "border-amber-500/20 from-amber-500/[0.06]",
  };
  const brightColorMap: Record<string, string> = {
    cyan: "border-cyan-200/60 from-cyan-50",
    indigo: "border-indigo-200/60 from-indigo-50",
    amber: "border-amber-200/60 from-amber-50",
  };
  const colorMap = bright ? brightColorMap : darkColorMap;
  const cls = color ? (colorMap[color] || "") : (bright ? "border-slate-200/60 from-slate-50" : "border-slate-700/30 from-slate-500/[0.04]");
  const textColor = color === "cyan" ? (bright ? "text-cyan-600" : "text-cyan-300") :
    color === "indigo" ? (bright ? "text-indigo-600" : "text-indigo-300") :
    color === "amber" ? (bright ? "text-amber-600" : "text-amber-300") :
    (bright ? "text-slate-800" : "text-white");
  return (
    <div className={`rounded-2xl border bg-gradient-to-br ${cls} to-transparent px-4 py-3`}>
      <div className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>{label}</div>
      <div className={`text-sm font-bold mt-0.5 truncate ${mono ? "font-mono text-xs" : ""} ${textColor}`} title={value}>
        {value}
      </div>
    </div>
  );
}
