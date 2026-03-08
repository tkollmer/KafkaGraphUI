import { useEffect } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { DataTable } from "../components/DataTable";

export function BrokersView() {
  const { brokers, brokersLoading, clusterInfo, fetchBrokers, fetchClusterInfo } = useKafkaStore();

  useEffect(() => {
    fetchBrokers();
    fetchClusterInfo();
  }, [fetchBrokers, fetchClusterInfo]);

  const columns = [
    { key: "id", label: "ID", className: "w-20 text-center", render: (r: Record<string, unknown>) => (
      <span className="font-mono font-bold text-cyan-300">{String(r.id)}</span>
    )},
    { key: "host", label: "Host", render: (r: Record<string, unknown>) => (
      <span className="font-mono">{String(r.host)}</span>
    )},
    { key: "port", label: "Port", className: "w-24 text-center", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-slate-400">{String(r.port)}</span>
    )},
    { key: "rack", label: "Rack", className: "w-28", render: (r: Record<string, unknown>) => (
      <span className="text-slate-400">{r.rack ? String(r.rack) : "-"}</span>
    )},
    { key: "isController", label: "Role", className: "w-28", render: (r: Record<string, unknown>) => (
      r.isController
        ? <span className="text-[10px] font-semibold uppercase px-2 py-1 rounded-lg bg-cyan-500/15 text-cyan-300 border border-cyan-500/25">Controller</span>
        : <span className="text-slate-500 text-xs">Follower</span>
    )},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white">Brokers</h1>
          <p className="text-sm text-slate-500 mt-0.5">Cluster overview, broker health, and configuration</p>
        </div>
        <button
          onClick={() => { fetchBrokers(); fetchClusterInfo(); }}
          className="px-3.5 py-2 rounded-xl text-xs font-medium bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300 transition-all cursor-pointer"
        >
          Refresh
        </button>
      </div>

      {/* Cluster overview cards */}
      {clusterInfo && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          <InfoCard label="Cluster ID" value={clusterInfo.clusterId || "-"} mono />
          <InfoCard label="Controller" value={`Broker ${clusterInfo.controllerId}`} color="cyan" />
          <InfoCard label="Brokers" value={String(clusterInfo.brokerCount)} />
          <InfoCard label="Topics" value={String(clusterInfo.topicCount)} color="indigo" />
          <InfoCard label="Consumer Groups" value={String(clusterInfo.consumerGroupCount)} color="amber" />
        </div>
      )}

      {/* Broker table */}
      {brokersLoading && brokers.length === 0 ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <DataTable
          columns={columns}
          data={brokers as unknown as Record<string, unknown>[]}
          searchPlaceholder="Filter brokers..."
          searchKeys={["host", "id"]}
          emptyMessage="No brokers found"
        />
      )}
    </div>
  );
}

function InfoCard({ label, value, mono, color }: { label: string; value: string; mono?: boolean; color?: string }) {
  const colorMap: Record<string, string> = {
    cyan: "border-cyan-500/20 from-cyan-500/[0.06]",
    indigo: "border-indigo-500/20 from-indigo-500/[0.06]",
    amber: "border-amber-500/20 from-amber-500/[0.06]",
  };
  const cls = color ? colorMap[color] || "" : "border-slate-700/30 from-slate-500/[0.04]";
  const textColor = color === "cyan" ? "text-cyan-300" : color === "indigo" ? "text-indigo-300" : color === "amber" ? "text-amber-300" : "text-white";
  return (
    <div className={`rounded-2xl border bg-gradient-to-br ${cls} to-transparent px-4 py-3`}>
      <div className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">{label}</div>
      <div className={`text-sm font-bold mt-0.5 truncate ${mono ? "font-mono text-xs" : ""} ${textColor}`} title={value}>
        {value}
      </div>
    </div>
  );
}
