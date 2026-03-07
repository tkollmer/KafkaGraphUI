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
    { key: "id", label: "Broker ID", className: "w-28 text-center", render: (r: Record<string, unknown>) => (
      <span className="font-mono font-bold text-cyan-300">{String(r.id)}</span>
    )},
    { key: "host", label: "Host", render: (r: Record<string, unknown>) => (
      <span className="font-mono">{String(r.host)}</span>
    )},
    { key: "port", label: "Port", className: "w-24 text-center", render: (r: Record<string, unknown>) => (
      <span className="font-mono">{String(r.port)}</span>
    )},
    { key: "rack", label: "Rack", className: "w-28", render: (r: Record<string, unknown>) => (
      <span className="text-slate-400">{r.rack ? String(r.rack) : "-"}</span>
    )},
    { key: "isController", label: "Role", className: "w-28", render: (r: Record<string, unknown>) => (
      r.isController
        ? <span className="text-[9px] font-bold uppercase px-2 py-0.5 rounded bg-cyan-500/20 text-cyan-300 border border-cyan-500/30">Controller</span>
        : <span className="text-slate-500 text-xs">Follower</span>
    )},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold text-white">Brokers</h1>
          <p className="text-sm text-slate-400 mt-1">{brokers.length} brokers</p>
        </div>
        <button
          onClick={() => { fetchBrokers(); fetchClusterInfo(); }}
          className="px-3 py-1.5 rounded-xl text-xs font-medium bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300 transition-all cursor-pointer"
        >
          Refresh
        </button>
      </div>

      {/* Cluster overview cards */}
      {clusterInfo && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">
          <InfoCard label="Cluster ID" value={clusterInfo.clusterId || "-"} mono />
          <InfoCard label="Controller" value={String(clusterInfo.controllerId)} accent />
          <InfoCard label="Brokers" value={String(clusterInfo.brokerCount)} />
          <InfoCard label="Topics" value={String(clusterInfo.topicCount)} />
          <InfoCard label="Consumer Groups" value={String(clusterInfo.consumerGroupCount)} />
        </div>
      )}

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

function InfoCard({ label, value, mono, accent }: { label: string; value: string; mono?: boolean; accent?: boolean }) {
  return (
    <div className="bg-slate-900/80 backdrop-blur-xl border border-slate-700/50 rounded-2xl px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">{label}</div>
      <div className={`text-sm font-bold mt-1 truncate ${mono ? "font-mono" : ""} ${accent ? "text-cyan-300" : "text-white"}`} title={value}>
        {value}
      </div>
    </div>
  );
}
