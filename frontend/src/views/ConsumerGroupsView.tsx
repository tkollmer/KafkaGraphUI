import { useEffect, useState } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { DataTable } from "../components/DataTable";
import { ConsumerGroupDetail } from "./ConsumerGroupDetail";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function ConsumerGroupsView() {
  const { consumerGroups, consumerGroupsLoading, fetchConsumerGroups } = useKafkaStore();
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(null);

  useEffect(() => { fetchConsumerGroups(); }, [fetchConsumerGroups]);

  if (selectedGroupId) {
    return <ConsumerGroupDetail groupId={selectedGroupId} onBack={() => setSelectedGroupId(null)} />;
  }

  const columns = [
    { key: "groupId", label: "Group ID", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-amber-300">{String(r.groupId)}</span>
    )},
    { key: "status", label: "Status", className: "w-28", render: (r: Record<string, unknown>) => {
      const s = String(r.status);
      const color = s === "Stable" ? "text-emerald-400" : s === "Empty" ? "text-slate-400" : "text-amber-400";
      return <span className={`text-xs font-medium ${color}`}>{s}</span>;
    }},
    { key: "members", label: "Members", className: "w-24 text-center" },
    { key: "totalLag", label: "Total Lag", className: "w-32 text-right", render: (r: Record<string, unknown>) => {
      const lag = Number(r.totalLag);
      const color = lag > 1000 ? "text-red-400" : lag > 0 ? "text-amber-400" : "text-emerald-400";
      return <span className={`font-mono ${color}`}>{fmt(lag)}</span>;
    }},
    { key: "topics", label: "Topics", render: (r: Record<string, unknown>) => {
      const topics = r.topics as string[];
      return (
        <div className="flex gap-1 flex-wrap">
          {topics.slice(0, 3).map((t) => (
            <span key={t} className="text-[9px] bg-indigo-500/15 text-indigo-300 rounded px-1.5 py-0.5 border border-indigo-500/20">{t}</span>
          ))}
          {topics.length > 3 && <span className="text-[9px] text-slate-500">+{topics.length - 3}</span>}
        </div>
      );
    }},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold text-white">Consumer Groups</h1>
          <p className="text-sm text-slate-400 mt-1">{consumerGroups.length} groups</p>
        </div>
        <button
          onClick={() => fetchConsumerGroups()}
          className="px-3 py-1.5 rounded-xl text-xs font-medium bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300 transition-all cursor-pointer"
        >
          Refresh
        </button>
      </div>

      {consumerGroupsLoading && consumerGroups.length === 0 ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-amber-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <DataTable
          columns={columns}
          data={consumerGroups as unknown as Record<string, unknown>[]}
          onRowClick={(row) => setSelectedGroupId(String(row.groupId))}
          searchPlaceholder="Filter consumer groups..."
          searchKeys={["groupId"]}
          emptyMessage="No consumer groups found"
        />
      )}
    </div>
  );
}
