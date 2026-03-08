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

  const totalLag = consumerGroups.reduce((s, g) => s + (g.totalLag || 0), 0);
  const stableCount = consumerGroups.filter((g) => g.status === "Stable").length;

  const columns = [
    { key: "groupId", label: "Group ID", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-amber-300 font-medium">{String(r.groupId)}</span>
    )},
    { key: "status", label: "Status", className: "w-28", render: (r: Record<string, unknown>) => {
      const s = String(r.status);
      const color = s === "Stable" ? "bg-emerald-500/15 text-emerald-400 border-emerald-500/25" : s === "Empty" ? "bg-slate-800/50 text-slate-400 border-slate-700/40" : "bg-amber-500/15 text-amber-400 border-amber-500/25";
      return <span className={`text-[10px] font-semibold uppercase px-2 py-1 rounded-lg border ${color}`}>{s}</span>;
    }},
    { key: "members", label: "Members", className: "w-24 text-center" },
    { key: "totalLag", label: "Total Lag", className: "w-32 text-right", render: (r: Record<string, unknown>) => {
      const lag = Number(r.totalLag);
      const color = lag > 1000 ? "text-red-400" : lag > 0 ? "text-amber-400" : "text-emerald-400";
      return <span className={`font-mono font-medium tabular-nums ${color}`}>{fmt(lag)}</span>;
    }},
    { key: "topics", label: "Topics", render: (r: Record<string, unknown>) => {
      const topics = r.topics as string[];
      if (!topics || topics.length === 0) return <span className="text-slate-500">-</span>;
      return (
        <div className="flex gap-1 flex-wrap">
          {topics.slice(0, 3).map((t) => (
            <span key={t} className="text-[9px] bg-indigo-500/10 text-indigo-300/80 rounded-md px-1.5 py-0.5 border border-indigo-500/15">{t}</span>
          ))}
          {topics.length > 3 && <span className="text-[9px] text-slate-500">+{topics.length - 3}</span>}
        </div>
      );
    }},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white">Consumer Groups</h1>
          <p className="text-sm text-slate-500 mt-0.5">Monitor consumer lag, members, and offset management</p>
        </div>
        <button
          onClick={() => fetchConsumerGroups()}
          className="px-3.5 py-2 rounded-xl text-xs font-medium bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300 transition-all cursor-pointer"
        >
          Refresh
        </button>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-3">
        <SummaryCard label="Consumer Groups" value={String(consumerGroups.length)} color="amber" />
        <SummaryCard label="Stable" value={String(stableCount)} color="emerald" />
        <SummaryCard label="Total Lag" value={fmt(totalLag)} color={totalLag > 1000 ? "red" : "slate"} />
      </div>

      {/* Table */}
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

function SummaryCard({ label, value, color }: { label: string; value: string; color: string }) {
  const colorMap: Record<string, string> = {
    amber: "border-amber-500/20 from-amber-500/[0.06]",
    emerald: "border-emerald-500/20 from-emerald-500/[0.06]",
    red: "border-red-500/20 from-red-500/[0.06]",
    slate: "border-slate-700/30 from-slate-500/[0.04]",
  };
  return (
    <div className={`rounded-2xl border bg-gradient-to-br ${colorMap[color] || colorMap.slate} to-transparent px-4 py-3`}>
      <div className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">{label}</div>
      <div className="text-lg font-bold text-white mt-0.5 tabular-nums">{value}</div>
    </div>
  );
}
