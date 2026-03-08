import { useEffect, useState } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { DataTable } from "../components/DataTable";
import { Modal } from "../components/Modal";

interface Props {
  groupId: string;
  onBack: () => void;
}

type Tab = "members" | "offsets";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function ConsumerGroupDetail({ groupId, onBack }: Props) {
  const { selectedConsumerGroup, consumerGroupDetailLoading, fetchConsumerGroupDetail, resetOffsets } = useKafkaStore();
  const [activeTab, setActiveTab] = useState<Tab>("members");
  const [showReset, setShowReset] = useState(false);
  const [resetStrategy, setResetStrategy] = useState("latest");
  const [resetTopic, setResetTopic] = useState("");
  const [resetResult, setResetResult] = useState<{ success: boolean; message: string } | null>(null);
  const [resetting, setResetting] = useState(false);

  useEffect(() => { fetchConsumerGroupDetail(groupId); }, [groupId, fetchConsumerGroupDetail]);

  const handleReset = async () => {
    setResetting(true);
    setResetResult(null);
    const result = await resetOffsets(groupId, resetStrategy, resetTopic || undefined);
    if (result.success) {
      setResetResult({ success: true, message: "Offsets reset successfully" });
      fetchConsumerGroupDetail(groupId);
    } else {
      setResetResult({ success: false, message: result.error || "Failed to reset offsets" });
    }
    setResetting(false);
  };

  const memberColumns = [
    { key: "clientId", label: "Client ID", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-amber-300 font-medium">{String(r.clientId)}</span>
    )},
    { key: "clientHost", label: "Host", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-slate-400">{String(r.clientHost)}</span>
    )},
    { key: "partitions", label: "Assigned Partitions", render: (r: Record<string, unknown>) => {
      const parts = r.partitions as string[];
      if (!parts || parts.length === 0) return <span className="text-slate-500">-</span>;
      return (
        <div className="flex gap-1 flex-wrap">
          {parts.slice(0, 5).map((p) => (
            <span key={p} className="text-[9px] bg-slate-800/60 text-slate-300 rounded-md px-1.5 py-0.5 font-mono border border-slate-700/30">{p}</span>
          ))}
          {parts.length > 5 && <span className="text-[9px] text-slate-500">+{parts.length - 5}</span>}
        </div>
      );
    }},
  ];

  const offsetColumns = [
    { key: "topic", label: "Topic", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-indigo-300 font-medium">{String(r.topic)}</span>
    )},
    { key: "partition", label: "Partition", className: "w-24 text-center" },
    { key: "currentOffset", label: "Current Offset", className: "w-36 text-right", render: (r: Record<string, unknown>) => (
      <span className="font-mono tabular-nums">{Number(r.currentOffset).toLocaleString()}</span>
    )},
    { key: "endOffset", label: "End Offset", className: "w-36 text-right", render: (r: Record<string, unknown>) => (
      <span className="font-mono tabular-nums">{Number(r.endOffset).toLocaleString()}</span>
    )},
    { key: "lag", label: "Lag", className: "w-28 text-right", render: (r: Record<string, unknown>) => {
      const lag = Number(r.lag);
      const color = lag > 1000 ? "text-red-400" : lag > 0 ? "text-amber-400" : "text-emerald-400";
      return <span className={`font-mono font-bold tabular-nums ${color}`}>{fmt(lag)}</span>;
    }},
  ];

  const tabs: { id: Tab; label: string }[] = [
    { id: "members", label: "Members" },
    { id: "offsets", label: "Offsets" },
  ];

  const topicsList = selectedConsumerGroup
    ? [...new Set(selectedConsumerGroup.offsets.map((o) => o.topic))]
    : [];

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      {/* Back + Header */}
      <div className="flex items-center gap-4">
        <button
          onClick={onBack}
          className="px-3 py-2 rounded-xl text-xs font-medium bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300 transition-all cursor-pointer"
        >
          &larr; Back
        </button>
        <div className="flex-1">
          <div className="text-[10px] uppercase tracking-wider text-amber-400/70 font-medium">Consumer Group</div>
          <h1 className="text-xl font-bold text-white font-mono">{groupId}</h1>
        </div>
        {selectedConsumerGroup && (
          <div className="flex items-center gap-3">
            <span className={`text-[10px] font-semibold uppercase px-2.5 py-1 rounded-lg border ${
              selectedConsumerGroup.state === "Stable" ? "bg-emerald-500/15 text-emerald-400 border-emerald-500/25" : "bg-slate-800/50 text-slate-400 border-slate-700/40"
            }`}>
              {selectedConsumerGroup.state}
            </span>
            <button
              onClick={() => setShowReset(true)}
              className="px-3.5 py-2 rounded-xl text-xs font-medium bg-amber-500/15 border border-amber-500/30 text-amber-300 hover:bg-amber-500/25 transition-all cursor-pointer"
            >
              Reset Offsets
            </button>
          </div>
        )}
      </div>

      {consumerGroupDetailLoading && !selectedConsumerGroup ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-amber-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : selectedConsumerGroup && (
        <>
          {/* Tabs */}
          <div className="flex gap-1 bg-slate-900/50 rounded-xl p-1 w-fit border border-slate-800/50">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
                  activeTab === tab.id
                    ? "bg-amber-500/20 text-amber-300 border border-amber-500/30"
                    : "text-slate-400 hover:text-slate-300 border border-transparent"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {activeTab === "members" && (
            <DataTable
              columns={memberColumns}
              data={selectedConsumerGroup.members as unknown as Record<string, unknown>[]}
              searchPlaceholder="Filter members..."
              searchKeys={["clientId", "clientHost"]}
              emptyMessage="No active members"
            />
          )}

          {activeTab === "offsets" && (
            <DataTable
              columns={offsetColumns}
              data={selectedConsumerGroup.offsets as unknown as Record<string, unknown>[]}
              searchPlaceholder="Filter by topic..."
              searchKeys={["topic"]}
              emptyMessage="No offset data"
            />
          )}
        </>
      )}

      {/* Reset Offsets Modal */}
      <Modal title="Reset Consumer Group Offsets" open={showReset} onClose={() => { setShowReset(false); setResetResult(null); }}>
        <div className="space-y-4">
          {resetResult && (
            <div className={`p-3 rounded-xl border text-sm ${
              resetResult.success
                ? "bg-emerald-950/50 border-emerald-500/30 text-emerald-300"
                : "bg-red-950/50 border-red-500/30 text-red-300"
            }`}>
              {resetResult.message}
            </div>
          )}
          <p className="text-sm text-slate-300">
            Reset offsets for <span className="font-mono text-amber-300 font-medium">{groupId}</span>. The consumer group must be inactive (no running consumers).
          </p>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Strategy</label>
            <div className="flex gap-2 mt-2">
              {["earliest", "latest"].map((s) => (
                <button
                  key={s}
                  onClick={() => setResetStrategy(s)}
                  className={`px-4 py-2 rounded-xl text-xs font-medium transition-all cursor-pointer ${
                    resetStrategy === s
                      ? "bg-amber-500/20 border border-amber-500/30 text-amber-300"
                      : "bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                  }`}
                >
                  {s.charAt(0).toUpperCase() + s.slice(1)}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Topic (optional - all if empty)</label>
            <select
              value={resetTopic}
              onChange={(e) => setResetTopic(e.target.value)}
              className="w-full mt-1.5 bg-slate-800/80 rounded-xl px-3.5 py-2.5 border border-slate-700/50 text-sm text-white focus:outline-none focus:border-amber-500/50"
            >
              <option value="">All topics</option>
              {topicsList.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => { setShowReset(false); setResetResult(null); }} className="px-4 py-2 rounded-xl text-sm font-medium bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 transition-colors cursor-pointer">Cancel</button>
            <button onClick={handleReset} disabled={resetting} className="px-4 py-2 rounded-xl text-sm font-medium bg-amber-500/20 border border-amber-500/30 text-amber-300 hover:bg-amber-500/25 transition-colors cursor-pointer disabled:opacity-40">
              {resetting ? "Resetting..." : "Reset Offsets"}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
