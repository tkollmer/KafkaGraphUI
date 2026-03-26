import { useEffect, useState, useCallback, useRef, useMemo } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { useThemeStore } from "../store/themeStore";
import { useToastStore } from "../store/toastStore";
import { DataTable } from "../components/DataTable";
import { Modal } from "../components/Modal";
import { MessageInspector } from "../panels/MessageInspector";
import { apiFetch } from "../hooks/useApi";

interface Props {
  topicName: string;
  onBack: () => void;
}

type Tab = "partitions" | "config" | "messages" | "produce" | "reassign" | "config-diff" | "keys" | "search" | "replay" | "consumers" | "timeline" | "capacity";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function fmtBytes(b: number): string {
  if (b >= 1_073_741_824) return `${(b / 1_073_741_824).toFixed(1)} GB`;
  if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)} MB`;
  if (b >= 1_024) return `${(b / 1_024).toFixed(1)} KB`;
  return `${b} B`;
}

function formatRetention(ms: string | undefined): string {
  if (!ms || ms === "-1") return "Forever";
  const n = parseInt(ms, 10);
  if (isNaN(n) || n < 0) return "Forever";
  if (n < 60000) return `${(n / 1000).toFixed(0)}s`;
  if (n < 3600000) return `${(n / 60000).toFixed(0)}m`;
  if (n < 86400000) return `${(n / 3600000).toFixed(1)}h`;
  return `${(n / 86400000).toFixed(1)}d`;
}

const configDescriptions: Record<string, string> = {
  "retention.ms": "How long messages are retained (-1 for forever)",
  "retention.bytes": "Max size per partition before old messages are deleted",
  "cleanup.policy": "delete: remove old segments, compact: keep latest per key",
  "max.message.bytes": "Maximum size of a single message batch",
  "segment.bytes": "Size of a single log segment file",
  "segment.ms": "Time before a new log segment is rolled",
  "min.insync.replicas": "Minimum ISR count for acks=all writes to succeed",
  "compression.type": "Compression codec: none, gzip, snappy, lz4, zstd, producer",
  "message.timestamp.type": "CreateTime or LogAppendTime",
  "flush.messages": "Messages between fsync calls (0 = rely on OS)",
  "flush.ms": "Time between fsync calls",
  "index.interval.bytes": "Interval between index entries",
  "min.cleanable.dirty.ratio": "Min ratio of dirty log to total for compaction",
  "delete.retention.ms": "How long tombstones are retained during compaction",
  "file.delete.delay.ms": "Delay before deleting a file from filesystem",
  "max.compaction.lag.ms": "Max time a message can remain uncompacted",
  "message.downconversion.enable": "Allow down-conversion for older consumers",
  "unclean.leader.election.enable": "Allow out-of-ISR replicas to become leader",
};

export function TopicDetail({ topicName, onBack }: Props) {
  const { selectedTopic, topicDetailLoading, fetchTopicDetail, produceMessage, updateTopicConfig, addTopicPartitions, consumerGroups, fetchConsumerGroups } = useKafkaStore();
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const [activeTab, setActiveTab] = useState<Tab>("partitions");
  const [produceForm, setProduceForm] = useState({ key: "", value: "", headers: "" });
  const [produceResult, setProduceResult] = useState<{ success: boolean; message: string } | null>(null);
  const [producing, setProducing] = useState(false);
  const [batchCount, setBatchCount] = useState(1);
  const [batchProgress, setBatchProgress] = useState<{ sent: number; failed: number; total: number } | null>(null);

  // Config editing
  const [editingConfig, setEditingConfig] = useState<{ key: string; value: string } | null>(null);
  const [configSaving, setConfigSaving] = useState(false);
  const [configResult, setConfigResult] = useState<{ success: boolean; message: string } | null>(null);

  // Partition increase
  const [showAddPartitions, setShowAddPartitions] = useState(false);
  const [newPartitionCount, setNewPartitionCount] = useState("");
  const [partitionResult, setPartitionResult] = useState<{ success: boolean; message: string } | null>(null);

  // Auto-refresh
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  useEffect(() => { fetchTopicDetail(topicName); }, [topicName, fetchTopicDetail]);
  useEffect(() => { fetchConsumerGroups(); }, [fetchConsumerGroups]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(() => fetchTopicDetail(topicName), 5000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, topicName, fetchTopicDetail]);

  // Key cardinality analysis from sampled messages
  const [keyCardinality, setKeyCardinality] = useState<{
    uniqueKeys: number; nullKeys: number; total: number;
    topKeys: [string, number][]; cardinalityRatio: number;
  } | null>(null);

  useEffect(() => {
    fetch(`/api/topics/${encodeURIComponent(topicName)}/messages`)
      .then((r) => r.ok ? r.json() : null)
      .then((data) => {
        if (!data?.messages?.length) return;
        const msgs = data.messages as { key: string | null }[];
        const keyCounts = new Map<string, number>();
        let nullKeys = 0;
        for (const m of msgs) {
          if (m.key === null || m.key === undefined) nullKeys++;
          else keyCounts.set(m.key, (keyCounts.get(m.key) || 0) + 1);
        }
        const topKeys = [...keyCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5) as [string, number][];
        setKeyCardinality({
          uniqueKeys: keyCounts.size,
          nullKeys,
          total: msgs.length,
          topKeys,
          cardinalityRatio: keyCounts.size / Math.max(1, msgs.length - nullKeys),
        });
      })
      .catch(() => {});
  }, [topicName]);

  const handleProduce = useCallback(async () => {
    setProducing(true);
    setProduceResult(null);
    setBatchProgress(null);
    let headers: Record<string, string> | undefined;
    if (produceForm.headers.trim()) {
      try {
        headers = JSON.parse(produceForm.headers);
      } catch {
        setProduceResult({ success: false, message: "Invalid headers JSON" });
        setProducing(false);
        return;
      }
    }

    if (batchCount <= 1) {
      const result = await produceMessage(topicName, produceForm.value, produceForm.key || undefined, headers);
      if (result.success) {
        setProduceResult({ success: true, message: `Sent to partition ${result.partition} at offset ${result.offset}` });
        setProduceForm({ key: "", value: "", headers: "" });
      } else {
        setProduceResult({ success: false, message: result.error || "Failed" });
      }
    } else {
      let sent = 0;
      let failed = 0;
      for (let i = 0; i < batchCount; i++) {
        const key = produceForm.key ? `${produceForm.key}-${i}` : undefined;
        const result = await produceMessage(topicName, produceForm.value, key, headers);
        if (result.success) sent++;
        else failed++;
        setBatchProgress({ sent, failed, total: batchCount });
      }
      setProduceResult({
        success: failed === 0,
        message: `Batch complete: ${sent} sent, ${failed} failed`,
      });
      setBatchProgress(null);
      if (failed === 0) setBatchCount(1);
    }
    setProducing(false);
  }, [topicName, produceForm, produceMessage, batchCount]);

  const handleConfigSave = useCallback(async () => {
    if (!editingConfig) return;
    setConfigSaving(true);
    setConfigResult(null);
    const result = await updateTopicConfig(topicName, { [editingConfig.key]: editingConfig.value });
    if (result.success) {
      setConfigResult({ success: true, message: `Updated ${editingConfig.key}` });
      setEditingConfig(null);
      fetchTopicDetail(topicName);
    } else {
      setConfigResult({ success: false, message: result.error || "Failed to update config" });
    }
    setConfigSaving(false);
  }, [editingConfig, topicName, updateTopicConfig, fetchTopicDetail]);

  const handleAddPartitions = useCallback(async () => {
    const total = parseInt(newPartitionCount, 10);
    if (!total || total < 1) return;
    setPartitionResult(null);
    const result = await addTopicPartitions(topicName, total);
    if (result.success) {
      setPartitionResult({ success: true, message: `Partitions increased to ${total}` });
      setShowAddPartitions(false);
      setNewPartitionCount("");
      fetchTopicDetail(topicName);
    } else {
      setPartitionResult({ success: false, message: result.error || "Failed" });
    }
  }, [newPartitionCount, topicName, addTopicPartitions, fetchTopicDetail]);

  const tabs: { id: Tab; label: string }[] = [
    { id: "partitions", label: "Partitions" },
    { id: "config", label: "Config" },
    { id: "config-diff", label: "Config Diff" },
    { id: "messages", label: "Messages" },
    { id: "produce", label: "Produce" },
    { id: "search", label: "Search" },
    { id: "replay", label: "Replay" },
    { id: "consumers", label: "Consumers" },
    { id: "reassign", label: "Reassign" },
    { id: "keys", label: "Key Analysis" },
    { id: "timeline", label: "Timeline" },
    { id: "capacity", label: "Capacity" },
  ];

  // Compute summary stats
  const totalMessages = selectedTopic?.partitions.reduce((s, p) => s + p.endOffset, 0) || 0;
  const partitionCount = selectedTopic?.partitions.length || 0;
  const underReplicatedCount = selectedTopic?.partitions.filter(p => p.isr.length < p.replicas.length).length || 0;

  // Leader skew detection
  const leaderSkew = useMemo(() => {
    if (!selectedTopic || selectedTopic.partitions.length < 2) return null;
    const leaderCounts = new Map<number, number>();
    for (const p of selectedTopic.partitions) {
      const leader = p.leader;
      leaderCounts.set(leader, (leaderCounts.get(leader) || 0) + 1);
    }
    if (leaderCounts.size < 2) return null;
    const counts = [...leaderCounts.values()];
    const maxCount = Math.max(...counts);
    const minCount = Math.min(...counts);
    const skewRatio = maxCount / Math.max(minCount, 1);
    return skewRatio > 2 ? { maxCount, minCount, brokers: leaderCounts.size, skewRatio } : null;
  }, [selectedTopic]);

  const partitionColumns = [
    { key: "partition", label: "Partition", className: "w-24 text-center" },
    { key: "leader", label: "Leader", className: "w-24 text-center" },
    { key: "replicas", label: "Replicas", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-xs">{(r.replicas as number[])?.join(", ") || "-"}</span>
    )},
    { key: "isr", label: "ISR", render: (r: Record<string, unknown>) => {
      const isr = r.isr as number[];
      const replicas = r.replicas as number[];
      const isUnderReplicated = isr && replicas && isr.length < replicas.length;
      return (
        <span className={`font-mono text-xs ${isUnderReplicated ? "text-red-500 font-bold" : ""}`}>
          {isr?.join(", ") || "-"}
          {isUnderReplicated && <span className="ml-1.5 text-[9px] uppercase font-bold text-red-400">Under-replicated</span>}
        </span>
      );
    }},
    { key: "endOffset", label: "End Offset", className: "w-32 text-right", render: (r: Record<string, unknown>) => (
      <span className="font-mono tabular-nums">{Number(r.endOffset).toLocaleString()}</span>
    )},
    { key: "_estSize", label: "Est. Size", className: "w-28 text-right", render: (r: Record<string, unknown>) => {
      const offset = Number(r.endOffset || 0);
      const bytes = offset * 1024; // rough estimate: ~1KB per message
      return <span className={`font-mono tabular-nums text-xs ${isBright ? "text-slate-500" : "text-slate-400"}`}>{fmtBytes(bytes)}</span>;
    }},
  ];

  const inputCls = `w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none focus:border-indigo-500/50 ${
    isBright ? "bg-slate-50 border-slate-200 text-slate-800 placeholder-slate-400" : "bg-slate-800/80 border-slate-700/50 text-white placeholder-slate-500"
  }`;

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      {/* Breadcrumb + Header */}
      <div>
        <nav className="flex items-center gap-1.5 mb-2">
          <button onClick={onBack} className={`text-[11px] font-medium transition-colors cursor-pointer hover:underline ${isBright ? "text-indigo-600" : "text-indigo-400"}`}>
            Topics
          </button>
          <span className={`text-[11px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>/</span>
          <span className={`text-[11px] font-mono font-medium truncate max-w-[300px] ${isBright ? "text-slate-600" : "text-slate-300"}`}>{topicName}</span>
        </nav>
      </div>
      <div className="flex items-center gap-4">
        <div className="flex-1">
          <h1 className={`text-2xl font-bold font-mono ${isBright ? "text-slate-800" : "text-white"}`}>{topicName}</h1>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`px-3 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              autoRefresh
                ? isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300"
                : isBright ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
            }`}
            title={autoRefresh ? "Stop auto-refresh (5s)" : "Auto-refresh every 5s"}
          >
            {autoRefresh ? "Auto (5s)" : "Auto"}
          </button>
          <button
            onClick={() => fetchTopicDetail(topicName)}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
            }`}
          >
            Refresh
          </button>
        </div>
      </div>

      {topicDetailLoading && !selectedTopic ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <>
          {/* Summary cards */}
          {selectedTopic && (
            <div className="grid grid-cols-5 gap-3">
              <SummaryCard label="Partitions" value={String(partitionCount)} color="indigo" bright={isBright} />
              <SummaryCard label="End Offsets" value={fmt(totalMessages)} color="emerald" bright={isBright} />
              <SummaryCard label="Replication" value={selectedTopic.partitions[0] ? String(selectedTopic.partitions[0].replicas.length) : "-"} color="slate" bright={isBright} />
              <SummaryCard label="Retention" value={formatRetention(selectedTopic.config["retention.ms"])} color="slate" bright={isBright} />
              <SummaryCard
                label="Under-replicated"
                value={String(underReplicatedCount)}
                color={underReplicatedCount > 0 ? "red" : "emerald"}
                bright={isBright}
              />
            </div>
          )}

          {/* Leader skew warning */}
          {leaderSkew && (
            <div className={`flex items-center gap-3 px-4 py-2.5 rounded-xl border ${
              isBright ? "bg-amber-50 border-amber-200/60" : "bg-amber-950/30 border-amber-500/20"
            }`}>
              <span className={`text-[10px] font-bold uppercase ${isBright ? "text-amber-600" : "text-amber-400"}`}>Leader Skew</span>
              <span className={`text-xs ${isBright ? "text-amber-700" : "text-amber-300"}`}>
                Partition leaders are unevenly distributed across {leaderSkew.brokers} brokers (max {leaderSkew.maxCount}, min {leaderSkew.minCount}, ratio {leaderSkew.skewRatio.toFixed(1)}x). Consider running preferred leader election.
              </span>
            </div>
          )}

          {/* Tabs */}
          <div className={`flex gap-1 rounded-xl p-1 w-fit border ${isBright ? "bg-slate-100/50 border-slate-200/50" : "bg-slate-900/50 border-slate-800/50"}`}>
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
                  activeTab === tab.id
                    ? isBright
                      ? "bg-white text-indigo-700 border border-indigo-200/60 shadow-sm"
                      : "bg-indigo-500/20 text-indigo-300 border border-indigo-500/30"
                    : isBright
                      ? "text-slate-500 hover:text-slate-700 border border-transparent"
                      : "text-slate-400 hover:text-slate-300 border border-transparent"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {activeTab === "partitions" && selectedTopic && (
            <div className="space-y-4">
              {/* Partition offset distribution */}
              {partitionCount > 0 && (
                <OffsetDistribution partitions={selectedTopic.partitions} bright={isBright} />
              )}
              {/* Broker partition distribution */}
              {partitionCount > 0 && (
                <BrokerDistribution partitions={selectedTopic.partitions} bright={isBright} />
              )}
              {/* Replica assignment grid */}
              {partitionCount > 0 && partitionCount <= 50 && (
                <ReplicaGrid partitions={selectedTopic.partitions} bright={isBright} />
              )}

              {/* Key cardinality */}
              {keyCardinality && keyCardinality.total > 0 && (
                <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
                  <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                    Key Cardinality (sampled)
                  </div>
                  <div className="grid grid-cols-4 gap-3 mb-3">
                    <div className={`rounded-xl px-3 py-2.5 border text-center ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Unique Keys</div>
                      <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{keyCardinality.uniqueKeys}</div>
                    </div>
                    <div className={`rounded-xl px-3 py-2.5 border text-center ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Null Keys</div>
                      <div className={`text-lg font-bold tabular-nums ${keyCardinality.nullKeys > 0 ? "text-amber-500" : isBright ? "text-slate-800" : "text-white"}`}>{keyCardinality.nullKeys}</div>
                    </div>
                    <div className={`rounded-xl px-3 py-2.5 border text-center ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Sampled</div>
                      <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{keyCardinality.total}</div>
                    </div>
                    <div className={`rounded-xl px-3 py-2.5 border text-center ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Cardinality</div>
                      <div className={`text-lg font-bold tabular-nums ${
                        keyCardinality.cardinalityRatio > 0.9 ? "text-emerald-500" : keyCardinality.cardinalityRatio > 0.5 ? "text-amber-500" : "text-red-500"
                      }`}>{(keyCardinality.cardinalityRatio * 100).toFixed(0)}%</div>
                    </div>
                  </div>
                  {keyCardinality.topKeys.length > 0 && (
                    <div className="space-y-1">
                      <div className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Top Keys</div>
                      {keyCardinality.topKeys.map(([key, count]) => {
                        const pct = (count / keyCardinality.total) * 100;
                        const isHotKey = pct > 20;
                        return (
                          <div key={key} className="flex items-center gap-2">
                            <span className={`text-[10px] font-mono truncate w-32 shrink-0 ${isBright ? "text-slate-600" : "text-slate-300"}`}>{key}</span>
                            <div className={`flex-1 h-2 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                              <div className={`h-full rounded-full ${isHotKey ? "bg-red-500" : isBright ? "bg-indigo-400" : "bg-indigo-500/70"}`} style={{ width: `${Math.max(3, pct)}%` }} />
                            </div>
                            <span className={`text-[10px] font-mono font-bold w-16 text-right ${isHotKey ? "text-red-500" : isBright ? "text-slate-500" : "text-slate-400"}`}>
                              {count} ({pct.toFixed(0)}%){isHotKey ? " HOT" : ""}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}

              <div className="flex items-center justify-between">
                <div className={`text-sm font-medium ${isBright ? "text-slate-600" : "text-slate-400"}`}>
                  {partitionCount} partitions
                </div>
                <button
                  onClick={() => { setShowAddPartitions(true); setNewPartitionCount(String(partitionCount + 1)); }}
                  className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                    isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                  }`}
                >
                  + Add Partitions
                </button>
                <button
                  onClick={async () => {
                    try {
                      await apiFetch("/api/cluster/elect-leaders", { method: "POST", body: JSON.stringify({ topic: topicName }) });
                      useToastStore.getState().addToast("Preferred leader election triggered", "success");
                    } catch (e) {
                      useToastStore.getState().addToast(String(e), "error");
                    }
                  }}
                  className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                    isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
                  }`}
                >
                  Elect Leaders
                </button>
              </div>
              <DataTable
                columns={partitionColumns}
                data={selectedTopic.partitions as unknown as Record<string, unknown>[]}
                searchPlaceholder="Filter partitions..."
                emptyMessage="No partition data"
              />

              {/* Recent messages preview */}
              <RecentMessages topic={topicName} bright={isBright} />

              {/* Consumed By */}
              {(() => {
                const consuming = consumerGroups.filter(g => g.topics.includes(topicName));
                if (consuming.length === 0) return null;
                return (
                  <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
                    <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                      Consumed By
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {consuming.map(g => (
                        <span
                          key={g.groupId}
                          className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium cursor-default ${
                            isBright
                              ? "bg-indigo-50 border border-indigo-200/60 text-indigo-700"
                              : "bg-indigo-500/15 border border-indigo-500/30 text-indigo-300"
                          }`}
                          title={`Status: ${g.status} | Members: ${g.members} | Lag: ${g.totalLag}`}
                        >
                          <span className={`w-1.5 h-1.5 rounded-full ${
                            g.status === "Stable" ? "bg-emerald-400" : g.status === "Empty" ? "bg-amber-400" : "bg-red-400"
                          }`} />
                          {g.groupId}
                        </span>
                      ))}
                    </div>
                  </div>
                );
              })()}
            </div>
          )}

          {activeTab === "config" && selectedTopic && (
            <div className="space-y-4">
              {configResult && (
                <div className={`p-3 rounded-xl border text-sm ${
                  configResult.success
                    ? isBright ? "bg-emerald-50 border-emerald-200 text-emerald-700" : "bg-emerald-950/50 border-emerald-500/30 text-emerald-300"
                    : isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
                }`}>
                  {configResult.message}
                </div>
              )}

              {/* Quick-set presets */}
              <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
                <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                  Quick Actions
                </div>
                <div className="flex gap-2 flex-wrap">
                  {([
                    { label: "Retention 1h", config: { "retention.ms": "3600000" } },
                    { label: "Retention 1d", config: { "retention.ms": "86400000" } },
                    { label: "Retention 7d", config: { "retention.ms": "604800000" } },
                    { label: "Retention \u221E", config: { "retention.ms": "-1" } },
                    { label: "Compact", config: { "cleanup.policy": "compact" } },
                    { label: "Delete", config: { "cleanup.policy": "delete" } },
                    { label: "Compact+Delete", config: { "cleanup.policy": "compact,delete" } },
                  ] as { label: string; config: Record<string, string> }[]).map((preset) => {
                    const [key, val] = Object.entries(preset.config)[0];
                    const isActive = selectedTopic.config[key] === val;
                    return (
                      <button
                        key={preset.label}
                        onClick={async () => {
                          setConfigSaving(true);
                          setConfigResult(null);
                          const result = await updateTopicConfig(topicName, preset.config);
                          if (result.success) {
                            setConfigResult({ success: true, message: `Applied: ${preset.label}` });
                            fetchTopicDetail(topicName);
                          } else {
                            setConfigResult({ success: false, message: result.error || "Failed" });
                          }
                          setConfigSaving(false);
                        }}
                        disabled={configSaving || isActive}
                        className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-all cursor-pointer disabled:opacity-40 ${
                          isActive
                            ? isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700" : "bg-emerald-500/20 border-emerald-500/30 text-emerald-300"
                            : isBright ? "bg-white border-slate-200/60 text-slate-600 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                        }`}
                      >
                        {isActive ? "\u2713 " : ""}{preset.label}
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Config table with descriptions */}
              <div className={`rounded-2xl border overflow-hidden ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
                <table className="w-full">
                  <thead>
                    <tr className={`border-b ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
                      <th className={`px-5 py-3.5 text-left text-[11px] uppercase tracking-wider font-semibold ${isBright ? "text-slate-500" : "text-slate-400"}`}>Key</th>
                      <th className={`px-5 py-3.5 text-left text-[11px] uppercase tracking-wider font-semibold ${isBright ? "text-slate-500" : "text-slate-400"}`}>Value</th>
                      <th className={`px-5 py-3.5 text-right text-[11px] uppercase tracking-wider font-semibold w-24 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(selectedTopic.config).map(([k, v], i) => {
                      const desc = configDescriptions[k];
                      return (
                        <tr key={k} className={`border-b last:border-0 ${isBright ? "border-slate-100" : "border-slate-800/30"} ${i % 2 === 1 ? (isBright ? "bg-slate-50/50" : "bg-slate-800/[0.08]") : ""}`}>
                          <td className={`px-5 py-3 ${isBright ? "text-slate-700" : "text-slate-300"}`}>
                            <div className="text-sm font-mono">{k}</div>
                            {desc && <div className={`text-[10px] mt-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>{desc}</div>}
                          </td>
                          <td className={`px-5 py-3 text-sm font-mono ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>
                            {editingConfig?.key === k ? (
                              <input
                                type="text"
                                value={editingConfig.value}
                                onChange={(e) => setEditingConfig({ key: k, value: e.target.value })}
                                className={`rounded-lg px-2 py-1 border text-sm font-mono w-full ${
                                  isBright ? "bg-white border-indigo-200 text-slate-800" : "bg-slate-800 border-indigo-500/40 text-white"
                                }`}
                                autoFocus
                                onKeyDown={(e) => {
                                  if (e.key === "Enter") handleConfigSave();
                                  if (e.key === "Escape") setEditingConfig(null);
                                }}
                              />
                            ) : (
                              <span title={v}>{v.length > 60 ? v.slice(0, 57) + "..." : v}</span>
                            )}
                          </td>
                          <td className="px-5 py-3 text-right">
                            {editingConfig?.key === k ? (
                              <div className="flex gap-1 justify-end">
                                <button
                                  onClick={handleConfigSave}
                                  disabled={configSaving}
                                  className={`px-2 py-1 rounded-lg text-[10px] font-medium cursor-pointer ${
                                    isBright ? "bg-emerald-50 text-emerald-700 hover:bg-emerald-100" : "bg-emerald-500/20 text-emerald-300 hover:bg-emerald-500/30"
                                  }`}
                                >
                                  {configSaving ? "..." : "Save"}
                                </button>
                                <button
                                  onClick={() => setEditingConfig(null)}
                                  className={`px-2 py-1 rounded-lg text-[10px] font-medium cursor-pointer ${
                                    isBright ? "text-slate-500 hover:bg-slate-100" : "text-slate-400 hover:bg-slate-800"
                                  }`}
                                >
                                  Cancel
                                </button>
                              </div>
                            ) : (
                              <button
                                onClick={() => setEditingConfig({ key: k, value: v })}
                                className={`text-[10px] font-medium cursor-pointer ${
                                  isBright ? "text-indigo-500 hover:text-indigo-700" : "text-indigo-400/60 hover:text-indigo-300"
                                }`}
                              >
                                Edit
                              </button>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {activeTab === "messages" && (
            <div className={`rounded-2xl border overflow-hidden ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
              <MessageInspector topic={topicName} onClose={() => setActiveTab("partitions")} embedded />
            </div>
          )}

          {activeTab === "produce" && (
            <div className={`max-w-xl rounded-2xl border p-6 space-y-5 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
              <div>
                <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"} mb-1`}>Send a message</h3>
                <p className={`text-xs ${isBright ? "text-slate-500" : "text-slate-500"}`}>Produce a message to <span className={`font-mono ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{topicName}</span></p>
              </div>

              {/* Template presets */}
              <div>
                <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Templates</label>
                <div className="flex gap-2 mt-1.5 flex-wrap">
                  {[
                    { label: "JSON Event", key: "event-key", value: '{\n  "event": "user.action",\n  "userId": "abc123",\n  "timestamp": "' + new Date().toISOString() + '",\n  "data": {}\n}', headers: '{"content-type": "application/json"}' },
                    { label: "Plain Text", key: "", value: "Hello, Kafka!", headers: "" },
                    { label: "Avro-like", key: "record-1", value: '{\n  "schema": "com.example.Event",\n  "payload": {\n    "id": 1,\n    "name": "test",\n    "active": true\n  }\n}', headers: '{"content-type": "application/avro+json"}' },
                    { label: "Tombstone", key: "delete-key", value: "", headers: "" },
                  ].map((t) => (
                    <button
                      key={t.label}
                      onClick={() => setProduceForm({ key: t.key, value: t.value, headers: t.headers })}
                      className={`px-2.5 py-1.5 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
                        isBright
                          ? "bg-slate-50 border-slate-200/60 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
                          : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
                      }`}
                    >
                      {t.label}
                    </button>
                  ))}
                </div>
              </div>

              {produceResult && (
                <div className={`p-3 rounded-xl border text-sm ${
                  produceResult.success
                    ? isBright ? "bg-emerald-50 border-emerald-200 text-emerald-700" : "bg-emerald-950/50 border-emerald-500/30 text-emerald-300"
                    : isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
                }`}>
                  {produceResult.message}
                </div>
              )}

              <div>
                <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Key (optional)</label>
                <input type="text" value={produceForm.key} onChange={(e) => setProduceForm({ ...produceForm, key: e.target.value })} className={inputCls} placeholder="message-key" />
              </div>
              <div>
                <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Value</label>
                <textarea value={produceForm.value} onChange={(e) => setProduceForm({ ...produceForm, value: e.target.value })} className={`${inputCls} font-mono min-h-[120px] resize-y`} placeholder='{"event": "test"}' />
              </div>
              <div>
                <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Headers (JSON, optional)</label>
                <input type="text" value={produceForm.headers} onChange={(e) => setProduceForm({ ...produceForm, headers: e.target.value })} className={`${inputCls} font-mono`} placeholder='{"content-type": "application/json"}' />
              </div>
              {/* Batch count selector */}
              <div>
                <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Batch Count</label>
                <div className="flex items-center gap-2 mt-1.5">
                  {[1, 10, 100, 1000].map((n) => (
                    <button
                      key={n}
                      onClick={() => setBatchCount(n)}
                      className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-all cursor-pointer ${
                        batchCount === n
                          ? isBright
                            ? "bg-indigo-50 border-indigo-300/60 text-indigo-700 shadow-sm"
                            : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300"
                          : isBright
                            ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                            : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                      }`}
                    >
                      {n === 1 ? "Single" : `×${n}`}
                    </button>
                  ))}
                  <input
                    type="number"
                    min={1}
                    max={10000}
                    value={batchCount}
                    onChange={(e) => setBatchCount(Math.max(1, Math.min(10000, parseInt(e.target.value) || 1)))}
                    className={`w-20 px-2 py-1.5 rounded-lg text-xs font-mono border focus:outline-none focus:border-indigo-500/50 ${
                      isBright ? "bg-slate-50 border-slate-200 text-slate-700" : "bg-slate-800/60 border-slate-700/40 text-slate-300"
                    }`}
                  />
                  {batchCount > 1 && (
                    <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                      Keys will be suffixed with -0, -1, ...
                    </span>
                  )}
                </div>
              </div>

              {/* Batch progress */}
              {batchProgress && (
                <div className="space-y-2">
                  <div className="flex items-center justify-between">
                    <span className={`text-xs font-medium ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                      Sending {batchProgress.sent + batchProgress.failed} / {batchProgress.total}
                    </span>
                    <span className={`text-[10px] font-mono ${batchProgress.failed > 0 ? "text-red-400" : isBright ? "text-emerald-600" : "text-emerald-400"}`}>
                      {batchProgress.sent} ok{batchProgress.failed > 0 ? `, ${batchProgress.failed} failed` : ""}
                    </span>
                  </div>
                  <div className={`w-full h-2 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                    <div className="h-full flex">
                      <div
                        className="h-full bg-emerald-500 transition-all duration-150"
                        style={{ width: `${(batchProgress.sent / batchProgress.total) * 100}%` }}
                      />
                      {batchProgress.failed > 0 && (
                        <div
                          className="h-full bg-red-500 transition-all duration-150"
                          style={{ width: `${(batchProgress.failed / batchProgress.total) * 100}%` }}
                        />
                      )}
                    </div>
                  </div>
                </div>
              )}

              <div className="flex gap-2">
                <button
                  onClick={() => {
                    try {
                      const parsed = JSON.parse(produceForm.value);
                      setProduceForm({ ...produceForm, value: JSON.stringify(parsed, null, 2) });
                      setProduceResult(null);
                    } catch {
                      setProduceResult({ success: false, message: "Invalid JSON -- cannot format" });
                    }
                  }}
                  disabled={!produceForm.value.trim()}
                  className={`px-4 py-2.5 rounded-xl text-sm font-medium border transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                    isBright
                      ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50"
                      : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
                  }`}
                >
                  Format JSON
                </button>
                <button
                  onClick={handleProduce}
                  disabled={producing || !produceForm.value.trim()}
                  className={`px-5 py-2.5 rounded-xl text-sm font-medium border transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                    isBright
                      ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                      : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                  }`}
                >
                  {producing ? "Sending..." : batchCount > 1 ? `Send ${batchCount} Messages` : "Send Message"}
                </button>
              </div>
            </div>
          )}

          {activeTab === "reassign" && selectedTopic && (
            <ReassignPanel topicName={topicName} partitions={selectedTopic.partitions} isBright={isBright} />
          )}

          {activeTab === "search" && (
            <MessageSearchPanel topicName={topicName} isBright={isBright} />
          )}

          {activeTab === "replay" && (
            <ReplayPanel topicName={topicName} isBright={isBright} />
          )}

          {activeTab === "config-diff" && selectedTopic && (
            <ConfigDiffPanel topicName={topicName} isBright={isBright} />
          )}

          {activeTab === "consumers" && (
            <TopicConsumersPanel topicName={topicName} isBright={isBright} />
          )}

          {activeTab === "keys" && (
            <KeyDistributionPanel topicName={topicName} isBright={isBright} />
          )}
          {activeTab === "timeline" && selectedTopic && (
            <PartitionTimelinePanel partitions={selectedTopic.partitions} isBright={isBright} topicName={topicName} />
          )}
          {activeTab === "capacity" && selectedTopic && (
            <CapacityPlanningPanel partitions={selectedTopic.partitions} config={selectedTopic.config} isBright={isBright} />
          )}
        </>
      )}

      {/* Add Partitions Modal */}
      <Modal title="Add Partitions" open={showAddPartitions} onClose={() => { setShowAddPartitions(false); setPartitionResult(null); }}>
        <div className="space-y-4">
          {partitionResult && (
            <div className={`p-3 rounded-xl border text-sm ${
              partitionResult.success
                ? isBright ? "bg-emerald-50 border-emerald-200 text-emerald-700" : "bg-emerald-950/50 border-emerald-500/30 text-emerald-300"
                : isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
            }`}>
              {partitionResult.message}
            </div>
          )}
          <p className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>
            Increase the number of partitions for <span className={`font-mono font-medium ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{topicName}</span>.
            Current: <span className="font-bold">{partitionCount}</span> partitions. This cannot be undone.
          </p>
          <div>
            <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Total Partitions</label>
            <input
              type="number"
              value={newPartitionCount}
              onChange={(e) => setNewPartitionCount(e.target.value)}
              className={inputCls}
              min={partitionCount + 1}
            />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => { setShowAddPartitions(false); setPartitionResult(null); }} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}>Cancel</button>
            <button
              onClick={handleAddPartitions}
              disabled={!newPartitionCount || parseInt(newPartitionCount) <= partitionCount}
              className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
              }`}
            >
              Increase Partitions
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
}

function RecentMessages({ topic, bright }: { topic: string; bright: boolean }) {
  const [messages, setMessages] = useState<{ offset: number; partition: number; timestamp: number; key: string | null; value: unknown }[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`/api/topics/${encodeURIComponent(topic)}/messages`)
      .then((r) => { if (!r.ok) throw new Error(); return r.json(); })
      .then((data) => setMessages((data as { offset: number; partition: number; timestamp: number; key: string | null; value: unknown }[]).slice(0, 5)))
      .catch(() => setMessages([]))
      .finally(() => setLoading(false));
  }, [topic]);

  if (loading) return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className={`text-[11px] uppercase tracking-wider font-medium mb-2 ${bright ? "text-slate-500" : "text-slate-400"}`}>Recent Messages</div>
      <div className="flex items-center justify-center py-4">
        <div className="w-4 h-4 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
      </div>
    </div>
  );

  if (messages.length === 0) return null;

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${bright ? "text-slate-500" : "text-slate-400"}`}>
        Recent Messages (Latest {messages.length})
      </div>
      <div className="space-y-2">
        {messages.map((msg, i) => {
          const valStr = typeof msg.value === "string" ? msg.value : JSON.stringify(msg.value);
          const ts = new Date(msg.timestamp).toLocaleTimeString();
          return (
            <div key={i} className={`rounded-xl border px-3 py-2 ${bright ? "border-slate-100 bg-slate-50/50" : "border-slate-800/40 bg-slate-800/20"}`}>
              <div className="flex items-center gap-3 mb-1">
                <span className={`text-[10px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>P{msg.partition}:O{msg.offset}</span>
                <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>{ts}</span>
                {msg.key && <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${bright ? "bg-indigo-50 text-indigo-600" : "bg-indigo-500/10 text-indigo-300"}`}>{msg.key}</span>}
              </div>
              <div className={`text-xs font-mono truncate ${bright ? "text-slate-600" : "text-slate-300"}`} title={valStr}>
                {valStr.length > 200 ? valStr.slice(0, 200) + "..." : valStr}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ReplicaGrid({ partitions, bright }: { partitions: { partition: number; leader: number; replicas: number[]; isr: number[] }[]; bright: boolean }) {
  // Get all broker IDs involved
  const brokerIds = [...new Set(partitions.flatMap((p) => p.replicas))].sort((a, b) => a - b);
  if (brokerIds.length <= 1 || partitions.length === 0) return null;

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-3">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Replica Assignment Grid
        </span>
        <div className="flex items-center gap-3">
          <span className="flex items-center gap-1 text-[9px]">
            <span className={`w-3 h-3 rounded ${bright ? "bg-cyan-400" : "bg-cyan-500"}`} />
            <span className={bright ? "text-slate-500" : "text-slate-400"}>Leader+ISR</span>
          </span>
          <span className="flex items-center gap-1 text-[9px]">
            <span className={`w-3 h-3 rounded ${bright ? "bg-indigo-300" : "bg-indigo-500/60"}`} />
            <span className={bright ? "text-slate-500" : "text-slate-400"}>Follower ISR</span>
          </span>
          <span className="flex items-center gap-1 text-[9px]">
            <span className={`w-3 h-3 rounded ${bright ? "bg-red-300" : "bg-red-500/50"}`} />
            <span className={bright ? "text-slate-500" : "text-slate-400"}>Out of ISR</span>
          </span>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr>
              <th className={`text-[10px] font-mono text-left px-2 py-1.5 ${bright ? "text-slate-500" : "text-slate-400"}`}>P#</th>
              {brokerIds.map((bId) => (
                <th key={bId} className={`text-[10px] font-mono text-center px-2 py-1.5 ${bright ? "text-slate-500" : "text-slate-400"}`}>
                  B{bId}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {partitions.map((p) => (
              <tr key={p.partition}>
                <td className={`text-[10px] font-mono font-bold px-2 py-1 ${bright ? "text-slate-600" : "text-slate-300"}`}>
                  {p.partition}
                </td>
                {brokerIds.map((bId) => {
                  const isLeader = p.leader === bId;
                  const isReplica = p.replicas.includes(bId);
                  const isInISR = p.isr.includes(bId);
                  let cellCls = "";
                  let content = "";
                  if (isLeader && isInISR) {
                    cellCls = bright ? "bg-cyan-400/80 text-white" : "bg-cyan-500 text-white";
                    content = "L";
                  } else if (isReplica && isInISR) {
                    cellCls = bright ? "bg-indigo-300/70 text-white" : "bg-indigo-500/60 text-white";
                    content = "F";
                  } else if (isReplica && !isInISR) {
                    cellCls = bright ? "bg-red-300/70 text-white" : "bg-red-500/50 text-white";
                    content = "!";
                  } else {
                    cellCls = bright ? "bg-slate-50" : "bg-slate-800/20";
                  }
                  return (
                    <td key={bId} className="px-1 py-1 text-center">
                      <div className={`w-7 h-7 rounded-lg flex items-center justify-center text-[9px] font-bold mx-auto ${cellCls}`}>
                        {content}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function OffsetDistribution({ partitions, bright }: { partitions: { partition: number; endOffset: number; isr: number[]; replicas: number[] }[]; bright: boolean }) {
  const maxOffset = Math.max(...partitions.map((p) => p.endOffset), 1);
  const barHeight = 32;

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-3">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Offset Distribution
        </span>
        <span className={`text-[11px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>
          max: {maxOffset.toLocaleString()}
        </span>
      </div>
      <div className="flex items-end gap-1" style={{ height: barHeight + 20 }}>
        {partitions.map((p) => {
          const height = maxOffset > 0 ? (p.endOffset / maxOffset) * barHeight : 0;
          const isUnderReplicated = p.isr.length < p.replicas.length;
          return (
            <div
              key={p.partition}
              className="flex-1 flex flex-col items-center gap-1"
              title={`Partition ${p.partition}: ${p.endOffset.toLocaleString()} offsets${isUnderReplicated ? " (under-replicated)" : ""}`}
            >
              <div
                className={`w-full min-w-[4px] rounded-t transition-all duration-500 ${
                  isUnderReplicated
                    ? "bg-red-500"
                    : bright ? "bg-indigo-400" : "bg-indigo-500/70"
                }`}
                style={{ height: Math.max(2, height) }}
              />
              <span className={`text-[8px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>
                {p.partition}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function BrokerDistribution({ partitions, bright }: { partitions: { partition: number; replicas: number[]; isr: number[] }[]; bright: boolean }) {
  // Group partitions by leader (first replica)
  const brokerMap = new Map<number, { total: number; underReplicated: number }>();
  for (const p of partitions) {
    const leader = p.replicas[0] ?? -1;
    const entry = brokerMap.get(leader) || { total: 0, underReplicated: 0 };
    entry.total++;
    if (p.isr.length < p.replicas.length) entry.underReplicated++;
    brokerMap.set(leader, entry);
  }
  const brokers = [...brokerMap.entries()].sort((a, b) => a[0] - b[0]);
  if (brokers.length <= 1) return null;
  const maxCount = Math.max(...brokers.map(([, v]) => v.total), 1);

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-3">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Partition Distribution by Broker
        </span>
        <span className={`text-[11px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>
          {brokers.length} brokers
        </span>
      </div>
      <div className="space-y-2">
        {brokers.map(([brokerId, { total, underReplicated }]) => (
          <div key={brokerId} className="flex items-center gap-3">
            <span className={`text-[11px] font-mono w-16 text-right shrink-0 ${bright ? "text-slate-500" : "text-slate-400"}`}>
              Broker {brokerId}
            </span>
            <div className={`flex-1 rounded-full overflow-hidden h-4 ${bright ? "bg-slate-100" : "bg-slate-800/60"}`}>
              <div className="h-full flex">
                {underReplicated > 0 && (
                  <div
                    className="h-full bg-red-500 transition-all duration-500"
                    style={{ width: `${(underReplicated / maxCount) * 100}%` }}
                    title={`${underReplicated} under-replicated`}
                  />
                )}
                <div
                  className={`h-full transition-all duration-500 ${bright ? "bg-cyan-400" : "bg-cyan-500/70"}`}
                  style={{ width: `${((total - underReplicated) / maxCount) * 100}%` }}
                />
              </div>
            </div>
            <span className={`text-[11px] font-mono font-bold w-8 ${bright ? "text-slate-600" : "text-slate-300"}`}>
              {total}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function SummaryCard({ label, value, color, bright }: { label: string; value: string; color: string; bright: boolean }) {
  const darkColorMap: Record<string, string> = {
    indigo: "border-indigo-500/20 from-indigo-500/[0.06]",
    emerald: "border-emerald-500/20 from-emerald-500/[0.06]",
    red: "border-red-500/20 from-red-500/[0.06]",
    slate: "border-slate-700/30 from-slate-500/[0.04]",
  };
  const brightColorMap: Record<string, string> = {
    indigo: "border-indigo-200/60 from-indigo-50",
    emerald: "border-emerald-200/60 from-emerald-50",
    red: "border-red-200/60 from-red-50",
    slate: "border-slate-200/60 from-slate-50",
  };
  const colorMap = bright ? brightColorMap : darkColorMap;
  return (
    <div className={`rounded-2xl border bg-gradient-to-br ${colorMap[color] || colorMap.slate} to-transparent px-4 py-3`}>
      <div className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>{label}</div>
      <div className={`text-xl font-bold mt-0.5 tabular-nums ${bright ? "text-slate-800" : "text-white"}`}>{value}</div>
    </div>
  );
}

interface ReassignPanelProps {
  topicName: string;
  partitions: { partition: number; leader: number; replicas: number[]; isr: number[] }[];
  isBright: boolean;
}

function ReassignPanel({ topicName, partitions, isBright }: ReassignPanelProps) {
  const addToast = useToastStore((s) => s.addToast);
  const [assignments, setAssignments] = useState<Record<number, string>>({});
  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState<{ inProgress: boolean; reassignments: unknown[]; note?: string } | null>(null);
  const [checkingStatus, setCheckingStatus] = useState(false);

  const allBrokers = useMemo(() => {
    const ids = new Set<number>();
    partitions.forEach((p) => {
      p.replicas.forEach((r) => ids.add(r));
      if (p.leader >= 0) ids.add(p.leader);
    });
    return Array.from(ids).sort((a, b) => a - b);
  }, [partitions]);

  const checkStatus = async () => {
    setCheckingStatus(true);
    try {
      const data = await apiFetch<{ inProgress: boolean; reassignments: unknown[]; note?: string }>(
        `/api/topics/${encodeURIComponent(topicName)}/reassign`
      );
      setStatus(data);
    } catch (e) {
      addToast(String(e), "error");
    }
    setCheckingStatus(false);
  };

  useEffect(() => { checkStatus(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleReassign = async () => {
    const parsed: { partition: number; replicas: number[] }[] = [];
    for (const [partStr, replicasStr] of Object.entries(assignments)) {
      if (!replicasStr.trim()) continue;
      const replicas = replicasStr.split(",").map((s) => parseInt(s.trim(), 10)).filter((n) => !isNaN(n));
      if (replicas.length === 0) continue;
      parsed.push({ partition: parseInt(partStr, 10), replicas });
    }
    if (parsed.length === 0) {
      addToast("No partition assignments specified", "error");
      return;
    }
    setSubmitting(true);
    try {
      await apiFetch(`/api/topics/${encodeURIComponent(topicName)}/reassign`, {
        method: "POST",
        body: JSON.stringify({ assignments: parsed }),
      });
      addToast(`Reassignment started for ${parsed.length} partition(s)`, "success");
      setAssignments({});
      checkStatus();
    } catch (e) {
      addToast(String(e), "error");
    }
    setSubmitting(false);
  };

  return (
    <div className="space-y-4">
      {/* Status */}
      <div className={`rounded-2xl border p-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
        <div className="flex items-center justify-between mb-3">
          <h4 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Reassignment Status</h4>
          <button
            onClick={checkStatus}
            disabled={checkingStatus}
            className={`text-[10px] font-medium px-2.5 py-1 rounded-lg border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
            }`}
          >
            {checkingStatus ? "Checking..." : "Refresh"}
          </button>
        </div>
        {status ? (
          <div className={`flex items-center gap-2 py-2 px-3 rounded-xl ${
            status.inProgress
              ? isBright ? "bg-amber-50" : "bg-amber-500/10"
              : isBright ? "bg-emerald-50" : "bg-emerald-500/10"
          }`}>
            <div className={`w-2 h-2 rounded-full ${status.inProgress ? "bg-amber-500 animate-pulse" : "bg-emerald-500"}`} />
            <span className={`text-xs font-medium ${
              status.inProgress
                ? isBright ? "text-amber-700" : "text-amber-300"
                : isBright ? "text-emerald-700" : "text-emerald-300"
            }`}>
              {status.inProgress
                ? `${status.reassignments.length} partition(s) reassigning`
                : "No reassignments in progress"}
            </span>
          </div>
        ) : (
          <div className="flex items-center justify-center py-4">
            <div className="w-4 h-4 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
          </div>
        )}
        {status?.note && (
          <p className={`text-[10px] mt-2 ${isBright ? "text-slate-400" : "text-slate-500"}`}>{status.note}</p>
        )}
      </div>

      {/* Assignment editor */}
      <div className={`rounded-2xl border p-4 space-y-3 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
        <div>
          <h4 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Reassign Partitions</h4>
          <p className={`text-xs mt-0.5 ${isBright ? "text-slate-500" : "text-slate-500"}`}>
            Specify new broker IDs (comma-separated) for each partition. Available brokers: {allBrokers.join(", ")}
          </p>
        </div>
        <div className="space-y-1.5 max-h-64 overflow-y-auto">
          {partitions.map((p) => (
            <div key={p.partition} className={`flex items-center gap-3 py-2 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
              <span className={`text-[11px] font-mono w-6 text-center shrink-0 ${isBright ? "text-slate-500" : "text-slate-400"}`}>P{p.partition}</span>
              <div className="flex items-center gap-1 shrink-0">
                <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Current:</span>
                <span className={`text-[10px] font-mono ${isBright ? "text-slate-600" : "text-slate-300"}`}>[{p.replicas.join(",")}]</span>
                <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>L:{p.leader}</span>
              </div>
              <svg className={`w-3 h-3 shrink-0 ${isBright ? "text-slate-300" : "text-slate-600"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path d="M5 12h14m-7-7l7 7-7 7" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
              <input
                type="text"
                placeholder={p.replicas.join(",")}
                value={assignments[p.partition] || ""}
                onChange={(e) => setAssignments({ ...assignments, [p.partition]: e.target.value })}
                className={`flex-1 px-2 py-1 rounded-lg text-xs font-mono border outline-none ${
                  isBright
                    ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                    : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-600"
                }`}
              />
            </div>
          ))}
        </div>
        <div className="flex items-center gap-2 pt-2">
          <button
            onClick={() => {
              const balanced: Record<number, string> = {};
              partitions.forEach((p) => {
                const shuffled = [...allBrokers].sort(() => Math.random() - 0.5);
                balanced[p.partition] = shuffled.slice(0, p.replicas.length).join(",");
              });
              setAssignments(balanced);
            }}
            className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
            }`}
          >
            Auto-balance
          </button>
          <button
            onClick={() => setAssignments({})}
            className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
            }`}
          >
            Clear
          </button>
          <div className="flex-1" />
          <button
            onClick={handleReassign}
            disabled={submitting || Object.values(assignments).every((v) => !v.trim())}
            className={`px-4 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
              isBright
                ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
            }`}
          >
            {submitting ? "Reassigning..." : "Start Reassignment"}
          </button>
        </div>
      </div>
    </div>
  );
}

interface ConfigDiffEntry {
  key: string;
  value: string;
  default: string;
  source: string;
  is_overridden: boolean;
}

function ConfigDiffPanel({ topicName, isBright }: { topicName: string; isBright: boolean }) {
  const [entries, setEntries] = useState<ConfigDiffEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<"all" | "overridden">("overridden");
  const [search, setSearch] = useState("");

  useEffect(() => {
    setLoading(true);
    apiFetch<{ configs: ConfigDiffEntry[] }>(`/api/topics/${encodeURIComponent(topicName)}/config-diff`)
      .then((r) => setEntries(r.configs || []))
      .catch(() => setEntries([]))
      .finally(() => setLoading(false));
  }, [topicName]);

  const filtered = useMemo(() => {
    let result = entries;
    if (filter === "overridden") result = result.filter((e) => e.is_overridden);
    if (search) result = result.filter((e) => e.key.toLowerCase().includes(search.toLowerCase()));
    return result;
  }, [entries, filter, search]);

  const overriddenCount = entries.filter((e) => e.is_overridden).length;

  if (loading) return (
    <div className="flex items-center justify-center py-12">
      <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
    </div>
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-1">
          {(["overridden", "all"] as const).map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
                filter === f
                  ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                  : isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
              }`}
            >
              {f === "overridden" ? `Overridden (${overriddenCount})` : `All (${entries.length})`}
            </button>
          ))}
        </div>
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Filter configs..."
          className={`px-3 py-1.5 rounded-lg text-xs border outline-none ${
            isBright
              ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-400"
              : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-500"
          }`}
        />
      </div>

      {filtered.length === 0 ? (
        <div className={`text-center py-8 text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          {filter === "overridden" ? "No overridden configs" : "No configs match filter"}
        </div>
      ) : (
        <div className="space-y-1.5 max-h-[500px] overflow-y-auto">
          {filtered.map((entry) => (
            <div
              key={entry.key}
              className={`rounded-xl border px-4 py-3 ${
                entry.is_overridden
                  ? isBright ? "border-indigo-200/60 bg-indigo-50/50" : "border-indigo-500/20 bg-indigo-500/[0.04]"
                  : isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"
              }`}
            >
              <div className="flex items-center justify-between mb-1.5">
                <span className={`text-xs font-mono font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>{entry.key}</span>
                <div className="flex items-center gap-2">
                  {entry.is_overridden && (
                    <span className={`text-[9px] px-1.5 py-0.5 rounded font-medium ${
                      isBright ? "bg-indigo-100 text-indigo-600" : "bg-indigo-500/20 text-indigo-300"
                    }`}>OVERRIDDEN</span>
                  )}
                  <span className={`text-[9px] px-1.5 py-0.5 rounded ${
                    isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"
                  }`}>{entry.source}</span>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <span className={`text-xs font-mono ${
                  entry.is_overridden
                    ? isBright ? "text-indigo-600" : "text-indigo-300"
                    : isBright ? "text-slate-600" : "text-slate-400"
                }`}>{entry.value}</span>
                {entry.is_overridden && entry.value !== entry.default && (
                  <>
                    <span className={`text-[10px] ${isBright ? "text-slate-300" : "text-slate-600"}`}>←</span>
                    <span className={`text-[10px] font-mono line-through ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                      {entry.default || "(empty)"}
                    </span>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

interface KeyDistData {
  total_sampled: number;
  unique_keys: number;
  null_key_count: number;
  top_keys: { key: string; count: number; percentage: number }[];
  key_entropy: number;
}

function KeyDistributionPanel({ topicName, isBright }: { topicName: string; isBright: boolean }) {
  const [data, setData] = useState<KeyDistData | null>(null);
  const [loading, setLoading] = useState(false);
  const [sampleSize, setSampleSize] = useState("1000");
  const [showChart, setShowChart] = useState(true);

  const fetchData = useCallback(() => {
    setLoading(true);
    apiFetch<KeyDistData>(`/api/topics/${encodeURIComponent(topicName)}/key-distribution?sample_size=${sampleSize}`)
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [topicName, sampleSize]);

  useEffect(() => { fetchData(); }, [fetchData]);

  if (loading) return (
    <div className="flex items-center justify-center py-12">
      <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
    </div>
  );

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <span className={`text-[11px] ${isBright ? "text-slate-500" : "text-slate-400"}`}>Sample size:</span>
          {["100", "500", "1000", "5000"].map((size) => (
            <button
              key={size}
              onClick={() => setSampleSize(size)}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
                sampleSize === size
                  ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                  : isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
              }`}
            >
              {size}
            </button>
          ))}
        </div>
        <button
          onClick={fetchData}
          className={`px-3 py-1 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
            isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
          }`}
        >
          Refresh
        </button>
        <button
          onClick={() => setShowChart(!showChart)}
          className={`px-3 py-1 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
            isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
          }`}
        >
          {showChart ? "Table" : "Chart"}
        </button>
      </div>

      {!data ? (
        <div className={`text-center py-8 text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          No key distribution data available. Topic may be empty.
        </div>
      ) : (
        <>
          {/* Summary metrics */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            {[
              { label: "Messages Sampled", value: data.total_sampled.toLocaleString() },
              { label: "Unique Keys", value: data.unique_keys.toLocaleString() },
              { label: "Null Keys", value: `${data.null_key_count} (${data.total_sampled > 0 ? ((data.null_key_count / data.total_sampled) * 100).toFixed(1) : 0}%)` },
              { label: "Key Entropy", value: data.key_entropy.toFixed(3) },
            ].map((m) => (
              <div key={m.label} className={`rounded-xl border px-3 py-2.5 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
                <div className={`text-[10px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>{m.label}</div>
                <div className={`text-lg font-bold tabular-nums mt-0.5 ${isBright ? "text-slate-800" : "text-white"}`}>{m.value}</div>
              </div>
            ))}
          </div>

          {/* Key distribution chart/table */}
          {data.top_keys.length > 0 && (
            <div className={`rounded-xl border p-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Top Keys (showing {Math.min(data.top_keys.length, 30)})
              </div>
              {showChart ? (
                <div className="space-y-1.5 max-h-[400px] overflow-y-auto">
                  {data.top_keys.slice(0, 30).map((k) => (
                    <div key={k.key} className="flex items-center gap-3">
                      <span className={`text-[11px] font-mono w-40 truncate shrink-0 ${isBright ? "text-slate-600" : "text-slate-300"}`} title={k.key}>
                        {k.key === "null" ? <em className={isBright ? "text-slate-400" : "text-slate-500"}>null</em> : k.key}
                      </span>
                      <div className={`flex-1 h-5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                        <div
                          className={`h-full rounded-full transition-all ${
                            k.key === "null"
                              ? isBright ? "bg-slate-300" : "bg-slate-500/60"
                              : isBright ? "bg-indigo-400" : "bg-indigo-500/70"
                          }`}
                          style={{ width: `${Math.max(2, k.percentage)}%` }}
                        />
                      </div>
                      <span className={`text-[10px] font-mono tabular-nums w-20 text-right ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                        {k.count} ({k.percentage.toFixed(1)}%)
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="overflow-x-auto max-h-[400px]">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className={`border-b ${isBright ? "border-slate-200" : "border-slate-700"}`}>
                        <th className={`text-left py-2 px-2 font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Key</th>
                        <th className={`text-right py-2 px-2 font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Count</th>
                        <th className={`text-right py-2 px-2 font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>%</th>
                      </tr>
                    </thead>
                    <tbody>
                      {data.top_keys.slice(0, 50).map((k) => (
                        <tr key={k.key} className={`border-b ${isBright ? "border-slate-100" : "border-slate-800"}`}>
                          <td className={`py-1.5 px-2 font-mono ${isBright ? "text-slate-700" : "text-slate-300"}`}>{k.key}</td>
                          <td className={`py-1.5 px-2 text-right font-mono tabular-nums ${isBright ? "text-slate-600" : "text-slate-400"}`}>{k.count.toLocaleString()}</td>
                          <td className={`py-1.5 px-2 text-right font-mono tabular-nums ${isBright ? "text-slate-500" : "text-slate-400"}`}>{k.percentage.toFixed(2)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

interface SearchResult {
  partition: number;
  offset: number;
  timestamp: number;
  key: string | null;
  value: string;
  headers: { key: string; value: string }[];
}

function MessageSearchPanel({ topicName, isBright }: { topicName: string; isBright: boolean }) {
  const [keyPattern, setKeyPattern] = useState("");
  const [valuePattern, setValuePattern] = useState("");
  const [partition, setPartition] = useState("");
  const [maxResults, setMaxResults] = useState("50");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [searching, setSearching] = useState(false);
  const [searched, setSearched] = useState(false);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);

  const inputCls = `w-full px-3 py-2 rounded-xl text-xs font-mono border outline-none ${
    isBright
      ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-400"
      : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-500"
  }`;

  const handleSearch = async () => {
    setSearching(true);
    setSearched(true);
    try {
      const body: Record<string, unknown> = { max_results: Number(maxResults) || 50 };
      if (keyPattern.trim()) body.key_pattern = keyPattern.trim();
      if (valuePattern.trim()) body.value_pattern = valuePattern.trim();
      if (partition.trim()) body.partition = Number(partition);
      const data = await apiFetch<{ messages: SearchResult[] }>(
        `/api/topics/${encodeURIComponent(topicName)}/search`,
        { method: "POST", body: JSON.stringify(body) }
      );
      setResults(data.messages || []);
    } catch {
      setResults([]);
    }
    setSearching(false);
  };

  return (
    <div className="space-y-4">
      {/* Search form */}
      <div className={`rounded-xl border p-4 space-y-3 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Key Pattern (regex)
            </label>
            <input
              type="text"
              value={keyPattern}
              onChange={(e) => setKeyPattern(e.target.value)}
              placeholder="e.g. user-.*"
              className={inputCls}
            />
          </div>
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Value Pattern (regex)
            </label>
            <input
              type="text"
              value={valuePattern}
              onChange={(e) => setValuePattern(e.target.value)}
              placeholder='e.g. "error":true'
              className={inputCls}
            />
          </div>
        </div>
        <div className="grid grid-cols-3 gap-3">
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Partition (optional)
            </label>
            <input
              type="number"
              value={partition}
              onChange={(e) => setPartition(e.target.value)}
              placeholder="All"
              className={inputCls}
              min="0"
            />
          </div>
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Max Results
            </label>
            <input
              type="number"
              value={maxResults}
              onChange={(e) => setMaxResults(e.target.value)}
              className={inputCls}
              min="1"
              max="500"
            />
          </div>
          <div className="flex items-end">
            <button
              onClick={handleSearch}
              disabled={searching}
              className={`w-full px-4 py-2 rounded-xl text-xs font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                isBright
                  ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                  : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
              }`}
            >
              {searching ? "Searching..." : "Search Messages"}
            </button>
          </div>
        </div>
      </div>

      {/* Results */}
      {searching && (
        <div className="flex items-center justify-center py-8">
          <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      )}

      {!searching && searched && (
        <div className={`rounded-xl border p-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className="flex items-center justify-between mb-3">
            <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              {results.length} Result{results.length !== 1 ? "s" : ""}
            </span>
            {results.length > 0 && (
              <button
                onClick={() => {
                  const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a");
                  a.href = url; a.download = `${topicName}-search-results.json`;
                  a.click(); URL.revokeObjectURL(url);
                }}
                className={`text-[10px] font-medium px-2.5 py-1 rounded-lg border transition-colors cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
                }`}
              >
                Export JSON
              </button>
            )}
          </div>
          {results.length === 0 ? (
            <div className={`text-center py-6 text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              No messages found matching your criteria
            </div>
          ) : (
            <div className="space-y-1.5 max-h-[500px] overflow-y-auto">
              {results.map((msg, idx) => {
                const valStr = typeof msg.value === "string" ? msg.value : JSON.stringify(msg.value);
                const isExpanded = expandedIdx === idx;
                return (
                  <div
                    key={`${msg.partition}-${msg.offset}`}
                    className={`rounded-xl border px-3 py-2 cursor-pointer transition-colors ${
                      isBright ? "border-slate-100 bg-slate-50/50 hover:bg-slate-50" : "border-slate-800/40 bg-slate-800/20 hover:bg-slate-800/40"
                    }`}
                    onClick={() => setExpandedIdx(isExpanded ? null : idx)}
                  >
                    <div className="flex items-center gap-3 mb-1">
                      <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>P{msg.partition}:O{msg.offset}</span>
                      <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                        {new Date(msg.timestamp).toLocaleString()}
                      </span>
                      {msg.key && (
                        <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
                          isBright ? "bg-indigo-50 text-indigo-600" : "bg-indigo-500/10 text-indigo-300"
                        }`}>{msg.key}</span>
                      )}
                    </div>
                    <div className={`text-xs font-mono ${isExpanded ? "whitespace-pre-wrap break-all" : "truncate"} ${
                      isBright ? "text-slate-600" : "text-slate-300"
                    }`}>
                      {isExpanded ? valStr : (valStr.length > 200 ? valStr.slice(0, 200) + "..." : valStr)}
                    </div>
                    {isExpanded && msg.headers && msg.headers.length > 0 && (
                      <div className={`mt-2 pt-2 border-t ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
                        <span className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Headers</span>
                        <div className="flex flex-wrap gap-1 mt-1">
                          {msg.headers.map((h, hi) => (
                            <span key={hi} className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${
                              isBright ? "bg-slate-100 text-slate-600" : "bg-slate-800 text-slate-400"
                            }`}>{h.key}: {h.value}</span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ReplayPanel({ topicName, isBright }: { topicName: string; isBright: boolean }) {
  const addToast = useToastStore((s) => s.addToast);
  const [targetTopic, setTargetTopic] = useState("");
  const [partition, setPartition] = useState("");
  const [offset, setOffset] = useState("0");
  const [limit, setLimit] = useState("50");
  const [replaying, setReplaying] = useState(false);
  const [result, setResult] = useState<{ copied: number; errors: number } | null>(null);

  const inputCls = `w-full px-3 py-2 rounded-xl text-xs font-mono border outline-none ${
    isBright
      ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-400"
      : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-500"
  }`;

  const handleReplay = async () => {
    if (!targetTopic.trim()) { addToast("Target topic is required", "error"); return; }
    setReplaying(true);
    setResult(null);
    try {
      const body: Record<string, unknown> = { targetTopic: targetTopic.trim(), limit: Number(limit) || 50 };
      if (partition.trim()) body.partition = Number(partition);
      if (offset.trim()) body.offset = Number(offset);
      const data = await apiFetch<{ copied: number; errors: number }>(
        `/api/topics/${encodeURIComponent(topicName)}/replay`,
        { method: "POST", body: JSON.stringify(body) }
      );
      setResult(data);
      addToast(`Replayed ${data.copied} message(s) to ${targetTopic}`, data.errors > 0 ? "error" : "success");
    } catch (e) {
      addToast(String(e), "error");
    }
    setReplaying(false);
  };

  return (
    <div className="space-y-4">
      <div className={`rounded-xl border p-4 space-y-3 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
        <div>
          <h4 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Message Replay</h4>
          <p className={`text-xs mt-0.5 ${isBright ? "text-slate-500" : "text-slate-500"}`}>
            Copy messages from <span className="font-mono font-medium">{topicName}</span> to another topic
          </p>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>Target Topic</label>
            <input type="text" value={targetTopic} onChange={(e) => setTargetTopic(e.target.value)} placeholder="destination-topic" className={inputCls} />
          </div>
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>Limit</label>
            <input type="number" value={limit} onChange={(e) => setLimit(e.target.value)} min="1" max="200" className={inputCls} />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>Source Partition (optional)</label>
            <input type="number" value={partition} onChange={(e) => setPartition(e.target.value)} placeholder="All" min="0" className={inputCls} />
          </div>
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium mb-1 block ${isBright ? "text-slate-500" : "text-slate-400"}`}>Start Offset</label>
            <input type="number" value={offset} onChange={(e) => setOffset(e.target.value)} min="0" className={inputCls} />
          </div>
        </div>
        <div className="flex items-center gap-3 pt-1">
          <button
            onClick={handleReplay}
            disabled={replaying || !targetTopic.trim()}
            className={`px-4 py-2 rounded-xl text-xs font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
              isBright
                ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
            }`}
          >
            {replaying ? "Replaying..." : "Start Replay"}
          </button>
          {result && (
            <span className={`text-xs font-medium ${
              result.errors > 0 ? isBright ? "text-amber-600" : "text-amber-400" : isBright ? "text-emerald-600" : "text-emerald-400"
            }`}>
              {result.copied} copied, {result.errors} errors
            </span>
          )}
        </div>
      </div>
      <div className={`text-[10px] px-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
        Messages will be replayed to the target topic preserving keys and headers. Max 200 messages per replay.
      </div>
    </div>
  );
}

function TopicConsumersPanel({ topicName, isBright }: { topicName: string; isBright: boolean }) {
  const [consumers, setConsumers] = useState<{
    groupId: string;
    state: string;
    members: number;
    totalLag: number;
    partitions: { partition: number; currentOffset: number; endOffset: number; lag: number }[];
  }[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    apiFetch<typeof consumers>(`/api/topics/${encodeURIComponent(topicName)}/consumer-groups`)
      .then(setConsumers)
      .catch(() => setConsumers([]))
      .finally(() => setLoading(false));
  }, [topicName]);

  if (loading) return (
    <div className="flex items-center justify-center h-32">
      <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
    </div>
  );

  if (consumers.length === 0) return (
    <div className={`text-center py-12 text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
      No consumer groups are consuming from this topic
    </div>
  );

  const totalLag = consumers.reduce((s, c) => s + c.totalLag, 0);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-3 gap-3">
        <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-amber-50/50 border-amber-200/40" : "bg-amber-500/[0.06] border-amber-500/15"}`}>
          <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Consumer Groups</div>
          <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{consumers.length}</div>
        </div>
        <div className={`rounded-xl px-3 py-2.5 border ${totalLag > 1000 ? (isBright ? "bg-red-50/50 border-red-200/40" : "bg-red-500/[0.06] border-red-500/15") : isBright ? "bg-emerald-50/50 border-emerald-200/40" : "bg-emerald-500/[0.06] border-emerald-500/15"}`}>
          <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Total Lag</div>
          <div className={`text-lg font-bold tabular-nums ${totalLag > 1000 ? "text-red-500" : "text-emerald-500"}`}>
            {totalLag > 1000000 ? `${(totalLag / 1000000).toFixed(1)}M` : totalLag > 1000 ? `${(totalLag / 1000).toFixed(1)}K` : totalLag}
          </div>
        </div>
        <div className={`rounded-xl px-3 py-2.5 border ${isBright ? "bg-slate-50 border-slate-200/40" : "bg-slate-800/30 border-slate-700/20"}`}>
          <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Stable</div>
          <div className={`text-lg font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>
            {consumers.filter((c) => c.state === "Stable").length}/{consumers.length}
          </div>
        </div>
      </div>

      <div className="space-y-2">
        {consumers.sort((a, b) => b.totalLag - a.totalLag).map((c) => (
          <div key={c.groupId} className={`rounded-xl border px-4 py-3 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <span className={`text-xs font-mono font-medium ${isBright ? "text-amber-600" : "text-amber-300"}`}>{c.groupId}</span>
                <span className={`text-[9px] font-semibold uppercase px-1.5 py-0.5 rounded-md border ${
                  c.state === "Stable"
                    ? isBright ? "bg-emerald-50 text-emerald-700 border-emerald-200" : "bg-emerald-500/15 text-emerald-400 border-emerald-500/25"
                    : isBright ? "bg-slate-100 text-slate-500 border-slate-200" : "bg-slate-800/50 text-slate-400 border-slate-700/40"
                }`}>{c.state}</span>
              </div>
              <div className="flex items-center gap-3">
                <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{c.members} members</span>
                <span className={`text-xs font-mono font-bold tabular-nums ${
                  c.totalLag > 1000 ? "text-red-500" : c.totalLag > 0 ? "text-amber-500" : "text-emerald-500"
                }`}>lag: {c.totalLag > 1000 ? `${(c.totalLag / 1000).toFixed(1)}K` : c.totalLag}</span>
              </div>
            </div>
            {/* Per-partition lag bars */}
            {c.partitions.length > 0 && (
              <div className="flex gap-0.5 items-end h-6">
                {c.partitions.sort((a, b) => a.partition - b.partition).map((p) => {
                  const maxPartLag = Math.max(...c.partitions.map((pp) => pp.lag), 1);
                  const h = p.lag > 0 ? Math.max(3, (p.lag / maxPartLag) * 24) : 2;
                  const color = p.lag === 0 ? (isBright ? "bg-emerald-300" : "bg-emerald-500/50") : p.lag > 100 ? (isBright ? "bg-red-400" : "bg-red-500/60") : (isBright ? "bg-amber-400" : "bg-amber-500/60");
                  return (
                    <div
                      key={p.partition}
                      className={`flex-1 rounded-t-sm ${color}`}
                      style={{ height: `${h}px` }}
                      title={`P${p.partition}: lag ${p.lag.toLocaleString()}`}
                    />
                  );
                })}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

/* ---- Partition Timeline Panel ---- */
function PartitionTimelinePanel({ partitions, isBright, topicName }: {
  partitions: { partition: number; leader: number; replicas: number[]; isr: number[]; endOffset: number }[];
  isBright: boolean;
  topicName: string;
}) {
  const [snapshots, setSnapshots] = useState<{ ts: number; offsets: number[] }[]>([]);
  const [autoTrack, setAutoTrack] = useState(true);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);
  const { fetchTopicDetail } = useKafkaStore();

  // Collect offset snapshots over time
  useEffect(() => {
    const offsets = partitions.map((p) => p.endOffset);
    setSnapshots((prev) => [...prev, { ts: Date.now(), offsets }].slice(-60));
  }, [partitions]);

  // Auto-refresh for tracking
  useEffect(() => {
    if (autoTrack) {
      intervalRef.current = setInterval(() => fetchTopicDetail(topicName), 3000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoTrack, topicName, fetchTopicDetail]);

  const totalMessages = partitions.reduce((s, p) => s + p.endOffset, 0);
  const maxEndOffset = Math.max(...partitions.map((p) => p.endOffset), 1);

  // Partition write rates computed from snapshots
  const partitionRates = partitions.map((_p, i) => {
    if (snapshots.length < 2) return 0;
    const first = snapshots[0];
    const last = snapshots[snapshots.length - 1];
    const dt = (last.ts - first.ts) / 1000;
    if (dt <= 0) return 0;
    return ((last.offsets[i] || 0) - (first.offsets[i] || 0)) / dt;
  });

  const maxRate = Math.max(...partitionRates, 0.1);
  const partitionColors = ["#6366f1", "#8b5cf6", "#0ea5e9", "#10b981", "#f59e0b", "#ef4444", "#ec4899", "#14b8a6", "#f97316", "#a855f7", "#06b6d4", "#84cc16"];

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Partition Activity Timeline</h3>
          <p className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
            {snapshots.length} snapshots collected &middot; {partitions.length} partitions
          </p>
        </div>
        <button
          onClick={() => setAutoTrack(!autoTrack)}
          className={`px-2.5 py-1.5 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
            autoTrack
              ? isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300"
              : isBright ? "bg-white border-slate-200 text-slate-500" : "bg-slate-800 border-slate-700 text-slate-400"
          }`}
        >
          {autoTrack ? "Tracking Active" : "Start Tracking"}
        </button>
      </div>

      {/* Partition offset bars - current state */}
      <div className={`rounded-2xl border p-5 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
        <div className={`text-[10px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
          Current Offset Distribution
        </div>
        <div className="space-y-2">
          {partitions.map((p, i) => {
            const pct = maxEndOffset > 0 ? (p.endOffset / maxEndOffset) * 100 : 0;
            const color = partitionColors[i % partitionColors.length];
            return (
              <div key={p.partition} className="flex items-center gap-2">
                <span className={`text-[10px] font-mono w-6 text-right ${isBright ? "text-slate-500" : "text-slate-400"}`}>P{p.partition}</span>
                <div className={`flex-1 h-4 rounded-full overflow-hidden relative ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                  <div className="h-full rounded-full transition-all" style={{ width: `${Math.max(pct, 1)}%`, backgroundColor: color, opacity: 0.7 }} />
                  {/* ISR indicator */}
                  {p.isr.length < p.replicas.length && (
                    <div className="absolute right-1 top-0.5 w-1.5 h-3 rounded-full bg-red-500 animate-pulse" title="Under-replicated" />
                  )}
                </div>
                <span className={`text-[9px] font-mono w-16 text-right ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                  {p.endOffset > 1000000 ? `${(p.endOffset / 1000000).toFixed(1)}M` : p.endOffset > 1000 ? `${(p.endOffset / 1000).toFixed(0)}K` : p.endOffset}
                </span>
                <span className={`text-[9px] font-mono w-12 text-right ${partitionRates[i] > 0 ? (isBright ? "text-emerald-600" : "text-emerald-400") : isBright ? "text-slate-400" : "text-slate-500"}`}>
                  {partitionRates[i] > 0 ? `+${partitionRates[i].toFixed(1)}/s` : "idle"}
                </span>
              </div>
            );
          })}
        </div>
      </div>

      {/* Write rate chart over time */}
      {snapshots.length >= 3 && (
        <div className={`rounded-2xl border p-5 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
          <div className={`text-[10px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
            Write Rate Per Partition Over Time
          </div>
          <svg width="100%" viewBox="0 0 700 200" className="select-none">
            {/* Grid */}
            {[0, 0.25, 0.5, 0.75, 1].map((pct) => {
              const y = 10 + 160 * (1 - pct);
              return <line key={pct} x1={40} y1={y} x2={690} y2={y} stroke={isBright ? "#e2e8f0" : "#1e293b"} strokeWidth={0.5} />;
            })}
            {/* Per-partition lines */}
            {partitions.slice(0, 12).map((_p, pi) => {
              const points = snapshots.slice(1).map((snap, si) => {
                const prev = snapshots[si];
                const dt = Math.max((snap.ts - prev.ts) / 1000, 0.1);
                const rate = ((snap.offsets[pi] || 0) - (prev.offsets[pi] || 0)) / dt;
                const x = 40 + (si / Math.max(snapshots.length - 2, 1)) * 650;
                const y = 10 + 160 * (1 - Math.min(rate / maxRate, 1));
                return `${x.toFixed(1)},${y.toFixed(1)}`;
              });
              if (points.length < 2) return null;
              return (
                <polyline
                  key={pi}
                  points={points.join(" ")}
                  fill="none"
                  stroke={partitionColors[pi % partitionColors.length]}
                  strokeWidth={1.5}
                  strokeLinejoin="round"
                  opacity={0.7}
                />
              );
            })}
            {/* Time labels */}
            {[0, Math.floor(snapshots.length / 2), snapshots.length - 1].map((i) => {
              if (!snapshots[i]) return null;
              const x = 40 + (i / Math.max(snapshots.length - 1, 1)) * 650;
              const t = new Date(snapshots[i].ts);
              return (
                <text key={i} x={x} y={192} textAnchor="middle" fontSize={9}
                  fill={isBright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">
                  {t.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                </text>
              );
            })}
            {/* Y-axis */}
            {[0, 0.5, 1].map((pct) => (
              <text key={pct} x={36} y={10 + 160 * (1 - pct) + 3} textAnchor="end" fontSize={9}
                fill={isBright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">
                {(maxRate * pct).toFixed(0)}
              </text>
            ))}
            <text x={42} y={8} fontSize={9} fill={isBright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">msg/s</text>
          </svg>
          {/* Partition legend */}
          <div className="flex flex-wrap gap-2 mt-2">
            {partitions.slice(0, 12).map((p, i) => (
              <div key={i} className="flex items-center gap-1">
                <span className="w-2.5 h-2.5 rounded-sm" style={{ backgroundColor: partitionColors[i % partitionColors.length], opacity: 0.7 }} />
                <span className={`text-[9px] font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>P{p.partition}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Partition balance analysis */}
      <div className={`rounded-2xl border p-5 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
        <div className={`text-[10px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
          Partition Balance Analysis
        </div>
        {(() => {
          const avgOffset = totalMessages / partitions.length;
          const stdDev = Math.sqrt(partitions.reduce((s, p) => s + Math.pow(p.endOffset - avgOffset, 2), 0) / partitions.length);
          const skewPct = avgOffset > 0 ? (stdDev / avgOffset) * 100 : 0;
          const balanceScore = Math.max(0, 100 - skewPct);
          const leaders = partitions.reduce<Record<number, number>>((acc, p) => { acc[p.leader] = (acc[p.leader] || 0) + 1; return acc; }, {});
          return (
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className={`text-lg font-bold ${balanceScore > 80 ? (isBright ? "text-emerald-600" : "text-emerald-400") : balanceScore > 50 ? (isBright ? "text-amber-600" : "text-amber-400") : (isBright ? "text-red-600" : "text-red-400")}`}>
                  {balanceScore.toFixed(0)}%
                </div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Balance Score</div>
              </div>
              <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>
                  {avgOffset > 1000000 ? `${(avgOffset / 1000000).toFixed(1)}M` : avgOffset > 1000 ? `${(avgOffset / 1000).toFixed(0)}K` : avgOffset.toFixed(0)}
                </div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Avg Offsets/Part</div>
              </div>
              <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{Object.keys(leaders).length}</div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Leader Brokers</div>
              </div>
              <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className={`text-lg font-bold ${
                  partitions.some((p) => p.isr.length < p.replicas.length) ? (isBright ? "text-red-600" : "text-red-400") : isBright ? "text-emerald-600" : "text-emerald-400"
                }`}>
                  {partitions.filter((p) => p.isr.length === p.replicas.length).length}/{partitions.length}
                </div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>In-Sync</div>
              </div>
            </div>
          );
        })()}
      </div>
    </div>
  );
}

function CapacityPlanningPanel({ partitions, config, isBright }: {
  partitions: { partition: number; leader: number; replicas: number[]; isr: number[]; endOffset: number }[];
  config: Record<string, string>;
  isBright: boolean;
}) {
  const [snapshots, setSnapshots] = useState<{ ts: number; totalMessages: number; perPartition: number[] }[]>([]);
  const [autoTrack, setAutoTrack] = useState(true);

  useEffect(() => {
    const capture = () => {
      const perPartition = partitions.map((p) => p.endOffset);
      const totalMessages = perPartition.reduce((s, v) => s + v, 0);
      setSnapshots((prev) => {
        const next = [...prev, { ts: Date.now(), totalMessages, perPartition }];
        return next.length > 120 ? next.slice(-120) : next;
      });
    };
    capture();
    if (!autoTrack) return;
    const iv = setInterval(capture, 5000);
    return () => clearInterval(iv);
  }, [partitions, autoTrack]);

  const retentionMs = parseInt(config["retention.ms"] || "-1", 10);
  const retentionBytes = parseInt(config["retention.bytes"] || "-1", 10);
  const segmentBytes = parseInt(config["segment.bytes"] || "1073741824", 10);
  const replicationFactor = partitions.length > 0 ? partitions[0].replicas.length : 1;
  const totalMessages = partitions.reduce((s, p) => s + (p.endOffset), 0);
  const maxPartMessages = Math.max(...partitions.map((p) => p.endOffset), 1);

  // Estimate growth rate from snapshots (messages/sec)
  const growthRate = useMemo(() => {
    if (snapshots.length < 2) return 0;
    const first = snapshots[0];
    const last = snapshots[snapshots.length - 1];
    const dtSec = (last.ts - first.ts) / 1000;
    if (dtSec < 1) return 0;
    return (last.totalMessages - first.totalMessages) / dtSec;
  }, [snapshots]);

  // Estimated avg message size (rough estimate from segment size and messages)
  const estMsgSize = totalMessages > 0 ? Math.max(100, Math.min(10000, segmentBytes / Math.max(1, totalMessages / partitions.length))) : 500;
  const currentStorageEst = totalMessages * estMsgSize * replicationFactor;

  // Projected storage at different time horizons
  const projections = useMemo(() => {
    const rate = Math.max(0, growthRate);
    const horizons = [
      { label: "1 hour", sec: 3600 },
      { label: "1 day", sec: 86400 },
      { label: "7 days", sec: 604800 },
      { label: "30 days", sec: 2592000 },
    ];
    return horizons.map(({ label, sec }) => {
      const additionalMsgs = rate * sec;
      const retainedMsgs = retentionMs > 0 ? Math.min(totalMessages + additionalMsgs, rate * (retentionMs / 1000) + totalMessages) : totalMessages + additionalMsgs;
      const storage = retainedMsgs * estMsgSize * replicationFactor;
      return { label, messages: Math.round(retainedMsgs), storage };
    });
  }, [growthRate, totalMessages, retentionMs, estMsgSize, replicationFactor]);

  // Retention impact
  const retentionImpact = useMemo(() => {
    if (retentionMs <= 0 && retentionBytes <= 0) return { type: "unbounded" as const, note: "No retention limit - storage will grow indefinitely" };
    const parts: string[] = [];
    if (retentionMs > 0) parts.push(`Time: ${retentionMs < 3600000 ? `${(retentionMs / 60000).toFixed(0)}m` : retentionMs < 86400000 ? `${(retentionMs / 3600000).toFixed(1)}h` : `${(retentionMs / 86400000).toFixed(1)}d`}`);
    if (retentionBytes > 0) parts.push(`Size: ${fmtBytes(retentionBytes)} per partition`);
    const maxStorage = retentionBytes > 0 ? retentionBytes * partitions.length * replicationFactor : -1;
    return { type: "bounded" as const, note: parts.join(" | "), maxStorage };
  }, [retentionMs, retentionBytes, partitions.length, replicationFactor]);

  const cardCls = `rounded-2xl border ${isBright ? "border-slate-200 bg-white" : "border-slate-700/50 bg-slate-800/50"} p-5`;
  const labelCls = `text-[10px] uppercase tracking-wider font-semibold ${isBright ? "text-slate-400" : "text-slate-500"}`;
  const valCls = `text-xl font-bold ${isBright ? "text-slate-800" : "text-white"}`;

  return (
    <div className="space-y-5">
      {/* Current State */}
      <div className={cardCls}>
        <div className="flex items-center justify-between mb-4">
          <h3 className={`font-semibold ${isBright ? "text-slate-700" : "text-slate-200"}`}>Current State</h3>
          <button onClick={() => setAutoTrack(!autoTrack)} className={`text-xs px-2 py-1 rounded ${autoTrack ? "bg-emerald-500/20 text-emerald-400" : isBright ? "bg-slate-100 text-slate-500" : "bg-slate-700 text-slate-400"}`}>
            {autoTrack ? "Tracking" : "Paused"}
          </button>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div><div className={valCls}>{fmt(totalMessages)}</div><div className={labelCls}>Total Messages</div></div>
          <div><div className={valCls}>{fmtBytes(currentStorageEst)}</div><div className={labelCls}>Est. Storage</div></div>
          <div><div className={`${valCls} ${growthRate > 0 ? (isBright ? "text-blue-600" : "text-blue-400") : ""}`}>{growthRate > 0 ? `${growthRate.toFixed(1)}/s` : "Idle"}</div><div className={labelCls}>Write Rate</div></div>
          <div><div className={valCls}>{partitions.length} × RF{replicationFactor}</div><div className={labelCls}>Partitions × RF</div></div>
        </div>
      </div>

      {/* Growth Rate Chart */}
      {snapshots.length > 1 && (
        <div className={cardCls}>
          <h3 className={`font-semibold mb-3 ${isBright ? "text-slate-700" : "text-slate-200"}`}>Message Growth Over Time</h3>
          <svg viewBox="0 0 600 150" className="w-full" style={{ height: 150 }}>
            {(() => {
              const minTs = snapshots[0].ts;
              const maxTs = snapshots[snapshots.length - 1].ts;
              const minMsg = Math.min(...snapshots.map((s) => s.totalMessages));
              const maxMsg = Math.max(...snapshots.map((s) => s.totalMessages));
              const rangeTs = Math.max(1, maxTs - minTs);
              const rangeMsg = Math.max(1, maxMsg - minMsg);
              const pts = snapshots.map((s) => {
                const x = 40 + ((s.ts - minTs) / rangeTs) * 540;
                const y = 135 - ((s.totalMessages - minMsg) / rangeMsg) * 120;
                return `${x},${y}`;
              });
              const fillPts = [`40,135`, ...pts, `${40 + ((maxTs - minTs) / rangeTs) * 540},135`];
              return (
                <>
                  <defs><linearGradient id="capGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#3b82f6" stopOpacity="0.3" /><stop offset="100%" stopColor="#3b82f6" stopOpacity="0" /></linearGradient></defs>
                  <polygon points={fillPts.join(" ")} fill="url(#capGrad)" />
                  <polyline points={pts.join(" ")} fill="none" stroke="#3b82f6" strokeWidth="2" />
                  <text x="20" y="15" fontSize="10" fill={isBright ? "#64748b" : "#94a3b8"} textAnchor="middle">{fmt(maxMsg)}</text>
                  <text x="20" y="140" fontSize="10" fill={isBright ? "#64748b" : "#94a3b8"} textAnchor="middle">{fmt(minMsg)}</text>
                </>
              );
            })()}
          </svg>
          <div className={`text-xs text-center ${isBright ? "text-slate-400" : "text-slate-500"}`}>{snapshots.length} samples over {((snapshots[snapshots.length - 1].ts - snapshots[0].ts) / 1000).toFixed(0)}s</div>
        </div>
      )}

      {/* Storage Projections */}
      <div className={cardCls}>
        <h3 className={`font-semibold mb-3 ${isBright ? "text-slate-700" : "text-slate-200"}`}>Storage Projections</h3>
        <div className="space-y-3">
          {projections.map((p) => {
            const maxStore = retentionImpact.type === "bounded" && retentionImpact.maxStorage > 0 ? retentionImpact.maxStorage : projections[projections.length - 1].storage;
            const pct = Math.min(100, (p.storage / Math.max(1, maxStore)) * 100);
            const isOver = retentionImpact.type === "bounded" && retentionImpact.maxStorage > 0 && p.storage > retentionImpact.maxStorage * 0.8;
            return (
              <div key={p.label}>
                <div className="flex justify-between text-xs mb-1">
                  <span className={isBright ? "text-slate-600" : "text-slate-300"}>{p.label}</span>
                  <span className={isBright ? "text-slate-500" : "text-slate-400"}>{fmt(p.messages)} msgs | {fmtBytes(p.storage)}</span>
                </div>
                <div className={`h-3 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-700/50"}`}>
                  <div className={`h-full rounded-full transition-all ${isOver ? "bg-amber-500" : "bg-blue-500"}`} style={{ width: `${pct}%` }} />
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Retention Analysis */}
      <div className={cardCls}>
        <h3 className={`font-semibold mb-3 ${isBright ? "text-slate-700" : "text-slate-200"}`}>Retention Analysis</h3>
        <div className={`rounded-xl p-4 ${retentionImpact.type === "unbounded" ? (isBright ? "bg-amber-50 border border-amber-200" : "bg-amber-500/10 border border-amber-500/30") : (isBright ? "bg-emerald-50 border border-emerald-200" : "bg-emerald-500/10 border border-emerald-500/30")}`}>
          <div className="flex items-center gap-2 mb-2">
            <span className={`text-sm font-semibold ${retentionImpact.type === "unbounded" ? (isBright ? "text-amber-700" : "text-amber-400") : (isBright ? "text-emerald-700" : "text-emerald-400")}`}>
              {retentionImpact.type === "unbounded" ? "Unbounded Retention" : "Bounded Retention"}
            </span>
          </div>
          <div className={`text-xs ${isBright ? "text-slate-600" : "text-slate-300"}`}>{retentionImpact.note}</div>
          {retentionImpact.type === "bounded" && retentionImpact.maxStorage > 0 && (
            <div className={`mt-2 text-xs ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Max storage capacity: {fmtBytes(retentionImpact.maxStorage)} ({fmtBytes(retentionImpact.maxStorage / partitions.length)} per partition)
            </div>
          )}
        </div>
        <div className="grid grid-cols-3 gap-3 mt-3">
          <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
            <div className={valCls}>{config["cleanup.policy"] || "delete"}</div>
            <div className={labelCls}>Cleanup Policy</div>
          </div>
          <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
            <div className={valCls}>{formatRetention(config["retention.ms"])}</div>
            <div className={labelCls}>Retention Time</div>
          </div>
          <div className={`rounded-xl p-3 ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
            <div className={valCls}>{retentionBytes > 0 ? fmtBytes(retentionBytes) : "∞"}</div>
            <div className={labelCls}>Retention Bytes</div>
          </div>
        </div>
      </div>

      {/* Partition Size Distribution */}
      <div className={cardCls}>
        <h3 className={`font-semibold mb-3 ${isBright ? "text-slate-700" : "text-slate-200"}`}>Partition Size Distribution</h3>
        <div className="space-y-1.5">
          {partitions.map((p) => {
            const msgs = p.endOffset;
            const pct = (msgs / maxPartMessages) * 100;
            const colors = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b", "#ef4444", "#ec4899", "#6366f1"];
            return (
              <div key={p.partition} className="flex items-center gap-2">
                <span className={`text-[10px] w-6 text-right ${isBright ? "text-slate-400" : "text-slate-500"}`}>P{p.partition}</span>
                <div className={`flex-1 h-4 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-700/50"}`}>
                  <div className="h-full rounded-full" style={{ width: `${Math.max(1, pct)}%`, backgroundColor: colors[p.partition % colors.length] }} />
                </div>
                <span className={`text-[10px] w-16 text-right ${isBright ? "text-slate-500" : "text-slate-400"}`}>{fmt(msgs)}</span>
              </div>
            );
          })}
        </div>
        {(() => {
          const sizes = partitions.map((p) => p.endOffset);
          const avg = sizes.reduce((s, v) => s + v, 0) / Math.max(1, sizes.length);
          const stdDev = Math.sqrt(sizes.reduce((s, v) => s + (v - avg) ** 2, 0) / Math.max(1, sizes.length));
          const cv = avg > 0 ? (stdDev / avg) * 100 : 0;
          return (
            <div className={`mt-3 text-xs ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Coefficient of variation: <span className={`font-semibold ${cv > 50 ? (isBright ? "text-red-600" : "text-red-400") : cv > 20 ? (isBright ? "text-amber-600" : "text-amber-400") : (isBright ? "text-emerald-600" : "text-emerald-400")}`}>{cv.toFixed(1)}%</span>
              {cv > 50 ? " — Highly skewed, consider rebalancing" : cv > 20 ? " — Moderate skew" : " — Well balanced"}
            </div>
          );
        })()}
      </div>
    </div>
  );
}
