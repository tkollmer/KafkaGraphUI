import { useEffect, useState, useRef } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { useThemeStore } from "../store/themeStore";
import { useToastStore } from "../store/toastStore";
import { useNavigationStore } from "../store/navigationStore";
import { useGraphStore } from "../store/graphStore";
import { DataTable } from "../components/DataTable";
import { Modal } from "../components/Modal";
import { SkeletonTable } from "../components/Skeleton";
import { FreshnessIndicator } from "../components/FreshnessIndicator";
import { TopicDetail } from "./TopicDetail";
import { useFavoritesStore } from "../store/favoritesStore";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function fmtBytes(b: number): string {
  if (b >= 1_099_511_627_776) return `${(b / 1_099_511_627_776).toFixed(1)} TB`;
  if (b >= 1_073_741_824) return `${(b / 1_073_741_824).toFixed(1)} GB`;
  if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)} MB`;
  if (b >= 1_024) return `${(b / 1_024).toFixed(1)} KB`;
  return `${b} B`;
}

export function TopicsView() {
  const { topics, topicsLoading, fetchTopics, createTopic, deleteTopic, topicsLastFetched, consumerGroups, fetchConsumerGroups } = useKafkaStore();
  const { theme } = useThemeStore();
  const addToast = useToastStore((s) => s.addToast);
  const isBright = theme === "bright";
  const { pendingTopicName, clearPending } = useNavigationStore();
  const [selectedTopicName, setSelectedTopicName] = useState<string | null>(pendingTopicName);
  const [showCreate, setShowCreate] = useState(false);
  const [showDelete, setShowDelete] = useState<string | null>(null);
  const [createForm, setCreateForm] = useState({ name: "", partitions: "1", replicationFactor: "1", cleanupPolicy: "delete", retentionMs: "-1", retentionBytes: "-1", minInsyncReplicas: "1" });
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [selectedTopics, setSelectedTopics] = useState<Record<string, unknown>[]>([]);
  const [showBulkDelete, setShowBulkDelete] = useState(false);
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const [showCompare, setShowCompare] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);
  const graphMetrics = useGraphStore((s) => s.metrics);
  const [throughputHistory, setThroughputHistory] = useState<Map<string, number[]>>(new Map());

  useEffect(() => { fetchTopics(); fetchConsumerGroups(); }, [fetchTopics, fetchConsumerGroups]);

  // Track throughput history from graph metrics
  useEffect(() => {
    if (Object.keys(graphMetrics).length === 0) return;
    setThroughputHistory((prev) => {
      const next = new Map(prev);
      for (const topic of topics) {
        const rate = graphMetrics[topic.name]?.msgPerSec || 0;
        const hist = next.get(topic.name) || [];
        next.set(topic.name, [...hist, rate].slice(-12));
      }
      return next;
    });
  }, [graphMetrics, topics]);

  // Handle deep-link navigation from pipeline
  useEffect(() => {
    if (pendingTopicName) {
      setSelectedTopicName(pendingTopicName);
      clearPending();
    }
  }, [pendingTopicName, clearPending]);

  useEffect(() => {
    if (autoRefresh && !selectedTopicName) {
      intervalRef.current = setInterval(() => fetchTopics(), 5000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, selectedTopicName, fetchTopics]);

  if (selectedTopicName) {
    return <TopicDetail topicName={selectedTopicName} onBack={() => setSelectedTopicName(null)} />;
  }

  const handleCreate = async () => {
    setError(null);
    if (!createForm.name.trim()) { setError("Topic name is required"); return; }
    const topicName = createForm.name;
    const result = await createTopic(topicName, Number(createForm.partitions), Number(createForm.replicationFactor));
    if (result.success) {
      setShowCreate(false);
      setCreateForm({ name: "", partitions: "1", replicationFactor: "1", cleanupPolicy: "delete", retentionMs: "-1", retentionBytes: "-1", minInsyncReplicas: "1" });
      addToast(`Topic '${topicName}' created`, "success");
      fetchTopics();
    } else {
      const msg = result.error || "Failed to create topic";
      setError(msg);
      addToast(msg, "error");
    }
  };

  const handleDelete = async () => {
    if (!showDelete) return;
    const topicName = showDelete;
    const result = await deleteTopic(topicName);
    if (result.success) {
      setShowDelete(null);
      addToast(`Topic '${topicName}' deleted`, "success");
      fetchTopics();
    } else {
      const msg = result.error || "Failed to delete topic";
      setError(msg);
      addToast(msg, "error");
    }
  };

  const handleBulkDelete = async () => {
    setBulkDeleting(true);
    let successCount = 0;
    let failCount = 0;
    for (const t of selectedTopics) {
      const name = String(t.name);
      const result = await deleteTopic(name);
      if (result.success) successCount++;
      else failCount++;
    }
    setBulkDeleting(false);
    setShowBulkDelete(false);
    setSelectedTopics([]);
    if (successCount > 0) addToast(`Deleted ${successCount} topic${successCount > 1 ? "s" : ""}`, "success");
    if (failCount > 0) addToast(`Failed to delete ${failCount} topic${failCount > 1 ? "s" : ""}`, "error");
    fetchTopics();
  };

  const totalMessages = topics.reduce((s, t) => s + (t.totalMessages || 0), 0);
  const totalPartitions = topics.reduce((s, t) => s + (t.partitions || 0), 0);
  const estimatedSize = totalMessages * 1024; // ~1KB avg msg size estimate

  const columns = [
    { key: "name", label: "Topic Name", render: (r: Record<string, unknown>) => {
      const name = String(r.name);
      const isFav = useFavoritesStore.getState().isFavoriteTopic(name);
      return (
        <span className="flex items-center gap-1.5">
          <button
            onClick={(e) => { e.stopPropagation(); useFavoritesStore.getState().toggleFavoriteTopic(name); }}
            className={`shrink-0 cursor-pointer transition-colors ${isFav ? "text-amber-400" : isBright ? "text-slate-300 hover:text-amber-400" : "text-slate-600 hover:text-amber-400"}`}
            title={isFav ? "Remove from favorites" : "Add to favorites"}
          >
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill={isFav ? "currentColor" : "none"} stroke="currentColor" strokeWidth={2}>
              <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" strokeLinejoin="round" />
            </svg>
          </button>
          <span className={`font-mono font-medium ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{name}</span>
        </span>
      );
    }},
    { key: "partitions", label: "Partitions", className: "w-28 text-center" },
    { key: "replicationFactor", label: "RF", className: "w-20 text-center" },
    { key: "msgPerSec", label: "Rate", className: "w-36 text-right", render: (r: Record<string, unknown>) => {
      const rate = Number(r.msgPerSec || 0);
      const hist = throughputHistory.get(String(r.name)) || [];
      return (
        <div className="flex items-center gap-2 justify-end">
          {hist.length > 2 && (() => {
            const max = Math.max(...hist, 0.1);
            const w = 40;
            const h = 14;
            const points = hist.map((v, i) => `${(i / (hist.length - 1)) * w},${h - (v / max) * (h - 2) - 1}`).join(" ");
            return (
              <svg width={w} height={h} className="shrink-0">
                <polyline points={points} fill="none" stroke={rate > 0 ? "#22c55e" : isBright ? "#cbd5e1" : "#334155"} strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            );
          })()}
          <span className={`font-mono tabular-nums text-xs ${rate > 0 ? "text-emerald-500" : isBright ? "text-slate-400" : "text-slate-500"}`}>
            {rate > 0 ? `${rate.toFixed(0)}/s` : "idle"}
          </span>
        </div>
      );
    }},
    { key: "totalMessages", label: "Messages", className: "w-40 text-right", render: (r: Record<string, unknown>) => {
      const msgs = Number(r.totalMessages);
      const maxMsgs = Math.max(...topics.map(t => t.totalMessages || 0), 1);
      const barWidth = msgs > 0 ? (msgs / maxMsgs) * 100 : 0;
      return (
        <div className="flex items-center gap-2 justify-end">
          {msgs > 0 && (
            <div className={`w-16 h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
              <div className="h-full rounded-full bg-indigo-500/60" style={{ width: `${barWidth}%` }} />
            </div>
          )}
          <span className="font-mono tabular-nums">{fmt(msgs)}</span>
        </div>
      );
    }},
    { key: "estSize", label: "Est. Size", className: "w-28 text-right", render: (r: Record<string, unknown>) => {
      const msgs = Number(r.totalMessages || 0);
      const est = msgs * 512; // Rough estimate: 512 bytes avg message size
      return <span className={`font-mono tabular-nums text-xs ${isBright ? "text-slate-500" : "text-slate-400"}`}>{fmtBytes(est)}</span>;
    }},
    { key: "_consumers", label: "Consumers", className: "w-24 text-center", render: (r: Record<string, unknown>) => {
      const name = String(r.name);
      const count = consumerGroups.filter((g) => g.topics.includes(name)).length;
      return (
        <span className={`font-mono tabular-nums text-xs ${count > 0 ? (isBright ? "text-amber-600" : "text-amber-300") : isBright ? "text-slate-400" : "text-slate-500"}`}>
          {count > 0 ? count : "-"}
        </span>
      );
    }},
    { key: "_actions", label: "", sortable: false, className: "w-16 text-right", render: (r: Record<string, unknown>) => (
      <button
        onClick={(e) => { e.stopPropagation(); setShowDelete(String(r.name)); }}
        className="text-xs text-red-400/50 hover:text-red-500 transition-colors cursor-pointer"
      >
        Delete
      </button>
    )},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Topics</h1>
          <div className="flex items-center gap-3 mt-0.5">
            <p className={`text-sm ${isBright ? "text-slate-500" : "text-slate-500"}`}>Manage Kafka topics, inspect messages, and produce events</p>
            <FreshnessIndicator timestamp={topicsLastFetched} />
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoRefresh(!autoRefresh)}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              autoRefresh
                ? isBright
                  ? "bg-emerald-50 border-emerald-200/60 text-emerald-700"
                  : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300"
                : isBright
                  ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                  : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
            }`}
            title={autoRefresh ? "Stop auto-refresh (5s)" : "Auto-refresh every 5s"}
          >
            {autoRefresh ? "Auto (5s)" : "Auto"}
          </button>
          <button
            onClick={() => fetchTopics()}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50 hover:text-slate-700"
                : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
            }`}
          >
            Refresh
          </button>
          <button
            onClick={() => setShowCreate(true)}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
            }`}
          >
            + Create Topic
          </button>
        </div>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-3">
        <SummaryCard label="Topics" value={String(topics.length)} color="indigo" bright={isBright} />
        <SummaryCard label="Total Partitions" value={String(totalPartitions)} color="slate" bright={isBright} />
        <SummaryCard label="Total Messages" value={fmt(totalMessages)} color="emerald" bright={isBright} />
        <SummaryCard label="Est. Size" value={totalMessages > 0 ? `~${fmtBytes(estimatedSize)}` : "—"} color="amber" bright={isBright} />
      </div>

      {/* Table */}
      {topicsLoading && topics.length === 0 ? (
        <SkeletonTable rows={8} cols={5} />
      ) : (
        <>
          {selectedTopics.length > 0 && (
            <div className={`flex items-center gap-3 px-4 py-2.5 rounded-xl border ${
              isBright ? "bg-slate-50 border-slate-200/60" : "bg-slate-900/30 border-slate-700/20"
            }`}>
              <span className={`text-xs font-medium ${isBright ? "text-slate-700" : "text-slate-300"}`}>
                {selectedTopics.length} topic{selectedTopics.length > 1 ? "s" : ""} selected
              </span>
              {selectedTopics.length >= 2 && selectedTopics.length <= 4 && (
                <button
                  onClick={() => setShowCompare(true)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
                    isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
                  }`}
                >
                  Compare ({selectedTopics.length})
                </button>
              )}
              <button
                onClick={() => {
                  const blob = new Blob([JSON.stringify(selectedTopics, null, 2)], { type: "application/json" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a"); a.href = url; a.download = "kafka-topics-export.json"; a.click(); URL.revokeObjectURL(url);
                }}
                className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
                }`}
              >
                Export JSON
              </button>
              <button
                onClick={() => setShowBulkDelete(true)}
                className="px-3 py-1.5 rounded-lg text-xs font-medium bg-red-500/20 border border-red-500/40 text-red-400 hover:bg-red-500/30 transition-colors cursor-pointer"
              >
                Delete Selected
              </button>
            </div>
          )}
          <DataTable
            columns={columns}
            data={topics as unknown as Record<string, unknown>[]}
            onRowClick={(row) => setSelectedTopicName(String(row.name))}
            searchPlaceholder="Filter topics..."
            searchKeys={["name"]}
            emptyMessage="No topics found"
            exportFilename="kafka-topics"
            selectionKey="name"
            onSelectionChange={setSelectedTopics}
          />
        </>
      )}

      {/* Create Modal */}
      <Modal title="Create Topic" open={showCreate} onClose={() => { setShowCreate(false); setError(null); }}>
        <div className="space-y-4">
          {error && <div className={`p-3 rounded-xl border text-sm ${isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"}`}>{error}</div>}

          {/* Template presets */}
          <div>
            <label className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Quick Template</label>
            <div className="flex gap-2 mt-1.5 flex-wrap">
              {([
                { label: "Standard", desc: "General purpose", partitions: "6", rf: "3", cleanup: "delete", retention: "604800000", minIsr: "2" },
                { label: "High Throughput", desc: "Many partitions, short retention", partitions: "24", rf: "2", cleanup: "delete", retention: "86400000", minIsr: "1" },
                { label: "Compacted", desc: "KTable / state store", partitions: "6", rf: "3", cleanup: "compact", retention: "-1", minIsr: "2" },
                { label: "Event Log", desc: "Append-only, long retention", partitions: "12", rf: "3", cleanup: "delete", retention: "-1", minIsr: "2" },
              ] as { label: string; desc: string; partitions: string; rf: string; cleanup: string; retention: string; minIsr: string }[]).map((tpl) => (
                <button
                  key={tpl.label}
                  onClick={() => setCreateForm({ ...createForm, partitions: tpl.partitions, replicationFactor: tpl.rf, cleanupPolicy: tpl.cleanup, retentionMs: tpl.retention, minInsyncReplicas: tpl.minIsr })}
                  className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
                    createForm.partitions === tpl.partitions && createForm.replicationFactor === tpl.rf && createForm.cleanupPolicy === tpl.cleanup
                      ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/20 border-indigo-500/30 text-indigo-300"
                      : isBright ? "bg-white border-slate-200/60 text-slate-600 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                  }`}
                  title={tpl.desc}
                >
                  {tpl.label}
                </button>
              ))}
            </div>
          </div>

          <div>
            <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Topic Name</label>
            <input
              type="text"
              value={createForm.name}
              onChange={(e) => setCreateForm({ ...createForm, name: e.target.value })}
              className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none focus:border-indigo-500/50 ${
                isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
              }`}
              placeholder="my-topic"
              autoFocus
            />
            {createForm.name && !/^[a-zA-Z0-9._-]+$/.test(createForm.name) && (
              <p className={`text-[10px] mt-1 ${isBright ? "text-amber-600" : "text-amber-400"}`}>
                Topic names should only contain letters, numbers, dots, hyphens, and underscores
              </p>
            )}
            {createForm.name && topics.some((t) => t.name === createForm.name) && (
              <p className={`text-[10px] mt-1 ${isBright ? "text-red-600" : "text-red-400"}`}>
                A topic with this name already exists
              </p>
            )}
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Partitions</label>
              <input
                type="number"
                value={createForm.partitions}
                onChange={(e) => setCreateForm({ ...createForm, partitions: e.target.value })}
                className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none focus:border-indigo-500/50 ${
                  isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
                }`}
                min="1"
              />
              {Number(createForm.partitions) > 100 && (
                <p className={`text-[10px] mt-1 ${isBright ? "text-amber-600" : "text-amber-400"}`}>High partition count can increase broker overhead</p>
              )}
            </div>
            <div>
              <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Replication Factor</label>
              <input
                type="number"
                value={createForm.replicationFactor}
                onChange={(e) => setCreateForm({ ...createForm, replicationFactor: e.target.value })}
                className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none focus:border-indigo-500/50 ${
                  isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
                }`}
                min="1"
              />
              {Number(createForm.replicationFactor) < 2 && (
                <p className={`text-[10px] mt-1 ${isBright ? "text-amber-600" : "text-amber-400"}`}>RF=1 means no fault tolerance</p>
              )}
            </div>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Cleanup Policy</label>
              <div className="flex gap-1.5 mt-1.5">
                {["delete", "compact", "compact,delete"].map((policy) => (
                  <button
                    key={policy}
                    onClick={() => setCreateForm({ ...createForm, cleanupPolicy: policy })}
                    className={`px-2.5 py-1.5 rounded-lg text-[11px] font-medium border transition-all cursor-pointer flex-1 ${
                      createForm.cleanupPolicy === policy
                        ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/20 border-indigo-500/30 text-indigo-300"
                        : isBright ? "bg-white border-slate-200/60 text-slate-600 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                    }`}
                  >
                    {policy}
                  </button>
                ))}
              </div>
            </div>
            <div>
              <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Min In-Sync Replicas</label>
              <input
                type="number"
                value={createForm.minInsyncReplicas}
                onChange={(e) => setCreateForm({ ...createForm, minInsyncReplicas: e.target.value })}
                className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none focus:border-indigo-500/50 ${
                  isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
                }`}
                min="1"
              />
            </div>
          </div>
          <div>
            <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Retention</label>
            <div className="flex gap-1.5 mt-1.5">
              {[
                { label: "Forever", value: "-1" },
                { label: "1 Hour", value: "3600000" },
                { label: "1 Day", value: "86400000" },
                { label: "7 Days", value: "604800000" },
                { label: "30 Days", value: "2592000000" },
              ].map((r) => (
                <button
                  key={r.value}
                  onClick={() => setCreateForm({ ...createForm, retentionMs: r.value })}
                  className={`px-2 py-1.5 rounded-lg text-[10px] font-medium border transition-all cursor-pointer flex-1 ${
                    createForm.retentionMs === r.value
                      ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/20 border-indigo-500/30 text-indigo-300"
                      : isBright ? "bg-white border-slate-200/60 text-slate-600 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                  }`}
                >
                  {r.label}
                </button>
              ))}
            </div>
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => { setShowCreate(false); setError(null); }} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}>Cancel</button>
            <button onClick={handleCreate} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
            }`}>Create</button>
          </div>
        </div>
      </Modal>

      {/* Bulk Delete Confirm Modal */}
      <Modal title="Delete Selected Topics" open={showBulkDelete} onClose={() => setShowBulkDelete(false)}>
        <div className="space-y-4">
          <p className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>
            Are you sure you want to delete <span className="font-bold">{selectedTopics.length}</span> topic{selectedTopics.length > 1 ? "s" : ""}? This action cannot be undone.
          </p>
          <div className={`max-h-40 overflow-y-auto rounded-xl border p-2 space-y-1 ${
            isBright ? "border-slate-200 bg-slate-50" : "border-slate-700/50 bg-slate-800/30"
          }`}>
            {selectedTopics.map((t) => (
              <div key={String(t.name)} className={`text-xs font-mono px-2 py-1 rounded ${isBright ? "text-red-600" : "text-red-300"}`}>
                {String(t.name)}
              </div>
            ))}
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => setShowBulkDelete(false)} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}>Cancel</button>
            <button onClick={handleBulkDelete} disabled={bulkDeleting} className="px-4 py-2 rounded-xl text-sm font-medium bg-red-500/20 border border-red-500/40 text-red-400 hover:bg-red-500/30 transition-colors cursor-pointer disabled:opacity-40">
              {bulkDeleting ? "Deleting..." : `Delete ${selectedTopics.length} Topic${selectedTopics.length > 1 ? "s" : ""}`}
            </button>
          </div>
        </div>
      </Modal>

      {/* Compare Modal */}
      {showCompare && selectedTopics.length >= 2 && (
        <TopicCompare
          topics={selectedTopics}
          consumerGroups={consumerGroups}
          bright={isBright}
          onClose={() => setShowCompare(false)}
        />
      )}

      {/* Delete Confirm Modal with Impact Analysis */}
      <Modal title="Delete Topic" open={!!showDelete} onClose={() => { setShowDelete(null); setError(null); }}>
        {(() => {
          const topicInfo = topics.find((t) => t.name === showDelete);
          const affectedGroups = consumerGroups.filter((g) => g.topics.includes(showDelete || ""));
          const msgCount = topicInfo?.totalMessages || 0;
          const hasImpact = affectedGroups.length > 0 || msgCount > 0;
          return (
            <div className="space-y-4">
              {error && <div className={`p-3 rounded-xl border text-sm ${isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"}`}>{error}</div>}
              <p className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                Are you sure you want to delete topic <span className={`font-mono font-medium ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{showDelete}</span>? This action cannot be undone.
              </p>

              {/* Impact analysis */}
              {hasImpact && (
                <div className={`rounded-xl border p-3 space-y-2 ${isBright ? "bg-amber-50/50 border-amber-200/60" : "bg-amber-950/20 border-amber-500/20"}`}>
                  <div className={`text-[10px] uppercase tracking-wider font-bold ${isBright ? "text-amber-600" : "text-amber-400"}`}>Impact Analysis</div>
                  <div className="grid grid-cols-3 gap-2">
                    <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Messages</div>
                      <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>{fmt(msgCount)}</div>
                    </div>
                    <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Partitions</div>
                      <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>{topicInfo?.partitions || 0}</div>
                    </div>
                    <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
                      <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Consumers</div>
                      <div className={`text-sm font-bold font-mono ${affectedGroups.length > 0 ? "text-red-500" : isBright ? "text-slate-700" : "text-white"}`}>{affectedGroups.length}</div>
                    </div>
                  </div>
                  {affectedGroups.length > 0 && (
                    <div>
                      <div className={`text-[10px] mb-1 ${isBright ? "text-amber-600" : "text-amber-400"}`}>Affected consumer groups:</div>
                      <div className="flex flex-wrap gap-1">
                        {affectedGroups.map((g) => (
                          <span key={g.groupId} className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                            isBright ? "bg-white border-slate-200 text-slate-600" : "bg-slate-800/60 border-slate-700/40 text-slate-300"
                          }`}>{g.groupId}</span>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}

              <div className="flex justify-end gap-2 pt-2">
                <button onClick={() => { setShowDelete(null); setError(null); }} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
                }`}>Cancel</button>
                <button onClick={handleDelete} className="px-4 py-2 rounded-xl text-sm font-medium bg-red-500/20 border border-red-500/40 text-red-400 hover:bg-red-500/30 transition-colors cursor-pointer">Delete</button>
              </div>
            </div>
          );
        })()}
      </Modal>
    </div>
  );
}

function TopicCompare({ topics, consumerGroups, bright, onClose }: { topics: Record<string, unknown>[]; consumerGroups: { groupId: string; topics: string[] }[]; bright: boolean; onClose: () => void }) {
  const gridCls = `grid gap-2` + (topics.length === 2 ? " grid-cols-3" : topics.length === 3 ? " grid-cols-4" : " grid-cols-5");
  const colors = ["bg-indigo-500", "bg-cyan-500", "bg-amber-500", "bg-emerald-500"];

  const metricRows = [
    { label: "Partitions", values: topics.map((t) => String(t.partitions || 0)) },
    { label: "Replication", values: topics.map((t) => String(t.replicationFactor || 1)) },
    { label: "Messages", values: topics.map((t) => fmt(Number(t.totalMessages || 0))) },
    { label: "Rate", values: topics.map((t) => Number(t.msgPerSec || 0) > 0 ? `${Number(t.msgPerSec).toFixed(0)}/s` : "idle") },
    { label: "Est. Size", values: topics.map((t) => fmtBytes(Number(t.totalMessages || 0) * 1024)) },
    { label: "Consumers", values: topics.map((t) => String(consumerGroups.filter((g) => g.topics.includes(String(t.name))).length)) },
  ];

  const maxMsgs = Math.max(...topics.map((t) => Number(t.totalMessages || 0)), 1);

  return (
    <Modal title={`Compare ${topics.length} Topics`} open onClose={onClose}>
      <div className="space-y-4">
        {/* Headers */}
        <div className={gridCls}>
          <div />
          {topics.map((t) => (
            <div key={String(t.name)} className={`text-center font-mono text-[10px] font-bold truncate px-1 ${bright ? "text-indigo-600" : "text-indigo-300"}`} title={String(t.name)}>
              {String(t.name)}
            </div>
          ))}
        </div>
        {/* Metric rows */}
        {metricRows.map((row) => {
          const allSame = new Set(row.values).size === 1;
          return (
            <div key={row.label} className={`${gridCls} py-2 px-3 rounded-xl ${
              !allSame
                ? bright ? "bg-amber-50/50" : "bg-amber-500/[0.04]"
                : bright ? "bg-slate-50/50" : "bg-slate-800/20"
            }`}>
              <div className={`text-xs font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>{row.label}</div>
              {row.values.map((v, i) => (
                <div key={i} className={`text-xs font-mono text-center tabular-nums ${bright ? "text-slate-700" : "text-slate-300"}`}>{v}</div>
              ))}
            </div>
          );
        })}
        {/* Bar chart */}
        <div className={`rounded-xl border p-3 space-y-2 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className={`text-[10px] uppercase tracking-wider font-medium ${bright ? "text-slate-400" : "text-slate-500"}`}>Message Volume</div>
          <div className="space-y-1.5">
            {topics.map((t, i) => {
              const msgs = Number(t.totalMessages || 0);
              return (
                <div key={String(t.name)} className="flex items-center gap-2">
                  <span className={`text-[10px] font-mono w-20 truncate ${bright ? "text-slate-500" : "text-slate-400"}`}>{String(t.name)}</span>
                  <div className={`flex-1 h-3 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                    <div className={`h-full rounded-full ${colors[i % colors.length]} transition-all`} style={{ width: `${(msgs / maxMsgs) * 100}%` }} />
                  </div>
                  <span className={`text-[10px] font-mono tabular-nums w-14 text-right ${bright ? "text-slate-500" : "text-slate-400"}`}>{fmt(msgs)}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </Modal>
  );
}

function SummaryCard({ label, value, color, bright }: { label: string; value: string; color: string; bright: boolean }) {
  const darkColorMap: Record<string, string> = {
    indigo: "border-indigo-500/20 from-indigo-500/[0.06]",
    amber: "border-amber-500/20 from-amber-500/[0.06]",
    emerald: "border-emerald-500/20 from-emerald-500/[0.06]",
    slate: "border-slate-700/30 from-slate-500/[0.04]",
  };
  const brightColorMap: Record<string, string> = {
    indigo: "border-indigo-200/60 from-indigo-50",
    amber: "border-amber-200/60 from-amber-50",
    emerald: "border-emerald-200/60 from-emerald-50",
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
