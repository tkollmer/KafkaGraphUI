import { useEffect, useState } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { DataTable } from "../components/DataTable";
import { Modal } from "../components/Modal";
import { TopicDetail } from "./TopicDetail";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function TopicsView() {
  const { topics, topicsLoading, fetchTopics, createTopic, deleteTopic } = useKafkaStore();
  const [selectedTopicName, setSelectedTopicName] = useState<string | null>(null);
  const [showCreate, setShowCreate] = useState(false);
  const [showDelete, setShowDelete] = useState<string | null>(null);
  const [createForm, setCreateForm] = useState({ name: "", partitions: "1", replicationFactor: "1" });
  const [error, setError] = useState<string | null>(null);

  useEffect(() => { fetchTopics(); }, [fetchTopics]);

  if (selectedTopicName) {
    return <TopicDetail topicName={selectedTopicName} onBack={() => setSelectedTopicName(null)} />;
  }

  const handleCreate = async () => {
    setError(null);
    if (!createForm.name.trim()) { setError("Topic name is required"); return; }
    const result = await createTopic(createForm.name, Number(createForm.partitions), Number(createForm.replicationFactor));
    if (result.success) {
      setShowCreate(false);
      setCreateForm({ name: "", partitions: "1", replicationFactor: "1" });
      fetchTopics();
    } else {
      setError(result.error || "Failed to create topic");
    }
  };

  const handleDelete = async () => {
    if (!showDelete) return;
    const result = await deleteTopic(showDelete);
    if (result.success) {
      setShowDelete(null);
      fetchTopics();
    } else {
      setError(result.error || "Failed to delete topic");
    }
  };

  const columns = [
    { key: "name", label: "Topic Name", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-indigo-300">{String(r.name)}</span>
    )},
    { key: "partitions", label: "Partitions", className: "w-28 text-center" },
    { key: "replicationFactor", label: "RF", className: "w-20 text-center" },
    { key: "totalMessages", label: "Messages", className: "w-32 text-right", render: (r: Record<string, unknown>) => (
      <span className="font-mono">{fmt(Number(r.totalMessages))}</span>
    )},
    { key: "_actions", label: "", sortable: false, className: "w-20", render: (r: Record<string, unknown>) => (
      <button
        onClick={(e) => { e.stopPropagation(); setShowDelete(String(r.name)); }}
        className="text-xs text-red-400/60 hover:text-red-400 transition-colors cursor-pointer"
      >
        Delete
      </button>
    )},
  ];

  return (
    <div className="p-6 flex-1 overflow-y-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-xl font-bold text-white">Topics</h1>
          <p className="text-sm text-slate-400 mt-1">{topics.length} topics</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => fetchTopics()}
            className="px-3 py-1.5 rounded-xl text-xs font-medium bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300 transition-all cursor-pointer"
          >
            Refresh
          </button>
          <button
            onClick={() => setShowCreate(true)}
            className="px-3 py-1.5 rounded-xl text-xs font-medium bg-indigo-500/20 border border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30 transition-all cursor-pointer"
          >
            Create Topic
          </button>
        </div>
      </div>

      {topicsLoading && topics.length === 0 ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <DataTable
          columns={columns}
          data={topics as unknown as Record<string, unknown>[]}
          onRowClick={(row) => setSelectedTopicName(String(row.name))}
          searchPlaceholder="Filter topics..."
          searchKeys={["name"]}
          emptyMessage="No topics found"
        />
      )}

      {/* Create Modal */}
      <Modal title="Create Topic" open={showCreate} onClose={() => { setShowCreate(false); setError(null); }}>
        <div className="space-y-4">
          {error && <div className="p-3 rounded-xl bg-red-950/50 border border-red-500/30 text-red-300 text-sm">{error}</div>}
          <div>
            <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Topic Name</label>
            <input
              type="text"
              value={createForm.name}
              onChange={(e) => setCreateForm({ ...createForm, name: e.target.value })}
              className="w-full mt-1 bg-slate-800/80 rounded-xl px-3 py-2 border border-slate-700/50 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500/50"
              placeholder="my-topic"
            />
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Partitions</label>
              <input
                type="number"
                value={createForm.partitions}
                onChange={(e) => setCreateForm({ ...createForm, partitions: e.target.value })}
                className="w-full mt-1 bg-slate-800/80 rounded-xl px-3 py-2 border border-slate-700/50 text-sm text-white focus:outline-none focus:border-indigo-500/50"
                min="1"
              />
            </div>
            <div>
              <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Replication Factor</label>
              <input
                type="number"
                value={createForm.replicationFactor}
                onChange={(e) => setCreateForm({ ...createForm, replicationFactor: e.target.value })}
                className="w-full mt-1 bg-slate-800/80 rounded-xl px-3 py-2 border border-slate-700/50 text-sm text-white focus:outline-none focus:border-indigo-500/50"
                min="1"
              />
            </div>
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button
              onClick={() => { setShowCreate(false); setError(null); }}
              className="px-4 py-2 rounded-xl text-sm font-medium bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 transition-colors cursor-pointer"
            >
              Cancel
            </button>
            <button
              onClick={handleCreate}
              className="px-4 py-2 rounded-xl text-sm font-medium bg-indigo-500/20 border border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30 transition-colors cursor-pointer"
            >
              Create
            </button>
          </div>
        </div>
      </Modal>

      {/* Delete Confirm Modal */}
      <Modal title="Delete Topic" open={!!showDelete} onClose={() => { setShowDelete(null); setError(null); }}>
        <div className="space-y-4">
          {error && <div className="p-3 rounded-xl bg-red-950/50 border border-red-500/30 text-red-300 text-sm">{error}</div>}
          <p className="text-sm text-slate-300">
            Are you sure you want to delete topic <span className="font-mono text-indigo-300">{showDelete}</span>? This action cannot be undone.
          </p>
          <div className="flex justify-end gap-2 pt-2">
            <button
              onClick={() => { setShowDelete(null); setError(null); }}
              className="px-4 py-2 rounded-xl text-sm font-medium bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 transition-colors cursor-pointer"
            >
              Cancel
            </button>
            <button
              onClick={handleDelete}
              className="px-4 py-2 rounded-xl text-sm font-medium bg-red-500/20 border border-red-500/40 text-red-300 hover:bg-red-500/30 transition-colors cursor-pointer"
            >
              Delete
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
