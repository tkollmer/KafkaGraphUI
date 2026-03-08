import { useEffect, useState, useCallback } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { DataTable } from "../components/DataTable";
import { MessageInspector } from "../panels/MessageInspector";

interface Props {
  topicName: string;
  onBack: () => void;
}

type Tab = "partitions" | "config" | "messages" | "produce";

export function TopicDetail({ topicName, onBack }: Props) {
  const { selectedTopic, topicDetailLoading, fetchTopicDetail, produceMessage } = useKafkaStore();
  const [activeTab, setActiveTab] = useState<Tab>("partitions");
  const [produceForm, setProduceForm] = useState({ key: "", value: "", headers: "" });
  const [produceResult, setProduceResult] = useState<{ success: boolean; message: string } | null>(null);
  const [producing, setProducing] = useState(false);

  useEffect(() => { fetchTopicDetail(topicName); }, [topicName, fetchTopicDetail]);

  const handleProduce = useCallback(async () => {
    setProducing(true);
    setProduceResult(null);
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
    const result = await produceMessage(topicName, produceForm.value, produceForm.key || undefined, headers);
    if (result.success) {
      setProduceResult({ success: true, message: `Sent to partition ${result.partition} at offset ${result.offset}` });
      setProduceForm({ key: "", value: "", headers: "" });
    } else {
      setProduceResult({ success: false, message: result.error || "Failed" });
    }
    setProducing(false);
  }, [topicName, produceForm, produceMessage]);

  const tabs: { id: Tab; label: string }[] = [
    { id: "partitions", label: "Partitions" },
    { id: "config", label: "Config" },
    { id: "messages", label: "Messages" },
    { id: "produce", label: "Produce" },
  ];

  const partitionColumns = [
    { key: "partition", label: "Partition", className: "w-24 text-center" },
    { key: "leader", label: "Leader", className: "w-24 text-center" },
    { key: "replicas", label: "Replicas", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-xs">{(r.replicas as number[])?.join(", ") || "-"}</span>
    )},
    { key: "isr", label: "ISR", render: (r: Record<string, unknown>) => (
      <span className="font-mono text-xs">{(r.isr as number[])?.join(", ") || "-"}</span>
    )},
    { key: "endOffset", label: "End Offset", className: "w-32 text-right", render: (r: Record<string, unknown>) => (
      <span className="font-mono tabular-nums">{Number(r.endOffset).toLocaleString()}</span>
    )},
  ];

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
        <div>
          <div className="text-[10px] uppercase tracking-wider text-indigo-400/70 font-medium">Topic</div>
          <h1 className="text-xl font-bold text-white font-mono">{topicName}</h1>
        </div>
      </div>

      {topicDetailLoading && !selectedTopic ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <>
          {/* Tabs */}
          <div className="flex gap-1 bg-slate-900/50 rounded-xl p-1 w-fit border border-slate-800/50">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
                  activeTab === tab.id
                    ? "bg-indigo-500/20 text-indigo-300 border border-indigo-500/30"
                    : "text-slate-400 hover:text-slate-300 border border-transparent"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {/* Partitions */}
          {activeTab === "partitions" && selectedTopic && (
            <DataTable
              columns={partitionColumns}
              data={selectedTopic.partitions as unknown as Record<string, unknown>[]}
              searchPlaceholder="Filter partitions..."
              emptyMessage="No partition data"
            />
          )}

          {/* Config */}
          {activeTab === "config" && selectedTopic && (
            <div className="rounded-2xl border border-slate-700/40 bg-slate-900/40 overflow-hidden">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-700/40">
                    <th className="px-5 py-3.5 text-left text-[10px] uppercase tracking-wider font-semibold text-slate-400">Key</th>
                    <th className="px-5 py-3.5 text-left text-[10px] uppercase tracking-wider font-semibold text-slate-400">Value</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(selectedTopic.config).map(([k, v], i) => (
                    <tr key={k} className={`border-b border-slate-800/30 last:border-0 ${i % 2 === 1 ? "bg-slate-800/[0.08]" : ""}`}>
                      <td className="px-5 py-3 text-sm font-mono text-slate-300">{k}</td>
                      <td className="px-5 py-3 text-sm font-mono text-indigo-300">{v}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Messages */}
          {activeTab === "messages" && (
            <div className="rounded-2xl border border-slate-700/40 bg-slate-900/40 overflow-hidden">
              <MessageInspector topic={topicName} onClose={() => setActiveTab("partitions")} embedded />
            </div>
          )}

          {/* Produce */}
          {activeTab === "produce" && (
            <div className="max-w-xl rounded-2xl border border-slate-700/40 bg-slate-900/40 p-6 space-y-5">
              <div>
                <h3 className="text-sm font-semibold text-white mb-1">Send a message</h3>
                <p className="text-xs text-slate-500">Produce a message to <span className="font-mono text-indigo-300">{topicName}</span></p>
              </div>

              {produceResult && (
                <div className={`p-3 rounded-xl border text-sm ${
                  produceResult.success
                    ? "bg-emerald-950/50 border-emerald-500/30 text-emerald-300"
                    : "bg-red-950/50 border-red-500/30 text-red-300"
                }`}>
                  {produceResult.message}
                </div>
              )}

              <div>
                <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Key (optional)</label>
                <input
                  type="text"
                  value={produceForm.key}
                  onChange={(e) => setProduceForm({ ...produceForm, key: e.target.value })}
                  className="w-full mt-1.5 bg-slate-800/80 rounded-xl px-3.5 py-2.5 border border-slate-700/50 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500/50"
                  placeholder="message-key"
                />
              </div>
              <div>
                <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Value</label>
                <textarea
                  value={produceForm.value}
                  onChange={(e) => setProduceForm({ ...produceForm, value: e.target.value })}
                  className="w-full mt-1.5 bg-slate-800/80 rounded-xl px-3.5 py-2.5 border border-slate-700/50 text-sm text-white font-mono placeholder-slate-500 focus:outline-none focus:border-indigo-500/50 min-h-[120px] resize-y"
                  placeholder='{"event": "test"}'
                />
              </div>
              <div>
                <label className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Headers (JSON, optional)</label>
                <input
                  type="text"
                  value={produceForm.headers}
                  onChange={(e) => setProduceForm({ ...produceForm, headers: e.target.value })}
                  className="w-full mt-1.5 bg-slate-800/80 rounded-xl px-3.5 py-2.5 border border-slate-700/50 text-sm text-white font-mono placeholder-slate-500 focus:outline-none focus:border-indigo-500/50"
                  placeholder='{"content-type": "application/json"}'
                />
              </div>
              <button
                onClick={handleProduce}
                disabled={producing || !produceForm.value.trim()}
                className="px-5 py-2.5 rounded-xl text-sm font-medium bg-indigo-500/15 border border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25 transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed"
              >
                {producing ? "Sending..." : "Send Message"}
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
