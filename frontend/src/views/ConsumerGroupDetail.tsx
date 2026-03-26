import { useEffect, useState, useRef, useMemo } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { useThemeStore } from "../store/themeStore";
import { DataTable } from "../components/DataTable";
import { Modal } from "../components/Modal";

interface Props {
  groupId: string;
  onBack: () => void;
}

type Tab = "members" | "offsets" | "lag" | "rebalances" | "timeline" | "heatmap" | "lag-trend";

interface RebalanceEvent {
  timestamp: number;
  type: "member_join" | "member_leave" | "partition_reassign" | "state_change";
  description: string;
  membersBefore: number;
  membersAfter: number;
  state: string;
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function ConsumerGroupDetail({ groupId, onBack }: Props) {
  const { selectedConsumerGroup, consumerGroupDetailLoading, fetchConsumerGroupDetail, resetOffsets, deleteConsumerGroup } = useKafkaStore();
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const [activeTab, setActiveTab] = useState<Tab>("members");
  const [showReset, setShowReset] = useState(false);
  const [showDelete, setShowDelete] = useState(false);
  const [resetStrategy, setResetStrategy] = useState("latest");
  const [resetTopic, setResetTopic] = useState("");
  const [resetTimestamp, setResetTimestamp] = useState("");
  const [resetOffset, setResetOffset] = useState("");
  const [resetResult, setResetResult] = useState<{ success: boolean; message: string } | null>(null);
  const [resetting, setResetting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  useEffect(() => { fetchConsumerGroupDetail(groupId); }, [groupId, fetchConsumerGroupDetail]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(() => fetchConsumerGroupDetail(groupId), 5000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, groupId, fetchConsumerGroupDetail]);

  const handleReset = async () => {
    setResetting(true);
    setResetResult(null);
    const ts = resetStrategy === "timestamp" && resetTimestamp ? new Date(resetTimestamp).getTime() : undefined;
    const off = resetStrategy === "specific" && resetOffset ? parseInt(resetOffset, 10) : undefined;
    if (resetStrategy === "timestamp" && !ts) {
      setResetResult({ success: false, message: "Please select a valid timestamp" });
      setResetting(false);
      return;
    }
    if (resetStrategy === "specific" && (off === undefined || isNaN(off) || off < 0)) {
      setResetResult({ success: false, message: "Please enter a valid offset number" });
      setResetting(false);
      return;
    }
    const result = await resetOffsets(groupId, resetStrategy, resetTopic || undefined, ts, off);
    if (result.success) {
      setResetResult({ success: true, message: "Offsets reset successfully" });
      fetchConsumerGroupDetail(groupId);
    } else {
      setResetResult({ success: false, message: result.error || "Failed to reset offsets" });
    }
    setResetting(false);
  };

  const handleDelete = async () => {
    setDeleteError(null);
    const result = await deleteConsumerGroup(groupId);
    if (result.success) {
      onBack();
    } else {
      setDeleteError(result.error || "Failed to delete consumer group");
    }
  };

  // Compute per-member lag by matching assigned partitions to offset data
  const memberLagMap = useMemo(() => {
    if (!selectedConsumerGroup) return new Map<string, number>();
    const map = new Map<string, number>();
    for (const m of selectedConsumerGroup.members) {
      let totalLag = 0;
      for (const pStr of m.partitions) {
        // pStr is like "topic-0" or "topic:0" depending on format
        const match = selectedConsumerGroup.offsets.find((o) => {
          const key1 = `${o.topic}-${o.partition}`;
          const key2 = `${o.topic}:${o.partition}`;
          return pStr === key1 || pStr === key2 || pStr === `${o.topic}/${o.partition}`;
        });
        if (match) totalLag += match.lag;
      }
      map.set(m.memberId, totalLag);
    }
    return map;
  }, [selectedConsumerGroup]);

  const memberColumns = [
    { key: "clientId", label: "Client ID", render: (r: Record<string, unknown>) => (
      <span className={`font-mono font-medium ${isBright ? "text-amber-600" : "text-amber-300"}`}>{String(r.clientId)}</span>
    )},
    { key: "clientHost", label: "Host", render: (r: Record<string, unknown>) => (
      <span className={`font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>{String(r.clientHost)}</span>
    )},
    { key: "partitions", label: "Assigned Partitions", render: (r: Record<string, unknown>) => {
      const parts = r.partitions as string[];
      if (!parts || parts.length === 0) return <span className={isBright ? "text-slate-400" : "text-slate-500"}>-</span>;
      return (
        <div className="flex gap-1 flex-wrap">
          {parts.slice(0, 5).map((p) => (
            <span key={p} className={`text-[9px] rounded-md px-1.5 py-0.5 font-mono border ${
              isBright ? "bg-slate-100 text-slate-600 border-slate-200" : "bg-slate-800/60 text-slate-300 border-slate-700/30"
            }`}>{p}</span>
          ))}
          {parts.length > 5 && <span className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>+{parts.length - 5}</span>}
        </div>
      );
    }},
    { key: "_lag", label: "Lag", className: "w-28 text-right", render: (r: Record<string, unknown>) => {
      const lag = memberLagMap.get(String(r.memberId)) || 0;
      const color = lag > 1000 ? "text-red-500" : lag > 0 ? "text-amber-500" : "text-emerald-500";
      return <span className={`font-mono font-bold tabular-nums ${color}`}>{fmt(lag)}</span>;
    }},
    { key: "_partCount", label: "# Parts", className: "w-20 text-center", render: (r: Record<string, unknown>) => {
      const parts = r.partitions as string[];
      return <span className={`font-mono tabular-nums ${isBright ? "text-slate-600" : "text-slate-300"}`}>{parts?.length || 0}</span>;
    }},
  ];

  const offsetColumns = [
    { key: "topic", label: "Topic", render: (r: Record<string, unknown>) => (
      <span className={`font-mono font-medium ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{String(r.topic)}</span>
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
      const color = lag > 1000 ? "text-red-500" : lag > 0 ? "text-amber-500" : "text-emerald-500";
      return <span className={`font-mono font-bold tabular-nums ${color}`}>{fmt(lag)}</span>;
    }},
  ];

  const tabs: { id: Tab; label: string }[] = [
    { id: "members", label: "Members" },
    { id: "offsets", label: "Offsets" },
    { id: "lag", label: "Lag Chart" },
    { id: "timeline", label: "Offset Timeline" },
    { id: "heatmap", label: "Partition Heatmap" },
    { id: "lag-trend", label: "Lag Trend" },
    { id: "rebalances", label: "Rebalances" },
  ];

  const [selectedTopicFilter, setSelectedTopicFilter] = useState<string | null>(null);

  const topicsList = selectedConsumerGroup
    ? [...new Set(selectedConsumerGroup.offsets.map((o) => o.topic))]
    : [];

  const filteredOffsets = useMemo(() => {
    if (!selectedConsumerGroup) return [];
    if (!selectedTopicFilter) return selectedConsumerGroup.offsets;
    return selectedConsumerGroup.offsets.filter((o) => o.topic === selectedTopicFilter);
  }, [selectedConsumerGroup, selectedTopicFilter]);

  // Summary stats
  const totalLag = selectedConsumerGroup?.offsets.reduce((s, o) => s + o.lag, 0) || 0;
  const memberCount = selectedConsumerGroup?.members.length || 0;
  const topicCount = topicsList.length;
  const partitionCount = selectedConsumerGroup?.offsets.length || 0;

  // Rebalance history tracking
  const [rebalanceHistory, setRebalanceHistory] = useState<RebalanceEvent[]>([]);
  const prevSnapshotRef = useRef<{ memberCount: number; state: string; assignmentKey: string } | null>(null);

  useEffect(() => {
    if (!selectedConsumerGroup) return;
    const memberCount = selectedConsumerGroup.members.length;
    const state = selectedConsumerGroup.state;
    // Create a fingerprint of partition assignments
    const assignmentKey = selectedConsumerGroup.members
      .map((m) => `${m.clientId}:${m.partitions.sort().join(",")}`)
      .sort()
      .join("|");

    const prev = prevSnapshotRef.current;
    if (prev) {
      const events: RebalanceEvent[] = [];
      if (prev.state !== state) {
        events.push({
          timestamp: Date.now(),
          type: "state_change",
          description: `State changed: ${prev.state} → ${state}`,
          membersBefore: prev.memberCount,
          membersAfter: memberCount,
          state,
        });
      }
      if (memberCount > prev.memberCount) {
        events.push({
          timestamp: Date.now(),
          type: "member_join",
          description: `${memberCount - prev.memberCount} member(s) joined (${prev.memberCount} → ${memberCount})`,
          membersBefore: prev.memberCount,
          membersAfter: memberCount,
          state,
        });
      } else if (memberCount < prev.memberCount) {
        events.push({
          timestamp: Date.now(),
          type: "member_leave",
          description: `${prev.memberCount - memberCount} member(s) left (${prev.memberCount} → ${memberCount})`,
          membersBefore: prev.memberCount,
          membersAfter: memberCount,
          state,
        });
      }
      if (prev.assignmentKey !== assignmentKey && memberCount === prev.memberCount && events.length === 0) {
        events.push({
          timestamp: Date.now(),
          type: "partition_reassign",
          description: `Partition reassignment detected (${memberCount} members)`,
          membersBefore: prev.memberCount,
          membersAfter: memberCount,
          state,
        });
      }
      if (events.length > 0) {
        setRebalanceHistory((h) => [...events, ...h].slice(0, 50));
      }
    }
    prevSnapshotRef.current = { memberCount, state, assignmentKey };
  }, [selectedConsumerGroup]);

  // State history tracking
  const [stateHistory, setStateHistory] = useState<{ ts: number; state: string }[]>([]);
  useEffect(() => {
    if (!selectedConsumerGroup) return;
    const state = selectedConsumerGroup.state;
    setStateHistory((prev) => {
      // Only add if state changed or first entry
      if (prev.length === 0 || prev[prev.length - 1].state !== state) {
        return [...prev, { ts: Date.now(), state }].slice(-30);
      }
      return prev;
    });
  }, [selectedConsumerGroup]);

  // Lag history tracking
  const [lagHistory, setLagHistory] = useState<number[]>([]);
  useEffect(() => {
    if (totalLag > 0 || lagHistory.length > 0) {
      setLagHistory((prev) => [...prev, totalLag].slice(-30));
    }
  }, [totalLag]); // eslint-disable-line react-hooks/exhaustive-deps

  // SLA monitoring: track lag samples and compliance against threshold
  const [lagThreshold, setLagThreshold] = useState(1000);
  const [lagSamples, setLagSamples] = useState<{ ts: number; lag: number; ok: boolean }[]>([]);
  useEffect(() => {
    if (!selectedConsumerGroup) return;
    setLagSamples((prev) => [
      ...prev,
      { ts: Date.now(), lag: totalLag, ok: totalLag <= lagThreshold },
    ].slice(-120));
  }, [selectedConsumerGroup, totalLag, lagThreshold]);

  // Alert rules
  const [alertRules, setAlertRules] = useState<{ id: number; name: string; threshold: number; window: number }[]>([
    { id: 1, name: "High Lag", threshold: 10000, window: 3 },
  ]);
  const [triggeredAlerts, setTriggeredAlerts] = useState<{ ruleId: number; name: string; ts: number; lag: number }[]>([]);
  const nextAlertId = useRef(2);

  // Evaluate alert rules
  useEffect(() => {
    if (!selectedConsumerGroup || lagSamples.length === 0) return;
    for (const rule of alertRules) {
      const recentSamples = lagSamples.slice(-rule.window);
      const allAbove = recentSamples.length >= rule.window && recentSamples.every((s) => s.lag > rule.threshold);
      if (allAbove) {
        // Only trigger if not already triggered in last 30s
        const lastTrigger = triggeredAlerts.find((a) => a.ruleId === rule.id);
        if (!lastTrigger || Date.now() - lastTrigger.ts > 30_000) {
          setTriggeredAlerts((prev) => [
            { ruleId: rule.id, name: rule.name, ts: Date.now(), lag: totalLag },
            ...prev,
          ].slice(0, 50));
        }
      }
    }
  }, [lagSamples]); // eslint-disable-line react-hooks/exhaustive-deps

  // Offset progress tracking per topic
  const [offsetSnapshots, setOffsetSnapshots] = useState<{ ts: number; offsets: Map<string, { current: number; end: number }> }[]>([]);
  useEffect(() => {
    if (!selectedConsumerGroup || selectedConsumerGroup.offsets.length === 0) return;
    const topicOffsets = new Map<string, { current: number; end: number }>();
    for (const o of selectedConsumerGroup.offsets) {
      const existing = topicOffsets.get(o.topic) || { current: 0, end: 0 };
      existing.current += o.currentOffset;
      existing.end += o.endOffset;
      topicOffsets.set(o.topic, existing);
    }
    setOffsetSnapshots((prev) => [...prev, { ts: Date.now(), offsets: topicOffsets }].slice(-20));
  }, [selectedConsumerGroup]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      <div className="flex items-center gap-4">
        <div className="flex-1">
          <nav className="flex items-center gap-1.5 mb-1">
            <button onClick={onBack} className={`text-[11px] font-medium transition-colors cursor-pointer hover:underline ${isBright ? "text-amber-600" : "text-amber-400"}`}>
              Consumer Groups
            </button>
            <span className={`text-[11px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>/</span>
            <span className={`text-[11px] font-mono font-medium truncate max-w-[300px] ${isBright ? "text-slate-600" : "text-slate-300"}`}>{groupId}</span>
          </nav>
          <h1 className={`text-2xl font-bold font-mono ${isBright ? "text-slate-800" : "text-white"}`}>{groupId}</h1>
        </div>
        {selectedConsumerGroup && (
          <div className="flex items-center gap-2">
            <span className={`text-[10px] font-semibold uppercase px-2.5 py-1 rounded-lg border ${
              selectedConsumerGroup.state === "Stable"
                ? isBright ? "bg-emerald-50 text-emerald-700 border-emerald-200" : "bg-emerald-500/15 text-emerald-400 border-emerald-500/25"
                : isBright ? "bg-slate-100 text-slate-500 border-slate-200" : "bg-slate-800/50 text-slate-400 border-slate-700/40"
            }`}>
              {selectedConsumerGroup.state}
            </span>
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
              onClick={() => fetchConsumerGroupDetail(groupId)}
              className={`px-3 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                isBright ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
              }`}
            >
              Refresh
            </button>
            <button
              onClick={() => {
                if (!selectedConsumerGroup) return;
                const data = { groupId: selectedConsumerGroup.groupId, state: selectedConsumerGroup.state, offsets: selectedConsumerGroup.offsets };
                const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a"); a.href = url; a.download = `${groupId}-offsets.json`; a.click(); URL.revokeObjectURL(url);
              }}
              className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
              }`}
            >
              Export Offsets
            </button>
            <button
              onClick={() => setShowReset(true)}
              className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                isBright ? "bg-amber-50 border-amber-200/60 text-amber-700 hover:bg-amber-100" : "bg-amber-500/15 border-amber-500/30 text-amber-300 hover:bg-amber-500/25"
              }`}
            >
              Reset Offsets
            </button>
            <button
              onClick={() => setShowDelete(true)}
              className="px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer text-red-400/70 hover:text-red-500 border-red-500/20 hover:border-red-500/40 hover:bg-red-500/10"
            >
              Delete
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
          {/* Summary cards */}
          <div className="grid grid-cols-5 gap-3">
            <SummaryCard label="Members" value={String(memberCount)} color="amber" bright={isBright} />
            <SummaryCard label="Topics" value={String(topicCount)} color="indigo" bright={isBright} />
            <SummaryCard label="Partitions" value={String(partitionCount)} color="slate" bright={isBright} />
            <SummaryCard label="Total Lag" value={fmt(totalLag)} color={totalLag > 1000 ? "red" : totalLag > 0 ? "amber" : "emerald"} bright={isBright} />
            {/* ETA to catch up */}
            {(() => {
              if (offsetSnapshots.length < 2 || totalLag === 0) return <SummaryCard label="ETA" value="--" color="slate" bright={isBright} />;
              const latest = offsetSnapshots[offsetSnapshots.length - 1];
              const prev = offsetSnapshots[offsetSnapshots.length - 2];
              const dtSec = Math.max((latest.ts - prev.ts) / 1000, 1);
              let totalConsumeRate = 0;
              for (const [topic, curr] of latest.offsets) {
                const prevData = prev.offsets.get(topic);
                if (prevData) totalConsumeRate += Math.max(0, (curr.current - prevData.current) / dtSec);
              }
              if (totalConsumeRate <= 0) return <SummaryCard label="ETA" value="stalled" color="red" bright={isBright} />;
              const etaSec = totalLag / totalConsumeRate;
              const etaStr = etaSec < 60 ? `${Math.round(etaSec)}s` : etaSec < 3600 ? `${Math.round(etaSec / 60)}m` : `${(etaSec / 3600).toFixed(1)}h`;
              return <SummaryCard label="ETA" value={etaStr} color={etaSec > 3600 ? "red" : etaSec > 300 ? "amber" : "emerald"} bright={isBright} />;
            })()}
          </div>

          {/* Lag trend chart */}
          {lagHistory.length > 1 && (
            <LagSparkline data={lagHistory} bright={isBright} />
          )}

          {/* State History */}
          {stateHistory.length > 1 && (
            <div className={`rounded-2xl border px-5 py-3 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
              <div className="flex items-center justify-between mb-2">
                <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                  State Timeline
                </span>
                <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                  {stateHistory.length} transitions
                </span>
              </div>
              <div className="flex items-center gap-0.5">
                {stateHistory.map((entry, i) => {
                  const nextTs = i < stateHistory.length - 1 ? stateHistory[i + 1].ts : Date.now();
                  const duration = nextTs - entry.ts;
                  const totalSpan = Date.now() - stateHistory[0].ts;
                  const widthPct = totalSpan > 0 ? Math.max(3, (duration / totalSpan) * 100) : 100;
                  const stateColor = entry.state === "Stable"
                    ? isBright ? "bg-emerald-400" : "bg-emerald-500/70"
                    : entry.state === "Empty"
                      ? isBright ? "bg-slate-300" : "bg-slate-600"
                      : entry.state === "Dead"
                        ? isBright ? "bg-red-400" : "bg-red-500/70"
                        : isBright ? "bg-amber-400" : "bg-amber-500/70";
                  const durStr = duration < 60_000 ? `${Math.round(duration / 1000)}s` : `${Math.round(duration / 60_000)}m`;
                  return (
                    <div
                      key={`${entry.ts}-${i}`}
                      className={`h-4 rounded-sm ${stateColor} transition-all`}
                      style={{ width: `${widthPct}%` }}
                      title={`${entry.state} for ${durStr}`}
                    />
                  );
                })}
              </div>
              <div className="flex justify-between mt-1">
                {stateHistory.slice(-4).map((entry, i) => (
                  <span key={`${entry.ts}-${i}`} className={`text-[8px] font-mono ${
                    entry.state === "Stable" ? "text-emerald-500" : entry.state === "Dead" ? "text-red-500" : isBright ? "text-slate-400" : "text-slate-500"
                  }`}>
                    {entry.state}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* SLA Compliance Monitor */}
          {lagSamples.length >= 2 && (
            <SlaMonitor samples={lagSamples} threshold={lagThreshold} onThresholdChange={setLagThreshold} bright={isBright} />
          )}

          {/* Alert Rules */}
          <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className="flex items-center justify-between mb-3">
              <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Alert Rules {triggeredAlerts.length > 0 && <span className="text-red-500 ml-1">({triggeredAlerts.length})</span>}
              </span>
              <button
                onClick={() => {
                  const id = nextAlertId.current++;
                  setAlertRules((prev) => [...prev, { id, name: `Rule ${id}`, threshold: 5000, window: 3 }]);
                }}
                className={`text-[10px] px-2 py-0.5 rounded-md border cursor-pointer transition-colors ${
                  isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
                }`}
              >
                + Add Rule
              </button>
            </div>
            <div className="space-y-2">
              {alertRules.map((rule) => (
                <div key={rule.id} className={`flex items-center gap-2 px-3 py-2 rounded-xl border ${
                  isBright ? "bg-slate-50/50 border-slate-200/40" : "bg-slate-800/20 border-slate-700/20"
                }`}>
                  <input
                    type="text"
                    value={rule.name}
                    onChange={(e) => setAlertRules((prev) => prev.map((r) => r.id === rule.id ? { ...r, name: e.target.value } : r))}
                    className={`text-[11px] font-medium bg-transparent border-none outline-none w-24 ${isBright ? "text-slate-700" : "text-slate-200"}`}
                  />
                  <span className={`text-[10px] shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>lag &gt;</span>
                  <input
                    type="number"
                    value={rule.threshold}
                    onChange={(e) => setAlertRules((prev) => prev.map((r) => r.id === rule.id ? { ...r, threshold: Number(e.target.value) } : r))}
                    className={`text-[11px] font-mono bg-transparent border-none outline-none w-16 text-right ${isBright ? "text-slate-700" : "text-slate-200"}`}
                  />
                  <span className={`text-[10px] shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>for</span>
                  <input
                    type="number"
                    value={rule.window}
                    onChange={(e) => setAlertRules((prev) => prev.map((r) => r.id === rule.id ? { ...r, window: Math.max(1, Number(e.target.value)) } : r))}
                    className={`text-[11px] font-mono bg-transparent border-none outline-none w-8 text-right ${isBright ? "text-slate-700" : "text-slate-200"}`}
                    min={1}
                  />
                  <span className={`text-[10px] shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>samples</span>
                  <button
                    onClick={() => setAlertRules((prev) => prev.filter((r) => r.id !== rule.id))}
                    className={`ml-auto text-[10px] px-1.5 py-0.5 rounded cursor-pointer ${isBright ? "text-red-400 hover:text-red-600" : "text-red-500/50 hover:text-red-400"}`}
                  >
                    x
                  </button>
                </div>
              ))}
            </div>
            {/* Triggered alerts */}
            {triggeredAlerts.length > 0 && (
              <div className="mt-3">
                <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-red-500" : "text-red-400"}`}>
                  Triggered ({triggeredAlerts.length})
                </div>
                <div className="space-y-1 max-h-28 overflow-y-auto">
                  {triggeredAlerts.map((a, i) => (
                    <div key={`${a.ts}-${i}`} className={`flex items-center justify-between text-[10px] px-2 py-1 rounded-lg ${
                      isBright ? "bg-red-50/50" : "bg-red-950/20"
                    }`}>
                      <span className={`font-medium ${isBright ? "text-red-600" : "text-red-400"}`}>{a.name}</span>
                      <span className={`font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                        lag {a.lag.toLocaleString()} at {new Date(a.ts).toLocaleTimeString()}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className={`flex gap-1 rounded-xl p-1 w-fit border ${isBright ? "bg-slate-100/50 border-slate-200/50" : "bg-slate-900/50 border-slate-800/50"}`}>
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-2 rounded-lg text-xs font-medium transition-all cursor-pointer ${
                  activeTab === tab.id
                    ? isBright
                      ? "bg-white text-amber-700 border border-amber-200/60 shadow-sm"
                      : "bg-amber-500/20 text-amber-300 border border-amber-500/30"
                    : isBright
                      ? "text-slate-500 hover:text-slate-700 border border-transparent"
                      : "text-slate-400 hover:text-slate-300 border border-transparent"
                }`}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {activeTab === "members" && (
            <>
              {/* Member assignment visualization */}
              {selectedConsumerGroup.members.length > 0 && (
                <MemberAssignmentGrid members={selectedConsumerGroup.members} bright={isBright} />
              )}
              {/* Member-to-topic ownership matrix */}
              {selectedConsumerGroup.members.length > 0 && (() => {
                const memberTopics: Record<string, Record<string, number>> = {};
                const allTopics = new Set<string>();
                selectedConsumerGroup.members.forEach((m) => {
                  memberTopics[m.clientId] = {};
                  m.partitions.forEach((p) => {
                    const parts = p.split("-");
                    parts.pop();
                    const topic = parts.join("-");
                    if (topic) {
                      allTopics.add(topic);
                      memberTopics[m.clientId][topic] = (memberTopics[m.clientId][topic] || 0) + 1;
                    }
                  });
                });
                const topics = [...allTopics].sort();
                if (topics.length === 0) return null;
                return (
                  <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
                    <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                      Member &times; Topic Assignment Matrix
                    </div>
                    <div className="overflow-x-auto">
                      <table className="text-[10px]">
                        <thead>
                          <tr>
                            <th className={`text-left px-2 py-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Member</th>
                            {topics.map((t) => (
                              <th key={t} className={`text-center px-1.5 py-1 font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`} title={t}>
                                {t.length > 12 ? t.slice(0, 11) + "..." : t}
                              </th>
                            ))}
                            <th className={`text-center px-2 py-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Total</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedConsumerGroup.members.map((m) => {
                            const total = m.partitions.length;
                            return (
                              <tr key={m.memberId} className={`border-t ${isBright ? "border-slate-100" : "border-slate-800/30"}`}>
                                <td className={`px-2 py-1 font-mono truncate max-w-[100px] ${isBright ? "text-slate-600" : "text-slate-300"}`}>{m.clientId}</td>
                                {topics.map((t) => {
                                  const count = memberTopics[m.clientId]?.[t] || 0;
                                  return (
                                    <td key={t} className="text-center px-1.5 py-1">
                                      {count > 0 ? (
                                        <span className={`inline-block min-w-[18px] px-1 py-0.5 rounded font-bold ${
                                          isBright ? "bg-indigo-50 text-indigo-600" : "bg-indigo-500/15 text-indigo-400"
                                        }`}>{count}</span>
                                      ) : (
                                        <span className={isBright ? "text-slate-300" : "text-slate-700"}>-</span>
                                      )}
                                    </td>
                                  );
                                })}
                                <td className={`text-center px-2 py-1 font-bold ${isBright ? "text-slate-700" : "text-white"}`}>{total}</td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                );
              })()}
              <DataTable
                columns={memberColumns}
                data={selectedConsumerGroup.members as unknown as Record<string, unknown>[]}
                searchPlaceholder="Filter members..."
                searchKeys={["clientId", "clientHost"]}
                emptyMessage="No active members"
              />
            </>
          )}

          {activeTab === "offsets" && (
            <>
              {/* Consumption velocity */}
              {offsetSnapshots.length >= 2 && (
                <ConsumptionVelocity snapshots={offsetSnapshots} bright={isBright} />
              )}
              {/* Per-topic lag breakdown */}
              {topicsList.length > 0 && (
                <TopicLagBreakdown offsets={selectedConsumerGroup.offsets} bright={isBright} />
              )}
              {/* Topic filter chips */}
              {topicsList.length > 1 && (
                <div className="flex items-center gap-2 flex-wrap">
                  <span className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Filter:</span>
                  <button
                    onClick={() => setSelectedTopicFilter(null)}
                    className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
                      !selectedTopicFilter
                        ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/20 border-indigo-500/30 text-indigo-300"
                        : isBright ? "bg-white border-slate-200/60 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                    }`}
                  >All ({selectedConsumerGroup.offsets.length})</button>
                  {topicsList.map((t) => {
                    const count = selectedConsumerGroup.offsets.filter((o) => o.topic === t).length;
                    const active = selectedTopicFilter === t;
                    return (
                      <button
                        key={t}
                        onClick={() => setSelectedTopicFilter(active ? null : t)}
                        className={`px-2.5 py-1 rounded-lg text-[11px] font-mono font-medium border transition-all cursor-pointer ${
                          active
                            ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/20 border-indigo-500/30 text-indigo-300"
                            : isBright ? "bg-white border-slate-200/60 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                        }`}
                      >{t} ({count})</button>
                    );
                  })}
                </div>
              )}
              <DataTable
                columns={offsetColumns}
                data={filteredOffsets as unknown as Record<string, unknown>[]}
                searchPlaceholder="Filter by topic..."
                searchKeys={["topic"]}
                emptyMessage="No offset data"
              />
            </>
          )}

          {activeTab === "lag" && selectedConsumerGroup && (
            <LagChart offsets={selectedConsumerGroup.offsets || []} bright={isBright} />
          )}

          {activeTab === "rebalances" && (
            <RebalanceTimeline events={rebalanceHistory} bright={isBright} />
          )}

          {activeTab === "timeline" && selectedConsumerGroup && (
            <OffsetTimeline offsets={selectedConsumerGroup.offsets} groupId={groupId} bright={isBright} />
          )}

          {activeTab === "heatmap" && selectedConsumerGroup && (
            <PartitionHeatmap offsets={selectedConsumerGroup.offsets} members={selectedConsumerGroup.members} bright={isBright} />
          )}
          {activeTab === "lag-trend" && selectedConsumerGroup && (
            <LagTrendPanel offsets={selectedConsumerGroup.offsets} groupId={groupId} bright={isBright} />
          )}
        </>
      )}

      {/* Reset Offsets Modal */}
      <Modal title="Reset Consumer Group Offsets" open={showReset} onClose={() => { setShowReset(false); setResetResult(null); }}>
        <div className="space-y-4">
          {resetResult && (
            <div className={`p-3 rounded-xl border text-sm ${
              resetResult.success
                ? isBright ? "bg-emerald-50 border-emerald-200 text-emerald-700" : "bg-emerald-950/50 border-emerald-500/30 text-emerald-300"
                : isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
            }`}>
              {resetResult.message}
            </div>
          )}
          <p className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>
            Reset offsets for <span className={`font-mono font-medium ${isBright ? "text-amber-600" : "text-amber-300"}`}>{groupId}</span>. The consumer group must be inactive.
          </p>
          <div>
            <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Strategy</label>
            <div className="flex gap-2 mt-2 flex-wrap">
              {[
                { id: "earliest", label: "Earliest" },
                { id: "latest", label: "Latest" },
                { id: "timestamp", label: "By Timestamp" },
                { id: "specific", label: "Specific Offset" },
              ].map((s) => (
                <button
                  key={s.id}
                  onClick={() => setResetStrategy(s.id)}
                  className={`px-4 py-2 rounded-xl text-xs font-medium transition-all cursor-pointer ${
                    resetStrategy === s.id
                      ? isBright ? "bg-amber-50 border border-amber-200/60 text-amber-700" : "bg-amber-500/20 border border-amber-500/30 text-amber-300"
                      : isBright ? "bg-slate-100 border border-slate-200 text-slate-500 hover:bg-slate-200" : "bg-slate-800/50 border border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                  }`}
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>
          {resetStrategy === "timestamp" && (
            <div>
              <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Seek to Timestamp</label>
              <input
                type="datetime-local"
                value={resetTimestamp}
                onChange={(e) => setResetTimestamp(e.target.value)}
                className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none ${
                  isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
                }`}
              />
              <p className={`text-[10px] mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                Seeks to the first offset at or after this time
              </p>
            </div>
          )}
          {resetStrategy === "specific" && (
            <div>
              <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Target Offset</label>
              <input
                type="number"
                min={0}
                value={resetOffset}
                onChange={(e) => setResetOffset(e.target.value)}
                placeholder="0"
                className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm font-mono focus:outline-none ${
                  isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
                }`}
              />
              <p className={`text-[10px] mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                Sets all selected partitions to this offset
              </p>
            </div>
          )}
          <div>
            <label className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Topic (optional)</label>
            <select
              value={resetTopic}
              onChange={(e) => setResetTopic(e.target.value)}
              className={`w-full mt-1.5 rounded-xl px-3.5 py-2.5 border text-sm focus:outline-none ${
                isBright ? "bg-slate-50 border-slate-200 text-slate-800" : "bg-slate-800/80 border-slate-700/50 text-white"
              }`}
            >
              <option value="">All topics</option>
              {topicsList.map((t) => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => { setShowReset(false); setResetResult(null); }} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}>Cancel</button>
            <button onClick={handleReset} disabled={resetting} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
              isBright ? "bg-amber-50 border-amber-200/60 text-amber-700 hover:bg-amber-100" : "bg-amber-500/20 border-amber-500/30 text-amber-300 hover:bg-amber-500/25"
            }`}>
              {resetting ? "Resetting..." : "Reset Offsets"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Delete Consumer Group Modal */}
      <Modal title="Delete Consumer Group" open={showDelete} onClose={() => { setShowDelete(false); setDeleteError(null); }}>
        <div className="space-y-4">
          {deleteError && (
            <div className={`p-3 rounded-xl border text-sm ${
              isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
            }`}>
              {deleteError}
            </div>
          )}
          <p className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>
            Are you sure you want to delete consumer group <span className={`font-mono font-medium ${isBright ? "text-amber-600" : "text-amber-300"}`}>{groupId}</span>? The group must be inactive. This action cannot be undone.
          </p>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => { setShowDelete(false); setDeleteError(null); }} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}>Cancel</button>
            <button onClick={handleDelete} className="px-4 py-2 rounded-xl text-sm font-medium bg-red-500/20 border border-red-500/40 text-red-400 hover:bg-red-500/30 transition-colors cursor-pointer">Delete</button>
          </div>
        </div>
      </Modal>
    </div>
  );
}

function TopicLagBreakdown({ offsets, bright }: { offsets: { topic: string; partition: number; lag: number }[]; bright: boolean }) {
  const byTopic = new Map<string, number>();
  offsets.forEach((o) => {
    byTopic.set(o.topic, (byTopic.get(o.topic) || 0) + o.lag);
  });
  const entries = [...byTopic.entries()].sort((a, b) => b[1] - a[1]);
  const maxLag = Math.max(...entries.map(([, v]) => v), 1);

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${bright ? "text-slate-500" : "text-slate-400"}`}>
        Lag by Topic
      </div>
      <div className="space-y-2">
        {entries.map(([topic, lag]) => {
          const pct = maxLag > 0 ? (lag / maxLag) * 100 : 0;
          const color = lag > 1000 ? "bg-red-500" : lag > 100 ? "bg-amber-500" : "bg-emerald-500";
          return (
            <div key={topic} className="flex items-center gap-3">
              <span className={`text-[11px] font-mono truncate w-40 shrink-0 ${bright ? "text-indigo-600" : "text-indigo-300"}`}>{topic}</span>
              <div className={`flex-1 h-2 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                <div className={`h-full rounded-full transition-all duration-500 ${color}`} style={{ width: `${pct}%` }} />
              </div>
              <span className={`text-[11px] font-mono font-bold tabular-nums w-16 text-right ${
                lag > 1000 ? "text-red-500" : lag > 0 ? "text-amber-500" : "text-emerald-500"
              }`}>
                {fmt(lag)}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function LagSparkline({ data, bright }: { data: number[]; bright: boolean }) {
  const max = Math.max(...data, 1);
  const h = 48;
  const w = 280;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - (v / max) * (h - 8) - 4;
    return `${x},${y}`;
  }).join(" ");
  const fillPoints = `0,${h} ${points} ${w},${h}`;
  const current = data[data.length - 1] || 0;
  const prev = data.length > 1 ? data[data.length - 2] : current;
  const trend = current > prev ? "up" : current < prev ? "down" : "flat";

  return (
    <div className={`rounded-2xl border px-4 py-3 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-2">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>Lag Trend</span>
        <div className="flex items-center gap-1.5">
          <span className={`text-xs font-mono font-bold ${current > 1000 ? "text-red-500" : current > 0 ? "text-amber-500" : "text-emerald-500"}`}>
            {current.toLocaleString()}
          </span>
          <span className={`text-[10px] ${trend === "up" ? "text-red-400" : trend === "down" ? "text-emerald-400" : bright ? "text-slate-400" : "text-slate-500"}`}>
            {trend === "up" ? "\u2191" : trend === "down" ? "\u2193" : "\u2192"}
          </span>
        </div>
      </div>
      <svg width={w} height={h} className="w-full">
        <polygon points={fillPoints} fill={bright ? "rgba(99,102,241,0.08)" : "rgba(99,102,241,0.15)"} />
        <polyline
          points={points}
          fill="none"
          stroke={current > 1000 ? "#ef4444" : current > 100 ? "#f59e0b" : "#22c55e"}
          strokeWidth={2}
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </svg>
    </div>
  );
}

function ConsumptionVelocity({ snapshots, bright }: { snapshots: { ts: number; offsets: Map<string, { current: number; end: number }> }[]; bright: boolean }) {
  const latest = snapshots[snapshots.length - 1];
  const prev = snapshots[snapshots.length - 2];
  if (!latest || !prev) return null;

  const dtSec = Math.max((latest.ts - prev.ts) / 1000, 1);
  const velocities: { topic: string; consumeRate: number; produceRate: number; progress: number }[] = [];

  for (const [topic, curr] of latest.offsets) {
    const prevData = prev.offsets.get(topic);
    if (!prevData) continue;
    const consumeRate = Math.max(0, (curr.current - prevData.current) / dtSec);
    const produceRate = Math.max(0, (curr.end - prevData.end) / dtSec);
    const progress = curr.end > 0 ? (curr.current / curr.end) * 100 : 100;
    velocities.push({ topic, consumeRate, produceRate, progress });
  }

  if (velocities.length === 0) return null;

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${bright ? "text-slate-500" : "text-slate-400"}`}>
        Consumption Velocity
      </div>
      <div className="space-y-3">
        {velocities.map((v) => (
          <div key={v.topic}>
            <div className="flex items-center justify-between mb-1">
              <span className={`text-[11px] font-mono truncate max-w-[180px] ${bright ? "text-indigo-600" : "text-indigo-300"}`}>{v.topic}</span>
              <div className="flex items-center gap-3">
                <span className={`text-[10px] font-mono ${v.consumeRate > 0 ? "text-emerald-500" : bright ? "text-slate-400" : "text-slate-500"}`}>
                  {v.consumeRate > 0 ? `${v.consumeRate.toFixed(0)} consume/s` : "idle"}
                </span>
                <span className={`text-[10px] font-mono ${v.produceRate > 0 ? "text-amber-500" : bright ? "text-slate-400" : "text-slate-500"}`}>
                  {v.produceRate > 0 ? `${v.produceRate.toFixed(0)} produce/s` : "idle"}
                </span>
              </div>
            </div>
            <div className={`h-2 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-800/50"}`}>
              <div
                className={`h-full rounded-full transition-all duration-700 ${
                  v.progress >= 99.9 ? "bg-emerald-500" : v.progress > 50 ? "bg-cyan-500" : "bg-amber-500"
                }`}
                style={{ width: `${Math.min(100, v.progress)}%` }}
              />
            </div>
            <div className={`text-[9px] text-right mt-0.5 ${bright ? "text-slate-400" : "text-slate-500"}`}>
              {v.progress.toFixed(1)}% consumed
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function MemberAssignmentGrid({ members, bright }: { members: { memberId: string; clientId: string; clientHost: string; partitions: string[] }[]; bright: boolean }) {
  const colors = ["bg-cyan-500", "bg-amber-500", "bg-emerald-500", "bg-rose-500", "bg-violet-500", "bg-blue-500", "bg-orange-500", "bg-teal-500"];
  const totalPartitions = members.reduce((s, m) => s + m.partitions.length, 0);
  if (totalPartitions === 0) return null;
  const avgPartitions = totalPartitions / members.length;
  const maxPartitions = Math.max(...members.map((m) => m.partitions.length));
  const minPartitions = Math.min(...members.map((m) => m.partitions.length));
  const isBalanced = maxPartitions - minPartitions <= 1;

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-3">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Partition Assignment
        </span>
        <div className="flex items-center gap-2">
          <span className={`text-[10px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>
            avg {avgPartitions.toFixed(1)}/member
          </span>
          {isBalanced ? (
            <span className={`text-[9px] uppercase font-bold px-1.5 py-0.5 rounded ${bright ? "bg-emerald-50 text-emerald-600" : "bg-emerald-500/15 text-emerald-400"}`}>
              Balanced
            </span>
          ) : (
            <span className={`text-[9px] uppercase font-bold px-1.5 py-0.5 rounded ${bright ? "bg-amber-50 text-amber-600" : "bg-amber-500/15 text-amber-400"}`}>
              Skewed
            </span>
          )}
        </div>
      </div>
      <div className="space-y-2">
        {members.map((m, i) => {
          const barWidth = maxPartitions > 0 ? (m.partitions.length / maxPartitions) * 100 : 0;
          const color = colors[i % colors.length];
          return (
            <div key={m.memberId} className="flex items-center gap-3">
              <div className={`w-24 shrink-0 truncate text-[11px] font-mono ${bright ? "text-slate-600" : "text-slate-300"}`} title={m.clientId}>
                {m.clientId}
              </div>
              <div className={`flex-1 h-5 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                <div
                  className={`h-full rounded-full ${color} opacity-70 transition-all duration-500 flex items-center px-2`}
                  style={{ width: `${Math.max(barWidth, 8)}%` }}
                >
                  <span className="text-[9px] font-bold text-white whitespace-nowrap">{m.partitions.length}</span>
                </div>
              </div>
              <div className="flex gap-0.5 flex-wrap max-w-[200px]">
                {m.partitions.slice(0, 8).map((p) => (
                  <span key={p} className={`text-[8px] rounded px-1 py-0.5 font-mono ${bright ? "bg-slate-100 text-slate-500" : "bg-slate-800/60 text-slate-400"}`}>{p}</span>
                ))}
                {m.partitions.length > 8 && (
                  <span className={`text-[8px] ${bright ? "text-slate-400" : "text-slate-500"}`}>+{m.partitions.length - 8}</span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function RebalanceTimeline({ events, bright }: { events: RebalanceEvent[]; bright: boolean }) {
  if (events.length === 0) {
    return (
      <div className={`rounded-2xl border px-6 py-10 text-center ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
        <div className={`text-sm font-medium ${bright ? "text-slate-400" : "text-slate-500"}`}>
          No rebalance events detected yet
        </div>
        <div className={`text-[11px] mt-1 ${bright ? "text-slate-400" : "text-slate-500"}`}>
          Enable auto-refresh to monitor rebalances in real time
        </div>
      </div>
    );
  }

  const iconMap: Record<string, { icon: string; color: string }> = {
    member_join: { icon: "+", color: bright ? "bg-emerald-100 text-emerald-700 border-emerald-200" : "bg-emerald-500/20 text-emerald-400 border-emerald-500/30" },
    member_leave: { icon: "-", color: bright ? "bg-red-100 text-red-700 border-red-200" : "bg-red-500/20 text-red-400 border-red-500/30" },
    partition_reassign: { icon: "\u21C4", color: bright ? "bg-amber-100 text-amber-700 border-amber-200" : "bg-amber-500/20 text-amber-400 border-amber-500/30" },
    state_change: { icon: "\u25CF", color: bright ? "bg-indigo-100 text-indigo-700 border-indigo-200" : "bg-indigo-500/20 text-indigo-400 border-indigo-500/30" },
  };

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-4">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Rebalance Timeline ({events.length} events)
        </span>
        <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>
          Session-only tracking (resets on page reload)
        </span>
      </div>
      <div className="relative">
        {/* Timeline line */}
        <div className={`absolute left-4 top-0 bottom-0 w-px ${bright ? "bg-slate-200" : "bg-slate-700/50"}`} />
        <div className="space-y-3">
          {events.map((event, i) => {
            const { icon, color } = iconMap[event.type] || iconMap.state_change;
            const timeAgo = Math.round((Date.now() - event.timestamp) / 1000);
            const timeStr = timeAgo < 60 ? `${timeAgo}s ago` : timeAgo < 3600 ? `${Math.round(timeAgo / 60)}m ago` : `${Math.round(timeAgo / 3600)}h ago`;
            return (
              <div key={`${event.timestamp}-${i}`} className="flex items-start gap-3 pl-0 relative">
                <div className={`w-8 h-8 rounded-xl flex items-center justify-center shrink-0 border text-sm font-bold z-10 ${color}`}>
                  {icon}
                </div>
                <div className="flex-1 min-w-0 pt-1">
                  <div className="flex items-center gap-2">
                    <span className={`text-xs font-medium ${bright ? "text-slate-700" : "text-slate-200"}`}>
                      {event.description}
                    </span>
                    <span className={`text-[9px] font-mono shrink-0 ${bright ? "text-slate-400" : "text-slate-500"}`}>
                      {timeStr}
                    </span>
                  </div>
                  <div className={`text-[10px] mt-0.5 ${bright ? "text-slate-400" : "text-slate-500"}`}>
                    {new Date(event.timestamp).toLocaleTimeString()} &middot; State: {event.state} &middot; Members: {event.membersAfter}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function SlaMonitor({ samples, threshold, onThresholdChange, bright }: {
  samples: { ts: number; lag: number; ok: boolean }[];
  threshold: number;
  onThresholdChange: (v: number) => void;
  bright: boolean;
}) {
  const okCount = samples.filter((s) => s.ok).length;
  const compliance = samples.length > 0 ? (okCount / samples.length) * 100 : 100;
  const complianceStr = compliance.toFixed(1);
  const isGood = compliance >= 99;
  const isWarn = compliance >= 95 && !isGood;
  const isBad = compliance < 95;

  // Build a mini heatmap of recent samples (most recent on the right)
  const recentSamples = samples.slice(-60);

  // Longest consecutive breach
  let maxBreach = 0;
  let curBreach = 0;
  for (const s of samples) {
    if (!s.ok) { curBreach++; maxBreach = Math.max(maxBreach, curBreach); } else { curBreach = 0; }
  }

  // Current streak
  let streak = 0;
  let streakOk = true;
  for (let i = samples.length - 1; i >= 0; i--) {
    if (i === samples.length - 1) { streakOk = samples[i].ok; streak = 1; continue; }
    if (samples[i].ok === streakOk) { streak++; } else break;
  }

  const presets = [100, 500, 1000, 5000, 10000];

  return (
    <div className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
      <div className="flex items-center justify-between mb-3">
        <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          Lag SLA Compliance
        </span>
        <div className="flex items-center gap-2">
          <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Threshold:</span>
          <div className="flex gap-1">
            {presets.map((p) => (
              <button
                key={p}
                onClick={() => onThresholdChange(p)}
                className={`px-2 py-0.5 rounded-md text-[10px] font-mono font-medium border transition-all cursor-pointer ${
                  threshold === p
                    ? bright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/20 border-indigo-500/30 text-indigo-300"
                    : bright ? "bg-white border-slate-200/60 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                }`}
              >
                {p >= 1000 ? `${p / 1000}K` : p}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="flex items-center gap-6">
        {/* Big compliance number */}
        <div className="text-center shrink-0">
          <div className={`text-3xl font-bold font-mono tabular-nums ${
            isGood ? "text-emerald-500" : isWarn ? "text-amber-500" : "text-red-500"
          }`}>
            {complianceStr}%
          </div>
          <div className={`text-[10px] mt-0.5 ${bright ? "text-slate-400" : "text-slate-500"}`}>
            {okCount}/{samples.length} samples OK
          </div>
        </div>

        {/* Sample heatmap */}
        <div className="flex-1">
          <div className="flex gap-px flex-wrap">
            {recentSamples.map((s, i) => (
              <div
                key={i}
                className={`w-2 h-3 rounded-[2px] ${
                  s.ok
                    ? bright ? "bg-emerald-300" : "bg-emerald-500/60"
                    : bright ? "bg-red-400" : "bg-red-500/70"
                }`}
                title={`Lag: ${s.lag.toLocaleString()} at ${new Date(s.ts).toLocaleTimeString()}`}
              />
            ))}
          </div>
          <div className="flex justify-between mt-1">
            <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>older</span>
            <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>now</span>
          </div>
        </div>

        {/* Stats */}
        <div className="shrink-0 grid grid-cols-2 gap-x-4 gap-y-1">
          <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Current streak</span>
          <span className={`text-[10px] font-mono font-bold text-right ${streakOk ? "text-emerald-500" : "text-red-500"}`}>
            {streak} {streakOk ? "OK" : "BREACH"}
          </span>
          <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Longest breach</span>
          <span className={`text-[10px] font-mono font-bold text-right ${maxBreach > 5 ? "text-red-500" : maxBreach > 0 ? "text-amber-500" : "text-emerald-500"}`}>
            {maxBreach} samples
          </span>
          <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>SLA target</span>
          <span className={`text-[10px] font-mono font-bold text-right ${bright ? "text-slate-600" : "text-slate-300"}`}>
            lag &le; {threshold >= 1000 ? `${threshold / 1000}K` : threshold}
          </span>
          <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Status</span>
          <span className={`text-[10px] font-bold text-right uppercase ${
            isGood ? "text-emerald-500" : isWarn ? "text-amber-500" : "text-red-500"
          }`}>
            {isGood ? "Healthy" : isWarn ? "At Risk" : isBad ? "Breached" : "Unknown"}
          </span>
        </div>
      </div>
    </div>
  );
}

function SummaryCard({ label, value, color, bright }: { label: string; value: string; color: string; bright: boolean }) {
  const darkColorMap: Record<string, string> = {
    amber: "border-amber-500/20 from-amber-500/[0.06]",
    indigo: "border-indigo-500/20 from-indigo-500/[0.06]",
    emerald: "border-emerald-500/20 from-emerald-500/[0.06]",
    red: "border-red-500/20 from-red-500/[0.06]",
    slate: "border-slate-700/30 from-slate-500/[0.04]",
  };
  const brightColorMap: Record<string, string> = {
    amber: "border-amber-200/60 from-amber-50",
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

interface LagChartProps {
  offsets: { topic: string; partition: number; currentOffset: number; endOffset: number; lag: number }[];
  bright: boolean;
}

function LagChart({ offsets, bright }: LagChartProps) {
  const [groupBy, setGroupBy] = useState<"partition" | "topic">("topic");

  if (offsets.length === 0) {
    return (
      <div className={`text-center py-16 ${bright ? "text-slate-400" : "text-slate-500"}`}>
        <p className="text-sm">No offset data available for chart</p>
      </div>
    );
  }

  const maxLag = Math.max(...offsets.map((o) => o.lag), 1);
  const totalLag = offsets.reduce((s, o) => s + o.lag, 0);

  // Group by topic
  const byTopic: Record<string, { lag: number; partitions: number; maxLag: number }> = {};
  offsets.forEach((o) => {
    if (!byTopic[o.topic]) byTopic[o.topic] = { lag: 0, partitions: 0, maxLag: 0 };
    byTopic[o.topic].lag += o.lag;
    byTopic[o.topic].partitions += 1;
    byTopic[o.topic].maxLag = Math.max(byTopic[o.topic].maxLag, o.lag);
  });
  const topicEntries = Object.entries(byTopic).sort(([, a], [, b]) => b.lag - a.lag);
  const maxTopicLag = Math.max(...topicEntries.map(([, v]) => v.lag), 1);

  const barColors = ["#6366f1", "#8b5cf6", "#ec4899", "#f59e0b", "#10b981", "#0ea5e9", "#f43f5e", "#84cc16"];

  return (
    <div className="space-y-4">
      {/* Summary */}
      <div className={`rounded-2xl border p-4 ${bright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
        <div className="flex items-center justify-between mb-3">
          <h4 className={`text-sm font-semibold ${bright ? "text-slate-800" : "text-white"}`}>Lag Distribution</h4>
          <div className="flex items-center gap-1">
            {(["topic", "partition"] as const).map((mode) => (
              <button
                key={mode}
                onClick={() => setGroupBy(mode)}
                className={`px-2.5 py-1 rounded-lg text-[10px] font-medium transition-colors cursor-pointer ${
                  groupBy === mode
                    ? bright ? "bg-indigo-50 text-indigo-700" : "bg-indigo-500/15 text-indigo-300"
                    : bright ? "text-slate-400 hover:text-slate-600" : "text-slate-500 hover:text-slate-300"
                }`}
              >
                By {mode}
              </button>
            ))}
          </div>
        </div>

        <div className="flex items-center gap-4 mb-4">
          <div>
            <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Total Lag</div>
            <div className={`text-lg font-bold font-mono ${totalLag > 0 ? "text-amber-500" : bright ? "text-emerald-600" : "text-emerald-400"}`}>
              {totalLag.toLocaleString()}
            </div>
          </div>
          <div>
            <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Max Partition Lag</div>
            <div className={`text-lg font-bold font-mono ${bright ? "text-slate-700" : "text-slate-200"}`}>{maxLag.toLocaleString()}</div>
          </div>
          <div>
            <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Partitions</div>
            <div className={`text-lg font-bold font-mono ${bright ? "text-slate-700" : "text-slate-200"}`}>{offsets.length}</div>
          </div>
          <div>
            <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Zero Lag</div>
            <div className={`text-lg font-bold font-mono ${bright ? "text-emerald-600" : "text-emerald-400"}`}>
              {offsets.filter((o) => o.lag === 0).length}
            </div>
          </div>
        </div>

        {groupBy === "topic" ? (
          <div className="space-y-2">
            {topicEntries.map(([topic, data], i) => (
              <div key={topic} className={`flex items-center gap-3 py-2 px-3 rounded-xl ${bright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <span className={`w-2 h-2 rounded-full shrink-0`} style={{ background: barColors[i % barColors.length] }} />
                <span className={`text-[11px] font-mono truncate w-40 shrink-0 ${bright ? "text-slate-700" : "text-slate-300"}`}>{topic}</span>
                <div className={`flex-1 h-4 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                  <div
                    className="h-full rounded-full transition-all"
                    style={{ width: `${(data.lag / maxTopicLag) * 100}%`, background: barColors[i % barColors.length], opacity: 0.7 }}
                  />
                </div>
                <span className={`text-[11px] font-mono tabular-nums w-20 text-right ${data.lag > 0 ? "text-amber-500" : bright ? "text-emerald-600" : "text-emerald-400"}`}>
                  {data.lag.toLocaleString()}
                </span>
                <span className={`text-[9px] w-10 text-right ${bright ? "text-slate-400" : "text-slate-500"}`}>
                  P{data.partitions}
                </span>
              </div>
            ))}
          </div>
        ) : (
          <div className="space-y-1">
            {offsets.sort((a, b) => b.lag - a.lag).map((o) => (
              <div key={`${o.topic}-${o.partition}`} className={`flex items-center gap-2 py-1.5 px-3 rounded-lg ${bright ? "hover:bg-slate-50" : "hover:bg-slate-800/30"}`}>
                <span className={`text-[10px] font-mono w-28 truncate shrink-0 ${bright ? "text-slate-500" : "text-slate-400"}`}>{o.topic}</span>
                <span className={`text-[10px] font-mono w-8 text-center shrink-0 ${bright ? "text-slate-400" : "text-slate-500"}`}>P{o.partition}</span>
                <div className={`flex-1 h-3 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-800/60"}`}>
                  <div
                    className="h-full rounded-full transition-all"
                    style={{
                      width: `${(o.lag / maxLag) * 100}%`,
                      background: o.lag === 0 ? "#10b981" : o.lag > maxLag * 0.8 ? "#ef4444" : "#f59e0b",
                    }}
                  />
                </div>
                <span className={`text-[10px] font-mono tabular-nums w-16 text-right ${
                  o.lag === 0 ? (bright ? "text-emerald-600" : "text-emerald-400") : "text-amber-500"
                }`}>
                  {o.lag.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Partition heatmap */}
      <div className={`rounded-2xl border p-4 ${bright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
        <h4 className={`text-sm font-semibold mb-3 ${bright ? "text-slate-800" : "text-white"}`}>Partition Lag Heatmap</h4>
        <div className="flex flex-wrap gap-1">
          {offsets.sort((a, b) => {
            if (a.topic !== b.topic) return a.topic.localeCompare(b.topic);
            return a.partition - b.partition;
          }).map((o) => {
            const intensity = maxLag > 0 ? o.lag / maxLag : 0;
            const bg = o.lag === 0
              ? (bright ? "#d1fae5" : "#064e3b")
              : intensity > 0.8
                ? (bright ? "#fecaca" : "#7f1d1d")
                : intensity > 0.4
                  ? (bright ? "#fed7aa" : "#78350f")
                  : (bright ? "#fef3c7" : "#422006");
            return (
              <div
                key={`${o.topic}-${o.partition}`}
                className="w-6 h-6 rounded flex items-center justify-center"
                style={{ background: bg }}
                title={`${o.topic} P${o.partition}: lag ${o.lag.toLocaleString()}`}
              >
                <span className={`text-[7px] font-mono font-bold ${
                  intensity > 0.5 ? "text-white" : bright ? "text-slate-600" : "text-slate-300"
                }`}>
                  {o.partition}
                </span>
              </div>
            );
          })}
        </div>
        <div className="flex items-center gap-3 mt-3">
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ background: bright ? "#d1fae5" : "#064e3b" }} />
            <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>0 lag</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ background: bright ? "#fef3c7" : "#422006" }} />
            <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Low</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ background: bright ? "#fed7aa" : "#78350f" }} />
            <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Medium</span>
          </div>
          <div className="flex items-center gap-1">
            <div className="w-3 h-3 rounded" style={{ background: bright ? "#fecaca" : "#7f1d1d" }} />
            <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>High</span>
          </div>
        </div>
      </div>
    </div>
  );
}

interface OffsetTimelineProps {
  offsets: { topic: string; partition: number; currentOffset: number; endOffset: number; lag: number }[];
  groupId: string;
  bright: boolean;
}

interface TimelineSample {
  ts: number;
  totalOffset: number;
  totalEnd: number;
  totalLag: number;
  byTopic: Record<string, { offset: number; end: number; lag: number }>;
}

function OffsetTimeline({ offsets, bright }: OffsetTimelineProps) {
  const [samples, setSamples] = useState<TimelineSample[]>([]);
  const [selectedTopic, setSelectedTopic] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  const topics = useMemo(() => [...new Set(offsets.map((o) => o.topic))].sort(), [offsets]);

  // Record a sample from current offsets
  useEffect(() => {
    const snap = (): TimelineSample => {
      const byTopic: Record<string, { offset: number; end: number; lag: number }> = {};
      let totalOffset = 0, totalEnd = 0, totalLag = 0;
      for (const o of offsets) {
        totalOffset += o.currentOffset;
        totalEnd += o.endOffset;
        totalLag += o.lag;
        if (!byTopic[o.topic]) byTopic[o.topic] = { offset: 0, end: 0, lag: 0 };
        byTopic[o.topic].offset += o.currentOffset;
        byTopic[o.topic].end += o.endOffset;
        byTopic[o.topic].lag += o.lag;
      }
      return { ts: Date.now(), totalOffset, totalEnd, totalLag, byTopic };
    };

    setSamples((prev) => [...prev, snap()].slice(-120));

    intervalRef.current = setInterval(() => {
      setSamples((prev) => [...prev, snap()].slice(-120));
    }, 5000);

    return () => clearInterval(intervalRef.current);
  }, [offsets]);

  const viewData = useMemo(() => {
    if (selectedTopic) {
      return samples.map((s) => ({
        ts: s.ts,
        offset: s.byTopic[selectedTopic]?.offset || 0,
        end: s.byTopic[selectedTopic]?.end || 0,
        lag: s.byTopic[selectedTopic]?.lag || 0,
      }));
    }
    return samples.map((s) => ({ ts: s.ts, offset: s.totalOffset, end: s.totalEnd, lag: s.totalLag }));
  }, [samples, selectedTopic]);

  const svgW = 600;
  const svgH = 160;
  const padL = 50;
  const padR = 10;
  const padT = 10;
  const padB = 30;

  const chartW = svgW - padL - padR;
  const chartH = svgH - padT - padB;

  const drawLine = (data: number[], color: string) => {
    if (data.length < 2) return null;
    const max = Math.max(...data, 1);
    const points = data.map((v, i) => {
      const x = padL + (i / (data.length - 1)) * chartW;
      const y = padT + chartH - (v / max) * chartH;
      return `${x},${y}`;
    }).join(" ");
    return <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" />;
  };

  const lagFillPath = (data: number[]) => {
    if (data.length < 2) return null;
    const max = Math.max(...data, 1);
    const pts = data.map((v, i) => {
      const x = padL + (i / (data.length - 1)) * chartW;
      const y = padT + chartH - (v / max) * chartH;
      return { x, y };
    });
    const d = `M${pts.map((p) => `${p.x},${p.y}`).join("L")}L${pts[pts.length - 1].x},${padT + chartH}L${pts[0].x},${padT + chartH}Z`;
    return <path d={d} fill={bright ? "rgba(239,68,68,0.08)" : "rgba(239,68,68,0.12)"} />;
  };

  const currentLag = viewData.length > 0 ? viewData[viewData.length - 1].lag : 0;
  const firstLag = viewData.length > 0 ? viewData[0].lag : 0;
  const lagDelta = currentLag - firstLag;

  return (
    <div className="space-y-4">
      {/* Topic filter */}
      <div className="flex items-center gap-2 flex-wrap">
        <button
          onClick={() => setSelectedTopic(null)}
          className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
            !selectedTopic
              ? bright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
              : bright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
          }`}
        >
          All Topics
        </button>
        {topics.map((t) => (
          <button
            key={t}
            onClick={() => setSelectedTopic(t)}
            className={`px-3 py-1.5 rounded-lg text-[11px] font-mono border transition-colors cursor-pointer ${
              selectedTopic === t
                ? bright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                : bright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
            }`}
          >
            {t.length > 20 ? t.slice(0, 19) + "…" : t}
          </button>
        ))}
      </div>

      {/* Summary */}
      <div className="grid grid-cols-3 gap-3">
        <div className={`rounded-xl border px-3 py-2.5 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Current Lag</div>
          <div className={`text-xl font-bold tabular-nums mt-0.5 ${
            currentLag > 10000 ? bright ? "text-red-600" : "text-red-400"
              : currentLag > 1000 ? bright ? "text-amber-600" : "text-amber-400"
              : bright ? "text-emerald-600" : "text-emerald-400"
          }`}>{currentLag.toLocaleString()}</div>
        </div>
        <div className={`rounded-xl border px-3 py-2.5 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Lag Trend</div>
          <div className={`text-xl font-bold tabular-nums mt-0.5 ${
            lagDelta > 0 ? bright ? "text-red-600" : "text-red-400"
              : lagDelta < 0 ? bright ? "text-emerald-600" : "text-emerald-400"
              : bright ? "text-slate-600" : "text-slate-300"
          }`}>{lagDelta > 0 ? "+" : ""}{lagDelta.toLocaleString()}</div>
        </div>
        <div className={`rounded-xl border px-3 py-2.5 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
          <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Samples</div>
          <div className={`text-xl font-bold tabular-nums mt-0.5 ${bright ? "text-slate-800" : "text-white"}`}>{samples.length}</div>
        </div>
      </div>

      {/* Chart */}
      <div className={`rounded-xl border p-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
        <div className="flex items-center justify-between mb-2">
          <span className={`text-[11px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
            Offset & Lag Over Time
          </span>
          <div className="flex items-center gap-3">
            <span className="flex items-center gap-1 text-[9px]">
              <span className={`w-3 h-0.5 ${bright ? "bg-indigo-400" : "bg-indigo-500"}`} />
              <span className={bright ? "text-slate-500" : "text-slate-400"}>Consumer Offset</span>
            </span>
            <span className="flex items-center gap-1 text-[9px]">
              <span className={`w-3 h-0.5 ${bright ? "bg-sky-400" : "bg-sky-500"}`} />
              <span className={bright ? "text-slate-500" : "text-slate-400"}>End Offset</span>
            </span>
            <span className="flex items-center gap-1 text-[9px]">
              <span className={`w-3 h-0.5 ${bright ? "bg-red-400" : "bg-red-500"}`} />
              <span className={bright ? "text-slate-500" : "text-slate-400"}>Lag</span>
            </span>
          </div>
        </div>
        {viewData.length < 2 ? (
          <div className={`text-center py-8 text-sm ${bright ? "text-slate-400" : "text-slate-500"}`}>
            Collecting data... (samples every 5s)
          </div>
        ) : (
          <svg width="100%" viewBox={`0 0 ${svgW} ${svgH}`} className="overflow-visible">
            {/* Grid lines */}
            {[0, 0.25, 0.5, 0.75, 1].map((pct) => {
              const y = padT + chartH * (1 - pct);
              return (
                <line key={pct} x1={padL} x2={padL + chartW} y1={y} y2={y}
                  stroke={bright ? "#e2e8f0" : "#1e293b"} strokeWidth={0.5} />
              );
            })}
            {/* Time axis labels */}
            {viewData.length >= 2 && [0, 0.5, 1].map((pct) => {
              const idx = Math.floor(pct * (viewData.length - 1));
              const x = padL + (idx / (viewData.length - 1)) * chartW;
              const time = new Date(viewData[idx].ts).toLocaleTimeString();
              return (
                <text key={pct} x={x} y={svgH - 4} textAnchor="middle" fontSize={9}
                  fill={bright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">{time}</text>
              );
            })}
            {/* Lag fill */}
            {lagFillPath(viewData.map((d) => d.lag))}
            {/* Lines */}
            {drawLine(viewData.map((d) => d.offset), bright ? "#6366f1" : "#818cf8")}
            {drawLine(viewData.map((d) => d.end), bright ? "#0ea5e9" : "#38bdf8")}
            {drawLine(viewData.map((d) => d.lag), bright ? "#ef4444" : "#f87171")}
            {/* Y-axis labels */}
            {(() => {
              const maxVal = Math.max(...viewData.map((d) => Math.max(d.offset, d.end, d.lag)), 1);
              return [0, 0.5, 1].map((pct) => {
                const val = Math.round(maxVal * pct);
                const y = padT + chartH * (1 - pct);
                const label = val > 1000000 ? `${(val / 1000000).toFixed(1)}M` : val > 1000 ? `${(val / 1000).toFixed(0)}K` : String(val);
                return (
                  <text key={pct} x={padL - 4} y={y + 3} textAnchor="end" fontSize={9}
                    fill={bright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">{label}</text>
                );
              });
            })()}
          </svg>
        )}
      </div>

      <div className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>
        Data is collected live while this tab is open. Refreshes every 5 seconds.
      </div>
    </div>
  );
}

function PartitionHeatmap({ offsets, members, bright }: {
  offsets: { topic: string; partition: number; currentOffset: number; endOffset: number; lag: number }[];
  members: { memberId: string; clientId: string; partitions: string[] }[];
  bright: boolean;
}) {
  // Group offsets by topic
  const topicGroups = useMemo(() => {
    const map = new Map<string, typeof offsets>();
    for (const o of offsets) {
      const list = map.get(o.topic) || [];
      list.push(o);
      map.set(o.topic, list);
    }
    return [...map.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  }, [offsets]);

  // Build member assignment lookup
  const partitionOwner = useMemo(() => {
    const map = new Map<string, string>();
    for (const m of members) {
      for (const p of m.partitions) {
        map.set(p, m.clientId);
      }
    }
    return map;
  }, [members]);

  const maxLag = Math.max(...offsets.map((o) => o.lag), 1);

  const memberColors = useMemo(() => {
    const colors = ["#6366f1", "#06b6d4", "#f59e0b", "#10b981", "#ec4899", "#8b5cf6", "#f97316", "#14b8a6"];
    const clientIds = [...new Set(members.map((m) => m.clientId))];
    const map = new Map<string, string>();
    clientIds.forEach((id, i) => map.set(id, colors[i % colors.length]));
    return map;
  }, [members]);

  return (
    <div className="space-y-4">
      {/* Legend */}
      <div className="flex items-center gap-4 flex-wrap">
        <span className={`text-[10px] uppercase tracking-wider font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>Members:</span>
        {[...memberColors.entries()].map(([clientId, color]) => (
          <div key={clientId} className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-sm shrink-0" style={{ backgroundColor: color, opacity: 0.7 }} />
            <span className={`text-[10px] font-mono ${bright ? "text-slate-600" : "text-slate-300"}`}>{clientId}</span>
          </div>
        ))}
      </div>

      {topicGroups.map(([topic, parts]) => {
        const sorted = [...parts].sort((a, b) => a.partition - b.partition);
        return (
          <div key={topic} className={`rounded-2xl border px-5 py-4 ${bright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className="flex items-center justify-between mb-3">
              <span className={`text-[12px] font-mono font-medium ${bright ? "text-indigo-600" : "text-indigo-300"}`}>{topic}</span>
              <span className={`text-[10px] font-mono ${bright ? "text-slate-400" : "text-slate-500"}`}>{sorted.length} partitions</span>
            </div>
            <div className="flex flex-wrap gap-1.5">
              {sorted.map((p) => {
                const lagPct = maxLag > 0 ? p.lag / maxLag : 0;
                const key1 = `${p.topic}-${p.partition}`;
                const key2 = `${p.topic}:${p.partition}`;
                const owner = partitionOwner.get(key1) || partitionOwner.get(key2) || null;
                const ownerColor = owner ? memberColors.get(owner) : undefined;

                // Color intensity based on lag
                const bgColor = p.lag === 0
                  ? bright ? "rgba(16,185,129,0.15)" : "rgba(16,185,129,0.1)"
                  : lagPct > 0.5
                    ? bright ? `rgba(239,68,68,${0.1 + lagPct * 0.3})` : `rgba(239,68,68,${0.1 + lagPct * 0.2})`
                    : bright ? `rgba(245,158,11,${0.1 + lagPct * 0.3})` : `rgba(245,158,11,${0.1 + lagPct * 0.2})`;

                return (
                  <div
                    key={p.partition}
                    className={`relative rounded-lg px-2.5 py-2 min-w-[56px] text-center transition-all border ${
                      bright ? "border-slate-200/40" : "border-slate-700/20"
                    }`}
                    style={{ backgroundColor: bgColor }}
                    title={`P${p.partition} | Lag: ${p.lag.toLocaleString()} | Offset: ${p.currentOffset.toLocaleString()}/${p.endOffset.toLocaleString()}${owner ? ` | Owner: ${owner}` : ""}`}
                  >
                    {ownerColor && (
                      <div className="absolute top-0.5 right-0.5 w-2 h-2 rounded-full" style={{ backgroundColor: ownerColor }} />
                    )}
                    <div className={`text-[10px] font-bold tabular-nums ${bright ? "text-slate-700" : "text-slate-200"}`}>P{p.partition}</div>
                    <div className={`text-[9px] font-mono tabular-nums ${
                      p.lag === 0 ? "text-emerald-500" : p.lag > 1000 ? "text-red-500" : "text-amber-500"
                    }`}>{p.lag > 999 ? `${(p.lag / 1000).toFixed(1)}K` : p.lag}</div>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}

      <div className="flex items-center gap-4">
        <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>Lag scale:</span>
        <div className="flex items-center gap-1">
          <span className="w-4 h-3 rounded-sm" style={{ backgroundColor: bright ? "rgba(16,185,129,0.15)" : "rgba(16,185,129,0.1)" }} />
          <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>0</span>
        </div>
        <div className="flex items-center gap-1">
          <span className="w-4 h-3 rounded-sm" style={{ backgroundColor: bright ? "rgba(245,158,11,0.25)" : "rgba(245,158,11,0.2)" }} />
          <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>medium</span>
        </div>
        <div className="flex items-center gap-1">
          <span className="w-4 h-3 rounded-sm" style={{ backgroundColor: bright ? "rgba(239,68,68,0.35)" : "rgba(239,68,68,0.25)" }} />
          <span className={`text-[9px] ${bright ? "text-slate-400" : "text-slate-500"}`}>high</span>
        </div>
      </div>
    </div>
  );
}

interface LagSnapshot {
  ts: number;
  totalLag: number;
  perTopic: Record<string, number>;
}

function LagTrendPanel({ offsets, groupId, bright }: {
  offsets: { topic: string; partition: number; currentOffset: number; endOffset: number; lag: number }[];
  groupId: string;
  bright: boolean;
}) {
  const storageKey = `lag-trend-${groupId}`;
  const [history, setHistory] = useState<LagSnapshot[]>(() => {
    try {
      const raw = localStorage.getItem(storageKey);
      return raw ? JSON.parse(raw) : [];
    } catch { return []; }
  });
  const [autoCapture, setAutoCapture] = useState(true);

  // Capture a snapshot every 10 seconds and persist to localStorage
  useEffect(() => {
    const capture = () => {
      const perTopic: Record<string, number> = {};
      let totalLag = 0;
      for (const o of offsets) {
        perTopic[o.topic] = (perTopic[o.topic] || 0) + o.lag;
        totalLag += o.lag;
      }
      setHistory((prev) => {
        // Keep max 1000 entries (~2.7 hours at 10s interval)
        const next = [...prev, { ts: Date.now(), totalLag, perTopic }];
        const trimmed = next.length > 1000 ? next.slice(-1000) : next;
        try { localStorage.setItem(storageKey, JSON.stringify(trimmed)); } catch { /* quota exceeded */ }
        return trimmed;
      });
    };
    capture();
    if (!autoCapture) return;
    const iv = setInterval(capture, 10000);
    return () => clearInterval(iv);
  }, [offsets, storageKey, autoCapture]);

  const topics = useMemo(() => {
    const set = new Set<string>();
    history.forEach((s) => Object.keys(s.perTopic).forEach((t) => set.add(t)));
    return [...set].sort();
  }, [history]);

  const clearHistory = () => {
    setHistory([]);
    localStorage.removeItem(storageKey);
  };

  const cardCls = `rounded-2xl border ${bright ? "border-slate-200 bg-white" : "border-slate-700/50 bg-slate-800/50"} p-5`;
  const colors = ["#3b82f6", "#8b5cf6", "#06b6d4", "#10b981", "#f59e0b", "#ef4444", "#ec4899", "#6366f1"];

  // Stats
  const currentLag = history.length > 0 ? history[history.length - 1].totalLag : 0;
  const minLag = history.length > 0 ? Math.min(...history.map((s) => s.totalLag)) : 0;
  const maxLag = history.length > 0 ? Math.max(...history.map((s) => s.totalLag)) : 0;
  const avgLag = history.length > 0 ? history.reduce((s, h) => s + h.totalLag, 0) / history.length : 0;
  const lagTrend = history.length >= 2 ? history[history.length - 1].totalLag - history[history.length - 2].totalLag : 0;

  return (
    <div className="space-y-5">
      {/* Summary Stats */}
      <div className={cardCls}>
        <div className="flex items-center justify-between mb-4">
          <h3 className={`font-semibold ${bright ? "text-slate-700" : "text-slate-200"}`}>Lag Trend — Persistent History</h3>
          <div className="flex items-center gap-2">
            <button onClick={() => setAutoCapture(!autoCapture)} className={`text-xs px-2 py-1 rounded ${autoCapture ? "bg-emerald-500/20 text-emerald-400" : bright ? "bg-slate-100 text-slate-500" : "bg-slate-700 text-slate-400"}`}>
              {autoCapture ? "Capturing" : "Paused"}
            </button>
            <button onClick={clearHistory} className={`text-xs px-2 py-1 rounded ${bright ? "bg-red-50 text-red-600" : "bg-red-500/10 text-red-400"}`}>Clear</button>
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <div>
            <div className={`text-xl font-bold ${bright ? "text-slate-800" : "text-white"}`}>{fmt(currentLag)}</div>
            <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Current</div>
          </div>
          <div>
            <div className={`text-xl font-bold ${bright ? "text-slate-800" : "text-white"}`}>{fmt(minLag)}</div>
            <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Min</div>
          </div>
          <div>
            <div className={`text-xl font-bold ${bright ? "text-slate-800" : "text-white"}`}>{fmt(maxLag)}</div>
            <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Max</div>
          </div>
          <div>
            <div className={`text-xl font-bold ${bright ? "text-slate-800" : "text-white"}`}>{fmt(Math.round(avgLag))}</div>
            <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Average</div>
          </div>
          <div>
            <div className={`text-xl font-bold ${lagTrend > 0 ? (bright ? "text-red-600" : "text-red-400") : lagTrend < 0 ? (bright ? "text-emerald-600" : "text-emerald-400") : (bright ? "text-slate-800" : "text-white")}`}>
              {lagTrend > 0 ? `+${fmt(lagTrend)}` : lagTrend < 0 ? fmt(lagTrend) : "—"}
            </div>
            <div className={`text-[10px] uppercase tracking-wider ${bright ? "text-slate-400" : "text-slate-500"}`}>Trend</div>
          </div>
        </div>
        <div className={`mt-2 text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>{history.length} samples | {history.length > 1 ? `${((Date.now() - history[0].ts) / 60000).toFixed(0)} min span` : "just started"}</div>
      </div>

      {/* Total Lag Chart */}
      {history.length > 1 && (
        <div className={cardCls}>
          <h3 className={`font-semibold mb-3 ${bright ? "text-slate-700" : "text-slate-200"}`}>Total Lag Over Time</h3>
          <svg viewBox="0 0 600 160" className="w-full" style={{ height: 160 }}>
            {(() => {
              const minTs = history[0].ts;
              const maxTs = history[history.length - 1].ts;
              const rangeTs = Math.max(1, maxTs - minTs);
              const rangeLag = Math.max(1, maxLag - minLag);
              const pts = history.map((s) => {
                const x = 40 + ((s.ts - minTs) / rangeTs) * 540;
                const y = 140 - ((s.totalLag - minLag) / rangeLag) * 120;
                return `${x},${y}`;
              });
              const fillPts = [`40,140`, ...pts, `${40 + 540},140`];
              // Color based on current lag level
              const color = currentLag > maxLag * 0.8 ? "#ef4444" : currentLag > maxLag * 0.5 ? "#f59e0b" : "#10b981";
              return (
                <>
                  <defs><linearGradient id="lagTrendGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={color} stopOpacity="0.3" /><stop offset="100%" stopColor={color} stopOpacity="0" /></linearGradient></defs>
                  {/* Grid lines */}
                  {[0, 0.25, 0.5, 0.75, 1].map((f) => (
                    <line key={f} x1="40" y1={140 - f * 120} x2="580" y2={140 - f * 120} stroke={bright ? "#e2e8f0" : "#334155"} strokeWidth="0.5" />
                  ))}
                  <polygon points={fillPts.join(" ")} fill="url(#lagTrendGrad)" />
                  <polyline points={pts.join(" ")} fill="none" stroke={color} strokeWidth="2" />
                  {/* Y-axis labels */}
                  <text x="36" y="15" fontSize="9" fill={bright ? "#94a3b8" : "#64748b"} textAnchor="end">{fmt(maxLag)}</text>
                  <text x="36" y="145" fontSize="9" fill={bright ? "#94a3b8" : "#64748b"} textAnchor="end">{fmt(minLag)}</text>
                </>
              );
            })()}
          </svg>
        </div>
      )}

      {/* Per-Topic Lag Breakdown */}
      {topics.length > 0 && history.length > 1 && (
        <div className={cardCls}>
          <h3 className={`font-semibold mb-3 ${bright ? "text-slate-700" : "text-slate-200"}`}>Per-Topic Lag Trend</h3>
          <svg viewBox="0 0 600 160" className="w-full" style={{ height: 160 }}>
            {(() => {
              const minTs = history[0].ts;
              const maxTs = history[history.length - 1].ts;
              const rangeTs = Math.max(1, maxTs - minTs);
              let globalMax = 0;
              topics.forEach((t) => {
                history.forEach((s) => { globalMax = Math.max(globalMax, s.perTopic[t] || 0); });
              });
              if (globalMax === 0) globalMax = 1;
              return (
                <>
                  {[0, 0.5, 1].map((f) => (
                    <line key={f} x1="40" y1={140 - f * 120} x2="580" y2={140 - f * 120} stroke={bright ? "#e2e8f0" : "#334155"} strokeWidth="0.5" />
                  ))}
                  {topics.slice(0, 8).map((topic, ti) => {
                    const pts = history.map((s) => {
                      const x = 40 + ((s.ts - minTs) / rangeTs) * 540;
                      const y = 140 - ((s.perTopic[topic] || 0) / globalMax) * 120;
                      return `${x},${y}`;
                    }).join(" ");
                    return <polyline key={topic} points={pts} fill="none" stroke={colors[ti % colors.length]} strokeWidth="1.5" opacity="0.8" />;
                  })}
                  <text x="36" y="15" fontSize="9" fill={bright ? "#94a3b8" : "#64748b"} textAnchor="end">{fmt(globalMax)}</text>
                  <text x="36" y="145" fontSize="9" fill={bright ? "#94a3b8" : "#64748b"} textAnchor="end">0</text>
                </>
              );
            })()}
          </svg>
          <div className="flex flex-wrap gap-3 mt-2">
            {topics.slice(0, 8).map((t, i) => (
              <div key={t} className="flex items-center gap-1.5">
                <span className="w-3 h-2 rounded-sm" style={{ backgroundColor: colors[i % colors.length] }} />
                <span className={`text-[10px] ${bright ? "text-slate-500" : "text-slate-400"}`}>{t}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Current Per-Topic Breakdown */}
      {history.length > 0 && topics.length > 0 && (
        <div className={cardCls}>
          <h3 className={`font-semibold mb-3 ${bright ? "text-slate-700" : "text-slate-200"}`}>Current Lag by Topic</h3>
          <div className="space-y-2">
            {topics.map((t, i) => {
              const lag = history[history.length - 1].perTopic[t] || 0;
              const pct = maxLag > 0 ? (lag / maxLag) * 100 : 0;
              return (
                <div key={t}>
                  <div className="flex justify-between text-xs mb-0.5">
                    <span className={bright ? "text-slate-600" : "text-slate-300"}>{t}</span>
                    <span className={bright ? "text-slate-500" : "text-slate-400"}>{fmt(lag)}</span>
                  </div>
                  <div className={`h-2.5 rounded-full overflow-hidden ${bright ? "bg-slate-100" : "bg-slate-700/50"}`}>
                    <div className="h-full rounded-full" style={{ width: `${Math.max(1, pct)}%`, backgroundColor: colors[i % colors.length] }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
