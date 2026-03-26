import { useEffect, useState, useRef, useMemo } from "react";
import { useKafkaStore } from "../store/kafkaStore";
import { useThemeStore } from "../store/themeStore";
import { useNavigationStore } from "../store/navigationStore";
import { useGraphStore } from "../store/graphStore";

interface MetricSnapshot {
  ts: number;
  totalMessages: number;
  totalLag: number;
  topics: number;
  consumers: number;
  partitions: number;
  underReplicated: number;
}

export function DashboardView() {
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const { setActiveView } = useNavigationStore();
  const connectionStatus = useGraphStore((s) => s.connectionStatus);
  const {
    topics, consumerGroups, brokers, clusterInfo, clusterHealth,
    fetchTopics, fetchConsumerGroups, fetchBrokers, fetchClusterInfo, fetchClusterHealth,
  } = useKafkaStore();

  const [history, setHistory] = useState<MetricSnapshot[]>([]);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  // Initial fetch
  useEffect(() => {
    fetchTopics();
    fetchConsumerGroups();
    fetchBrokers();
    fetchClusterInfo();
    fetchClusterHealth();
  }, [fetchTopics, fetchConsumerGroups, fetchBrokers, fetchClusterInfo, fetchClusterHealth]);

  // Auto-refresh every 5s
  useEffect(() => {
    intervalRef.current = setInterval(() => {
      fetchTopics();
      fetchConsumerGroups();
      fetchClusterHealth();
    }, 5000);
    return () => clearInterval(intervalRef.current);
  }, [fetchTopics, fetchConsumerGroups, fetchClusterHealth]);

  // Track metric history
  useEffect(() => {
    const totalMessages = topics.reduce((s, t) => s + t.totalMessages, 0);
    const totalLag = consumerGroups.reduce((s, g) => s + (g.totalLag || 0), 0);
    const partitions = topics.reduce((s, t) => s + t.partitions, 0);
    const underReplicated = clusterHealth?.underReplicatedCount || 0;
    setHistory((prev) => [
      ...prev,
      { ts: Date.now(), totalMessages, totalLag, topics: topics.length, consumers: consumerGroups.length, partitions, underReplicated },
    ].slice(-60));
  }, [topics, consumerGroups, clusterHealth]);

  const totalMessages = topics.reduce((s, t) => s + t.totalMessages, 0);
  const totalLag = consumerGroups.reduce((s, g) => s + (g.totalLag || 0), 0);
  const totalPartitions = topics.reduce((s, t) => s + t.partitions, 0);
  const activeGroups = consumerGroups.filter((g) => g.status === "Stable").length;
  const emptyGroups = consumerGroups.filter((g) => g.status === "Empty").length;
  const topTopics = [...topics].sort((a, b) => b.totalMessages - a.totalMessages).slice(0, 5);
  const topLagGroups = [...consumerGroups].sort((a, b) => (b.totalLag || 0) - (a.totalLag || 0)).slice(0, 5);

  // DLQ detection
  const dlqTopics = useMemo(() => {
    const dlqPatterns = [/\.dlq$/i, /\.dead[-_]?letter/i, /\.error$/i, /[-_]dlq$/i, /[-_]dead[-_]?letter/i, /[-_]errors?$/i, /\.retry$/i, /[-_]retry$/i];
    return topics.filter((t) => dlqPatterns.some((p) => p.test(t.name)));
  }, [topics]);
  const dlqMessages = dlqTopics.reduce((s, t) => s + t.totalMessages, 0);

  const healthStatus = (() => {
    if (!clusterHealth) return "unknown";
    if (clusterHealth.offlinePartitionCount > 0) return "critical";
    if (clusterHealth.underReplicatedCount > 0) return "warning";
    return "healthy";
  })();

  const healthColor = healthStatus === "healthy"
    ? isBright ? "text-emerald-600" : "text-emerald-400"
    : healthStatus === "warning"
      ? isBright ? "text-amber-600" : "text-amber-400"
      : healthStatus === "critical"
        ? isBright ? "text-red-600" : "text-red-400"
        : isBright ? "text-slate-500" : "text-slate-400";

  const healthBg = healthStatus === "healthy"
    ? isBright ? "bg-emerald-50 border-emerald-200/60" : "bg-emerald-500/10 border-emerald-500/20"
    : healthStatus === "warning"
      ? isBright ? "bg-amber-50 border-amber-200/60" : "bg-amber-500/10 border-amber-500/20"
      : healthStatus === "critical"
        ? isBright ? "bg-red-50 border-red-200/60" : "bg-red-500/10 border-red-500/20"
        : isBright ? "bg-slate-50 border-slate-200/60" : "bg-slate-800/50 border-slate-700/30";

  const Sparkline = ({ data, color, height = 32 }: { data: number[]; color: string; height?: number }) => {
    if (data.length < 2) return null;
    const max = Math.max(...data, 1);
    const min = Math.min(...data);
    const range = max - min || 1;
    const w = 120;
    const points = data.map((v, i) => {
      const x = (i / (data.length - 1)) * w;
      const y = height - ((v - min) / range) * (height - 4) - 2;
      return `${x},${y}`;
    }).join(" ");
    return (
      <svg width={w} height={height} className="shrink-0">
        <polyline points={points} fill="none" stroke={color} strokeWidth={1.5} strokeLinejoin="round" />
      </svg>
    );
  };

  const card = (cls: string = "") => `rounded-2xl border p-5 ${isBright ? `bg-white/80 border-slate-200/60 ${cls}` : `bg-slate-900/60 border-slate-700/30 ${cls}`}`;

  return (
    <div className={`flex-1 overflow-y-auto px-8 py-6 ${isBright ? "text-slate-800" : "text-white"}`}>
      {/* Header with Health Score */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Cluster Dashboard</h1>
          <p className={`text-sm mt-0.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
            {clusterInfo?.clusterId ? `Cluster ${clusterInfo.clusterId.slice(0, 8)}...` : "Kafka Cluster"} &middot; {brokers.length} brokers
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Health Score */}
          {(() => {
            let score = 100;
            const deductions: string[] = [];
            if (clusterHealth?.offlinePartitionCount && clusterHealth.offlinePartitionCount > 0) {
              score -= Math.min(40, clusterHealth.offlinePartitionCount * 20);
              deductions.push(`${clusterHealth.offlinePartitionCount} offline partitions`);
            }
            if (clusterHealth?.underReplicatedCount && clusterHealth.underReplicatedCount > 0) {
              score -= Math.min(20, clusterHealth.underReplicatedCount * 5);
              deductions.push(`${clusterHealth.underReplicatedCount} under-replicated`);
            }
            const deadGroups = consumerGroups.filter((g) => g.status === "Dead").length;
            if (deadGroups > 0) { score -= Math.min(10, deadGroups * 3); deductions.push(`${deadGroups} dead groups`); }
            const highLag = consumerGroups.filter((g) => (g.totalLag || 0) > 10000).length;
            if (highLag > 0) { score -= Math.min(10, highLag * 2); deductions.push(`${highLag} high-lag groups`); }
            const singleReplica = topics.filter((t) => t.replicationFactor <= 1).length;
            if (singleReplica > 0) { score -= Math.min(10, singleReplica); deductions.push(`${singleReplica} RF=1 topics`); }
            if (connectionStatus !== "connected") { score -= 30; deductions.push("disconnected"); }
            if (brokers.length <= 1 && brokers.length > 0) { score -= 5; deductions.push("single broker"); }
            score = Math.max(0, score);
            const scoreColor = score >= 90 ? "#10b981" : score >= 70 ? "#f59e0b" : score >= 50 ? "#f97316" : "#ef4444";
            const circumference = 2 * Math.PI * 20;
            const filled = (score / 100) * circumference;
            return (
              <div className={`flex items-center gap-3 px-4 py-2.5 rounded-xl border ${healthBg}`} title={deductions.length > 0 ? `Deductions: ${deductions.join(", ")}` : "Perfect health"}>
                <div className="relative">
                  <svg width="52" height="52" viewBox="0 0 52 52">
                    <circle cx="26" cy="26" r="20" fill="none" stroke={isBright ? "#e2e8f0" : "#1e293b"} strokeWidth="4" />
                    <circle cx="26" cy="26" r="20" fill="none" stroke={scoreColor} strokeWidth="4"
                      strokeDasharray={`${filled} ${circumference - filled}`} strokeDashoffset={circumference / 4} strokeLinecap="round" />
                    <text x="26" y="30" textAnchor="middle" fontSize="14" fontWeight="bold" fill={scoreColor} fontFamily="ui-monospace, monospace">
                      {score}
                    </text>
                  </svg>
                </div>
                <div>
                  <div className={`text-xs font-semibold capitalize ${healthColor}`}>{healthStatus}</div>
                  <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                    {deductions.length === 0 ? "All checks passing" : deductions.slice(0, 2).join(", ")}
                  </div>
                </div>
              </div>
            );
          })()}
        </div>
      </div>

      {/* Key metrics row */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {[
          { label: "Topics", value: topics.length, spark: history.map((h) => h.topics), color: "#6366f1", click: () => setActiveView("topics") },
          { label: "Consumer Groups", value: consumerGroups.length, spark: history.map((h) => h.consumers), color: "#8b5cf6", click: () => setActiveView("consumers") },
          { label: "Brokers", value: brokers.length, spark: [], color: "#0ea5e9", click: () => setActiveView("brokers") },
          { label: "Partitions", value: totalPartitions, spark: history.map((h) => h.partitions), color: "#10b981" },
        ].map((metric) => (
          <div
            key={metric.label}
            className={`${card()} ${metric.click ? "cursor-pointer hover:shadow-md transition-shadow" : ""}`}
            onClick={metric.click}
          >
            <div className={`text-[11px] uppercase tracking-wider font-medium mb-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>{metric.label}</div>
            <div className="flex items-end justify-between">
              <span className={`text-3xl font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>{metric.value.toLocaleString()}</span>
              {metric.spark.length > 1 && <Sparkline data={metric.spark} color={metric.color} />}
            </div>
          </div>
        ))}
      </div>

      {/* Second row: Messages + Lag */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Total Messages</div>
          <div className="flex items-end justify-between">
            <span className={`text-3xl font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>
              {totalMessages > 1000000 ? `${(totalMessages / 1000000).toFixed(1)}M` : totalMessages > 1000 ? `${(totalMessages / 1000).toFixed(1)}K` : totalMessages.toLocaleString()}
            </span>
            <Sparkline data={history.map((h) => h.totalMessages)} color="#6366f1" />
          </div>
          {history.length >= 2 && (
            <div className={`text-[10px] mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              {(() => {
                const diff = (history[history.length - 1]?.totalMessages || 0) - (history[0]?.totalMessages || 0);
                return diff > 0 ? `+${diff.toLocaleString()} since monitoring started` : "No change";
              })()}
            </div>
          )}
        </div>
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Total Consumer Lag</div>
          <div className="flex items-end justify-between">
            <span className={`text-3xl font-bold tabular-nums ${totalLag > 10000 ? (isBright ? "text-red-600" : "text-red-400") : totalLag > 1000 ? (isBright ? "text-amber-600" : "text-amber-400") : isBright ? "text-emerald-600" : "text-emerald-400"}`}>
              {totalLag > 1000000 ? `${(totalLag / 1000000).toFixed(1)}M` : totalLag > 1000 ? `${(totalLag / 1000).toFixed(1)}K` : totalLag.toLocaleString()}
            </span>
            <Sparkline data={history.map((h) => h.totalLag)} color={totalLag > 10000 ? "#ef4444" : totalLag > 1000 ? "#f59e0b" : "#10b981"} />
          </div>
          <div className={`text-[10px] mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
            {activeGroups} stable, {emptyGroups} empty of {consumerGroups.length} groups
          </div>
        </div>
      </div>

      {/* Throughput estimates */}
      {history.length >= 3 && (() => {
        const recent = history.slice(-6);
        const timeDiff = (recent[recent.length - 1].ts - recent[0].ts) / 1000;
        const msgDiff = recent[recent.length - 1].totalMessages - recent[0].totalMessages;
        const lagDiff = recent[recent.length - 1].totalLag - recent[0].totalLag;
        const msgPerSec = timeDiff > 0 ? msgDiff / timeDiff : 0;
        const lagRate = timeDiff > 0 ? lagDiff / timeDiff : 0;
        const throughputData = history.slice(1).map((h, i) => {
          const prev = history[i];
          const dt = (h.ts - prev.ts) / 1000;
          return dt > 0 ? (h.totalMessages - prev.totalMessages) / dt : 0;
        });
        return (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
            <div className={card()}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Message Rate</div>
              <div className="flex items-end justify-between">
                <span className={`text-2xl font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>
                  {msgPerSec > 1000 ? `${(msgPerSec / 1000).toFixed(1)}K` : msgPerSec.toFixed(1)} <span className={`text-sm font-normal ${isBright ? "text-slate-400" : "text-slate-500"}`}>msg/s</span>
                </span>
                <Sparkline data={throughputData} color="#6366f1" />
              </div>
            </div>
            <div className={card()}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Lag Rate</div>
              <div className="flex items-end justify-between">
                <span className={`text-2xl font-bold tabular-nums ${
                  lagRate > 10 ? (isBright ? "text-red-600" : "text-red-400")
                    : lagRate > 0 ? (isBright ? "text-amber-600" : "text-amber-400")
                    : isBright ? "text-emerald-600" : "text-emerald-400"
                }`}>
                  {lagRate > 0 ? "+" : ""}{lagRate.toFixed(1)} <span className={`text-sm font-normal ${isBright ? "text-slate-400" : "text-slate-500"}`}>lag/s</span>
                </span>
              </div>
              <div className={`text-[10px] mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                {lagRate <= 0 ? "Consumers keeping up" : "Consumers falling behind"}
              </div>
            </div>
            <div className={card()}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Consumer Processing Rate</div>
              <div className="flex items-end justify-between">
                <span className={`text-2xl font-bold tabular-nums ${isBright ? "text-slate-800" : "text-white"}`}>
                  {Math.max(0, msgPerSec - lagRate).toFixed(1)} <span className={`text-sm font-normal ${isBright ? "text-slate-400" : "text-slate-500"}`}>msg/s</span>
                </span>
              </div>
              <div className={`text-[10px] mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                Estimated consumer throughput
              </div>
            </div>
          </div>
        );
      })()}

      {/* Message flow rate chart */}
      {history.length >= 4 && (
        <div className={`${card()} mb-6`}>
          <div className="flex items-center justify-between mb-3">
            <span className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Message Flow Over Time</span>
            <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>{history.length} samples</span>
          </div>
          {(() => {
            const w = 700, h = 180, padL = 50, padR = 10, padT = 10, padB = 28;
            const chartW = w - padL - padR, chartH = h - padT - padB;
            // Compute throughput data from history diffs
            const throughputData = history.slice(1).map((pt, i) => {
              const prev = history[i];
              const dt = Math.max((pt.ts - prev.ts) / 1000, 0.1);
              return { ts: pt.ts, msgRate: (pt.totalMessages - prev.totalMessages) / dt, lag: pt.totalLag, msgs: pt.totalMessages };
            });
            if (throughputData.length < 2) return null;
            const maxRate = Math.max(...throughputData.map((d) => d.msgRate), 1);
            const maxLag = Math.max(...throughputData.map((d) => d.lag), 1);
            const toX = (i: number) => padL + (i / (throughputData.length - 1)) * chartW;
            const rateToY = (v: number) => padT + chartH * (1 - v / maxRate);
            const lagToY = (v: number) => padT + chartH * (1 - v / maxLag);
            const rateLine = throughputData.map((d, i) => `${i === 0 ? "M" : "L"}${toX(i).toFixed(1)},${rateToY(d.msgRate).toFixed(1)}`).join(" ");
            const lagLine = throughputData.map((d, i) => `${i === 0 ? "M" : "L"}${toX(i).toFixed(1)},${lagToY(d.lag).toFixed(1)}`).join(" ");
            // Fill under rate curve
            const rateFill = rateLine + ` L${toX(throughputData.length - 1).toFixed(1)},${padT + chartH} L${padL},${padT + chartH} Z`;
            const fmtVal = (v: number) => v > 1000000 ? `${(v / 1000000).toFixed(1)}M` : v > 1000 ? `${(v / 1000).toFixed(0)}K` : v.toFixed(0);
            return (
              <svg width="100%" viewBox={`0 0 ${w} ${h}`} className="select-none">
                {/* Grid lines */}
                {[0, 0.25, 0.5, 0.75, 1].map((pct) => {
                  const y = padT + chartH * (1 - pct);
                  return <line key={pct} x1={padL} y1={y} x2={w - padR} y2={y} stroke={isBright ? "#e2e8f0" : "#1e293b"} strokeWidth={0.5} />;
                })}
                {/* Rate fill */}
                <path d={rateFill} fill={isBright ? "rgba(99,102,241,0.08)" : "rgba(99,102,241,0.1)"} />
                {/* Rate line */}
                <path d={rateLine} fill="none" stroke="#6366f1" strokeWidth={2} strokeLinejoin="round" />
                {/* Lag line (dashed, secondary axis) */}
                <path d={lagLine} fill="none" stroke={totalLag > 10000 ? "#ef4444" : "#f59e0b"} strokeWidth={1.5} strokeDasharray="4 3" strokeLinejoin="round" />
                {/* Y-axis labels (left = rate) */}
                {[0, 0.5, 1].map((pct) => (
                  <text key={`r${pct}`} x={padL - 4} y={padT + chartH * (1 - pct) + 3} textAnchor="end" fontSize={9}
                    fill={isBright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">{fmtVal(maxRate * pct)}</text>
                ))}
                {/* Time labels */}
                {[0, Math.floor(throughputData.length / 2), throughputData.length - 1].map((i) => {
                  const t = new Date(throughputData[i].ts);
                  return (
                    <text key={i} x={toX(i)} y={h - 4} textAnchor="middle" fontSize={9}
                      fill={isBright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">
                      {t.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}
                    </text>
                  );
                })}
                {/* Axis labels */}
                <text x={padL + 4} y={padT + 12} fontSize={9} fill="#6366f1" fontFamily="ui-monospace, monospace">msg/s</text>
                <text x={w - padR - 4} y={padT + 12} textAnchor="end" fontSize={9} fill={totalLag > 10000 ? "#ef4444" : "#f59e0b"} fontFamily="ui-monospace, monospace">lag (dashed)</text>
              </svg>
            );
          })()}
        </div>
      )}

      {/* Health details + Top topics + Top lag groups */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 mb-6">
        {/* Cluster Health */}
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Cluster Health</div>
          <div className="space-y-2">
            {[
              { label: "Under-Replicated", value: clusterHealth?.underReplicatedCount ?? 0, bad: (clusterHealth?.underReplicatedCount ?? 0) > 0 },
              { label: "Offline Partitions", value: clusterHealth?.offlinePartitionCount ?? 0, bad: (clusterHealth?.offlinePartitionCount ?? 0) > 0 },
              { label: "Total Partitions", value: clusterHealth?.totalPartitions ?? totalPartitions, bad: false },
              { label: "Connection", value: connectionStatus, bad: connectionStatus !== "connected" },
            ].map((item) => (
              <div key={item.label} className="flex items-center justify-between">
                <span className={`text-xs ${isBright ? "text-slate-600" : "text-slate-300"}`}>{item.label}</span>
                <span className={`text-xs font-bold font-mono ${
                  item.bad ? (isBright ? "text-red-600" : "text-red-400") : isBright ? "text-slate-700" : "text-white"
                }`}>
                  {typeof item.value === "number" ? item.value.toLocaleString() : item.value}
                </span>
              </div>
            ))}
          </div>
          {clusterHealth && clusterHealth.underReplicated.length > 0 && (
            <div className={`mt-3 pt-2 border-t ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
              <div className={`text-[9px] uppercase tracking-wider mb-1 ${isBright ? "text-amber-600" : "text-amber-400"}`}>Under-Replicated</div>
              {clusterHealth.underReplicated.slice(0, 3).map((ur) => (
                <div key={`${ur.topic}-${ur.partition}`} className={`text-[10px] font-mono ${isBright ? "text-slate-600" : "text-slate-400"}`}>
                  {ur.topic}:P{ur.partition} (ISR {ur.isr}/{ur.replicas})
                </div>
              ))}
              {clusterHealth.underReplicated.length > 3 && (
                <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                  +{clusterHealth.underReplicated.length - 3} more
                </div>
              )}
            </div>
          )}
        </div>

        {/* Top Topics by Messages */}
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Top Topics by Messages</div>
          {topTopics.length === 0 ? (
            <div className={`text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>No topics</div>
          ) : (
            <div className="space-y-2">
              {topTopics.map((t) => {
                const pct = totalMessages > 0 ? (t.totalMessages / totalMessages) * 100 : 0;
                return (
                  <div
                    key={t.name}
                    className="cursor-pointer"
                    onClick={() => { useKafkaStore.getState().fetchTopicDetail(t.name); useNavigationStore.getState().navigateToTopic(t.name); }}
                  >
                    <div className="flex items-center justify-between mb-0.5">
                      <span className={`text-xs font-mono truncate max-w-[160px] ${isBright ? "text-slate-700 hover:text-indigo-600" : "text-slate-300 hover:text-indigo-300"}`}>{t.name}</span>
                      <span className={`text-[10px] font-mono tabular-nums ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                        {t.totalMessages > 1000 ? `${(t.totalMessages / 1000).toFixed(0)}K` : t.totalMessages}
                      </span>
                    </div>
                    <div className={`h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                      <div className={`h-full rounded-full ${isBright ? "bg-indigo-400" : "bg-indigo-500/70"}`} style={{ width: `${Math.max(2, pct)}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Top Consumer Groups by Lag */}
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Top Groups by Lag</div>
          {topLagGroups.length === 0 ? (
            <div className={`text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>No consumer groups</div>
          ) : (
            <div className="space-y-2">
              {topLagGroups.map((g) => {
                const pct = totalLag > 0 ? ((g.totalLag || 0) / totalLag) * 100 : 0;
                return (
                  <div
                    key={g.groupId}
                    className="cursor-pointer"
                    onClick={() => { useNavigationStore.getState().navigateToConsumerGroup(g.groupId); }}
                  >
                    <div className="flex items-center justify-between mb-0.5">
                      <span className={`text-xs font-mono truncate max-w-[160px] ${isBright ? "text-slate-700 hover:text-purple-600" : "text-slate-300 hover:text-purple-300"}`}>{g.groupId}</span>
                      <span className={`text-[10px] font-mono tabular-nums ${
                        (g.totalLag || 0) > 5000 ? (isBright ? "text-red-600" : "text-red-400") : isBright ? "text-slate-500" : "text-slate-400"
                      }`}>
                        {(g.totalLag || 0) > 1000 ? `${((g.totalLag || 0) / 1000).toFixed(1)}K` : g.totalLag || 0}
                      </span>
                    </div>
                    <div className={`h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                      <div className={`h-full rounded-full ${
                        (g.totalLag || 0) > 5000 ? (isBright ? "bg-red-400" : "bg-red-500/70") : isBright ? "bg-purple-400" : "bg-purple-500/70"
                      }`} style={{ width: `${Math.max(2, pct)}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* Broker distribution + Consumer group states */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-6">
        {/* Leader Distribution */}
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Leader Distribution</div>
          {clusterHealth?.leaderDistribution ? (
            <div className="space-y-2">
              {Object.entries(clusterHealth.leaderDistribution).sort((a, b) => Number(a[0]) - Number(b[0])).map(([brokerId, count]) => {
                const total = clusterHealth.totalPartitions || 1;
                const pct = (count / total) * 100;
                const broker = brokers.find((b) => b.id === Number(brokerId));
                return (
                  <div key={brokerId}>
                    <div className="flex items-center justify-between mb-0.5">
                      <span className={`text-xs ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                        Broker {brokerId}{broker?.isController ? " (ctrl)" : ""}{broker?.rack ? ` [${broker.rack}]` : ""}
                      </span>
                      <span className={`text-[10px] font-mono tabular-nums ${isBright ? "text-slate-500" : "text-slate-400"}`}>{count} ({pct.toFixed(0)}%)</span>
                    </div>
                    <div className={`h-2 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                      <div
                        className={`h-full rounded-full transition-all ${isBright ? "bg-sky-400" : "bg-sky-500/70"}`}
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className={`text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>Loading...</div>
          )}
        </div>

        {/* Consumer Group States */}
        <div className={card()}>
          <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Consumer Group States</div>
          {(() => {
            const stateCounts = consumerGroups.reduce<Record<string, number>>((acc, g) => {
              acc[g.status] = (acc[g.status] || 0) + 1;
              return acc;
            }, {});
            const stateColors: Record<string, string> = {
              Stable: isBright ? "bg-emerald-400" : "bg-emerald-500/80",
              Empty: isBright ? "bg-slate-300" : "bg-slate-500/60",
              Dead: isBright ? "bg-red-400" : "bg-red-500/80",
              PreparingRebalance: isBright ? "bg-amber-400" : "bg-amber-500/80",
              CompletingRebalance: isBright ? "bg-amber-300" : "bg-amber-400/80",
            };
            const entries = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]);
            const total = consumerGroups.length || 1;
            return (
              <>
                {/* Stacked bar */}
                <div className="flex h-6 rounded-lg overflow-hidden mb-3">
                  {entries.map(([state, count]) => (
                    <div
                      key={state}
                      className={`${stateColors[state] || (isBright ? "bg-slate-200" : "bg-slate-600")} transition-all`}
                      style={{ width: `${(count / total) * 100}%` }}
                      title={`${state}: ${count}`}
                    />
                  ))}
                </div>
                <div className="grid grid-cols-2 gap-2">
                  {entries.map(([state, count]) => (
                    <div key={state} className="flex items-center gap-2">
                      <span className={`w-2.5 h-2.5 rounded-sm ${stateColors[state] || (isBright ? "bg-slate-200" : "bg-slate-600")}`} />
                      <span className={`text-xs ${isBright ? "text-slate-600" : "text-slate-300"}`}>{state}</span>
                      <span className={`text-xs font-bold ml-auto ${isBright ? "text-slate-700" : "text-white"}`}>{count}</span>
                    </div>
                  ))}
                </div>
              </>
            );
          })()}
        </div>
      </div>

      {/* Topic-Consumer Dependency Graph */}
      {(() => {
        const topicConsumerMap = useMemo(() => {
          const map: Record<string, string[]> = {};
          consumerGroups.forEach((g) => {
            (g.topics || []).forEach((t) => {
              if (!map[t]) map[t] = [];
              if (!map[t].includes(g.groupId)) map[t].push(g.groupId);
            });
          });
          return map;
        }, [consumerGroups]);

        const connectedTopics = Object.keys(topicConsumerMap).slice(0, 12);
        const connectedGroups = [...new Set(connectedTopics.flatMap((t) => topicConsumerMap[t]))].slice(0, 12);

        if (connectedTopics.length === 0) return null;

        const topicColors = ["#6366f1", "#8b5cf6", "#0ea5e9", "#10b981", "#f59e0b", "#ef4444", "#ec4899", "#14b8a6", "#f97316", "#a855f7", "#06b6d4", "#84cc16"];
        const leftX = 20;
        const rightX = 380;
        const svgW = 440;
        const rowH = 28;
        const topPad = 16;
        const svgH = Math.max(connectedTopics.length, connectedGroups.length) * rowH + topPad * 2;

        return (
          <div className={`${card()} mb-6`}>
            <div className="flex items-center justify-between mb-3">
              <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Topic → Consumer Group Dependencies</div>
              <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                {connectedTopics.length} topics · {connectedGroups.length} groups
              </span>
            </div>
            <div className="overflow-x-auto">
              <svg width="100%" viewBox={`0 0 ${svgW} ${svgH}`} className="min-w-[400px]">
                {/* Connection lines */}
                {connectedTopics.map((topic, ti) => {
                  const y1 = topPad + ti * rowH + rowH / 2;
                  return topicConsumerMap[topic].map((gId) => {
                    const gi = connectedGroups.indexOf(gId);
                    if (gi < 0) return null;
                    const y2 = topPad + gi * rowH + rowH / 2;
                    return (
                      <path
                        key={`${topic}-${gId}`}
                        d={`M${leftX + 120},${y1} C${leftX + 200},${y1} ${rightX - 80},${y2} ${rightX - 60},${y2}`}
                        fill="none"
                        stroke={topicColors[ti % topicColors.length]}
                        strokeWidth={1.5}
                        opacity={0.4}
                        strokeLinecap="round"
                      />
                    );
                  });
                })}
                {/* Topic labels (left) */}
                {connectedTopics.map((topic, i) => {
                  const y = topPad + i * rowH + rowH / 2;
                  return (
                    <g key={`t-${topic}`} className="cursor-pointer" onClick={() => { useKafkaStore.getState().fetchTopicDetail(topic); useNavigationStore.getState().navigateToTopic(topic); }}>
                      <circle cx={leftX + 6} cy={y} r={4} fill={topicColors[i % topicColors.length]} opacity={0.8} />
                      <text
                        x={leftX + 16}
                        y={y + 4}
                        fontSize={10}
                        fontFamily="ui-monospace, monospace"
                        fill={isBright ? "#475569" : "#94a3b8"}
                      >
                        {topic.length > 18 ? topic.slice(0, 17) + "…" : topic}
                      </text>
                    </g>
                  );
                })}
                {/* Consumer group labels (right) */}
                {connectedGroups.map((gId, i) => {
                  const y = topPad + i * rowH + rowH / 2;
                  const group = consumerGroups.find((g) => g.groupId === gId);
                  const stateColor = group?.status === "Stable" ? "#10b981" : group?.status === "Empty" ? "#64748b" : "#f59e0b";
                  return (
                    <g key={`g-${gId}`} className="cursor-pointer" onClick={() => useNavigationStore.getState().navigateToConsumerGroup(gId)}>
                      <circle cx={rightX - 54} cy={y} r={4} fill={stateColor} opacity={0.8} />
                      <text
                        x={rightX - 44}
                        y={y + 4}
                        fontSize={10}
                        fontFamily="ui-monospace, monospace"
                        fill={isBright ? "#475569" : "#94a3b8"}
                      >
                        {gId.length > 18 ? gId.slice(0, 17) + "…" : gId}
                      </text>
                    </g>
                  );
                })}
              </svg>
            </div>
          </div>
        );
      })()}

      {/* Alerts & Notifications */}
      {(() => {
        const alerts: { severity: "critical" | "warning" | "info"; message: string; action?: () => void }[] = [];
        if (clusterHealth?.offlinePartitionCount && clusterHealth.offlinePartitionCount > 0) {
          alerts.push({ severity: "critical", message: `${clusterHealth.offlinePartitionCount} offline partition(s) detected` });
        }
        if (clusterHealth?.underReplicatedCount && clusterHealth.underReplicatedCount > 0) {
          alerts.push({ severity: "warning", message: `${clusterHealth.underReplicatedCount} under-replicated partition(s)` });
        }
        const deadGroups = consumerGroups.filter((g) => g.status === "Dead");
        if (deadGroups.length > 0) {
          alerts.push({ severity: "warning", message: `${deadGroups.length} dead consumer group(s)`, action: () => setActiveView("consumers") });
        }
        const rebalancingGroups = consumerGroups.filter((g) => g.status === "PreparingRebalance" || g.status === "CompletingRebalance");
        if (rebalancingGroups.length > 0) {
          alerts.push({ severity: "info", message: `${rebalancingGroups.length} consumer group(s) rebalancing` });
        }
        const highLagGroups = consumerGroups.filter((g) => (g.totalLag || 0) > 10000);
        if (highLagGroups.length > 0) {
          alerts.push({ severity: "warning", message: `${highLagGroups.length} consumer group(s) with high lag (>10K)`, action: () => setActiveView("consumers") });
        }
        const singleReplicaTopics = topics.filter((t) => t.replicationFactor <= 1);
        if (singleReplicaTopics.length > 0) {
          alerts.push({ severity: "info", message: `${singleReplicaTopics.length} topic(s) with replication factor 1 (no fault tolerance)`, action: () => setActiveView("topics") });
        }
        if (dlqTopics.length > 0 && dlqMessages > 0) {
          alerts.push({ severity: "warning", message: `${dlqTopics.length} DLQ topic(s) with ${dlqMessages.toLocaleString()} messages`, action: () => setActiveView("topics") });
        }
        if (connectionStatus !== "connected") {
          alerts.push({ severity: "critical", message: `Server connection: ${connectionStatus}` });
        }

        if (alerts.length === 0) return null;

        const sevColors = {
          critical: isBright ? "border-red-200/80 bg-red-50/60" : "border-red-500/20 bg-red-500/[0.06]",
          warning: isBright ? "border-amber-200/80 bg-amber-50/60" : "border-amber-500/20 bg-amber-500/[0.06]",
          info: isBright ? "border-blue-200/80 bg-blue-50/60" : "border-blue-500/20 bg-blue-500/[0.06]",
        };
        const sevDot = {
          critical: "bg-red-500 animate-pulse",
          warning: "bg-amber-500",
          info: "bg-blue-400",
        };
        const sevText = {
          critical: isBright ? "text-red-700" : "text-red-300",
          warning: isBright ? "text-amber-700" : "text-amber-300",
          info: isBright ? "text-blue-700" : "text-blue-300",
        };

        return (
          <div className={`${card()} mb-6`}>
            <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Alerts ({alerts.length})
            </div>
            <div className="space-y-1.5">
              {alerts.map((a, i) => (
                <div
                  key={i}
                  className={`flex items-center gap-2.5 px-3 py-2 rounded-xl border ${sevColors[a.severity]} ${a.action ? "cursor-pointer" : ""}`}
                  onClick={a.action}
                >
                  <span className={`w-2 h-2 rounded-full shrink-0 ${sevDot[a.severity]}`} />
                  <span className={`text-xs font-medium flex-1 ${sevText[a.severity]}`}>{a.message}</span>
                  {a.action && (
                    <svg className={`w-3.5 h-3.5 shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path d="M9 5l7 7-7 7" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  )}
                </div>
              ))}
            </div>
          </div>
        );
      })()}

      {/* Dead Letter Queues */}
      {dlqTopics.length > 0 && (
        <div className={`${card()} mb-6`}>
          <div className="flex items-center justify-between mb-3">
            <div className={`text-[11px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Dead Letter Queues ({dlqTopics.length})
            </div>
            <span className={`text-[10px] font-mono ${dlqMessages > 0 ? (isBright ? "text-amber-600" : "text-amber-400") : isBright ? "text-slate-400" : "text-slate-500"}`}>
              {dlqMessages.toLocaleString()} total messages
            </span>
          </div>
          <div className="space-y-2">
            {dlqTopics.slice(0, 8).map((t) => {
              const pct = dlqMessages > 0 ? (t.totalMessages / dlqMessages) * 100 : 0;
              return (
                <div
                  key={t.name}
                  className="cursor-pointer"
                  onClick={() => { useKafkaStore.getState().fetchTopicDetail(t.name); useNavigationStore.getState().navigateToTopic(t.name); }}
                >
                  <div className="flex items-center justify-between mb-0.5">
                    <span className={`text-xs font-mono truncate max-w-[200px] ${isBright ? "text-slate-700 hover:text-red-600" : "text-slate-300 hover:text-red-300"}`}>{t.name}</span>
                    <div className="flex items-center gap-2">
                      <span className={`text-[10px] font-mono tabular-nums ${
                        t.totalMessages > 1000 ? (isBright ? "text-red-600" : "text-red-400") : isBright ? "text-slate-500" : "text-slate-400"
                      }`}>
                        {t.totalMessages > 1000 ? `${(t.totalMessages / 1000).toFixed(1)}K` : t.totalMessages}
                      </span>
                      <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold uppercase ${
                        t.totalMessages > 0
                          ? isBright ? "bg-red-50 text-red-600" : "bg-red-500/10 text-red-400"
                          : isBright ? "bg-emerald-50 text-emerald-600" : "bg-emerald-500/10 text-emerald-400"
                      }`}>
                        {t.totalMessages > 0 ? "active" : "empty"}
                      </span>
                    </div>
                  </div>
                  <div className={`h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                    <div className={`h-full rounded-full ${
                      t.totalMessages > 1000 ? (isBright ? "bg-red-400" : "bg-red-500/70") : isBright ? "bg-amber-400" : "bg-amber-500/70"
                    }`} style={{ width: `${Math.max(2, pct)}%` }} />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Quick actions */}
      <div className={card("mb-6")}>
        <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Quick Actions</div>
        <div className="flex flex-wrap gap-2">
          {[
            { label: "View Pipeline", view: "pipeline" as const },
            { label: "Browse Topics", view: "topics" as const },
            { label: "Consumer Groups", view: "consumers" as const },
            { label: "Broker Status", view: "brokers" as const },
            { label: "Schema Registry", view: "schemas" as const },
            { label: "ACLs", view: "acls" as const },
            { label: "Quotas", view: "quotas" as const },
            { label: "Connectors", view: "connectors" as const },
            { label: "Settings", view: "settings" as const },
          ].map((action) => (
            <button
              key={action.label}
              onClick={() => setActiveView(action.view)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
                isBright
                  ? "bg-white border-slate-200 text-slate-600 hover:bg-indigo-50 hover:border-indigo-200/60 hover:text-indigo-700"
                  : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-indigo-500/15 hover:border-indigo-500/30 hover:text-indigo-300"
              }`}
            >
              {action.label}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
