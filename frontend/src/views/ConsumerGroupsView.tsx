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
import { ConsumerGroupDetail } from "./ConsumerGroupDetail";
import { useFavoritesStore } from "../store/favoritesStore";

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export function ConsumerGroupsView() {
  const { consumerGroups, consumerGroupsLoading, fetchConsumerGroups, deleteConsumerGroup, consumerGroupsLastFetched } = useKafkaStore();
  const { theme } = useThemeStore();
  const addToast = useToastStore((s) => s.addToast);
  const lagWarnThreshold = useGraphStore((s) => s.config.lagWarnThreshold);
  const isBright = theme === "bright";
  const { pendingGroupId, clearPending } = useNavigationStore();
  const [selectedGroupId, setSelectedGroupId] = useState<string | null>(pendingGroupId);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [showDelete, setShowDelete] = useState<string | null>(null);
  const [deleteError, setDeleteError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  useEffect(() => { fetchConsumerGroups(); }, [fetchConsumerGroups]);

  // Handle deep-link navigation from pipeline
  useEffect(() => {
    if (pendingGroupId) {
      setSelectedGroupId(pendingGroupId);
      clearPending();
    }
  }, [pendingGroupId, clearPending]);

  // Auto-refresh every 5 seconds
  useEffect(() => {
    if (autoRefresh && !selectedGroupId) {
      intervalRef.current = setInterval(() => fetchConsumerGroups(), 5000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, selectedGroupId, fetchConsumerGroups]);

  if (selectedGroupId) {
    return <ConsumerGroupDetail groupId={selectedGroupId} onBack={() => setSelectedGroupId(null)} />;
  }

  const totalLag = consumerGroups.reduce((s, g) => s + (g.totalLag || 0), 0);
  const stableCount = consumerGroups.filter((g) => g.status === "Stable").length;
  const alertCount = consumerGroups.filter((g) => (g.totalLag || 0) > lagWarnThreshold).length;

  // Unique statuses and filtered data
  const uniqueStatuses = [...new Set(consumerGroups.map((g) => String(g.status)))].sort();
  const filteredGroups = statusFilter
    ? consumerGroups.filter((g) => g.status === statusFilter)
    : consumerGroups;

  const columns = [
    { key: "groupId", label: "Group ID", render: (r: Record<string, unknown>) => {
      const gid = String(r.groupId);
      const isFav = useFavoritesStore.getState().isFavoriteGroup(gid);
      return (
        <span className="flex items-center gap-1.5">
          <button
            onClick={(e) => { e.stopPropagation(); useFavoritesStore.getState().toggleFavoriteGroup(gid); }}
            className={`shrink-0 cursor-pointer transition-colors ${isFav ? "text-amber-400" : isBright ? "text-slate-300 hover:text-amber-400" : "text-slate-600 hover:text-amber-400"}`}
            title={isFav ? "Remove from favorites" : "Add to favorites"}
          >
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill={isFav ? "currentColor" : "none"} stroke="currentColor" strokeWidth={2}>
              <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" strokeLinejoin="round" />
            </svg>
          </button>
          <span className={`font-mono font-medium ${isBright ? "text-amber-600" : "text-amber-300"}`}>{gid}</span>
        </span>
      );
    }},
    { key: "status", label: "Status", className: "w-28", render: (r: Record<string, unknown>) => {
      const s = String(r.status);
      const color = s === "Stable"
        ? isBright ? "bg-emerald-50 text-emerald-700 border-emerald-200" : "bg-emerald-500/15 text-emerald-400 border-emerald-500/25"
        : s === "Empty"
          ? isBright ? "bg-slate-100 text-slate-500 border-slate-200" : "bg-slate-800/50 text-slate-400 border-slate-700/40"
          : isBright ? "bg-amber-50 text-amber-700 border-amber-200" : "bg-amber-500/15 text-amber-400 border-amber-500/25";
      return <span className={`text-[10px] font-semibold uppercase px-2 py-1 rounded-lg border ${color}`}>{s}</span>;
    }},
    { key: "members", label: "Members", className: "w-24 text-center" },
    { key: "totalLag", label: "Total Lag", className: "w-40 text-right", render: (r: Record<string, unknown>) => {
      const lag = Number(r.totalLag);
      const color = lag > 1000 ? "text-red-500" : lag > 0 ? "text-amber-500" : "text-emerald-500";
      const barColor = lag > 1000 ? "bg-red-500" : lag > 100 ? "bg-amber-500" : "bg-emerald-500";
      const barWidth = lag > 0 ? Math.min(100, Math.log10(lag + 1) * 25) : 0;
      return (
        <div className="flex items-center gap-2 justify-end">
          {lag > 0 && (
            <div className={`w-16 h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/60"}`}>
              <div className={`h-full rounded-full ${barColor}`} style={{ width: `${barWidth}%` }} />
            </div>
          )}
          <span className={`font-mono font-medium tabular-nums ${color}`}>{fmt(lag)}</span>
        </div>
      );
    }},
    { key: "topics", label: "Topics", render: (r: Record<string, unknown>) => {
      const topics = r.topics as string[];
      if (!topics || topics.length === 0) return <span className={isBright ? "text-slate-400" : "text-slate-500"}>-</span>;
      return (
        <div className="flex gap-1 flex-wrap">
          {topics.slice(0, 3).map((t) => (
            <span key={t} className={`text-[9px] rounded-md px-1.5 py-0.5 border ${
              isBright ? "bg-indigo-50 text-indigo-600 border-indigo-200/50" : "bg-indigo-500/10 text-indigo-300/80 border-indigo-500/15"
            }`}>{t}</span>
          ))}
          {topics.length > 3 && <span className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>+{topics.length - 3}</span>}
        </div>
      );
    }},
    { key: "_actions", label: "", sortable: false, className: "w-16 text-right", render: (r: Record<string, unknown>) => (
      <button
        onClick={(e) => { e.stopPropagation(); setShowDelete(String(r.groupId)); }}
        className="text-xs text-red-400/50 hover:text-red-500 transition-colors cursor-pointer"
      >
        Delete
      </button>
    )},
  ];

  const handleDelete = async () => {
    if (!showDelete) return;
    setDeleteError(null);
    const groupId = showDelete;
    const result = await deleteConsumerGroup(groupId);
    if (result.success) {
      setShowDelete(null);
      addToast(`Consumer group '${groupId}' deleted`, "success");
      fetchConsumerGroups();
    } else {
      const msg = result.error || "Failed to delete consumer group";
      setDeleteError(msg);
      addToast(msg, "error");
    }
  };

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Consumer Groups</h1>
          <div className="flex items-center gap-3 mt-0.5">
            <p className={`text-sm ${isBright ? "text-slate-500" : "text-slate-500"}`}>Monitor consumer lag, members, and offset management</p>
            <FreshnessIndicator timestamp={consumerGroupsLastFetched} />
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
            onClick={() => fetchConsumerGroups()}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
            }`}
          >
            Refresh
          </button>
          {consumerGroups.length > 0 && (
            <button
              onClick={() => {
                const data = consumerGroups.map((g) => ({
                  groupId: g.groupId,
                  status: g.status,
                  members: g.members,
                  totalLag: g.totalLag,
                  topics: g.topics,
                }));
                const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = "kafka-consumer-groups.json";
                a.click();
                URL.revokeObjectURL(url);
                addToast("Exported consumer groups as JSON", "success", 2000);
              }}
              className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                isBright
                  ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                  : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
              }`}
              title="Export all consumer groups as JSON"
            >
              JSON
            </button>
          )}
        </div>
      </div>

      <div className="grid grid-cols-4 gap-3">
        <SummaryCard label="Consumer Groups" value={String(consumerGroups.length)} color="amber" bright={isBright} />
        <SummaryCard label="Stable" value={String(stableCount)} color="emerald" bright={isBright} />
        <SummaryCard label="Total Lag" value={fmt(totalLag)} color={totalLag > lagWarnThreshold ? "red" : "slate"} bright={isBright} />
        <SummaryCard label={`Alerts (>${fmt(lagWarnThreshold)})`} value={String(alertCount)} color={alertCount > 0 ? "red" : "emerald"} bright={isBright} />
      </div>

      {/* State distribution + Lag distribution */}
      {consumerGroups.length > 0 && (() => {
        const stateCounts = consumerGroups.reduce<Record<string, number>>((acc, g) => {
          acc[g.status] = (acc[g.status] || 0) + 1;
          return acc;
        }, {});
        const stateColors: Record<string, string> = {
          Stable: "#10b981", Empty: "#64748b", Dead: "#ef4444",
          PreparingRebalance: "#f59e0b", CompletingRebalance: "#eab308",
        };
        const entries = Object.entries(stateCounts).sort((a, b) => b[1] - a[1]);
        const total = consumerGroups.length;

        // Donut chart arcs
        const r = 44, cx = 56, cy = 56, inner = 28;
        let cumAngle = -Math.PI / 2;
        const arcs = entries.map(([state, count]) => {
          const angle = (count / total) * Math.PI * 2;
          const startAngle = cumAngle;
          cumAngle += angle;
          const endAngle = cumAngle;
          const x1o = cx + r * Math.cos(startAngle), y1o = cy + r * Math.sin(startAngle);
          const x2o = cx + r * Math.cos(endAngle), y2o = cy + r * Math.sin(endAngle);
          const x1i = cx + inner * Math.cos(endAngle), y1i = cy + inner * Math.sin(endAngle);
          const x2i = cx + inner * Math.cos(startAngle), y2i = cy + inner * Math.sin(startAngle);
          const large = angle > Math.PI ? 1 : 0;
          const d = `M${x1o},${y1o} A${r},${r} 0 ${large} 1 ${x2o},${y2o} L${x1i},${y1i} A${inner},${inner} 0 ${large} 0 ${x2i},${y2i} Z`;
          return { state, count, d, color: stateColors[state] || "#64748b" };
        });

        // Lag histogram buckets
        const lags = consumerGroups.map((g) => g.totalLag || 0);
        const lagBuckets = [
          { label: "0", min: 0, max: 0 },
          { label: "1-100", min: 1, max: 100 },
          { label: "101-1K", min: 101, max: 1000 },
          { label: "1K-10K", min: 1001, max: 10000 },
          { label: "10K-100K", min: 10001, max: 100000 },
          { label: ">100K", min: 100001, max: Infinity },
        ];
        const lagBucketCounts = lagBuckets.map((b) => lags.filter((l) => l >= b.min && l <= b.max).length);
        const maxBucket = Math.max(...lagBucketCounts, 1);

        return (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {/* State donut */}
            <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>State Distribution</div>
              <div className="flex items-center gap-6">
                <svg width={112} height={112} viewBox="0 0 112 112" className="shrink-0">
                  {arcs.map((a) => (
                    <path key={a.state} d={a.d} fill={a.color} opacity={0.8} />
                  ))}
                  <text x={cx} y={cy - 4} textAnchor="middle" fontSize={18} fontWeight="bold"
                    fill={isBright ? "#1e293b" : "#f1f5f9"} fontFamily="ui-monospace, monospace">{total}</text>
                  <text x={cx} y={cy + 10} textAnchor="middle" fontSize={8}
                    fill={isBright ? "#94a3b8" : "#475569"} fontFamily="ui-monospace, monospace">groups</text>
                </svg>
                <div className="flex-1 space-y-1.5">
                  {entries.map(([state, count]) => (
                    <div key={state} className="flex items-center gap-2">
                      <span className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ backgroundColor: stateColors[state] || "#64748b" }} />
                      <span className={`text-xs flex-1 ${isBright ? "text-slate-600" : "text-slate-300"}`}>{state}</span>
                      <span className={`text-xs font-bold font-mono tabular-nums ${isBright ? "text-slate-700" : "text-white"}`}>{count}</span>
                      <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                        ({((count / total) * 100).toFixed(0)}%)
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Lag histogram */}
            <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
              <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Lag Distribution</div>
              <div className="flex items-end gap-2 h-24">
                {lagBuckets.map((b, i) => {
                  const count = lagBucketCounts[i];
                  const pct = (count / maxBucket) * 100;
                  const barColor = b.min === 0 ? (isBright ? "bg-emerald-400" : "bg-emerald-500/70")
                    : b.min <= 1000 ? (isBright ? "bg-amber-400" : "bg-amber-500/70")
                    : (isBright ? "bg-red-400" : "bg-red-500/70");
                  return (
                    <div key={b.label} className="flex-1 flex flex-col items-center gap-1">
                      <span className={`text-[9px] font-mono tabular-nums ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                        {count > 0 ? count : ""}
                      </span>
                      <div className={`w-full rounded-t-md transition-all ${barColor}`}
                        style={{ height: `${Math.max(count > 0 ? 4 : 0, pct)}%` }} />
                    </div>
                  );
                })}
              </div>
              <div className="flex gap-2 mt-1">
                {lagBuckets.map((b) => (
                  <div key={b.label} className={`flex-1 text-center text-[8px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                    {b.label}
                  </div>
                ))}
              </div>
            </div>
          </div>
        );
      })()}

      {/* Lag alert banner */}
      {alertCount > 0 && (
        <div className={`rounded-2xl border px-5 py-3 flex items-start gap-3 ${
          isBright ? "bg-red-50/50 border-red-200/60" : "bg-red-950/20 border-red-500/20"
        }`}>
          <div className={`w-8 h-8 rounded-xl flex items-center justify-center shrink-0 ${isBright ? "bg-red-100" : "bg-red-500/20"}`}>
            <svg className={`w-4 h-4 ${isBright ? "text-red-600" : "text-red-400"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M12 9v2m0 4h.01M5.07 19h13.86c1.54 0 2.5-1.67 1.73-3L13.73 4c-.77-1.33-2.69-1.33-3.46 0L3.34 16c-.77 1.33.19 3 1.73 3z" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
          <div className="flex-1 min-w-0">
            <div className={`text-xs font-bold ${isBright ? "text-red-700" : "text-red-400"}`}>
              {alertCount} group{alertCount > 1 ? "s" : ""} exceeding lag threshold ({fmt(lagWarnThreshold)})
            </div>
            <div className="flex flex-wrap gap-1.5 mt-1.5">
              {consumerGroups
                .filter((g) => (g.totalLag || 0) > lagWarnThreshold)
                .sort((a, b) => (b.totalLag || 0) - (a.totalLag || 0))
                .map((g) => (
                  <button
                    key={g.groupId}
                    onClick={() => setSelectedGroupId(g.groupId)}
                    className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-lg text-[10px] font-mono border transition-all cursor-pointer ${
                      isBright
                        ? "bg-white border-red-200/60 text-red-700 hover:bg-red-50"
                        : "bg-red-950/30 border-red-500/30 text-red-300 hover:bg-red-500/20"
                    }`}
                  >
                    <span className="truncate max-w-[120px]">{g.groupId}</span>
                    <span className="font-bold">{fmt(g.totalLag)}</span>
                  </button>
                ))}
            </div>
          </div>
        </div>
      )}

      {/* Lag comparison chart - top 10 groups by lag */}
      {consumerGroups.length > 0 && (() => {
        const sorted = [...consumerGroups]
          .filter((g) => (g.totalLag || 0) > 0)
          .sort((a, b) => (b.totalLag || 0) - (a.totalLag || 0))
          .slice(0, 10);
        if (sorted.length === 0) return null;
        const maxLag = Math.max(...sorted.map((g) => g.totalLag || 0), 1);
        return (
          <div className={`rounded-2xl border px-5 py-4 ${isBright ? "border-slate-200/60 bg-white/60" : "border-slate-700/30 bg-slate-900/30"}`}>
            <div className={`text-[11px] uppercase tracking-wider font-medium mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
              Lag Comparison (Top {sorted.length})
            </div>
            <div className="space-y-2">
              {sorted.map((g) => {
                const lag = g.totalLag || 0;
                const pct = (lag / maxLag) * 100;
                const color = lag > 1000 ? "bg-red-500" : lag > 100 ? "bg-amber-500" : "bg-emerald-500";
                return (
                  <div key={g.groupId} className="flex items-center gap-3 cursor-pointer hover:opacity-80" onClick={() => setSelectedGroupId(String(g.groupId))}>
                    <span className={`text-[11px] font-mono truncate w-44 shrink-0 ${isBright ? "text-amber-600" : "text-amber-300"}`}>{String(g.groupId)}</span>
                    <div className={`flex-1 h-2 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                      <div className={`h-full rounded-full transition-all duration-500 ${color}`} style={{ width: `${pct}%` }} />
                    </div>
                    <span className={`text-[11px] font-mono font-bold tabular-nums w-16 text-right ${
                      lag > 1000 ? "text-red-500" : lag > 0 ? "text-amber-500" : "text-emerald-500"
                    }`}>{fmt(lag)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })()}

      {/* Status filter chips */}
      {uniqueStatuses.length > 1 && (
        <div className="flex items-center gap-2 flex-wrap">
          <span className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Status:</span>
          <button
            onClick={() => setStatusFilter(null)}
            className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
              !statusFilter
                ? isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/20 border-amber-500/30 text-amber-300"
                : isBright ? "bg-white border-slate-200/60 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
            }`}
          >All ({consumerGroups.length})</button>
          {uniqueStatuses.map((s) => {
            const count = consumerGroups.filter((g) => g.status === s).length;
            const active = statusFilter === s;
            return (
              <button
                key={s}
                onClick={() => setStatusFilter(active ? null : s)}
                className={`px-2.5 py-1 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
                  active
                    ? isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/20 border-amber-500/30 text-amber-300"
                    : isBright ? "bg-white border-slate-200/60 text-slate-500 hover:bg-slate-50" : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
                }`}
              >{s} ({count})</button>
            );
          })}
        </div>
      )}

      {consumerGroupsLoading && consumerGroups.length === 0 ? (
        <SkeletonTable rows={6} cols={4} />
      ) : (
        <DataTable
          columns={columns}
          data={filteredGroups as unknown as Record<string, unknown>[]}
          onRowClick={(row) => setSelectedGroupId(String(row.groupId))}
          searchPlaceholder="Filter consumer groups..."
          searchKeys={["groupId"]}
          emptyMessage="No consumer groups found"
          exportFilename="kafka-consumer-groups"
        />
      )}

      {/* Delete Consumer Group Modal */}
      <Modal title="Delete Consumer Group" open={!!showDelete} onClose={() => { setShowDelete(null); setDeleteError(null); }}>
        <div className="space-y-4">
          {deleteError && (
            <div className={`p-3 rounded-xl border text-sm ${
              isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
            }`}>
              {deleteError}
            </div>
          )}
          <p className={`text-sm ${isBright ? "text-slate-600" : "text-slate-300"}`}>
            Are you sure you want to delete consumer group <span className={`font-mono font-medium ${isBright ? "text-amber-600" : "text-amber-300"}`}>{showDelete}</span>? The group must be inactive. This action cannot be undone.
          </p>
          <div className="flex justify-end gap-2 pt-2">
            <button onClick={() => { setShowDelete(null); setDeleteError(null); }} className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}>Cancel</button>
            <button onClick={handleDelete} className="px-4 py-2 rounded-xl text-sm font-medium bg-red-500/20 border border-red-500/40 text-red-400 hover:bg-red-500/30 transition-colors cursor-pointer">Delete</button>
          </div>
        </div>
      </Modal>
    </div>
  );
}

function SummaryCard({ label, value, color, bright }: { label: string; value: string; color: string; bright: boolean }) {
  const darkColorMap: Record<string, string> = {
    amber: "border-amber-500/20 from-amber-500/[0.06]",
    emerald: "border-emerald-500/20 from-emerald-500/[0.06]",
    red: "border-red-500/20 from-red-500/[0.06]",
    slate: "border-slate-700/30 from-slate-500/[0.04]",
  };
  const brightColorMap: Record<string, string> = {
    amber: "border-amber-200/60 from-amber-50",
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
