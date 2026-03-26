import { useEffect, useState, useMemo } from "react";
import { useThemeStore } from "../store/themeStore";
import { useToastStore } from "../store/toastStore";
import { apiFetch } from "../hooks/useApi";

interface QuotaEntry {
  entity: Record<string, string>;
  quotas: Record<string, number | string>;
}

export function QuotasView() {
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const addToast = useToastStore((s) => s.addToast);

  const [quotas, setQuotas] = useState<QuotaEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  // Create form
  const [showCreate, setShowCreate] = useState(false);
  const [entityType, setEntityType] = useState("user");
  const [entityName, setEntityName] = useState("");
  const [quotaKey, setQuotaKey] = useState("producer_byte_rate");
  const [quotaValue, setQuotaValue] = useState("");
  const [creating, setCreating] = useState(false);

  const fetchQuotas = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiFetch<{ quotas: QuotaEntry[]; count: number; error?: string }>("/api/quotas");
      setQuotas(data.quotas || []);
      if (data.error) setError(data.error);
    } catch (e) {
      setError(String(e));
    }
    setLoading(false);
  };

  useEffect(() => { fetchQuotas(); }, []);

  const handleCreate = async () => {
    if (!entityName.trim() || !quotaValue.trim()) return;
    setCreating(true);
    try {
      await apiFetch("/api/quotas", {
        method: "POST",
        body: JSON.stringify({
          entityType,
          entityName: entityName.trim(),
          quotas: { [quotaKey]: Number(quotaValue) },
        }),
      });
      addToast(`Quota set for ${entityType}:${entityName}`, "success");
      setShowCreate(false);
      setEntityName("");
      setQuotaValue("");
      fetchQuotas();
    } catch (e) {
      addToast(String(e), "error");
    }
    setCreating(false);
  };

  const handleDelete = async (entry: QuotaEntry, key: string) => {
    const eType = Object.keys(entry.entity)[0] || "user";
    const eName = entry.entity[eType] || "";
    try {
      await apiFetch("/api/quotas", {
        method: "DELETE",
        body: JSON.stringify({
          entityType: eType,
          entityName: eName,
          quotaKeys: [key],
        }),
      });
      addToast(`Quota "${key}" removed for ${eType}:${eName}`, "success");
      fetchQuotas();
    } catch (e) {
      addToast(String(e), "error");
    }
  };

  // Summary stats
  const quotaSummary = useMemo(() => {
    const byType: Record<string, number> = {};
    const byKey: Record<string, { count: number; min: number; max: number; avg: number }> = {};
    const entityTypes: Record<string, number> = {};

    quotas.forEach((q) => {
      Object.keys(q.entity).forEach((t) => { entityTypes[t] = (entityTypes[t] || 0) + 1; });
      Object.entries(q.quotas).forEach(([k, v]) => {
        const num = Number(v);
        if (!byKey[k]) byKey[k] = { count: 0, min: Infinity, max: -Infinity, avg: 0 };
        byKey[k].count++;
        if (!isNaN(num)) {
          byKey[k].min = Math.min(byKey[k].min, num);
          byKey[k].max = Math.max(byKey[k].max, num);
          byKey[k].avg += num;
        }
        byType[k] = (byType[k] || 0) + 1;
      });
    });

    Object.values(byKey).forEach((v) => { if (v.count > 0) v.avg /= v.count; });
    return { byType, byKey, entityTypes, totalEntries: quotas.length };
  }, [quotas]);

  const filtered = quotas.filter((q) => {
    if (!filter) return true;
    const f = filter.toLowerCase();
    const entityStr = Object.entries(q.entity).map(([k, v]) => `${k}:${v}`).join(" ").toLowerCase();
    const quotaStr = Object.keys(q.quotas).join(" ").toLowerCase();
    return entityStr.includes(f) || quotaStr.includes(f);
  });

  const quotaKeyOptions = [
    { value: "producer_byte_rate", label: "Producer Byte Rate", desc: "Max bytes/sec for producing" },
    { value: "consumer_byte_rate", label: "Consumer Byte Rate", desc: "Max bytes/sec for consuming" },
    { value: "request_percentage", label: "Request Percentage", desc: "Max % of request handler threads" },
    { value: "controller_mutation_rate", label: "Controller Mutation Rate", desc: "Max mutations/sec allowed" },
  ];

  function fmtBytes(b: number): string {
    if (b >= 1_073_741_824) return `${(b / 1_073_741_824).toFixed(1)} GB/s`;
    if (b >= 1_048_576) return `${(b / 1_048_576).toFixed(1)} MB/s`;
    if (b >= 1_024) return `${(b / 1_024).toFixed(1)} KB/s`;
    return `${b} B/s`;
  }

  function fmtQuotaValue(key: string, value: number | string): string {
    const v = Number(value);
    if (isNaN(v)) return String(value);
    if (key.includes("byte_rate")) return fmtBytes(v);
    if (key.includes("percentage")) return `${v.toFixed(1)}%`;
    return String(v);
  }

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Quotas</h1>
          <p className={`text-sm mt-0.5 ${isBright ? "text-slate-500" : "text-slate-500"}`}>Client quota management for rate limiting</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={fetchQuotas}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
            }`}
          >
            Refresh
          </button>
          {quotas.length > 0 && (
            <button
              onClick={() => {
                const blob = new Blob([JSON.stringify(quotas, null, 2)], { type: "application/json" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url; a.download = "kafka-quotas.json"; a.click();
                URL.revokeObjectURL(url);
              }}
              className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
                isBright
                  ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50"
                  : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50"
              }`}
            >
              Export
            </button>
          )}
          <button
            onClick={() => setShowCreate(true)}
            className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
            }`}
          >
            + Set Quota
          </button>
        </div>
      </div>

      {error && (
        <div className={`rounded-xl border p-4 text-sm ${isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/10 border-amber-500/20 text-amber-300"}`}>
          {error}
        </div>
      )}

      {/* Search */}
      <div className="flex items-center gap-3">
        <input
          type="text"
          placeholder="Filter quotas..."
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className={`flex-1 px-3.5 py-2 rounded-xl text-sm border outline-none ${
            isBright
              ? "bg-white border-slate-200/80 text-slate-800 focus:border-indigo-400 placeholder:text-slate-400"
              : "bg-slate-800/50 border-slate-700/40 text-white focus:border-indigo-500 placeholder:text-slate-500"
          }`}
        />
        <span className={`text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          {filtered.length} {filtered.length === 1 ? "entry" : "entries"}
        </span>
      </div>

      {/* Summary Cards */}
      {quotas.length > 0 && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          {/* Total entries */}
          <div className={`rounded-xl border p-4 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
            <div className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{quotaSummary.totalEntries}</div>
            <div className={`text-[10px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Quota Entries</div>
          </div>
          {/* Entity type breakdown */}
          <div className={`rounded-xl border p-4 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
            <div className="flex items-center gap-2 flex-wrap">
              {Object.entries(quotaSummary.entityTypes).map(([type, count]) => (
                <div key={type} className="text-center">
                  <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{count}</div>
                  <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>{type}</div>
                </div>
              ))}
            </div>
            <div className={`text-[10px] uppercase tracking-wider mt-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>By Entity Type</div>
          </div>
          {/* Quota type distribution */}
          <div className={`rounded-xl border p-4 col-span-2 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
            <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Quota Distribution</div>
            <div className="space-y-1.5">
              {Object.entries(quotaSummary.byKey).map(([key, stats]) => {
                const maxWidth = Math.max(...Object.values(quotaSummary.byKey).map((s) => s.count));
                const pct = maxWidth > 0 ? (stats.count / maxWidth) * 100 : 0;
                return (
                  <div key={key} className="flex items-center gap-2">
                    <span className={`text-[10px] font-mono w-40 truncate shrink-0 ${isBright ? "text-slate-600" : "text-slate-300"}`}>{key}</span>
                    <div className={`flex-1 h-3 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                      <div className={`h-full rounded-full transition-all ${
                        key.includes("producer") ? "bg-indigo-500" : key.includes("consumer") ? "bg-cyan-500" : key.includes("request") ? "bg-amber-500" : "bg-violet-500"
                      }`} style={{ width: `${pct}%` }} />
                    </div>
                    <span className={`text-[10px] font-mono w-8 text-right ${isBright ? "text-slate-500" : "text-slate-400"}`}>{stats.count}</span>
                    {stats.min !== Infinity && (
                      <span className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                        {key.includes("byte_rate") ? `${fmtBytes(stats.min)} - ${fmtBytes(stats.max)}` : `${stats.min} - ${stats.max}`}
                      </span>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : filtered.length === 0 && !error ? (
        <div className={`text-center py-16 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          <svg className="w-12 h-12 mx-auto mb-3 opacity-40" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
            <path d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <p className="text-sm font-medium">No quotas configured</p>
          <p className="text-xs mt-1">Set quotas to rate-limit clients</p>
        </div>
      ) : (
        <div className="space-y-3">
          {filtered.map((entry, i) => {
            return (
              <div key={i} className={`rounded-2xl border p-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    {Object.entries(entry.entity).map(([type, name]) => (
                      <span key={type} className={`text-xs font-medium px-2 py-0.5 rounded-lg ${
                        type === "user"
                          ? isBright ? "bg-indigo-50 text-indigo-700" : "bg-indigo-500/15 text-indigo-300"
                          : type === "client-id"
                            ? isBright ? "bg-cyan-50 text-cyan-700" : "bg-cyan-500/15 text-cyan-300"
                            : isBright ? "bg-slate-100 text-slate-600" : "bg-slate-800 text-slate-300"
                      }`}>
                        {type}: <span className="font-mono">{name}</span>
                      </span>
                    ))}
                  </div>
                </div>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
                  {Object.entries(entry.quotas).map(([key, value]) => {
                    const num = Number(value);
                    const stats = quotaSummary.byKey[key];
                    const pctOfMax = stats && stats.max > 0 && !isNaN(num) ? (num / stats.max) * 100 : 0;
                    const colorClass = key.includes("producer") ? "bg-indigo-500" : key.includes("consumer") ? "bg-cyan-500" : key.includes("request") ? "bg-amber-500" : "bg-violet-500";
                    return (
                      <div key={key} className={`py-2 px-3 rounded-xl ${
                        isBright ? "bg-slate-50" : "bg-slate-800/30"
                      }`}>
                        <div className="flex items-center justify-between">
                          <div>
                            <div className={`text-[11px] font-medium ${isBright ? "text-slate-700" : "text-slate-300"}`}>{key}</div>
                            <div className={`text-xs font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>{fmtQuotaValue(key, value)}</div>
                          </div>
                          <button
                            onClick={() => handleDelete(entry, key)}
                            className={`text-[10px] px-1.5 py-1 rounded transition-colors cursor-pointer ${
                              isBright ? "text-red-400 hover:bg-red-50 hover:text-red-600" : "text-red-400/60 hover:bg-red-500/10 hover:text-red-400"
                            }`}
                          >
                            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
                            </svg>
                          </button>
                        </div>
                        {/* Usage bar relative to max */}
                        {stats && stats.max > 0 && (
                          <div className={`mt-1.5 h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-200" : "bg-slate-700/40"}`}>
                            <div className={`h-full rounded-full transition-all ${colorClass}`} style={{ width: `${Math.max(pctOfMax, 2)}%` }} />
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* Create Quota Form */}
      {showCreate && (
        <div className={`fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm`} onClick={() => setShowCreate(false)}>
          <div className={`w-full max-w-md rounded-2xl border p-6 shadow-2xl ${
            isBright ? "bg-white border-slate-200" : "bg-slate-900 border-slate-700/60"
          }`} onClick={(e) => e.stopPropagation()}>
            <h3 className={`text-sm font-semibold mb-4 ${isBright ? "text-slate-800" : "text-white"}`}>Set Quota</h3>
            <div className="space-y-3">
              <div>
                <label className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Entity Type</label>
                <div className="flex gap-2 mt-1.5">
                  {["user", "client-id", "ip"].map((t) => (
                    <button
                      key={t}
                      onClick={() => setEntityType(t)}
                      className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
                        entityType === t
                          ? isBright ? "bg-indigo-50 border-indigo-200 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                          : isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
                      }`}
                    >
                      {t}
                    </button>
                  ))}
                </div>
              </div>
              <div>
                <label className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Entity Name</label>
                <input
                  type="text"
                  value={entityName}
                  onChange={(e) => setEntityName(e.target.value)}
                  placeholder={entityType === "user" ? "alice" : entityType === "client-id" ? "my-app" : "192.168.1.1"}
                  className={`w-full mt-1.5 px-3 py-2 rounded-xl text-sm border outline-none ${
                    isBright
                      ? "bg-slate-50 border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                      : "bg-slate-800/80 border-slate-700/50 text-white focus:border-indigo-500 placeholder:text-slate-600"
                  }`}
                />
              </div>
              <div>
                <label className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>Quota Type</label>
                <div className="flex flex-wrap gap-1.5 mt-1.5">
                  {quotaKeyOptions.map((opt) => (
                    <button
                      key={opt.value}
                      onClick={() => setQuotaKey(opt.value)}
                      className={`px-2.5 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer ${
                        quotaKey === opt.value
                          ? isBright ? "bg-indigo-50 border-indigo-200 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                          : isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700 text-slate-400 hover:bg-slate-700"
                      }`}
                      title={opt.desc}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </div>
              <div>
                <label className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                  Value {quotaKey.includes("byte_rate") ? "(bytes/sec)" : quotaKey.includes("percentage") ? "(%)" : ""}
                </label>
                <input
                  type="number"
                  value={quotaValue}
                  onChange={(e) => setQuotaValue(e.target.value)}
                  placeholder={quotaKey.includes("byte_rate") ? "1048576" : "25"}
                  className={`w-full mt-1.5 px-3 py-2 rounded-xl text-sm font-mono border outline-none ${
                    isBright
                      ? "bg-slate-50 border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                      : "bg-slate-800/80 border-slate-700/50 text-white focus:border-indigo-500 placeholder:text-slate-600"
                  }`}
                />
              </div>
              <div className="flex justify-end gap-2 pt-2">
                <button
                  onClick={() => setShowCreate(false)}
                  className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer ${
                    isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
                  }`}
                >
                  Cancel
                </button>
                <button
                  onClick={handleCreate}
                  disabled={creating || !entityName.trim() || !quotaValue.trim()}
                  className={`px-4 py-2 rounded-xl text-sm font-medium border transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                    isBright
                      ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                      : "bg-indigo-500/20 border-indigo-500/40 text-indigo-300 hover:bg-indigo-500/30"
                  }`}
                >
                  {creating ? "Setting..." : "Set Quota"}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
