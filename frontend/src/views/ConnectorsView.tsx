import { useEffect, useState, useCallback, useMemo } from "react";
import { useThemeStore } from "../store/themeStore";

interface ConnectorTask {
  id: number;
  state: string;
  worker_id?: string;
}

interface Connector {
  name: string;
  state: string;
  type: string;
  tasks: ConnectorTask[];
}

interface ConnectorDetail {
  name: string;
  config: Record<string, string>;
  status?: {
    connector: { state: string; worker_id: string };
    tasks: ConnectorTask[];
  };
}

export function ConnectorsView() {
  const { theme } = useThemeStore();
  const isBright = theme === "bright";

  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<ConnectorDetail | null>(null);
  const [filterText, setFilterText] = useState("");

  // Create form
  const [showCreate, setShowCreate] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createConfig, setCreateConfig] = useState('{\n  "connector.class": "",\n  "tasks.max": "1"\n}');
  const [creating, setCreating] = useState(false);

  // Config editor
  const [editingConfig, setEditingConfig] = useState(false);
  const [editConfig, setEditConfig] = useState<Record<string, string>>({});
  const [savingConfig, setSavingConfig] = useState(false);

  // Plugins
  const [plugins, setPlugins] = useState<{ class: string; type: string }[]>([]);

  const fetchConnectors = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch("/api/connect/connectors");
      if (resp.status === 503) {
        setError("Kafka Connect not configured. Set KAFKA_CONNECT_URL environment variable.");
        setConnectors([]);
        return;
      }
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setConnectors(data.connectors || []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchConnectors();
    fetch("/api/connect/connector-plugins").then((r) => r.ok ? r.json() : null).then((d) => {
      if (d?.plugins) setPlugins(d.plugins);
    }).catch(() => {});
  }, [fetchConnectors]);

  const selectConnector = async (name: string) => {
    try {
      const resp = await fetch(`/api/connect/connectors/${encodeURIComponent(name)}`);
      if (resp.ok) setSelected(await resp.json());
    } catch { /* ignore */ }
  };

  const saveConfig = async () => {
    if (!selected) return;
    setSavingConfig(true);
    try {
      const resp = await fetch(`/api/connect/connectors/${encodeURIComponent(selected.name)}/config`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(editConfig),
      });
      if (resp.ok) {
        setEditingConfig(false);
        selectConnector(selected.name);
      }
    } catch { /* ignore */ }
    setSavingConfig(false);
  };

  const getConnectorType = (c: Connector | ConnectorDetail): "source" | "sink" | "unknown" => {
    const cls = ("config" in c ? c.config?.["connector.class"] : "") || "";
    const lc = cls.toLowerCase();
    if (lc.includes("source")) return "source";
    if (lc.includes("sink")) return "sink";
    // Check type field from list
    if ("type" in c) {
      const t = (c as Connector).type?.toLowerCase();
      if (t === "source") return "source";
      if (t === "sink") return "sink";
    }
    return "unknown";
  };

  const typeBadge = (type: "source" | "sink" | "unknown") => {
    if (type === "source") return isBright ? "bg-blue-50 text-blue-600 border-blue-200/60" : "bg-blue-500/10 text-blue-400 border-blue-500/20";
    if (type === "sink") return isBright ? "bg-violet-50 text-violet-600 border-violet-200/60" : "bg-violet-500/10 text-violet-400 border-violet-500/20";
    return isBright ? "bg-slate-50 text-slate-500 border-slate-200" : "bg-slate-800/40 text-slate-400 border-slate-700/40";
  };

  // Summary stats
  const totalTasks = connectors.reduce((a, c) => a + c.tasks.length, 0);
  const failedTasks = connectors.reduce((a, c) => a + c.tasks.filter((t) => t.state === "FAILED").length, 0);

  // Connector class distribution
  const classDistribution = useMemo(() => {
    const map: Record<string, number> = {};
    connectors.forEach((c) => {
      const cls = c.type || "unknown";
      map[cls] = (map[cls] || 0) + 1;
    });
    return Object.entries(map).sort((a, b) => b[1] - a[1]);
  }, [connectors]);

  const handleAction = async (name: string, action: "pause" | "resume" | "restart" | "delete") => {
    const method = action === "delete" ? "DELETE" : action === "restart" ? "POST" : "PUT";
    const path = action === "delete"
      ? `/api/connect/connectors/${encodeURIComponent(name)}`
      : `/api/connect/connectors/${encodeURIComponent(name)}/${action}`;
    await fetch(path, { method });
    if (action === "delete" && selected?.name === name) setSelected(null);
    setTimeout(fetchConnectors, 500);
  };

  const handleCreate = async () => {
    setCreating(true);
    try {
      const config = JSON.parse(createConfig);
      const resp = await fetch("/api/connect/connectors", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: createName, config }),
      });
      if (resp.ok) {
        setShowCreate(false);
        setCreateName("");
        fetchConnectors();
      }
    } catch { /* ignore */ }
    setCreating(false);
  };

  const stateColor = (state: string) => {
    if (state === "RUNNING") return isBright ? "text-emerald-600 bg-emerald-50" : "text-emerald-400 bg-emerald-500/10";
    if (state === "PAUSED") return isBright ? "text-amber-600 bg-amber-50" : "text-amber-400 bg-amber-500/10";
    if (state === "FAILED") return isBright ? "text-red-600 bg-red-50" : "text-red-400 bg-red-500/10";
    return isBright ? "text-slate-500 bg-slate-50" : "text-slate-400 bg-slate-800/50";
  };

  const filtered = filterText
    ? connectors.filter((c) => c.name.toLowerCase().includes(filterText.toLowerCase()))
    : connectors;

  const running = connectors.filter((c) => c.state === "RUNNING").length;
  const paused = connectors.filter((c) => c.state === "PAUSED").length;
  const failed = connectors.filter((c) => c.state === "FAILED").length;

  return (
    <div className={`flex-1 overflow-hidden flex flex-col ${isBright ? "text-slate-800" : "text-white"}`}>
      {/* Header */}
      <div className={`px-8 py-5 border-b flex items-center justify-between ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
        <div>
          <h1 className={`text-xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Kafka Connect</h1>
          <p className={`text-xs mt-0.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
            {connectors.length} connectors &middot; {totalTasks} tasks &middot; {running} running, {paused} paused{failed > 0 ? `, ${failed} failed` : ""}{failedTasks > 0 ? ` (${failedTasks} task failures)` : ""}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowCreate(!showCreate)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
              showCreate
                ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                : isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}
          >
            Create Connector
          </button>
          <button
            onClick={fetchConnectors}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}
          >
            Refresh
          </button>
        </div>
      </div>

      {/* Create form */}
      {showCreate && (
        <div className={`px-8 py-4 border-b space-y-3 ${isBright ? "border-slate-200/40 bg-indigo-50/30" : "border-slate-800/50 bg-indigo-950/10"}`}>
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Connector Name</label>
              <input
                type="text"
                value={createName}
                onChange={(e) => setCreateName(e.target.value)}
                placeholder="my-connector"
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs font-mono border focus:outline-none ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={handleCreate}
                disabled={creating || !createName}
                className={`px-4 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                  isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                }`}
              >
                {creating ? "Creating..." : "Create"}
              </button>
            </div>
          </div>
          <div>
            <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Config (JSON)</label>
            <textarea
              value={createConfig}
              onChange={(e) => setCreateConfig(e.target.value)}
              rows={5}
              className={`w-full mt-0.5 rounded-lg px-3 py-2 text-xs font-mono border focus:outline-none resize-y ${
                isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
              }`}
            />
          </div>
        </div>
      )}

      {error && (
        <div className={`mx-8 mt-4 p-4 rounded-xl border text-sm ${
          isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/10 border-amber-500/20 text-amber-300"
        }`}>{error}</div>
      )}

      {/* Health Overview */}
      {connectors.length > 0 && (
        <div className={`mx-8 mt-4 flex items-stretch gap-4`}>
          {/* Status donut */}
          <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
            <svg width="40" height="40" viewBox="0 0 40 40">
              {(() => {
                const total = connectors.length || 1;
                const segments = [
                  { count: running, color: "#10b981" },
                  { count: paused, color: "#f59e0b" },
                  { count: failed, color: "#ef4444" },
                  { count: total - running - paused - failed, color: "#64748b" },
                ].filter((s) => s.count > 0);
                let cum = 0;
                return segments.map((seg, i) => {
                  const frac = seg.count / total;
                  const dash = frac * 100.53;
                  const offset = cum * 100.53;
                  cum += frac;
                  return <circle key={i} cx="20" cy="20" r="16" fill="none" stroke={seg.color} strokeWidth="5" strokeDasharray={`${dash} ${100.53 - dash}`} strokeDashoffset={-offset} transform="rotate(-90 20 20)" />;
                });
              })()}
            </svg>
            <div className="text-[10px] space-y-0.5">
              <div className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-emerald-500" /><span className={isBright ? "text-slate-600" : "text-slate-300"}>{running} Running</span></div>
              <div className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-amber-500" /><span className={isBright ? "text-slate-600" : "text-slate-300"}>{paused} Paused</span></div>
              {failed > 0 && <div className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-red-500" /><span className={isBright ? "text-slate-600" : "text-slate-300"}>{failed} Failed</span></div>}
            </div>
          </div>
          {/* Task summary */}
          <div className={`flex items-center gap-4 px-4 py-3 rounded-xl border ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
            <div className="text-center">
              <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{totalTasks}</div>
              <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Tasks</div>
            </div>
            <div className="text-center">
              <div className={`text-lg font-bold ${failedTasks > 0 ? (isBright ? "text-red-600" : "text-red-400") : isBright ? "text-emerald-600" : "text-emerald-400"}`}>
                {failedTasks}
              </div>
              <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Failed</div>
            </div>
            <div className="text-center">
              <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>
                {totalTasks > 0 ? Math.round(((totalTasks - failedTasks) / totalTasks) * 100) : 100}%
              </div>
              <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Health</div>
            </div>
          </div>
          {/* Type distribution */}
          {classDistribution.length > 0 && (
            <div className={`flex items-center gap-2 px-4 py-3 rounded-xl border flex-1 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
              <div className={`text-[9px] uppercase tracking-wider font-medium shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Types</div>
              <div className="flex items-center gap-2 flex-wrap">
                {classDistribution.map(([type, count]) => (
                  <span key={type} className={`text-[10px] font-medium px-2 py-0.5 rounded border ${
                    type.toLowerCase() === "source"
                      ? isBright ? "bg-blue-50 text-blue-600 border-blue-200/60" : "bg-blue-500/10 text-blue-400 border-blue-500/20"
                      : type.toLowerCase() === "sink"
                        ? isBright ? "bg-violet-50 text-violet-600 border-violet-200/60" : "bg-violet-500/10 text-violet-400 border-violet-500/20"
                        : isBright ? "bg-slate-50 text-slate-500 border-slate-200" : "bg-slate-800/40 text-slate-400 border-slate-700/40"
                  }`}>
                    {type}: {count}
                  </span>
                ))}
              </div>
            </div>
          )}
          {/* Plugins count */}
          {plugins.length > 0 && (
            <div className={`flex items-center gap-2 px-4 py-3 rounded-xl border ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
              <div className="text-center">
                <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{plugins.length}</div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Plugins</div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Main content */}
      <div className="flex-1 overflow-hidden flex px-8 py-5 gap-5">
        {/* Connector list */}
        <div className={`w-80 shrink-0 flex flex-col rounded-2xl border ${isBright ? "bg-white/80 border-slate-200/60" : "bg-slate-900/60 border-slate-700/30"}`}>
          <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
            <input
              type="text"
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              placeholder="Filter connectors..."
              className={`w-full px-3 py-1.5 rounded-lg text-xs border focus:outline-none ${
                isBright ? "bg-slate-50 border-slate-200 text-slate-700 placeholder-slate-400" : "bg-slate-800/60 border-slate-700/40 text-slate-300 placeholder-slate-500"
              }`}
            />
          </div>
          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center h-32">
                <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
              </div>
            ) : filtered.length === 0 ? (
              <div className={`px-4 py-8 text-center text-xs ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                {filterText ? "No matching connectors" : "No connectors found"}
              </div>
            ) : (
              filtered.map((c) => (
                <div
                  key={c.name}
                  onClick={() => selectConnector(c.name)}
                  className={`px-4 py-3 cursor-pointer border-b transition-colors ${
                    selected?.name === c.name
                      ? isBright ? "bg-indigo-50 border-indigo-100" : "bg-indigo-500/10 border-indigo-500/10"
                      : isBright ? "border-slate-100 hover:bg-slate-50" : "border-slate-800/30 hover:bg-slate-800/30"
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className={`text-xs font-mono font-medium truncate ${
                      selected?.name === c.name ? (isBright ? "text-indigo-700" : "text-indigo-300") : isBright ? "text-slate-700" : "text-slate-300"
                    }`}>{c.name}</span>
                    <div className="flex items-center gap-1 shrink-0">
                      <span className={`text-[8px] px-1 py-0.5 rounded border font-semibold uppercase ${typeBadge(getConnectorType(c))}`}>
                        {getConnectorType(c) === "unknown" ? c.type : getConnectorType(c)}
                      </span>
                      <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold uppercase ${stateColor(c.state)}`}>{c.state}</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                      {c.tasks.length} task{c.tasks.length !== 1 ? "s" : ""}
                      {c.tasks.some((t) => t.state === "FAILED") && (
                        <span className={isBright ? "text-red-500" : "text-red-400"}> &middot; {c.tasks.filter((t) => t.state === "FAILED").length} failed</span>
                      )}
                    </div>
                    {/* Mini task health bar */}
                    {c.tasks.length > 0 && (
                      <div className="flex gap-0.5 ml-auto">
                        {c.tasks.map((t) => (
                          <div key={t.id} className={`w-1.5 h-1.5 rounded-full ${
                            t.state === "RUNNING" ? "bg-emerald-500" : t.state === "FAILED" ? "bg-red-500 animate-pulse" : "bg-amber-500"
                          }`} title={`Task ${t.id}: ${t.state}`} />
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Detail panel */}
        <div className={`flex-1 flex flex-col rounded-2xl border ${isBright ? "bg-white/80 border-slate-200/60" : "bg-slate-900/60 border-slate-700/30"}`}>
          {selected ? (
            <>
              <div className={`px-5 py-3 border-b flex items-center justify-between ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
                <div>
                  <div className="flex items-center gap-2">
                    <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-800" : "text-white"}`}>{selected.name}</div>
                    <span className={`text-[9px] px-1.5 py-0.5 rounded border font-semibold uppercase ${typeBadge(getConnectorType(selected))}`}>
                      {getConnectorType(selected)}
                    </span>
                  </div>
                  <div className={`text-[10px] mt-0.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                    {selected.status?.connector?.state || "UNKNOWN"} &middot; Worker: {selected.status?.connector?.worker_id || "unknown"}
                    {selected.config?.["connector.class"] && (
                      <span> &middot; {selected.config["connector.class"].split(".").pop()}</span>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-1.5">
                  {selected.status?.connector?.state === "RUNNING" && (
                    <button
                      onClick={() => handleAction(selected.name, "pause")}
                      className={`px-2 py-1 rounded text-[10px] font-medium border cursor-pointer ${
                        isBright ? "border-amber-200 text-amber-600 hover:bg-amber-50" : "border-amber-500/30 text-amber-400 hover:bg-amber-500/10"
                      }`}
                    >Pause</button>
                  )}
                  {selected.status?.connector?.state === "PAUSED" && (
                    <button
                      onClick={() => handleAction(selected.name, "resume")}
                      className={`px-2 py-1 rounded text-[10px] font-medium border cursor-pointer ${
                        isBright ? "border-emerald-200 text-emerald-600 hover:bg-emerald-50" : "border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/10"
                      }`}
                    >Resume</button>
                  )}
                  <button
                    onClick={() => handleAction(selected.name, "restart")}
                    className={`px-2 py-1 rounded text-[10px] font-medium border cursor-pointer ${
                      isBright ? "border-slate-200 text-slate-500 hover:bg-slate-50" : "border-slate-700/40 text-slate-400 hover:bg-slate-800"
                    }`}
                  >Restart</button>
                  <button
                    onClick={() => handleAction(selected.name, "delete")}
                    className={`px-2 py-1 rounded text-[10px] font-medium border cursor-pointer ${
                      isBright ? "border-red-200 text-red-500 hover:bg-red-50" : "border-red-500/30 text-red-400 hover:bg-red-500/10"
                    }`}
                  >Delete</button>
                </div>
              </div>
              {/* Tasks */}
              {selected.status?.tasks && selected.status.tasks.length > 0 && (
                <div className={`px-5 py-3 border-b ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
                  <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Tasks</div>
                  <div className="space-y-1.5">
                    {selected.status.tasks.map((t) => (
                      <div key={t.id} className={`flex items-center justify-between px-3 py-2 rounded-lg ${
                        t.state === "FAILED"
                          ? isBright ? "bg-red-50/80 border border-red-200/40" : "bg-red-500/[0.06] border border-red-500/15"
                          : isBright ? "bg-slate-50" : "bg-slate-800/30"
                      }`}>
                        <div className="flex items-center gap-2">
                          <span className={`w-2 h-2 rounded-full ${
                            t.state === "RUNNING" ? "bg-emerald-500" : t.state === "FAILED" ? "bg-red-500 animate-pulse" : "bg-amber-500"
                          }`} />
                          <span className={`text-[11px] font-mono font-medium ${isBright ? "text-slate-700" : "text-slate-300"}`}>
                            Task {t.id}
                          </span>
                          <span className={`text-[9px] px-1.5 py-0.5 rounded font-bold uppercase ${stateColor(t.state)}`}>{t.state}</span>
                          {t.worker_id && <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>@{t.worker_id}</span>}
                        </div>
                        <button
                          onClick={() => {
                            fetch(`/api/connect/connectors/${encodeURIComponent(selected.name)}/tasks/${t.id}/restart`, { method: "POST" });
                            setTimeout(() => selectConnector(selected.name), 1000);
                          }}
                          className={`px-2 py-0.5 rounded text-[9px] font-medium border transition-colors cursor-pointer ${
                            t.state === "FAILED"
                              ? isBright ? "border-red-200 text-red-600 hover:bg-red-100" : "border-red-500/30 text-red-400 hover:bg-red-500/15"
                              : isBright ? "border-slate-200 text-slate-500 hover:bg-slate-100" : "border-slate-700 text-slate-400 hover:bg-slate-700"
                          }`}
                        >
                          Restart Task
                        </button>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {/* Config */}
              <div className="flex-1 overflow-y-auto p-5">
                <div className="flex items-center justify-between mb-2">
                  <div className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                    Configuration ({selected.config ? Object.keys(selected.config).length : 0} keys)
                  </div>
                  <div className="flex items-center gap-1.5">
                    {editingConfig ? (
                      <>
                        <button
                          onClick={() => setEditingConfig(false)}
                          className={`px-2 py-0.5 rounded text-[10px] font-medium border cursor-pointer ${
                            isBright ? "border-slate-200 text-slate-500 hover:bg-slate-50" : "border-slate-700 text-slate-400 hover:bg-slate-800"
                          }`}
                        >Cancel</button>
                        <button
                          onClick={saveConfig}
                          disabled={savingConfig}
                          className={`px-2 py-0.5 rounded text-[10px] font-medium border cursor-pointer disabled:opacity-40 ${
                            isBright ? "border-emerald-200 text-emerald-600 hover:bg-emerald-50" : "border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/10"
                          }`}
                        >{savingConfig ? "Saving..." : "Save"}</button>
                      </>
                    ) : (
                      <button
                        onClick={() => {
                          setEditConfig({ ...(selected.config || {}) });
                          setEditingConfig(true);
                        }}
                        className={`px-2 py-0.5 rounded text-[10px] font-medium border cursor-pointer ${
                          isBright ? "border-slate-200 text-slate-500 hover:bg-slate-50" : "border-slate-700 text-slate-400 hover:bg-slate-800"
                        }`}
                      >Edit Config</button>
                    )}
                  </div>
                </div>
                {editingConfig ? (
                  <div className="space-y-1.5">
                    {Object.entries(editConfig).sort().map(([k, v]) => {
                      const isSecret = k.includes("password") || k.includes("secret") || k.includes("key") || k.includes("token");
                      return (
                        <div key={k} className={`flex items-center gap-2 px-2 py-1 rounded-lg ${
                          isBright ? "bg-slate-50" : "bg-slate-800/30"
                        }`}>
                          <span className={`text-[11px] font-mono shrink-0 w-48 truncate ${isBright ? "text-slate-500" : "text-slate-400"}`} title={k}>{k}</span>
                          <input
                            type={isSecret ? "password" : "text"}
                            value={v}
                            onChange={(e) => setEditConfig({ ...editConfig, [k]: e.target.value })}
                            className={`flex-1 text-[11px] font-mono px-2 py-0.5 rounded border focus:outline-none ${
                              isBright ? "bg-white border-slate-200 text-slate-700 focus:border-indigo-300" : "bg-slate-800/60 border-slate-700/40 text-slate-300 focus:border-indigo-500/40"
                            }`}
                          />
                          <button
                            onClick={() => {
                              const next = { ...editConfig };
                              delete next[k];
                              setEditConfig(next);
                            }}
                            className={`text-[9px] px-1 rounded cursor-pointer ${
                              isBright ? "text-red-400 hover:text-red-600" : "text-red-500/40 hover:text-red-400"
                            }`}
                          >&#x2715;</button>
                        </div>
                      );
                    })}
                    <div className="flex gap-2 mt-2">
                      <button
                        onClick={() => setEditConfig({ ...editConfig, "": "" })}
                        className={`px-2 py-0.5 rounded text-[10px] font-medium border cursor-pointer ${
                          isBright ? "border-slate-200 text-slate-500 hover:bg-slate-50" : "border-slate-700 text-slate-400 hover:bg-slate-800"
                        }`}
                      >+ Add Key</button>
                    </div>
                  </div>
                ) : (
                  <div className="space-y-1">
                    {selected.config && Object.entries(selected.config).sort().map(([k, v]) => {
                      const isSecret = k.includes("password") || k.includes("secret") || k.includes("key.") || k.includes("token");
                      const isClass = k === "connector.class";
                      return (
                        <div key={k} className="flex gap-2">
                          <span className={`text-[11px] font-mono shrink-0 w-52 truncate ${
                            isClass ? (isBright ? "text-indigo-500 font-medium" : "text-indigo-400 font-medium") : isBright ? "text-slate-500" : "text-slate-400"
                          }`} title={k}>{k}</span>
                          <span className={`text-[11px] font-mono truncate ${isBright ? "text-slate-700" : "text-slate-300"}`} title={isSecret ? "***" : v}>
                            {isSecret ? "********" : v}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className={`flex-1 flex items-center justify-center text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              Select a connector to view details
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
