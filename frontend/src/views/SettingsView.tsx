import { useState, useEffect, useMemo } from "react";
import { useThemeStore } from "../store/themeStore";
import { useGraphStore } from "../store/graphStore";
import { useKafkaStore } from "../store/kafkaStore";
import { useToastStore } from "../store/toastStore";
import { useClusterStore } from "../store/clusterStore";
import { apiFetch } from "../hooks/useApi";

interface LagAlertRule {
  id: string;
  groupPattern: string;
  threshold: number;
  enabled: boolean;
}

function loadLagAlertRules(): LagAlertRule[] {
  try {
    const stored = localStorage.getItem("kafka-lag-alert-rules");
    return stored ? JSON.parse(stored) : [];
  } catch { return []; }
}

function saveLagAlertRules(rules: LagAlertRule[]) {
  localStorage.setItem("kafka-lag-alert-rules", JSON.stringify(rules));
}

interface HealthData {
  status: string;
  kafka_connected: boolean;
  uptime: number;
  ws_clients: number;
  topics: number;
  consumerGroups: number;
  totalLag: number;
  totalMsgPerSec: number;
  graphNodes: number;
  graphEdges: number;
  pollIntervalMs: number;
}

interface ConfigData {
  showProducers: boolean;
  samplingEnabled: boolean;
  lagWarnThreshold: number;
  animationsEnabled: boolean;
  producerGroupRegex: string;
  pollIntervalMs: number;
}

export function SettingsView() {
  const { theme, toggleTheme } = useThemeStore();
  const isBright = theme === "bright";
  const config = useGraphStore((s) => s.config);
  const hideSystemTopics = useGraphStore((s) => s.hideSystemTopics);
  const { clusterInfo, fetchClusterInfo } = useKafkaStore();

  const [health, setHealth] = useState<HealthData | null>(null);
  const [serverConfig, setServerConfig] = useState<ConfigData | null>(null);
  const [loading, setLoading] = useState(true);
  const [editLag, setEditLag] = useState("");
  const [editPoll, setEditPoll] = useState("");
  const { clusters, addCluster, removeCluster, activeClusterId, setActiveCluster } = useClusterStore();
  const [newClusterName, setNewClusterName] = useState("");
  const [newClusterUrl, setNewClusterUrl] = useState("");
  const [lagAlertRules, setLagAlertRules] = useState<LagAlertRule[]>(loadLagAlertRules());
  const [newAlertPattern, setNewAlertPattern] = useState("");
  const [newAlertThreshold, setNewAlertThreshold] = useState("1000");
  const { consumerGroups } = useKafkaStore();

  const lagAlerts = useMemo(() => {
    if (!lagAlertRules.length || !consumerGroups.length) return [];
    return consumerGroups.filter((g) => {
      return lagAlertRules.some((rule) => {
        if (!rule.enabled) return false;
        try {
          const re = new RegExp(rule.groupPattern);
          return re.test(g.groupId) && g.totalLag > rule.threshold;
        } catch { return false; }
      });
    }).map((g) => ({
      groupId: g.groupId,
      lag: g.totalLag,
      rule: lagAlertRules.find((r) => {
        try { return r.enabled && new RegExp(r.groupPattern).test(g.groupId); }
        catch { return false; }
      })!,
    }));
  }, [lagAlertRules, consumerGroups]);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [h, c] = await Promise.all([
          apiFetch<HealthData>("/api/health"),
          apiFetch<ConfigData>("/api/config"),
        ]);
        setHealth(h);
        setServerConfig(c);
        setEditLag(String(c.lagWarnThreshold));
        setEditPoll(String(c.pollIntervalMs));
      } catch (e) {
        console.error("Failed to load settings data:", e);
      }
      setLoading(false);
    }
    load();
    fetchClusterInfo();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const handleRefresh = async () => {
    setLoading(true);
    try {
      const [h, c] = await Promise.all([
        apiFetch<HealthData>("/api/health"),
        apiFetch<ConfigData>("/api/config"),
      ]);
      setHealth(h);
      setServerConfig(c);
      setEditLag(String(c.lagWarnThreshold));
      setEditPoll(String(c.pollIntervalMs));
    } catch (e) {
      console.error("Failed to refresh:", e);
    }
    setLoading(false);
    fetchClusterInfo();
  };

  const formatUptime = (secs: number) => {
    if (secs < 60) return `${secs}s`;
    if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
    if (secs < 86400) return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
    return `${Math.floor(secs / 86400)}d ${Math.floor((secs % 86400) / 3600)}h`;
  };

  const updateToggle = async (endpoint: string, key: string, value: boolean) => {
    try {
      await apiFetch(endpoint, { method: "PUT", body: JSON.stringify({ enabled: value }) });
      setServerConfig((prev) => prev ? { ...prev, [key]: value } : prev);
      useToastStore.getState().addToast(`${key} updated`, "success", 2000);
    } catch (e) {
      useToastStore.getState().addToast(`Failed to update: ${e}`, "error");
    }
  };

  const updateLagThreshold = async () => {
    const val = parseInt(editLag, 10);
    if (isNaN(val) || val < 0) return;
    try {
      await apiFetch("/api/config/lag-threshold", { method: "PUT", body: JSON.stringify({ threshold: val }) });
      setServerConfig((prev) => prev ? { ...prev, lagWarnThreshold: val } : prev);
      useGraphStore.getState().setConfig({ lagWarnThreshold: val });
      useToastStore.getState().addToast("Lag threshold updated", "success", 2000);
    } catch (e) {
      useToastStore.getState().addToast(`Failed: ${e}`, "error");
    }
  };

  const updatePollInterval = async () => {
    const val = parseInt(editPoll, 10);
    if (isNaN(val) || val < 500) return;
    try {
      await apiFetch("/api/config/poll-interval", { method: "PUT", body: JSON.stringify({ intervalMs: val }) });
      setServerConfig((prev) => prev ? { ...prev, pollIntervalMs: val } : prev);
      useToastStore.getState().addToast("Poll interval updated", "success", 2000);
    } catch (e) {
      useToastStore.getState().addToast(`Failed: ${e}`, "error");
    }
  };

  return (
    <div className="p-6 flex-1 overflow-y-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className={`text-2xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Settings</h1>
          <p className={`text-sm mt-0.5 ${isBright ? "text-slate-500" : "text-slate-500"}`}>System status, configuration, and diagnostics</p>
        </div>
        <button
          onClick={handleRefresh}
          className={`px-3.5 py-2 rounded-xl text-xs font-medium border transition-all cursor-pointer ${
            isBright
              ? "bg-white border-slate-200/80 text-slate-500 hover:bg-slate-50 hover:text-slate-700"
              : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
          }`}
        >
          Refresh
        </button>
      </div>

      {loading ? (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Health Status */}
          {health && (
            <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
              <div className="flex items-center gap-2">
                <div className={`w-3 h-3 rounded-full ${health.kafka_connected ? "bg-emerald-500" : "bg-red-500"}`} />
                <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>System Health</h3>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <InfoRow label="Status" value={health.status} isBright={isBright} />
                <InfoRow label="Kafka" value={health.kafka_connected ? "Connected" : "Disconnected"} isBright={isBright} />
                <InfoRow label="Uptime" value={formatUptime(Math.round(health.uptime))} isBright={isBright} />
                <InfoRow label="WS Clients" value={String(health.ws_clients)} isBright={isBright} />
                <InfoRow label="Topics" value={String(health.topics)} isBright={isBright} />
                <InfoRow label="Consumer Groups" value={String(health.consumerGroups)} isBright={isBright} />
                <InfoRow label="Total Lag" value={health.totalLag.toLocaleString()} isBright={isBright} />
                <InfoRow label="Throughput" value={`${health.totalMsgPerSec.toFixed(1)} msg/s`} isBright={isBright} />
                <InfoRow label="Graph Nodes" value={String(health.graphNodes)} isBright={isBright} />
                <InfoRow label="Graph Edges" value={String(health.graphEdges)} isBright={isBright} />
              </div>
            </div>
          )}

          {/* Cluster Info */}
          {clusterInfo && (
            <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
              <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Cluster Information</h3>
              <div className="grid grid-cols-2 gap-3">
                <InfoRow label="Cluster ID" value={clusterInfo.clusterId || "N/A"} mono isBright={isBright} />
                <InfoRow label="Controller" value={`Broker ${clusterInfo.controllerId}`} isBright={isBright} />
                <InfoRow label="Brokers" value={String(clusterInfo.brokerCount)} isBright={isBright} />
                <InfoRow label="Topics" value={String(clusterInfo.topicCount)} isBright={isBright} />
                <InfoRow label="Consumer Groups" value={String(clusterInfo.consumerGroupCount)} isBright={isBright} />
              </div>
            </div>
          )}

          {/* Server Configuration — Editable */}
          {serverConfig && (
            <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
              <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Server Configuration</h3>
              <div className="space-y-3">
                <ToggleRow
                  label="Show Producers"
                  desc="Display inferred producer nodes on the pipeline"
                  value={serverConfig.showProducers}
                  onChange={(v) => updateToggle("/api/config/show-producers", "showProducers", v)}
                  isBright={isBright}
                />
                <ToggleRow
                  label="Message Sampling"
                  desc="Enable real-time message sampling from topics"
                  value={serverConfig.samplingEnabled}
                  onChange={(v) => updateToggle("/api/config/sampling", "samplingEnabled", v)}
                  isBright={isBright}
                />
                <ToggleRow
                  label="Animations"
                  desc="Enable edge flow animations on the pipeline"
                  value={serverConfig.animationsEnabled}
                  onChange={(v) => updateToggle("/api/config/animations", "animationsEnabled", v)}
                  isBright={isBright}
                />

                <NumberRow
                  label="Lag Warn Threshold"
                  desc="Highlight consumers with lag above this value"
                  value={editLag}
                  onChange={setEditLag}
                  onApply={updateLagThreshold}
                  suffix=""
                  isBright={isBright}
                />
                <NumberRow
                  label="Poll Interval"
                  desc="How often to poll Kafka (min 500ms)"
                  value={editPoll}
                  onChange={setEditPoll}
                  onApply={updatePollInterval}
                  suffix="ms"
                  isBright={isBright}
                />

                <InfoRow label="Producer Regex" value={serverConfig.producerGroupRegex || "(none)"} mono isBright={isBright} />
              </div>
            </div>
          )}

          {/* Client Configuration — Editable */}
          <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
            <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Client Configuration</h3>
            <div className="space-y-3">
              <ToggleRow
                label="Dark Mode"
                desc="Toggle between dark and bright theme"
                value={theme === "dark"}
                onChange={toggleTheme}
                isBright={isBright}
              />
              <ToggleRow
                label="Hide System Topics"
                desc="Hide topics starting with __ from the pipeline"
                value={hideSystemTopics}
                onChange={(v) => useGraphStore.getState().setHideSystemTopics(v)}
                isBright={isBright}
              />
              <InfoRow label="Show Producers" value={config.showProducers ? "Yes" : "No"} isBright={isBright} />
              <InfoRow label="Lag Threshold" value={String(config.lagWarnThreshold)} isBright={isBright} />
              <InfoRow label="Animations" value={config.animationsEnabled ? "Enabled" : "Disabled"} isBright={isBright} />
            </div>
          </div>

          {/* Lag Alert Rules */}
          <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
            <div className="flex items-center justify-between">
              <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Consumer Lag Alert Rules</h3>
              <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>{lagAlertRules.length} rules</span>
            </div>
            <div className="space-y-2">
              {lagAlertRules.map((rule) => (
                <div key={rule.id} className={`flex items-center gap-3 py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                  <button
                    onClick={() => {
                      const updated = lagAlertRules.map((r) => r.id === rule.id ? { ...r, enabled: !r.enabled } : r);
                      setLagAlertRules(updated);
                      saveLagAlertRules(updated);
                    }}
                    className={`relative shrink-0 rounded-full transition-colors duration-200 cursor-pointer ${
                      rule.enabled ? "bg-indigo-500" : isBright ? "bg-slate-300" : "bg-slate-600"
                    }`}
                    style={{ width: 32, height: 18 }}
                  >
                    <span className={`absolute top-0.5 left-0.5 w-[14px] h-[14px] rounded-full bg-white shadow transition-transform duration-200 ${rule.enabled ? "translate-x-[14px]" : ""}`} />
                  </button>
                  <div className="flex-1 min-w-0">
                    <span className={`text-[11px] font-mono block truncate ${isBright ? "text-slate-700" : "text-slate-200"}`}>{rule.groupPattern}</span>
                    <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Threshold: {rule.threshold.toLocaleString()}</span>
                  </div>
                  <button
                    onClick={() => {
                      const updated = lagAlertRules.filter((r) => r.id !== rule.id);
                      setLagAlertRules(updated);
                      saveLagAlertRules(updated);
                    }}
                    className={`text-[10px] px-1.5 py-1 rounded-lg transition-colors cursor-pointer ${isBright ? "text-red-400 hover:bg-red-50 hover:text-red-600" : "text-red-400/60 hover:bg-red-500/10 hover:text-red-400"}`}
                  >
                    <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </button>
                </div>
              ))}
            </div>
            <div className={`border-t pt-3 ${isBright ? "border-slate-200/60" : "border-slate-700/30"}`}>
              <div className={`text-[11px] font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Add Alert Rule</div>
              <div className="flex gap-2">
                <input
                  type="text"
                  placeholder="Group pattern (regex)"
                  value={newAlertPattern}
                  onChange={(e) => setNewAlertPattern(e.target.value)}
                  className={`flex-[2] px-2.5 py-1.5 rounded-lg text-xs font-mono border outline-none ${
                    isBright ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                      : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-600"
                  }`}
                />
                <input
                  type="number"
                  placeholder="Threshold"
                  value={newAlertThreshold}
                  onChange={(e) => setNewAlertThreshold(e.target.value)}
                  className={`w-24 px-2.5 py-1.5 rounded-lg text-xs font-mono border outline-none ${
                    isBright ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                      : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-600"
                  }`}
                />
                <button
                  onClick={() => {
                    if (!newAlertPattern.trim()) return;
                    try { new RegExp(newAlertPattern); } catch { useToastStore.getState().addToast("Invalid regex", "error"); return; }
                    const rule: LagAlertRule = { id: Date.now().toString(), groupPattern: newAlertPattern.trim(), threshold: Math.max(0, parseInt(newAlertThreshold) || 1000), enabled: true };
                    const updated = [...lagAlertRules, rule];
                    setLagAlertRules(updated);
                    saveLagAlertRules(updated);
                    setNewAlertPattern("");
                    setNewAlertThreshold("1000");
                    useToastStore.getState().addToast("Alert rule added", "success", 2000);
                  }}
                  disabled={!newAlertPattern.trim()}
                  className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                    isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                      : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                  }`}
                >
                  Add
                </button>
              </div>
            </div>
            {lagAlerts.length > 0 && (
              <div className={`border-t pt-3 ${isBright ? "border-slate-200/60" : "border-slate-700/30"}`}>
                <div className="flex items-center gap-2 mb-2">
                  <span className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
                  <span className={`text-[11px] font-semibold ${isBright ? "text-red-600" : "text-red-400"}`}>
                    {lagAlerts.length} Active Alert{lagAlerts.length > 1 ? "s" : ""}
                  </span>
                </div>
                <div className="space-y-1 max-h-32 overflow-y-auto">
                  {lagAlerts.map((a) => (
                    <div key={a.groupId} className={`flex items-center justify-between py-1.5 px-2 rounded-lg text-[10px] ${
                      isBright ? "bg-red-50/50" : "bg-red-500/[0.06]"
                    }`}>
                      <span className={`font-mono truncate ${isBright ? "text-red-700" : "text-red-300"}`}>{a.groupId}</span>
                      <span className={`font-bold shrink-0 ml-2 ${isBright ? "text-red-600" : "text-red-400"}`}>
                        lag: {a.lag.toLocaleString()} &gt; {a.rule.threshold.toLocaleString()}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Cluster Management */}
          <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
            <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Clusters</h3>
            <div className="space-y-2">
              {clusters.map((c) => (
                <div key={c.id} className={`flex items-center gap-3 py-2.5 px-3 rounded-xl ${
                  c.id === activeClusterId
                    ? isBright ? "bg-indigo-50 ring-1 ring-indigo-200" : "bg-indigo-500/10 ring-1 ring-indigo-500/30"
                    : isBright ? "bg-slate-50" : "bg-slate-800/30"
                }`}>
                  <span className="w-3 h-3 rounded-full shrink-0" style={{ background: c.color }} />
                  <div className="min-w-0 flex-1">
                    <div className={`text-[12px] font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>{c.name}</div>
                    <div className={`text-[10px] font-mono truncate ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                      {c.url || "(same origin)"}
                    </div>
                  </div>
                  {c.id === activeClusterId ? (
                    <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${
                      isBright ? "bg-indigo-100 text-indigo-700" : "bg-indigo-500/20 text-indigo-300"
                    }`}>Active</span>
                  ) : (
                    <button
                      onClick={() => setActiveCluster(c.id)}
                      className={`text-[10px] font-medium px-2 py-1 rounded-lg border transition-colors cursor-pointer ${
                        isBright
                          ? "bg-white border-slate-200 text-slate-600 hover:bg-indigo-50 hover:text-indigo-700 hover:border-indigo-200"
                          : "bg-slate-900 border-slate-700 text-slate-400 hover:bg-indigo-500/10 hover:text-indigo-300 hover:border-indigo-500/30"
                      }`}
                    >
                      Switch
                    </button>
                  )}
                  {c.id !== "default" && (
                    <button
                      onClick={() => removeCluster(c.id)}
                      className={`text-[10px] px-1.5 py-1 rounded-lg transition-colors cursor-pointer ${
                        isBright ? "text-red-400 hover:bg-red-50 hover:text-red-600" : "text-red-400/60 hover:bg-red-500/10 hover:text-red-400"
                      }`}
                      title="Remove cluster"
                    >
                      <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                        <path d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    </button>
                  )}
                </div>
              ))}
            </div>
            <div className={`border-t pt-3 mt-3 ${isBright ? "border-slate-200/60" : "border-slate-700/30"}`}>
              <div className={`text-[11px] font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Add Cluster</div>
              <div className="flex gap-2">
                <input
                  type="text"
                  placeholder="Name"
                  value={newClusterName}
                  onChange={(e) => setNewClusterName(e.target.value)}
                  className={`flex-1 px-2.5 py-1.5 rounded-lg text-xs border outline-none ${
                    isBright
                      ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                      : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-600"
                  }`}
                />
                <input
                  type="text"
                  placeholder="http://host:port"
                  value={newClusterUrl}
                  onChange={(e) => setNewClusterUrl(e.target.value)}
                  className={`flex-[2] px-2.5 py-1.5 rounded-lg text-xs font-mono border outline-none ${
                    isBright
                      ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400 placeholder:text-slate-300"
                      : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500 placeholder:text-slate-600"
                  }`}
                />
                <button
                  onClick={() => {
                    if (newClusterName.trim()) {
                      addCluster({ name: newClusterName.trim(), url: newClusterUrl.trim(), color: "" });
                      setNewClusterName("");
                      setNewClusterUrl("");
                      useToastStore.getState().addToast(`Cluster "${newClusterName.trim()}" added`, "success", 2000);
                    }
                  }}
                  disabled={!newClusterName.trim()}
                  className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 disabled:cursor-not-allowed ${
                    isBright
                      ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                      : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                  }`}
                >
                  Add
                </button>
              </div>
            </div>
          </div>

          {/* Data Export / Import */}
          <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
            <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Data Management</h3>
            <div className="space-y-3">
              <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className="min-w-0 mr-3">
                  <div className={`text-[12px] font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>Export Settings</div>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Download all client settings, alert rules, and favorites</div>
                </div>
                <button
                  onClick={() => {
                    const data = {
                      theme: localStorage.getItem("kafka-ui-theme"),
                      lagAlertRules: loadLagAlertRules(),
                      favorites: localStorage.getItem("kafka-favorites"),
                      clusters: localStorage.getItem("kafka-clusters"),
                      exportedAt: new Date().toISOString(),
                    };
                    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
                    const url = URL.createObjectURL(blob);
                    const a = document.createElement("a"); a.href = url; a.download = "kafka-ui-settings.json"; a.click(); URL.revokeObjectURL(url);
                    useToastStore.getState().addToast("Settings exported", "success", 2000);
                  }}
                  className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer shrink-0 ${
                    isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
                      : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                  }`}
                >
                  Export
                </button>
              </div>
              <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className="min-w-0 mr-3">
                  <div className={`text-[12px] font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>Import Settings</div>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Restore settings from a previously exported JSON file</div>
                </div>
                <label className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer shrink-0 ${
                  isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50"
                    : "bg-slate-800 border-slate-700/50 text-slate-400 hover:bg-slate-700"
                }`}>
                  Import
                  <input type="file" accept=".json" className="hidden" onChange={(e) => {
                    const file = e.target.files?.[0];
                    if (!file) return;
                    const reader = new FileReader();
                    reader.onload = () => {
                      try {
                        const data = JSON.parse(reader.result as string);
                        if (data.theme) localStorage.setItem("kafka-ui-theme", data.theme);
                        if (data.lagAlertRules) { saveLagAlertRules(data.lagAlertRules); setLagAlertRules(data.lagAlertRules); }
                        if (data.favorites) localStorage.setItem("kafka-favorites", data.favorites);
                        if (data.clusters) localStorage.setItem("kafka-clusters", data.clusters);
                        useToastStore.getState().addToast("Settings imported — reload for full effect", "success", 3000);
                      } catch { useToastStore.getState().addToast("Invalid settings file", "error"); }
                    };
                    reader.readAsText(file);
                    e.target.value = "";
                  }} />
                </label>
              </div>
              <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
                <div className="min-w-0 mr-3">
                  <div className={`text-[12px] font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>Clear Local Data</div>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Remove all client-side preferences and cached data</div>
                </div>
                <button
                  onClick={() => {
                    if (!confirm("Clear all local settings? This cannot be undone.")) return;
                    const keys = Object.keys(localStorage).filter((k) => k.startsWith("kafka-"));
                    keys.forEach((k) => localStorage.removeItem(k));
                    useToastStore.getState().addToast(`Cleared ${keys.length} local settings`, "success", 2000);
                  }}
                  className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer shrink-0 ${
                    isBright ? "bg-red-50 border-red-200/60 text-red-600 hover:bg-red-100"
                      : "bg-red-500/10 border-red-500/25 text-red-400 hover:bg-red-500/20"
                  }`}
                >
                  Clear
                </button>
              </div>
            </div>
          </div>

          {/* Keyboard Shortcuts */}
          <div className={`rounded-2xl border p-6 space-y-4 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
            <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>Keyboard Shortcuts</h3>
            <div className="space-y-2">
              <ShortcutRow keys={["1"]} desc="Dashboard" isBright={isBright} />
              <ShortcutRow keys={["2"]} desc="Pipeline graph" isBright={isBright} />
              <ShortcutRow keys={["3"]} desc="Applications" isBright={isBright} />
              <ShortcutRow keys={["4"]} desc="Topics" isBright={isBright} />
              <ShortcutRow keys={["5"]} desc="Consumer Groups" isBright={isBright} />
              <ShortcutRow keys={["6"]} desc="Brokers" isBright={isBright} />
              <ShortcutRow keys={["7"]} desc="Schema Registry" isBright={isBright} />
              <ShortcutRow keys={["8"]} desc="Connectors" isBright={isBright} />
              <ShortcutRow keys={["9"]} desc="Settings" isBright={isBright} />
              <ShortcutRow keys={["R"]} desc="Refresh current view data" isBright={isBright} />
              <ShortcutRow keys={["F"]} desc="Fit all nodes in view" isBright={isBright} />
              <ShortcutRow keys={["L"]} desc="Re-layout graph" isBright={isBright} />
              <ShortcutRow keys={[navigator.platform.includes("Mac") ? "\u2318" : "Ctrl", "K"]} desc="Command palette" isBright={isBright} />
              <ShortcutRow keys={["T"]} desc="Toggle dark/bright theme" isBright={isBright} />
              <ShortcutRow keys={["Z"]} desc="Toggle zen (fullscreen) mode" isBright={isBright} />
              <ShortcutRow keys={["/"]} desc="Focus pipeline search" isBright={isBright} />
              <ShortcutRow keys={["?"]} desc="Show keyboard shortcuts" isBright={isBright} />
              <ShortcutRow keys={["Esc"]} desc="Deselect / close panels" isBright={isBright} />
              <ShortcutRow keys={["Right-click"]} desc="Context menu on nodes" isBright={isBright} />
              <ShortcutRow keys={["Double-click"]} desc="Open inspector / navigate" isBright={isBright} />
            </div>
          </div>

          {/* API Endpoints Reference */}
          <div className={`rounded-2xl border p-6 space-y-4 lg:col-span-2 ${isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"}`}>
            <div className="flex items-center justify-between">
              <h3 className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>API Endpoints</h3>
              <span className={`text-[10px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>54 endpoints</span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6">
              <div className="space-y-1.5">
                <div className={`text-[10px] font-bold uppercase tracking-wider mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Topics</div>
                <ApiRow method="GET" path="/api/topics" isBright={isBright} />
                <ApiRow method="POST" path="/api/topics" isBright={isBright} />
                <ApiRow method="GET" path="/api/topics/:topic" isBright={isBright} />
                <ApiRow method="DELETE" path="/api/topics/:topic" isBright={isBright} />
                <ApiRow method="PUT" path="/api/topics/:topic/config" isBright={isBright} />
                <ApiRow method="POST" path="/api/topics/:topic/partitions" isBright={isBright} />
                <ApiRow method="GET" path="/api/topics/:topic/messages" isBright={isBright} />
                <ApiRow method="POST" path="/api/topics/:topic/produce" isBright={isBright} />
                <ApiRow method="GET" path="/api/topics/:topic/config-diff" isBright={isBright} />
                <ApiRow method="GET" path="/api/topics/:topic/key-distribution" isBright={isBright} />
                <ApiRow method="POST" path="/api/topics/:topic/search" isBright={isBright} />
                <ApiRow method="POST" path="/api/topics/:topic/replay" isBright={isBright} />
                <ApiRow method="POST" path="/api/topics/:topic/reassign" isBright={isBright} />
                <ApiRow method="GET" path="/api/topics/:topic/reassign" isBright={isBright} />

                <div className={`text-[10px] font-bold uppercase tracking-wider mt-3 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Consumer Groups</div>
                <ApiRow method="GET" path="/api/consumer-groups" isBright={isBright} />
                <ApiRow method="GET" path="/api/consumer-groups/:id" isBright={isBright} />
                <ApiRow method="DELETE" path="/api/consumer-groups/:id" isBright={isBright} />
                <ApiRow method="POST" path="/api/consumer-groups/:id/reset-offsets" isBright={isBright} />

                <div className={`text-[10px] font-bold uppercase tracking-wider mt-3 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Brokers & Cluster</div>
                <ApiRow method="GET" path="/api/brokers" isBright={isBright} />
                <ApiRow method="GET" path="/api/brokers/:id/config" isBright={isBright} />
                <ApiRow method="PUT" path="/api/brokers/:id/config" isBright={isBright} />
                <ApiRow method="GET" path="/api/cluster" isBright={isBright} />
                <ApiRow method="GET" path="/api/cluster/health" isBright={isBright} />
                <ApiRow method="POST" path="/api/cluster/elect-leaders" isBright={isBright} />
              </div>
              <div className="space-y-1.5">
                <div className={`text-[10px] font-bold uppercase tracking-wider mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Schema Registry</div>
                <ApiRow method="GET" path="/api/schema-registry/subjects" isBright={isBright} />
                <ApiRow method="GET" path="/api/schema-registry/subjects/:s/versions" isBright={isBright} />
                <ApiRow method="GET" path="/api/schema-registry/subjects/:s/versions/:v" isBright={isBright} />
                <ApiRow method="POST" path="/api/schema-registry/subjects/:s/versions" isBright={isBright} />
                <ApiRow method="DELETE" path="/api/schema-registry/subjects/:s" isBright={isBright} />
                <ApiRow method="GET" path="/api/schema-registry/config" isBright={isBright} />
                <ApiRow method="GET" path="/api/schema-registry/config/:s" isBright={isBright} />

                <div className={`text-[10px] font-bold uppercase tracking-wider mt-3 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Kafka Connect</div>
                <ApiRow method="GET" path="/api/connect/connectors" isBright={isBright} />
                <ApiRow method="GET" path="/api/connect/connectors/:name" isBright={isBright} />
                <ApiRow method="POST" path="/api/connect/connectors" isBright={isBright} />
                <ApiRow method="PUT" path="/api/connect/connectors/:name/config" isBright={isBright} />
                <ApiRow method="DELETE" path="/api/connect/connectors/:name" isBright={isBright} />
                <ApiRow method="PUT" path="/api/connect/connectors/:name/pause" isBright={isBright} />
                <ApiRow method="PUT" path="/api/connect/connectors/:name/resume" isBright={isBright} />
                <ApiRow method="POST" path="/api/connect/connectors/:name/restart" isBright={isBright} />
                <ApiRow method="POST" path="/api/connect/connectors/:name/tasks/:id/restart" isBright={isBright} />
                <ApiRow method="GET" path="/api/connect/plugins" isBright={isBright} />

                <div className={`text-[10px] font-bold uppercase tracking-wider mt-3 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>ACLs & Quotas</div>
                <ApiRow method="GET" path="/api/acls" isBright={isBright} />
                <ApiRow method="POST" path="/api/acls" isBright={isBright} />
                <ApiRow method="DELETE" path="/api/acls" isBright={isBright} />
                <ApiRow method="GET" path="/api/quotas" isBright={isBright} />
                <ApiRow method="POST" path="/api/quotas" isBright={isBright} />
                <ApiRow method="DELETE" path="/api/quotas" isBright={isBright} />

                <div className={`text-[10px] font-bold uppercase tracking-wider mt-3 mb-1 ${isBright ? "text-slate-400" : "text-slate-500"}`}>System</div>
                <ApiRow method="GET" path="/api/health" isBright={isBright} />
                <ApiRow method="GET" path="/api/config" isBright={isBright} />
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function InfoRow({ label, value, mono, isBright }: { label: string; value: string; mono?: boolean; isBright: boolean }) {
  return (
    <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
      <span className={`text-[11px] font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>{label}</span>
      <span className={`text-xs font-medium ${mono ? "font-mono" : ""} ${isBright ? "text-slate-800" : "text-white"}`}>{value}</span>
    </div>
  );
}

function ToggleRow({ label, desc, value, onChange, isBright }: {
  label: string; desc: string; value: boolean; onChange: (v: boolean) => void; isBright: boolean;
}) {
  return (
    <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
      <div className="min-w-0 mr-3">
        <div className={`text-[12px] font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>{label}</div>
        <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{desc}</div>
      </div>
      <button
        onClick={() => onChange(!value)}
        className={`relative shrink-0 w-10 h-5.5 rounded-full transition-colors duration-200 cursor-pointer ${
          value
            ? "bg-indigo-500"
            : isBright ? "bg-slate-300" : "bg-slate-600"
        }`}
        style={{ width: 40, height: 22 }}
      >
        <span
          className={`absolute top-0.5 left-0.5 w-[18px] h-[18px] rounded-full bg-white shadow transition-transform duration-200 ${
            value ? "translate-x-[18px]" : ""
          }`}
        />
      </button>
    </div>
  );
}

function NumberRow({ label, desc, value, onChange, onApply, suffix, isBright }: {
  label: string; desc: string; value: string; onChange: (v: string) => void; onApply: () => void; suffix: string; isBright: boolean;
}) {
  return (
    <div className={`flex items-center justify-between py-2.5 px-3 rounded-xl ${isBright ? "bg-slate-50" : "bg-slate-800/30"}`}>
      <div className="min-w-0 mr-3">
        <div className={`text-[12px] font-medium ${isBright ? "text-slate-700" : "text-slate-200"}`}>{label}</div>
        <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{desc}</div>
      </div>
      <div className="flex items-center gap-1.5 shrink-0">
        <input
          type="number"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") onApply(); }}
          className={`w-20 px-2 py-1 rounded-lg text-xs font-mono text-right border outline-none ${
            isBright
              ? "bg-white border-slate-200 text-slate-800 focus:border-indigo-400"
              : "bg-slate-900 border-slate-700 text-white focus:border-indigo-500"
          }`}
        />
        {suffix && <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{suffix}</span>}
        <button
          onClick={onApply}
          className={`px-2 py-1 rounded-lg text-[10px] font-medium border transition-colors cursor-pointer ${
            isBright
              ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100"
              : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
          }`}
        >
          Apply
        </button>
      </div>
    </div>
  );
}

function ApiRow({ method, path, isBright }: { method: string; path: string; isBright: boolean }) {
  const methodColors: Record<string, string> = {
    GET: isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/20 text-emerald-400",
    POST: isBright ? "bg-blue-100 text-blue-700" : "bg-blue-500/20 text-blue-400",
    PUT: isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/20 text-amber-400",
    DELETE: isBright ? "bg-red-100 text-red-700" : "bg-red-500/20 text-red-400",
  };
  return (
    <div className={`flex items-center gap-2 py-1.5 px-3 rounded-lg ${isBright ? "hover:bg-slate-50" : "hover:bg-slate-800/30"}`}>
      <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${methodColors[method] || ""}`}>{method}</span>
      <span className={`text-xs font-mono ${isBright ? "text-slate-600" : "text-slate-300"}`}>{path}</span>
    </div>
  );
}

function ShortcutRow({ keys, desc, isBright }: { keys: string[]; desc: string; isBright: boolean }) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-1">
        {keys.map((key, i) => (
          <span key={i}>
            <kbd className={`px-2 py-1 rounded-lg text-[11px] font-mono font-medium ${
              isBright ? "bg-slate-100 text-slate-600 border border-slate-200" : "bg-slate-800 text-slate-300 border border-slate-700"
            }`}>{key}</kbd>
            {i < keys.length - 1 && <span className={`text-[10px] mx-0.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>+</span>}
          </span>
        ))}
      </div>
      <span className={`text-xs ${isBright ? "text-slate-500" : "text-slate-400"}`}>{desc}</span>
    </div>
  );
}
