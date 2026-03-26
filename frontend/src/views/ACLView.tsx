import { useEffect, useState, useCallback } from "react";
import { useThemeStore } from "../store/themeStore";
import { useKafkaStore } from "../store/kafkaStore";

interface ACLEntry {
  principal: string;
  host: string;
  operation: string;
  permission: string;
  resourceType: string;
  resourceName: string;
  patternType: string;
}

export function ACLView() {
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const { topics, consumerGroups } = useKafkaStore();

  const [acls, setAcls] = useState<ACLEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filterText, setFilterText] = useState("");
  const [filterField, setFilterField] = useState<"all" | "principal" | "resource" | "operation">("all");

  // Create form
  const [showCreate, setShowCreate] = useState(false);
  const [form, setForm] = useState({
    resourceType: "TOPIC",
    resourceName: "",
    principal: "User:",
    operation: "READ",
    permission: "ALLOW",
    patternType: "LITERAL",
    host: "*",
  });
  const [creating, setCreating] = useState(false);
  const [createResult, setCreateResult] = useState<{ success: boolean; message: string } | null>(null);

  const fetchACLs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch("/api/acls");
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (data.error) setError(data.error);
      setAcls(data.acls || []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchACLs(); }, [fetchACLs]);

  const handleCreate = async () => {
    setCreating(true);
    setCreateResult(null);
    try {
      const resp = await fetch("/api/acls", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(form),
      });
      const data = await resp.json();
      if (resp.ok && data.success) {
        setCreateResult({ success: true, message: "ACL created" });
        fetchACLs();
      } else {
        setCreateResult({ success: false, message: data.detail || data.error || "Failed" });
      }
    } catch (e) {
      setCreateResult({ success: false, message: String(e) });
    }
    setCreating(false);
  };

  const handleDelete = async (acl: ACLEntry) => {
    try {
      await fetch("/api/acls", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          resourceType: acl.resourceType,
          resourceName: acl.resourceName,
          principal: acl.principal,
          operation: acl.operation,
          permission: acl.permission,
        }),
      });
      fetchACLs();
    } catch { /* ignore */ }
  };

  const filtered = acls.filter((a) => {
    if (!filterText) return true;
    const search = filterText.toLowerCase();
    if (filterField === "principal") return a.principal.toLowerCase().includes(search);
    if (filterField === "resource") return a.resourceName.toLowerCase().includes(search);
    if (filterField === "operation") return a.operation.toLowerCase().includes(search);
    return (
      a.principal.toLowerCase().includes(search) ||
      a.resourceName.toLowerCase().includes(search) ||
      a.operation.toLowerCase().includes(search) ||
      a.resourceType.toLowerCase().includes(search)
    );
  });

  const permColor = (perm: string) => {
    if (perm === "ALLOW") return isBright ? "text-emerald-600 bg-emerald-50" : "text-emerald-400 bg-emerald-500/10";
    if (perm === "DENY") return isBright ? "text-red-600 bg-red-50" : "text-red-400 bg-red-500/10";
    return isBright ? "text-slate-500 bg-slate-50" : "text-slate-400 bg-slate-800/50";
  };

  const resourceTypes = ["TOPIC", "GROUP", "CLUSTER", "TRANSACTIONAL_ID", "DELEGATION_TOKEN"];
  const operations = ["READ", "WRITE", "CREATE", "DELETE", "ALTER", "DESCRIBE", "CLUSTER_ACTION", "ALL"];

  return (
    <div className={`flex-1 overflow-hidden flex flex-col ${isBright ? "text-slate-800" : "text-white"}`}>
      {/* Header */}
      <div className={`px-8 py-5 border-b flex items-center justify-between ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
        <div>
          <h1 className={`text-xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Access Control Lists</h1>
          <p className={`text-xs mt-0.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
            {acls.length} ACL entries &middot; {new Set(acls.map((a) => a.principal)).size} principals
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
            Create ACL
          </button>
          <button
            onClick={fetchACLs}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}
          >
            Refresh
          </button>
          {acls.length > 0 && (
            <button
              onClick={() => {
                const blob = new Blob([JSON.stringify(acls, null, 2)], { type: "application/json" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url; a.download = "kafka-acls.json"; a.click();
                URL.revokeObjectURL(url);
              }}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
                isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-400 hover:bg-slate-700"
              }`}
            >
              Export
            </button>
          )}
        </div>
      </div>

      {/* Create form */}
      {showCreate && (
        <div className={`px-8 py-4 border-b space-y-3 ${isBright ? "border-slate-200/40 bg-indigo-50/30" : "border-slate-800/50 bg-indigo-950/10"}`}>
          {createResult && (
            <div className={`text-xs px-3 py-1.5 rounded ${
              createResult.success
                ? isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/15 text-emerald-300"
                : isBright ? "bg-red-100 text-red-700" : "bg-red-500/15 text-red-300"
            }`}>{createResult.message}</div>
          )}
          <div className="grid grid-cols-4 gap-3">
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Resource Type</label>
              <select
                value={form.resourceType}
                onChange={(e) => setForm({ ...form, resourceType: e.target.value })}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs border focus:outline-none cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              >
                {resourceTypes.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
            </div>
            <div className="relative">
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Resource Name</label>
              <input
                type="text"
                value={form.resourceName}
                onChange={(e) => setForm({ ...form, resourceName: e.target.value })}
                placeholder={form.resourceType === "TOPIC" ? "my-topic" : form.resourceType === "GROUP" ? "my-consumer-group" : "resource-name"}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs font-mono border focus:outline-none ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
                list="acl-resource-suggestions"
              />
              <datalist id="acl-resource-suggestions">
                {form.resourceType === "TOPIC" && topics.map((t) => <option key={t.name} value={t.name} />)}
                {form.resourceType === "GROUP" && consumerGroups.map((g) => <option key={g.groupId} value={g.groupId} />)}
              </datalist>
            </div>
            <div className="relative">
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Principal</label>
              <input
                type="text"
                value={form.principal}
                onChange={(e) => setForm({ ...form, principal: e.target.value })}
                placeholder="User:alice"
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs font-mono border focus:outline-none ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
                list="acl-principal-suggestions"
              />
              <datalist id="acl-principal-suggestions">
                {[...new Set(acls.map((a) => a.principal))].map((p) => <option key={p} value={p} />)}
              </datalist>
            </div>
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Operation</label>
              <select
                value={form.operation}
                onChange={(e) => setForm({ ...form, operation: e.target.value })}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs border focus:outline-none cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              >
                {operations.map((o) => <option key={o} value={o}>{o}</option>)}
              </select>
            </div>
          </div>
          <div className="grid grid-cols-4 gap-3">
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Permission</label>
              <select
                value={form.permission}
                onChange={(e) => setForm({ ...form, permission: e.target.value })}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs border focus:outline-none cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              >
                <option value="ALLOW">ALLOW</option>
                <option value="DENY">DENY</option>
              </select>
            </div>
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Pattern</label>
              <select
                value={form.patternType}
                onChange={(e) => setForm({ ...form, patternType: e.target.value })}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs border focus:outline-none cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              >
                <option value="LITERAL">LITERAL</option>
                <option value="PREFIXED">PREFIXED</option>
              </select>
            </div>
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Host</label>
              <input
                type="text"
                value={form.host}
                onChange={(e) => setForm({ ...form, host: e.target.value })}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs font-mono border focus:outline-none ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={handleCreate}
                disabled={creating || !form.resourceName || !form.principal}
                className={`px-4 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                  isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                }`}
              >
                {creating ? "Creating..." : "Create"}
              </button>
            </div>
          </div>
        </div>
      )}

      {error && (
        <div className={`mx-8 mt-4 p-3 rounded-xl border text-xs ${
          isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/10 border-amber-500/20 text-amber-300"
        }`}>{error}</div>
      )}

      {/* Summary Overview */}
      {acls.length > 0 && (() => {
        const principals = [...new Set(acls.map((a) => a.principal))];
        const resources = [...new Set(acls.map((a) => `${a.resourceType}:${a.resourceName}`))];
        const allowCount = acls.filter((a) => a.permission === "ALLOW").length;
        const denyCount = acls.filter((a) => a.permission === "DENY").length;
        const resourceTypeCounts: Record<string, number> = {};
        acls.forEach((a) => { resourceTypeCounts[a.resourceType] = (resourceTypeCounts[a.resourceType] || 0) + 1; });
        const opCounts: Record<string, number> = {};
        acls.forEach((a) => { opCounts[a.operation] = (opCounts[a.operation] || 0) + 1; });
        return (
          <div className={`mx-8 mt-4 flex items-stretch gap-4`}>
            {/* Permission donut */}
            <div className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
              <svg width="44" height="44" viewBox="0 0 44 44">
                {(() => {
                  const total = acls.length || 1;
                  const allowFrac = allowCount / total;
                  const denyFrac = denyCount / total;
                  const C = 2 * Math.PI * 17;
                  return (
                    <>
                      <circle cx="22" cy="22" r="17" fill="none" stroke="#10b981" strokeWidth="5"
                        strokeDasharray={`${allowFrac * C} ${C - allowFrac * C}`} strokeDashoffset={0} transform="rotate(-90 22 22)" />
                      <circle cx="22" cy="22" r="17" fill="none" stroke="#ef4444" strokeWidth="5"
                        strokeDasharray={`${denyFrac * C} ${C - denyFrac * C}`} strokeDashoffset={-(allowFrac * C)} transform="rotate(-90 22 22)" />
                    </>
                  );
                })()}
              </svg>
              <div className="text-[10px] space-y-0.5">
                <div className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-emerald-500" /><span className={isBright ? "text-slate-600" : "text-slate-300"}>{allowCount} Allow</span></div>
                <div className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-red-500" /><span className={isBright ? "text-slate-600" : "text-slate-300"}>{denyCount} Deny</span></div>
              </div>
            </div>
            {/* Stats */}
            <div className={`flex items-center gap-4 px-4 py-3 rounded-xl border ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
              <div className="text-center">
                <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{principals.length}</div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Principals</div>
              </div>
              <div className="text-center">
                <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{resources.length}</div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Resources</div>
              </div>
              <div className="text-center">
                <div className={`text-lg font-bold ${isBright ? "text-slate-800" : "text-white"}`}>{[...new Set(acls.map((a) => a.host))].length}</div>
                <div className={`text-[9px] uppercase tracking-wider ${isBright ? "text-slate-400" : "text-slate-500"}`}>Hosts</div>
              </div>
            </div>
            {/* Resource type distribution */}
            <div className={`flex items-center gap-2 px-4 py-3 rounded-xl border flex-1 ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
              <div className={`text-[9px] uppercase tracking-wider font-medium shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Resources</div>
              <div className="flex items-center gap-2 flex-wrap">
                {Object.entries(resourceTypeCounts).sort((a, b) => b[1] - a[1]).map(([type, count]) => (
                  <span key={type} className={`text-[10px] font-medium px-2 py-0.5 rounded border ${
                    type === "TOPIC"
                      ? isBright ? "bg-indigo-50 text-indigo-600 border-indigo-200/60" : "bg-indigo-500/10 text-indigo-400 border-indigo-500/20"
                      : type === "GROUP"
                        ? isBright ? "bg-violet-50 text-violet-600 border-violet-200/60" : "bg-violet-500/10 text-violet-400 border-violet-500/20"
                        : type === "CLUSTER"
                          ? isBright ? "bg-cyan-50 text-cyan-600 border-cyan-200/60" : "bg-cyan-500/10 text-cyan-400 border-cyan-500/20"
                          : isBright ? "bg-slate-50 text-slate-500 border-slate-200" : "bg-slate-800/40 text-slate-400 border-slate-700/40"
                  }`}>
                    {type}: {count}
                  </span>
                ))}
              </div>
            </div>
            {/* Operation distribution */}
            <div className={`flex items-center gap-2 px-4 py-3 rounded-xl border ${isBright ? "bg-white/60 border-slate-200/60" : "bg-slate-900/40 border-slate-700/30"}`}>
              <div className={`text-[9px] uppercase tracking-wider font-medium shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>Ops</div>
              <div className="flex items-center gap-1 flex-wrap">
                {Object.entries(opCounts).sort((a, b) => b[1] - a[1]).slice(0, 4).map(([op, count]) => (
                  <span key={op} className={`text-[9px] font-mono px-1.5 py-0.5 rounded ${isBright ? "bg-slate-100 text-slate-600" : "bg-slate-800/50 text-slate-400"}`}>
                    {op}:{count}
                  </span>
                ))}
              </div>
            </div>
          </div>
        );
      })()}

      {/* Filter bar */}
      <div className={`px-8 py-3 border-b flex items-center gap-3 ${isBright ? "border-slate-200/40" : "border-slate-800/40"}`}>
        <select
          value={filterField}
          onChange={(e) => setFilterField(e.target.value as typeof filterField)}
          className={`px-2 py-1 rounded-lg text-[11px] border focus:outline-none cursor-pointer ${
            isBright ? "bg-white border-slate-200 text-slate-600" : "bg-slate-800/60 border-slate-700/40 text-slate-300"
          }`}
        >
          <option value="all">All fields</option>
          <option value="principal">Principal</option>
          <option value="resource">Resource</option>
          <option value="operation">Operation</option>
        </select>
        <input
          type="text"
          value={filterText}
          onChange={(e) => setFilterText(e.target.value)}
          placeholder="Filter ACLs..."
          className={`flex-1 max-w-xs px-3 py-1 rounded-lg text-[11px] border focus:outline-none ${
            isBright ? "bg-white border-slate-200 text-slate-700 placeholder-slate-400" : "bg-slate-800/60 border-slate-700/40 text-slate-300 placeholder-slate-500"
          }`}
        />
        <span className={`text-[11px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{filtered.length} entries</span>
      </div>

      {/* ACL table */}
      <div className="flex-1 overflow-y-auto px-8 py-4">
        {loading ? (
          <div className="flex items-center justify-center h-32">
            <div className="w-5 h-5 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
          </div>
        ) : filtered.length === 0 ? (
          <div className={`text-center py-12 text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
            {filterText ? "No matching ACLs" : "No ACLs found"}
          </div>
        ) : (
          <>
          {/* Permission Matrix - grouped by principal */}
          {filtered.length > 0 && filtered.length <= 200 && (() => {
            const principals = [...new Set(filtered.map((a) => a.principal))].slice(0, 10);
            const resources = [...new Set(filtered.map((a) => `${a.resourceType}:${a.resourceName}`))].slice(0, 15);
            if (principals.length < 2 || resources.length < 2) return null;
            const matrix: Record<string, Record<string, { allow: string[]; deny: string[] }>> = {};
            principals.forEach((p) => {
              matrix[p] = {};
              resources.forEach((r) => { matrix[p][r] = { allow: [], deny: [] }; });
            });
            filtered.forEach((a) => {
              const key = `${a.resourceType}:${a.resourceName}`;
              if (matrix[a.principal]?.[key]) {
                if (a.permission === "ALLOW") matrix[a.principal][key].allow.push(a.operation);
                else matrix[a.principal][key].deny.push(a.operation);
              }
            });
            return (
              <div className="mb-6">
                <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                  Permission Matrix (Top {principals.length} principals x {resources.length} resources)
                </div>
                <div className="overflow-x-auto">
                  <table className="text-[9px]">
                    <thead>
                      <tr>
                        <th className={`text-left px-2 py-1 ${isBright ? "text-slate-500" : "text-slate-400"}`}>Principal</th>
                        {resources.map((r) => (
                          <th key={r} className={`text-center px-1 py-1 font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`} title={r}>
                            {r.split(":")[1]?.slice(0, 10) || r.slice(0, 10)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {principals.map((p) => (
                        <tr key={p} className={`border-t ${isBright ? "border-slate-100" : "border-slate-800/30"}`}>
                          <td className={`px-2 py-1 font-mono truncate max-w-[120px] ${isBright ? "text-indigo-600" : "text-indigo-400"}`}>{p}</td>
                          {resources.map((r) => {
                            const cell = matrix[p][r];
                            const hasAllow = cell.allow.length > 0;
                            const hasDeny = cell.deny.length > 0;
                            return (
                              <td key={r} className="text-center px-1 py-1" title={[...cell.allow.map((o) => `ALLOW:${o}`), ...cell.deny.map((o) => `DENY:${o}`)].join(", ")}>
                                {hasAllow && hasDeny ? (
                                  <span className={`px-1 py-0.5 rounded ${isBright ? "bg-amber-50 text-amber-600" : "bg-amber-500/10 text-amber-400"}`}>
                                    {cell.allow.length}A/{cell.deny.length}D
                                  </span>
                                ) : hasAllow ? (
                                  <span className={`px-1 py-0.5 rounded ${isBright ? "bg-emerald-50 text-emerald-600" : "bg-emerald-500/10 text-emerald-400"}`}>
                                    {cell.allow.length === 1 ? cell.allow[0].slice(0, 3) : `${cell.allow.length}A`}
                                  </span>
                                ) : hasDeny ? (
                                  <span className={`px-1 py-0.5 rounded ${isBright ? "bg-red-50 text-red-600" : "bg-red-500/10 text-red-400"}`}>
                                    {cell.deny.length === 1 ? cell.deny[0].slice(0, 3) : `${cell.deny.length}D`}
                                  </span>
                                ) : (
                                  <span className={isBright ? "text-slate-300" : "text-slate-700"}>-</span>
                                )}
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
          })()}

          <table className="w-full">
            <thead>
              <tr className={`text-[10px] uppercase tracking-wider ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                <th className="text-left pb-2 font-medium">Resource</th>
                <th className="text-left pb-2 font-medium">Name</th>
                <th className="text-left pb-2 font-medium">Pattern</th>
                <th className="text-left pb-2 font-medium">Principal</th>
                <th className="text-left pb-2 font-medium">Host</th>
                <th className="text-left pb-2 font-medium">Operation</th>
                <th className="text-left pb-2 font-medium">Permission</th>
                <th className="text-right pb-2 font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((acl, i) => (
                <tr
                  key={i}
                  className={`border-t ${isBright ? "border-slate-100" : "border-slate-800/30"} hover:${isBright ? "bg-slate-50/50" : "bg-slate-800/20"}`}
                >
                  <td className="py-2">
                    <span className={`text-[10px] font-mono px-1.5 py-0.5 rounded ${isBright ? "bg-slate-100 text-slate-600" : "bg-slate-800/50 text-slate-400"}`}>
                      {acl.resourceType}
                    </span>
                  </td>
                  <td className={`py-2 text-xs font-mono ${isBright ? "text-slate-700" : "text-slate-300"}`}>{acl.resourceName}</td>
                  <td className={`py-2 text-[10px] ${isBright ? "text-slate-500" : "text-slate-400"}`}>{acl.patternType}</td>
                  <td className={`py-2 text-xs font-mono ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{acl.principal}</td>
                  <td className={`py-2 text-[10px] font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>{acl.host}</td>
                  <td className={`py-2 text-xs font-bold ${isBright ? "text-slate-700" : "text-white"}`}>{acl.operation}</td>
                  <td className="py-2">
                    <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded ${permColor(acl.permission)}`}>{acl.permission}</span>
                  </td>
                  <td className="py-2 text-right">
                    <button
                      onClick={() => handleDelete(acl)}
                      className={`text-[10px] px-1.5 py-0.5 rounded cursor-pointer transition-colors ${
                        isBright ? "text-red-400 hover:text-red-600 hover:bg-red-50" : "text-red-500 hover:text-red-300 hover:bg-red-500/10"
                      }`}
                    >
                      Delete
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          </>
        )}
      </div>
    </div>
  );
}
