import { useEffect, useState, useCallback, useMemo } from "react";
import { useThemeStore } from "../store/themeStore";

interface SchemaVersion {
  subject: string;
  version: number;
  id: number;
  schema: string;
  schemaType?: string;
}

export function SchemaRegistryView() {
  const { theme } = useThemeStore();
  const isBright = theme === "bright";

  const [subjects, setSubjects] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [versions, setVersions] = useState<number[]>([]);
  const [selectedSchema, setSelectedSchema] = useState<SchemaVersion | null>(null);
  const [globalCompat, setGlobalCompat] = useState("UNKNOWN");
  const [subjectCompat, setSubjectCompat] = useState("UNKNOWN");
  const [filterText, setFilterText] = useState("");
  const [compareVersion, setCompareVersion] = useState<SchemaVersion | null>(null);
  const [showDiff, setShowDiff] = useState(false);

  // Register form
  const [showRegister, setShowRegister] = useState(false);
  const [regSubject, setRegSubject] = useState("");
  const [regSchema, setRegSchema] = useState("");
  const [regType, setRegType] = useState("AVRO");
  const [registering, setRegistering] = useState(false);
  const [regResult, setRegResult] = useState<{ success: boolean; message: string } | null>(null);

  // Compatibility testing
  const [compatTestSchema, setCompatTestSchema] = useState("");
  const [compatTestResult, setCompatTestResult] = useState<{ compatible: boolean; message: string } | null>(null);
  const [compatTesting, setCompatTesting] = useState(false);
  const [showCompatTest, setShowCompatTest] = useState(false);

  // Schema evolution lineage - load all versions for field change tracking
  const [allVersionSchemas, setAllVersionSchemas] = useState<SchemaVersion[]>([]);
  useEffect(() => {
    if (!selectedSubject || versions.length < 2) { setAllVersionSchemas([]); return; }
    Promise.all(
      versions.map((v) =>
        fetch(`/api/schema-registry/subjects/${encodeURIComponent(selectedSubject)}/versions/${v}`)
          .then((r) => r.ok ? r.json() : null)
          .catch(() => null)
      )
    ).then((results) => setAllVersionSchemas(results.filter(Boolean)));
  }, [selectedSubject, versions]);

  const evolutionLineage = useMemo(() => {
    if (allVersionSchemas.length < 2) return [];
    const getFields = (schema: string): Map<string, string> => {
      try {
        const parsed = JSON.parse(schema);
        const fields = new Map<string, string>();
        if (parsed.fields && Array.isArray(parsed.fields)) {
          for (const f of parsed.fields) fields.set(f.name, typeof f.type === "string" ? f.type : JSON.stringify(f.type));
        } else if (parsed.properties && typeof parsed.properties === "object") {
          for (const [k, v] of Object.entries(parsed.properties)) fields.set(k, (v as { type?: string })?.type || "object");
        }
        return fields;
      } catch { return new Map(); }
    };
    const changes: { fromVersion: number; toVersion: number; added: string[]; removed: string[]; modified: { field: string; from: string; to: string }[] }[] = [];
    for (let i = 1; i < allVersionSchemas.length; i++) {
      const prev = getFields(allVersionSchemas[i - 1].schema);
      const curr = getFields(allVersionSchemas[i].schema);
      const added: string[] = [];
      const removed: string[] = [];
      const modified: { field: string; from: string; to: string }[] = [];
      for (const [k, v] of curr) {
        if (!prev.has(k)) added.push(k);
        else if (prev.get(k) !== v) modified.push({ field: k, from: prev.get(k)!, to: v });
      }
      for (const k of prev.keys()) { if (!curr.has(k)) removed.push(k); }
      changes.push({ fromVersion: allVersionSchemas[i - 1].version, toVersion: allVersionSchemas[i].version, added, removed, modified });
    }
    return changes;
  }, [allVersionSchemas]);

  // Schema field analysis
  const schemaFields = useMemo(() => {
    if (!selectedSchema) return null;
    try {
      const parsed = JSON.parse(selectedSchema.schema);
      if (parsed.fields && Array.isArray(parsed.fields)) {
        return parsed.fields.map((f: { name: string; type: unknown; doc?: string }) => ({
          name: f.name,
          type: typeof f.type === "string" ? f.type : JSON.stringify(f.type),
          doc: f.doc || "",
        }));
      }
      if (parsed.properties && typeof parsed.properties === "object") {
        return Object.entries(parsed.properties).map(([name, def]: [string, unknown]) => ({
          name,
          type: (def as { type?: string })?.type || "object",
          doc: (def as { description?: string })?.description || "",
        }));
      }
    } catch { /* not parseable */ }
    return null;
  }, [selectedSchema]);

  const fetchSubjects = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch("/api/schema-registry/subjects");
      if (resp.status === 503) {
        setError("Schema Registry not configured. Set SCHEMA_REGISTRY_URL environment variable.");
        setSubjects([]);
        return;
      }
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setSubjects(data.subjects || []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchSubjects(); }, [fetchSubjects]);
  useEffect(() => {
    fetch("/api/schema-registry/config")
      .then((r) => r.ok ? r.json() : null)
      .then((data) => { if (data) setGlobalCompat(data.compatibilityLevel); })
      .catch(() => {});
  }, []);

  const selectSubject = async (subject: string) => {
    setSelectedSubject(subject);
    setSelectedSchema(null);
    try {
      const resp = await fetch(`/api/schema-registry/subjects/${encodeURIComponent(subject)}/versions`);
      if (!resp.ok) return;
      const data = await resp.json();
      setVersions(data.versions || []);
      // Auto-load latest
      const latestResp = await fetch(`/api/schema-registry/subjects/${encodeURIComponent(subject)}/versions/latest`);
      if (latestResp.ok) {
        const schema = await latestResp.json();
        setSelectedSchema(schema);
      }
      // Load subject compatibility
      fetch(`/api/schema-registry/config/${encodeURIComponent(subject)}`)
        .then((r) => r.ok ? r.json() : null)
        .then((data) => { if (data) setSubjectCompat(data.compatibilityLevel); })
        .catch(() => setSubjectCompat("UNKNOWN"));
    } catch { /* ignore */ }
  };

  const loadVersion = async (version: number) => {
    if (!selectedSubject) return;
    try {
      const resp = await fetch(`/api/schema-registry/subjects/${encodeURIComponent(selectedSubject)}/versions/${version}`);
      if (resp.ok) setSelectedSchema(await resp.json());
    } catch { /* ignore */ }
  };

  const handleRegister = async () => {
    setRegistering(true);
    setRegResult(null);
    try {
      const resp = await fetch(`/api/schema-registry/subjects/${encodeURIComponent(regSubject)}/versions`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ schema: regSchema, schemaType: regType }),
      });
      const data = await resp.json();
      if (resp.ok) {
        setRegResult({ success: true, message: `Registered as ID ${data.id}` });
        fetchSubjects();
        setRegSchema("");
      } else {
        setRegResult({ success: false, message: data.detail || "Registration failed" });
      }
    } catch (e) {
      setRegResult({ success: false, message: String(e) });
    }
    setRegistering(false);
  };

  const handleDelete = async (subject: string) => {
    try {
      const resp = await fetch(`/api/schema-registry/subjects/${encodeURIComponent(subject)}`, { method: "DELETE" });
      if (resp.ok) {
        fetchSubjects();
        if (selectedSubject === subject) {
          setSelectedSubject(null);
          setSelectedSchema(null);
        }
      }
    } catch { /* ignore */ }
  };

  const filtered = filterText
    ? subjects.filter((s) => s.toLowerCase().includes(filterText.toLowerCase()))
    : subjects;

  const formatSchema = (schema: string): string => {
    try {
      return JSON.stringify(JSON.parse(schema), null, 2);
    } catch {
      return schema;
    }
  };

  // Auto-load prev version for compare when diff mode activated
  useEffect(() => {
    if (showDiff && selectedSubject && selectedSchema && versions.length > 1 && !compareVersion) {
      const prevVer = versions.filter((v) => v < selectedSchema.version).pop();
      if (prevVer) {
        fetch(`/api/schema-registry/subjects/${encodeURIComponent(selectedSubject)}/versions/${prevVer}`)
          .then((r) => r.ok ? r.json() : null)
          .then((data) => { if (data) setCompareVersion(data); })
          .catch(() => {});
      }
    }
  }, [showDiff, selectedSubject, selectedSchema, versions, compareVersion]);

  const card = `rounded-2xl border p-5 ${isBright ? "bg-white/80 border-slate-200/60" : "bg-slate-900/60 border-slate-700/30"}`;

  return (
    <div className={`flex-1 overflow-hidden flex flex-col ${isBright ? "text-slate-800" : "text-white"}`}>
      {/* Header */}
      <div className={`px-8 py-5 border-b flex items-center justify-between ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
        <div>
          <h1 className={`text-xl font-bold ${isBright ? "text-slate-800" : "text-white"}`}>Schema Registry</h1>
          <p className={`text-xs mt-0.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
            {subjects.length} subjects &middot; Compatibility: {globalCompat}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setShowRegister(!showRegister)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
              showRegister
                ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                : isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}
          >
            Register Schema
          </button>
          <button
            onClick={fetchSubjects}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
            }`}
          >
            Refresh
          </button>
        </div>
      </div>

      {/* Register form */}
      {showRegister && (
        <div className={`px-8 py-4 border-b space-y-3 ${isBright ? "border-slate-200/40 bg-indigo-50/30" : "border-slate-800/50 bg-indigo-950/10"}`}>
          {regResult && (
            <div className={`text-xs px-3 py-1.5 rounded ${
              regResult.success
                ? isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/15 text-emerald-300"
                : isBright ? "bg-red-100 text-red-700" : "bg-red-500/15 text-red-300"
            }`}>{regResult.message}</div>
          )}
          <div className="grid grid-cols-3 gap-3">
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Subject</label>
              <input
                type="text"
                value={regSubject}
                onChange={(e) => setRegSubject(e.target.value)}
                placeholder="my-topic-value"
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs font-mono border focus:outline-none ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              />
            </div>
            <div>
              <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Type</label>
              <select
                value={regType}
                onChange={(e) => setRegType(e.target.value)}
                className={`w-full mt-0.5 rounded-lg px-3 py-1.5 text-xs border focus:outline-none cursor-pointer ${
                  isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                }`}
              >
                <option value="AVRO">Avro</option>
                <option value="JSON">JSON Schema</option>
                <option value="PROTOBUF">Protobuf</option>
              </select>
            </div>
            <div className="flex items-end">
              <button
                onClick={handleRegister}
                disabled={registering || !regSubject || !regSchema}
                className={`px-4 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                  isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                }`}
              >
                {registering ? "Registering..." : "Register"}
              </button>
            </div>
          </div>
          <div>
            <label className={`text-[10px] uppercase ${isBright ? "text-slate-500" : "text-slate-400"}`}>Schema</label>
            <textarea
              value={regSchema}
              onChange={(e) => setRegSchema(e.target.value)}
              placeholder='{"type": "record", "name": "MyRecord", "fields": [...]}'
              rows={4}
              className={`w-full mt-0.5 rounded-lg px-3 py-2 text-xs font-mono border focus:outline-none resize-y ${
                isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
              }`}
            />
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className={`mx-8 mt-4 p-4 rounded-xl border text-sm ${
          isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/10 border-amber-500/20 text-amber-300"
        }`}>{error}</div>
      )}

      {/* Main content */}
      <div className="flex-1 overflow-hidden flex px-8 py-5 gap-5">
        {/* Subject list */}
        <div className={`w-72 shrink-0 flex flex-col ${card}`} style={{ padding: 0 }}>
          <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
            <input
              type="text"
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              placeholder="Filter subjects..."
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
                {filterText ? "No matching subjects" : "No subjects found"}
              </div>
            ) : (
              filtered.map((subject) => (
                <div
                  key={subject}
                  onClick={() => selectSubject(subject)}
                  className={`px-4 py-2.5 cursor-pointer border-b transition-colors flex items-center justify-between group ${
                    selectedSubject === subject
                      ? isBright ? "bg-indigo-50 border-indigo-100" : "bg-indigo-500/10 border-indigo-500/10"
                      : isBright ? "border-slate-100 hover:bg-slate-50" : "border-slate-800/30 hover:bg-slate-800/30"
                  }`}
                >
                  <span className={`text-xs font-mono truncate ${
                    selectedSubject === subject
                      ? isBright ? "text-indigo-700" : "text-indigo-300"
                      : isBright ? "text-slate-700" : "text-slate-300"
                  }`}>{subject}</span>
                  <button
                    onClick={(e) => { e.stopPropagation(); handleDelete(subject); }}
                    className={`opacity-0 group-hover:opacity-100 text-[10px] px-1.5 py-0.5 rounded cursor-pointer transition-opacity ${
                      isBright ? "text-red-500 hover:bg-red-50" : "text-red-400 hover:bg-red-500/10"
                    }`}
                    title="Delete subject"
                  >
                    Del
                  </button>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Schema viewer */}
        <div className={`flex-1 flex flex-col ${card}`} style={{ padding: 0 }}>
          {selectedSubject && selectedSchema ? (
            <>
              <div className={`px-5 py-3 border-b flex items-center justify-between ${isBright ? "border-slate-200/40" : "border-slate-700/30"}`}>
                <div>
                  <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-800" : "text-white"}`}>{selectedSubject}</div>
                  <div className={`text-[10px] mt-0.5 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                    Version {selectedSchema.version} &middot; ID {selectedSchema.id} &middot; {selectedSchema.schemaType || "AVRO"} &middot; Compat: {subjectCompat}
                  </div>
                </div>
                <div className="flex items-center gap-1.5">
                  <button
                    onClick={() => { setShowCompatTest(!showCompatTest); setCompatTestResult(null); }}
                    className={`px-2 py-0.5 rounded text-[10px] font-medium transition-colors cursor-pointer ${
                      showCompatTest
                        ? isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/20 text-emerald-300"
                        : isBright ? "text-slate-400 hover:bg-slate-100 border border-slate-200" : "text-slate-500 hover:bg-slate-800 border border-slate-700"
                    }`}
                  >
                    Test
                  </button>
                  {versions.length > 1 && (
                    <button
                      onClick={() => setShowDiff(!showDiff)}
                      className={`px-2 py-0.5 rounded text-[10px] font-medium transition-colors cursor-pointer ${
                        showDiff
                          ? isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/20 text-amber-300"
                          : isBright ? "text-slate-400 hover:bg-slate-100 border border-slate-200" : "text-slate-500 hover:bg-slate-800 border border-slate-700"
                      }`}
                    >
                      {showDiff ? "View" : "Diff"}
                    </button>
                  )}
                  {versions.map((v) => (
                    <button
                      key={v}
                      onClick={() => {
                        if (showDiff && selectedSchema.version !== v) {
                          // Load compare version
                          fetch(`/api/schema-registry/subjects/${encodeURIComponent(selectedSubject)}/versions/${v}`)
                            .then((r) => r.ok ? r.json() : null)
                            .then((data) => { if (data) setCompareVersion(data); })
                            .catch(() => {});
                        } else {
                          loadVersion(v);
                        }
                      }}
                      className={`px-2 py-0.5 rounded text-[10px] font-mono font-bold transition-colors cursor-pointer ${
                        selectedSchema.version === v
                          ? isBright ? "bg-indigo-100 text-indigo-700" : "bg-indigo-500/20 text-indigo-300"
                          : compareVersion?.version === v && showDiff
                            ? isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/20 text-amber-300"
                            : isBright ? "text-slate-400 hover:bg-slate-100" : "text-slate-500 hover:bg-slate-800"
                      }`}
                    >
                      v{v}
                    </button>
                  ))}
                </div>
              </div>
              {/* Compatibility test panel */}
              {showCompatTest && (
                <div className={`px-5 py-3 border-b space-y-2 ${isBright ? "border-slate-200/40 bg-indigo-50/20" : "border-slate-700/30 bg-indigo-950/10"}`}>
                  <div className="flex items-center justify-between">
                    <span className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                      Compatibility Test against {selectedSubject}
                    </span>
                    {compatTestResult && (
                      <span className={`text-[10px] font-bold px-2 py-0.5 rounded ${
                        compatTestResult.compatible
                          ? isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/20 text-emerald-300"
                          : isBright ? "bg-red-100 text-red-700" : "bg-red-500/20 text-red-300"
                      }`}>{compatTestResult.message}</span>
                    )}
                  </div>
                  <textarea
                    value={compatTestSchema}
                    onChange={(e) => setCompatTestSchema(e.target.value)}
                    placeholder="Paste schema to test compatibility..."
                    rows={3}
                    className={`w-full rounded-lg px-3 py-2 text-xs font-mono border focus:outline-none resize-y ${
                      isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
                    }`}
                  />
                  <button
                    onClick={async () => {
                      if (!compatTestSchema.trim() || !selectedSubject) return;
                      setCompatTesting(true);
                      setCompatTestResult(null);
                      try {
                        const resp = await fetch(`/api/schema-registry/subjects/${encodeURIComponent(selectedSubject)}/versions`, {
                          method: "POST",
                          headers: { "Content-Type": "application/json" },
                          body: JSON.stringify({ schema: compatTestSchema, schemaType: selectedSchema?.schemaType || "AVRO" }),
                        });
                        if (resp.ok) {
                          setCompatTestResult({ compatible: true, message: "Compatible - would register successfully" });
                        } else {
                          const data = await resp.json().catch(() => null);
                          setCompatTestResult({ compatible: false, message: data?.detail || `Incompatible (HTTP ${resp.status})` });
                        }
                      } catch (e) {
                        setCompatTestResult({ compatible: false, message: String(e) });
                      }
                      setCompatTesting(false);
                    }}
                    disabled={compatTesting || !compatTestSchema.trim()}
                    className={`px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
                      isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-700 hover:bg-indigo-100" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300 hover:bg-indigo-500/25"
                    }`}
                  >
                    {compatTesting ? "Testing..." : "Test Compatibility"}
                  </button>
                </div>
              )}

              {showDiff && compareVersion ? (
                <SchemaDiff schemaA={selectedSchema} schemaB={compareVersion} bright={isBright} formatSchema={formatSchema} />
              ) : (
                <div className="flex-1 overflow-y-auto p-5 space-y-4">
                  {/* Schema field table */}
                  {schemaFields && schemaFields.length > 0 && (
                    <div className={`rounded-xl border overflow-hidden ${isBright ? "border-slate-200/40" : "border-slate-700/20"}`}>
                      <table className="w-full text-xs">
                        <thead>
                          <tr className={isBright ? "bg-slate-50" : "bg-slate-800/30"}>
                            <th className={`text-left px-3 py-1.5 font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Field</th>
                            <th className={`text-left px-3 py-1.5 font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Type</th>
                            <th className={`text-left px-3 py-1.5 font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>Doc</th>
                          </tr>
                        </thead>
                        <tbody>
                          {schemaFields.map((f: { name: string; type: string; doc: string }) => (
                            <tr key={f.name} className={`border-t ${isBright ? "border-slate-100" : "border-slate-800/30"}`}>
                              <td className={`px-3 py-1.5 font-mono font-medium ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{f.name}</td>
                              <td className={`px-3 py-1.5 font-mono ${isBright ? "text-amber-600" : "text-amber-300"}`}>{f.type}</td>
                              <td className={`px-3 py-1.5 ${isBright ? "text-slate-400" : "text-slate-500"}`}>{f.doc || "-"}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* Version timeline */}
                  {versions.length > 1 && (
                    <div>
                      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                        Version History ({versions.length} versions)
                      </div>
                      <div className="flex items-center gap-0.5">
                        {versions.map((v, i) => (
                          <div key={v} className="flex items-center">
                            <button
                              onClick={() => loadVersion(v)}
                              className={`w-7 h-7 rounded-full flex items-center justify-center text-[10px] font-bold font-mono transition-all cursor-pointer ${
                                selectedSchema?.version === v
                                  ? isBright ? "bg-indigo-500 text-white" : "bg-indigo-500 text-white"
                                  : isBright ? "bg-slate-100 text-slate-500 hover:bg-indigo-50 hover:text-indigo-600" : "bg-slate-800/50 text-slate-400 hover:bg-indigo-500/20 hover:text-indigo-300"
                              }`}
                            >
                              {v}
                            </button>
                            {i < versions.length - 1 && (
                              <div className={`w-4 h-0.5 ${isBright ? "bg-slate-200" : "bg-slate-700"}`} />
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Evolution Lineage */}
                  {evolutionLineage.length > 0 && (
                    <div>
                      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                        Schema Evolution ({evolutionLineage.length} changes)
                      </div>
                      <div className="space-y-2">
                        {evolutionLineage.map((change, i) => {
                          const hasChanges = change.added.length > 0 || change.removed.length > 0 || change.modified.length > 0;
                          return (
                            <div key={i} className={`rounded-xl border p-3 ${isBright ? "border-slate-200/40 bg-slate-50/50" : "border-slate-700/20 bg-slate-800/20"}`}>
                              <div className="flex items-center gap-2 mb-1.5">
                                <span className={`text-[10px] font-mono font-bold ${isBright ? "text-indigo-600" : "text-indigo-400"}`}>
                                  v{change.fromVersion} → v{change.toVersion}
                                </span>
                                {change.added.length > 0 && <span className={`text-[9px] px-1.5 py-0.5 rounded font-mono ${isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/15 text-emerald-300"}`}>+{change.added.length}</span>}
                                {change.removed.length > 0 && <span className={`text-[9px] px-1.5 py-0.5 rounded font-mono ${isBright ? "bg-red-100 text-red-700" : "bg-red-500/15 text-red-300"}`}>-{change.removed.length}</span>}
                                {change.modified.length > 0 && <span className={`text-[9px] px-1.5 py-0.5 rounded font-mono ${isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/15 text-amber-300"}`}>~{change.modified.length}</span>}
                                {!hasChanges && <span className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>No field changes</span>}
                              </div>
                              {hasChanges && (
                                <div className="space-y-0.5">
                                  {change.added.map((f) => (
                                    <div key={f} className={`text-[10px] font-mono ${isBright ? "text-emerald-600" : "text-emerald-400"}`}>+ {f}</div>
                                  ))}
                                  {change.removed.map((f) => (
                                    <div key={f} className={`text-[10px] font-mono line-through ${isBright ? "text-red-600" : "text-red-400"}`}>- {f}</div>
                                  ))}
                                  {change.modified.map((m) => (
                                    <div key={m.field} className={`text-[10px] font-mono ${isBright ? "text-amber-600" : "text-amber-400"}`}>
                                      ~ {m.field}: <span className="line-through opacity-60">{m.from}</span> → {m.to}
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}

                  <pre className={`text-xs font-mono leading-relaxed whitespace-pre-wrap ${
                    isBright ? "text-slate-700" : "text-slate-300"
                  }`}>
                    {formatSchema(selectedSchema.schema)}
                  </pre>
                </div>
              )}
            </>
          ) : (
            <div className={`flex-1 flex items-center justify-center text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              {selectedSubject ? "Loading schema..." : "Select a subject to view its schema"}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function SchemaDiff({ schemaA, schemaB, bright, formatSchema }: {
  schemaA: SchemaVersion; schemaB: SchemaVersion; bright: boolean;
  formatSchema: (s: string) => string;
}) {
  const linesA = useMemo(() => formatSchema(schemaA.schema).split("\n"), [schemaA.schema, formatSchema]);
  const linesB = useMemo(() => formatSchema(schemaB.schema).split("\n"), [schemaB.schema, formatSchema]);

  // Simple line-based diff
  const diff = useMemo(() => {
    const result: { type: "same" | "added" | "removed"; line: string; lineNo: number }[] = [];
    const setA = new Set(linesA);
    const setB = new Set(linesB);
    const maxLen = Math.max(linesA.length, linesB.length);

    // LCS-based approach simplified: walk both arrays
    let ai = 0, bi = 0;
    while (ai < linesA.length || bi < linesB.length) {
      if (ai < linesA.length && bi < linesB.length && linesA[ai] === linesB[bi]) {
        result.push({ type: "same", line: linesA[ai], lineNo: bi + 1 });
        ai++; bi++;
      } else if (ai < linesA.length && !setB.has(linesA[ai])) {
        result.push({ type: "removed", line: linesA[ai], lineNo: ai + 1 });
        ai++;
      } else if (bi < linesB.length && !setA.has(linesB[bi])) {
        result.push({ type: "added", line: linesB[bi], lineNo: bi + 1 });
        bi++;
      } else if (ai < linesA.length) {
        result.push({ type: "removed", line: linesA[ai], lineNo: ai + 1 });
        ai++;
      } else {
        result.push({ type: "added", line: linesB[bi], lineNo: bi + 1 });
        bi++;
      }
      if (result.length > maxLen + 500) break;
    }
    return result;
  }, [linesA, linesB]);

  const addedCount = diff.filter((d) => d.type === "added").length;
  const removedCount = diff.filter((d) => d.type === "removed").length;

  return (
    <div className="flex-1 overflow-y-auto">
      <div className={`px-5 py-2 border-b flex items-center gap-3 ${bright ? "border-slate-200/40" : "border-slate-700/30"}`}>
        <span className={`text-[10px] font-medium ${bright ? "text-slate-500" : "text-slate-400"}`}>
          v{schemaA.version} → v{schemaB.version}
        </span>
        {addedCount > 0 && <span className={`text-[10px] font-mono ${bright ? "text-emerald-600" : "text-emerald-400"}`}>+{addedCount}</span>}
        {removedCount > 0 && <span className={`text-[10px] font-mono ${bright ? "text-red-600" : "text-red-400"}`}>-{removedCount}</span>}
        {addedCount === 0 && removedCount === 0 && <span className={`text-[10px] ${bright ? "text-slate-400" : "text-slate-500"}`}>No changes</span>}
      </div>
      <div className="px-5 py-3">
        {diff.map((d, i) => (
          <div
            key={i}
            className={`flex text-xs font-mono leading-5 ${
              d.type === "added"
                ? bright ? "bg-emerald-50/80 text-emerald-800" : "bg-emerald-500/[0.08] text-emerald-300"
                : d.type === "removed"
                  ? bright ? "bg-red-50/80 text-red-800 line-through opacity-70" : "bg-red-500/[0.08] text-red-300 line-through opacity-70"
                  : bright ? "text-slate-600" : "text-slate-400"
            }`}
          >
            <span className={`w-6 shrink-0 text-right pr-2 select-none ${
              d.type === "added" ? bright ? "text-emerald-500" : "text-emerald-500"
                : d.type === "removed" ? bright ? "text-red-400" : "text-red-500"
                : bright ? "text-slate-300" : "text-slate-600"
            }`}>
              {d.type === "added" ? "+" : d.type === "removed" ? "-" : " "}
            </span>
            <span className="whitespace-pre-wrap break-all">{d.line || " "}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
