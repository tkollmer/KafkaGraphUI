import { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { useThemeStore } from "../store/themeStore";

function JsonHighlight({ value, bright }: { value: unknown; bright: boolean }) {
  const json = useMemo(() => {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }, [value]);

  const highlighted = useMemo(() => {
    const parts: { text: string; type: "key" | "string" | "number" | "boolean" | "null" | "punct" }[] = [];
    const lines = json.split("\n");
    for (const line of lines) {
      // Match JSON tokens
      let rest = line;
      while (rest.length > 0) {
        // Key
        const keyMatch = rest.match(/^(\s*)"([^"]+)":/);
        if (keyMatch) {
          if (keyMatch[1]) parts.push({ text: keyMatch[1], type: "punct" });
          parts.push({ text: `"${keyMatch[2]}"`, type: "key" });
          parts.push({ text: ":", type: "punct" });
          rest = rest.slice(keyMatch[0].length);
          continue;
        }
        // String value
        const strMatch = rest.match(/^(\s*)"((?:[^"\\]|\\.)*)"/);
        if (strMatch) {
          if (strMatch[1]) parts.push({ text: strMatch[1], type: "punct" });
          parts.push({ text: `"${strMatch[2]}"`, type: "string" });
          rest = rest.slice(strMatch[0].length);
          continue;
        }
        // Number
        const numMatch = rest.match(/^(\s*)(-?\d+\.?\d*(?:[eE][+-]?\d+)?)/);
        if (numMatch) {
          if (numMatch[1]) parts.push({ text: numMatch[1], type: "punct" });
          parts.push({ text: numMatch[2], type: "number" });
          rest = rest.slice(numMatch[0].length);
          continue;
        }
        // Boolean/null
        const boolMatch = rest.match(/^(\s*)(true|false|null)/);
        if (boolMatch) {
          if (boolMatch[1]) parts.push({ text: boolMatch[1], type: "punct" });
          parts.push({ text: boolMatch[2], type: boolMatch[2] === "null" ? "null" : "boolean" });
          rest = rest.slice(boolMatch[0].length);
          continue;
        }
        // Punctuation or anything else
        parts.push({ text: rest[0], type: "punct" });
        rest = rest.slice(1);
      }
      parts.push({ text: "\n", type: "punct" });
    }
    return parts;
  }, [json]);

  const colorMap = bright
    ? { key: "text-indigo-700", string: "text-emerald-700", number: "text-amber-600", boolean: "text-blue-600", null: "text-red-500", punct: "text-slate-400" }
    : { key: "text-indigo-300", string: "text-emerald-300", number: "text-amber-300", boolean: "text-blue-300", null: "text-red-400", punct: "text-slate-500" };

  return (
    <>
      {highlighted.map((p, i) => (
        <span key={i} className={colorMap[p.type]}>{p.text}</span>
      ))}
    </>
  );
}

interface Message {
  offset: number;
  partition: number;
  timestamp: number;
  key: string | null;
  headers: Record<string, string | null>;
  value: unknown;
  format: string;
}

interface Props {
  topic: string;
  onClose: () => void;
  embedded?: boolean;
}

export function MessageInspector({ topic, onClose, embedded }: Props) {
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);
  const [copied, setCopied] = useState<number | null>(null);
  const [filterText, setFilterText] = useState("");
  const [useRegex, setUseRegex] = useState(false);
  const [regexError, setRegexError] = useState<string | null>(null);
  const [partitionFilter, setPartitionFilter] = useState<number | "all">("all");
  const [autoRefresh, setAutoRefresh] = useState(false);
  const intervalRef = useRef<ReturnType<typeof setInterval>>(undefined);

  const fetchMessages = useCallback(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/topics/${encodeURIComponent(topic)}/messages`)
      .then((r) => {
        if (r.status === 403) throw new Error("Message sampling is disabled on this cluster");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => setMessages(data.messages || []))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [topic]);

  useEffect(() => { fetchMessages(); }, [fetchMessages]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(fetchMessages, 3000);
      return () => clearInterval(intervalRef.current);
    }
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [autoRefresh, fetchMessages]);

  const copyPayload = (msg: Message, idx: number) => {
    const text = typeof msg.value === "object" ? JSON.stringify(msg.value, null, 2) : String(msg.value);
    navigator.clipboard.writeText(text);
    setCopied(idx);
    setTimeout(() => setCopied(null), 1500);
  };

  const partitions = [...new Set(messages.map((m) => m.partition))].sort((a, b) => a - b);

  const filtered = messages
    .filter((m) => partitionFilter === "all" || m.partition === partitionFilter)
    .filter((m) => {
      if (!filterText) return true;
      const valStr = typeof m.value === "object" ? JSON.stringify(m.value) : String(m.value);
      if (useRegex) {
        try {
          const re = new RegExp(filterText, "i");
          if (regexError) setRegexError(null);
          return re.test(valStr) || (m.key ? re.test(m.key) : false);
        } catch {
          if (!regexError) setRegexError("Invalid regex");
          return true;
        }
      }
      const search = filterText.toLowerCase();
      return valStr.toLowerCase().includes(search) ||
        (m.key && m.key.toLowerCase().includes(search)) ||
        String(m.partition).includes(search);
    });

  const formatBadgeCls = (format: string) => {
    if (format === "json") return isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/20 text-emerald-400";
    if (format === "utf8" || format === "text") return isBright ? "bg-blue-100 text-blue-700" : "bg-blue-500/20 text-blue-400";
    if (format === "avro") return isBright ? "bg-orange-100 text-orange-700" : "bg-orange-500/20 text-orange-400";
    if (format === "protobuf" || format === "binary") return isBright ? "bg-purple-100 text-purple-700" : "bg-purple-500/20 text-purple-400";
    return isBright ? "bg-slate-200 text-slate-600" : "bg-slate-500/20 text-slate-400";
  };

  // Detect schema from JSON values
  const detectSchema = (msg: Message): string | null => {
    if (msg.format !== "json" || typeof msg.value !== "object" || !msg.value) return null;
    const val = msg.value as Record<string, unknown>;
    if (val.schema && val.payload) return "Avro-style";
    if (val.specversion && val.type && val.source) return "CloudEvents";
    if (val.$schema) return "JSON Schema";
    return null;
  };

  // Format distribution
  const formatDist = messages.reduce<Record<string, number>>((acc, m) => {
    acc[m.format] = (acc[m.format] || 0) + 1;
    return acc;
  }, {});

  // Key distribution analysis
  const keyAnalysis = (() => {
    if (messages.length === 0) return null;
    const keyCounts = new Map<string, number>();
    let nullKeys = 0;
    for (const m of messages) {
      if (m.key === null || m.key === undefined) {
        nullKeys++;
      } else {
        keyCounts.set(m.key, (keyCounts.get(m.key) || 0) + 1);
      }
    }
    const uniqueKeys = keyCounts.size;
    const topKeys = [...keyCounts.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);
    const nullPct = messages.length > 0 ? (nullKeys / messages.length) * 100 : 0;
    return { uniqueKeys, nullKeys, nullPct, topKeys, total: messages.length };
  })();

  const [showKeyDist, setShowKeyDist] = useState(false);
  const [showTimeline, setShowTimeline] = useState(false);
  const [showHeaders, setShowHeaders] = useState(false);
  const [showSizes, setShowSizes] = useState(false);
  const [showFormats, setShowFormats] = useState(false);

  // Enhanced format detection
  const formatAnalysis = (() => {
    if (messages.length === 0) return null;
    const formats = new Map<string, number>();
    const schemas: string[] = [];
    let avgValueLen = 0;
    let jsonNested = 0;
    let jsonFlat = 0;
    for (const m of messages) {
      formats.set(m.format, (formats.get(m.format) || 0) + 1);
      const str = typeof m.value === "object" ? JSON.stringify(m.value) : String(m.value ?? "");
      avgValueLen += str.length;
      if (m.format === "json" && typeof m.value === "object" && m.value) {
        const schema = detectSchema(m);
        if (schema && !schemas.includes(schema)) schemas.push(schema);
        const vals = Object.values(m.value as Record<string, unknown>);
        if (vals.some((v) => typeof v === "object" && v !== null)) jsonNested++;
        else jsonFlat++;
      }
    }
    avgValueLen = Math.round(avgValueLen / messages.length);
    const dominant = [...formats.entries()].sort((a, b) => b[1] - a[1])[0];
    return { formats: [...formats.entries()], schemas, avgValueLen, jsonNested, jsonFlat, dominant, total: messages.length };
  })();

  // Value size analysis
  const sizeAnalysis = (() => {
    if (messages.length === 0) return null;
    const sizes = messages.map((m) => {
      const str = typeof m.value === "object" ? JSON.stringify(m.value) : String(m.value ?? "");
      return new Blob([str]).size;
    }).sort((a, b) => a - b);
    const min = sizes[0];
    const max = sizes[sizes.length - 1];
    const avg = sizes.reduce((s, v) => s + v, 0) / sizes.length;
    const p50 = sizes[Math.floor(sizes.length * 0.5)];
    const p99 = sizes[Math.floor(sizes.length * 0.99)];
    const total = sizes.reduce((s, v) => s + v, 0);
    // Create 8 buckets for histogram
    const range = max - min;
    const buckets = new Array(8).fill(0);
    if (range > 0) {
      for (const s of sizes) {
        const idx = Math.min(7, Math.floor(((s - min) / range) * 8));
        buckets[idx]++;
      }
    } else {
      buckets[4] = sizes.length;
    }
    return { min, max, avg, p50, p99, total, buckets, count: sizes.length };
  })();

  // Header analysis
  const headerAnalysis = (() => {
    if (messages.length === 0) return null;
    const headerKeys = new Map<string, number>();
    let withHeaders = 0;
    for (const m of messages) {
      if (m.headers && Object.keys(m.headers).length > 0) {
        withHeaders++;
        for (const k of Object.keys(m.headers)) {
          headerKeys.set(k, (headerKeys.get(k) || 0) + 1);
        }
      }
    }
    if (headerKeys.size === 0) return null;
    const topHeaders = [...headerKeys.entries()].sort((a, b) => b[1] - a[1]).slice(0, 8);
    return { withHeaders, total: messages.length, uniqueKeys: headerKeys.size, topHeaders };
  })();

  // Seek state
  const [showSeek, setShowSeek] = useState(false);
  const [seekPartition, setSeekPartition] = useState("0");
  const [seekMode, setSeekMode] = useState<"offset" | "timestamp">("offset");
  const [seekOffset, setSeekOffset] = useState("");
  const [seekTimestamp, setSeekTimestamp] = useState("");
  const [seekLimit, setSeekLimit] = useState("50");
  const [seeking, setSeeking] = useState(false);

  const handleSeek = async () => {
    setSeeking(true);
    setError(null);
    try {
      const params = new URLSearchParams({ partition: seekPartition, limit: seekLimit });
      if (seekMode === "offset") {
        params.set("offset", seekOffset || "0");
      } else {
        const ts = seekTimestamp ? new Date(seekTimestamp).getTime() : Date.now();
        params.set("timestamp", String(ts));
      }
      const resp = await fetch(`/api/topics/${encodeURIComponent(topic)}/messages?${params}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      setMessages(data.messages || []);
    } catch (e) {
      setError(String(e));
    }
    setSeeking(false);
    setLoading(false);
  };

  // Replay state
  const [showReplay, setShowReplay] = useState(false);
  const [replayTarget, setReplayTarget] = useState("");
  const [replaying, setReplaying] = useState(false);
  const [replayResult, setReplayResult] = useState<{ success: boolean; message: string } | null>(null);

  const handleReplay = async () => {
    if (!replayTarget) return;
    setReplaying(true);
    setReplayResult(null);
    try {
      const resp = await fetch(`/api/topics/${encodeURIComponent(topic)}/replay`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ targetTopic: replayTarget, limit: filtered.length || 50 }),
      });
      const data = await resp.json();
      if (resp.ok && data.success) {
        setReplayResult({ success: true, message: `Copied ${data.copied} messages to ${replayTarget}` });
      } else {
        setReplayResult({ success: false, message: data.detail || "Replay failed" });
      }
    } catch (e) {
      setReplayResult({ success: false, message: String(e) });
    }
    setReplaying(false);
  };

  // Produce form state
  const [showProduce, setShowProduce] = useState(false);
  const [produceKey, setProduceKey] = useState("");
  const [produceValue, setProduceValue] = useState("");
  const [producePartition, setProducePartition] = useState("");
  const [producing, setProducing] = useState(false);
  const [produceResult, setProduceResult] = useState<{ success: boolean; message: string } | null>(null);

  const handleProduce = async () => {
    setProducing(true);
    setProduceResult(null);
    try {
      const body: Record<string, unknown> = { value: produceValue };
      if (produceKey) body.key = produceKey;
      if (producePartition) body.partition = Number(producePartition);
      const resp = await fetch(`/api/topics/${encodeURIComponent(topic)}/produce`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await resp.json();
      if (resp.ok && data.success) {
        setProduceResult({ success: true, message: `Produced to P${data.partition} offset ${data.offset}` });
        setProduceValue("");
        setProduceKey("");
        setTimeout(fetchMessages, 500);
      } else {
        setProduceResult({ success: false, message: data.detail || data.error || "Failed to produce" });
      }
    } catch (e) {
      setProduceResult({ success: false, message: String(e) });
    }
    setProducing(false);
  };

  // Timestamp range analysis
  const timeRange = (() => {
    if (messages.length === 0) return null;
    const timestamps = messages.map((m) => m.timestamp).filter((t) => t > 0).sort((a, b) => a - b);
    if (timestamps.length === 0) return null;
    const earliest = timestamps[0];
    const latest = timestamps[timestamps.length - 1];
    const spanMs = latest - earliest;
    // Bucket into 12 time slots for mini-timeline
    const buckets = new Array(12).fill(0);
    if (spanMs > 0) {
      for (const ts of timestamps) {
        const idx = Math.min(11, Math.floor(((ts - earliest) / spanMs) * 12));
        buckets[idx]++;
      }
    } else {
      buckets[6] = timestamps.length;
    }
    return { earliest, latest, spanMs, buckets, count: timestamps.length };
  })();

  const renderMessageCard = (msg: Message, i: number) => (
    <div
      key={`${msg.partition}-${msg.offset}`}
      className={`rounded-xl border transition-all duration-200 cursor-pointer ${
        expandedIdx === i
          ? isBright
            ? "bg-white border-indigo-300/60 shadow-md"
            : "bg-slate-800/80 border-indigo-500/40 shadow-lg"
          : isBright
            ? "bg-white/60 border-slate-200/80 hover:bg-white hover:border-slate-300/80"
            : "bg-slate-900/50 border-slate-800/50 hover:bg-slate-800/40 hover:border-slate-700/50"
      }`}
      onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
    >
      <div className="flex items-center gap-2 px-4 py-2.5">
        <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${formatBadgeCls(msg.format)}`}>{msg.format}</span>
        {detectSchema(msg) && (
          <span className={`text-[9px] px-1.5 py-0.5 rounded border ${isBright ? "bg-violet-50 border-violet-200 text-violet-600" : "bg-violet-500/10 border-violet-500/20 text-violet-400"}`}>
            {detectSchema(msg)}
          </span>
        )}
        <span className={`text-[10px] font-mono ${isBright ? "text-slate-500" : "text-slate-500"}`}>P{msg.partition}:O{msg.offset}</span>
        {msg.key && <span className={`text-[10px] font-mono truncate max-w-[120px] ${isBright ? "text-amber-600" : "text-amber-400/80"}`} title={msg.key}>key={msg.key}</span>}
        {msg.headers && Object.keys(msg.headers).length > 0 && expandedIdx !== i && (
          <span className={`text-[9px] px-1.5 py-0.5 rounded ${isBright ? "bg-violet-50 text-violet-600" : "bg-violet-500/10 text-violet-400"}`} title={Object.keys(msg.headers).join(", ")}>
            H:{Object.keys(msg.headers).length}
          </span>
        )}
        <span className={`text-[10px] ml-auto ${isBright ? "text-slate-400" : "text-slate-500"}`}>{new Date(msg.timestamp).toLocaleTimeString()}</span>
        <button onClick={(e) => { e.stopPropagation(); copyPayload(msg, i); }} className={`transition-colors cursor-pointer ${isBright ? "text-slate-400 hover:text-slate-700" : "text-slate-500 hover:text-white"}`} title="Copy payload">
          {copied === i ? (
            <svg className="w-3.5 h-3.5 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" /></svg>
          ) : (
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" /></svg>
          )}
        </button>
      </div>
      <div className={`px-4 pb-3 ${expandedIdx === i ? "" : "max-h-12 overflow-hidden"}`}>
        <pre className={`text-xs font-mono leading-relaxed rounded-lg p-2 whitespace-pre-wrap break-all ${
          isBright
            ? `bg-slate-50 ${msg.format === "json" ? "text-emerald-700" : "text-slate-700"}`
            : `bg-slate-950/50 ${msg.format === "json" ? "text-emerald-300/90" : "text-slate-300"}`
        } ${expandedIdx !== i ? "line-clamp-2" : ""}`}>
          {expandedIdx === i && msg.format === "json"
            ? <JsonHighlight value={msg.value} bright={isBright} />
            : typeof msg.value === "object" ? JSON.stringify(msg.value, null, 2) : String(msg.value ?? "null")}
        </pre>
        {expandedIdx === i && (
          <div className={`flex items-center gap-2 mt-1 px-1`}>
            <span className={`text-[9px] px-1.5 py-0.5 rounded ${
              msg.format === "json"
                ? isBright ? "bg-emerald-50 text-emerald-600" : "bg-emerald-500/10 text-emerald-400"
                : msg.format === "avro"
                  ? isBright ? "bg-violet-50 text-violet-600" : "bg-violet-500/10 text-violet-400"
                  : isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"
            }`}>
              {msg.format || "text"}
            </span>
            <span className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              {typeof msg.value === "object" ? JSON.stringify(msg.value).length : String(msg.value ?? "").length} chars
            </span>
          </div>
        )}
      </div>
      {expandedIdx === i && msg.headers && Object.keys(msg.headers).length > 0 && (
        <div className={`px-4 pb-3 border-t pt-2 ${isBright ? "border-slate-200/60" : "border-slate-800/50"}`}>
          <div className={`text-[10px] uppercase tracking-wider mb-1 ${isBright ? "text-slate-500" : "text-slate-500"}`}>Headers</div>
          {Object.entries(msg.headers).map(([k, v]) => (
            <div key={k} className="flex gap-2 text-[10px]">
              <span className={isBright ? "text-slate-500" : "text-slate-400"}>{k}:</span>
              <span className={`font-mono ${isBright ? "text-slate-700" : "text-slate-300"}`}>{v}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );

  const headerContent = (
    <div className={`flex items-center justify-between px-5 py-4 border-b ${isBright ? "border-slate-200/60" : "border-slate-700/50"}`}>
      <div className="flex items-center gap-3">
        <div className={`w-8 h-8 rounded-xl flex items-center justify-center ${isBright ? "bg-indigo-100" : "bg-indigo-500/20"}`}>
          <span className={`text-sm font-bold ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>M</span>
        </div>
        <div>
          <div className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-slate-500" : "text-slate-400"}`}>{embedded ? "Messages" : "Message Inspector"}</div>
          <div className={`text-sm font-semibold ${isBright ? "text-slate-800" : "text-white"}`}>{topic}</div>
        </div>
      </div>
      <div className="flex items-center gap-2">
        <button
          onClick={() => setAutoRefresh(!autoRefresh)}
          className={`px-2.5 py-1.5 rounded-lg text-[10px] font-medium border transition-colors cursor-pointer ${
            autoRefresh
              ? isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300"
              : isBright ? "bg-white border-slate-200 text-slate-500 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-400 hover:bg-slate-700"
          }`}
          title={autoRefresh ? "Stop auto-refresh" : "Auto-refresh every 3s"}
        >
          {autoRefresh ? "Live" : "Auto"}
        </button>
        <button
          onClick={fetchMessages}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
            isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
          }`}
        >
          Refresh
        </button>
        <button
          onClick={() => { setShowSeek(!showSeek); setShowProduce(false); setShowReplay(false); }}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
            showSeek
              ? isBright ? "bg-amber-50 border-amber-200/60 text-amber-700" : "bg-amber-500/15 border-amber-500/30 text-amber-300"
              : isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
          }`}
        >
          Seek
        </button>
        <button
          onClick={() => { setShowReplay(!showReplay); setShowProduce(false); setShowSeek(false); }}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
            showReplay
              ? isBright ? "bg-cyan-50 border-cyan-200/60 text-cyan-700" : "bg-cyan-500/15 border-cyan-500/30 text-cyan-300"
              : isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
          }`}
        >
          Replay
        </button>
        <button
          onClick={() => { setShowProduce(!showProduce); setShowSeek(false); setShowReplay(false); }}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors cursor-pointer ${
            showProduce
              ? isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300"
              : isBright ? "bg-white border-slate-200 text-slate-600 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-300 hover:bg-slate-700"
          }`}
        >
          Produce
        </button>
        {!embedded && (
          <button
            onClick={onClose}
            className={`w-8 h-8 rounded-lg flex items-center justify-center border transition-colors cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-400 hover:text-slate-700 hover:bg-slate-50" : "bg-slate-800 border-slate-700/50 text-slate-400 hover:text-white hover:bg-slate-700"
            }`}
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        )}
      </div>
    </div>
  );

  const statsBar = !loading && !error ? (
    <div className={`px-5 py-2 border-b flex items-center gap-2 text-[10px] ${isBright ? "border-slate-200/40 text-slate-500" : "border-slate-800/50 text-slate-400"}`}>
      <span>{filtered.length}{filterText || partitionFilter !== "all" ? ` / ${messages.length}` : ""} msgs</span>
      {Object.entries(formatDist).map(([fmt, count]) => (
        <span key={fmt} className={`px-1 py-0.5 rounded text-[9px] font-medium ${formatBadgeCls(fmt)}`}>{fmt} ({count})</span>
      ))}
      {partitions.length > 1 && (
        <select
          value={partitionFilter === "all" ? "all" : String(partitionFilter)}
          onChange={(e) => setPartitionFilter(e.target.value === "all" ? "all" : Number(e.target.value))}
          className={`px-1.5 py-0.5 rounded-md text-[10px] border focus:outline-none cursor-pointer ${
            isBright ? "bg-slate-50 border-slate-200 text-slate-600" : "bg-slate-800/60 border-slate-700/40 text-slate-300"
          }`}
        >
          <option value="all">All P</option>
          {partitions.map((p) => <option key={p} value={p}>P{p}</option>)}
        </select>
      )}
      {autoRefresh && (
        <span className={`flex items-center gap-1 ${isBright ? "text-emerald-600" : "text-emerald-400"}`}>
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
          live
        </span>
      )}
      <button
        onClick={(e) => {
          e.stopPropagation();
          const data = filtered.map((m) => ({
            offset: m.offset, partition: m.partition, timestamp: m.timestamp, key: m.key, value: m.value, headers: m.headers,
          }));
          navigator.clipboard.writeText(JSON.stringify(data, null, 2));
        }}
        className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
          isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
        }`}
        title="Copy all messages as JSON"
      >
        Copy
      </button>
      <button
        onClick={(e) => {
          e.stopPropagation();
          const data = filtered.map((m) => ({
            offset: m.offset, partition: m.partition, timestamp: new Date(m.timestamp).toISOString(), key: m.key,
            value: typeof m.value === "object" ? JSON.stringify(m.value) : String(m.value ?? ""),
            headers: m.headers ? JSON.stringify(m.headers) : "",
          }));
          const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a"); a.href = url; a.download = `${topic}-messages.json`; a.click();
          URL.revokeObjectURL(url);
        }}
        className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
          isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
        }`}
        title="Download messages as JSON"
      >
        JSON
      </button>
      <button
        onClick={(e) => {
          e.stopPropagation();
          const header = "offset,partition,timestamp,key,value\n";
          const rows = filtered.map((m) => {
            const val = typeof m.value === "object" ? JSON.stringify(m.value) : String(m.value ?? "");
            return `${m.offset},${m.partition},${new Date(m.timestamp).toISOString()},"${(m.key || "").replace(/"/g, '""')}","${val.replace(/"/g, '""')}"`;
          }).join("\n");
          const blob = new Blob([header + rows], { type: "text/csv" });
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a"); a.href = url; a.download = `${topic}-messages.csv`; a.click();
          URL.revokeObjectURL(url);
        }}
        className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
          isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
        }`}
        title="Download messages as CSV"
      >
        CSV
      </button>
      {keyAnalysis && keyAnalysis.total > 0 && (
        <button
          onClick={(e) => { e.stopPropagation(); setShowKeyDist(!showKeyDist); }}
          className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
            showKeyDist
              ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-600" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
              : isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
          title="Key distribution analysis"
        >
          Keys
        </button>
      )}
      {timeRange && (
        <button
          onClick={(e) => { e.stopPropagation(); setShowTimeline(!showTimeline); }}
          className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
            showTimeline
              ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-600" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
              : isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
          title="Message timestamp timeline"
        >
          Time
        </button>
      )}
      {headerAnalysis && (
        <button
          onClick={(e) => { e.stopPropagation(); setShowHeaders(!showHeaders); }}
          className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
            showHeaders
              ? isBright ? "bg-violet-50 border-violet-200/60 text-violet-600" : "bg-violet-500/15 border-violet-500/30 text-violet-300"
              : isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
          title="Header analysis"
        >
          Hdrs
        </button>
      )}
      {sizeAnalysis && (
        <button
          onClick={(e) => { e.stopPropagation(); setShowSizes(!showSizes); }}
          className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
            showSizes
              ? isBright ? "bg-cyan-50 border-cyan-200/60 text-cyan-600" : "bg-cyan-500/15 border-cyan-500/30 text-cyan-300"
              : isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
          title="Value size histogram"
        >
          Size
        </button>
      )}
      {formatAnalysis && formatAnalysis.formats.length > 0 && (
        <button
          onClick={(e) => { e.stopPropagation(); setShowFormats(!showFormats); }}
          className={`px-1.5 py-0.5 rounded-md border transition-colors cursor-pointer ${
            showFormats
              ? isBright ? "bg-violet-50 border-violet-200/60 text-violet-600" : "bg-violet-500/15 border-violet-500/30 text-violet-300"
              : isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
          title="Format analysis"
        >
          Fmt
        </button>
      )}
      <div className="ml-auto flex items-center gap-1">
        <button
          onClick={(e) => { e.stopPropagation(); setUseRegex(!useRegex); setRegexError(null); }}
          className={`px-1.5 py-0.5 rounded-md border text-[10px] font-mono font-bold transition-colors cursor-pointer ${
            useRegex
              ? isBright ? "bg-rose-50 border-rose-200/60 text-rose-600" : "bg-rose-500/15 border-rose-500/30 text-rose-300"
              : isBright ? "border-slate-200 text-slate-400 hover:text-slate-600 hover:bg-slate-50" : "border-slate-700/40 text-slate-500 hover:text-slate-300 hover:bg-slate-800"
          }`}
          title={useRegex ? "Switch to plain text search" : "Switch to regex search"}
        >
          .*
        </button>
        <div className="relative">
          <input
            type="text"
            value={filterText}
            onChange={(e) => { setFilterText(e.target.value); setRegexError(null); }}
            placeholder={useRegex ? "Regex..." : "Filter..."}
            className={`px-2 py-1 rounded-md text-[10px] border focus:outline-none w-32 ${
              regexError
                ? isBright ? "bg-red-50 border-red-300 text-red-700" : "bg-red-950/30 border-red-500/40 text-red-300"
                : isBright ? "bg-slate-50 border-slate-200 text-slate-700 placeholder-slate-400" : "bg-slate-800/60 border-slate-700/40 text-slate-300 placeholder-slate-500"
            }`}
            onClick={(e) => e.stopPropagation()}
          />
          {regexError && <span className="absolute -top-4 right-0 text-[8px] text-red-400">{regexError}</span>}
        </div>
      </div>
    </div>
  ) : null;

  const keyDistPanel = showKeyDist && keyAnalysis ? (
    <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40 bg-slate-50/50" : "border-slate-800/50 bg-slate-900/30"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
        Key Distribution
      </div>
      <div className="grid grid-cols-3 gap-2 mb-2">
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Unique Keys</div>
          <div className={`text-sm font-bold font-mono ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>{keyAnalysis.uniqueKeys}</div>
        </div>
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Null Keys</div>
          <div className={`text-sm font-bold font-mono ${keyAnalysis.nullPct > 50 ? "text-amber-500" : isBright ? "text-slate-700" : "text-white"}`}>{keyAnalysis.nullPct.toFixed(0)}%</div>
        </div>
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Total</div>
          <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>{keyAnalysis.total}</div>
        </div>
      </div>
      {keyAnalysis.topKeys.length > 0 && (
        <div className="space-y-1">
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Top keys:</div>
          {keyAnalysis.topKeys.map(([key, count]) => {
            const pct = (count / keyAnalysis.total) * 100;
            return (
              <div key={key} className="flex items-center gap-2">
                <span className={`text-[10px] font-mono truncate max-w-[120px] shrink-0 ${isBright ? "text-amber-600" : "text-amber-400"}`}>{key}</span>
                <div className={`flex-1 h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                  <div className={`h-full rounded-full ${isBright ? "bg-indigo-400" : "bg-indigo-500/70"}`} style={{ width: `${pct}%` }} />
                </div>
                <span className={`text-[10px] font-mono tabular-nums w-12 text-right shrink-0 ${isBright ? "text-slate-500" : "text-slate-400"}`}>{count} ({pct.toFixed(0)}%)</span>
              </div>
            );
          })}
        </div>
      )}
    </div>
  ) : null;

  const timelinePanel = showTimeline && timeRange ? (
    <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40 bg-slate-50/50" : "border-slate-800/50 bg-slate-900/30"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
        Timestamp Distribution
      </div>
      <div className="flex items-center justify-between mb-2">
        <span className={`text-[10px] font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>
          {new Date(timeRange.earliest).toLocaleString()}
        </span>
        <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          span: {timeRange.spanMs < 1000 ? `${timeRange.spanMs}ms` :
                 timeRange.spanMs < 60000 ? `${(timeRange.spanMs / 1000).toFixed(1)}s` :
                 timeRange.spanMs < 3600000 ? `${(timeRange.spanMs / 60000).toFixed(1)}m` :
                 `${(timeRange.spanMs / 3600000).toFixed(1)}h`}
        </span>
        <span className={`text-[10px] font-mono ${isBright ? "text-slate-500" : "text-slate-400"}`}>
          {new Date(timeRange.latest).toLocaleString()}
        </span>
      </div>
      <div className="flex items-end gap-0.5 h-8">
        {timeRange.buckets.map((count, i) => {
          const maxBucket = Math.max(...timeRange.buckets, 1);
          const height = (count / maxBucket) * 100;
          return (
            <div
              key={i}
              className="flex-1 flex items-end"
              title={`${count} messages`}
            >
              <div
                className={`w-full rounded-t transition-all ${
                  count > 0
                    ? isBright ? "bg-indigo-400" : "bg-indigo-500/70"
                    : isBright ? "bg-slate-100" : "bg-slate-800/30"
                }`}
                style={{ height: `${Math.max(count > 0 ? 8 : 2, height)}%` }}
              />
            </div>
          );
        })}
      </div>
    </div>
  ) : null;

  const headersPanel = showHeaders && headerAnalysis ? (
    <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40 bg-violet-50/30" : "border-slate-800/50 bg-violet-950/10"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-violet-500" : "text-violet-400"}`}>
        Header Analysis
      </div>
      <div className="grid grid-cols-3 gap-2 mb-2">
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>With Headers</div>
          <div className={`text-sm font-bold font-mono ${isBright ? "text-violet-600" : "text-violet-300"}`}>
            {headerAnalysis.withHeaders}/{headerAnalysis.total}
          </div>
        </div>
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Coverage</div>
          <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>
            {((headerAnalysis.withHeaders / headerAnalysis.total) * 100).toFixed(0)}%
          </div>
        </div>
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Unique Keys</div>
          <div className={`text-sm font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>{headerAnalysis.uniqueKeys}</div>
        </div>
      </div>
      <div className="space-y-1">
        <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Header keys:</div>
        {headerAnalysis.topHeaders.map(([key, count]) => {
          const pct = (count / headerAnalysis.total) * 100;
          return (
            <div key={key} className="flex items-center gap-2">
              <span className={`text-[10px] font-mono truncate max-w-[120px] shrink-0 ${isBright ? "text-violet-600" : "text-violet-400"}`}>{key}</span>
              <div className={`flex-1 h-1.5 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                <div className={`h-full rounded-full ${isBright ? "bg-violet-400" : "bg-violet-500/70"}`} style={{ width: `${pct}%` }} />
              </div>
              <span className={`text-[10px] font-mono tabular-nums w-12 text-right shrink-0 ${isBright ? "text-slate-500" : "text-slate-400"}`}>{count}</span>
            </div>
          );
        })}
      </div>
    </div>
  ) : null;

  const fmtSize = (b: number) => b >= 1048576 ? `${(b / 1048576).toFixed(1)} MB` : b >= 1024 ? `${(b / 1024).toFixed(1)} KB` : `${b} B`;

  const sizesPanel = showSizes && sizeAnalysis ? (
    <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40 bg-cyan-50/20" : "border-slate-800/50 bg-cyan-950/10"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-cyan-600" : "text-cyan-400"}`}>
        Value Size Distribution
      </div>
      <div className="grid grid-cols-5 gap-2 mb-2">
        {[
          { label: "Min", value: fmtSize(sizeAnalysis.min) },
          { label: "Avg", value: fmtSize(sizeAnalysis.avg) },
          { label: "P50", value: fmtSize(sizeAnalysis.p50) },
          { label: "P99", value: fmtSize(sizeAnalysis.p99) },
          { label: "Max", value: fmtSize(sizeAnalysis.max) },
        ].map((s) => (
          <div key={s.label} className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
            <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{s.label}</div>
            <div className={`text-[11px] font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>{s.value}</div>
          </div>
        ))}
      </div>
      <div className="flex items-end gap-0.5 h-8">
        {sizeAnalysis.buckets.map((count, i) => {
          const maxBucket = Math.max(...sizeAnalysis.buckets, 1);
          const height = (count / maxBucket) * 100;
          return (
            <div key={i} className="flex-1 flex items-end" title={`${count} messages`}>
              <div
                className={`w-full rounded-t transition-all ${count > 0 ? (isBright ? "bg-cyan-400" : "bg-cyan-500/70") : (isBright ? "bg-slate-100" : "bg-slate-800/30")}`}
                style={{ height: `${Math.max(count > 0 ? 8 : 2, height)}%` }}
              />
            </div>
          );
        })}
      </div>
      <div className="flex justify-between mt-1">
        <span className={`text-[8px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>{fmtSize(sizeAnalysis.min)}</span>
        <span className={`text-[8px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>Total: {fmtSize(sizeAnalysis.total)}</span>
        <span className={`text-[8px] font-mono ${isBright ? "text-slate-400" : "text-slate-500"}`}>{fmtSize(sizeAnalysis.max)}</span>
      </div>
    </div>
  ) : null;

  const formatsPanel = showFormats && formatAnalysis ? (
    <div className={`px-4 py-3 border-b ${isBright ? "border-slate-200/40 bg-violet-50/20" : "border-slate-800/50 bg-violet-950/10"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium mb-2 ${isBright ? "text-violet-600" : "text-violet-400"}`}>
        Format Analysis
      </div>
      <div className="grid grid-cols-3 gap-2 mb-2">
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Dominant</div>
          <div className={`text-[11px] font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>
            {formatAnalysis.dominant[0]}
          </div>
        </div>
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Avg Length</div>
          <div className={`text-[11px] font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>
            {formatAnalysis.avgValueLen > 1024 ? `${(formatAnalysis.avgValueLen / 1024).toFixed(1)}K` : formatAnalysis.avgValueLen} chars
          </div>
        </div>
        <div className={`rounded-lg px-2 py-1.5 text-center ${isBright ? "bg-white" : "bg-slate-800/60"}`}>
          <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>Schemas</div>
          <div className={`text-[11px] font-bold font-mono ${isBright ? "text-slate-700" : "text-white"}`}>
            {formatAnalysis.schemas.length > 0 ? formatAnalysis.schemas.join(", ") : "none"}
          </div>
        </div>
      </div>
      {/* Format bar chart */}
      <div className="space-y-1">
        {formatAnalysis.formats.map(([fmt, count]) => {
          const pct = (count / formatAnalysis.total) * 100;
          return (
            <div key={fmt} className="flex items-center gap-2">
              <span className={`text-[10px] font-mono w-14 shrink-0 ${formatBadgeCls(fmt)} px-1.5 py-0.5 rounded text-center`}>{fmt}</span>
              <div className={`flex-1 h-3 rounded-full overflow-hidden ${isBright ? "bg-slate-100" : "bg-slate-800/50"}`}>
                <div
                  className={`h-full rounded-full transition-all ${
                    fmt === "json" ? (isBright ? "bg-emerald-400" : "bg-emerald-500/70")
                      : fmt === "avro" ? (isBright ? "bg-orange-400" : "bg-orange-500/70")
                      : fmt === "protobuf" || fmt === "binary" ? (isBright ? "bg-purple-400" : "bg-purple-500/70")
                      : (isBright ? "bg-blue-400" : "bg-blue-500/70")
                  }`}
                  style={{ width: `${Math.max(4, pct)}%` }}
                />
              </div>
              <span className={`text-[10px] font-mono font-bold tabular-nums w-14 text-right ${isBright ? "text-slate-600" : "text-slate-300"}`}>
                {pct.toFixed(0)}% ({count})
              </span>
            </div>
          );
        })}
      </div>
      {/* JSON structure stats */}
      {(formatAnalysis.jsonNested > 0 || formatAnalysis.jsonFlat > 0) && (
        <div className={`flex gap-3 mt-2 text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          <span>JSON: {formatAnalysis.jsonFlat} flat, {formatAnalysis.jsonNested} nested</span>
        </div>
      )}
    </div>
  ) : null;

  const seekPanel = showSeek ? (
    <div className={`px-4 py-3 border-b space-y-2 ${isBright ? "border-slate-200/40 bg-amber-50/20" : "border-slate-800/50 bg-amber-950/10"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-amber-600" : "text-amber-400"}`}>
        Seek to Position
      </div>
      <div className="grid grid-cols-3 gap-2">
        <div>
          <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Partition</label>
          <select
            value={seekPartition}
            onChange={(e) => setSeekPartition(e.target.value)}
            className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none cursor-pointer ${
              isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
            }`}
            onClick={(e) => e.stopPropagation()}
          >
            {partitions.length > 0 ? partitions.map((p) => <option key={p} value={p}>P{p}</option>) : <option value="0">P0</option>}
          </select>
        </div>
        <div>
          <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Mode</label>
          <div className="flex mt-0.5 rounded-md overflow-hidden border" style={{ borderColor: isBright ? "#e2e8f0" : "rgba(71,85,105,0.4)" }}>
            <button
              onClick={(e) => { e.stopPropagation(); setSeekMode("offset"); }}
              className={`flex-1 px-2 py-1 text-[10px] font-medium transition-colors cursor-pointer ${
                seekMode === "offset"
                  ? isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/20 text-amber-300"
                  : isBright ? "bg-white text-slate-500 hover:bg-slate-50" : "bg-slate-800/60 text-slate-400 hover:bg-slate-700"
              }`}
            >
              Offset
            </button>
            <button
              onClick={(e) => { e.stopPropagation(); setSeekMode("timestamp"); }}
              className={`flex-1 px-2 py-1 text-[10px] font-medium transition-colors cursor-pointer ${
                seekMode === "timestamp"
                  ? isBright ? "bg-amber-100 text-amber-700" : "bg-amber-500/20 text-amber-300"
                  : isBright ? "bg-white text-slate-500 hover:bg-slate-50" : "bg-slate-800/60 text-slate-400 hover:bg-slate-700"
              }`}
            >
              Time
            </button>
          </div>
        </div>
        <div>
          <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Limit</label>
          <input
            type="number"
            value={seekLimit}
            onChange={(e) => setSeekLimit(e.target.value)}
            min={1}
            max={200}
            className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none ${
              isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
            }`}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      </div>
      <div className="flex items-end gap-2">
        {seekMode === "offset" ? (
          <div className="flex-1">
            <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Offset</label>
            <input
              type="number"
              value={seekOffset}
              onChange={(e) => setSeekOffset(e.target.value)}
              placeholder="0"
              min={0}
              className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none ${
                isBright ? "bg-white border-slate-200 text-slate-800 placeholder-slate-300" : "bg-slate-800/60 border-slate-700/40 text-white placeholder-slate-600"
              }`}
              onClick={(e) => e.stopPropagation()}
            />
          </div>
        ) : (
          <div className="flex-1">
            <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Timestamp</label>
            <input
              type="datetime-local"
              value={seekTimestamp}
              onChange={(e) => setSeekTimestamp(e.target.value)}
              className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none ${
                isBright ? "bg-white border-slate-200 text-slate-800" : "bg-slate-800/60 border-slate-700/40 text-white"
              }`}
              onClick={(e) => e.stopPropagation()}
            />
          </div>
        )}
        <button
          onClick={handleSeek}
          disabled={seeking}
          className={`px-3 py-1 rounded-md text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
            isBright ? "bg-amber-50 border-amber-200/60 text-amber-700 hover:bg-amber-100" : "bg-amber-500/15 border-amber-500/30 text-amber-300 hover:bg-amber-500/25"
          }`}
        >
          {seeking ? "Seeking..." : "Fetch"}
        </button>
      </div>
    </div>
  ) : null;

  const replayPanel = showReplay ? (
    <div className={`px-4 py-3 border-b space-y-2 ${isBright ? "border-slate-200/40 bg-cyan-50/20" : "border-slate-800/50 bg-cyan-950/10"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-cyan-600" : "text-cyan-400"}`}>
        Replay Messages to Another Topic
      </div>
      {replayResult && (
        <div className={`text-[10px] px-2 py-1 rounded ${
          replayResult.success
            ? isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/15 text-emerald-300"
            : isBright ? "bg-red-100 text-red-700" : "bg-red-500/15 text-red-300"
        }`}>{replayResult.message}</div>
      )}
      <div className="flex items-end gap-2">
        <div className="flex-1">
          <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Target Topic</label>
          <input
            type="text"
            value={replayTarget}
            onChange={(e) => setReplayTarget(e.target.value)}
            placeholder="target-topic-name"
            className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none ${
              isBright ? "bg-white border-slate-200 text-slate-800 placeholder-slate-300" : "bg-slate-800/60 border-slate-700/40 text-white placeholder-slate-600"
            }`}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
        <button
          onClick={handleReplay}
          disabled={replaying || !replayTarget}
          className={`px-3 py-1 rounded-md text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
            isBright ? "bg-cyan-50 border-cyan-200/60 text-cyan-700 hover:bg-cyan-100" : "bg-cyan-500/15 border-cyan-500/30 text-cyan-300 hover:bg-cyan-500/25"
          }`}
        >
          {replaying ? "Copying..." : `Copy ${filtered.length} msgs`}
        </button>
      </div>
      <div className={`text-[9px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>
        Copies currently visible messages ({filtered.length}) to the target topic. Keys and values are preserved.
      </div>
    </div>
  ) : null;

  const producePanel = showProduce ? (
    <div className={`px-4 py-3 border-b space-y-2 ${isBright ? "border-slate-200/40 bg-emerald-50/20" : "border-slate-800/50 bg-emerald-950/10"}`}>
      <div className={`text-[10px] uppercase tracking-wider font-medium ${isBright ? "text-emerald-600" : "text-emerald-400"}`}>
        Produce Message
      </div>
      {produceResult && (
        <div className={`text-[10px] px-2 py-1 rounded ${
          produceResult.success
            ? isBright ? "bg-emerald-100 text-emerald-700" : "bg-emerald-500/15 text-emerald-300"
            : isBright ? "bg-red-100 text-red-700" : "bg-red-500/15 text-red-300"
        }`}>{produceResult.message}</div>
      )}
      <div className="grid grid-cols-2 gap-2">
        <div>
          <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Key (optional)</label>
          <input
            type="text"
            value={produceKey}
            onChange={(e) => setProduceKey(e.target.value)}
            placeholder="message-key"
            className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none ${
              isBright ? "bg-white border-slate-200 text-slate-800 placeholder-slate-300" : "bg-slate-800/60 border-slate-700/40 text-white placeholder-slate-600"
            }`}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
        <div>
          <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Partition (optional)</label>
          <input
            type="number"
            value={producePartition}
            onChange={(e) => setProducePartition(e.target.value)}
            placeholder="auto"
            min={0}
            className={`w-full mt-0.5 rounded-md px-2 py-1 text-[11px] font-mono border focus:outline-none ${
              isBright ? "bg-white border-slate-200 text-slate-800 placeholder-slate-300" : "bg-slate-800/60 border-slate-700/40 text-white placeholder-slate-600"
            }`}
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      </div>
      <div>
        <label className={`text-[9px] uppercase ${isBright ? "text-slate-400" : "text-slate-500"}`}>Value</label>
        <textarea
          value={produceValue}
          onChange={(e) => setProduceValue(e.target.value)}
          placeholder='{"key": "value"}'
          rows={3}
          className={`w-full mt-0.5 rounded-md px-2 py-1.5 text-[11px] font-mono border focus:outline-none resize-y ${
            isBright ? "bg-white border-slate-200 text-slate-800 placeholder-slate-300" : "bg-slate-800/60 border-slate-700/40 text-white placeholder-slate-600"
          }`}
          onClick={(e) => e.stopPropagation()}
        />
      </div>
      <div className="flex justify-end">
        <button
          onClick={handleProduce}
          disabled={producing || !produceValue}
          className={`px-3 py-1 rounded-md text-[11px] font-medium border transition-colors cursor-pointer disabled:opacity-40 ${
            isBright ? "bg-emerald-50 border-emerald-200/60 text-emerald-700 hover:bg-emerald-100" : "bg-emerald-500/15 border-emerald-500/30 text-emerald-300 hover:bg-emerald-500/25"
          }`}
        >
          {producing ? "Sending..." : "Send"}
        </button>
      </div>
    </div>
  ) : null;

  const messagesList = (
    <>
      {loading && (
        <div className="flex items-center justify-center h-32">
          <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
        </div>
      )}
      {error && (
        <div className={`m-5 p-4 rounded-xl border text-sm ${
          isBright ? "bg-red-50 border-red-200 text-red-700" : "bg-red-950/50 border-red-500/30 text-red-300"
        }`}>{error}</div>
      )}
      {!loading && !error && filtered.length === 0 && (
        <div className={`flex flex-col items-center justify-center h-32 text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
          <span>{filterText ? "No matching messages" : "No messages in topic"}</span>
        </div>
      )}
      <div className="p-3 space-y-2">
        {filtered.map((msg, i) => renderMessageCard(msg, i))}
      </div>
    </>
  );

  if (embedded) {
    return (
      <div className="flex flex-col">
        {headerContent}
        {seekPanel}
        {replayPanel}
        {producePanel}
        {statsBar}
        {keyDistPanel}
        {timelinePanel}
        {headersPanel}
        {sizesPanel}
        {formatsPanel}
        <div className="max-h-[500px] overflow-y-auto">{messagesList}</div>
      </div>
    );
  }

  return (
    <div className="fixed right-0 top-0 h-full w-[520px] z-50 flex flex-col shadow-2xl shadow-black/50">
      <div className={`absolute inset-0 backdrop-blur-2xl border-l ${
        isBright ? "bg-white/95 border-slate-200/80" : "bg-slate-950/95 border-slate-700/50"
      }`} />
      <div className="relative flex flex-col h-full">
        {headerContent}
        {seekPanel}
        {replayPanel}
        {producePanel}
        {statsBar}
        {keyDistPanel}
        {timelinePanel}
        {headersPanel}
        {sizesPanel}
        {formatsPanel}
        <div className="flex-1 overflow-y-auto">{messagesList}</div>
      </div>
    </div>
  );
}
