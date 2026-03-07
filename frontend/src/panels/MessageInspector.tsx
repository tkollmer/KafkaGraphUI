import { useState, useEffect, useCallback } from "react";

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
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);
  const [copied, setCopied] = useState<number | null>(null);

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

  const copyPayload = (msg: Message, idx: number) => {
    const text = typeof msg.value === "object" ? JSON.stringify(msg.value, null, 2) : String(msg.value);
    navigator.clipboard.writeText(text);
    setCopied(idx);
    setTimeout(() => setCopied(null), 1500);
  };

  if (embedded) {
    return (
      <div className="flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700/50">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-xl bg-indigo-500/20 flex items-center justify-center">
              <span className="text-indigo-300 text-sm font-bold">M</span>
            </div>
            <div>
              <div className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Messages</div>
              <div className="text-sm font-semibold text-white">{topic}</div>
            </div>
          </div>
          <button
            onClick={fetchMessages}
            className="px-3 py-1.5 rounded-lg text-xs font-medium bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 transition-colors cursor-pointer"
          >
            Refresh
          </button>
        </div>

        {!loading && !error && (
          <div className="px-5 py-2 border-b border-slate-800/50 flex items-center gap-3 text-[10px] text-slate-400">
            <span>{messages.length} messages</span>
            <span className="text-slate-700">|</span>
            <span>Latest first</span>
          </div>
        )}

        <div className="max-h-[500px] overflow-y-auto">
          {loading && (
            <div className="flex items-center justify-center h-32">
              <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
            </div>
          )}
          {error && (
            <div className="m-5 p-4 rounded-xl bg-red-950/50 border border-red-500/30 text-red-300 text-sm">{error}</div>
          )}
          {!loading && !error && messages.length === 0 && (
            <div className="flex flex-col items-center justify-center h-32 text-slate-500 text-sm">
              <span>No messages in topic</span>
            </div>
          )}
          <div className="p-3 space-y-2">
            {messages.map((msg, i) => (
              <div
                key={`${msg.partition}-${msg.offset}`}
                className={`rounded-xl border transition-all duration-200 cursor-pointer ${
                  expandedIdx === i
                    ? "bg-slate-800/80 border-indigo-500/40 shadow-lg"
                    : "bg-slate-900/50 border-slate-800/50 hover:bg-slate-800/40 hover:border-slate-700/50"
                }`}
                onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
              >
                <div className="flex items-center gap-2 px-4 py-2.5">
                  <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${
                    msg.format === "json" ? "bg-emerald-500/20 text-emerald-400" :
                    msg.format === "utf8" ? "bg-blue-500/20 text-blue-400" :
                    "bg-slate-500/20 text-slate-400"
                  }`}>{msg.format}</span>
                  <span className="text-[10px] font-mono text-slate-500">P{msg.partition}:O{msg.offset}</span>
                  {msg.key && <span className="text-[10px] font-mono text-amber-400/80 truncate max-w-[120px]" title={msg.key}>key={msg.key}</span>}
                  <span className="text-[10px] text-slate-500 ml-auto">{new Date(msg.timestamp).toLocaleTimeString()}</span>
                  <button onClick={(e) => { e.stopPropagation(); copyPayload(msg, i); }} className="text-slate-500 hover:text-white transition-colors cursor-pointer" title="Copy payload">
                    {copied === i ? (
                      <svg className="w-3.5 h-3.5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" /></svg>
                    ) : (
                      <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}><rect x="9" y="9" width="13" height="13" rx="2" /><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" /></svg>
                    )}
                  </button>
                </div>
                <div className={`px-4 pb-3 ${expandedIdx === i ? "" : "max-h-12 overflow-hidden"}`}>
                  <pre className={`text-xs font-mono leading-relaxed rounded-lg p-2 bg-slate-950/50 whitespace-pre-wrap break-all ${
                    msg.format === "json" ? "text-emerald-300/90" : "text-slate-300"
                  } ${expandedIdx !== i ? "line-clamp-2" : ""}`}>
                    {typeof msg.value === "object" ? JSON.stringify(msg.value, null, 2) : String(msg.value ?? "null")}
                  </pre>
                </div>
                {expandedIdx === i && msg.headers && Object.keys(msg.headers).length > 0 && (
                  <div className="px-4 pb-3 border-t border-slate-800/50 pt-2">
                    <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Headers</div>
                    {Object.entries(msg.headers).map(([k, v]) => (
                      <div key={k} className="flex gap-2 text-[10px]"><span className="text-slate-400">{k}:</span><span className="text-slate-300 font-mono">{v}</span></div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed right-0 top-0 h-full w-[520px] z-50 flex flex-col shadow-2xl shadow-black/50">
      {/* Backdrop blur overlay */}
      <div className="absolute inset-0 bg-slate-950/95 backdrop-blur-2xl border-l border-slate-700/50" />

      {/* Content */}
      <div className="relative flex flex-col h-full">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700/50">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-xl bg-indigo-500/20 flex items-center justify-center">
              <span className="text-indigo-300 text-sm font-bold">M</span>
            </div>
            <div>
              <div className="text-[10px] uppercase tracking-wider text-slate-400 font-medium">Message Inspector</div>
              <div className="text-sm font-semibold text-white">{topic}</div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={fetchMessages}
              className="px-3 py-1.5 rounded-lg text-xs font-medium bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 transition-colors cursor-pointer"
            >
              Refresh
            </button>
            <button
              onClick={onClose}
              className="w-8 h-8 rounded-lg flex items-center justify-center bg-slate-800 border border-slate-700/50 text-slate-400 hover:text-white hover:bg-slate-700 transition-colors cursor-pointer"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
              </svg>
            </button>
          </div>
        </div>

        {/* Stats bar */}
        {!loading && !error && (
          <div className="px-5 py-2 border-b border-slate-800/50 flex items-center gap-3 text-[10px] text-slate-400">
            <span>{messages.length} messages</span>
            <span className="text-slate-700">|</span>
            <span>Latest first</span>
          </div>
        )}

        {/* Messages list */}
        <div className="flex-1 overflow-y-auto">
          {loading && (
            <div className="flex items-center justify-center h-32">
              <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
            </div>
          )}

          {error && (
            <div className="m-5 p-4 rounded-xl bg-red-950/50 border border-red-500/30 text-red-300 text-sm">
              {error}
            </div>
          )}

          {!loading && !error && messages.length === 0 && (
            <div className="flex flex-col items-center justify-center h-32 text-slate-500 text-sm">
              <span>No messages in topic</span>
            </div>
          )}

          <div className="p-3 space-y-2">
            {messages.map((msg, i) => (
              <div
                key={`${msg.partition}-${msg.offset}`}
                className={`rounded-xl border transition-all duration-200 cursor-pointer ${
                  expandedIdx === i
                    ? "bg-slate-800/80 border-indigo-500/40 shadow-lg"
                    : "bg-slate-900/50 border-slate-800/50 hover:bg-slate-800/40 hover:border-slate-700/50"
                }`}
                onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
              >
                {/* Message header */}
                <div className="flex items-center gap-2 px-4 py-2.5">
                  {/* Format badge */}
                  <span className={`text-[9px] font-bold uppercase px-1.5 py-0.5 rounded ${
                    msg.format === "json" ? "bg-emerald-500/20 text-emerald-400" :
                    msg.format === "utf8" ? "bg-blue-500/20 text-blue-400" :
                    "bg-slate-500/20 text-slate-400"
                  }`}>{msg.format}</span>

                  {/* Partition + Offset */}
                  <span className="text-[10px] font-mono text-slate-500">
                    P{msg.partition}:O{msg.offset}
                  </span>

                  {/* Key */}
                  {msg.key && (
                    <span className="text-[10px] font-mono text-amber-400/80 truncate max-w-[120px]" title={msg.key}>
                      key={msg.key}
                    </span>
                  )}

                  {/* Timestamp */}
                  <span className="text-[10px] text-slate-500 ml-auto">
                    {new Date(msg.timestamp).toLocaleTimeString()}
                  </span>

                  {/* Copy button */}
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      copyPayload(msg, i);
                    }}
                    className="text-slate-500 hover:text-white transition-colors cursor-pointer"
                    title="Copy payload"
                  >
                    {copied === i ? (
                      <svg className="w-3.5 h-3.5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                        <path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                    ) : (
                      <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                        <rect x="9" y="9" width="13" height="13" rx="2" />
                        <path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1" />
                      </svg>
                    )}
                  </button>
                </div>

                {/* Preview (always visible) */}
                <div className={`px-4 pb-3 ${expandedIdx === i ? "" : "max-h-12 overflow-hidden"}`}>
                  <pre className={`text-xs font-mono leading-relaxed rounded-lg p-2 bg-slate-950/50 whitespace-pre-wrap break-all ${
                    msg.format === "json" ? "text-emerald-300/90" : "text-slate-300"
                  } ${expandedIdx !== i ? "line-clamp-2" : ""}`}>
                    {typeof msg.value === "object"
                      ? JSON.stringify(msg.value, null, 2)
                      : String(msg.value ?? "null")}
                  </pre>
                </div>

                {/* Headers (expanded only) */}
                {expandedIdx === i && msg.headers && Object.keys(msg.headers).length > 0 && (
                  <div className="px-4 pb-3 border-t border-slate-800/50 pt-2">
                    <div className="text-[10px] text-slate-500 uppercase tracking-wider mb-1">Headers</div>
                    {Object.entries(msg.headers).map(([k, v]) => (
                      <div key={k} className="flex gap-2 text-[10px]">
                        <span className="text-slate-400">{k}:</span>
                        <span className="text-slate-300 font-mono">{v}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
