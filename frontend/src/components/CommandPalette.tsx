import { useState, useEffect, useRef, useMemo, useCallback } from "react";
import { useNavigationStore, type ActiveView } from "../store/navigationStore";
import { useKafkaStore } from "../store/kafkaStore";
import { useThemeStore } from "../store/themeStore";
import { useGraphStore } from "../store/graphStore";

interface CommandItem {
  id: string;
  label: string;
  sublabel?: string;
  category: string;
  icon: string;
  action: () => void;
}

export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const { setActiveView, navigateToTopic, navigateToConsumerGroup } = useNavigationStore();
  const topics = useKafkaStore((s) => s.topics);
  const consumerGroups = useKafkaStore((s) => s.consumerGroups);
  const brokers = useKafkaStore((s) => s.brokers);
  const { toggleTheme } = useThemeStore();
  const nodes = useGraphStore((s) => s.nodes);

  // Open/close with Cmd+K
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen((prev) => !prev);
        setQuery("");
        setSelectedIndex(0);
      }
      if (e.key === "Escape" && open) {
        setOpen(false);
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open]);

  // Focus input when opened
  useEffect(() => {
    if (open) setTimeout(() => inputRef.current?.focus(), 50);
  }, [open]);

  const items = useMemo<CommandItem[]>(() => {
    const cmds: CommandItem[] = [];

    // Navigation commands
    const navItems: { id: ActiveView; label: string; icon: string }[] = [
      { id: "dashboard", label: "Dashboard", icon: "D" },
      { id: "pipeline", label: "Pipeline View", icon: "P" },
      { id: "applications", label: "Applications View", icon: "A" },
      { id: "topics", label: "Topics View", icon: "T" },
      { id: "consumers", label: "Consumer Groups View", icon: "C" },
      { id: "brokers", label: "Brokers View", icon: "B" },
      { id: "schemas", label: "Schema Registry", icon: "R" },
      { id: "connectors", label: "Kafka Connect", icon: "K" },
      { id: "acls", label: "Access Control Lists", icon: "L" },
      { id: "quotas", label: "Quotas", icon: "Q" },
      { id: "settings", label: "Settings", icon: "S" },
    ];
    navItems.forEach((nav) => {
      cmds.push({
        id: `nav-${nav.id}`,
        label: nav.label,
        category: "Navigation",
        icon: nav.icon,
        action: () => { setActiveView(nav.id); setOpen(false); },
      });
    });

    // Theme toggle
    cmds.push({
      id: "toggle-theme",
      label: isBright ? "Switch to Dark Mode" : "Switch to Bright Mode",
      category: "Actions",
      icon: isBright ? "D" : "L",
      action: () => { toggleTheme(); setOpen(false); },
    });

    // Topic items — deep-link to topic detail
    topics.forEach((t) => {
      cmds.push({
        id: `topic-${t.name}`,
        label: t.name,
        sublabel: `${t.partitions} partitions, ${t.totalMessages.toLocaleString()} messages`,
        category: "Topics",
        icon: "T",
        action: () => { navigateToTopic(t.name); setOpen(false); },
      });
    });

    // Consumer group items — deep-link to group detail
    consumerGroups.forEach((g) => {
      cmds.push({
        id: `cg-${g.groupId}`,
        label: g.groupId,
        sublabel: `${g.status} - lag: ${g.totalLag.toLocaleString()}`,
        category: "Consumer Groups",
        icon: "C",
        action: () => { navigateToConsumerGroup(g.groupId); setOpen(false); },
      });
    });

    // Broker items
    brokers.forEach((b) => {
      cmds.push({
        id: `broker-${b.id}`,
        label: `Broker ${b.id}`,
        sublabel: `${b.host}:${b.port}${b.isController ? " (Controller)" : ""}`,
        category: "Brokers",
        icon: "B",
        action: () => { setActiveView("brokers"); setOpen(false); },
      });
    });

    // Graph node items
    const serviceNodes = nodes.filter((n) => n.type === "serviceNode");
    serviceNodes.forEach((n) => {
      cmds.push({
        id: `node-${n.id}`,
        label: String(n.data?.label || n.id),
        sublabel: "Service node in pipeline",
        category: "Pipeline Nodes",
        icon: "N",
        action: () => { setActiveView("pipeline"); setOpen(false); },
      });
    });

    return cmds;
  }, [topics, consumerGroups, brokers, nodes, isBright, setActiveView, toggleTheme, navigateToTopic, navigateToConsumerGroup]);

  const filtered = useMemo(() => {
    if (!query) return items.slice(0, 20);
    const q = query.toLowerCase();
    return items
      .filter((item) =>
        item.label.toLowerCase().includes(q) ||
        item.sublabel?.toLowerCase().includes(q) ||
        item.category.toLowerCase().includes(q)
      )
      .slice(0, 20);
  }, [items, query]);

  // Reset selection when filtered list changes
  useEffect(() => { setSelectedIndex(0); }, [filtered]);

  // Scroll selected item into view
  useEffect(() => {
    if (listRef.current) {
      const el = listRef.current.children[selectedIndex] as HTMLElement;
      el?.scrollIntoView({ block: "nearest" });
    }
  }, [selectedIndex]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex((i) => Math.min(i + 1, filtered.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && filtered[selectedIndex]) {
      e.preventDefault();
      filtered[selectedIndex].action();
    }
  }, [filtered, selectedIndex]);

  if (!open) return null;

  // Group items by category
  const grouped = new Map<string, CommandItem[]>();
  filtered.forEach((item) => {
    const list = grouped.get(item.category) || [];
    list.push(item);
    grouped.set(item.category, list);
  });

  let flatIndex = -1;

  const iconColors: Record<string, string> = {
    P: isBright ? "bg-purple-100 text-purple-600" : "bg-purple-500/20 text-purple-400",
    A: isBright ? "bg-violet-100 text-violet-600" : "bg-violet-500/20 text-violet-400",
    T: isBright ? "bg-indigo-100 text-indigo-600" : "bg-indigo-500/20 text-indigo-400",
    C: isBright ? "bg-amber-100 text-amber-600" : "bg-amber-500/20 text-amber-400",
    B: isBright ? "bg-cyan-100 text-cyan-600" : "bg-cyan-500/20 text-cyan-400",
    S: isBright ? "bg-slate-100 text-slate-600" : "bg-slate-700 text-slate-400",
    N: isBright ? "bg-emerald-100 text-emerald-600" : "bg-emerald-500/20 text-emerald-400",
    D: isBright ? "bg-slate-100 text-slate-600" : "bg-slate-700 text-slate-400",
    L: isBright ? "bg-amber-100 text-amber-600" : "bg-amber-500/20 text-amber-400",
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm"
        onClick={() => setOpen(false)}
      />
      {/* Palette */}
      <div className={`fixed top-[15%] left-1/2 -translate-x-1/2 z-50 w-full max-w-lg rounded-2xl border shadow-2xl overflow-hidden ${
        isBright
          ? "bg-white border-slate-200"
          : "bg-slate-900 border-slate-700/60"
      }`}>
        {/* Search input */}
        <div className={`flex items-center gap-3 px-4 py-3 border-b ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
          <svg className={`w-5 h-5 shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <circle cx="11" cy="11" r="8" />
            <path d="m21 21-4.35-4.35" strokeLinecap="round" />
          </svg>
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Search topics, groups, brokers, actions..."
            className={`flex-1 bg-transparent text-sm focus:outline-none ${
              isBright ? "text-slate-800 placeholder-slate-400" : "text-white placeholder-slate-500"
            }`}
          />
          <kbd className={`text-[10px] px-1.5 py-0.5 rounded border font-mono ${
            isBright ? "bg-slate-100 border-slate-200 text-slate-400" : "bg-slate-800 border-slate-700 text-slate-500"
          }`}>ESC</kbd>
        </div>

        {/* Results */}
        <div ref={listRef} className="max-h-[400px] overflow-y-auto py-2">
          {filtered.length === 0 ? (
            <div className={`px-4 py-8 text-center text-sm ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              No results found
            </div>
          ) : (
            Array.from(grouped.entries()).map(([category, items]) => (
              <div key={category}>
                <div className={`px-4 py-1.5 text-[10px] uppercase tracking-wider font-semibold ${
                  isBright ? "text-slate-400" : "text-slate-500"
                }`}>
                  {category}
                </div>
                {items.map((item) => {
                  flatIndex++;
                  const idx = flatIndex;
                  return (
                    <button
                      key={item.id}
                      onClick={item.action}
                      onMouseEnter={() => setSelectedIndex(idx)}
                      className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors cursor-pointer ${
                        idx === selectedIndex
                          ? isBright ? "bg-indigo-50" : "bg-indigo-500/10"
                          : ""
                      }`}
                    >
                      <div className={`w-7 h-7 rounded-lg flex items-center justify-center text-[10px] font-bold shrink-0 ${
                        iconColors[item.icon] || (isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400")
                      }`}>
                        {item.icon}
                      </div>
                      <div className="min-w-0 flex-1">
                        <div className={`text-sm font-medium truncate ${isBright ? "text-slate-800" : "text-white"}`}>
                          {item.label}
                        </div>
                        {item.sublabel && (
                          <div className={`text-[11px] truncate ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                            {item.sublabel}
                          </div>
                        )}
                      </div>
                    </button>
                  );
                })}
              </div>
            ))
          )}
        </div>

        {/* Footer */}
        <div className={`flex items-center gap-4 px-4 py-2.5 border-t text-[10px] ${
          isBright ? "border-slate-200/60 text-slate-400" : "border-slate-700/40 text-slate-500"
        }`}>
          <span className="flex items-center gap-1">
            <kbd className={`px-1 py-0.5 rounded border font-mono ${isBright ? "bg-slate-100 border-slate-200" : "bg-slate-800 border-slate-700"}`}>&uarr;&darr;</kbd>
            Navigate
          </span>
          <span className="flex items-center gap-1">
            <kbd className={`px-1 py-0.5 rounded border font-mono ${isBright ? "bg-slate-100 border-slate-200" : "bg-slate-800 border-slate-700"}`}>&crarr;</kbd>
            Select
          </span>
          <span className="flex items-center gap-1">
            <kbd className={`px-1 py-0.5 rounded border font-mono ${isBright ? "bg-slate-100 border-slate-200" : "bg-slate-800 border-slate-700"}`}>Esc</kbd>
            Close
          </span>
        </div>
      </div>
    </>
  );
}
