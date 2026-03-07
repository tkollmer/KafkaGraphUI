import { useState, useMemo } from "react";

interface Column<T> {
  key: string;
  label: string;
  render?: (row: T) => React.ReactNode;
  sortable?: boolean;
  className?: string;
}

interface Props<T> {
  columns: Column<T>[];
  data: T[];
  onRowClick?: (row: T) => void;
  searchPlaceholder?: string;
  searchKeys?: string[];
  emptyMessage?: string;
}

export function DataTable<T extends Record<string, unknown>>({
  columns,
  data,
  onRowClick,
  searchPlaceholder = "Search...",
  searchKeys,
  emptyMessage = "No data",
}: Props<T>) {
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const filtered = useMemo(() => {
    if (!search) return data;
    const q = search.toLowerCase();
    const keys = searchKeys || columns.map((c) => c.key);
    return data.filter((row) =>
      keys.some((k) => String(row[k] ?? "").toLowerCase().includes(q))
    );
  }, [data, search, searchKeys, columns]);

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    return [...filtered].sort((a, b) => {
      const av = a[sortKey], bv = b[sortKey];
      if (av == null && bv == null) return 0;
      if (av == null) return 1;
      if (bv == null) return -1;
      const cmp = typeof av === "number" && typeof bv === "number"
        ? av - bv
        : String(av).localeCompare(String(bv));
      return sortDir === "asc" ? cmp : -cmp;
    });
  }, [filtered, sortKey, sortDir]);

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
  };

  return (
    <div className="flex flex-col gap-3">
      {/* Search */}
      <div className="relative max-w-xs">
        <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.35-4.35" strokeLinecap="round" />
        </svg>
        <input
          type="text"
          placeholder={searchPlaceholder}
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full bg-slate-800/80 rounded-xl pl-9 pr-3 py-2 border border-slate-700/50 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500/50 focus:ring-1 focus:ring-indigo-500/30 transition-all"
        />
      </div>

      {/* Table */}
      <div className="rounded-2xl border border-slate-700/50 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className="bg-slate-800/60">
                {columns.map((col) => (
                  <th
                    key={col.key}
                    className={`px-4 py-3 text-left text-[10px] uppercase tracking-wider font-medium text-slate-400 ${
                      col.sortable !== false ? "cursor-pointer hover:text-slate-200 select-none" : ""
                    } ${col.className || ""}`}
                    onClick={() => col.sortable !== false && handleSort(col.key)}
                  >
                    <span className="flex items-center gap-1">
                      {col.label}
                      {sortKey === col.key && (
                        <span className="text-indigo-400">{sortDir === "asc" ? "\u2191" : "\u2193"}</span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {sorted.length === 0 ? (
                <tr>
                  <td colSpan={columns.length} className="px-4 py-8 text-center text-sm text-slate-500">
                    {emptyMessage}
                  </td>
                </tr>
              ) : (
                sorted.map((row, i) => (
                  <tr
                    key={i}
                    className={`transition-colors ${
                      onRowClick
                        ? "cursor-pointer hover:bg-slate-800/40"
                        : ""
                    }`}
                    onClick={() => onRowClick?.(row)}
                  >
                    {columns.map((col) => (
                      <td key={col.key} className={`px-4 py-3 text-sm text-slate-300 ${col.className || ""}`}>
                        {col.render ? col.render(row) : String(row[col.key] ?? "")}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="text-[10px] text-slate-500">
        {sorted.length} of {data.length} {data.length === 1 ? "row" : "rows"}
      </div>
    </div>
  );
}
