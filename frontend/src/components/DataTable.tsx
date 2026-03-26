import { useState, useMemo, useCallback, useRef, useEffect } from "react";
import { useThemeStore } from "../store/themeStore";

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
  pageSize?: number;
  exportFilename?: string;
  /** Enable row selection with checkboxes. Provide a unique key from the data to identify rows. */
  selectionKey?: string;
  onSelectionChange?: (selectedRows: T[]) => void;
}

export function DataTable<T extends Record<string, unknown>>({
  columns,
  data,
  onRowClick,
  searchPlaceholder = "Search...",
  searchKeys,
  emptyMessage = "No data",
  pageSize = 50,
  exportFilename,
  selectionKey,
  onSelectionChange,
}: Props<T>) {
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [page, setPage] = useState(0);
  const [focusedRow, setFocusedRow] = useState(-1);
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const [showColMenu, setShowColMenu] = useState(false);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const selectable = !!selectionKey;
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const debounceRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const tableRef = useRef<HTMLDivElement>(null);
  const colMenuRef = useRef<HTMLDivElement>(null);

  const visibleColumns = useMemo(() =>
    columns.filter((c) => !hiddenCols.has(c.key)),
  [columns, hiddenCols]);

  const toggleableColumns = useMemo(() =>
    columns.filter((c) => c.label && c.key !== "_actions"),
  [columns]);

  // Close column menu on outside click
  useEffect(() => {
    if (!showColMenu) return;
    const handler = (e: MouseEvent) => {
      if (colMenuRef.current && !colMenuRef.current.contains(e.target as Node)) {
        setShowColMenu(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [showColMenu]);

  // Debounce search for performance with large datasets
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => setDebouncedSearch(search), 150);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [search]);

  // Reset page and focused row when search changes
  useEffect(() => { setPage(0); setFocusedRow(-1); }, [debouncedSearch]);

  const filtered = useMemo(() => {
    if (!debouncedSearch) return data;
    const q = debouncedSearch.toLowerCase();
    const keys = searchKeys || columns.map((c) => c.key);
    return data.filter((row) =>
      keys.some((k) => String(row[k] ?? "").toLowerCase().includes(q))
    );
  }, [data, debouncedSearch, searchKeys, columns]);

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

  // Pagination
  const totalPages = Math.max(1, Math.ceil(sorted.length / pageSize));
  const paginatedRows = useMemo(() => {
    const start = page * pageSize;
    return sorted.slice(start, start + pageSize);
  }, [sorted, page, pageSize]);

  // Keyboard navigation for table rows
  useEffect(() => {
    const el = tableRef.current;
    if (!el) return;
    const handler = (e: KeyboardEvent) => {
      if (paginatedRows.length === 0) return;
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setFocusedRow((r) => Math.min(r + 1, paginatedRows.length - 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setFocusedRow((r) => Math.max(r - 1, 0));
      } else if (e.key === "Enter" && focusedRow >= 0 && onRowClick) {
        e.preventDefault();
        onRowClick(paginatedRows[focusedRow]);
      }
    };
    el.addEventListener("keydown", handler);
    return () => el.removeEventListener("keydown", handler);
  }, [paginatedRows, focusedRow, onRowClick]);

  // Selection helpers
  const toggleRow = useCallback((row: T) => {
    if (!selectionKey) return;
    const id = String(row[selectionKey]);
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  }, [selectionKey]);

  const toggleAll = useCallback(() => {
    if (!selectionKey) return;
    const allIds = paginatedRows.map((r) => String(r[selectionKey]));
    const allSelected = allIds.every((id) => selectedIds.has(id));
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (allSelected) { allIds.forEach((id) => next.delete(id)); }
      else { allIds.forEach((id) => next.add(id)); }
      return next;
    });
  }, [selectionKey, paginatedRows, selectedIds]);

  // Notify parent of selection changes
  useEffect(() => {
    if (!onSelectionChange || !selectionKey) return;
    const selected = data.filter((r) => selectedIds.has(String(r[selectionKey])));
    onSelectionChange(selected);
  }, [selectedIds, data, selectionKey, onSelectionChange]);

  const handleSort = useCallback((key: string) => {
    setSortKey((prev) => {
      if (prev === key) {
        setSortDir((d) => (d === "asc" ? "desc" : "asc"));
        return key;
      }
      setSortDir("asc");
      return key;
    });
  }, []);

  return (
    <div className="flex flex-col gap-4">
      {/* Search bar */}
      <div className="flex items-center gap-3">
        <div className="relative flex-1 max-w-sm">
          <svg className={`absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 ${isBright ? "text-slate-400" : "text-slate-500"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <circle cx="11" cy="11" r="8" />
            <path d="m21 21-4.35-4.35" strokeLinecap="round" />
          </svg>
          <input
            type="text"
            placeholder={searchPlaceholder}
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className={`w-full rounded-xl pl-10 pr-3 py-2.5 border text-[13px] focus:outline-none focus:border-indigo-500/50 focus:ring-1 focus:ring-indigo-500/20 transition-all ${
              isBright
                ? "bg-white/80 border-slate-200/80 text-slate-800 placeholder-slate-400"
                : "bg-slate-900/60 border-slate-700/40 text-white placeholder-slate-500"
            }`}
          />
        </div>
        <span className={`text-xs tabular-nums ${isBright ? "text-slate-500" : "text-slate-500"}`}>
          {sorted.length}{filtered.length < data.length ? ` of ${data.length}` : ""} {data.length === 1 ? "row" : "rows"}
          {selectable && selectedIds.size > 0 && (
            <span className={`ml-2 font-medium ${isBright ? "text-indigo-600" : "text-indigo-400"}`}>
              ({selectedIds.size} selected)
            </span>
          )}
        </span>
        {/* Column visibility toggle */}
        {toggleableColumns.length > 2 && (
          <div className="relative" ref={colMenuRef}>
            <button
              onClick={() => setShowColMenu(!showColMenu)}
              className={`p-1.5 rounded-lg border transition-all cursor-pointer ${
                isBright
                  ? "bg-white border-slate-200/60 text-slate-400 hover:text-slate-600"
                  : "bg-slate-800/50 border-slate-700/40 text-slate-500 hover:text-slate-300"
              }`}
              title="Toggle columns"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" strokeLinecap="round" />
                <circle cx="12" cy="12" r="3" />
              </svg>
            </button>
            {showColMenu && (
              <div className={`absolute right-0 top-full mt-1 z-50 rounded-xl border shadow-xl min-w-[160px] py-1 ${
                isBright ? "bg-white border-slate-200 shadow-black/10" : "bg-slate-900 border-slate-700 shadow-black/40"
              }`}>
                {toggleableColumns.map((col) => (
                  <label
                    key={col.key}
                    className={`flex items-center gap-2 px-3 py-1.5 text-xs cursor-pointer transition-colors ${
                      isBright ? "hover:bg-slate-50 text-slate-600" : "hover:bg-slate-800 text-slate-300"
                    }`}
                  >
                    <input
                      type="checkbox"
                      checked={!hiddenCols.has(col.key)}
                      onChange={() => {
                        setHiddenCols((prev) => {
                          const next = new Set(prev);
                          if (next.has(col.key)) next.delete(col.key);
                          else next.add(col.key);
                          return next;
                        });
                      }}
                      className="rounded"
                    />
                    {col.label}
                  </label>
                ))}
              </div>
            )}
          </div>
        )}
        {exportFilename && sorted.length > 0 && (
          <button
            onClick={() => {
              const exportCols = columns.filter((c) => c.key !== "_actions" && c.label);
              const header = exportCols.map((c) => c.label).join(",");
              const rows = sorted.map((row) =>
                exportCols.map((c) => {
                  const val = String(row[c.key] ?? "");
                  return val.includes(",") || val.includes('"') ? `"${val.replace(/"/g, '""')}"` : val;
                }).join(",")
              );
              const csv = [header, ...rows].join("\n");
              const blob = new Blob([csv], { type: "text/csv" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = `${exportFilename}.csv`;
              a.click();
              URL.revokeObjectURL(url);
            }}
            className={`px-2.5 py-1.5 rounded-lg text-[11px] font-medium border transition-all cursor-pointer ${
              isBright
                ? "bg-white border-slate-200/60 text-slate-500 hover:bg-slate-50 hover:text-slate-700"
                : "bg-slate-800/50 border-slate-700/40 text-slate-400 hover:bg-slate-700/50 hover:text-slate-300"
            }`}
            title="Export as CSV"
          >
            Export
          </button>
        )}
      </div>

      {/* Table */}
      <div
        ref={tableRef}
        tabIndex={0}
        className={`rounded-2xl border overflow-hidden transition-colors focus:outline-none focus:ring-2 focus:ring-indigo-500/30 ${
          isBright ? "border-slate-200/80 bg-white/60" : "border-slate-700/40 bg-slate-900/40"
        }`}
      >
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead>
              <tr className={`border-b ${isBright ? "border-slate-200/60" : "border-slate-700/40"}`}>
                {selectable && (
                  <th className="w-10 px-3 py-3.5">
                    <input
                      type="checkbox"
                      checked={paginatedRows.length > 0 && paginatedRows.every((r) => selectedIds.has(String(r[selectionKey!])))}
                      onChange={toggleAll}
                      className="rounded cursor-pointer"
                    />
                  </th>
                )}
                {visibleColumns.map((col) => (
                  <th
                    key={col.key}
                    className={`px-5 py-3.5 text-left text-[11px] uppercase tracking-wider font-semibold ${
                      col.sortable !== false ? "cursor-pointer select-none transition-colors" : ""
                    } ${isBright ? "text-slate-500 hover:text-slate-700" : "text-slate-400 hover:text-slate-200"} ${col.className || ""}`}
                    onClick={() => col.sortable !== false && handleSort(col.key)}
                  >
                    <span className="flex items-center gap-1.5">
                      {col.label}
                      {sortKey === col.key && (
                        <span className="text-indigo-500 text-xs">{sortDir === "asc" ? "\u2191" : "\u2193"}</span>
                      )}
                    </span>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {paginatedRows.length === 0 ? (
                <tr>
                  <td colSpan={visibleColumns.length + (selectable ? 1 : 0)} className={`px-5 py-12 text-center ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                    <div className="flex flex-col items-center gap-2">
                      <svg className={`w-8 h-8 ${isBright ? "text-slate-300" : "text-slate-600"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
                        <path d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4" strokeLinecap="round" strokeLinejoin="round" />
                      </svg>
                      <span className="text-sm">{debouncedSearch ? "No results matching your filter" : emptyMessage}</span>
                    </div>
                  </td>
                </tr>
              ) : (
                paginatedRows.map((row, i) => (
                  <tr
                    key={i}
                    className={`border-b last:border-0 transition-colors ${
                      isBright ? "border-slate-100" : "border-slate-800/30"
                    } ${
                      onRowClick ? "cursor-pointer" : ""
                    } ${
                      i === focusedRow
                        ? isBright ? "bg-indigo-50/80 ring-1 ring-inset ring-indigo-200/60" : "bg-indigo-500/[0.08] ring-1 ring-inset ring-indigo-500/20"
                        : onRowClick
                          ? isBright ? "hover:bg-indigo-50/50" : "hover:bg-indigo-500/[0.04]"
                          : ""
                    } ${
                      i === focusedRow ? "" :
                      i % 2 === 1
                        ? isBright ? "bg-slate-50/50" : "bg-slate-800/[0.08]"
                        : ""
                    }`}
                    onClick={() => onRowClick?.(row)}
                  >
                    {selectable && (
                      <td className="w-10 px-3 py-3.5" onClick={(e) => e.stopPropagation()}>
                        <input
                          type="checkbox"
                          checked={selectedIds.has(String(row[selectionKey!]))}
                          onChange={() => toggleRow(row)}
                          className="rounded cursor-pointer"
                        />
                      </td>
                    )}
                    {visibleColumns.map((col) => (
                      <td key={col.key} className={`px-5 py-3.5 text-[13px] ${isBright ? "text-slate-700" : "text-slate-300"} ${col.className || ""}`}>
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

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <span className={`text-xs ${isBright ? "text-slate-500" : "text-slate-500"}`}>
            Page {page + 1} of {totalPages}
          </span>
          <div className="flex items-center gap-1">
            <PaginationBtn
              label="First"
              onClick={() => setPage(0)}
              disabled={page === 0}
              bright={isBright}
            />
            <PaginationBtn
              label="Prev"
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              bright={isBright}
            />
            {/* Page numbers */}
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              const start = Math.max(0, Math.min(page - 2, totalPages - 5));
              const pageNum = start + i;
              if (pageNum >= totalPages) return null;
              return (
                <button
                  key={pageNum}
                  onClick={() => setPage(pageNum)}
                  className={`w-8 h-8 rounded-lg text-xs font-medium transition-all cursor-pointer ${
                    pageNum === page
                      ? isBright
                        ? "bg-indigo-100 text-indigo-700 border border-indigo-200"
                        : "bg-indigo-500/20 text-indigo-300 border border-indigo-500/30"
                      : isBright
                        ? "text-slate-500 hover:bg-slate-100 border border-transparent"
                        : "text-slate-400 hover:bg-slate-800 border border-transparent"
                  }`}
                >
                  {pageNum + 1}
                </button>
              );
            })}
            <PaginationBtn
              label="Next"
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              bright={isBright}
            />
            <PaginationBtn
              label="Last"
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
              bright={isBright}
            />
          </div>
        </div>
      )}
    </div>
  );
}

function PaginationBtn({ label, onClick, disabled, bright }: { label: string; onClick: () => void; disabled: boolean; bright: boolean }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`px-2.5 h-8 rounded-lg text-xs font-medium transition-all cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed ${
        bright
          ? "text-slate-500 hover:bg-slate-100 border border-slate-200/60"
          : "text-slate-400 hover:bg-slate-800 border border-slate-700/40"
      }`}
    >
      {label}
    </button>
  );
}
