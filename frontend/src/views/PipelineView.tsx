import { Component, useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { getNodesBounds } from "@xyflow/react";
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  SelectionMode,
  useReactFlow,
  type OnNodesChange,
  type OnEdgesChange,
  type Edge,
  type Node,
  applyNodeChanges,
  applyEdgeChanges,
} from "@xyflow/react";

import { useGraphStore } from "../store/graphStore";
import { useNavigationStore } from "../store/navigationStore";
import { useThemeStore } from "../store/themeStore";
import { useGraphLayout, type LayoutDirection } from "../hooks/useGraphLayout";
import { TopicNode } from "../nodes/TopicNode";
import { ConsumerNode } from "../nodes/ConsumerNode";
import { ProducerNode } from "../nodes/ProducerNode";
import { ServiceNode } from "../nodes/ServiceNode";
import { PipelineEdge } from "../edges/PipelineEdge";
import { MetricsPanel } from "../panels/MetricsPanel";
import { MessageInspector } from "../panels/MessageInspector";
import { EdgeDetailPanel } from "../panels/EdgeDetailPanel";
import { ContextMenu, type ContextMenuItem } from "../components/ContextMenu";

/** Inner error boundary for panels */
class PanelErrorBoundary extends Component<
  { children: ReactNode; fallback?: ReactNode },
  { hasError: boolean; error: string }
> {
  state = { hasError: false, error: "" };

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error: error.message };
  }

  componentDidCatch(error: Error) {
    console.error("[PanelErrorBoundary]", error);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="absolute left-4 bottom-4 w-[360px] z-50">
          <div className="rounded-2xl border border-red-500/30 bg-red-950/50 backdrop-blur-xl p-4">
            <div className="text-red-400 text-sm font-medium mb-2">Panel error</div>
            <div className="text-slate-400 text-xs mb-3">{this.state.error}</div>
            <button
              onClick={() => {
                this.setState({ hasError: false, error: "" });
                useGraphStore.getState().setSelectedNode(null);
                useGraphStore.getState().setInspectorTopic(null);
              }}
              className="px-3 py-1.5 rounded-lg text-xs bg-slate-800 border border-slate-700/50 text-slate-300 hover:bg-slate-700 cursor-pointer"
            >
              Dismiss
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

const nodeTypes = {
  topicNode: TopicNode,
  consumerNode: ConsumerNode,
  producerNode: ProducerNode,
  serviceNode: ServiceNode,
};

const edgeTypes = {
  pipelineEdge: PipelineEdge,
};

function MiniSparkline({ data, bright }: { data: number[]; bright: boolean }) {
  const w = 48, h = 16;
  const max = Math.max(...data, 1);
  const points = data.map((v, i) => `${(i / (data.length - 1)) * w},${h - (v / max) * h}`).join(" ");
  return (
    <svg width={w} height={h} className="shrink-0">
      <polyline
        points={points}
        fill="none"
        stroke={bright ? "#6366f1" : "#818cf8"}
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    </svg>
  );
}

let initialLayoutDone = false;

function computeFullPath(startId: string, edges: Edge[]): { nodeIds: Set<string>; edgeIds: Set<string> } {
  const nodeIds = new Set<string>([startId]);
  const edgeIds = new Set<string>();

  // Build adjacency maps for O(1) lookups
  const outgoing = new Map<string, { target: string; edgeId: string }[]>();
  const incoming = new Map<string, { source: string; edgeId: string }[]>();
  for (const edge of edges) {
    if (!outgoing.has(edge.source)) outgoing.set(edge.source, []);
    outgoing.get(edge.source)!.push({ target: edge.target, edgeId: edge.id });
    if (!incoming.has(edge.target)) incoming.set(edge.target, []);
    incoming.get(edge.target)!.push({ source: edge.source, edgeId: edge.id });
  }

  // BFS downstream
  const downQueue = [startId];
  while (downQueue.length > 0) {
    const current = downQueue.shift()!;
    for (const { target, edgeId } of outgoing.get(current) || []) {
      if (!nodeIds.has(target)) {
        nodeIds.add(target);
        edgeIds.add(edgeId);
        downQueue.push(target);
      }
    }
  }

  // BFS upstream
  const upQueue = [startId];
  while (upQueue.length > 0) {
    const current = upQueue.shift()!;
    for (const { source, edgeId } of incoming.get(current) || []) {
      if (!nodeIds.has(source)) {
        nodeIds.add(source);
        edgeIds.add(edgeId);
        upQueue.push(source);
      }
    }
  }

  return { nodeIds, edgeIds };
}

export function PipelineView() {
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const selectedEdge = useGraphStore((s) => s.selectedEdge);
  const inspectorTopic = useGraphStore((s) => s.inspectorTopic);
  const searchQuery = useGraphStore((s) => s.searchQuery);
  const hideSystemTopics = useGraphStore((s) => s.hideSystemTopics);
  const visibleNodeTypes = useGraphStore((s) => s.visibleNodeTypes);
  const rateHistory = useGraphStore((s) => s.rateHistory);
  const metrics = useGraphStore((s) => s.metrics);
  const connectionStatus = useGraphStore((s) => s.connectionStatus);
  const { setActiveView, navigateToTopic, navigateToConsumerGroup } = useNavigationStore();
  const { theme } = useThemeStore();
  const isBright = theme === "bright";

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; nodeId?: string; edgeId?: string } | null>(null);
  const [showSearch, setShowSearch] = useState(false);
  const [showMinimap, setShowMinimap] = useState(true);
  const [showDashboard, setShowDashboard] = useState(false);
  const [layoutDirection, setLayoutDirection] = useState<LayoutDirection>("LR");
  const searchInputRef = useRef<HTMLInputElement>(null);

  // Keyboard shortcuts
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        if (showSearch) {
          setShowSearch(false);
          useGraphStore.getState().setSearchQuery("");
        } else {
          useGraphStore.getState().setSelectedNode(null);
          useGraphStore.getState().setSelectedEdge(null);
          useGraphStore.getState().setInspectorTopic(null);
        }
      }
      // Cmd/Ctrl+K to open search
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setShowSearch(true);
        setTimeout(() => searchInputRef.current?.focus(), 50);
      }
      // "/" to open search (when not in an input)
      if (e.key === "/" && !showSearch && !(e.target instanceof HTMLInputElement) && !(e.target instanceof HTMLTextAreaElement)) {
        e.preventDefault();
        setShowSearch(true);
        setTimeout(() => searchInputRef.current?.focus(), 50);
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [showSearch]);

  const { layoutNodes } = useGraphLayout();
  const { fitView, zoomIn, zoomOut, getZoom } = useReactFlow();
  const [zoomLevel, setZoomLevel] = useState(1);

  // Detect namespaces from topic names
  const [namespaceFilter, setNamespaceFilter] = useState<string | null>(null);
  const namespaces = useMemo(() => {
    const prefixCounts = new Map<string, number>();
    for (const n of nodes) {
      if (n.type !== "topicNode") continue;
      const label = String(n.data?.label || "");
      // Try common separators: . - _
      for (const sep of [".", "-", "_"]) {
        const idx = label.indexOf(sep);
        if (idx > 0) {
          const prefix = label.slice(0, idx);
          prefixCounts.set(prefix, (prefixCounts.get(prefix) || 0) + 1);
          break;
        }
      }
    }
    // Only show namespaces with 2+ topics
    return [...prefixCounts.entries()].filter(([, c]) => c >= 2).sort((a, b) => b[1] - a[1]);
  }, [nodes]);

  // Filter nodes — optimized with early return
  const { filteredNodes, searchMatchIds } = useMemo(() => {
    let result = nodes;
    // Filter by node type visibility
    result = result.filter((n) => !n.type || visibleNodeTypes.has(n.type));
    if (hideSystemTopics) {
      result = result.filter((n) => !String(n.data?.label || "").startsWith("__"));
    }
    // Namespace filter
    if (namespaceFilter) {
      const topicIds = new Set<string>();
      for (const n of result) {
        if (n.type === "topicNode") {
          const label = String(n.data?.label || "");
          if (label.startsWith(namespaceFilter + ".") || label.startsWith(namespaceFilter + "-") || label.startsWith(namespaceFilter + "_")) {
            topicIds.add(n.id);
          }
        }
      }
      // Include topics matching namespace + all connected nodes
      const connectedIds = new Set(topicIds);
      edges.forEach((e) => {
        if (topicIds.has(e.source)) connectedIds.add(e.target);
        if (topicIds.has(e.target)) connectedIds.add(e.source);
      });
      result = result.filter((n) => connectedIds.has(n.id));
    }
    let matchIds: Set<string> | null = null;
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      matchIds = new Set<string>();
      result.forEach((n) => {
        if (String(n.data?.label || "").toLowerCase().includes(q)) matchIds!.add(n.id);
      });
      // Also include directly connected nodes
      const expandedIds = new Set(matchIds);
      edges.forEach((e) => {
        if (matchIds!.has(e.source)) expandedIds.add(e.target);
        if (matchIds!.has(e.target)) expandedIds.add(e.source);
      });
      result = result.filter((n) => expandedIds.has(n.id));
    }
    return { filteredNodes: result, searchMatchIds: matchIds };
  }, [nodes, searchQuery, hideSystemTopics, visibleNodeTypes, edges, namespaceFilter]);

  // Filter edges — use Set for O(1) lookups
  const filteredEdges = useMemo(() => {
    const nodeIds = new Set(filteredNodes.map((n) => n.id));
    return edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
  }, [edges, filteredNodes]);

  // Path highlighting with optimized BFS
  const { highlightNodeIds, highlightEdgeIds } = useMemo(() => {
    if (!selectedNode) return { highlightNodeIds: null, highlightEdgeIds: null };
    const path = computeFullPath(selectedNode, filteredEdges);
    return { highlightNodeIds: path.nodeIds, highlightEdgeIds: path.edgeIds };
  }, [selectedNode, filteredEdges]);

  const displayNodes: Node[] = useMemo(() => {
    let result = filteredNodes;
    if (highlightNodeIds) {
      result = result.map((n) => ({
        ...n,
        data: { ...n.data, _dimmed: !highlightNodeIds.has(n.id) },
      }));
    }
    // Add search glow to direct matches
    if (searchMatchIds) {
      result = result.map((n) => ({
        ...n,
        data: { ...n.data, _searchMatch: searchMatchIds.has(n.id) },
      }));
    }
    return result;
  }, [filteredNodes, highlightNodeIds, searchMatchIds]);

  const displayEdges: Edge[] = useMemo(() => {
    if (!highlightEdgeIds) return filteredEdges;
    return filteredEdges.map((e) => ({
      ...e,
      data: { ...e.data, _dimmed: !highlightEdgeIds.has(e.id) },
    }));
  }, [filteredEdges, highlightEdgeIds]);

  // Layout once on first data
  const nodeCount = filteredNodes.length;
  useEffect(() => {
    if (nodeCount === 0) return;
    if (initialLayoutDone) return;
    initialLayoutDone = true;

    const currentNodes = useGraphStore.getState().nodes;
    const currentEdges = useGraphStore.getState().edges;
    let ns = currentNodes;
    if (useGraphStore.getState().hideSystemTopics) {
      ns = ns.filter((n) => !String(n.data?.label || "").startsWith("__"));
    }
    const nodeIds = new Set(ns.map((n) => n.id));
    const es = currentEdges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));

    const laid = layoutNodes(ns, es);
    useGraphStore.setState({ nodes: laid });
    setTimeout(() => fitView({ padding: 0.15, duration: 500 }), 150);
  }, [nodeCount, layoutNodes, fitView]);

  const onNodesChange: OnNodesChange = useCallback((changes) => {
    useGraphStore.setState((s) => ({ nodes: applyNodeChanges(changes, s.nodes) }));
  }, []);

  const onEdgesChange: OnEdgesChange = useCallback((changes) => {
    useGraphStore.setState((s) => ({ edges: applyEdgeChanges(changes, s.edges) }));
  }, []);

  const handleNodeClick = useCallback(
    (event: React.MouseEvent, node: { id: string }) => {
      try {
        event.stopPropagation();
        event.preventDefault();
        useGraphStore.getState().setSelectedNode(node.id);
        useGraphStore.getState().setInspectorTopic(null);
      } catch (err) {
        console.error("[PipelineView] handleNodeClick error:", err);
      }
    },
    []
  );

  const handleEdgeClick = useCallback(
    (event: React.MouseEvent, edge: Edge) => {
      try {
        event.stopPropagation();
        event.preventDefault();
        useGraphStore.getState().setSelectedEdge(edge.id);
      } catch (err) {
        console.error("[PipelineView] handleEdgeClick error:", err);
      }
    },
    []
  );

  const handleNodeDoubleClick = useCallback(
    (_event: React.MouseEvent, node: { id: string; type?: string; data?: Record<string, unknown> }) => {
      // Double-click navigates to detailed view
      if (node.type === "topicNode" && node.data?.label) {
        navigateToTopic(String(node.data.label));
      } else if ((node.type === "consumerNode" || node.type === "serviceNode") && node.data?.label) {
        navigateToConsumerGroup(String(node.data.label));
      }
    },
    [navigateToTopic, navigateToConsumerGroup]
  );

  const handlePaneClick = useCallback(() => {
    try {
      useGraphStore.getState().setSelectedNode(null);
      useGraphStore.getState().setSelectedEdge(null);
      useGraphStore.getState().setInspectorTopic(null);
      setContextMenu(null);
    } catch (err) {
      console.error("[PipelineView] handlePaneClick error:", err);
    }
  }, []);

  const handleNodeContextMenu = useCallback(
    (event: React.MouseEvent, node: { id: string }) => {
      event.preventDefault();
      event.stopPropagation();
      setContextMenu({ x: event.clientX, y: event.clientY, nodeId: node.id });
    },
    []
  );

  const handleEdgeContextMenu = useCallback(
    (event: React.MouseEvent, edge: Edge) => {
      event.preventDefault();
      event.stopPropagation();
      setContextMenu({ x: event.clientX, y: event.clientY, edgeId: edge.id });
    },
    []
  );

  // Export pipeline graph as SVG or PNG
  const exportGraph = useCallback((format: "svg" | "png") => {
    const container = document.querySelector(".react-flow__viewport") as HTMLElement;
    if (!container || filteredNodes.length === 0) return;

    const bounds = getNodesBounds(filteredNodes);
    const padding = 40;
    const w = bounds.width + padding * 2;
    const h = bounds.height + padding * 2;

    // Build SVG manually from graph data
    const nodeColorMap: Record<string, { fill: string; stroke: string; text: string }> = {
      topicNode: { fill: "#312e81", stroke: "#6366f1", text: "#c7d2fe" },
      consumerNode: { fill: "#78350f", stroke: "#f59e0b", text: "#fde68a" },
      producerNode: { fill: "#14532d", stroke: "#22c55e", text: "#bbf7d0" },
      serviceNode: { fill: "#164e63", stroke: "#06b6d4", text: "#cffafe" },
    };
    const iconMap: Record<string, string> = {
      topicNode: "\u25A6",
      consumerNode: "\u25BC",
      producerNode: "\u25B2",
      serviceNode: "\u2B21",
    };

    let svgContent = `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">`;
    svgContent += `<rect width="${w}" height="${h}" fill="${isBright ? "#f8fafc" : "#0f172a"}" />`;
    // Grid dots
    for (let gx = 0; gx < w; gx += 32) {
      for (let gy = 0; gy < h; gy += 32) {
        svgContent += `<circle cx="${gx}" cy="${gy}" r="0.5" fill="${isBright ? "rgba(99,102,241,0.12)" : "rgba(99,102,241,0.08)"}" />`;
      }
    }

    // Draw edges
    for (const edge of filteredEdges) {
      const src = filteredNodes.find((n) => n.id === edge.source);
      const tgt = filteredNodes.find((n) => n.id === edge.target);
      if (!src?.position || !tgt?.position) continue;
      const sx = (src.position.x + (src.measured?.width || 180) / 2) - bounds.x + padding;
      const sy = (src.position.y + (src.measured?.height || 60) / 2) - bounds.y + padding;
      const tx = (tgt.position.x + (tgt.measured?.width || 180) / 2) - bounds.x + padding;
      const ty = (tgt.position.y + (tgt.measured?.height || 60) / 2) - bounds.y + padding;
      const lag = Number(edge.data?.lag || 0);
      const edgeColor = lag > 1000 ? "#ef4444" : lag > 0 ? "#f59e0b" : (isBright ? "#94a3b8" : "#475569");
      svgContent += `<line x1="${sx}" y1="${sy}" x2="${tx}" y2="${ty}" stroke="${edgeColor}" stroke-width="1.5" stroke-opacity="0.6" marker-end="url(#arrow)" />`;
    }

    // Arrow marker
    svgContent += `<defs><marker id="arrow" viewBox="0 0 10 10" refX="10" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="${isBright ? "#94a3b8" : "#475569"}" /></marker></defs>`;

    // Draw nodes
    for (const node of filteredNodes) {
      if (!node.position) continue;
      const nx = node.position.x - bounds.x + padding;
      const ny = node.position.y - bounds.y + padding;
      const nw = node.measured?.width || 180;
      const nh = node.measured?.height || 60;
      const style = nodeColorMap[node.type || ""] || nodeColorMap.topicNode;
      const icon = iconMap[node.type || ""] || "";
      const label = String(node.data?.label || node.id);
      svgContent += `<rect x="${nx}" y="${ny}" width="${nw}" height="${nh}" rx="12" fill="${style.fill}" stroke="${style.stroke}" stroke-width="1.5" />`;
      svgContent += `<text x="${nx + 14}" y="${ny + nh / 2 + 1}" font-family="monospace" font-size="10" fill="${style.text}" dominant-baseline="middle">${icon} ${label.length > 22 ? label.slice(0, 22) + "\u2026" : label}</text>`;
    }

    svgContent += `</svg>`;

    if (format === "svg") {
      const blob = new Blob([svgContent], { type: "image/svg+xml" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `kafka-pipeline-${new Date().toISOString().slice(0, 10)}.svg`;
      a.click();
      URL.revokeObjectURL(url);
    } else {
      // Render SVG to canvas for PNG
      const img = new Image();
      const svgBlob = new Blob([svgContent], { type: "image/svg+xml" });
      const url = URL.createObjectURL(svgBlob);
      img.onload = () => {
        const scale = 2; // 2x for retina
        const canvas = document.createElement("canvas");
        canvas.width = w * scale;
        canvas.height = h * scale;
        const ctx = canvas.getContext("2d");
        if (!ctx) return;
        ctx.scale(scale, scale);
        ctx.drawImage(img, 0, 0, w, h);
        URL.revokeObjectURL(url);
        canvas.toBlob((blob) => {
          if (!blob) return;
          const pngUrl = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = pngUrl;
          a.download = `kafka-pipeline-${new Date().toISOString().slice(0, 10)}.png`;
          a.click();
          URL.revokeObjectURL(pngUrl);
        }, "image/png");
      };
      img.src = url;
    }
  }, [filteredNodes, filteredEdges, isBright]);

  const contextMenuItems = useMemo((): ContextMenuItem[] => {
    if (!contextMenu) return [];

    // Edge context menu
    if (contextMenu.edgeId) {
      const edge = edges.find((e) => e.id === contextMenu.edgeId);
      if (!edge) return [];
      const sourceNode = nodes.find((n) => n.id === edge.source);
      const targetNode = nodes.find((n) => n.id === edge.target);
      const sourceLabel = String(sourceNode?.data?.label || edge.source);
      const targetLabel = String(targetNode?.data?.label || edge.target);
      const items: ContextMenuItem[] = [];

      items.push({
        label: `Select Edge`,
        icon: "\u2194",
        onClick: () => useGraphStore.getState().setSelectedEdge(edge.id),
      });

      if (sourceNode?.type === "topicNode") {
        items.push({
          label: `Inspect: ${sourceLabel}`,
          icon: "\uD83D\uDD0D",
          onClick: () => useGraphStore.getState().setInspectorTopic(sourceLabel),
        });
      }
      if (targetNode?.type === "topicNode") {
        items.push({
          label: `Inspect: ${targetLabel}`,
          icon: "\uD83D\uDD0D",
          onClick: () => useGraphStore.getState().setInspectorTopic(targetLabel),
        });
      }

      items.push({ label: "", icon: "", separator: true, onClick: () => {} });
      items.push({
        label: "Copy Edge Info",
        icon: "\uD83D\uDCCB",
        onClick: () => navigator.clipboard.writeText(`${sourceLabel} \u2192 ${targetLabel}`),
      });

      return items;
    }

    // Node context menu
    const node = nodes.find((n) => n.id === contextMenu.nodeId);
    if (!node) return [];
    const d = node.data as Record<string, unknown>;
    const label = String(d?.label || node.id);
    const items: ContextMenuItem[] = [];

    items.push({
      label: "Select & Highlight Path",
      icon: "\u2728",
      onClick: () => {
        useGraphStore.getState().setSelectedNode(node.id);
        useGraphStore.getState().setInspectorTopic(null);
      },
    });

    if (node.type === "topicNode" && d?.label) {
      items.push({
        label: "Inspect Messages",
        icon: "\uD83D\uDD0D",
        onClick: () => useGraphStore.getState().setInspectorTopic(String(d.label)),
      });
    }

    items.push({
      label: "Copy Name",
      icon: "\uD83D\uDCCB",
      onClick: () => navigator.clipboard.writeText(label),
    });

    items.push({ label: "", icon: "", separator: true, onClick: () => {} });

    if (node.type === "topicNode") {
      items.push({
        label: `Go to Topic: ${label}`,
        icon: "\u2192",
        onClick: () => navigateToTopic(label),
      });
    }
    if (node.type === "consumerNode" || node.type === "serviceNode") {
      items.push({
        label: `Go to Consumer: ${label}`,
        icon: "\u2192",
        onClick: () => navigateToConsumerGroup(label),
      });
    }
    if (node.type === "serviceNode") {
      items.push({
        label: "Go to Applications View",
        icon: "\u2192",
        onClick: () => setActiveView("applications"),
      });
    }

    return items;
  }, [contextMenu, nodes, edges, setActiveView, navigateToTopic, navigateToConsumerGroup]);

  return (
    <div className="flex-1 relative">
      <ReactFlow
        nodes={displayNodes}
        edges={displayEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        onNodeDoubleClick={handleNodeDoubleClick}
        onEdgeClick={handleEdgeClick}
        onPaneClick={handlePaneClick}
        onNodeContextMenu={handleNodeContextMenu}
        onEdgeContextMenu={handleEdgeContextMenu}
        onMoveEnd={() => setZoomLevel(getZoom())}
        selectionOnDrag
        selectionMode={SelectionMode.Partial}
        snapToGrid
        snapGrid={[20, 20]}
        colorMode={isBright ? "light" : "dark"}
        proOptions={{ hideAttribution: true }}
        defaultEdgeOptions={{ type: "pipelineEdge" }}
        minZoom={0.05}
        maxZoom={2}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={32}
          size={1}
          color={isBright ? "rgba(99, 102, 241, 0.12)" : "rgba(99, 102, 241, 0.08)"}
        />
        <Controls position="bottom-right" showInteractive={false} showZoom={false} showFitView={false} />
        {showMinimap && filteredNodes.length > 3 && (
          <MiniMap
            position="bottom-right"
            style={{ marginBottom: 200 }}
            pannable
            zoomable
            nodeColor={(node) => {
              if (node.id === selectedNode) return "#f59e0b";
              if (highlightNodeIds?.has(node.id)) {
                switch (node.type) {
                  case "topicNode": return "#818cf8";
                  case "serviceNode": return "#22d3ee";
                  case "consumerNode": return "#fbbf24";
                  case "producerNode": return "#4ade80";
                  default: return "#94a3b8";
                }
              }
              switch (node.type) {
                case "topicNode": return "#6366f1";
                case "serviceNode": return "#06b6d4";
                case "consumerNode": return "#f59e0b";
                case "producerNode": return "#22c55e";
                default: return "#475569";
              }
            }}
          />
        )}
      </ReactFlow>

      {/* Layout direction controls */}
      <div className={`absolute bottom-4 left-4 z-10 flex items-center gap-0.5 rounded-xl border backdrop-blur-xl shadow-lg px-1 ${
        isBright ? "bg-white/90 border-slate-200/60" : "bg-slate-900/90 border-slate-700/50"
      }`}>
        {([
          { dir: "LR" as LayoutDirection, label: "\u2192", title: "Left to Right" },
          { dir: "TB" as LayoutDirection, label: "\u2193", title: "Top to Bottom" },
          { dir: "RL" as LayoutDirection, label: "\u2190", title: "Right to Left" },
        ]).map(({ dir, label, title }) => (
          <button
            key={dir}
            onClick={() => {
              setLayoutDirection(dir);
              const currentNodes = useGraphStore.getState().nodes;
              const currentEdges = useGraphStore.getState().edges;
              let ns = currentNodes;
              if (useGraphStore.getState().hideSystemTopics) {
                ns = ns.filter((n) => !String(n.data?.label || "").startsWith("__"));
              }
              const nodeIds = new Set(ns.map((n) => n.id));
              const es = currentEdges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
              const laid = layoutNodes(ns, es, dir);
              useGraphStore.setState({ nodes: laid });
              setTimeout(() => { fitView({ padding: 0.15, duration: 400 }); setTimeout(() => setZoomLevel(getZoom()), 450); }, 50);
            }}
            className={`w-7 h-7 flex items-center justify-center text-sm rounded-lg transition-colors cursor-pointer ${
              layoutDirection === dir
                ? isBright ? "bg-indigo-100 text-indigo-600" : "bg-indigo-500/20 text-indigo-300"
                : isBright ? "text-slate-400 hover:bg-slate-100 hover:text-slate-600" : "text-slate-500 hover:bg-slate-800 hover:text-slate-300"
            }`}
            title={title}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Custom zoom controls */}
      <div className={`absolute bottom-4 right-4 z-10 flex items-center gap-1 rounded-xl border backdrop-blur-xl shadow-lg ${
        isBright ? "bg-white/90 border-slate-200/60" : "bg-slate-900/90 border-slate-700/50"
      }`}>
        <button
          onClick={() => { zoomOut({ duration: 200 }); setTimeout(() => setZoomLevel(getZoom()), 250); }}
          className={`w-8 h-8 flex items-center justify-center text-sm font-bold transition-colors cursor-pointer rounded-l-xl ${
            isBright ? "text-slate-500 hover:bg-slate-100" : "text-slate-400 hover:bg-slate-800"
          }`}
          title="Zoom out"
        >
          −
        </button>
        <span className={`text-[10px] font-mono w-10 text-center tabular-nums ${isBright ? "text-slate-500" : "text-slate-400"}`}>
          {Math.round(zoomLevel * 100)}%
        </span>
        <button
          onClick={() => { zoomIn({ duration: 200 }); setTimeout(() => setZoomLevel(getZoom()), 250); }}
          className={`w-8 h-8 flex items-center justify-center text-sm font-bold transition-colors cursor-pointer ${
            isBright ? "text-slate-500 hover:bg-slate-100" : "text-slate-400 hover:bg-slate-800"
          }`}
          title="Zoom in"
        >
          +
        </button>
        <span className={`w-px h-4 ${isBright ? "bg-slate-200" : "bg-slate-700"}`} />
        <button
          onClick={() => { fitView({ padding: 0.15, duration: 400 }); setTimeout(() => setZoomLevel(getZoom()), 450); }}
          className={`w-8 h-8 flex items-center justify-center transition-colors cursor-pointer rounded-r-xl ${
            isBright ? "text-slate-500 hover:bg-slate-100" : "text-slate-400 hover:bg-slate-800"
          }`}
          title="Fit to view"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path d="M4 8V4h4M20 8V4h-4M4 16v4h4M20 16v4h-4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
      </div>

      {/* Pipeline stats overlay */}
      {nodes.length > 0 && (
        <div className={`absolute top-3 right-3 z-10 flex items-center gap-2 px-3 py-1.5 rounded-xl border backdrop-blur-sm text-[10px] ${
          isBright ? "bg-white/80 border-slate-200/60 text-slate-500" : "bg-slate-900/80 border-slate-800/60 text-slate-400"
        }`}>
          <FilterChip label="Topics" count={nodes.filter((n) => n.type === "topicNode").length} active={visibleNodeTypes.has("topicNode")} onClick={() => useGraphStore.getState().toggleNodeTypeVisibility("topicNode")} color="indigo" bright={isBright} />
          <FilterChip label="Consumers" count={nodes.filter((n) => n.type === "consumerNode" || n.type === "serviceNode").length} active={visibleNodeTypes.has("consumerNode")} onClick={() => { useGraphStore.getState().toggleNodeTypeVisibility("consumerNode"); useGraphStore.getState().toggleNodeTypeVisibility("serviceNode"); }} color="amber" bright={isBright} />
          <FilterChip label="Producers" count={nodes.filter((n) => n.type === "producerNode").length} active={visibleNodeTypes.has("producerNode")} onClick={() => useGraphStore.getState().toggleNodeTypeVisibility("producerNode")} color="emerald" bright={isBright} />
          <span className={`w-px h-3 ${isBright ? "bg-slate-200" : "bg-slate-700"}`} />
          <span>{filteredEdges.length} edges</span>
          <button
            onClick={() => setShowMinimap(!showMinimap)}
            className={`px-1.5 py-0.5 rounded-md border text-[10px] font-medium transition-all cursor-pointer ${
              showMinimap
                ? isBright ? "bg-slate-100 border-slate-200/60 text-slate-600" : "bg-slate-700/50 border-slate-600/30 text-slate-300"
                : isBright ? "bg-transparent border-slate-200/40 text-slate-400" : "bg-transparent border-slate-700/30 text-slate-500"
            }`}
            title={showMinimap ? "Hide minimap" : "Show minimap"}
          >
            Map
          </button>
          {rateHistory.length > 1 && (
            <>
              <span className={`w-px h-3 ${isBright ? "bg-slate-200" : "bg-slate-700"}`} />
              <span className="font-mono">{Object.values(metrics).reduce((s, m) => s + (m.msgPerSec || 0), 0).toFixed(0)} msg/s</span>
              <MiniSparkline data={rateHistory} bright={isBright} />
            </>
          )}
          <button
            onClick={() => setShowDashboard(!showDashboard)}
            className={`px-1.5 py-0.5 rounded-md border text-[10px] font-medium transition-all cursor-pointer ${
              showDashboard
                ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-600" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                : isBright ? "bg-transparent border-slate-200/40 text-slate-400" : "bg-transparent border-slate-700/30 text-slate-500"
            }`}
            title="Toggle dashboard"
          >
            Stats
          </button>
          {namespaces.length > 0 && (
            <>
              <span className={`w-px h-3 ${isBright ? "bg-slate-200" : "bg-slate-700"}`} />
              <button
                onClick={() => setNamespaceFilter(null)}
                className={`px-1.5 py-0.5 rounded-md border text-[10px] font-medium transition-all cursor-pointer ${
                  !namespaceFilter
                    ? isBright ? "bg-indigo-50 border-indigo-200/60 text-indigo-600" : "bg-indigo-500/15 border-indigo-500/30 text-indigo-300"
                    : isBright ? "bg-transparent border-slate-200/40 text-slate-400" : "bg-transparent border-slate-700/30 text-slate-500"
                }`}
              >
                All
              </button>
              {namespaces.slice(0, 4).map(([ns, count]) => (
                <button
                  key={ns}
                  onClick={() => setNamespaceFilter(namespaceFilter === ns ? null : ns)}
                  className={`px-1.5 py-0.5 rounded-md border text-[10px] font-mono font-medium transition-all cursor-pointer ${
                    namespaceFilter === ns
                      ? isBright ? "bg-violet-50 border-violet-200/60 text-violet-600" : "bg-violet-500/15 border-violet-500/30 text-violet-300"
                      : isBright ? "bg-transparent border-slate-200/40 text-slate-400 hover:bg-slate-50" : "bg-transparent border-slate-700/30 text-slate-500 hover:bg-slate-800"
                  }`}
                  title={`${count} topics with prefix "${ns}"`}
                >
                  {ns} ({count})
                </button>
              ))}
            </>
          )}
          <span className={`w-px h-3 ${isBright ? "bg-slate-200" : "bg-slate-700"}`} />
          <button
            onClick={() => exportGraph("svg")}
            className={`px-1.5 py-0.5 rounded-md border text-[10px] font-medium transition-all cursor-pointer ${
              isBright ? "bg-transparent border-slate-200/40 text-slate-400 hover:bg-slate-50 hover:text-slate-600" : "bg-transparent border-slate-700/30 text-slate-500 hover:bg-slate-800 hover:text-slate-300"
            }`}
            title="Export as SVG"
          >
            SVG
          </button>
          <button
            onClick={() => exportGraph("png")}
            className={`px-1.5 py-0.5 rounded-md border text-[10px] font-medium transition-all cursor-pointer ${
              isBright ? "bg-transparent border-slate-200/40 text-slate-400 hover:bg-slate-50 hover:text-slate-600" : "bg-transparent border-slate-700/30 text-slate-500 hover:bg-slate-800 hover:text-slate-300"
            }`}
            title="Export as PNG"
          >
            PNG
          </button>
        </div>
      )}

      {/* Dashboard overlay */}
      {showDashboard && nodes.length > 0 && (
        <div className={`absolute top-14 right-3 z-10 w-64 rounded-xl border backdrop-blur-xl shadow-xl overflow-hidden ${
          isBright ? "bg-white/95 border-slate-200/80" : "bg-slate-900/95 border-slate-700/60"
        }`}>
          <div className={`px-3 py-2 border-b ${isBright ? "border-slate-200/40" : "border-slate-700/40"}`}>
            <span className={`text-[10px] uppercase tracking-wider font-semibold ${isBright ? "text-slate-500" : "text-slate-400"}`}>Pipeline Dashboard</span>
          </div>
          <div className="p-3 space-y-2">
            {/* Throughput */}
            <DashRow label="Total Throughput" value={`${Object.values(metrics).reduce((s, m) => s + (m.msgPerSec || 0), 0).toFixed(0)} msg/s`} color={Object.values(metrics).some(m => m.msgPerSec > 0) ? "emerald" : "slate"} bright={isBright} />
            <DashRow label="Active Topics" value={`${Object.values(metrics).filter(m => m.msgPerSec > 0).length} / ${nodes.filter(n => n.type === "topicNode").length}`} color="indigo" bright={isBright} />
            <DashRow label="Total Messages" value={Object.values(metrics).reduce((s, m) => s + (m.totalMessages || 0), 0).toLocaleString()} color="slate" bright={isBright} />
            {/* Consumer lag */}
            {(() => {
              const lagEdges = edges.filter(e => Number(e.data?.lag || 0) > 0);
              const totalLag = lagEdges.reduce((s, e) => s + Number(e.data?.lag || 0), 0);
              return totalLag > 0 ? (
                <DashRow label="Consumer Lag" value={totalLag > 1000 ? `${(totalLag / 1000).toFixed(1)}K` : String(totalLag)} color={totalLag > 10000 ? "red" : totalLag > 1000 ? "amber" : "emerald"} bright={isBright} />
              ) : null;
            })()}
            <DashRow label="Nodes" value={String(filteredNodes.length)} color="slate" bright={isBright} />
            <DashRow label="Connections" value={String(filteredEdges.length)} color="slate" bright={isBright} />
            <DashRow label="WebSocket" value={connectionStatus} color={connectionStatus === "connected" ? "emerald" : "red"} bright={isBright} />
          </div>
        </div>
      )}

      {/* Search bar */}
      {showSearch && (
        <div className={`absolute top-3 left-3 z-20 flex items-center gap-2 px-3 py-2 rounded-xl border backdrop-blur-xl shadow-lg ${
          isBright ? "bg-white/95 border-slate-200/80 shadow-black/5" : "bg-slate-900/95 border-slate-700/60 shadow-black/40"
        }`}>
          <svg className={`w-4 h-4 shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <circle cx="11" cy="11" r="8" />
            <path d="m21 21-4.35-4.35" strokeLinecap="round" />
          </svg>
          <input
            ref={searchInputRef}
            type="text"
            value={searchQuery}
            onChange={(e) => useGraphStore.getState().setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") {
                setShowSearch(false);
                useGraphStore.getState().setSearchQuery("");
              }
            }}
            placeholder="Search nodes..."
            className={`w-56 text-sm bg-transparent border-none outline-none ${
              isBright ? "text-slate-800 placeholder-slate-400" : "text-white placeholder-slate-500"
            }`}
            autoFocus
          />
          {searchQuery && (
            <span className={`text-[10px] tabular-nums shrink-0 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
              {searchMatchIds?.size || 0} match{(searchMatchIds?.size || 0) !== 1 ? "es" : ""}
            </span>
          )}
          <button
            onClick={() => { setShowSearch(false); useGraphStore.getState().setSearchQuery(""); }}
            className={`shrink-0 w-5 h-5 rounded flex items-center justify-center cursor-pointer ${
              isBright ? "text-slate-400 hover:text-slate-600" : "text-slate-500 hover:text-slate-300"
            }`}
          >
            <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path d="M6 18L18 6M6 6l12 12" strokeLinecap="round" />
            </svg>
          </button>
        </div>
      )}

      {/* Empty state overlay */}
      {nodes.length === 0 && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none z-10">
          <div className={`text-center ${isBright ? "text-slate-400" : "text-slate-500"}`}>
            <div className="w-10 h-10 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
            <div className="text-lg font-semibold mb-1">Waiting for Kafka data...</div>
            <div className="text-sm">Connect to a Kafka cluster to see the pipeline graph</div>
          </div>
        </div>
      )}

      {/* Keyboard shortcuts hint */}
      {nodes.length > 0 && !selectedNode && !selectedEdge && !inspectorTopic && (
        <div className={`absolute bottom-4 left-1/2 -translate-x-1/2 z-10 flex items-center gap-4 px-4 py-2 rounded-xl border backdrop-blur-sm text-[10px] ${
          isBright ? "bg-white/80 border-slate-200/60 text-slate-400" : "bg-slate-900/80 border-slate-800/60 text-slate-500"
        }`}>
          <span><kbd className={`px-1.5 py-0.5 rounded text-[9px] font-mono ${isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"}`}>F</kbd> Fit view</span>
          <span><kbd className={`px-1.5 py-0.5 rounded text-[9px] font-mono ${isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"}`}>L</kbd> Re-layout</span>
          <span><kbd className={`px-1.5 py-0.5 rounded text-[9px] font-mono ${isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"}`}>{navigator.platform.includes("Mac") ? "\u2318" : "Ctrl"}+K</kbd> Search</span>
          <span><kbd className={`px-1.5 py-0.5 rounded text-[9px] font-mono ${isBright ? "bg-slate-100 text-slate-500" : "bg-slate-800 text-slate-400"}`}>Esc</kbd> Deselect</span>
        </div>
      )}

      <PanelErrorBoundary>
        {selectedNode && (
          <MetricsPanel
            nodeId={selectedNode}
            onClose={() => {
              useGraphStore.getState().setSelectedNode(null);
              useGraphStore.getState().setInspectorTopic(null);
            }}
            onInspect={(topic) => useGraphStore.getState().setInspectorTopic(topic)}
          />
        )}
      </PanelErrorBoundary>

      <PanelErrorBoundary>
        {selectedEdge && !inspectorTopic && (
          <EdgeDetailPanel
            edgeId={selectedEdge}
            onClose={() => useGraphStore.getState().setSelectedEdge(null)}
            onInspectTopic={(topic) => {
              useGraphStore.getState().setInspectorTopic(topic);
              useGraphStore.getState().setSelectedEdge(null);
            }}
          />
        )}
      </PanelErrorBoundary>

      <PanelErrorBoundary>
        {inspectorTopic && (
          <MessageInspector
            topic={inspectorTopic}
            onClose={() => useGraphStore.getState().setInspectorTopic(null)}
          />
        )}
      </PanelErrorBoundary>

      {/* Right-click context menu */}
      {contextMenu && contextMenuItems.length > 0 && (
        <ContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          items={contextMenuItems}
          onClose={() => setContextMenu(null)}
        />
      )}
    </div>
  );
}

function DashRow({ label, value, color, bright }: { label: string; value: string; color: string; bright: boolean }) {
  const textColorMap: Record<string, string> = {
    emerald: "text-emerald-500",
    red: "text-red-500",
    amber: "text-amber-500",
    indigo: bright ? "text-indigo-600" : "text-indigo-400",
    slate: bright ? "text-slate-700" : "text-slate-300",
  };
  return (
    <div className="flex items-center justify-between">
      <span className={`text-[10px] ${bright ? "text-slate-500" : "text-slate-400"}`}>{label}</span>
      <span className={`text-[11px] font-mono font-bold tabular-nums ${textColorMap[color] || textColorMap.slate}`}>{value}</span>
    </div>
  );
}

function FilterChip({ label, count, active, onClick, color, bright }: { label: string; count: number; active: boolean; onClick: () => void; color: string; bright: boolean }) {
  const colorMap: Record<string, string> = {
    indigo: active ? (bright ? "bg-indigo-50 text-indigo-700 border-indigo-200/60" : "bg-indigo-500/15 text-indigo-300 border-indigo-500/30") : (bright ? "bg-slate-50 text-slate-400 border-slate-200/40" : "bg-slate-800/40 text-slate-500 border-slate-700/30"),
    amber: active ? (bright ? "bg-amber-50 text-amber-700 border-amber-200/60" : "bg-amber-500/15 text-amber-300 border-amber-500/30") : (bright ? "bg-slate-50 text-slate-400 border-slate-200/40" : "bg-slate-800/40 text-slate-500 border-slate-700/30"),
    emerald: active ? (bright ? "bg-emerald-50 text-emerald-700 border-emerald-200/60" : "bg-emerald-500/15 text-emerald-300 border-emerald-500/30") : (bright ? "bg-slate-50 text-slate-400 border-slate-200/40" : "bg-slate-800/40 text-slate-500 border-slate-700/30"),
  };
  return (
    <button
      onClick={onClick}
      className={`px-2 py-0.5 rounded-md border text-[10px] font-medium transition-all cursor-pointer ${colorMap[color] || colorMap.indigo} ${!active ? "opacity-50 line-through" : ""}`}
    >
      {label} {count}
    </button>
  );
}
