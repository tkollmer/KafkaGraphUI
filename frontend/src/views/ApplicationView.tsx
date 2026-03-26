import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useReactFlow,
  applyNodeChanges,
  applyEdgeChanges,
  type OnNodesChange,
  type OnEdgesChange,
  type Node,
  type Edge,
} from "@xyflow/react";
import Dagre from "@dagrejs/dagre";

import { useGraphStore } from "../store/graphStore";
import { useNavigationStore } from "../store/navigationStore";
import { useThemeStore } from "../store/themeStore";
import { ApplicationNode } from "../nodes/ApplicationNode";
import { TopicNode } from "../nodes/TopicNode";
import { ConsumerNode } from "../nodes/ConsumerNode";
import { ProducerNode } from "../nodes/ProducerNode";
import { ServiceNode } from "../nodes/ServiceNode";
import { PipelineEdge } from "../edges/PipelineEdge";
import { MetricsPanel } from "../panels/MetricsPanel";

const nodeTypes = {
  applicationNode: ApplicationNode,
  topicNode: TopicNode,
  consumerNode: ConsumerNode,
  producerNode: ProducerNode,
  serviceNode: ServiceNode,
};

const edgeTypes = {
  pipelineEdge: PipelineEdge,
};

interface AppGroup {
  id: string;
  label: string;
  services: string[];      // node IDs of services/consumers
  producers: string[];     // node IDs of producers
  internalTopics: string[]; // topics only used within this app
  externalTopics: string[]; // topics shared with other apps
  totalLag: number;
  totalMsgPerSec: number;
  lagWarning: boolean;
}

function groupNodesIntoApps(nodes: Node[], edges: Edge[]): AppGroup[] {
  // Pre-build lookup maps for O(1) access — critical for 500+ nodes
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));
  const nodesByType = new Map<string, Node[]>();
  for (const n of nodes) {
    const t = n.type || "";
    if (!nodesByType.has(t)) nodesByType.set(t, []);
    nodesByType.get(t)!.push(n);
  }

  const serviceNodes = [
    ...(nodesByType.get("serviceNode") || []),
    ...(nodesByType.get("consumerNode") || []),
  ];
  const producerNodes = nodesByType.get("producerNode") || [];

  // Build adjacency maps for edges
  const outgoing = new Map<string, string[]>();
  const incoming = new Map<string, string[]>();
  const edgesByNode = new Map<string, { other: string; edgeId: string }[]>();
  for (const edge of edges) {
    if (!outgoing.has(edge.source)) outgoing.set(edge.source, []);
    outgoing.get(edge.source)!.push(edge.target);
    if (!incoming.has(edge.target)) incoming.set(edge.target, []);
    incoming.get(edge.target)!.push(edge.source);
    if (!edgesByNode.has(edge.source)) edgesByNode.set(edge.source, []);
    edgesByNode.get(edge.source)!.push({ other: edge.target, edgeId: edge.id });
    if (!edgesByNode.has(edge.target)) edgesByNode.set(edge.target, []);
    edgesByNode.get(edge.target)!.push({ other: edge.source, edgeId: edge.id });
  }

  // Group services by their base name prefix
  const appMap = new Map<string, AppGroup>();

  for (const svc of serviceNodes) {
    const label = String(svc.data?.label || "");
    const appName = extractAppName(label);

    if (!appMap.has(appName)) {
      appMap.set(appName, {
        id: `app-${appName}`,
        label: appName,
        services: [],
        producers: [],
        internalTopics: [],
        externalTopics: [],
        totalLag: 0,
        totalMsgPerSec: 0,
        lagWarning: false,
      });
    }
    const app = appMap.get(appName)!;
    app.services.push(svc.id);
    app.totalLag += Number(svc.data?.totalLag || 0);
    if (svc.data?.lagWarning) app.lagWarning = true;
  }

  // Assign producers to apps using adjacency maps
  for (const prod of producerNodes) {
    const connectedTopics = outgoing.get(prod.id) || [];
    let assigned = false;

    for (const [, app] of appMap) {
      const appTopics = new Set<string>();
      for (const svcId of app.services) {
        for (const { other } of edgesByNode.get(svcId) || []) {
          if (nodeMap.get(other)?.type === "topicNode") appTopics.add(other);
        }
      }
      if (connectedTopics.some((t) => appTopics.has(t))) {
        app.producers.push(prod.id);
        assigned = true;
        break;
      }
    }
    if (!assigned && connectedTopics.length > 0) {
      const label = String(prod.data?.label || "unknown").replace(/ producer$/, "");
      const appName = extractAppName(label);
      if (!appMap.has(appName)) {
        appMap.set(appName, {
          id: `app-${appName}`, label: appName, services: [], producers: [],
          internalTopics: [], externalTopics: [], totalLag: 0, totalMsgPerSec: 0, lagWarning: false,
        });
      }
      appMap.get(appName)!.producers.push(prod.id);
    }
  }

  // Classify topics using adjacency maps
  const topicToApps = new Map<string, Set<string>>();
  for (const [appName, app] of appMap) {
    const allNodeIds = new Set([...app.services, ...app.producers]);
    for (const nodeId of allNodeIds) {
      for (const { other } of edgesByNode.get(nodeId) || []) {
        if (nodeMap.get(other)?.type === "topicNode") {
          if (!topicToApps.has(other)) topicToApps.set(other, new Set());
          topicToApps.get(other)!.add(appName);
        }
      }
    }
  }

  for (const [topicId, apps] of topicToApps) {
    if (apps.size === 1) {
      const appName = [...apps][0];
      appMap.get(appName)?.internalTopics.push(topicId);
    } else {
      for (const appName of apps) {
        appMap.get(appName)?.externalTopics.push(topicId);
      }
    }
  }

  // Calculate msg/sec per app using nodeMap
  for (const [, app] of appMap) {
    const allTopics = new Set([...app.internalTopics, ...app.externalTopics]);
    for (const topicId of allTopics) {
      const topic = nodeMap.get(topicId);
      if (topic) app.totalMsgPerSec += Number(topic.data?.msgPerSec || 0);
    }
  }

  return [...appMap.values()].filter((a) => a.services.length > 0 || a.producers.length > 0);
}

function extractAppName(label: string): string {
  return label
    .replace(/[-_](service|svc|consumer|processor|worker|handler|listener|producer)$/i, "")
    .replace(/[-_]\d+$/, "")
    .replace(/-[a-z0-9]{5,}$/, "");
}

function layoutAppNodes(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes;
  const g = new Dagre.graphlib.Graph();
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: "LR", nodesep: 140, ranksep: 250, marginx: 100, marginy: 100 });

  for (const node of nodes) {
    const isApp = node.type === "applicationNode";
    g.setNode(node.id, { width: isApp ? 340 : 280, height: isApp ? 240 : 140 });
  }
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }
  Dagre.layout(g);

  return nodes.map((node) => {
    const pos = g.node(node.id);
    return { ...node, position: { x: pos.x - pos.width / 2, y: pos.y - pos.height / 2 } };
  });
}

type AppSort = "name" | "lag" | "throughput" | "services";

export function ApplicationView() {
  const allNodes = useGraphStore((s) => s.nodes);
  const allEdges = useGraphStore((s) => s.edges);
  const hideSystemTopics = useGraphStore((s) => s.hideSystemTopics);
  const { expandedApp, setExpandedApp } = useNavigationStore();
  const { theme } = useThemeStore();
  const isBright = theme === "bright";
  const { fitView } = useReactFlow();
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [localNodes, setLocalNodes] = useState<Node[]>([]);
  const [localEdges, setLocalEdges] = useState<Edge[]>([]);
  const [appSearch, setAppSearch] = useState("");
  const [appSort, setAppSort] = useState<AppSort>("name");

  // Escape key to go back from drill-down
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        if (selectedNode) {
          setSelectedNode(null);
        } else if (expandedApp) {
          setExpandedApp(null);
        }
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [selectedNode, expandedApp, setExpandedApp]);

  // Filter system topics
  const filteredNodes = useMemo(() => {
    if (!hideSystemTopics) return allNodes;
    return allNodes.filter((n) => !String(n.data?.label || "").startsWith("__"));
  }, [allNodes, hideSystemTopics]);

  const filteredEdges = useMemo(() => {
    const ids = new Set(filteredNodes.map((n) => n.id));
    return allEdges.filter((e) => ids.has(e.source) && ids.has(e.target));
  }, [allEdges, filteredNodes]);

  // Group into applications
  const apps = useMemo(
    () => groupNodesIntoApps(filteredNodes, filteredEdges),
    [filteredNodes, filteredEdges]
  );

  // Build the view: either collapsed (app-level) or expanded (drill-down)
  useEffect(() => {
    if (!expandedApp) {
      // App-level view: show application nodes + shared topic nodes + edges between them
      const appNodes: Node[] = apps.map((app) => ({
        id: app.id,
        type: "applicationNode",
        position: { x: 0, y: 0 },
        data: {
          label: app.label,
          serviceCount: app.services.length,
          consumerCount: 0,
          producerCount: app.producers.length,
          topicCount: app.internalTopics.length + app.externalTopics.length,
          totalLag: app.totalLag,
          totalMsgPerSec: app.totalMsgPerSec,
          lagWarning: app.lagWarning,
        },
      }));

      // Shared/external topics that connect apps
      const sharedTopicIds = new Set<string>();
      const appEdges: Edge[] = [];
      const edgeSet = new Set<string>();

      for (const app of apps) {
        for (const topicId of app.externalTopics) {
          sharedTopicIds.add(topicId);
          // Create edge from app to shared topic or vice versa
          const isProducer = filteredEdges.some((e) =>
            app.services.concat(app.producers).includes(e.source) && e.target === topicId
          );
          const isConsumer = filteredEdges.some((e) =>
            e.source === topicId && app.services.includes(e.target)
          );
          if (isProducer) {
            const eid = `${app.id}->${topicId}`;
            if (!edgeSet.has(eid)) {
              edgeSet.add(eid);
              appEdges.push({
                id: eid,
                source: app.id,
                target: topicId,
                type: "pipelineEdge",
                data: { type: "produces", active: true },
              });
            }
          }
          if (isConsumer) {
            const eid = `${topicId}->${app.id}`;
            if (!edgeSet.has(eid)) {
              edgeSet.add(eid);
              appEdges.push({
                id: eid,
                source: topicId,
                target: app.id,
                type: "pipelineEdge",
                data: { type: "consumes", active: true },
              });
            }
          }
        }
      }

      const sharedTopicNodes: Node[] = [...sharedTopicIds].map((tid) => {
        const orig = filteredNodes.find((n) => n.id === tid);
        return orig || { id: tid, type: "topicNode", position: { x: 0, y: 0 }, data: { label: tid.replace("topic-", "") } };
      });

      const allViewNodes = [...appNodes, ...sharedTopicNodes];
      const laid = layoutAppNodes(allViewNodes, appEdges);
      setLocalNodes(laid);
      setLocalEdges(appEdges);
      setTimeout(() => fitView({ padding: 0.2, duration: 500 }), 200);
    } else {
      // Drill-down view: show the internal topology of the selected app
      const app = apps.find((a) => a.id === expandedApp);
      if (!app) return;

      const internalNodeIds = new Set([
        ...app.services,
        ...app.producers,
        ...app.internalTopics,
        ...app.externalTopics,
      ]);

      const drillNodes = filteredNodes.filter((n) => internalNodeIds.has(n.id));
      const drillNodeIds = new Set(drillNodes.map((n) => n.id));
      const drillEdges = filteredEdges.filter(
        (e) => drillNodeIds.has(e.source) && drillNodeIds.has(e.target)
      );

      // Mark external topics
      const externalSet = new Set(app.externalTopics);
      const markedNodes = drillNodes.map((n) => ({
        ...n,
        data: {
          ...n.data,
          _isExternal: externalSet.has(n.id),
        },
      }));

      const laid = layoutAppNodes(markedNodes, drillEdges);
      setLocalNodes(laid);
      setLocalEdges(drillEdges);
      setTimeout(() => fitView({ padding: 0.2, duration: 500 }), 200);
    }
  }, [expandedApp, apps, filteredNodes, filteredEdges, fitView]);

  const onNodesChange: OnNodesChange = useCallback((changes) => {
    setLocalNodes((ns) => applyNodeChanges(changes, ns));
  }, []);

  const onEdgesChange: OnEdgesChange = useCallback((changes) => {
    setLocalEdges((es) => applyEdgeChanges(changes, es));
  }, []);

  const handleNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      if (node.type === "applicationNode" && !expandedApp) {
        setExpandedApp(node.id);
      } else {
        setSelectedNode(node.id);
      }
    },
    [expandedApp, setExpandedApp]
  );

  const handlePaneClick = useCallback(() => {
    setSelectedNode(null);
  }, []);

  return (
    <div className="flex-1 relative">
      {/* Breadcrumb */}
      {expandedApp && (
        <div className={`absolute top-4 left-4 z-40 flex items-center gap-2 px-4 py-2 rounded-xl border backdrop-blur-xl ${
          isBright
            ? "bg-white/90 border-slate-200/80 text-slate-600"
            : "bg-slate-900/90 border-slate-700/50 text-slate-300"
        }`}>
          <button
            onClick={() => { setExpandedApp(null); setSelectedNode(null); }}
            className={`text-sm font-medium cursor-pointer transition-colors ${
              isBright ? "text-indigo-600 hover:text-indigo-800" : "text-indigo-400 hover:text-indigo-300"
            }`}
          >
            All Applications
          </button>
          <span className={isBright ? "text-slate-300" : "text-slate-600"}>/</span>
          <span className="text-sm font-bold">
            {apps.find((a) => a.id === expandedApp)?.label || expandedApp}
          </span>
        </div>
      )}

      <ReactFlow
        nodes={localNodes}
        edges={localEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        onPaneClick={handlePaneClick}
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
        <Controls position="bottom-right" showInteractive={false} />
        {localNodes.length > 10 && (
          <MiniMap
            position="bottom-right"
            style={{ marginBottom: 200 }}
            nodeColor={(node) => {
              switch (node.type) {
                case "applicationNode": return "#8b5cf6";
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

      {/* Metrics panel for drill-down selected node */}
      {selectedNode && expandedApp && (
        <MetricsPanel
          nodeId={selectedNode}
          onClose={() => setSelectedNode(null)}
          onInspect={() => {}}
        />
      )}

      {/* App summary when no app selected */}
      {!expandedApp && apps.length > 0 && (() => {
        const q = appSearch.toLowerCase();
        const filtered = q ? apps.filter((a) => a.label.toLowerCase().includes(q)) : apps;
        const sorted = [...filtered].sort((a, b) => {
          switch (appSort) {
            case "lag": return b.totalLag - a.totalLag;
            case "throughput": return b.totalMsgPerSec - a.totalMsgPerSec;
            case "services": return b.services.length - a.services.length;
            default: return a.label.localeCompare(b.label);
          }
        });
        return (
          <div className={`absolute left-4 bottom-4 w-[340px] max-h-[70vh] z-40 rounded-2xl border backdrop-blur-xl flex flex-col ${
            isBright
              ? "bg-white/95 border-slate-200/80 shadow-lg"
              : "bg-slate-900/95 border-slate-700/50 shadow-2xl shadow-black/50"
          }`}>
            <div className="p-4 pb-2">
              <div className={`text-[11px] uppercase tracking-wider font-semibold mb-3 ${isBright ? "text-slate-500" : "text-slate-400"}`}>
                Application Overview
              </div>
              <div className="grid grid-cols-3 gap-2 text-center mb-3">
                <div className={`rounded-lg px-2 py-2 ${isBright ? "bg-violet-50" : "bg-violet-500/10"}`}>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Apps</div>
                  <div className={`text-lg font-bold ${isBright ? "text-violet-600" : "text-violet-300"}`}>{apps.length}</div>
                </div>
                <div className={`rounded-lg px-2 py-2 ${isBright ? "bg-indigo-50" : "bg-indigo-500/10"}`}>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Shared Topics</div>
                  <div className={`text-lg font-bold ${isBright ? "text-indigo-600" : "text-indigo-300"}`}>
                    {new Set(apps.flatMap((a) => a.externalTopics)).size}
                  </div>
                </div>
                <div className={`rounded-lg px-2 py-2 ${isBright ? "bg-amber-50" : "bg-amber-500/10"}`}>
                  <div className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-400"}`}>Total Lag</div>
                  <div className={`text-lg font-bold ${
                    apps.some((a) => a.lagWarning) ? "text-red-500" : isBright ? "text-amber-600" : "text-amber-300"
                  }`}>
                    {fmt(apps.reduce((s, a) => s + a.totalLag, 0))}
                  </div>
                </div>
              </div>
              {/* Search + Sort */}
              <div className="flex items-center gap-2 mb-2">
                <input
                  type="text"
                  value={appSearch}
                  onChange={(e) => setAppSearch(e.target.value)}
                  placeholder="Search apps..."
                  className={`flex-1 px-2.5 py-1.5 rounded-lg text-xs border outline-none ${
                    isBright
                      ? "bg-slate-50 border-slate-200 text-slate-800 placeholder:text-slate-400 focus:border-indigo-400"
                      : "bg-slate-800/60 border-slate-700/50 text-white placeholder:text-slate-500 focus:border-indigo-500"
                  }`}
                />
                <select
                  value={appSort}
                  onChange={(e) => setAppSort(e.target.value as AppSort)}
                  className={`px-2 py-1.5 rounded-lg text-[11px] border outline-none cursor-pointer ${
                    isBright
                      ? "bg-slate-50 border-slate-200 text-slate-600"
                      : "bg-slate-800/60 border-slate-700/50 text-slate-300"
                  }`}
                >
                  <option value="name">Name</option>
                  <option value="lag">Lag</option>
                  <option value="throughput">Throughput</option>
                  <option value="services">Services</option>
                </select>
              </div>
            </div>
            {/* App list */}
            <div className="overflow-y-auto px-4 pb-4 space-y-1.5 max-h-[40vh]">
              {sorted.map((app) => (
                <button
                  key={app.id}
                  onClick={() => setExpandedApp(app.id)}
                  className={`w-full text-left px-3 py-2 rounded-xl border text-xs transition-all cursor-pointer ${
                    isBright
                      ? "border-slate-100 hover:border-violet-200 hover:bg-violet-50/50"
                      : "border-slate-800/50 hover:border-violet-500/30 hover:bg-violet-500/5"
                  }`}
                >
                  <div className={`font-medium truncate ${isBright ? "text-slate-700" : "text-slate-200"}`}>{app.label}</div>
                  <div className="flex items-center gap-3 mt-1">
                    <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{app.services.length} svc</span>
                    <span className={`text-[10px] ${isBright ? "text-slate-400" : "text-slate-500"}`}>{fmt(app.totalMsgPerSec)} msg/s</span>
                    {app.totalLag > 0 && (
                      <span className={`text-[10px] font-medium ${app.totalLag > 1000 ? "text-red-500" : "text-amber-500"}`}>lag: {fmt(app.totalLag)}</span>
                    )}
                    {app.lagWarning && <span className="text-[10px] text-red-400">!</span>}
                  </div>
                </button>
              ))}
              {sorted.length === 0 && (
                <div className={`text-center text-xs py-4 ${isBright ? "text-slate-400" : "text-slate-500"}`}>
                  {q ? "No matching apps" : "No applications detected"}
                </div>
              )}
            </div>
          </div>
        );
      })()}
    </div>
  );
}

function fmt(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}
