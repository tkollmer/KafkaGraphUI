import { Component, useCallback, useEffect, useMemo, type ReactNode } from "react";
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useReactFlow,
  type OnNodesChange,
  type OnEdgesChange,
  type Edge,
  type Node,
  applyNodeChanges,
  applyEdgeChanges,
} from "@xyflow/react";

import { useGraphStore } from "../store/graphStore";
import { useGraphLayout } from "../hooks/useGraphLayout";
import { TopicNode } from "../nodes/TopicNode";
import { ConsumerNode } from "../nodes/ConsumerNode";
import { ProducerNode } from "../nodes/ProducerNode";
import { ServiceNode } from "../nodes/ServiceNode";
import { PipelineEdge } from "../edges/PipelineEdge";
import { MetricsPanel } from "../panels/MetricsPanel";
import { MessageInspector } from "../panels/MessageInspector";

/** Inner error boundary for panels — prevents panel errors from crashing the graph */
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

// Module-level flag — survives component remounts (view switches)
let initialLayoutDone = false;

/** BFS downstream from a node following source→target edges */
function computeDownstreamPath(startId: string, edges: Edge[]): { nodeIds: Set<string>; edgeIds: Set<string> } {
  const nodeIds = new Set<string>([startId]);
  const edgeIds = new Set<string>();
  const queue = [startId];

  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const edge of edges) {
      if (edge.source === current && !nodeIds.has(edge.target)) {
        nodeIds.add(edge.target);
        edgeIds.add(edge.id);
        queue.push(edge.target);
      }
    }
  }

  return { nodeIds, edgeIds };
}

/** BFS upstream from a node following target→source edges */
function computeUpstreamPath(startId: string, edges: Edge[]): { nodeIds: Set<string>; edgeIds: Set<string> } {
  const nodeIds = new Set<string>([startId]);
  const edgeIds = new Set<string>();
  const queue = [startId];

  while (queue.length > 0) {
    const current = queue.shift()!;
    for (const edge of edges) {
      if (edge.target === current && !nodeIds.has(edge.source)) {
        nodeIds.add(edge.source);
        edgeIds.add(edge.id);
        queue.push(edge.source);
      }
    }
  }

  return { nodeIds, edgeIds };
}

/** Full pipeline path: upstream + downstream from a node */
function computeFullPath(startId: string, edges: Edge[]): { nodeIds: Set<string>; edgeIds: Set<string> } {
  const down = computeDownstreamPath(startId, edges);
  const up = computeUpstreamPath(startId, edges);
  return {
    nodeIds: new Set([...down.nodeIds, ...up.nodeIds]),
    edgeIds: new Set([...down.edgeIds, ...up.edgeIds]),
  };
}

export function PipelineView() {
  const nodes = useGraphStore((s) => s.nodes);
  const edges = useGraphStore((s) => s.edges);
  const selectedNode = useGraphStore((s) => s.selectedNode);
  const inspectorTopic = useGraphStore((s) => s.inspectorTopic);
  const searchQuery = useGraphStore((s) => s.searchQuery);
  const hideSystemTopics = useGraphStore((s) => s.hideSystemTopics);

  const { layoutNodes } = useGraphLayout();
  const { fitView } = useReactFlow();

  // Filter nodes
  const filteredNodes = useMemo(() => {
    let result = nodes;
    if (hideSystemTopics) {
      result = result.filter((n) => {
        const label = String(n.data?.label || "");
        return !label.startsWith("__");
      });
    }
    if (searchQuery) {
      const q = searchQuery.toLowerCase();
      const matchingIds = new Set<string>();
      result.forEach((n) => {
        const label = String(n.data?.label || "").toLowerCase();
        if (label.includes(q)) matchingIds.add(n.id);
      });
      edges.forEach((e) => {
        if (matchingIds.has(e.source)) matchingIds.add(e.target);
        if (matchingIds.has(e.target)) matchingIds.add(e.source);
      });
      result = result.filter((n) => matchingIds.has(n.id));
    }
    return result;
  }, [nodes, searchQuery, hideSystemTopics, edges]);

  // Filter edges
  const filteredEdges = useMemo(() => {
    const nodeIds = new Set(filteredNodes.map((n) => n.id));
    return edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));
  }, [edges, filteredNodes]);

  // Compute highlighted path when a node is selected
  const { highlightNodeIds, highlightEdgeIds } = useMemo(() => {
    if (!selectedNode) return { highlightNodeIds: null, highlightEdgeIds: null };
    const path = computeFullPath(selectedNode, filteredEdges);
    return { highlightNodeIds: path.nodeIds, highlightEdgeIds: path.edgeIds };
  }, [selectedNode, filteredEdges]);

  // Inject _dimmed flag into nodes for path highlighting
  const displayNodes: Node[] = useMemo(() => {
    if (!highlightNodeIds) return filteredNodes;
    return filteredNodes.map((n) => ({
      ...n,
      data: { ...n.data, _dimmed: !highlightNodeIds.has(n.id) },
    }));
  }, [filteredNodes, highlightNodeIds]);

  // Inject _dimmed flag into edges for path highlighting
  const displayEdges: Edge[] = useMemo(() => {
    if (!highlightEdgeIds) return filteredEdges;
    return filteredEdges.map((e) => ({
      ...e,
      data: { ...e.data, _dimmed: !highlightEdgeIds.has(e.id) },
    }));
  }, [filteredEdges, highlightEdgeIds]);

  // Layout only once on first data arrival — flag persists across remounts
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

  // Node click: select node + highlight path. Do NOT auto-open MessageInspector.
  const handleNodeClick = useCallback(
    (event: React.MouseEvent, node: { id: string }) => {
      try {
        // Prevent any event propagation or default browser behavior
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
        const state = useGraphStore.getState();
        const sourceNode = state.nodes.find((n) => n.id === edge.source);
        const targetNode = state.nodes.find((n) => n.id === edge.target);

        let topicLabel: string | null = null;
        if (sourceNode?.type === "topicNode") {
          topicLabel = String(sourceNode.data?.label || "");
        } else if (targetNode?.type === "topicNode") {
          topicLabel = String(targetNode.data?.label || "");
        }

        if (topicLabel) {
          state.setInspectorTopic(topicLabel);
        }
        state.setSelectedNode(null);
      } catch (err) {
        console.error("[PipelineView] handleEdgeClick error:", err);
      }
    },
    []
  );

  const handlePaneClick = useCallback(() => {
    try {
      useGraphStore.getState().setSelectedNode(null);
      useGraphStore.getState().setInspectorTopic(null);
    } catch (err) {
      console.error("[PipelineView] handlePaneClick error:", err);
    }
  }, []);

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
        onEdgeClick={handleEdgeClick}
        onPaneClick={handlePaneClick}
        snapToGrid
        snapGrid={[20, 20]}
        colorMode="dark"
        proOptions={{ hideAttribution: true }}
        defaultEdgeOptions={{ type: "pipelineEdge" }}
        minZoom={0.1}
        maxZoom={2}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={32}
          size={1}
          color="rgba(99, 102, 241, 0.08)"
        />
        <Controls position="bottom-right" showInteractive={false} />
        {filteredNodes.length > 15 && (
          <MiniMap
            position="bottom-right"
            style={{ marginBottom: 200 }}
            nodeColor={(node) => {
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
        {inspectorTopic && (
          <MessageInspector
            topic={inspectorTopic}
            onClose={() => useGraphStore.getState().setInspectorTopic(null)}
          />
        )}
      </PanelErrorBoundary>
    </div>
  );
}
