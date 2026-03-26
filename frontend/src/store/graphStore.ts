import { create } from "zustand";
import type { Node, Edge } from "@xyflow/react";

export interface GraphConfig {
  showProducers: boolean;
  samplingEnabled: boolean;
  lagWarnThreshold: number;
  animationsEnabled: boolean;
}

interface WsNodeChange {
  id: string;
  type?: string;
  data?: Record<string, unknown>;
  status?: string;
}

interface WsEdgeChange {
  id: string;
  source?: string;
  target?: string;
  data?: Record<string, unknown>;
}

export interface WsMessage {
  type: string;
  ts: number;
  nodes?: {
    added?: WsNodeChange[];
    updated?: WsNodeChange[];
    removed?: string[];
  };
  edges?: {
    added?: WsEdgeChange[];
    updated?: WsEdgeChange[];
    removed?: string[];
  };
  metrics?: Record<string, { msgPerSec: number; totalMessages: number }>;
  config?: Partial<GraphConfig>;
}

interface GraphState {
  nodes: Node[];
  edges: Edge[];
  metrics: Record<string, { msgPerSec: number; totalMessages: number }>;
  config: GraphConfig;
  connectionStatus: "connected" | "reconnecting" | "disconnected";
  selectedNode: string | null;
  selectedEdge: string | null;
  inspectorTopic: string | null;
  searchQuery: string;
  hideSystemTopics: boolean;
  /** Node type visibility filters */
  visibleNodeTypes: Set<string>;
  /** Total message rate across all topics - last 30 samples */
  rateHistory: number[];
  setConnectionStatus: (s: GraphState["connectionStatus"]) => void;
  setSelectedNode: (id: string | null) => void;
  setSelectedEdge: (id: string | null) => void;
  setInspectorTopic: (t: string | null) => void;
  setSearchQuery: (q: string) => void;
  setHideSystemTopics: (v: boolean) => void;
  toggleNodeTypeVisibility: (nodeType: string) => void;
  setConfig: (c: Partial<GraphConfig>) => void;
  applySnapshot: (msg: WsMessage) => void;
  applyDiff: (msg: WsMessage) => void;
}

function mapNodeType(t: string): string {
  switch (t) {
    case "topic": return "topicNode";
    case "service": return "serviceNode";
    case "consumer_group": return "consumerNode";
    case "producer": return "producerNode";
    default: return "topicNode";
  }
}

function wsNodeToRFNode(n: WsNodeChange, existingNodes?: Node[], existingEdges?: Edge[]): Node {
  let position = { x: 0, y: 0 };
  if (existingNodes && existingNodes.length > 0 && existingEdges) {
    // Use Map for O(1) lookups instead of .find()
    const nodeMap = new Map(existingNodes.map((x) => [x.id, x]));
    const connectedEdge = existingEdges.find(
      (e) => e.source === n.id || e.target === n.id
    );
    if (connectedEdge) {
      const connectedId = connectedEdge.source === n.id ? connectedEdge.target : connectedEdge.source;
      const connectedNode = nodeMap.get(connectedId);
      if (connectedNode) {
        position = {
          x: connectedNode.position.x + 300 * (connectedEdge.source === n.id ? -1 : 1),
          y: connectedNode.position.y + (Math.random() - 0.5) * 200,
        };
      }
    }
  }
  return {
    id: n.id,
    type: mapNodeType(n.type || "topic"),
    position,
    data: { ...n.data, status: n.status || "ok" },
  };
}

function wsEdgeToRFEdge(e: WsEdgeChange): Edge {
  return {
    id: e.id,
    source: e.source || "",
    target: e.target || "",
    type: "pipelineEdge",
    data: { ...e.data },
    animated: false,
  };
}

export const useGraphStore = create<GraphState>((set, get) => ({
  nodes: [],
  edges: [],
  metrics: {},
  config: {
    showProducers: true,
    samplingEnabled: true,
    lagWarnThreshold: 1000,
    animationsEnabled: true,
  },
  connectionStatus: "disconnected",
  selectedNode: null,
  selectedEdge: null,
  inspectorTopic: null,
  searchQuery: "",
  hideSystemTopics: true,
  visibleNodeTypes: new Set(["topicNode", "consumerNode", "serviceNode", "producerNode"]),
  rateHistory: [],

  setConnectionStatus: (s) => set({ connectionStatus: s }),
  setSelectedNode: (id) => set({ selectedNode: id, selectedEdge: null }),
  setSelectedEdge: (id) => set({ selectedEdge: id, selectedNode: null }),
  setInspectorTopic: (t) => set({ inspectorTopic: t }),
  setSearchQuery: (q) => set({ searchQuery: q }),
  setHideSystemTopics: (v) => set({ hideSystemTopics: v }),
  toggleNodeTypeVisibility: (nodeType) => set((s) => {
    const next = new Set(s.visibleNodeTypes);
    if (next.has(nodeType)) next.delete(nodeType);
    else next.add(nodeType);
    return { visibleNodeTypes: next };
  }),
  setConfig: (c) => set((s) => ({ config: { ...s.config, ...c } })),

  applySnapshot: (msg) => {
    const existing = get().nodes;
    const posMap = new Map(existing.map((n) => [n.id, n.position]));
    const nodes = (msg.nodes?.added || []).map((n) => {
      const rfNode = wsNodeToRFNode(n);
      const prev = posMap.get(rfNode.id);
      if (prev && (prev.x !== 0 || prev.y !== 0)) {
        rfNode.position = prev;
      }
      return rfNode;
    });
    const edges = (msg.edges?.added || []).map(wsEdgeToRFEdge);
    set({
      nodes,
      edges,
      metrics: msg.metrics || {},
      ...(msg.config ? { config: { ...get().config, ...msg.config } } : {}),
    });
  },

  applyDiff: (msg) => {
    set((state) => {
      // Use Maps for O(1) lookups — critical for 500+ nodes
      const nodeMap = new Map(state.nodes.map((n) => [n.id, n]));
      const edgeMap = new Map(state.edges.map((e) => [e.id, e]));

      // Add new nodes
      for (const n of msg.nodes?.added || []) {
        if (!nodeMap.has(n.id)) {
          const rfNode = wsNodeToRFNode(n, state.nodes, state.edges);
          nodeMap.set(n.id, rfNode);
        }
      }

      // Update existing nodes
      for (const n of msg.nodes?.updated || []) {
        const existing = nodeMap.get(n.id);
        if (existing) {
          nodeMap.set(n.id, {
            ...existing,
            data: { ...existing.data, ...n.data, status: n.status || "ok" },
          });
        }
      }

      // Add new edges
      for (const e of msg.edges?.added || []) {
        if (!edgeMap.has(e.id)) {
          edgeMap.set(e.id, wsEdgeToRFEdge(e));
        }
      }

      // Update existing edges
      for (const e of msg.edges?.updated || []) {
        const existing = edgeMap.get(e.id);
        if (existing) {
          edgeMap.set(e.id, { ...existing, data: { ...existing.data, ...e.data } });
        }
      }

      // Remove nodes
      for (const id of msg.nodes?.removed || []) {
        nodeMap.delete(id);
      }

      // Remove edges
      for (const id of msg.edges?.removed || []) {
        edgeMap.delete(id);
      }

      // Track total msg/sec for rate history
      const mergedMetrics = { ...state.metrics, ...(msg.metrics || {}) };
      const totalRate = Object.values(mergedMetrics).reduce((s, m) => s + (m.msgPerSec || 0), 0);
      const rateHistory = [...state.rateHistory, totalRate].slice(-30);

      return {
        nodes: [...nodeMap.values()],
        edges: [...edgeMap.values()],
        metrics: mergedMetrics,
        rateHistory,
      };
    });
  },
}));
