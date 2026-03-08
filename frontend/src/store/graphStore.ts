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
  inspectorTopic: string | null;
  searchQuery: string;
  hideSystemTopics: boolean;
  setConnectionStatus: (s: GraphState["connectionStatus"]) => void;
  setSelectedNode: (id: string | null) => void;
  setInspectorTopic: (t: string | null) => void;
  setSearchQuery: (q: string) => void;
  setHideSystemTopics: (v: boolean) => void;
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
  // Smart positioning: find a connected node and offset from it
  let position = { x: 0, y: 0 };
  if (existingNodes && existingNodes.length > 0 && existingEdges) {
    const connectedEdge = existingEdges.find(
      (e) => e.source === n.id || e.target === n.id
    );
    if (connectedEdge) {
      const connectedId = connectedEdge.source === n.id ? connectedEdge.target : connectedEdge.source;
      const connectedNode = existingNodes.find((x) => x.id === connectedId);
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
  inspectorTopic: null,
  searchQuery: "",
  hideSystemTopics: true,

  setConnectionStatus: (s) => set({ connectionStatus: s }),
  setSelectedNode: (id) => set({ selectedNode: id }),
  setInspectorTopic: (t) => set({ inspectorTopic: t }),
  setSearchQuery: (q) => set({ searchQuery: q }),
  setHideSystemTopics: (v) => set({ hideSystemTopics: v }),
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
      const nodes = [...state.nodes];
      const edges = [...state.edges];

      for (const n of msg.nodes?.added || []) {
        if (!nodes.find((x) => x.id === n.id)) {
          nodes.push(wsNodeToRFNode(n, nodes, edges));
        }
      }
      for (const n of msg.nodes?.updated || []) {
        const idx = nodes.findIndex((x) => x.id === n.id);
        if (idx >= 0) {
          nodes[idx] = {
            ...nodes[idx],
            data: { ...nodes[idx].data, ...n.data, status: n.status || "ok" },
          };
        }
      }
      // No longer remove nodes — backend sends "inactive" status updates instead

      for (const e of msg.edges?.added || []) {
        if (!edges.find((x) => x.id === e.id)) edges.push(wsEdgeToRFEdge(e));
      }
      for (const e of msg.edges?.updated || []) {
        const idx = edges.findIndex((x) => x.id === e.id);
        if (idx >= 0) {
          edges[idx] = { ...edges[idx], data: { ...edges[idx].data, ...e.data } };
        }
      }
      // No longer remove edges — backend sends inactive edge updates instead

      return { nodes, edges, metrics: { ...state.metrics, ...(msg.metrics || {}) } };
    });
  },
}));
