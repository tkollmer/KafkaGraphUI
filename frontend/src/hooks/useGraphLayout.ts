import { useCallback } from "react";
import Dagre from "@dagrejs/dagre";
import type { Node, Edge } from "@xyflow/react";

const NODE_SIZES: Record<string, { w: number; h: number }> = {
  topicNode: { w: 260, h: 130 },
  serviceNode: { w: 260, h: 160 },
  consumerNode: { w: 240, h: 110 },
  producerNode: { w: 240, h: 120 },
};

export function useGraphLayout() {
  // Stable callback — no store dependency. Callers always pass nodes/edges explicitly.
  const layoutNodes = useCallback(
    (inputNodes: Node[], inputEdges: Edge[]): Node[] => {
      if (inputNodes.length === 0) return inputNodes;

      const g = new Dagre.graphlib.Graph();
      g.setDefaultEdgeLabel(() => ({}));
      g.setGraph({
        rankdir: "LR",
        nodesep: 100,
        ranksep: 200,
        marginx: 80,
        marginy: 80,
        acyclicer: "greedy",
        ranker: "network-simplex",
      });

      for (const node of inputNodes) {
        const size = NODE_SIZES[node.type || "topicNode"] || { w: 220, h: 100 };
        g.setNode(node.id, { width: size.w, height: size.h });
      }

      for (const edge of inputEdges) {
        g.setEdge(edge.source, edge.target);
      }

      Dagre.layout(g);

      return inputNodes.map((node) => {
        const pos = g.node(node.id);
        return {
          ...node,
          position: {
            x: pos.x - pos.width / 2,
            y: pos.y - pos.height / 2,
          },
        };
      });
    },
    []
  );

  return { layoutNodes };
}
