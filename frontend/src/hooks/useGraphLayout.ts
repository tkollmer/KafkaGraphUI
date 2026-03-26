import { useCallback } from "react";
import Dagre from "@dagrejs/dagre";
import type { Node, Edge } from "@xyflow/react";

const NODE_SIZES: Record<string, { w: number; h: number }> = {
  topicNode: { w: 280, h: 140 },
  serviceNode: { w: 280, h: 180 },
  consumerNode: { w: 260, h: 120 },
  producerNode: { w: 260, h: 130 },
  applicationNode: { w: 340, h: 240 },
};

export type LayoutDirection = "LR" | "TB" | "RL" | "BT";

export function useGraphLayout() {
  const layoutNodes = useCallback(
    (inputNodes: Node[], inputEdges: Edge[], direction: LayoutDirection = "LR"): Node[] => {
      if (inputNodes.length === 0) return inputNodes;

      const g = new Dagre.graphlib.Graph();
      g.setDefaultEdgeLabel(() => ({}));

      // Adaptive spacing based on node count for less clutter
      const nodeCount = inputNodes.length;
      const nodesep = nodeCount > 200 ? 60 : nodeCount > 100 ? 80 : nodeCount > 50 ? 100 : 120;
      const ranksep = nodeCount > 200 ? 150 : nodeCount > 100 ? 180 : nodeCount > 50 ? 220 : 260;

      g.setGraph({
        rankdir: direction,
        nodesep,
        ranksep,
        marginx: 100,
        marginy: 100,
        acyclicer: "greedy",
        ranker: "network-simplex",
      });

      for (const node of inputNodes) {
        const size = NODE_SIZES[node.type || "topicNode"] || { w: 240, h: 120 };
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
