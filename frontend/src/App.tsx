import { useCallback } from "react";
import { ReactFlowProvider, useReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { useGraphStore } from "./store/graphStore";
import { useNavigationStore } from "./store/navigationStore";
import { useWebSocket } from "./hooks/useWebSocket";
import { useGraphLayout } from "./hooks/useGraphLayout";
import { Sidebar } from "./components/Sidebar";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { Toolbar } from "./panels/Toolbar";
import { PipelineView } from "./views/PipelineView";
import { TopicsView } from "./views/TopicsView";
import { ConsumerGroupsView } from "./views/ConsumerGroupsView";
import { BrokersView } from "./views/BrokersView";

function AppContent() {
  const { activeView } = useNavigationStore();
  const darkMode = useGraphStore((s) => s.darkMode);
  const { layoutNodes } = useGraphLayout();
  const { fitView } = useReactFlow();

  useWebSocket();

  const handleAutoLayout = useCallback(() => {
    const state = useGraphStore.getState();
    let ns = state.nodes;
    if (state.hideSystemTopics) {
      ns = ns.filter((n) => !String(n.data?.label || "").startsWith("__"));
    }
    const nodeIds = new Set(ns.map((n) => n.id));
    const es = state.edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));

    const laid = layoutNodes(ns, es);
    useGraphStore.setState({ nodes: laid });
    setTimeout(() => fitView({ padding: 0.15, duration: 500 }), 150);
  }, [layoutNodes, fitView]);

  const handleFitView = useCallback(() => {
    fitView({ padding: 0.15, duration: 400 });
  }, [fitView]);

  return (
    <div className={`w-screen h-screen flex ${darkMode ? "bg-[#030712]" : "bg-slate-100"}`}>
      <Sidebar />
      <div className="flex-1 h-full flex flex-col overflow-hidden">
        <Toolbar onAutoLayout={handleAutoLayout} onFitView={handleFitView} />
        {activeView === "pipeline" && <PipelineView />}
        {activeView === "topics" && <TopicsView />}
        {activeView === "consumers" && <ConsumerGroupsView />}
        {activeView === "brokers" && <BrokersView />}
      </div>
    </div>
  );
}

export default function App() {
  return (
    <ErrorBoundary>
      <ReactFlowProvider>
        <AppContent />
      </ReactFlowProvider>
    </ErrorBoundary>
  );
}
