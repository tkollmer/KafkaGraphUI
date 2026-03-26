import { useCallback, useEffect, lazy, Suspense } from "react";
import { ReactFlowProvider, useReactFlow } from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { useGraphStore } from "./store/graphStore";
import { useNavigationStore, type ActiveView } from "./store/navigationStore";
import { useThemeStore } from "./store/themeStore";
import { useKafkaStore } from "./store/kafkaStore";
import { useWebSocket } from "./hooks/useWebSocket";
import { useGraphLayout } from "./hooks/useGraphLayout";
import { Sidebar } from "./components/Sidebar";
import { ErrorBoundary } from "./components/ErrorBoundary";
import { Toolbar } from "./panels/Toolbar";
import { ToastContainer } from "./components/ToastContainer";
import { CommandPalette } from "./components/CommandPalette";
import { KeyboardShortcuts } from "./components/KeyboardShortcuts";
import { PipelineView } from "./views/PipelineView";

const DashboardView = lazy(() => import("./views/DashboardView").then((m) => ({ default: m.DashboardView })));
const ApplicationView = lazy(() => import("./views/ApplicationView").then((m) => ({ default: m.ApplicationView })));
const TopicsView = lazy(() => import("./views/TopicsView").then((m) => ({ default: m.TopicsView })));
const ConsumerGroupsView = lazy(() => import("./views/ConsumerGroupsView").then((m) => ({ default: m.ConsumerGroupsView })));
const BrokersView = lazy(() => import("./views/BrokersView").then((m) => ({ default: m.BrokersView })));
const SchemaRegistryView = lazy(() => import("./views/SchemaRegistryView").then((m) => ({ default: m.SchemaRegistryView })));
const ConnectorsView = lazy(() => import("./views/ConnectorsView").then((m) => ({ default: m.ConnectorsView })));
const ACLView = lazy(() => import("./views/ACLView").then((m) => ({ default: m.ACLView })));
const QuotasView = lazy(() => import("./views/QuotasView").then((m) => ({ default: m.QuotasView })));
const SettingsView = lazy(() => import("./views/SettingsView").then((m) => ({ default: m.SettingsView })));

function ViewLoader() {
  return (
    <div className="flex-1 flex items-center justify-center">
      <div className="w-6 h-6 border-2 border-indigo-500 border-t-transparent rounded-full animate-spin" />
    </div>
  );
}

function AppContent() {
  const { activeView, fullscreen, toggleFullscreen, setActiveView } = useNavigationStore();
  const { theme, toggleTheme } = useThemeStore();
  const { layoutNodes } = useGraphLayout();
  const { fitView } = useReactFlow();

  useWebSocket();

  // Apply theme class to root element
  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove("theme-bright");
    if (theme === "bright") root.classList.add("theme-bright");
  }, [theme]);

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

  // Global keyboard shortcuts
  useEffect(() => {
    const viewMap: Record<string, ActiveView> = {
      "1": "dashboard", "2": "pipeline", "3": "applications", "4": "topics",
      "5": "consumers", "6": "brokers", "7": "schemas", "8": "connectors", "9": "settings",
    };
    const handler = (e: KeyboardEvent) => {
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      if (viewMap[e.key]) {
        e.preventDefault();
        setActiveView(viewMap[e.key]);
      } else if (e.key === "r" || e.key === "R") {
        e.preventDefault();
        const { fetchTopics, fetchConsumerGroups, fetchBrokers } = useKafkaStore.getState();
        const av = useNavigationStore.getState().activeView;
        if (av === "topics") fetchTopics();
        else if (av === "consumers") fetchConsumerGroups();
        else if (av === "brokers") fetchBrokers();
      } else if (e.key === "f" || e.key === "F") {
        e.preventDefault();
        handleFitView();
      } else if (e.key === "l" || e.key === "L") {
        e.preventDefault();
        handleAutoLayout();
      } else if (e.key === "z" || e.key === "Z") {
        e.preventDefault();
        toggleFullscreen();
      } else if (e.key === "t" || e.key === "T") {
        e.preventDefault();
        toggleTheme();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [handleFitView, handleAutoLayout, toggleFullscreen, toggleTheme, setActiveView]);

  const bgClass = theme === "bright"
    ? "bg-[#f1f5f9]"
    : "bg-[#030712]";

  const connectionStatus = useGraphStore((s) => s.connectionStatus);

  return (
    <div className={`w-screen h-screen flex ${bgClass} transition-colors duration-300`}>
      <ToastContainer />
      <CommandPalette />
      <KeyboardShortcuts />
      {!fullscreen && <Sidebar />}
      <div className="flex-1 h-full flex flex-col overflow-hidden">
        {/* Connection status banner */}
        {connectionStatus === "reconnecting" && (
          <div className={`border-b px-4 py-2 text-center text-xs font-medium flex items-center justify-center gap-2 ${
            theme === "bright"
              ? "bg-amber-50 border-amber-200/60 text-amber-700"
              : "bg-amber-500/20 border-amber-500/30 text-amber-300"
          }`}>
            <div className={`w-3 h-3 border-2 border-t-transparent rounded-full animate-spin ${
              theme === "bright" ? "border-amber-500" : "border-amber-400"
            }`} />
            Reconnecting to Kafka...
          </div>
        )}
        {connectionStatus === "disconnected" && (
          <div className={`border-b px-4 py-2 text-center text-xs font-medium ${
            theme === "bright"
              ? "bg-red-50 border-red-200/60 text-red-700"
              : "bg-red-500/20 border-red-500/30 text-red-300"
          }`}>
            Disconnected from server
          </div>
        )}
        {!fullscreen && <Toolbar onAutoLayout={handleAutoLayout} onFitView={handleFitView} />}
        {activeView === "pipeline" && <PipelineView />}
        <Suspense fallback={<ViewLoader />}>
          {activeView === "dashboard" && <DashboardView />}
          {activeView === "applications" && <ApplicationView />}
          {activeView === "topics" && <TopicsView />}
          {activeView === "consumers" && <ConsumerGroupsView />}
          {activeView === "brokers" && <BrokersView />}
          {activeView === "schemas" && <SchemaRegistryView />}
          {activeView === "connectors" && <ConnectorsView />}
          {activeView === "acls" && <ACLView />}
          {activeView === "quotas" && <QuotasView />}
          {activeView === "settings" && <SettingsView />}
        </Suspense>
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
