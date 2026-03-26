import { create } from "zustand";

export type ActiveView = "dashboard" | "pipeline" | "applications" | "topics" | "consumers" | "brokers" | "schemas" | "connectors" | "acls" | "quotas" | "settings";

interface NavigationState {
  activeView: ActiveView;
  sidebarCollapsed: boolean;
  fullscreen: boolean;
  /** For drill-down in application view: which app is expanded */
  expandedApp: string | null;
  /** Deep-link: navigate directly to a specific topic or consumer group */
  pendingTopicName: string | null;
  pendingGroupId: string | null;
  setActiveView: (view: ActiveView) => void;
  toggleSidebar: () => void;
  toggleFullscreen: () => void;
  setExpandedApp: (app: string | null) => void;
  navigateToTopic: (name: string) => void;
  navigateToConsumerGroup: (groupId: string) => void;
  clearPending: () => void;
}

export const useNavigationStore = create<NavigationState>((set) => ({
  activeView: "dashboard",
  sidebarCollapsed: false,
  fullscreen: false,
  expandedApp: null,
  pendingTopicName: null,
  pendingGroupId: null,
  setActiveView: (view) => set({ activeView: view, expandedApp: null }),
  toggleSidebar: () => set((s) => ({ sidebarCollapsed: !s.sidebarCollapsed })),
  toggleFullscreen: () => set((s) => ({ fullscreen: !s.fullscreen })),
  setExpandedApp: (app) => set({ expandedApp: app }),
  navigateToTopic: (name) => set({ activeView: "topics", pendingTopicName: name, pendingGroupId: null, expandedApp: null, fullscreen: false }),
  navigateToConsumerGroup: (groupId) => set({ activeView: "consumers", pendingGroupId: groupId, pendingTopicName: null, expandedApp: null, fullscreen: false }),
  clearPending: () => set({ pendingTopicName: null, pendingGroupId: null }),
}));
