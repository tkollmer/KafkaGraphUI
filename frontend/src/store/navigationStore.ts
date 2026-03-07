import { create } from "zustand";

export type ActiveView = "pipeline" | "topics" | "consumers" | "brokers";

interface NavigationState {
  activeView: ActiveView;
  sidebarCollapsed: boolean;
  setActiveView: (view: ActiveView) => void;
  toggleSidebar: () => void;
}

export const useNavigationStore = create<NavigationState>((set) => ({
  activeView: "pipeline",
  sidebarCollapsed: false,
  setActiveView: (view) => set({ activeView: view }),
  toggleSidebar: () => set((s) => ({ sidebarCollapsed: !s.sidebarCollapsed })),
}));
