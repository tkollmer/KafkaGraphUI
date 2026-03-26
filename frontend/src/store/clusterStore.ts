import { create } from "zustand";

interface ClusterConfig {
  id: string;
  name: string;
  url: string;
  color: string;
}

interface ClusterState {
  clusters: ClusterConfig[];
  activeClusterId: string;
  addCluster: (cluster: Omit<ClusterConfig, "id">) => void;
  removeCluster: (id: string) => void;
  setActiveCluster: (id: string) => void;
  getActiveCluster: () => ClusterConfig;
}

const STORAGE_KEY = "kafka-debug-clusters";
const COLORS = ["#6366f1", "#8b5cf6", "#ec4899", "#f59e0b", "#10b981", "#0ea5e9"];

function loadClusters(): { clusters: ClusterConfig[]; activeId: string } {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const data = JSON.parse(raw);
      return { clusters: data.clusters || [], activeId: data.activeId || "default" };
    }
  } catch { /* ignore */ }
  return { clusters: [], activeId: "default" };
}

function saveClusters(clusters: ClusterConfig[], activeId: string) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify({ clusters, activeId }));
}

const initial = loadClusters();
const defaultCluster: ClusterConfig = { id: "default", name: "Local", url: "", color: COLORS[0] };

export const useClusterStore = create<ClusterState>((set, get) => ({
  clusters: initial.clusters.length > 0 ? initial.clusters : [defaultCluster],
  activeClusterId: initial.activeId,

  addCluster: (cluster) => {
    const id = `cluster-${Date.now()}`;
    const color = COLORS[get().clusters.length % COLORS.length];
    const newCluster = { ...cluster, id, color: cluster.color || color };
    const clusters = [...get().clusters, newCluster];
    saveClusters(clusters, get().activeClusterId);
    set({ clusters });
  },

  removeCluster: (id) => {
    if (id === "default") return;
    const clusters = get().clusters.filter((c) => c.id !== id);
    const activeId = get().activeClusterId === id ? "default" : get().activeClusterId;
    saveClusters(clusters, activeId);
    set({ clusters, activeClusterId: activeId });
  },

  setActiveCluster: (id) => {
    saveClusters(get().clusters, id);
    set({ activeClusterId: id });
    // Reload page to reconnect to new cluster
    window.location.reload();
  },

  getActiveCluster: () => {
    const { clusters, activeClusterId } = get();
    return clusters.find((c) => c.id === activeClusterId) || clusters[0] || defaultCluster;
  },
}));
