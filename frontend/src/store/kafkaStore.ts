import { create } from "zustand";
import { apiFetch } from "../hooks/useApi";

interface TopicSummary {
  name: string;
  partitions: number;
  replicationFactor: number;
  totalMessages: number;
}

interface TopicDetail {
  name: string;
  config: Record<string, string>;
  partitions: {
    partition: number;
    leader: number;
    replicas: number[];
    isr: number[];
    endOffset: number;
  }[];
}

interface ConsumerGroupSummary {
  groupId: string;
  status: string;
  members: number;
  totalLag: number;
  topics: string[];
}

interface ConsumerGroupDetail {
  groupId: string;
  state: string;
  members: {
    memberId: string;
    clientId: string;
    clientHost: string;
    partitions: string[];
  }[];
  offsets: {
    topic: string;
    partition: number;
    currentOffset: number;
    endOffset: number;
    lag: number;
  }[];
}

interface BrokerInfo {
  id: number;
  host: string;
  port: number;
  rack: string | null;
  isController: boolean;
}

interface ClusterInfo {
  clusterId: string;
  controllerId: number;
  brokerCount: number;
  topicCount: number;
  consumerGroupCount: number;
}

interface KafkaState {
  // Topics
  topics: TopicSummary[];
  topicsLoading: boolean;
  selectedTopic: TopicDetail | null;
  topicDetailLoading: boolean;
  topicsLastFetched: number | null;

  // Consumer groups
  consumerGroups: ConsumerGroupSummary[];
  consumerGroupsLoading: boolean;
  selectedConsumerGroup: ConsumerGroupDetail | null;
  consumerGroupDetailLoading: boolean;
  consumerGroupsLastFetched: number | null;

  // Brokers
  brokers: BrokerInfo[];
  brokersLoading: boolean;
  clusterInfo: ClusterInfo | null;
  brokersLastFetched: number | null;

  // Cluster health
  clusterHealth: {
    totalPartitions: number;
    underReplicatedCount: number;
    underReplicated: { topic: string; partition: number; replicas: number; isr: number }[];
    offlinePartitionCount: number;
    offlinePartitions: { topic: string; partition: number }[];
    leaderDistribution: Record<string, number>;
  } | null;

  // Actions
  fetchTopics: () => Promise<void>;
  fetchTopicDetail: (topic: string) => Promise<void>;
  createTopic: (name: string, partitions: number, replicationFactor: number) => Promise<{ success: boolean; error?: string }>;
  deleteTopic: (topic: string) => Promise<{ success: boolean; error?: string }>;
  produceMessage: (topic: string, value: string, key?: string, headers?: Record<string, string>) => Promise<{ success: boolean; error?: string; partition?: number; offset?: number }>;

  fetchConsumerGroups: () => Promise<void>;
  fetchConsumerGroupDetail: (groupId: string) => Promise<void>;
  resetOffsets: (groupId: string, strategy: string, topic?: string, timestamp?: number, offset?: number) => Promise<{ success: boolean; error?: string }>;
  deleteConsumerGroup: (groupId: string) => Promise<{ success: boolean; error?: string }>;

  updateTopicConfig: (topic: string, configs: Record<string, string>) => Promise<{ success: boolean; error?: string }>;
  addTopicPartitions: (topic: string, totalPartitions: number) => Promise<{ success: boolean; error?: string }>;

  fetchBrokers: () => Promise<void>;
  fetchClusterInfo: () => Promise<void>;
  fetchClusterHealth: () => Promise<void>;

  clearSelectedTopic: () => void;
  clearSelectedConsumerGroup: () => void;
}

export const useKafkaStore = create<KafkaState>((set) => ({
  topics: [],
  topicsLoading: false,
  selectedTopic: null,
  topicDetailLoading: false,
  topicsLastFetched: null,

  consumerGroups: [],
  consumerGroupsLoading: false,
  selectedConsumerGroup: null,
  consumerGroupDetailLoading: false,
  consumerGroupsLastFetched: null,

  brokers: [],
  brokersLoading: false,
  clusterInfo: null,
  brokersLastFetched: null,
  clusterHealth: null,

  fetchTopics: async () => {
    set({ topicsLoading: true });
    try {
      const data = await apiFetch<TopicSummary[]>("/api/topics");
      set({ topics: data, topicsLastFetched: Date.now() });
    } catch {
      set({ topics: [] });
    } finally {
      set({ topicsLoading: false });
    }
  },

  fetchTopicDetail: async (topic: string) => {
    set({ topicDetailLoading: true });
    try {
      const data = await apiFetch<TopicDetail>(`/api/topics/${encodeURIComponent(topic)}`);
      set({ selectedTopic: data });
    } catch {
      set({ selectedTopic: null });
    } finally {
      set({ topicDetailLoading: false });
    }
  },

  createTopic: async (name, partitions, replicationFactor) => {
    try {
      const data = await apiFetch<{ success: boolean }>("/api/topics", {
        method: "POST",
        body: JSON.stringify({ name, partitions, replicationFactor }),
      });
      return { success: data.success };
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  deleteTopic: async (topic) => {
    try {
      await apiFetch(`/api/topics/${encodeURIComponent(topic)}`, { method: "DELETE" });
      return { success: true };
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  produceMessage: async (topic, value, key, headers) => {
    try {
      const data = await apiFetch<{ success: boolean; partition: number; offset: number }>(
        `/api/topics/${encodeURIComponent(topic)}/produce`,
        { method: "POST", body: JSON.stringify({ value, key, headers }) }
      );
      return data;
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  fetchConsumerGroups: async () => {
    set({ consumerGroupsLoading: true });
    try {
      const data = await apiFetch<ConsumerGroupSummary[]>("/api/consumer-groups");
      set({ consumerGroups: data, consumerGroupsLastFetched: Date.now() });
    } catch {
      set({ consumerGroups: [] });
    } finally {
      set({ consumerGroupsLoading: false });
    }
  },

  fetchConsumerGroupDetail: async (groupId: string) => {
    set({ consumerGroupDetailLoading: true });
    try {
      const data = await apiFetch<ConsumerGroupDetail>(`/api/consumer-groups/${encodeURIComponent(groupId)}`);
      set({ selectedConsumerGroup: data });
    } catch {
      set({ selectedConsumerGroup: null });
    } finally {
      set({ consumerGroupDetailLoading: false });
    }
  },

  resetOffsets: async (groupId, strategy, topic, timestamp, offset) => {
    try {
      const body: Record<string, unknown> = { strategy, topic };
      if (timestamp !== undefined) body.timestamp = timestamp;
      if (offset !== undefined) body.offset = offset;
      const data = await apiFetch<{ success: boolean }>(`/api/consumer-groups/${encodeURIComponent(groupId)}/reset-offsets`, {
        method: "POST",
        body: JSON.stringify(body),
      });
      return { success: data.success };
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  deleteConsumerGroup: async (groupId) => {
    try {
      await apiFetch(`/api/consumer-groups/${encodeURIComponent(groupId)}`, { method: "DELETE" });
      return { success: true };
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  updateTopicConfig: async (topic, configs) => {
    try {
      const data = await apiFetch<{ success: boolean }>(`/api/topics/${encodeURIComponent(topic)}/config`, {
        method: "PUT",
        body: JSON.stringify({ configs }),
      });
      return { success: data.success };
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  addTopicPartitions: async (topic, totalPartitions) => {
    try {
      const data = await apiFetch<{ success: boolean }>(`/api/topics/${encodeURIComponent(topic)}/partitions`, {
        method: "POST",
        body: JSON.stringify({ totalPartitions }),
      });
      return { success: data.success };
    } catch (e) {
      return { success: false, error: e instanceof Error ? e.message : "Unknown error" };
    }
  },

  fetchBrokers: async () => {
    set({ brokersLoading: true });
    try {
      const data = await apiFetch<BrokerInfo[]>("/api/brokers");
      set({ brokers: data, brokersLastFetched: Date.now() });
    } catch {
      set({ brokers: [] });
    } finally {
      set({ brokersLoading: false });
    }
  },

  fetchClusterInfo: async () => {
    try {
      const data = await apiFetch<ClusterInfo>("/api/cluster");
      set({ clusterInfo: data });
    } catch {
      set({ clusterInfo: null });
    }
  },

  fetchClusterHealth: async () => {
    try {
      const data = await apiFetch<KafkaState["clusterHealth"]>("/api/cluster/health");
      set({ clusterHealth: data });
    } catch {
      set({ clusterHealth: null });
    }
  },

  clearSelectedTopic: () => set({ selectedTopic: null }),
  clearSelectedConsumerGroup: () => set({ selectedConsumerGroup: null }),
}));
