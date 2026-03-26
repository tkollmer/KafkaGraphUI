import { create } from "zustand";

interface FavoritesState {
  favoriteTopics: string[];
  favoriteGroups: string[];
  toggleFavoriteTopic: (name: string) => void;
  toggleFavoriteGroup: (groupId: string) => void;
  isFavoriteTopic: (name: string) => boolean;
  isFavoriteGroup: (groupId: string) => boolean;
}

const STORAGE_KEY = "kafka-debug-favorites";

function loadFromStorage(): { topics: string[]; groups: string[] } {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const data = JSON.parse(raw);
      return { topics: data.topics || [], groups: data.groups || [] };
    }
  } catch { /* ignore */ }
  return { topics: [], groups: [] };
}

function saveToStorage(topics: string[], groups: string[]) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify({ topics, groups }));
}

const initial = loadFromStorage();

export const useFavoritesStore = create<FavoritesState>((set, get) => ({
  favoriteTopics: initial.topics,
  favoriteGroups: initial.groups,

  toggleFavoriteTopic: (name) => {
    const { favoriteTopics, favoriteGroups } = get();
    const next = favoriteTopics.includes(name)
      ? favoriteTopics.filter((t) => t !== name)
      : [...favoriteTopics, name];
    saveToStorage(next, favoriteGroups);
    set({ favoriteTopics: next });
  },

  toggleFavoriteGroup: (groupId) => {
    const { favoriteTopics, favoriteGroups } = get();
    const next = favoriteGroups.includes(groupId)
      ? favoriteGroups.filter((g) => g !== groupId)
      : [...favoriteGroups, groupId];
    saveToStorage(favoriteTopics, next);
    set({ favoriteGroups: next });
  },

  isFavoriteTopic: (name) => get().favoriteTopics.includes(name),
  isFavoriteGroup: (groupId) => get().favoriteGroups.includes(groupId),
}));
