import { create } from "zustand";

export interface Toast {
  id: string;
  message: string;
  type: "success" | "error" | "info";
  duration?: number;
}

export interface EventLogEntry {
  id: string;
  message: string;
  type: "success" | "error" | "info";
  timestamp: number;
}

interface ToastState {
  toasts: Toast[];
  eventLog: EventLogEntry[];
  unreadCount: number;
  addToast: (message: string, type: Toast["type"], duration?: number) => void;
  removeToast: (id: string) => void;
  markAllRead: () => void;
  clearEventLog: () => void;
}

export const useToastStore = create<ToastState>((set) => ({
  toasts: [],
  eventLog: [],
  unreadCount: 0,
  addToast: (message, type, duration = 4000) => {
    const id = `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
    const entry: EventLogEntry = { id, message, type, timestamp: Date.now() };
    set((s) => ({
      toasts: [...s.toasts, { id, message, type, duration }],
      eventLog: [entry, ...s.eventLog].slice(0, 50),
      unreadCount: s.unreadCount + 1,
    }));
    setTimeout(() => {
      set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) }));
    }, duration);
  },
  removeToast: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
  markAllRead: () => set({ unreadCount: 0 }),
  clearEventLog: () => set({ eventLog: [], unreadCount: 0 }),
}));
