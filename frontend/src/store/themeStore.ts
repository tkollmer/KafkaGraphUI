import { create } from "zustand";

export type Theme = "dark" | "bright";

interface ThemeState {
  theme: Theme;
  setTheme: (t: Theme) => void;
  toggleTheme: () => void;
}

function getInitialTheme(): Theme {
  const stored = localStorage.getItem("kafka-ui-theme") as Theme;
  if (stored) return stored;
  if (typeof window !== "undefined" && window.matchMedia?.("(prefers-color-scheme: dark)").matches) return "dark";
  return "dark";
}

export const useThemeStore = create<ThemeState>((set) => ({
  theme: getInitialTheme(),
  setTheme: (t) => {
    localStorage.setItem("kafka-ui-theme", t);
    set({ theme: t });
  },
  toggleTheme: () =>
    set((s) => {
      const next = s.theme === "dark" ? "bright" : "dark";
      localStorage.setItem("kafka-ui-theme", next);
      return { theme: next };
    }),
}));
