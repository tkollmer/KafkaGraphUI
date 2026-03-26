function getBaseUrl(): string {
  try {
    const raw = localStorage.getItem("kafka-debug-clusters");
    if (raw) {
      const data = JSON.parse(raw);
      const clusters = data.clusters || [];
      const activeId = data.activeId || "default";
      const active = clusters.find((c: { id: string }) => c.id === activeId) || clusters[0];
      if (active?.url) return active.url.replace(/\/+$/, "");
    }
  } catch { /* ignore */ }
  return "";
}

export async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const base = getBaseUrl();
  const res = await fetch(`${base}${path}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    let detail = `HTTP ${res.status}`;
    try {
      const j = JSON.parse(text);
      if (j.detail) detail = j.detail;
    } catch {
      if (text) detail = text;
    }
    throw new Error(detail);
  }
  return res.json();
}
