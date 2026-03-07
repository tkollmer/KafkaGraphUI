export async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(path, {
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
