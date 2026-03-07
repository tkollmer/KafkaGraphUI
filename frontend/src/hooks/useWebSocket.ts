import { useEffect, useRef } from "react";
import { useGraphStore } from "../store/graphStore";

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    function connect() {
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      const host = import.meta.env.VITE_WS_URL || `${protocol}//${window.location.host}`;
      const url = `${host}/ws/graph`;

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        retryRef.current = 0;
        useGraphStore.getState().setConnectionStatus("connected");
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === "graph_snapshot") {
            useGraphStore.getState().applySnapshot(msg);
          } else if (msg.type === "graph_diff") {
            useGraphStore.getState().applyDiff(msg);
          } else if (msg.type === "queue_overflow") {
            // Request fresh snapshot
            fetch(`${window.location.origin}/api/graph/snapshot`)
              .then((r) => r.json())
              .then((snap) => useGraphStore.getState().applySnapshot({ ...snap, type: "graph_snapshot" }))
              .catch(console.error);
          }
        } catch (e) {
          console.error("Failed to parse WS message:", e);
        }
      };

      ws.onclose = () => {
        useGraphStore.getState().setConnectionStatus("reconnecting");
        const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
        retryRef.current++;
        timerRef.current = setTimeout(connect, delay);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
      wsRef.current?.close();
    };
  }, []); // Empty deps — connect once, never re-run
}
