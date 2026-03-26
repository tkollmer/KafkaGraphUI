import { useEffect, useRef } from "react";
import { useGraphStore, type WsMessage } from "../store/graphStore";
import { useToastStore } from "../store/toastStore";

/**
 * WebSocket hook with message batching and exponential backoff reconnection.
 * Buffers rapid diff messages and applies them in a single
 * requestAnimationFrame to avoid excessive re-renders.
 */
export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const batchRef = useRef<WsMessage[]>([]);
  const rafRef = useRef<number>(0);
  const unmountedRef = useRef(false);

  useEffect(() => {
    unmountedRef.current = false;

    function flushBatch() {
      const batch = batchRef.current;
      if (batch.length === 0) return;
      batchRef.current = [];

      const store = useGraphStore.getState();
      for (const msg of batch) {
        store.applyDiff(msg);
      }
    }

    function scheduleBatchFlush() {
      if (rafRef.current) return; // already scheduled
      rafRef.current = requestAnimationFrame(() => {
        rafRef.current = 0;
        flushBatch();
      });
    }

    function connect() {
      if (unmountedRef.current) return;

      let host = import.meta.env.VITE_WS_URL || "";
      if (!host) {
        // Check for cluster URL
        try {
          const raw = localStorage.getItem("kafka-debug-clusters");
          if (raw) {
            const data = JSON.parse(raw);
            const clusters = data.clusters || [];
            const activeId = data.activeId || "default";
            const active = clusters.find((c: { id: string }) => c.id === activeId) || clusters[0];
            if (active?.url) {
              const clusterUrl = active.url.replace(/\/+$/, "");
              host = clusterUrl.replace(/^http/, "ws");
            }
          }
        } catch { /* ignore */ }
        if (!host) {
          const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
          host = `${protocol}//${window.location.host}`;
        }
      }
      const url = `${host}/ws/graph`;

      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        const wasReconnecting = retryRef.current > 0;
        retryRef.current = 0;
        useGraphStore.getState().setConnectionStatus("connected");
        if (wasReconnecting) {
          useToastStore.getState().addToast("Reconnected to Kafka server", "success", 3000);
        }
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === "graph_snapshot") {
            // Snapshots are applied immediately
            batchRef.current = [];
            useGraphStore.getState().applySnapshot(msg);
          } else if (msg.type === "graph_diff") {
            // Diffs are batched for performance
            batchRef.current.push(msg);
            scheduleBatchFlush();
          } else if (msg.type === "queue_overflow") {
            batchRef.current = [];
            // Request fresh snapshot
            if (wsRef.current?.readyState === WebSocket.OPEN) {
              wsRef.current.send(JSON.stringify({ type: "request_snapshot" }));
            }
          }
        } catch (e) {
          console.error("Failed to parse WS message:", e);
        }
      };

      ws.onclose = () => {
        if (unmountedRef.current) return;
        const prevStatus = useGraphStore.getState().connectionStatus;
        useGraphStore.getState().setConnectionStatus("reconnecting");
        if (prevStatus === "connected") {
          useToastStore.getState().addToast("Connection lost. Reconnecting...", "error", 5000);
        }
        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 30s max
        const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
        retryRef.current++;
        timerRef.current = setTimeout(connect, delay);
      };

      ws.onerror = () => {
        ws.close();
      };
    }

    connect();

    // Reconnect on page visibility change (tab refocus)
    function handleVisibility() {
      if (document.visibilityState === "visible" && wsRef.current?.readyState !== WebSocket.OPEN) {
        if (timerRef.current) clearTimeout(timerRef.current);
        retryRef.current = 0;
        connect();
      }
    }
    document.addEventListener("visibilitychange", handleVisibility);

    return () => {
      unmountedRef.current = true;
      document.removeEventListener("visibilitychange", handleVisibility);
      if (timerRef.current) clearTimeout(timerRef.current);
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      wsRef.current?.close();
    };
  }, []);
}
