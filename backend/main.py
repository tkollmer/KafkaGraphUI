"""Kafka Live Debug UI — FastAPI backend."""

import asyncio
import json
import logging
import secrets
import time
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

from config import config
from startup_validator import validate_and_exit_on_error
from kafka_collector import KafkaCollector, ClusterSnapshot
from graph_state import GraphStateBuilder
from grouping_engine import GroupingEngine
from message_sampler import MessageSampler
from schema_registry import SchemaRegistryClient
from connect_client import ConnectClient
from metrics import update_metrics, get_metrics_text, ws_connected_clients
from kafka_admin import KafkaAdmin
from api_routes import router as api_router

# Configure structured logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
)
logger = logging.getLogger("kafka-ui")

# Validate config at startup
validate_and_exit_on_error(config)

# Core components
collector = KafkaCollector(
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    poll_interval_ms=config.POLL_INTERVAL_MS,
    sasl_enabled=config.KAFKA_SASL_ENABLED,
    sasl_username=config.KAFKA_SASL_USERNAME,
    sasl_password=config.KAFKA_SASL_PASSWORD,
    ssl_enabled=config.KAFKA_SSL_ENABLED,
)

graph_builder = GraphStateBuilder(
    show_producers=True,
    lag_warn_threshold=config.LAG_WARN_THRESHOLD,
)

grouping_engine = GroupingEngine(regex_pattern=config.PRODUCER_GROUP_REGEX)

sampler = MessageSampler(
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    max_messages=config.MAX_SAMPLE_MESSAGES,
    sasl_enabled=config.KAFKA_SASL_ENABLED,
    sasl_username=config.KAFKA_SASL_USERNAME,
    sasl_password=config.KAFKA_SASL_PASSWORD,
    ssl_enabled=config.KAFKA_SSL_ENABLED,
)

# WebSocket client management
ws_clients: dict[str, WebSocket] = {}
ws_queues: dict[str, deque] = {}
start_time = time.time()


async def broadcast_diff(snapshot: ClusterSnapshot):
    """Build graph diff and push to all WS clients.

    Optimizations:
    - JSON serialized once and reused across all clients
    - Disconnected clients cleaned up in batch after send loop
    - Overflow message pre-serialized
    """
    diff = graph_builder.update(snapshot)
    update_metrics(snapshot)

    if diff.is_empty():
        return

    if not ws_clients:
        return  # No clients, skip serialization

    msg = json.dumps(diff.to_dict(), separators=(",", ":"))  # compact JSON

    disconnected = []
    overflow_msg = None  # Lazy-init

    for client_id, ws in ws_clients.items():
        queue = ws_queues.get(client_id)
        if queue is not None and len(queue) >= config.MAX_WS_QUEUE:
            if overflow_msg is None:
                overflow_msg = json.dumps({"type": "queue_overflow", "ts": int(time.time() * 1000)})
            try:
                await ws.send_text(overflow_msg)
                queue.clear()
            except Exception:
                disconnected.append(client_id)
            continue

        try:
            await ws.send_text(msg)
        except Exception:
            disconnected.append(client_id)

    for cid in disconnected:
        ws_clients.pop(cid, None)
        ws_queues.pop(cid, None)
    ws_connected_clients.set(len(ws_clients))


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Connect to Kafka and start polling on startup."""
    logger.info(f"Connecting to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")
    kafka_admin = KafkaAdmin(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        sasl_enabled=config.KAFKA_SASL_ENABLED,
        sasl_username=config.KAFKA_SASL_USERNAME,
        sasl_password=config.KAFKA_SASL_PASSWORD,
        ssl_enabled=config.KAFKA_SSL_ENABLED,
    )
    try:
        await collector.connect()
        kafka_admin.connect()
        app.state.kafka_admin = kafka_admin
        app.state.message_sampler = sampler
        if config.SCHEMA_REGISTRY_URL:
            auth = (config.SCHEMA_REGISTRY_USER, config.SCHEMA_REGISTRY_PASSWORD) if config.SCHEMA_REGISTRY_USER else None
            app.state.schema_registry = SchemaRegistryClient(config.SCHEMA_REGISTRY_URL, auth=auth)
            logger.info(f"Schema Registry configured at {config.SCHEMA_REGISTRY_URL}")
        if config.CONNECT_URL:
            app.state.connect_client = ConnectClient(config.CONNECT_URL)
            logger.info(f"Kafka Connect configured at {config.CONNECT_URL}")
        task = asyncio.create_task(collector.start_polling(callback=broadcast_diff))
        logger.info("Kafka polling started")
        yield
        collector.stop()
        kafka_admin.close()
        task.cancel()
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        yield


app = FastAPI(title="Kafka Live Debug UI", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


# Basic Auth middleware
if config.UI_AUTH_ENABLED:
    from fastapi.security import HTTPBasic, HTTPBasicCredentials
    from starlette.middleware.base import BaseHTTPMiddleware

    class BasicAuthMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next):
            # Skip auth for health and metrics
            if request.url.path in ("/api/health", "/metrics"):
                return await call_next(request)

            auth = request.headers.get("Authorization")
            if not auth or not auth.startswith("Basic "):
                return Response(
                    status_code=401,
                    headers={"WWW-Authenticate": "Basic"},
                    content="Unauthorized",
                )

            import base64
            try:
                decoded = base64.b64decode(auth[6:]).decode("utf-8")
                username, password = decoded.split(":", 1)
            except Exception:
                return Response(status_code=401, headers={"WWW-Authenticate": "Basic"})

            if not (secrets.compare_digest(username, config.UI_USERNAME)
                    and secrets.compare_digest(password, config.UI_PASSWORD)):
                return Response(status_code=401, headers={"WWW-Authenticate": "Basic"})

            return await call_next(request)

    app.add_middleware(BasicAuthMiddleware)


@app.get("/api/health")
async def health():
    snapshot = collector.snapshot
    total_topics = len(snapshot.topics)
    total_groups = len(snapshot.consumer_groups)
    total_lag = sum(g.total_lag for g in snapshot.consumer_groups.values())
    total_msg_rate = sum(t.msg_per_sec for t in snapshot.topics.values())
    graph_nodes = len(graph_builder._nodes)
    graph_edges = len(graph_builder._edges)

    return {
        "status": "ok" if collector.connected else "degraded",
        "kafka_connected": collector.connected,
        "uptime": round(time.time() - start_time, 1),
        "ws_clients": len(ws_clients),
        "topics": total_topics,
        "consumerGroups": total_groups,
        "totalLag": total_lag,
        "totalMsgPerSec": round(total_msg_rate, 1),
        "graphNodes": graph_nodes,
        "graphEdges": graph_edges,
        "pollIntervalMs": config.POLL_INTERVAL_MS,
    }


@app.get("/api/graph/snapshot")
async def graph_snapshot():
    return graph_builder.get_snapshot()


@app.get("/api/config")
async def get_config():
    return {
        "showProducers": config.SHOW_PRODUCERS,
        "samplingEnabled": config.SAMPLING_ENABLED,
        "lagWarnThreshold": config.LAG_WARN_THRESHOLD,
        "animationsEnabled": config.ANIMATIONS_ENABLED,
        "producerGroupRegex": config.PRODUCER_GROUP_REGEX,
        "pollIntervalMs": config.POLL_INTERVAL_MS,
    }


@app.put("/api/config/show-producers")
async def toggle_producers(request: Request):
    body = await request.json()
    config.SHOW_PRODUCERS = body.get("enabled", False)
    graph_builder.show_producers = config.SHOW_PRODUCERS
    return {"showProducers": config.SHOW_PRODUCERS}


@app.put("/api/config/producer-regex")
async def update_regex(request: Request):
    body = await request.json()
    regex = body.get("regex", "")
    import re
    try:
        re.compile(regex)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"Invalid regex: {e}")
    config.PRODUCER_GROUP_REGEX = regex
    grouping_engine.pattern = regex
    return {"regex": regex}


@app.put("/api/config/sampling")
async def toggle_sampling(request: Request):
    body = await request.json()
    config.SAMPLING_ENABLED = body.get("enabled", False)
    return {"samplingEnabled": config.SAMPLING_ENABLED}


@app.put("/api/config/animations")
async def toggle_animations(request: Request):
    body = await request.json()
    config.ANIMATIONS_ENABLED = body.get("enabled", True)
    return {"animationsEnabled": config.ANIMATIONS_ENABLED}


@app.put("/api/config/lag-threshold")
async def update_lag_threshold(request: Request):
    body = await request.json()
    val = int(body.get("threshold", 1000))
    if val < 0:
        raise HTTPException(status_code=400, detail="Threshold must be >= 0")
    config.LAG_WARN_THRESHOLD = val
    graph_builder.lag_warn_threshold = val
    return {"lagWarnThreshold": config.LAG_WARN_THRESHOLD}


@app.put("/api/config/poll-interval")
async def update_poll_interval(request: Request):
    body = await request.json()
    val = int(body.get("intervalMs", 2000))
    if val < 500:
        raise HTTPException(status_code=400, detail="Poll interval must be >= 500ms")
    config.POLL_INTERVAL_MS = val
    collector.poll_interval_ms = val
    return {"pollIntervalMs": config.POLL_INTERVAL_MS}


@app.post("/api/config/preview-grouping")
async def preview_grouping(request: Request):
    body = await request.json()
    client_ids = body.get("clientIds", [])
    regex = body.get("regex", config.PRODUCER_GROUP_REGEX)
    result = grouping_engine.preview_grouping(client_ids, regex)
    return result


@app.get("/metrics")
async def prometheus_metrics():
    body, content_type = get_metrics_text()
    return Response(content=body, media_type=content_type)


@app.websocket("/ws/graph")
async def websocket_graph(websocket: WebSocket):
    await websocket.accept()
    client_id = secrets.token_hex(8)
    ws_clients[client_id] = websocket
    ws_queues[client_id] = deque(maxlen=config.MAX_WS_QUEUE)
    ws_connected_clients.set(len(ws_clients))

    logger.info(f"WS client connected: {client_id}")

    # Send initial snapshot
    try:
        snapshot = graph_builder.get_snapshot()
        # Include config in initial snapshot
        snapshot["config"] = {
            "showProducers": config.SHOW_PRODUCERS,
            "samplingEnabled": config.SAMPLING_ENABLED,
            "lagWarnThreshold": config.LAG_WARN_THRESHOLD,
            "animationsEnabled": config.ANIMATIONS_ENABLED,
        }
        await websocket.send_text(json.dumps(snapshot))
    except Exception as e:
        logger.error(f"Failed to send snapshot to {client_id}: {e}")

    try:
        while True:
            # Keep connection alive, handle client messages
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "request_snapshot":
                    snapshot = graph_builder.get_snapshot()
                    await websocket.send_text(json.dumps(snapshot))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        logger.info(f"WS client disconnected: {client_id}")
    finally:
        ws_clients.pop(client_id, None)
        ws_queues.pop(client_id, None)
        ws_connected_clients.set(len(ws_clients))


# Serve React frontend static files (in production)
import os
from fastapi.responses import FileResponse

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    # Mount assets at /assets for hashed JS/CSS files
    assets_dir = os.path.join(static_dir, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    # Serve other static files (favicon, etc.)
    @app.get("/vite.svg")
    async def vite_svg():
        fpath = os.path.join(static_dir, "vite.svg")
        if os.path.isfile(fpath):
            return FileResponse(fpath)
        return Response(status_code=404)

    # SPA catch-all: serve index.html for any non-API/non-asset path
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        # Try to serve a static file if it exists (with path traversal protection)
        if full_path and ".." not in full_path:
            file_path = os.path.realpath(os.path.join(static_dir, full_path))
            if file_path.startswith(os.path.realpath(static_dir)) and os.path.isfile(file_path):
                return FileResponse(file_path)
        # Otherwise serve index.html for SPA routing
        index_path = os.path.join(static_dir, "index.html")
        if os.path.isfile(index_path):
            return FileResponse(index_path, media_type="text/html")
        return Response(content="Not found", status_code=404)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=config.PORT)
