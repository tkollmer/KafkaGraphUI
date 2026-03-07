# Kafka Debug Flow

A real-time Kafka pipeline visualizer and management UI. See your entire event-driven architecture as a live, interactive graph — topics, services, consumer groups, and the data flowing between them.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg)

## Features

**Live Pipeline Graph**
- Real-time visualization of topics, services, consumer groups, and producers
- Auto-detected service topology — services that both consume and produce are shown as pipeline nodes
- Click any node to highlight the full upstream/downstream data flow path
- Animated edges showing active message flow with lag indicators
- Click edges to inspect messages flowing through a topic
- Dagre-based auto-layout with manual re-layout support

**Kafka Management**
- **Topics** — list, create, delete, inspect config, view per-partition details, produce messages
- **Consumer Groups** — list, view members, per-partition lag, reset offsets
- **Brokers** — cluster info, broker list with controller status
- **Message Inspector** — sample recent messages from any topic with JSON formatting

**Operations**
- WebSocket-based real-time updates (configurable poll interval)
- Inactive nodes/edges kept visible (dimmed) instead of disappearing
- SASL/SSL authentication support
- Prometheus metrics at `/metrics`
- Single Docker image, no external dependencies beyond Kafka

## Quick Start

### Connect to your existing Kafka cluster

```yaml
# docker-compose.yml
services:
  kafka-debug-flow:
    image: ghcr.io/YOUR_USERNAME/kafka-debug-flow:latest
    ports:
      - "8080:8080"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: "your-broker:9092"
      SAMPLING_ENABLED: "true"
```

```bash
docker compose up -d
open http://localhost:8080
```

### Try the demo (includes Redpanda + example services)

```bash
git clone https://github.com/YOUR_USERNAME/kafka-debug-flow.git
cd kafka-debug-flow
docker compose -f docker-compose.dev.yml up -d
open http://localhost:8899
```

The demo starts a Redpanda broker, 5 example microservices (order, payment, notification, analytics, inventory), and the UI.

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `POLL_INTERVAL_MS` | `2000` | How often to poll Kafka for updates |
| `SAMPLING_ENABLED` | `false` | Enable message sampling/inspection |
| `MAX_SAMPLE_MESSAGES` | `20` | Messages per sample request |
| `LAG_WARN_THRESHOLD` | `1000` | Lag threshold for visual warnings |
| `SHOW_PRODUCERS` | `false` | Show inferred producer nodes |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

**SASL Authentication:**

| Variable | Default | Description |
|---|---|---|
| `KAFKA_SASL_ENABLED` | `false` | Enable SASL authentication |
| `KAFKA_SASL_USERNAME` | | SASL username |
| `KAFKA_SASL_PASSWORD` | | SASL password |
| `KAFKA_SSL_ENABLED` | `false` | Enable SSL/TLS |

## Architecture

```
┌─────────────────────────────────┐
│  React + ReactFlow + Tailwind   │  Frontend (SPA)
│  Zustand state, WebSocket sub   │
└──────────────┬──────────────────┘
               │ WS + REST
┌──────────────┴──────────────────┐
│  FastAPI + uvicorn              │  Backend
│  ├── WebSocket /ws/graph        │  Real-time graph diffs
│  ├── REST /api/*                │  Management operations
│  ├── KafkaCollector             │  Polls cluster metadata
│  ├── GraphStateBuilder          │  Computes topology diffs
│  ├── KafkaAdmin                 │  Admin operations
│  └── MessageSampler             │  Message inspection
└──────────────┬──────────────────┘
               │ kafka-python-ng
┌──────────────┴──────────────────┐
│  Kafka / Redpanda / MSK / ...   │
└─────────────────────────────────┘
```

## Development

**Prerequisites:** Node.js 20+, Python 3.12+

```bash
# Backend
cd backend
pip install -r requirements.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 uvicorn main:app --reload --port 8080

# Frontend (separate terminal)
cd frontend
npm install
VITE_WS_URL=ws://localhost:8080 npm run dev
```

**Build Docker image:**

```bash
docker build -t kafka-debug-flow .
```

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/topics` | List all topics |
| GET | `/api/topics/{topic}` | Topic detail (config, partitions) |
| POST | `/api/topics` | Create topic |
| DELETE | `/api/topics/{topic}` | Delete topic |
| GET | `/api/topics/{topic}/messages` | Sample messages |
| POST | `/api/topics/{topic}/produce` | Produce a message |
| GET | `/api/consumer-groups` | List consumer groups |
| GET | `/api/consumer-groups/{group}` | Consumer group detail |
| POST | `/api/consumer-groups/{group}/reset-offsets` | Reset offsets |
| GET | `/api/brokers` | List brokers |
| GET | `/api/cluster` | Cluster info |
| GET | `/api/graph/snapshot` | Current graph state |
| GET | `/api/health` | Health check |
| GET | `/metrics` | Prometheus metrics |

## Tech Stack

- **Frontend:** React 19, ReactFlow, Tailwind CSS 4, Zustand, Dagre, TypeScript
- **Backend:** Python 3.12, FastAPI, kafka-python-ng, uvicorn
- **Packaging:** Multi-stage Docker build, single container

## License

MIT
