# Contributing to Kafka Debug Flow

Thanks for your interest in contributing! Here's how to get started.

## Development Setup

### Prerequisites

- Node.js 20+
- Python 3.12+
- Docker & Docker Compose
- A running Kafka cluster (or use the dev compose file)

### Getting Started

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/kafka-debug-flow.git
cd kafka-debug-flow

# Start Kafka + the app in dev mode
docker compose -f docker-compose.dev.yml up -d

# Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8080

# Frontend (separate terminal)
cd frontend
npm install
npm run dev
```

### Running Tests

```bash
# Backend tests
cd backend
python -m pytest tests/ -v

# Frontend typecheck
cd frontend
npx tsc --noEmit

# Frontend build
npm run build
```

## Project Structure

```
kafka-debug-flow/
├── backend/                  # FastAPI + WebSocket server
│   ├── main.py              # App entrypoint, WebSocket handler
│   ├── kafka_collector.py   # Kafka polling & snapshot builder
│   ├── kafka_admin.py       # Admin operations (CRUD, produce, reset)
│   ├── api_routes.py        # REST API endpoints
│   ├── graph_state.py       # Pipeline graph diff engine
│   ├── config.py            # Environment-based configuration
│   └── tests/               # Pytest test suite
├── frontend/                 # React + ReactFlow UI
│   └── src/
│       ├── views/           # Page components (Pipeline, Topics, etc.)
│       ├── nodes/           # Custom ReactFlow node components
│       ├── edges/           # Custom edge components
│       ├── panels/          # Overlay panels (Metrics, Inspector)
│       ├── components/      # Shared UI components
│       ├── store/           # Zustand state stores
│       └── hooks/           # Custom hooks (WebSocket, API)
├── Dockerfile               # Multi-stage production build
├── docker-compose.yml       # Production deployment
└── docker-compose.dev.yml   # Development with Kafka
```

## Guidelines

### Code Style

- **Python**: Follow PEP 8. No linter is enforced but keep it clean.
- **TypeScript**: Strict mode enabled. Run `npx tsc --noEmit` before submitting.
- **CSS**: Use Tailwind utility classes. Follow existing patterns for colors and spacing.

### Commit Messages

Use clear, concise commit messages:

```
Add topic creation form validation
Fix consumer group lag calculation
Update sidebar navigation icons
```

### Pull Requests

1. Create a feature branch from `main`
2. Make your changes with tests where applicable
3. Ensure all checks pass (`pytest`, `tsc --noEmit`, `npm run build`)
4. Open a PR with a clear description of what changed and why

### Adding New Features

- **New API endpoints**: Add to `api_routes.py`, implement in `kafka_admin.py`, add tests in `tests/test_api_routes.py`
- **New views**: Create in `frontend/src/views/`, add to sidebar in `Sidebar.tsx` and view switch in `App.tsx`
- **New node types**: Create in `frontend/src/nodes/`, register in `PipelineView.tsx` nodeTypes

## Reporting Issues

Open an issue with:
- Steps to reproduce
- Expected vs actual behavior
- Browser/OS/Kafka version if relevant
- Console errors or screenshots if available

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).
