# Changelog

All notable changes to this project will be documented in this file.

## [1.1.0] - 2026-03-26

### Added

- **Dashboard Enhancements**
  - Cluster health score with circular SVG progress indicator and weighted deduction system
  - Dead Letter Queue (DLQ) auto-detection via regex patterns with per-topic message counts and alerts
  - Quick-action navigation cards

- **Topics Management — New Tabs**
  - **Timeline** — real-time partition offset tracking with write rate per partition charts and balance analysis
  - **Capacity** — storage projections (1h/1d/7d/30d), growth rate estimation, retention impact analysis, partition size distribution with coefficient of variation
  - **Config Diff** — highlights non-default configuration values
  - **Search** — full-text search across message keys and values
  - **Replay** — replay messages from specific offsets or timestamps
  - **Consumers** — shows consumer groups consuming from this topic
  - **Key Analysis** — key cardinality analysis with top keys distribution
  - Leader skew detection in partitions view

- **Consumer Groups — New Features**
  - **Lag Trend** tab — persistent lag trending with localStorage history, per-topic lag breakdown charts, min/max/avg/trend stats
  - **Partition Heatmap** tab — partition lag heatmap across members
  - **Rebalance Timeline** tab — rebalance event tracking
  - Member-to-topic ownership matrix
  - Lag alert rules with regex pattern matching
  - Sparkline lag history in consumer group list
  - Delete consumer group support

- **Schema Registry**
  - Schema evolution lineage — field-level change tracking across versions (added/removed/modified fields with color-coded badges)
  - Schema diff viewer between any two versions
  - Compatibility testing against existing subjects
  - Register new schemas (Avro, JSON Schema, Protobuf)
  - Field table for schema inspection

- **Kafka Connect**
  - Full connector management (list, detail, pause, resume, restart, delete)
  - Type badges (source/sink) with automatic class name detection
  - Inline config editor with save, add/remove keys, secret masking
  - Health overview with status donut chart, task summary, type distribution
  - Task restart support
  - Plugin listing

- **ACLs View**
  - ACL listing with multi-criteria filtering
  - Summary overview with permission donut chart (allow/deny)
  - Resource type and operation distribution badges
  - Permission matrix — principal × resource grid with color-coded cells

- **Quotas View**
  - Quota entry listing with entity type breakdown
  - Summary cards with quota distribution bars
  - Per-entry usage visualization

- **Brokers View Enhancements**
  - Log directory sizes and disk usage
  - Rack-aware visualization
  - Broker configuration viewer
  - Leader distribution analysis

- **Global Features**
  - Command palette (Cmd+K) search across all entities
  - Sidebar favorites and recent items with localStorage persistence
  - Freshness indicators showing data age
  - Auto-refresh with configurable intervals
  - Toast notification system

- **Infrastructure**
  - Helm chart for Kubernetes deployment
  - 652+ backend tests
  - Schema Registry integration (SCHEMA_REGISTRY_URL)
  - Kafka Connect integration (CONNECT_URL)
  - Added `requests` to Python dependencies

### Fixed

- Build errors with TypeScript strict mode (unused imports, type mismatches)
- Topic creation form now properly resets all fields including advanced options

## [1.0.0] - 2026-03-08

### Added

- **Pipeline View**: Real-time Kafka topology visualization with ReactFlow
  - Topic, consumer group, service, and producer node types
  - Animated edge particles showing data flow direction
  - Click-to-highlight connected upstream/downstream paths
  - Metrics panel with per-partition lag breakdown
  - Manual re-layout button (no auto-layout disruption)
  - Inactive nodes shown as dimmed instead of removed

- **Topics Management**: Full CRUD for Kafka topics
  - List all topics with partition count, replication factor, and message stats
  - Topic detail view with configuration, partition info, and message inspector
  - Create and delete topics
  - Produce messages with optional key, headers, and partition targeting

- **Consumer Groups Management**: Monitor and manage consumer groups
  - List all groups with status, member count, and total lag
  - Group detail with member assignments and per-partition offset tracking
  - Reset offsets to earliest or latest (per-topic or all)

- **Brokers View**: Cluster overview
  - Broker list with host, port, rack, and controller status
  - Cluster info cards (cluster ID, controller, topic/partition counts)

- **Message Inspector**: Real-time message sampling
  - Live message stream from any topic
  - JSON syntax highlighting with collapsible keys
  - Partition and offset display

- **Infrastructure**
  - Multi-stage Docker build (Node + Python)
  - Docker Compose for production deployment
  - Docker Compose for development with Kafka
  - GitHub Actions CI (tests, typecheck, Docker build)
  - GitHub Actions Release (versioned Docker publish to GHCR)
  - SASL/SSL authentication support
  - Optional UI basic auth
  - Environment-based configuration with validation
  - Health check endpoint
  - Non-root container user
