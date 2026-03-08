# Changelog

All notable changes to this project will be documented in this file.

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
