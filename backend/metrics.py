"""Prometheus metrics endpoint."""

from prometheus_client import Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST

kafka_consumer_lag_total = Gauge(
    "kafka_consumer_lag_total",
    "Total consumer lag across all groups and partitions",
    ["group_id"],
)

kafka_topic_msg_rate = Gauge(
    "kafka_topic_msg_rate",
    "Messages per second for each topic",
    ["topic"],
)

ws_connected_clients = Gauge(
    "ws_connected_clients",
    "Number of currently connected WebSocket clients",
)

kafka_topics_total = Gauge(
    "kafka_topics_total",
    "Total number of Kafka topics",
)

kafka_consumer_groups_total = Gauge(
    "kafka_consumer_groups_total",
    "Total number of consumer groups",
)


def update_metrics(snapshot):
    """Update Prometheus gauges from a cluster snapshot."""
    from kafka_collector import ClusterSnapshot

    if not isinstance(snapshot, ClusterSnapshot):
        return

    kafka_topics_total.set(len(snapshot.topics))
    kafka_consumer_groups_total.set(len(snapshot.consumer_groups))

    for topic_name, info in snapshot.topics.items():
        kafka_topic_msg_rate.labels(topic=topic_name).set(info.msg_per_sec)

    for gid, info in snapshot.consumer_groups.items():
        kafka_consumer_lag_total.labels(group_id=gid).set(info.total_lag)


def get_metrics_text() -> tuple[str, str]:
    """Return (metrics_body, content_type) for the /metrics endpoint."""
    return generate_latest().decode("utf-8"), CONTENT_TYPE_LATEST
