"""Tests for GraphStateBuilder and diff computation."""

import pytest
from graph_state import GraphStateBuilder, GraphDiff
from kafka_collector import ClusterSnapshot, TopicInfo, ConsumerGroupInfo


def make_snapshot(
    topics: dict[str, TopicInfo] | None = None,
    consumer_groups: dict[str, ConsumerGroupInfo] | None = None,
    active_partitions: dict[str, set[int]] | None = None,
    timestamp: float = 1000.0,
) -> ClusterSnapshot:
    return ClusterSnapshot(
        topics=topics or {},
        consumer_groups=consumer_groups or {},
        active_partitions=active_partitions or {},
        timestamp=timestamp,
    )


def topic(partitions=1, msg_per_sec=0.0, total_messages=0, status="ok", name="t"):
    return TopicInfo(name=name, partitions=partitions, msg_per_sec=msg_per_sec, total_messages=total_messages, status=status)


def consumer_group(members=1, total_lag=0, status="Stable", subscribed_topics=None, per_partition_lag=None, member_client_ids=None, group_id="g"):
    return ConsumerGroupInfo(
        group_id=group_id, members=members, total_lag=total_lag, status=status,
        subscribed_topics=subscribed_topics or [],
        per_partition_lag=per_partition_lag or {},
        member_client_ids=member_client_ids or [],
    )


class TestGraphStateBuilder:
    def test_empty_snapshot(self):
        builder = GraphStateBuilder()
        diff = builder.update(make_snapshot())
        assert diff.is_empty()

    def test_topic_nodes_created(self):
        builder = GraphStateBuilder(show_producers=False)
        snapshot = make_snapshot(topics={
            "orders": topic(name="orders", partitions=3, msg_per_sec=10, total_messages=1000),
            "payments": topic(name="payments", partitions=1, msg_per_sec=5, total_messages=500),
        })
        diff = builder.update(snapshot)
        assert len(diff.nodes_added) == 2
        node_ids = {n["id"] for n in diff.nodes_added}
        assert "topic-orders" in node_ids
        assert "topic-payments" in node_ids

    def test_consumer_group_nodes(self):
        builder = GraphStateBuilder(show_producers=False)
        snapshot = make_snapshot(
            topics={"orders": topic(name="orders")},
            consumer_groups={
                "my-consumer": consumer_group(
                    group_id="my-consumer", members=2, total_lag=100,
                    subscribed_topics=["orders"],
                    per_partition_lag={"orders-0": 100},
                    member_client_ids=["client-1", "client-2"],
                )
            },
        )
        diff = builder.update(snapshot)
        node_types = {n["id"]: n["type"] for n in diff.nodes_added}
        assert "topic-orders" in node_types
        assert "cg-my-consumer" in node_types
        assert node_types["cg-my-consumer"] == "consumer_group"

    def test_edges_created(self):
        builder = GraphStateBuilder(show_producers=False)
        snapshot = make_snapshot(
            topics={"orders": topic(name="orders")},
            consumer_groups={
                "my-consumer": consumer_group(
                    group_id="my-consumer", subscribed_topics=["orders"],
                )
            },
        )
        diff = builder.update(snapshot)
        assert len(diff.edges_added) >= 1
        edge = diff.edges_added[0]
        assert edge["source"] == "topic-orders"
        assert edge["target"] == "cg-my-consumer"

    def test_node_update_on_data_change(self):
        builder = GraphStateBuilder(show_producers=False)
        builder.update(make_snapshot(
            topics={"orders": topic(name="orders", total_messages=100)}
        ))
        diff2 = builder.update(make_snapshot(
            topics={"orders": topic(name="orders", msg_per_sec=5, total_messages=200)}
        ))
        assert len(diff2.nodes_updated) >= 1
        assert any(n["id"] == "topic-orders" for n in diff2.nodes_updated)

    def test_inactive_nodes_on_disappearance(self):
        builder = GraphStateBuilder(show_producers=False)
        builder.update(make_snapshot(
            topics={"orders": topic(name="orders")}
        ))
        diff2 = builder.update(make_snapshot(topics={}))
        assert len(diff2.nodes_removed) == 0
        inactive = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert len(inactive) >= 1

    def test_snapshot_includes_all_nodes(self):
        builder = GraphStateBuilder(show_producers=False)
        builder.update(make_snapshot(topics={
            "orders": topic(name="orders"),
            "payments": topic(name="payments"),
        }))
        snapshot = builder.get_snapshot()
        assert snapshot["type"] == "graph_snapshot"
        assert len(snapshot["nodes"]["added"]) == 2

    def test_diff_to_dict_format(self):
        diff = GraphDiff(
            nodes_added=[{"id": "n1", "type": "topic"}],
            edges_added=[{"id": "e1", "source": "n1", "target": "n2"}],
            timestamp=1000.0,
        )
        d = diff.to_dict()
        assert d["type"] == "graph_diff"
        assert len(d["nodes"]["added"]) == 1
        assert len(d["edges"]["added"]) == 1
        assert d["ts"] == 1000000

    def test_lag_warning_flag(self):
        builder = GraphStateBuilder(lag_warn_threshold=50, show_producers=False)
        snapshot = make_snapshot(
            topics={"orders": topic(name="orders")},
            consumer_groups={
                "my-consumer": consumer_group(
                    group_id="my-consumer", total_lag=100,
                    subscribed_topics=["orders"],
                    per_partition_lag={"orders-0": 100},
                )
            },
        )
        diff = builder.update(snapshot)
        consumer_node = next(n for n in diff.nodes_added if n["id"] == "cg-my-consumer")
        assert consumer_node["data"]["lagWarning"] is True

    def test_metrics_in_diff(self):
        builder = GraphStateBuilder(show_producers=False)
        snapshot = make_snapshot(
            topics={"orders": topic(name="orders", msg_per_sec=10, total_messages=1000)}
        )
        diff = builder.update(snapshot)
        assert "topic-orders" in diff.metrics
        assert diff.metrics["topic-orders"]["msgPerSec"] == 10

    def test_diff_is_empty(self):
        diff = GraphDiff()
        assert diff.is_empty() is True

    def test_diff_not_empty_with_nodes(self):
        diff = GraphDiff(nodes_added=[{"id": "n1"}])
        assert diff.is_empty() is False
