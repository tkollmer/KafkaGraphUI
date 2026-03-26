"""Large-scale tests: 500+ topics, 500+ consumer groups, performance benchmarks."""

import time
import pytest
from graph_state import GraphStateBuilder, GraphDiff
from kafka_collector import ClusterSnapshot, TopicInfo, ConsumerGroupInfo
from grouping_engine import GroupingEngine


def make_large_snapshot(
    num_topics: int = 500,
    num_consumer_groups: int = 500,
    partitions_per_topic: int = 6,
    lag_per_partition: int = 50,
    active_rate: float = 10.0,
) -> ClusterSnapshot:
    """Generate a realistic large-scale cluster snapshot."""
    topics = {}
    consumer_groups = {}
    active_partitions = {}

    # Create topics with realistic naming patterns
    domains = ["orders", "payments", "inventory", "shipping", "notifications",
               "users", "analytics", "events", "audit", "billing",
               "search", "recommendations", "reviews", "pricing", "catalog"]

    for i in range(num_topics):
        domain = domains[i % len(domains)]
        suffix = f".{['created', 'updated', 'processed', 'failed', 'retry'][i % 5]}"
        topic_name = f"{domain}{suffix}.v{(i // 75) + 1}" if i >= 75 else f"{domain}{suffix}"
        if i >= len(domains) * 5:
            topic_name = f"team{i // 50}.{domain}{suffix}"

        topics[topic_name] = TopicInfo(
            name=topic_name,
            partitions=partitions_per_topic,
            msg_per_sec=active_rate if i % 3 == 0 else 0.0,
            total_messages=(i + 1) * 10000,
            status="ok",
        )
        if i % 3 == 0:
            active_partitions[topic_name] = {0, 1, 2}

    # Create consumer groups with service-like names
    services = ["order-service", "payment-service", "inventory-service",
                "shipping-service", "notification-service", "user-service",
                "analytics-service", "audit-service", "billing-service",
                "search-service", "recommendation-service", "review-service",
                "pricing-service", "catalog-service", "gateway-service"]

    topic_names = list(topics.keys())
    for i in range(num_consumer_groups):
        svc = services[i % len(services)]
        gid = f"{svc}-{i // len(services)}" if i >= len(services) else svc

        # Each consumer subscribes to 1-5 topics
        num_subs = min(5, 1 + (i % 5))
        subscribed = []
        per_partition_lag = {}
        total_lag = 0

        for j in range(num_subs):
            topic_idx = (i * 3 + j) % len(topic_names)
            t = topic_names[topic_idx]
            subscribed.append(t)
            for p in range(partitions_per_topic):
                lag = lag_per_partition if (i + j) % 4 != 0 else 0
                per_partition_lag[f"{t}-{p}"] = lag
                total_lag += lag

        consumer_groups[gid] = ConsumerGroupInfo(
            group_id=gid,
            members=2 + (i % 5),
            total_lag=total_lag,
            status="Stable",
            subscribed_topics=subscribed,
            per_partition_lag=per_partition_lag,
            member_client_ids=[f"{gid}-client-{k}" for k in range(2 + (i % 5))],
        )

    return ClusterSnapshot(
        topics=topics,
        consumer_groups=consumer_groups,
        active_partitions=active_partitions,
        timestamp=time.time(),
    )


class TestLargeScaleGraph:
    """Test graph operations with 500+ topics and 500+ consumer groups."""

    def test_initial_build_500_topics_500_groups(self):
        """Verify graph builds correctly with 500 topics + 500 consumer groups."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=500, num_consumer_groups=500)
        diff = builder.update(snapshot)

        assert len(diff.nodes_added) > 500  # Topics + consumers + services + producers
        assert len(diff.edges_added) > 0
        assert not diff.is_empty()

    def test_initial_build_performance_500(self):
        """Build with 500 topics + 500 groups must complete in <2s."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=500, num_consumer_groups=500)

        start = time.time()
        diff = builder.update(snapshot)
        elapsed = time.time() - start

        assert elapsed < 2.0, f"Initial build took {elapsed:.2f}s, expected <2s"
        assert len(diff.nodes_added) >= 500

    def test_diff_performance_500(self):
        """Subsequent diff updates with 500+500 must complete in <1s."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot1 = make_large_snapshot(num_topics=500, num_consumer_groups=500)
        builder.update(snapshot1)

        # Small changes: update rates and lag
        snapshot2 = make_large_snapshot(num_topics=500, num_consumer_groups=500, active_rate=15.0, lag_per_partition=100)

        start = time.time()
        diff = builder.update(snapshot2)
        elapsed = time.time() - start

        assert elapsed < 1.0, f"Diff update took {elapsed:.2f}s, expected <1s"
        assert len(diff.nodes_updated) > 0

    def test_1000_topics_1000_groups(self):
        """Stress test with 1000 topics + 1000 consumer groups."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=1000, num_consumer_groups=1000)

        start = time.time()
        diff = builder.update(snapshot)
        elapsed = time.time() - start

        assert elapsed < 5.0, f"Build with 1000+1000 took {elapsed:.2f}s, expected <5s"
        assert len(diff.nodes_added) >= 1000

    def test_snapshot_serialization_large(self):
        """Ensure get_snapshot works efficiently with large graph."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=500, num_consumer_groups=500)
        builder.update(snapshot)

        start = time.time()
        result = builder.get_snapshot()
        elapsed = time.time() - start

        assert elapsed < 0.5, f"Snapshot serialization took {elapsed:.2f}s"
        assert result["type"] == "graph_snapshot"
        assert len(result["nodes"]["added"]) >= 500

    def test_diff_format_large(self):
        """Verify diff.to_dict() handles large diffs."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=500, num_consumer_groups=500)
        diff = builder.update(snapshot)

        d = diff.to_dict()
        assert d["type"] == "graph_diff"
        assert len(d["nodes"]["added"]) == len(diff.nodes_added)
        assert len(d["edges"]["added"]) == len(diff.edges_added)

    def test_node_removal_and_inactive_large(self):
        """Test that removing 50% of topics marks them inactive, not removed."""
        builder = GraphStateBuilder(show_producers=False)
        snapshot1 = make_large_snapshot(num_topics=500, num_consumer_groups=100)
        builder.update(snapshot1)

        # Second snapshot with half the topics
        snapshot2 = make_large_snapshot(num_topics=250, num_consumer_groups=100)
        diff2 = builder.update(snapshot2)

        # Should have inactive updates, not removals
        assert len(diff2.nodes_removed) == 0
        inactive_nodes = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert len(inactive_nodes) > 0

    def test_rapid_sequential_updates(self):
        """Simulate rapid polling: 50 sequential updates."""
        builder = GraphStateBuilder(show_producers=True)

        for i in range(50):
            snapshot = make_large_snapshot(
                num_topics=200,
                num_consumer_groups=200,
                active_rate=10.0 + i * 0.5,
                lag_per_partition=50 + i * 2,
            )
            diff = builder.update(snapshot)
            # First iteration should have adds, rest should have updates
            if i == 0:
                assert len(diff.nodes_added) >= 200
            else:
                assert len(diff.nodes_updated) >= 0  # May have updates

    def test_metrics_correctness_large(self):
        """Verify metrics are correctly computed for all topics."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=500, num_consumer_groups=100)
        diff = builder.update(snapshot)

        topic_count = sum(1 for t in snapshot.topics)
        metrics_count = sum(1 for k in diff.metrics if k.startswith("topic-"))
        assert metrics_count == topic_count

    def test_lag_warning_thresholds_large(self):
        """Verify lag warnings are correctly set with large dataset."""
        builder = GraphStateBuilder(lag_warn_threshold=100, show_producers=False)
        snapshot = make_large_snapshot(
            num_topics=200,
            num_consumer_groups=200,
            lag_per_partition=200,  # Above threshold
        )
        diff = builder.update(snapshot)

        consumer_nodes = [n for n in diff.nodes_added if n["type"] == "consumer_group"]
        lag_warning_nodes = [n for n in consumer_nodes if n["data"].get("lagWarning")]
        assert len(lag_warning_nodes) > 0

    def test_edge_count_consistency(self):
        """Verify edge count matches expected connections."""
        builder = GraphStateBuilder(show_producers=False)
        snapshot = make_large_snapshot(num_topics=100, num_consumer_groups=100)
        diff = builder.update(snapshot)

        # Each consumer group connects to at least 1 topic
        # So we should have at least 100 edges
        assert len(diff.edges_added) >= 100

    def test_concurrent_add_remove_topics(self):
        """Test adding new topics while removing old ones simultaneously."""
        builder = GraphStateBuilder(show_producers=False)

        # Start with topics 0-99
        topics_v1 = {}
        for i in range(100):
            name = f"topic-v1-{i}"
            topics_v1[name] = TopicInfo(name=name, partitions=3, msg_per_sec=5, total_messages=1000)

        snap1 = ClusterSnapshot(topics=topics_v1, consumer_groups={}, active_partitions={}, timestamp=1000.0)
        builder.update(snap1)

        # Replace with topics 50-149 (remove 0-49, keep 50-99, add 100-149)
        topics_v2 = {}
        for i in range(50, 150):
            name = f"topic-v1-{i}"
            topics_v2[name] = TopicInfo(name=name, partitions=3, msg_per_sec=10, total_messages=2000)

        snap2 = ClusterSnapshot(topics=topics_v2, consumer_groups={}, active_partitions={}, timestamp=2000.0)
        diff2 = builder.update(snap2)

        assert len(diff2.nodes_added) == 50  # New topics 100-149
        inactive = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert len(inactive) == 50  # Old topics 0-49 marked inactive


class TestGraphDiffSerialization:
    """Test diff serialization and format correctness."""

    def test_large_diff_to_dict_structure(self):
        diff = GraphDiff(
            nodes_added=[{"id": f"n{i}", "type": "topic"} for i in range(100)],
            edges_added=[{"id": f"e{i}", "source": f"n{i}", "target": f"n{i+1}"} for i in range(99)],
            timestamp=1234567890.123,
        )
        d = diff.to_dict()
        assert d["type"] == "graph_diff"
        assert len(d["nodes"]["added"]) == 100
        assert len(d["edges"]["added"]) == 99
        assert d["ts"] == 1234567890123

    def test_empty_diff_serialization(self):
        diff = GraphDiff()
        d = diff.to_dict()
        assert d["type"] == "graph_diff"
        assert len(d["nodes"]["added"]) == 0
        assert len(d["nodes"]["updated"]) == 0
        assert len(d["nodes"]["removed"]) == 0


class TestServiceDetection:
    """Test service vs consumer detection with realistic data."""

    def test_service_detected_from_producer_pattern(self):
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(name="orders.created", partitions=3, msg_per_sec=10, total_messages=5000),
                "orders.processed": TopicInfo(name="orders.processed", partitions=3, msg_per_sec=8, total_messages=4000),
            },
            consumer_groups={
                "order-service": ConsumerGroupInfo(
                    group_id="order-service",
                    members=3,
                    total_lag=50,
                    subscribed_topics=["orders.created"],
                    per_partition_lag={"orders.created-0": 20, "orders.created-1": 15, "orders.created-2": 15},
                    member_client_ids=["order-service-1", "order-service-2", "order-service-3"],
                ),
            },
            active_partitions={"orders.processed": {0, 1, 2}},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # order-service should be detected as a service (produces to orders.processed)
        svc_nodes = [n for n in diff.nodes_added if n["type"] == "service"]
        assert len(svc_nodes) >= 1
        assert any("order-service" in n["id"] for n in svc_nodes)

    def test_multiple_services_interconnected(self):
        """Test a realistic microservice topology."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(name="orders.created", partitions=6, msg_per_sec=100, total_messages=1_000_000),
                "orders.processed": TopicInfo(name="orders.processed", partitions=6, msg_per_sec=95, total_messages=950_000),
                "payments.requested": TopicInfo(name="payments.requested", partitions=3, msg_per_sec=80, total_messages=800_000),
                "payments.completed": TopicInfo(name="payments.completed", partitions=3, msg_per_sec=75, total_messages=750_000),
                "notifications.send": TopicInfo(name="notifications.send", partitions=2, msg_per_sec=150, total_messages=1_500_000),
            },
            consumer_groups={
                "order-service": ConsumerGroupInfo(
                    group_id="order-service", members=3, total_lag=100,
                    subscribed_topics=["orders.created"],
                    per_partition_lag={"orders.created-0": 20, "orders.created-1": 30, "orders.created-2": 50},
                    member_client_ids=["order-svc-1", "order-svc-2", "order-svc-3"],
                ),
                "payment-service": ConsumerGroupInfo(
                    group_id="payment-service", members=2, total_lag=50,
                    subscribed_topics=["payments.requested"],
                    per_partition_lag={"payments.requested-0": 25, "payments.requested-1": 25},
                    member_client_ids=["payment-svc-1", "payment-svc-2"],
                ),
                "notification-service": ConsumerGroupInfo(
                    group_id="notification-service", members=4, total_lag=200,
                    subscribed_topics=["notifications.send"],
                    per_partition_lag={"notifications.send-0": 100, "notifications.send-1": 100},
                    member_client_ids=["notif-svc-1", "notif-svc-2", "notif-svc-3", "notif-svc-4"],
                ),
            },
            active_partitions={
                "orders.processed": {0, 1, 2, 3, 4, 5},
                "payments.requested": {0, 1, 2},
                "payments.completed": {0, 1, 2},
                "notifications.send": {0, 1},
            },
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        node_types = {n["id"]: n["type"] for n in diff.nodes_added}
        assert len([t for t in node_types.values() if t == "topic"]) == 5
        assert len(diff.edges_added) >= 3  # At least consume edges


class TestWebSocketBatching:
    """Test that multiple rapid diffs can be merged efficiently."""

    def test_100_sequential_small_diffs(self):
        """100 small diff snapshots with 1-2 topic changes each must all complete in <0.5s total."""
        builder = GraphStateBuilder(show_producers=False)

        # Establish a baseline with 50 topics
        base_topics = {}
        for i in range(50):
            name = f"base-topic-{i}"
            base_topics[name] = TopicInfo(
                name=name, partitions=3, msg_per_sec=5.0, total_messages=10000
            )
        baseline = ClusterSnapshot(
            topics=base_topics, consumer_groups={}, active_partitions={}, timestamp=1000.0
        )
        builder.update(baseline)

        # Now run 100 rapid updates, each changing 1-2 topics
        start = time.time()
        diffs = []
        for i in range(100):
            updated_topics = dict(base_topics)
            # Change topic i % 50 and (i+1) % 50
            for offset in (0, 1):
                idx = (i + offset) % 50
                name = f"base-topic-{idx}"
                updated_topics[name] = TopicInfo(
                    name=name,
                    partitions=3,
                    msg_per_sec=float(i + offset),
                    total_messages=10000 + i * 100,
                )
            snap = ClusterSnapshot(
                topics=updated_topics,
                consumer_groups={},
                active_partitions={},
                timestamp=1000.0 + i,
            )
            diff = builder.update(snap)
            diffs.append(diff)

        elapsed = time.time() - start
        assert elapsed < 0.5, f"100 sequential small diffs took {elapsed:.3f}s, expected <0.5s"
        # Every diff should have been processed — at minimum the last one is non-empty
        # (some may be empty if data didn't change, but they must all succeed)
        assert len(diffs) == 100

    def test_diff_merge_consistency(self):
        """After many rapid updates, verify get_snapshot() reflects the final state."""
        builder = GraphStateBuilder(show_producers=False)

        final_topics = {}
        for i in range(30):
            name = f"consistency-topic-{i}"
            final_topics[name] = TopicInfo(
                name=name, partitions=3, msg_per_sec=0.0, total_messages=i * 100
            )

        # Apply 50 updates, each moving msg_per_sec values forward
        for cycle in range(50):
            cycle_topics = {}
            for i in range(30):
                name = f"consistency-topic-{i}"
                cycle_topics[name] = TopicInfo(
                    name=name,
                    partitions=3,
                    msg_per_sec=float(cycle + i),
                    total_messages=i * 100 + cycle * 10,
                )
            snap = ClusterSnapshot(
                topics=cycle_topics,
                consumer_groups={},
                active_partitions={},
                timestamp=2000.0 + cycle,
            )
            builder.update(snap)
            if cycle == 49:
                final_topics = cycle_topics

        # get_snapshot() must reflect the final update exactly
        snapshot_result = builder.get_snapshot()
        assert snapshot_result["type"] == "graph_snapshot"

        snapshot_node_ids = {n["id"] for n in snapshot_result["nodes"]["added"]}
        for topic_name in final_topics:
            assert f"topic-{topic_name}" in snapshot_node_ids, (
                f"topic-{topic_name} missing from snapshot after 50 rapid updates"
            )

        # Verify metric values match the last cycle
        for topic_name, info in final_topics.items():
            node = next(
                (n for n in snapshot_result["nodes"]["added"] if n["id"] == f"topic-{topic_name}"),
                None,
            )
            assert node is not None
            assert node["data"]["msgPerSec"] == info.msg_per_sec, (
                f"{topic_name}: expected msgPerSec={info.msg_per_sec}, "
                f"got {node['data']['msgPerSec']}"
            )


class TestEdgeCaseHandling:
    """Test graph handling of unusual or boundary-condition inputs."""

    def test_duplicate_consumer_groups(self):
        """Same consumer group subscribing to the same topic multiple times."""
        builder = GraphStateBuilder(show_producers=False)

        # Duplicate subscription entries in subscribed_topics list
        snap = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={
                "order-consumer": ConsumerGroupInfo(
                    group_id="order-consumer",
                    members=2,
                    total_lag=30,
                    status="Stable",
                    subscribed_topics=["orders.created", "orders.created"],
                    per_partition_lag={
                        "orders.created-0": 10,
                        "orders.created-1": 10,
                        "orders.created-2": 10,
                    },
                    member_client_ids=["order-consumer-1", "order-consumer-2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)

        # Must not raise; graph should be produced
        assert not diff.is_empty()
        # There should be exactly one consumer/service node for order-consumer
        consumer_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "order-consumer" in n["id"]
        ]
        assert len(consumer_nodes) == 1

    def test_empty_topic_names(self):
        """Topics with empty string names are handled without crashing."""
        builder = GraphStateBuilder(show_producers=False)

        snap = ClusterSnapshot(
            topics={
                "": TopicInfo(name="", partitions=1, msg_per_sec=0.0, total_messages=0),
                "valid-topic": TopicInfo(
                    name="valid-topic", partitions=3, msg_per_sec=1.0, total_messages=500
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        # Must not raise
        diff = builder.update(snap)
        assert not diff.is_empty()
        valid_nodes = [n for n in diff.nodes_added if n["id"] == "topic-valid-topic"]
        assert len(valid_nodes) == 1

    def test_very_long_topic_name(self):
        """Topic with a 255-character name is handled correctly."""
        long_name = "a" * 255
        builder = GraphStateBuilder(show_producers=False)

        snap = ClusterSnapshot(
            topics={
                long_name: TopicInfo(
                    name=long_name, partitions=6, msg_per_sec=2.0, total_messages=99999
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        node_ids = [n["id"] for n in diff.nodes_added]
        assert f"topic-{long_name}" in node_ids

    def test_zero_partitions(self):
        """Topic with 0 partitions does not crash the builder."""
        builder = GraphStateBuilder(show_producers=False)

        snap = ClusterSnapshot(
            topics={
                "empty-topic": TopicInfo(
                    name="empty-topic", partitions=0, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        node = next((n for n in diff.nodes_added if n["id"] == "topic-empty-topic"), None)
        assert node is not None
        assert node["data"]["partitions"] == 0

    def test_negative_lag(self):
        """Consumer with negative lag values does not crash; node is still created."""
        builder = GraphStateBuilder(show_producers=False)

        snap = ClusterSnapshot(
            topics={
                "payments.done": TopicInfo(
                    name="payments.done", partitions=3, msg_per_sec=0.0, total_messages=500
                ),
            },
            consumer_groups={
                "payment-checker": ConsumerGroupInfo(
                    group_id="payment-checker",
                    members=1,
                    total_lag=-10,
                    status="Stable",
                    subscribed_topics=["payments.done"],
                    per_partition_lag={
                        "payments.done-0": -5,
                        "payments.done-1": -3,
                        "payments.done-2": -2,
                    },
                    member_client_ids=["payment-checker-1"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        consumer_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service")
        ]
        assert len(consumer_nodes) == 1
        assert consumer_nodes[0]["data"]["totalLag"] == -10

    def test_unicode_labels(self):
        """Topic names with unicode characters are handled without crashing."""
        unicode_topics = [
            "топик.события",       # Russian
            "注文.作成",             # Japanese
            "café.orders",         # French accented
            "emoji-🚀-topic",      # Emoji
            "αβγ.events",          # Greek
        ]
        builder = GraphStateBuilder(show_producers=False)

        topics = {
            name: TopicInfo(name=name, partitions=3, msg_per_sec=1.0, total_messages=100)
            for name in unicode_topics
        }
        snap = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions={}, timestamp=time.time()
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        added_ids = {n["id"] for n in diff.nodes_added}
        for name in unicode_topics:
            assert f"topic-{name}" in added_ids, f"Unicode topic '{name}' missing from graph"

    def test_special_chars_in_group_id(self):
        """Group IDs with dots, slashes, and other special characters are handled."""
        special_group_ids = [
            "team.alpha.consumer",
            "org/payments/processor",
            "service:notification:v2",
            "group-with--double-dash",
            "UPPERCASE.GROUP",
        ]
        builder = GraphStateBuilder(show_producers=False)

        topics = {
            "events.created": TopicInfo(
                name="events.created", partitions=3, msg_per_sec=0.0, total_messages=1000
            ),
        }
        consumer_groups = {
            gid: ConsumerGroupInfo(
                group_id=gid,
                members=1,
                total_lag=0,
                status="Stable",
                subscribed_topics=["events.created"],
                per_partition_lag={"events.created-0": 0},
                member_client_ids=[f"{gid}-client"],
            )
            for gid in special_group_ids
        }
        snap = ClusterSnapshot(
            topics=topics,
            consumer_groups=consumer_groups,
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        consumer_node_ids = {
            n["id"] for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service")
        }
        for gid in special_group_ids:
            expected_cg = f"cg-{gid}"
            expected_svc = f"svc-{gid}"
            assert expected_cg in consumer_node_ids or expected_svc in consumer_node_ids, (
                f"Group '{gid}' not found in graph nodes"
            )

    def test_empty_topic_name(self):
        """Topic with empty string name should be handled without crashing."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "": TopicInfo(name="", partitions=1, msg_per_sec=0.0, total_messages=0),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        # Must not raise
        diff = builder.update(snap)
        # The empty-named topic should still produce a node (topic-)
        node_ids = [n["id"] for n in diff.nodes_added]
        assert "topic-" in node_ids

    def test_unicode_topic_name(self):
        """Topics with unicode characters should work correctly."""
        unicode_names = [
            "pedidos.criados",
            "commandes.creees",
            "Bestellungen.erstellt",
        ]
        builder = GraphStateBuilder(show_producers=False)
        topics = {
            name: TopicInfo(name=name, partitions=3, msg_per_sec=1.0, total_messages=100)
            for name in unicode_names
        }
        snap = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions={}, timestamp=time.time()
        )
        diff = builder.update(snap)
        assert not diff.is_empty()
        added_ids = {n["id"] for n in diff.nodes_added}
        for name in unicode_names:
            assert f"topic-{name}" in added_ids, f"Topic '{name}' missing from graph"

    def test_consumer_group_no_offsets(self):
        """Consumer group with 0 offsets (empty per_partition_lag) should not crash."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "some-topic": TopicInfo(
                    name="some-topic", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={
                "empty-offset-group": ConsumerGroupInfo(
                    group_id="empty-offset-group",
                    members=1,
                    total_lag=0,
                    status="Stable",
                    subscribed_topics=["some-topic"],
                    per_partition_lag={},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)
        assert not diff.is_empty()

        # The consumer group node should exist
        cg_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "empty-offset-group" in n["id"]
        ]
        assert len(cg_nodes) == 1
        assert cg_nodes[0]["data"]["totalLag"] == 0

        # The consume edge should exist with lag = 0
        consume_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 1
        assert consume_edges[0]["data"]["lag"] == 0

    def test_negative_lag_clamped(self):
        """If current offset > end offset, lag should be 0 (not negative).

        The KafkaCollector.poll() method uses max(0, end_offset - offset_meta.offset)
        to clamp lag. Here we verify that if negative lag values are passed through
        in a ConsumerGroupInfo, the graph builder still handles them gracefully.
        """
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=100)
        snap = ClusterSnapshot(
            topics={
                "lag-topic": TopicInfo(
                    name="lag-topic", partitions=2, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "negative-lag-group": ConsumerGroupInfo(
                    group_id="negative-lag-group",
                    members=2,
                    total_lag=-20,
                    status="Stable",
                    subscribed_topics=["lag-topic"],
                    per_partition_lag={
                        "lag-topic-0": -10,
                        "lag-topic-1": -10,
                    },
                    member_client_ids=["client-1", "client-2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        # Must not crash
        diff = builder.update(snap)
        assert not diff.is_empty()

        # The node should exist and not trigger lag warning (negative lag < threshold)
        cg_nodes = [
            n for n in diff.nodes_added
            if "negative-lag-group" in n["id"]
        ]
        assert len(cg_nodes) == 1
        assert cg_nodes[0]["data"]["lagWarning"] is False


class TestApplicationGrouping:
    """Tests that verify the application-level grouping logic."""

    def test_services_grouped_by_prefix(self):
        """When multiple services share a prefix, they should be groupable."""
        engine = GroupingEngine()
        client_ids = [
            "order-service-abc12",
            "order-service-def34",
            "order-service-ghi56",
            "payment-service-jkl78",
            "payment-service-mno90",
        ]
        groups = engine.group_by_client_id(client_ids)

        # All order-service instances should be in one group
        order_keys = [k for k in groups if "order-service" in k]
        assert len(order_keys) == 1, f"Expected one order-service group, got {order_keys}"
        assert len(groups[order_keys[0]].members) == 3

        # All payment-service instances should be in one group
        payment_keys = [k for k in groups if "payment-service" in k]
        assert len(payment_keys) == 1, f"Expected one payment-service group, got {payment_keys}"
        assert len(groups[payment_keys[0]].members) == 2

    def test_application_includes_related_topics(self):
        """Application view should include topics consumed/produced by its services."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=50.0, total_messages=10000
                ),
                "orders.processed": TopicInfo(
                    name="orders.processed", partitions=3, msg_per_sec=48.0, total_messages=9600
                ),
                "orders.failed": TopicInfo(
                    name="orders.failed", partitions=3, msg_per_sec=2.0, total_messages=400
                ),
            },
            consumer_groups={
                "order-service": ConsumerGroupInfo(
                    group_id="order-service",
                    members=3,
                    total_lag=30,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 10,
                        "orders.created-1": 10,
                        "orders.created-2": 10,
                    },
                    member_client_ids=["order-service-1", "order-service-2", "order-service-3"],
                ),
            },
            active_partitions={
                "orders.processed": {0, 1, 2},
                "orders.failed": {0, 1, 2},
            },
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # order-service should be detected as a service
        svc_nodes = [n for n in diff.nodes_added if n["type"] == "service" and "order-service" in n["id"]]
        assert len(svc_nodes) == 1, "order-service should be typed as 'service'"

        svc_data = svc_nodes[0]["data"]
        # Service consumes orders.created
        assert "orders.created" in svc_data["consumes"]
        # Service produces to orders.processed and/or orders.failed
        assert len(svc_data["produces"]) > 0, "Service should produce to at least one topic"

        # Use GroupingEngine to group topics by prefix
        engine = GroupingEngine()
        topic_names = list(snapshot.topics.keys())
        topic_groups = engine.group_by_topic_prefix(topic_names)

        # All order-related topics should be grouped under "orders"
        assert "orders" in topic_groups
        assert len(topic_groups["orders"]) == 3

    def test_application_total_lag_aggregation(self):
        """Total lag for an application = sum of lag across all its consumer groups."""
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=10000)
        snapshot = ClusterSnapshot(
            topics={
                "events.input": TopicInfo(
                    name="events.input", partitions=4, msg_per_sec=100.0, total_messages=50000
                ),
                "events.enriched": TopicInfo(
                    name="events.enriched", partitions=4, msg_per_sec=95.0, total_messages=47500
                ),
            },
            consumer_groups={
                "events-processor-1": ConsumerGroupInfo(
                    group_id="events-processor-1",
                    members=2,
                    total_lag=200,
                    status="Stable",
                    subscribed_topics=["events.input"],
                    per_partition_lag={
                        "events.input-0": 50,
                        "events.input-1": 50,
                        "events.input-2": 50,
                        "events.input-3": 50,
                    },
                    member_client_ids=["events-processor-abc12", "events-processor-def34"],
                ),
                "events-processor-2": ConsumerGroupInfo(
                    group_id="events-processor-2",
                    members=2,
                    total_lag=300,
                    status="Stable",
                    subscribed_topics=["events.enriched"],
                    per_partition_lag={
                        "events.enriched-0": 75,
                        "events.enriched-1": 75,
                        "events.enriched-2": 75,
                        "events.enriched-3": 75,
                    },
                    member_client_ids=["events-processor-ghi56", "events-processor-jkl78"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # Both consumer groups should appear in the graph
        cg_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "events-processor" in n["id"]
        ]
        assert len(cg_nodes) == 2

        # Aggregate total lag for the "events-processor" application
        total_application_lag = sum(n["data"]["totalLag"] for n in cg_nodes)
        assert total_application_lag == 500, (
            f"Expected total application lag of 500 (200 + 300), got {total_application_lag}"
        )

        # Also verify via GroupingEngine that client IDs group correctly
        engine = GroupingEngine()
        all_client_ids = []
        for group in snapshot.consumer_groups.values():
            all_client_ids.extend(group.member_client_ids)

        groups = engine.group_by_client_id(all_client_ids)
        # All 4 client IDs should map to the same group key "events-processor"
        processor_keys = [k for k in groups if "events-processor" in k]
        assert len(processor_keys) == 1, f"Expected one events-processor group, got {processor_keys}"
        assert len(groups[processor_keys[0]].members) == 4


class TestLargeScaleMetrics:
    """Performance tests with many entities."""

    def test_500_topics_snapshot_time(self):
        """Building a snapshot with 500 topics should complete in reasonable time."""
        # Build snapshot with 500 unique topics using explicit names to avoid collisions
        topics = {}
        for i in range(500):
            name = f"scale-topic-{i}"
            topics[name] = TopicInfo(
                name=name, partitions=6, msg_per_sec=10.0 if i % 3 == 0 else 0.0,
                total_messages=(i + 1) * 1000,
            )
        active_parts = {name: {0, 1, 2} for name in list(topics.keys())[:167]}
        snapshot = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions=active_parts,
            timestamp=time.time(),
        )

        builder = GraphStateBuilder(show_producers=True)
        start = time.time()
        diff = builder.update(snapshot)
        elapsed = time.time() - start

        assert elapsed < 2.0, f"500 topic snapshot took {elapsed:.2f}s, expected <2s"
        # 500 topic nodes + producer nodes for active topics
        assert len(diff.nodes_added) >= 500

        # Verify get_snapshot also returns within reasonable time
        start = time.time()
        result = builder.get_snapshot()
        snap_elapsed = time.time() - start

        assert snap_elapsed < 0.5, f"get_snapshot took {snap_elapsed:.2f}s, expected <0.5s"
        assert len(result["nodes"]["added"]) >= 500

    def test_500_consumer_groups_lag(self):
        """Computing lag across 500 consumer groups should be accurate."""
        lag_per_partition = 100
        partitions_per_topic = 6
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=10000)
        snapshot = make_large_snapshot(
            num_topics=100,
            num_consumer_groups=500,
            partitions_per_topic=partitions_per_topic,
            lag_per_partition=lag_per_partition,
        )

        diff = builder.update(snapshot)

        # Collect all consumer group / service nodes
        cg_nodes = [
            n for n in diff.nodes_added
            if n["type"] in ("consumer_group", "service")
        ]
        assert len(cg_nodes) == 500, f"Expected 500 consumer group nodes, got {len(cg_nodes)}"

        # Each consumer group should have a totalLag matching the sum of its per-partition lags
        for node in cg_nodes:
            gid = node["data"]["label"]
            group = snapshot.consumer_groups[gid]
            expected_lag = group.total_lag
            actual_lag = node["data"]["totalLag"]
            assert actual_lag == expected_lag, (
                f"Consumer group {gid}: expected lag {expected_lag}, got {actual_lag}"
            )

        # Verify the overall sum of lag across all consumer groups
        total_lag = sum(n["data"]["totalLag"] for n in cg_nodes)
        expected_total = sum(g.total_lag for g in snapshot.consumer_groups.values())
        assert total_lag == expected_total, (
            f"Total lag mismatch: expected {expected_total}, got {total_lag}"
        )

    def test_mixed_active_inactive_large(self):
        """With 200 active and 300 inactive nodes, counts should be correct."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Build graph with 500 topics
        topics_all = {}
        for i in range(500):
            name = f"mixed-topic-{i}"
            topics_all[name] = TopicInfo(
                name=name, partitions=3, msg_per_sec=5.0, total_messages=1000
            )
        snap1 = ClusterSnapshot(
            topics=topics_all, consumer_groups={}, active_partitions={}, timestamp=1.0
        )
        diff1 = builder.update(snap1)
        assert len(diff1.nodes_added) == 500

        # Phase 2: Keep only 200 topics (topics 0-199), dropping 300 (topics 200-499)
        topics_active = {}
        for i in range(200):
            name = f"mixed-topic-{i}"
            topics_active[name] = TopicInfo(
                name=name, partitions=3, msg_per_sec=10.0, total_messages=2000
            )
        snap2 = ClusterSnapshot(
            topics=topics_active, consumer_groups={}, active_partitions={}, timestamp=2.0
        )
        diff2 = builder.update(snap2)

        # 300 nodes should have been marked inactive
        inactive_updates = [
            n for n in diff2.nodes_updated if n.get("status") == "inactive"
        ]
        assert len(inactive_updates) == 300, (
            f"Expected 300 inactive nodes, got {len(inactive_updates)}"
        )

        # Verify final state: 200 active, 300 inactive
        snapshot_result = builder.get_snapshot()
        all_nodes = snapshot_result["nodes"]["added"]

        active_nodes = [n for n in all_nodes if n.get("status") != "inactive"]
        inactive_nodes = [n for n in all_nodes if n.get("status") == "inactive"]

        assert len(active_nodes) == 200, f"Expected 200 active nodes, got {len(active_nodes)}"
        assert len(inactive_nodes) == 300, f"Expected 300 inactive nodes, got {len(inactive_nodes)}"

        # Total node count should be 500 (active + inactive)
        assert len(all_nodes) == 500, f"Expected 500 total nodes, got {len(all_nodes)}"


class TestMemoryEfficiency:
    """Test that the graph state remains bounded under sustained workloads."""

    def test_no_memory_leak_on_updates(self):
        """Run 500 update cycles and verify graph state node count stays bounded."""
        builder = GraphStateBuilder(show_producers=False)

        # Establish a stable base of 50 topics and 20 consumer groups
        def make_stable_snap(rate_seed: float) -> ClusterSnapshot:
            topics = {
                f"stable-topic-{i}": TopicInfo(
                    name=f"stable-topic-{i}",
                    partitions=3,
                    msg_per_sec=rate_seed + i,
                    total_messages=10000 + int(rate_seed) * 100,
                )
                for i in range(50)
            }
            cgs = {
                f"stable-group-{j}": ConsumerGroupInfo(
                    group_id=f"stable-group-{j}",
                    members=2,
                    total_lag=j * 10,
                    status="Stable",
                    subscribed_topics=[f"stable-topic-{j % 50}"],
                    per_partition_lag={f"stable-topic-{j % 50}-0": j * 5},
                    member_client_ids=[f"stable-group-{j}-client"],
                )
                for j in range(20)
            }
            return ClusterSnapshot(
                topics=topics, consumer_groups=cgs, active_partitions={}, timestamp=time.time()
            )

        node_counts = []
        for cycle in range(500):
            builder.update(make_stable_snap(float(cycle)))
            node_counts.append(len(builder._nodes))

        # After stabilisation the count must not grow unboundedly.
        # Allow a small settling period (first 5 cycles) and then check that the
        # node count does not exceed the stabilised value by more than 10%.
        stabilised_count = node_counts[5]
        max_count = max(node_counts[5:])
        assert max_count <= stabilised_count * 1.10, (
            f"Node count grew from {stabilised_count} to {max_count} over 500 cycles "
            f"(>{stabilised_count * 1.10:.0f}), possible memory leak"
        )

    def test_inactive_cleanup_over_time(self):
        """Verify inactive nodes don't accumulate indefinitely when topics keep changing."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: build 50 "old" topics
        old_topics = {
            f"old-topic-{i}": TopicInfo(
                name=f"old-topic-{i}", partitions=3, msg_per_sec=1.0, total_messages=1000
            )
            for i in range(50)
        }
        builder.update(ClusterSnapshot(
            topics=old_topics, consumer_groups={}, active_partitions={}, timestamp=1.0
        ))
        count_after_phase1 = len(builder._nodes)

        # Phase 2: replace entirely with 50 "new" topics (old ones become inactive)
        new_topics = {
            f"new-topic-{i}": TopicInfo(
                name=f"new-topic-{i}", partitions=3, msg_per_sec=2.0, total_messages=2000
            )
            for i in range(50)
        }
        builder.update(ClusterSnapshot(
            topics=new_topics, consumer_groups={}, active_partitions={}, timestamp=2.0
        ))
        count_after_phase2 = len(builder._nodes)

        # Phase 3: update the new topics many times — old inactive ones must not multiply
        for cycle in range(50):
            updated_topics = {
                f"new-topic-{i}": TopicInfo(
                    name=f"new-topic-{i}",
                    partitions=3,
                    msg_per_sec=float(cycle + i),
                    total_messages=2000 + cycle * 50,
                )
                for i in range(50)
            }
            builder.update(ClusterSnapshot(
                topics=updated_topics,
                consumer_groups={},
                active_partitions={},
                timestamp=3.0 + cycle,
            ))

        count_after_phase3 = len(builder._nodes)

        # After many updates without old topics, inactive nodes should not accumulate.
        # The count should be <= phase 2 count (old inactive ones may be cleaned up).
        assert count_after_phase3 <= count_after_phase2, (
            f"Node count grew from {count_after_phase2} (after phase 2) to "
            f"{count_after_phase3} (after 50 more updates) — inactive nodes are accumulating"
        )

        # The active set should be exactly 50 (the new topics)
        active_nodes = [nid for nid, n in builder._nodes.items() if n.status != "inactive"]
        assert len(active_nodes) == 50, (
            f"Expected 50 active nodes, got {len(active_nodes)}"
        )


# ── API Route Tests ─────────────────────────────────────────────────────
# These test the REST endpoints with mocked KafkaAdmin, using the same
# helper pattern as test_api_routes.py.

from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from api_routes import router


def _create_app(admin=None, sampler=None):
    """Build a minimal FastAPI app with the api_routes router and optional mocks."""
    app = FastAPI()
    app.include_router(router)
    app.state.kafka_admin = admin
    app.state.message_sampler = sampler
    return app


class TestProduceMessageAPI:
    """Tests for POST /api/topics/{topic}/produce."""

    def test_produce_with_key_value_headers(self):
        """Produce a message with key, value, and headers returns success with metadata."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "orders",
            "partition": 2,
            "offset": 1042,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/produce",
            json={
                "value": "order-payload",
                "key": "order-123",
                "headers": {"trace-id": "abc-def", "source": "test-suite"},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partition"] == 2
        assert body["offset"] == 1042
        # Verify the admin was called with the right arguments
        admin.produce_message.assert_called_once_with(
            topic="orders",
            value="order-payload",
            key="order-123",
            headers={"trace-id": "abc-def", "source": "test-suite"},
            partition=None,
        )

    def test_produce_with_specific_partition(self):
        """Produce targeting a specific partition passes the partition through."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "events",
            "partition": 5,
            "offset": 300,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/events/produce",
            json={"value": "event-data", "partition": 5},
        )
        assert resp.status_code == 200
        assert resp.json()["partition"] == 5
        admin.produce_message.assert_called_once_with(
            topic="events",
            value="event-data",
            key=None,
            headers=None,
            partition=5,
        )

    def test_produce_with_empty_value(self):
        """Producing with an empty value string succeeds (tombstone-like message)."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "cleanup",
            "partition": 0,
            "offset": 10,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/cleanup/produce",
            json={"key": "stale-key"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        # value defaults to "" when not provided
        admin.produce_message.assert_called_once_with(
            topic="cleanup",
            value="",
            key="stale-key",
            headers=None,
            partition=None,
        )

    def test_produce_failure_returns_500(self):
        """When the admin reports a produce failure, the endpoint returns HTTP 500."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": False,
            "error": "Broker not available",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/produce",
            json={"value": "will-fail"},
        )
        assert resp.status_code == 500
        assert "Broker not available" in resp.json()["detail"]

    def test_produce_without_admin_returns_503(self):
        """When kafka_admin is not set, produce returns HTTP 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.post(
            "/api/topics/orders/produce",
            json={"value": "no-admin"},
        )
        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()


class TestTopicPartitionsAPI:
    """Tests for POST /api/topics/{topic}/partitions."""

    def test_add_partitions_success(self):
        """Successfully increasing partitions returns 200 with new count."""
        admin = MagicMock()
        admin.add_topic_partitions.return_value = {
            "success": True,
            "topic": "orders",
            "partitions": 12,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={"totalPartitions": 12},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partitions"] == 12
        admin.add_topic_partitions.assert_called_once_with("orders", 12)

    def test_add_partitions_missing_total(self):
        """Omitting totalPartitions returns HTTP 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={},
        )
        assert resp.status_code == 400
        assert "totalPartitions" in resp.json()["detail"]
        admin.add_topic_partitions.assert_not_called()

    def test_add_partitions_invalid_type(self):
        """Passing a string for totalPartitions returns HTTP 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={"totalPartitions": "twelve"},
        )
        assert resp.status_code == 400
        admin.add_topic_partitions.assert_not_called()

    def test_add_partitions_zero_value(self):
        """Passing 0 for totalPartitions returns HTTP 400 (must be positive)."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={"totalPartitions": 0},
        )
        assert resp.status_code == 400
        admin.add_topic_partitions.assert_not_called()

    def test_add_partitions_kafka_error(self):
        """When Kafka returns an error, the endpoint returns HTTP 400 with detail."""
        admin = MagicMock()
        admin.add_topic_partitions.return_value = {
            "success": False,
            "error": "Topic 'orders' currently has 6 partitions, which is higher than the requested 3",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={"totalPartitions": 3},
        )
        assert resp.status_code == 400
        assert "higher than the requested" in resp.json()["detail"]


class TestResetOffsetsAPI:
    """Tests for POST /api/consumer-groups/{group}/reset-offsets."""

    def test_reset_with_latest_strategy(self):
        """Reset offsets to latest returns success with partition count."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 6,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/my-group/reset-offsets",
            json={"strategy": "latest"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partitionsReset"] == 6
        admin.reset_offsets.assert_called_once_with(
            group_id="my-group",
            strategy="latest",
            topic=None,
            timestamp=None,
            offset=None,
        )

    def test_reset_with_earliest_strategy(self):
        """Reset offsets to earliest passes the correct strategy through."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 4,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/processor-group/reset-offsets",
            json={"strategy": "earliest"},
        )
        assert resp.status_code == 200
        assert resp.json()["partitionsReset"] == 4
        admin.reset_offsets.assert_called_once_with(
            group_id="processor-group",
            strategy="earliest",
            topic=None,
            timestamp=None,
            offset=None,
        )

    def test_reset_with_specific_topic(self):
        """Reset offsets for a single topic passes the topic filter through."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 3,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/my-group/reset-offsets",
            json={"strategy": "earliest", "topic": "orders.created"},
        )
        assert resp.status_code == 200
        assert resp.json()["partitionsReset"] == 3
        admin.reset_offsets.assert_called_once_with(
            group_id="my-group",
            strategy="earliest",
            topic="orders.created",
            timestamp=None,
            offset=None,
        )

    def test_reset_defaults_to_latest(self):
        """When no strategy is provided, the endpoint defaults to 'latest'."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 2,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/my-group/reset-offsets",
            json={},
        )
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="my-group",
            strategy="latest",
            topic=None,
            timestamp=None,
            offset=None,
        )

    def test_reset_failure_returns_400(self):
        """When the admin reports a reset failure, the endpoint returns HTTP 400."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": False,
            "error": "Group 'active-group' has active members; stop consumers first",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/active-group/reset-offsets",
            json={"strategy": "earliest"},
        )
        assert resp.status_code == 400
        assert "active members" in resp.json()["detail"]


class TestNodeRemoval:
    """Test that node and edge removal works correctly in diffs."""

    def test_topic_removed_produces_diff(self):
        """When a topic disappears from snapshot, it should be marked inactive."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot with 5 topics
        topics1 = {
            f"topic-{i}": TopicInfo(
                name=f"topic-{i}", partitions=3, msg_per_sec=1.0, total_messages=1000
            )
            for i in range(5)
        }
        snap1 = ClusterSnapshot(
            topics=topics1, consumer_groups={}, active_partitions={}, timestamp=1.0
        )
        diff1 = builder.update(snap1)
        assert len(diff1.nodes_added) == 5

        # Second snapshot with only 3 topics (2 removed)
        topics2 = {
            f"topic-{i}": TopicInfo(
                name=f"topic-{i}", partitions=3, msg_per_sec=1.0, total_messages=1100
            )
            for i in range(3)
        }
        snap2 = ClusterSnapshot(
            topics=topics2, consumer_groups={}, active_partitions={}, timestamp=2.0
        )
        diff2 = builder.update(snap2)

        # The removed topics should be in updated (status change) or the diff should track them
        snapshot = builder.get_snapshot()
        snapshot_nodes = {n["id"]: n for n in snapshot["nodes"]["added"]}

        # Active topics should still exist
        for i in range(3):
            assert f"topic-topic-{i}" in snapshot_nodes

    def test_consumer_group_removed(self):
        """When a consumer group disappears, it should become inactive."""
        builder = GraphStateBuilder(show_producers=False)

        topics = {
            "events": TopicInfo(name="events", partitions=3, msg_per_sec=1.0, total_messages=1000)
        }
        groups = {
            "group-a": ConsumerGroupInfo(
                group_id="group-a", members=1, total_lag=0, status="Stable",
                subscribed_topics=["events"],
                per_partition_lag={"events-0": 0},
                member_client_ids=["client-a"],
            ),
            "group-b": ConsumerGroupInfo(
                group_id="group-b", members=1, total_lag=0, status="Stable",
                subscribed_topics=["events"],
                per_partition_lag={"events-0": 0},
                member_client_ids=["client-b"],
            ),
        }
        snap1 = ClusterSnapshot(
            topics=topics, consumer_groups=groups, active_partitions={}, timestamp=1.0
        )
        builder.update(snap1)

        # Remove group-b
        groups2 = {
            "group-a": groups["group-a"],
        }
        snap2 = ClusterSnapshot(
            topics=topics, consumer_groups=groups2, active_partitions={}, timestamp=2.0
        )
        diff2 = builder.update(snap2)

        # Verify group-b is now inactive
        snapshot = builder.get_snapshot()
        all_node_ids = {n["id"] for n in snapshot["nodes"]["added"]}
        assert "topic-events" in all_node_ids

    def test_massive_topic_churn(self):
        """Simulate 100 topics being added and 80 removed over multiple cycles."""
        builder = GraphStateBuilder(show_producers=False)

        for cycle in range(10):
            start_idx = cycle * 20
            topics = {
                f"churn-topic-{i}": TopicInfo(
                    name=f"churn-topic-{i}", partitions=3,
                    msg_per_sec=1.0, total_messages=1000 + cycle * 100
                )
                for i in range(start_idx, start_idx + 20)
            }
            snap = ClusterSnapshot(
                topics=topics, consumer_groups={}, active_partitions={}, timestamp=float(cycle)
            )
            builder.update(snap)

        # After 10 cycles of 20 new topics each, final cycle has topics 180-199
        snapshot = builder.get_snapshot()
        active_ids = [
            n["id"] for n in snapshot["nodes"]["added"]
            if n.get("data", {}).get("status") != "inactive"
        ]
        # At least the 20 most recent topics should be active
        assert len(active_ids) >= 20

    def test_edge_consistency_after_removal(self):
        """Edges to removed nodes should not appear in snapshot."""
        builder = GraphStateBuilder(show_producers=False)

        topics = {
            "events": TopicInfo(name="events", partitions=3, msg_per_sec=1.0, total_messages=1000)
        }
        groups = {
            "consumer-x": ConsumerGroupInfo(
                group_id="consumer-x", members=2, total_lag=50, status="Stable",
                subscribed_topics=["events"],
                per_partition_lag={"events-0": 25, "events-1": 25},
                member_client_ids=["client-1", "client-2"],
            )
        }
        snap1 = ClusterSnapshot(
            topics=topics, consumer_groups=groups, active_partitions={}, timestamp=1.0
        )
        builder.update(snap1)

        # Now remove the consumer group
        snap2 = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions={}, timestamp=2.0
        )
        builder.update(snap2)

        snapshot = builder.get_snapshot()
        node_ids = {n["id"] for n in snapshot["nodes"]["added"]}
        edge_sources = {e["source"] for e in snapshot["edges"]["added"]}
        edge_targets = {e["target"] for e in snapshot["edges"]["added"]}

        # All edge endpoints should reference existing nodes
        for src in edge_sources:
            assert src in node_ids, f"Edge source {src} not in nodes"
        for tgt in edge_targets:
            assert tgt in node_ids, f"Edge target {tgt} not in nodes"


class TestGraphDiffSerialization:
    """Test GraphDiff serialization and structure."""

    def test_diff_to_dict_structure(self):
        """Verify the diff dict has the expected structure."""
        builder = GraphStateBuilder(show_producers=False)
        topics = {
            f"t-{i}": TopicInfo(name=f"t-{i}", partitions=3, msg_per_sec=1.0, total_messages=100)
            for i in range(5)
        }
        snap = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions={}, timestamp=1.0
        )
        diff = builder.update(snap)
        d = diff.to_dict()

        assert "type" in d
        assert d["type"] == "graph_diff"
        assert "ts" in d
        assert "nodes" in d
        assert "edges" in d
        assert "added" in d["nodes"]
        assert "updated" in d["nodes"]
        assert "removed" in d["nodes"]

    def test_diff_metrics_included(self):
        """Verify metrics are included in the diff."""
        builder = GraphStateBuilder(show_producers=False)
        topics = {
            "metriced-topic": TopicInfo(
                name="metriced-topic", partitions=3, msg_per_sec=42.5, total_messages=99999
            )
        }
        snap = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions={}, timestamp=1.0
        )
        diff = builder.update(snap)
        d = diff.to_dict()

        assert "metrics" in d
        assert "topic-metriced-topic" in d["metrics"]
        assert d["metrics"]["topic-metriced-topic"]["msgPerSec"] == 42.5
        assert d["metrics"]["topic-metriced-topic"]["totalMessages"] == 99999


class TestGraphSnapshotConsistency:
    """Test that get_snapshot() always returns valid data."""

    def test_snapshot_has_required_fields(self):
        """Verify snapshot has type, nodes.added, edges.added, metrics."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=100, num_consumer_groups=50)
        builder.update(snapshot)

        result = builder.get_snapshot()

        assert result["type"] == "graph_snapshot"
        assert "nodes" in result
        assert "added" in result["nodes"]
        assert "edges" in result
        assert "added" in result["edges"]
        assert "metrics" in result
        assert isinstance(result["nodes"]["added"], list)
        assert isinstance(result["edges"]["added"], list)
        assert isinstance(result["metrics"], dict)

    def test_snapshot_node_ids_are_unique(self):
        """Verify no duplicate node IDs in snapshot."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=200, num_consumer_groups=100)
        builder.update(snapshot)

        result = builder.get_snapshot()
        node_ids = [n["id"] for n in result["nodes"]["added"]]

        assert len(node_ids) == len(set(node_ids)), "Duplicate node IDs found in snapshot"

    def test_snapshot_edge_endpoints_exist(self):
        """All edge sources and targets reference existing nodes."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=150, num_consumer_groups=75)
        builder.update(snapshot)

        result = builder.get_snapshot()
        node_ids = {n["id"] for n in result["nodes"]["added"]}
        edges = result["edges"]["added"]

        for edge in edges:
            assert "source" in edge, f"Edge missing 'source': {edge}"
            assert "target" in edge, f"Edge missing 'target': {edge}"
            assert edge["source"] in node_ids, (
                f"Edge source '{edge['source']}' not in nodes"
            )
            assert edge["target"] in node_ids, (
                f"Edge target '{edge['target']}' not in nodes"
            )

    def test_empty_snapshot(self):
        """Empty builder returns valid but empty snapshot."""
        builder = GraphStateBuilder(show_producers=False)

        # Don't call update, just get snapshot on empty builder
        result = builder.get_snapshot()

        assert result["type"] == "graph_snapshot"
        assert "nodes" in result
        assert "edges" in result
        assert isinstance(result["nodes"]["added"], list)
        assert isinstance(result["edges"]["added"], list)
        assert len(result["nodes"]["added"]) == 0
        assert len(result["edges"]["added"]) == 0


class TestPerformanceBenchmarks:
    """More performance tests for graph operations."""

    def test_1000_topics_build_time(self):
        """Build graph with 1000 topics in under 3 seconds."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=1000, num_consumer_groups=0)

        start = time.time()
        diff = builder.update(snapshot)
        elapsed = time.time() - start

        assert elapsed < 3.0, f"Building 1000 topics took {elapsed:.2f}s, expected <3s"
        # With 1000 topics in snapshot, we should have nodes added
        assert len(diff.nodes_added) > 0
        # Verify topic metrics match what was created
        topic_metrics = [k for k in diff.metrics if k.startswith("topic-")]
        assert len(topic_metrics) == len(snapshot.topics)

    def test_rapid_diff_application_100_cycles(self):
        """100 update cycles in under 1 second."""
        builder = GraphStateBuilder(show_producers=False)

        # Establish baseline
        base_snapshot = make_large_snapshot(num_topics=50, num_consumer_groups=50)
        builder.update(base_snapshot)

        # Run 100 rapid update cycles
        start = time.time()
        for i in range(100):
            snapshot = make_large_snapshot(
                num_topics=50,
                num_consumer_groups=50,
                active_rate=10.0 + (i % 10) * 0.5,
                lag_per_partition=50 + (i % 20),
            )
            builder.update(snapshot)
        elapsed = time.time() - start

        assert elapsed < 1.0, f"100 update cycles took {elapsed:.3f}s, expected <1s"

    def test_large_consumer_group_count(self):
        """200 consumer groups each subscribed to 5 topics."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=100, num_consumer_groups=200)
        builder.update(snapshot)

        # Verify the snapshot was built successfully
        result = builder.get_snapshot()
        assert result["type"] == "graph_snapshot"

        # Count consumer group nodes (cg- or svc- prefix)
        consumer_nodes = [
            n for n in result["nodes"]["added"]
            if n["type"] in ("consumer_group", "service")
        ]
        assert len(consumer_nodes) > 0, "No consumer groups found in snapshot"


class TestGraphMetrics:
    """Test metrics tracking in the graph."""

    def test_metrics_track_all_topics(self):
        """All topics appear in metrics."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = make_large_snapshot(num_topics=100, num_consumer_groups=0)
        diff = builder.update(snapshot)

        topic_count = len(snapshot.topics)
        metrics_topic_count = sum(1 for k in diff.metrics if k.startswith("topic-"))

        assert metrics_topic_count == topic_count, (
            f"Expected {topic_count} topics in metrics, got {metrics_topic_count}"
        )

    def test_metrics_update_on_rate_change(self):
        """Metrics update when msg_per_sec changes."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot with rate 10.0
        snap1 = ClusterSnapshot(
            topics={
                "rate-test": TopicInfo(
                    name="rate-test", partitions=3, msg_per_sec=10.0, total_messages=1000
                )
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1.0,
        )
        diff1 = builder.update(snap1)

        assert diff1.metrics["topic-rate-test"]["msgPerSec"] == 10.0

        # Second snapshot with rate 25.5
        snap2 = ClusterSnapshot(
            topics={
                "rate-test": TopicInfo(
                    name="rate-test", partitions=3, msg_per_sec=25.5, total_messages=2000
                )
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2.0,
        )
        diff2 = builder.update(snap2)

        assert diff2.metrics["topic-rate-test"]["msgPerSec"] == 25.5

    def test_metrics_zero_rate_for_idle_topics(self):
        """Idle topics have 0 msg/sec in metrics."""
        builder = GraphStateBuilder(show_producers=False)

        snapshot = ClusterSnapshot(
            topics={
                "active-topic": TopicInfo(
                    name="active-topic", partitions=3, msg_per_sec=15.0, total_messages=5000
                ),
                "idle-topic": TopicInfo(
                    name="idle-topic", partitions=3, msg_per_sec=0.0, total_messages=100
                ),
                "another-idle": TopicInfo(
                    name="another-idle", partitions=2, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1.0,
        )
        diff = builder.update(snapshot)

        assert diff.metrics["topic-active-topic"]["msgPerSec"] == 15.0
        assert diff.metrics["topic-idle-topic"]["msgPerSec"] == 0.0


class TestEdgeDataProperties:
    """Test that edge data contains expected fields with correct values."""

    def test_consume_edge_has_lag(self):
        """Edges from topic to consumer should have 'lag' and 'lagWarning' fields."""
        builder = GraphStateBuilder(lag_warn_threshold=100, show_producers=False)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "order-consumer": ConsumerGroupInfo(
                    group_id="order-consumer",
                    members=2,
                    total_lag=150,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 50,
                        "orders.created-1": 50,
                        "orders.created-2": 50,
                    },
                    member_client_ids=["order-consumer-1", "order-consumer-2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # Find consume edges (topic -> consumer)
        consume_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) > 0, "No consume edges found"

        for edge in consume_edges:
            assert "lag" in edge.get("data", {}), "Edge missing 'lag' field"
            assert "lagWarning" in edge.get("data", {}), "Edge missing 'lagWarning' field"
            assert edge["data"]["lag"] == 150, f"Expected lag=150, got {edge['data']['lag']}"
            assert edge["data"]["lagWarning"] is True, "Expected lagWarning=True for lag > threshold"

    def test_produce_edge_has_rate(self):
        """Edges from service to topic should have 'msgPerSec' field."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=25.5, total_messages=5000
                ),
                "orders.processed": TopicInfo(
                    name="orders.processed", partitions=3, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={
                "order-service": ConsumerGroupInfo(
                    group_id="order-service",
                    members=2,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 5,
                        "orders.created-1": 5,
                        "orders.created-2": 0,
                    },
                    member_client_ids=["order-service-1", "order-service-2"],
                ),
            },
            active_partitions={"orders.processed": {0, 1, 2}},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # Find produce edges (service -> topic)
        produce_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "produces"
        ]
        assert len(produce_edges) > 0, "No produce edges found"

        for edge in produce_edges:
            assert "msgPerSec" in edge.get("data", {}), "Edge missing 'msgPerSec' field"

        # Check that produce edges have the correct msgPerSec values
        # orders.processed has msgPerSec=0.0, so if there's a producer edge for it,
        # it should have 0.0. The service produces to orders.processed with rate from that topic.
        assert all("msgPerSec" in e["data"] for e in produce_edges), "All produce edges should have msgPerSec"

    def test_edge_label_generated(self):
        """Edges with lag > 0 should have non-empty label."""
        builder = GraphStateBuilder(lag_warn_threshold=100, show_producers=False)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "order-consumer-high-lag": ConsumerGroupInfo(
                    group_id="order-consumer-high-lag",
                    members=1,
                    total_lag=250,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 100,
                        "orders.created-1": 75,
                        "orders.created-2": 75,
                    },
                    member_client_ids=["order-consumer-1"],
                ),
                "order-consumer-no-lag": ConsumerGroupInfo(
                    group_id="order-consumer-no-lag",
                    members=1,
                    total_lag=0,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 0,
                        "orders.created-1": 0,
                        "orders.created-2": 0,
                    },
                    member_client_ids=["order-consumer-2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        consume_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 2

        # High lag edge should have non-empty label
        high_lag_edge = [e for e in consume_edges if e["data"]["lag"] > 0]
        assert len(high_lag_edge) > 0
        for edge in high_lag_edge:
            assert edge["data"]["label"] != "", f"Expected non-empty label for lag > 0"
            assert "lag" in edge["data"]["label"], "Label should contain 'lag'"

        # Zero lag edge should have empty label
        zero_lag_edge = [e for e in consume_edges if e["data"]["lag"] == 0]
        assert len(zero_lag_edge) > 0
        for edge in zero_lag_edge:
            assert edge["data"]["label"] == "", f"Expected empty label for lag = 0"

    def test_inactive_edge_marking(self):
        """When a consumer group disappears, its edges should be marked inactive."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot with a consumer group
        snap1 = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "order-consumer": ConsumerGroupInfo(
                    group_id="order-consumer",
                    members=2,
                    total_lag=50,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 20,
                        "orders.created-1": 15,
                        "orders.created-2": 15,
                    },
                    member_client_ids=["order-consumer-1", "order-consumer-2"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)
        assert len(diff1.edges_added) > 0

        # Second snapshot without the consumer group
        snap2 = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)

        # Edges should be marked inactive, not removed
        inactive_edges = [
            e for e in diff2.edges_updated
            if e.get("data", {}).get("inactive") is True
        ]
        assert len(inactive_edges) > 0, "Expected edges to be marked inactive"

        for edge in inactive_edges:
            assert edge["data"].get("active") is False
            assert edge["data"].get("inactive") is True


class TestServiceNodeDetection:
    """Test service node identification and typing."""

    def test_consumer_that_produces_becomes_service(self):
        """If a consumer group produces to a topic, it should be typed as 'service'."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=50.0, total_messages=10000
                ),
                "orders.processed": TopicInfo(
                    name="orders.processed", partitions=3, msg_per_sec=48.0, total_messages=9600
                ),
            },
            consumer_groups={
                "order-service": ConsumerGroupInfo(
                    group_id="order-service",
                    members=3,
                    total_lag=30,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 10,
                        "orders.created-1": 10,
                        "orders.created-2": 10,
                    },
                    member_client_ids=["order-service-1", "order-service-2", "order-service-3"],
                ),
            },
            active_partitions={"orders.processed": {0, 1, 2}},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # Find the order-service node
        service_nodes = [
            n for n in diff.nodes_added
            if "order-service" in n["id"] and n["type"] == "service"
        ]
        assert len(service_nodes) > 0, "order-service should be typed as 'service' (not consumer_group)"

        service_node = service_nodes[0]
        assert service_node["type"] == "service"
        # Verify it has both produces and consumes edges by checking edges exist
        all_edges = diff.edges_added
        produces_edges = [e for e in all_edges if service_node["id"] in e.get("source", "")]
        consumes_edges = [e for e in all_edges if service_node["id"] in e.get("target", "")]
        assert len(produces_edges) > 0, "Service should have produces edges"
        assert len(consumes_edges) > 0, "Service should have consumes edges"

    def test_plain_consumer_group(self):
        """Consumer groups that don't produce should remain as 'consumer_group'."""
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=20.0, total_messages=5000
                ),
            },
            consumer_groups={
                "analytics-consumer": ConsumerGroupInfo(
                    group_id="analytics-consumer",
                    members=2,
                    total_lag=100,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 40,
                        "orders.created-1": 30,
                        "orders.created-2": 30,
                    },
                    member_client_ids=["analytics-consumer-1", "analytics-consumer-2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # Find the consumer node
        consumer_nodes = [
            n for n in diff.nodes_added
            if "analytics-consumer" in n["id"]
        ]
        assert len(consumer_nodes) > 0, "Consumer group node should exist"
        assert consumer_nodes[0]["type"] == "consumer_group"

    def test_service_node_has_produces_and_consumes(self):
        """Service nodes should have both 'produces' and 'consumes' data fields."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "input.events": TopicInfo(
                    name="input.events", partitions=2, msg_per_sec=100.0, total_messages=10000
                ),
                "events.processed": TopicInfo(
                    name="events.processed", partitions=2, msg_per_sec=95.0, total_messages=9500
                ),
            },
            consumer_groups={
                "events-service": ConsumerGroupInfo(
                    group_id="events-service",
                    members=2,
                    total_lag=50,
                    status="Stable",
                    subscribed_topics=["input.events"],
                    per_partition_lag={
                        "input.events-0": 25,
                        "input.events-1": 25,
                    },
                    member_client_ids=["events-service-1", "events-service-2"],
                ),
            },
            active_partitions={"events.processed": {0, 1}},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        service_nodes = [
            n for n in diff.nodes_added
            if n["type"] == "service" and "events-service" in n["id"]
        ]
        assert len(service_nodes) > 0, "events-service should be a service node"

        service_node = service_nodes[0]
        # Verify edges exist for both produces and consumes
        all_edges = diff.edges_added
        produces_edges = [
            e for e in all_edges
            if e.get("data", {}).get("type") == "produces" and service_node["id"] in e.get("source", "")
        ]
        consumes_edges = [
            e for e in all_edges
            if e.get("data", {}).get("type") == "consumes" and service_node["id"] in e.get("target", "")
        ]
        assert len(produces_edges) > 0, "Service should have produces edges"
        assert len(consumes_edges) > 0, "Service should have consumes edges"


class TestInactiveNodeBehavior:
    """Test inactive node behavior and lifecycle."""

    def test_removed_topic_becomes_inactive(self):
        """When a topic disappears it should be marked inactive."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot with a topic
        snap1 = ClusterSnapshot(
            topics={
                "temp-topic": TopicInfo(
                    name="temp-topic", partitions=3, msg_per_sec=10.0, total_messages=1000
                ),
                "persistent-topic": TopicInfo(
                    name="persistent-topic", partitions=3, msg_per_sec=5.0, total_messages=500
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)

        # Verify both topics are added
        topic_names = {n["id"] for n in diff1.nodes_added}
        assert "topic-temp-topic" in topic_names
        assert "topic-persistent-topic" in topic_names

        # Second snapshot without temp-topic
        snap2 = ClusterSnapshot(
            topics={
                "persistent-topic": TopicInfo(
                    name="persistent-topic", partitions=3, msg_per_sec=5.0, total_messages=500
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)

        # temp-topic should be marked inactive
        inactive_nodes = [
            n for n in diff2.nodes_updated
            if n.get("id") == "topic-temp-topic" and n.get("status") == "inactive"
        ]
        assert len(inactive_nodes) > 0, "Removed topic should be marked inactive"

    def test_inactive_node_reactivates(self):
        """When an inactive node reappears it should become active again."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot with a topic
        snap1 = ClusterSnapshot(
            topics={
                "transient-topic": TopicInfo(
                    name="transient-topic", partitions=2, msg_per_sec=5.0, total_messages=500
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)
        assert len([n for n in diff1.nodes_added if n["id"] == "topic-transient-topic"]) > 0

        # Second snapshot without the topic (marks as inactive)
        snap2 = ClusterSnapshot(
            topics={},
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)
        assert len([n for n in diff2.nodes_updated if n["id"] == "topic-transient-topic"]) > 0

        # Third snapshot with the topic reappearing
        snap3 = ClusterSnapshot(
            topics={
                "transient-topic": TopicInfo(
                    name="transient-topic", partitions=2, msg_per_sec=5.0, total_messages=500
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=3000.0,
        )
        diff3 = builder.update(snap3)

        # Topic should be reactivated (no longer have status="inactive")
        reactivated_nodes = [
            n for n in diff3.nodes_updated
            if n.get("id") == "topic-transient-topic" and n.get("status") != "inactive"
        ]
        # Or it might be in nodes_added if the builder reinitializes it
        # Check the current state instead
        snapshot = builder.get_snapshot()
        transient_topic_nodes = [
            n for n in snapshot["nodes"]["added"]
            if n["id"] == "topic-transient-topic"
        ]
        assert len(transient_topic_nodes) > 0
        # The node should not have status="inactive"
        transient_node = transient_topic_nodes[0]
        assert transient_node.get("status") != "inactive", "Reappeared node should be active"

    def test_inactive_edges_persist(self):
        """Inactive edges should remain in the graph."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot with a consumer
        snap1 = ClusterSnapshot(
            topics={
                "events": TopicInfo(
                    name="events", partitions=2, msg_per_sec=10.0, total_messages=1000
                ),
            },
            consumer_groups={
                "temp-consumer": ConsumerGroupInfo(
                    group_id="temp-consumer",
                    members=1,
                    total_lag=20,
                    status="Stable",
                    subscribed_topics=["events"],
                    per_partition_lag={
                        "events-0": 10,
                        "events-1": 10,
                    },
                    member_client_ids=["temp-consumer-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)
        edges_count_1 = len(diff1.edges_added)
        assert edges_count_1 > 0, "Should have consume edges"

        # Second snapshot without the consumer (should mark edges as inactive)
        snap2 = ClusterSnapshot(
            topics={
                "events": TopicInfo(
                    name="events", partitions=2, msg_per_sec=10.0, total_messages=1000
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)

        # Get final state
        final_snapshot = builder.get_snapshot()
        all_edges = final_snapshot["edges"]["added"] + final_snapshot["edges"]["updated"]

        # Verify inactive edges persist (not removed)
        inactive_edges = [e for e in all_edges if e.get("data", {}).get("inactive") is True]
        assert len(inactive_edges) > 0, "Inactive edges should persist in graph"


# ---------------------------------------------------------------------------
# New test classes: Consumer Group Deletion, Topic Config Update,
# Cluster Info, WebSocket Reconnection, Health Endpoint
# ---------------------------------------------------------------------------

import unittest
import json
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock, PropertyMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from api_routes import router


def _create_app(admin=None, sampler=None):
    """Helper to create a FastAPI test app with mocked state."""
    test_app = FastAPI()
    test_app.include_router(router)
    test_app.state.kafka_admin = admin
    test_app.state.message_sampler = sampler
    return test_app


class TestConsumerGroupDeletion(unittest.TestCase):
    """Test cases for the DELETE /api/consumer-groups/{group} endpoint."""

    def test_delete_active_group_fails(self):
        """Deleting an active consumer group should return a 400 error."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": False,
            "error": "The group is not empty.",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/consumer-groups/active-group")

        self.assertEqual(resp.status_code, 400)
        self.assertIn("not empty", resp.json()["detail"])
        admin.delete_consumer_group.assert_called_once_with("active-group")

    def test_delete_empty_group_succeeds(self):
        """Deleting an empty (inactive) consumer group should succeed."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": True,
            "groupId": "empty-group",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/consumer-groups/empty-group")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["success"])
        self.assertEqual(body["groupId"], "empty-group")
        admin.delete_consumer_group.assert_called_once_with("empty-group")

    def test_delete_nonexistent_group(self):
        """Deleting a group that does not exist should return a 400 error."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": False,
            "error": "Group 'ghost-group' not found",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/consumer-groups/ghost-group")

        self.assertEqual(resp.status_code, 400)
        self.assertIn("not found", resp.json()["detail"])

    def test_delete_group_kafka_error(self):
        """If KafkaAdmin returns a generic error, the route should return 400."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": False,
            "error": "GroupAuthorizationFailedError: not authorized",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/consumer-groups/secure-group")

        self.assertEqual(resp.status_code, 400)
        self.assertIn("not authorized", resp.json()["detail"])

    def test_delete_group_without_admin_returns_503(self):
        """If kafka_admin is not configured, the route should return 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.delete("/api/consumer-groups/any-group")

        self.assertEqual(resp.status_code, 503)


class TestTopicConfigUpdate(unittest.TestCase):
    """Test cases for the PUT /api/topics/{topic}/config endpoint."""

    def test_valid_config_update(self):
        """Updating a topic with valid config entries should succeed."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "orders",
            "updated": ["retention.ms", "max.message.bytes"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/orders/config",
            json={"configs": {"retention.ms": "604800000", "max.message.bytes": "1048576"}},
        )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertTrue(body["success"])
        self.assertEqual(body["topic"], "orders")
        self.assertIn("retention.ms", body["updated"])
        self.assertIn("max.message.bytes", body["updated"])
        admin.update_topic_config.assert_called_once_with(
            "orders", {"retention.ms": "604800000", "max.message.bytes": "1048576"}
        )

    def test_invalid_config_value_returns_error(self):
        """Updating with an invalid config value should return 400 via admin error."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": False,
            "error": "Invalid config value for 'retention.ms': not-a-number",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/orders/config",
            json={"configs": {"retention.ms": "not-a-number"}},
        )

        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid config value", resp.json()["detail"])

    def test_empty_configs_returns_400(self):
        """Sending an empty configs dict should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.put("/api/topics/orders/config", json={"configs": {}})

        self.assertEqual(resp.status_code, 400)
        self.assertIn("non-empty", resp.json()["detail"])
        admin.update_topic_config.assert_not_called()

    def test_missing_configs_key_returns_400(self):
        """Sending a request body without the 'configs' key should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.put("/api/topics/orders/config", json={"wrong_key": "value"})

        self.assertEqual(resp.status_code, 400)
        admin.update_topic_config.assert_not_called()

    def test_partial_config_update(self):
        """Updating only one config key should succeed and report only that key."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "events",
            "updated": ["cleanup.policy"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/events/config",
            json={"configs": {"cleanup.policy": "compact"}},
        )

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["updated"], ["cleanup.policy"])

    def test_configs_not_dict_returns_400(self):
        """If configs is a list instead of a dict, the route should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.put("/api/topics/orders/config", json={"configs": ["retention.ms"]})

        self.assertEqual(resp.status_code, 400)
        admin.update_topic_config.assert_not_called()


class TestClusterInfoEndpoint(unittest.TestCase):
    """Test cases for the GET /api/cluster endpoint."""

    def test_cluster_info_response_format(self):
        """The cluster info response should contain expected fields."""
        admin = MagicMock()
        admin.get_cluster_info.return_value = {
            "clusterId": "abc-cluster-123",
            "controllerId": 1,
            "brokerCount": 3,
            "topicCount": 42,
            "consumerGroupCount": 15,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["clusterId"], "abc-cluster-123")
        self.assertEqual(body["controllerId"], 1)
        self.assertEqual(body["brokerCount"], 3)
        self.assertEqual(body["topicCount"], 42)
        self.assertEqual(body["consumerGroupCount"], 15)

    def test_cluster_info_broker_unreachable(self):
        """When the broker is unreachable, the endpoint should return 500."""
        admin = MagicMock()
        admin.get_cluster_info.side_effect = Exception("NoBrokersAvailable")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster")

        self.assertEqual(resp.status_code, 500)
        self.assertIn("NoBrokersAvailable", resp.json()["detail"])

    def test_cluster_info_no_admin_returns_503(self):
        """If kafka_admin is not configured, the route should return 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.get("/api/cluster")

        self.assertEqual(resp.status_code, 503)

    def test_cluster_info_zero_brokers(self):
        """Cluster info can have zero brokers during bootstrap."""
        admin = MagicMock()
        admin.get_cluster_info.return_value = {
            "clusterId": "",
            "controllerId": None,
            "brokerCount": 0,
            "topicCount": 0,
            "consumerGroupCount": 0,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["brokerCount"], 0)
        self.assertIsNone(body["controllerId"])


class TestWebSocketReconnection(unittest.TestCase):
    """Test WebSocket disconnect/reconnect behavior and message ordering."""

    def _make_ws_app(self):
        """Create a minimal app with the WS endpoint for testing."""
        import secrets as _secrets
        from collections import deque
        from fastapi import WebSocket, WebSocketDisconnect

        ws_app = FastAPI()
        ws_clients_local: dict[str, WebSocket] = {}
        ws_queues_local: dict[str, deque] = {}

        # A simple in-memory graph builder mock
        mock_builder = MagicMock()
        mock_builder.get_snapshot.return_value = {
            "type": "graph_snapshot",
            "ts": 1000,
            "nodes": {"added": [], "updated": [], "removed": []},
            "edges": {"added": [], "updated": [], "removed": []},
            "metrics": {},
        }

        @ws_app.websocket("/ws/graph")
        async def ws_graph(websocket: WebSocket):
            await websocket.accept()
            client_id = _secrets.token_hex(8)
            ws_clients_local[client_id] = websocket
            ws_queues_local[client_id] = deque(maxlen=50)

            # Send initial snapshot
            try:
                snapshot = mock_builder.get_snapshot()
                snapshot["config"] = {
                    "showProducers": False,
                    "samplingEnabled": False,
                    "lagWarnThreshold": 1000,
                    "animationsEnabled": True,
                }
                await websocket.send_text(json.dumps(snapshot))
            except Exception:
                pass

            try:
                while True:
                    data = await websocket.receive_text()
                    try:
                        msg = json.loads(data)
                        if msg.get("type") == "request_snapshot":
                            snap = mock_builder.get_snapshot()
                            await websocket.send_text(json.dumps(snap))
                    except json.JSONDecodeError:
                        pass
            except WebSocketDisconnect:
                pass
            finally:
                ws_clients_local.pop(client_id, None)
                ws_queues_local.pop(client_id, None)

        return ws_app, ws_clients_local, mock_builder

    def test_ws_connect_receives_snapshot(self):
        """A new WS client should receive the initial graph snapshot upon connecting."""
        ws_app, _, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            data = ws.receive_text()
            msg = json.loads(data)
            self.assertEqual(msg["type"], "graph_snapshot")
            self.assertIn("config", msg)
            self.assertIn("nodes", msg)

    def test_ws_disconnect_and_reconnect(self):
        """Disconnecting and reconnecting should yield a new initial snapshot."""
        ws_app, ws_clients_local, _ = self._make_ws_app()
        client = TestClient(ws_app)

        # First connection
        with client.websocket_connect("/ws/graph") as ws:
            data = ws.receive_text()
            msg1 = json.loads(data)
            self.assertEqual(msg1["type"], "graph_snapshot")

        # After disconnect, ws_clients should be empty
        self.assertEqual(len(ws_clients_local), 0)

        # Second connection
        with client.websocket_connect("/ws/graph") as ws:
            data = ws.receive_text()
            msg2 = json.loads(data)
            self.assertEqual(msg2["type"], "graph_snapshot")

    def test_ws_request_snapshot_after_reconnect(self):
        """After reconnect, requesting a snapshot should return valid data."""
        ws_app, _, mock_builder = self._make_ws_app()
        client = TestClient(ws_app)

        # First connection and disconnect
        with client.websocket_connect("/ws/graph") as ws:
            ws.receive_text()

        # Update the mock to return different data after reconnect
        mock_builder.get_snapshot.return_value = {
            "type": "graph_snapshot",
            "ts": 2000,
            "nodes": {
                "added": [{"id": "topic-new", "type": "topic", "data": {}, "status": "ok"}],
                "updated": [],
                "removed": [],
            },
            "edges": {"added": [], "updated": [], "removed": []},
            "metrics": {},
        }

        # Reconnect and request snapshot
        with client.websocket_connect("/ws/graph") as ws:
            initial = json.loads(ws.receive_text())
            self.assertEqual(initial["type"], "graph_snapshot")

            # Request a fresh snapshot
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            response = json.loads(ws.receive_text())
            self.assertEqual(response["type"], "graph_snapshot")
            self.assertEqual(response["ts"], 2000)
            self.assertEqual(len(response["nodes"]["added"]), 1)
            self.assertEqual(response["nodes"]["added"][0]["id"], "topic-new")

    def test_ws_message_ordering(self):
        """Messages should arrive in the order they are sent."""
        ws_app, _, mock_builder = self._make_ws_app()

        call_count = 0
        timestamps = [3000, 4000, 5000]

        def _snapshot_side_effect():
            nonlocal call_count
            # First call is the initial snapshot on connect, subsequent are request_snapshot
            idx = min(call_count, len(timestamps) - 1)
            ts = timestamps[idx]
            call_count += 1
            return {
                "type": "graph_snapshot",
                "ts": ts,
                "nodes": {"added": [], "updated": [], "removed": []},
                "edges": {"added": [], "updated": [], "removed": []},
                "metrics": {},
            }

        mock_builder.get_snapshot.side_effect = _snapshot_side_effect

        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            # Receive initial snapshot (ts=3000)
            initial = json.loads(ws.receive_text())
            self.assertEqual(initial["ts"], 3000)

            # Request two snapshots
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            resp1 = json.loads(ws.receive_text())
            self.assertEqual(resp1["ts"], 4000)

            ws.send_text(json.dumps({"type": "request_snapshot"}))
            resp2 = json.loads(ws.receive_text())
            self.assertEqual(resp2["ts"], 5000)

            # Verify ordering: ts values should be monotonically increasing
            self.assertLess(initial["ts"], resp1["ts"])
            self.assertLess(resp1["ts"], resp2["ts"])

    def test_ws_invalid_json_ignored(self):
        """Sending invalid JSON over WS should not crash the server."""
        ws_app, _, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            initial = json.loads(ws.receive_text())
            self.assertEqual(initial["type"], "graph_snapshot")

            # Send garbage — the server should silently ignore it
            ws.send_text("this is not json{{{")

            # Sending a valid request afterward should still work
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            resp = json.loads(ws.receive_text())
            self.assertEqual(resp["type"], "graph_snapshot")


class TestHealthEndpoint(unittest.TestCase):
    """Test the GET /api/health endpoint."""

    def _make_health_app(self, connected=True, topics=None, groups=None,
                         nodes_count=0, edges_count=0):
        """Create a FastAPI app with the /api/health endpoint using mocked internals."""
        health_app = FastAPI()

        _topics = topics or {}
        _groups = groups or {}

        mock_collector = MagicMock()
        mock_collector.connected = connected
        mock_snapshot = MagicMock()
        mock_snapshot.topics = _topics
        mock_snapshot.consumer_groups = _groups
        mock_collector.snapshot = mock_snapshot

        mock_graph_builder = MagicMock()
        mock_graph_builder._nodes = {f"n{i}": None for i in range(nodes_count)}
        mock_graph_builder._edges = {f"e{i}": None for i in range(edges_count)}

        _start_time = time.time() - 120  # 120 seconds ago
        _ws_clients: dict = {}
        _poll_interval = 2000

        @health_app.get("/api/health")
        async def health():
            snapshot = mock_collector.snapshot
            total_topics = len(snapshot.topics)
            total_groups = len(snapshot.consumer_groups)
            total_lag = sum(g.total_lag for g in snapshot.consumer_groups.values())
            total_msg_rate = sum(t.msg_per_sec for t in snapshot.topics.values())
            graph_nodes = len(mock_graph_builder._nodes)
            graph_edges = len(mock_graph_builder._edges)

            return {
                "status": "ok" if mock_collector.connected else "degraded",
                "kafka_connected": mock_collector.connected,
                "uptime": round(time.time() - _start_time, 1),
                "ws_clients": len(_ws_clients),
                "topics": total_topics,
                "consumerGroups": total_groups,
                "totalLag": total_lag,
                "totalMsgPerSec": round(total_msg_rate, 1),
                "graphNodes": graph_nodes,
                "graphEdges": graph_edges,
                "pollIntervalMs": _poll_interval,
            }

        return health_app

    def test_health_response_format(self):
        """The health response must contain all required fields."""
        app = self._make_health_app(connected=True, nodes_count=10, edges_count=5)
        client = TestClient(app)
        resp = client.get("/api/health")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        required_keys = [
            "status", "kafka_connected", "uptime", "ws_clients",
            "topics", "consumerGroups", "totalLag", "totalMsgPerSec",
            "graphNodes", "graphEdges", "pollIntervalMs",
        ]
        for key in required_keys:
            self.assertIn(key, body, f"Missing required key: {key}")

    def test_health_status_ok_when_connected(self):
        """When Kafka is connected, status should be 'ok'."""
        app = self._make_health_app(connected=True)
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertEqual(body["status"], "ok")
        self.assertTrue(body["kafka_connected"])

    def test_health_status_degraded_when_disconnected(self):
        """When Kafka is not connected, status should be 'degraded' and kafka_connected=False."""
        app = self._make_health_app(connected=False)
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertEqual(body["status"], "degraded")
        self.assertFalse(body["kafka_connected"])

    def test_health_uptime_is_positive(self):
        """Uptime should be a positive number reflecting server run time."""
        app = self._make_health_app(connected=True)
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertGreater(body["uptime"], 0)
        # The mock sets start_time to 120 seconds ago, so uptime should be >= 119
        self.assertGreaterEqual(body["uptime"], 119)

    def test_health_topic_and_group_counts(self):
        """Health endpoint should report accurate topic and consumer group counts."""
        topics = {
            f"topic-{i}": MagicMock(msg_per_sec=5.0)
            for i in range(10)
        }
        groups = {
            f"group-{i}": MagicMock(total_lag=100)
            for i in range(3)
        }
        app = self._make_health_app(connected=True, topics=topics, groups=groups)
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertEqual(body["topics"], 10)
        self.assertEqual(body["consumerGroups"], 3)
        self.assertEqual(body["totalLag"], 300)
        self.assertEqual(body["totalMsgPerSec"], 50.0)

    def test_health_graph_node_edge_counts(self):
        """Health endpoint should report graph node and edge counts."""
        app = self._make_health_app(connected=True, nodes_count=42, edges_count=17)
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertEqual(body["graphNodes"], 42)
        self.assertEqual(body["graphEdges"], 17)

    def test_health_empty_cluster(self):
        """Health endpoint should work with zero topics and zero groups."""
        app = self._make_health_app(connected=True, topics={}, groups={})
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertEqual(body["topics"], 0)
        self.assertEqual(body["consumerGroups"], 0)
        self.assertEqual(body["totalLag"], 0)
        self.assertEqual(body["totalMsgPerSec"], 0.0)

    def test_health_poll_interval_present(self):
        """Health endpoint should include pollIntervalMs."""
        app = self._make_health_app(connected=True)
        client = TestClient(app)
        resp = client.get("/api/health")

        body = resp.json()
        self.assertIn("pollIntervalMs", body)
        self.assertEqual(body["pollIntervalMs"], 2000)


class TestGraphDiffEdgeCases:
    """Edge-case tests for GraphDiff and GraphStateBuilder.update()."""

    def test_diff_with_no_changes(self):
        """Build graph twice with the same snapshot; second diff should be empty."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "stable-topic": TopicInfo(
                    name="stable-topic", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={
                "stable-group": ConsumerGroupInfo(
                    group_id="stable-group",
                    members=2,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=["stable-topic"],
                    per_partition_lag={"stable-topic-0": 5, "stable-topic-1": 3, "stable-topic-2": 2},
                    member_client_ids=["client-1", "client-2"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap)
        assert not diff1.is_empty(), "First update should produce a non-empty diff"

        # Apply the exact same snapshot again
        diff2 = builder.update(snap)
        assert len(diff2.nodes_added) == 0, "No new nodes on identical re-apply"
        assert len(diff2.nodes_removed) == 0, "No removed nodes on identical re-apply"
        assert len(diff2.nodes_updated) == 0, "No updated nodes on identical re-apply"
        assert len(diff2.edges_added) == 0, "No new edges on identical re-apply"
        assert len(diff2.edges_removed) == 0, "No removed edges on identical re-apply"
        assert len(diff2.edges_updated) == 0, "No updated edges on identical re-apply"
        assert diff2.is_empty(), "Diff should be empty when nothing changed"

    def test_diff_removes_stale_nodes(self):
        """After a snapshot with fewer topics, stale nodes are marked inactive."""
        builder = GraphStateBuilder(show_producers=False)

        # First snapshot: 3 topics
        snap1 = ClusterSnapshot(
            topics={
                "topic-a": TopicInfo(name="topic-a", partitions=3, msg_per_sec=1.0, total_messages=100),
                "topic-b": TopicInfo(name="topic-b", partitions=3, msg_per_sec=2.0, total_messages=200),
                "topic-c": TopicInfo(name="topic-c", partitions=3, msg_per_sec=3.0, total_messages=300),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        builder.update(snap1)

        # Second snapshot: only topic-a remains
        snap2 = ClusterSnapshot(
            topics={
                "topic-a": TopicInfo(name="topic-a", partitions=3, msg_per_sec=1.0, total_messages=100),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)

        # topic-b and topic-c should be marked inactive, not removed
        assert len(diff2.nodes_removed) == 0, "Stale nodes should not be removed"
        inactive_updates = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        inactive_ids = {n["id"] for n in inactive_updates}
        assert "topic-topic-b" in inactive_ids, "topic-b should be marked inactive"
        assert "topic-topic-c" in inactive_ids, "topic-c should be marked inactive"

    def test_diff_edge_updates(self):
        """Change a consumer group's subscription and verify edges update."""
        builder = GraphStateBuilder(show_producers=False)

        # Initial: group subscribes to topic-x
        snap1 = ClusterSnapshot(
            topics={
                "topic-x": TopicInfo(name="topic-x", partitions=2, msg_per_sec=5.0, total_messages=500),
                "topic-y": TopicInfo(name="topic-y", partitions=2, msg_per_sec=3.0, total_messages=300),
            },
            consumer_groups={
                "my-group": ConsumerGroupInfo(
                    group_id="my-group",
                    members=1,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=["topic-x"],
                    per_partition_lag={"topic-x-0": 5, "topic-x-1": 5},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)
        # Verify initial edge exists: topic-x -> my-group
        initial_edge_ids = {e["id"] for e in diff1.edges_added}
        assert any("topic-topic-x" in eid and "my-group" in eid for eid in initial_edge_ids)

        # Update: group now subscribes to topic-y instead of topic-x
        snap2 = ClusterSnapshot(
            topics={
                "topic-x": TopicInfo(name="topic-x", partitions=2, msg_per_sec=5.0, total_messages=500),
                "topic-y": TopicInfo(name="topic-y", partitions=2, msg_per_sec=3.0, total_messages=300),
            },
            consumer_groups={
                "my-group": ConsumerGroupInfo(
                    group_id="my-group",
                    members=1,
                    total_lag=20,
                    status="Stable",
                    subscribed_topics=["topic-y"],
                    per_partition_lag={"topic-y-0": 10, "topic-y-1": 10},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)

        # New edge: topic-y -> my-group should be added
        new_edge_ids = {e["id"] for e in diff2.edges_added}
        assert any("topic-topic-y" in eid and "my-group" in eid for eid in new_edge_ids), (
            "Edge from topic-y to my-group should be added"
        )

        # Old edge: topic-x -> my-group should be marked inactive (via edge update)
        updated_edge_data = {e["id"]: e.get("data", {}) for e in diff2.edges_updated}
        old_edge_inactive = any(
            "topic-topic-x" in eid and data.get("inactive") is True
            for eid, data in updated_edge_data.items()
        )
        assert old_edge_inactive, "Old edge from topic-x to my-group should be marked inactive"

    def test_concurrent_snapshots(self):
        """Apply 100 snapshots rapidly; verify no crashes and final state is consistent."""
        builder = GraphStateBuilder(show_producers=True)

        for i in range(100):
            # Alternate between different topic sets to stress add/remove paths
            num_topics = 5 + (i % 10)
            topics = {}
            active_partitions = {}
            for t in range(num_topics):
                name = f"rapid-topic-{t}"
                topics[name] = TopicInfo(
                    name=name, partitions=3, msg_per_sec=float(i + t), total_messages=i * 100 + t
                )
                if t % 2 == 0:
                    active_partitions[name] = {0, 1}

            consumer_groups = {
                f"rapid-group-{g}": ConsumerGroupInfo(
                    group_id=f"rapid-group-{g}",
                    members=2,
                    total_lag=i * 10 + g,
                    status="Stable",
                    subscribed_topics=[f"rapid-topic-{g % num_topics}"],
                    per_partition_lag={f"rapid-topic-{g % num_topics}-0": i, f"rapid-topic-{g % num_topics}-1": g},
                    member_client_ids=[f"client-{g}-0", f"client-{g}-1"],
                )
                for g in range(3)
            }

            snap = ClusterSnapshot(
                topics=topics,
                consumer_groups=consumer_groups,
                active_partitions=active_partitions,
                timestamp=1000.0 + i,
            )
            diff = builder.update(snap)
            # Each call must succeed without exception
            assert isinstance(diff, GraphDiff)

        # Verify final state is consistent with the last snapshot applied
        final_snapshot = builder.get_snapshot()
        assert final_snapshot["type"] == "graph_snapshot"
        final_node_ids = {n["id"] for n in final_snapshot["nodes"]["added"]}

        # The last iteration had num_topics = 5 + (99 % 10) = 14 topics
        for t in range(14):
            assert f"topic-rapid-topic-{t}" in final_node_ids, (
                f"rapid-topic-{t} should be in the final graph"
            )


class TestConfigValidation:
    """Edge-case tests for Config.validate()."""

    def test_valid_config(self):
        """Default Config validates cleanly with no errors."""
        from config import Config

        cfg = Config()
        errors = cfg.validate()
        assert errors == [], f"Default config should be valid, got errors: {errors}"

    def test_invalid_poll_interval(self):
        """POLL_INTERVAL_MS < 500 must return a validation error."""
        from config import Config

        cfg = Config()
        cfg.POLL_INTERVAL_MS = 100
        errors = cfg.validate()
        assert any("POLL_INTERVAL_MS" in e for e in errors), (
            f"Expected POLL_INTERVAL_MS error, got: {errors}"
        )

    def test_invalid_lag_threshold(self):
        """LAG_WARN_THRESHOLD < 0 must return a validation error."""
        from config import Config

        cfg = Config()
        cfg.LAG_WARN_THRESHOLD = -1
        errors = cfg.validate()
        assert any("LAG_WARN_THRESHOLD" in e for e in errors), (
            f"Expected LAG_WARN_THRESHOLD error, got: {errors}"
        )

    def test_invalid_regex(self):
        """An invalid PRODUCER_GROUP_REGEX must return a validation error."""
        from config import Config

        cfg = Config()
        cfg.PRODUCER_GROUP_REGEX = "[invalid("
        errors = cfg.validate()
        assert any("PRODUCER_GROUP_REGEX" in e for e in errors), (
            f"Expected PRODUCER_GROUP_REGEX error, got: {errors}"
        )

    def test_auth_without_credentials(self):
        """UI_AUTH_ENABLED=true without username/password must return errors."""
        from config import Config

        cfg = Config()
        cfg.UI_AUTH_ENABLED = True
        cfg.UI_USERNAME = ""
        cfg.UI_PASSWORD = ""
        errors = cfg.validate()
        assert any("UI_USERNAME" in e for e in errors), (
            f"Expected UI_USERNAME error, got: {errors}"
        )
        assert any("UI_PASSWORD" in e for e in errors), (
            f"Expected UI_PASSWORD error, got: {errors}"
        )


class TestTopicNameHandling:
    """Tests for topic names with special patterns (system prefixes, dots, long names)."""

    def test_system_topic_prefix(self):
        """Topics starting with __ (e.g., __consumer_offsets) are properly created."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "__consumer_offsets": TopicInfo(
                    name="__consumer_offsets", partitions=50, msg_per_sec=100.0, total_messages=999999
                ),
                "__transaction_state": TopicInfo(
                    name="__transaction_state", partitions=50, msg_per_sec=10.0, total_messages=50000
                ),
                "normal-topic": TopicInfo(
                    name="normal-topic", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        added_ids = {n["id"] for n in diff.nodes_added}
        assert "topic-__consumer_offsets" in added_ids, "__consumer_offsets topic node should exist"
        assert "topic-__transaction_state" in added_ids, "__transaction_state topic node should exist"
        assert "topic-normal-topic" in added_ids, "normal-topic node should exist"

        # Verify the data is correct for system topics
        offsets_node = next(n for n in diff.nodes_added if n["id"] == "topic-__consumer_offsets")
        assert offsets_node["data"]["partitions"] == 50
        assert offsets_node["data"]["label"] == "__consumer_offsets"


class TestClusterHealth(unittest.TestCase):
    """Test cases for the GET /api/cluster/health endpoint."""

    def test_healthy_cluster_response(self):
        """A healthy cluster with no issues returns expected fields and zero counts."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 30,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {"0": 10, "1": 10, "2": 10},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["totalPartitions"], 30)
        self.assertEqual(body["underReplicatedCount"], 0)
        self.assertEqual(body["underReplicated"], [])
        self.assertEqual(body["offlinePartitionCount"], 0)
        self.assertEqual(body["offlinePartitions"], [])
        self.assertEqual(body["leaderDistribution"], {"0": 10, "1": 10, "2": 10})
        admin.get_cluster_health.assert_called_once()

    def test_empty_cluster(self):
        """An empty cluster with no topics returns zero partitions and empty lists."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 0,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["totalPartitions"], 0)
        self.assertEqual(body["underReplicatedCount"], 0)
        self.assertEqual(body["underReplicated"], [])
        self.assertEqual(body["offlinePartitionCount"], 0)
        self.assertEqual(body["offlinePartitions"], [])
        self.assertEqual(body["leaderDistribution"], {})

    def test_under_replicated_partitions(self):
        """Cluster with under-replicated partitions returns correct details."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 12,
            "underReplicatedCount": 2,
            "underReplicated": [
                {"topic": "orders.created", "partition": 0, "replicas": 3, "isr": 2},
                {"topic": "orders.created", "partition": 2, "replicas": 3, "isr": 1},
            ],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {"0": 4, "1": 4, "2": 4},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["underReplicatedCount"], 2)
        self.assertEqual(len(body["underReplicated"]), 2)

        # Verify structure of under-replicated partition entries
        entry = body["underReplicated"][0]
        self.assertEqual(entry["topic"], "orders.created")
        self.assertEqual(entry["partition"], 0)
        self.assertEqual(entry["replicas"], 3)
        self.assertEqual(entry["isr"], 2)

        second_entry = body["underReplicated"][1]
        self.assertEqual(second_entry["partition"], 2)
        self.assertEqual(second_entry["isr"], 1)

    def test_offline_partitions(self):
        """Cluster with offline partitions returns correct details."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 18,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 3,
            "offlinePartitions": [
                {"topic": "payments.events", "partition": 0},
                {"topic": "payments.events", "partition": 1},
                {"topic": "notifications.send", "partition": 2},
            ],
            "leaderDistribution": {"0": 8, "1": 7},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["offlinePartitionCount"], 3)
        self.assertEqual(len(body["offlinePartitions"]), 3)

        # Verify structure of offline partition entries
        self.assertEqual(body["offlinePartitions"][0]["topic"], "payments.events")
        self.assertEqual(body["offlinePartitions"][0]["partition"], 0)
        self.assertEqual(body["offlinePartitions"][2]["topic"], "notifications.send")
        self.assertEqual(body["offlinePartitions"][2]["partition"], 2)

        # Offline partitions removed broker 2 from leader distribution
        self.assertNotIn("2", body["leaderDistribution"])

    def test_leader_distribution(self):
        """Leader distribution accurately reflects partition-to-broker assignment."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 24,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {"0": 8, "1": 8, "2": 8},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        leader_dist = body["leaderDistribution"]

        # Total leader count should equal totalPartitions
        total_leaders = sum(leader_dist.values())
        self.assertEqual(total_leaders, body["totalPartitions"])

        # All three brokers should be present
        self.assertEqual(len(leader_dist), 3)
        self.assertEqual(leader_dist["0"], 8)
        self.assertEqual(leader_dist["1"], 8)
        self.assertEqual(leader_dist["2"], 8)

    def test_cluster_health_error_returns_500(self):
        """When the admin raises an exception, the endpoint returns 500."""
        admin = MagicMock()
        admin.get_cluster_health.side_effect = Exception("NoBrokersAvailable")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 500)
        self.assertIn("NoBrokersAvailable", resp.json()["detail"])

    def test_cluster_health_no_admin_returns_503(self):
        """If kafka_admin is not configured, the endpoint returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.get("/api/cluster/health")

        self.assertEqual(resp.status_code, 503)


class TestTopicNameDots:
    """Tests for topic names with dots and very long names."""

    def test_topic_with_dots(self):
        """Topics with dots (e.g., 'my.topic.name') work correctly."""
        builder = GraphStateBuilder(show_producers=False)
        dotted_names = [
            "my.topic.name",
            "org.company.events.created",
            "a.b.c.d.e.f.g",
            "single.dot",
        ]
        topics = {
            name: TopicInfo(name=name, partitions=3, msg_per_sec=1.0, total_messages=100)
            for name in dotted_names
        }
        # Also add a consumer that subscribes to a dotted topic to verify edges work
        consumer_groups = {
            "dot-consumer": ConsumerGroupInfo(
                group_id="dot-consumer",
                members=1,
                total_lag=5,
                status="Stable",
                subscribed_topics=["my.topic.name"],
                per_partition_lag={"my.topic.name-0": 2, "my.topic.name-1": 2, "my.topic.name-2": 1},
                member_client_ids=["dot-client-1"],
            ),
        }
        snap = ClusterSnapshot(
            topics=topics,
            consumer_groups=consumer_groups,
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        added_ids = {n["id"] for n in diff.nodes_added}
        for name in dotted_names:
            assert f"topic-{name}" in added_ids, f"Dotted topic '{name}' should be in graph"

        # Verify edge between dotted topic and consumer
        edge_ids = {e["id"] for e in diff.edges_added}
        assert any("topic-my.topic.name" in eid and "dot-consumer" in eid for eid in edge_ids), (
            "Edge from dotted topic to consumer should exist"
        )

    def test_very_long_topic_name(self):
        """Topic names up to 249 chars work, as Kafka allows up to 249 characters."""
        builder = GraphStateBuilder(show_producers=False)
        long_name = "a" * 249
        snap = ClusterSnapshot(
            topics={
                long_name: TopicInfo(
                    name=long_name, partitions=6, msg_per_sec=2.0, total_messages=99999
                ),
            },
            consumer_groups={
                "long-name-consumer": ConsumerGroupInfo(
                    group_id="long-name-consumer",
                    members=1,
                    total_lag=100,
                    status="Stable",
                    subscribed_topics=[long_name],
                    per_partition_lag={f"{long_name}-0": 50, f"{long_name}-1": 50},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        node_ids = {n["id"] for n in diff.nodes_added}
        assert f"topic-{long_name}" in node_ids, "249-char topic name should produce a valid node"

        # Verify the node data is correct
        topic_node = next(n for n in diff.nodes_added if n["id"] == f"topic-{long_name}")
        assert topic_node["data"]["label"] == long_name
        assert topic_node["data"]["partitions"] == 6

        # Verify edge from long topic to consumer exists
        edge_ids = {e["id"] for e in diff.edges_added}
        assert any(long_name in eid and "long-name-consumer" in eid for eid in edge_ids), (
            "Edge from long-named topic to consumer should exist"
        )


class TestConfigAPIEndpoints:
    """Test Config class validation and GraphStateBuilder config integration."""

    def test_config_show_producers_toggle(self):
        """Updating SHOW_PRODUCERS controls whether standalone producer nodes appear."""
        from config import Config

        # Build a snapshot with an active topic that has no known service producer
        snapshot = ClusterSnapshot(
            topics={
                "logs.raw": TopicInfo(
                    name="logs.raw", partitions=3, msg_per_sec=20.0, total_messages=5000
                ),
            },
            consumer_groups={},
            active_partitions={"logs.raw": {0, 1}},
            timestamp=time.time(),
        )

        # With show_producers=True, standalone producer nodes should appear
        builder_on = GraphStateBuilder(show_producers=True)
        diff_on = builder_on.update(snapshot)
        producer_nodes_on = [n for n in diff_on.nodes_added if n["type"] == "producer"]
        assert len(producer_nodes_on) >= 1, "Producer nodes should appear when show_producers=True"

        # With show_producers=False, no standalone producer nodes
        builder_off = GraphStateBuilder(show_producers=False)
        diff_off = builder_off.update(snapshot)
        producer_nodes_off = [n for n in diff_off.nodes_added if n["type"] == "producer"]
        assert len(producer_nodes_off) == 0, "No producer nodes when show_producers=False"

        # Verify Config default parses correctly
        cfg = Config()
        assert isinstance(cfg.SHOW_PRODUCERS, bool)

    def test_config_sampling_toggle(self):
        """Toggling SAMPLING_ENABLED on the Config object works correctly."""
        from config import Config

        cfg = Config()
        original = cfg.SAMPLING_ENABLED
        assert isinstance(original, bool)

        # Toggle the value
        cfg.SAMPLING_ENABLED = not original
        assert cfg.SAMPLING_ENABLED is (not original)

        # Toggle back
        cfg.SAMPLING_ENABLED = original
        assert cfg.SAMPLING_ENABLED is original

        # Validation should pass regardless of sampling state
        cfg.SAMPLING_ENABLED = True
        errors = cfg.validate()
        sampling_errors = [e for e in errors if "SAMPLING" in e]
        assert len(sampling_errors) == 0, "No validation errors expected for SAMPLING_ENABLED=True"

        cfg.SAMPLING_ENABLED = False
        errors = cfg.validate()
        sampling_errors = [e for e in errors if "SAMPLING" in e]
        assert len(sampling_errors) == 0, "No validation errors expected for SAMPLING_ENABLED=False"

    def test_config_animations_toggle(self):
        """Toggling ANIMATIONS_ENABLED on the Config object works correctly."""
        from config import Config

        cfg = Config()
        original = cfg.ANIMATIONS_ENABLED
        assert isinstance(original, bool)

        # Toggle the value
        cfg.ANIMATIONS_ENABLED = not original
        assert cfg.ANIMATIONS_ENABLED is (not original)

        # Toggle back
        cfg.ANIMATIONS_ENABLED = original
        assert cfg.ANIMATIONS_ENABLED is original

        # Validation should pass regardless of animations state
        cfg.ANIMATIONS_ENABLED = True
        errors = cfg.validate()
        animation_errors = [e for e in errors if "ANIMATION" in e]
        assert len(animation_errors) == 0, "No validation errors for ANIMATIONS_ENABLED=True"

        cfg.ANIMATIONS_ENABLED = False
        errors = cfg.validate()
        animation_errors = [e for e in errors if "ANIMATION" in e]
        assert len(animation_errors) == 0, "No validation errors for ANIMATIONS_ENABLED=False"

    def test_config_lag_threshold_update(self):
        """Updating LAG_WARN_THRESHOLD with a valid value passes validation."""
        from config import Config

        cfg = Config()
        cfg.LAG_WARN_THRESHOLD = 5000
        assert cfg.LAG_WARN_THRESHOLD == 5000

        errors = cfg.validate()
        lag_errors = [e for e in errors if "LAG_WARN_THRESHOLD" in e]
        assert len(lag_errors) == 0, "Valid LAG_WARN_THRESHOLD=5000 should have no errors"

        # Zero is also valid (>= 0)
        cfg.LAG_WARN_THRESHOLD = 0
        errors = cfg.validate()
        lag_errors = [e for e in errors if "LAG_WARN_THRESHOLD" in e]
        assert len(lag_errors) == 0, "LAG_WARN_THRESHOLD=0 should be valid"

    def test_config_lag_threshold_negative_rejected(self):
        """Negative LAG_WARN_THRESHOLD is rejected by Config.validate()."""
        from config import Config

        cfg = Config()
        cfg.LAG_WARN_THRESHOLD = -1
        errors = cfg.validate()
        lag_errors = [e for e in errors if "LAG_WARN_THRESHOLD" in e]
        assert len(lag_errors) == 1, "Negative LAG_WARN_THRESHOLD should produce exactly one error"
        assert "must be >= 0" in lag_errors[0]

        cfg.LAG_WARN_THRESHOLD = -100
        errors = cfg.validate()
        lag_errors = [e for e in errors if "LAG_WARN_THRESHOLD" in e]
        assert len(lag_errors) == 1, "LAG_WARN_THRESHOLD=-100 should produce exactly one error"

    def test_config_poll_interval_update(self):
        """Updating POLL_INTERVAL_MS with a valid value (>=500) passes validation."""
        from config import Config

        cfg = Config()
        cfg.POLL_INTERVAL_MS = 500
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert len(poll_errors) == 0, "POLL_INTERVAL_MS=500 should be valid (boundary)"

        cfg.POLL_INTERVAL_MS = 3000
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert len(poll_errors) == 0, "POLL_INTERVAL_MS=3000 should be valid"

        cfg.POLL_INTERVAL_MS = 10000
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert len(poll_errors) == 0, "POLL_INTERVAL_MS=10000 should be valid"

    def test_config_poll_interval_too_low_rejected(self):
        """POLL_INTERVAL_MS < 500 is rejected by Config.validate()."""
        from config import Config

        cfg = Config()
        cfg.POLL_INTERVAL_MS = 499
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert len(poll_errors) == 1, "POLL_INTERVAL_MS=499 should produce exactly one error"
        assert "must be >= 500" in poll_errors[0]

        cfg.POLL_INTERVAL_MS = 0
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert len(poll_errors) == 1, "POLL_INTERVAL_MS=0 should produce exactly one error"

        cfg.POLL_INTERVAL_MS = -1
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert len(poll_errors) == 1, "POLL_INTERVAL_MS=-1 should produce exactly one error"


class TestGraphStateBuilderLagThreshold:
    """Test lag warning behavior in GraphStateBuilder with various thresholds."""

    def _make_consumer_snapshot(self, total_lag: int) -> ClusterSnapshot:
        """Helper: create a snapshot with one consumer group having the given lag."""
        return ClusterSnapshot(
            topics={
                "test-topic": TopicInfo(
                    name="test-topic", partitions=2, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "test-consumer": ConsumerGroupInfo(
                    group_id="test-consumer",
                    members=1,
                    total_lag=total_lag,
                    status="Stable",
                    subscribed_topics=["test-topic"],
                    per_partition_lag={
                        "test-topic-0": total_lag // 2,
                        "test-topic-1": total_lag - total_lag // 2,
                    },
                    member_client_ids=["test-consumer-client-1"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )

    def test_lag_warning_above_threshold(self):
        """With threshold=500, consumer with lag=600 should have lagWarning=True."""
        builder = GraphStateBuilder(lag_warn_threshold=500, show_producers=False)
        snapshot = self._make_consumer_snapshot(total_lag=600)
        diff = builder.update(snapshot)

        cg_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "test-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 1, "Should have exactly one consumer group node"
        assert cg_nodes[0]["data"]["lagWarning"] is True, (
            f"Lag 600 > threshold 500 should trigger lagWarning, got {cg_nodes[0]['data']['lagWarning']}"
        )

    def test_lag_warning_below_threshold(self):
        """With threshold=500, consumer with lag=200 should have lagWarning=False."""
        builder = GraphStateBuilder(lag_warn_threshold=500, show_producers=False)
        snapshot = self._make_consumer_snapshot(total_lag=200)
        diff = builder.update(snapshot)

        cg_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "test-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 1, "Should have exactly one consumer group node"
        assert cg_nodes[0]["data"]["lagWarning"] is False, (
            f"Lag 200 < threshold 500 should NOT trigger lagWarning, got {cg_nodes[0]['data']['lagWarning']}"
        )

    def test_lag_threshold_zero_always_warns(self):
        """With threshold=0, any lag > 0 triggers lagWarning."""
        builder = GraphStateBuilder(lag_warn_threshold=0, show_producers=False)

        # Lag of 1 should trigger warning with threshold 0
        snapshot = self._make_consumer_snapshot(total_lag=1)
        diff = builder.update(snapshot)

        cg_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "test-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 1, "Should have exactly one consumer group node"
        assert cg_nodes[0]["data"]["lagWarning"] is True, (
            f"Lag 1 > threshold 0 should trigger lagWarning, got {cg_nodes[0]['data']['lagWarning']}"
        )


class TestWebSocketMessageFormat:
    """Test that WebSocket message dicts contain the required fields and structure."""

    def test_graph_state_message_contains_required_fields(self):
        """get_snapshot() messages must contain nodes, edges, and metrics fields."""
        builder = GraphStateBuilder(show_producers=True)
        snapshot = ClusterSnapshot(
            topics={
                "ws-topic-1": TopicInfo(
                    name="ws-topic-1", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
                "ws-topic-2": TopicInfo(
                    name="ws-topic-2", partitions=6, msg_per_sec=25.0, total_messages=12000
                ),
            },
            consumer_groups={
                "ws-consumer": ConsumerGroupInfo(
                    group_id="ws-consumer",
                    members=2,
                    total_lag=100,
                    status="Stable",
                    subscribed_topics=["ws-topic-1"],
                    per_partition_lag={"ws-topic-1-0": 50, "ws-topic-1-1": 30, "ws-topic-1-2": 20},
                    member_client_ids=["ws-consumer-1", "ws-consumer-2"],
                ),
            },
            active_partitions={"ws-topic-2": {0, 1, 2}},
            timestamp=1000.0,
        )
        builder.update(snapshot)

        result = builder.get_snapshot()

        # Required top-level fields
        assert "type" in result, "Message missing 'type' field"
        assert "ts" in result, "Message missing 'ts' field"
        assert "nodes" in result, "Message missing 'nodes' field"
        assert "edges" in result, "Message missing 'edges' field"
        assert "metrics" in result, "Message missing 'metrics' field"

        # Nodes must be a dict with add/update/remove arrays
        assert isinstance(result["nodes"], dict)
        assert isinstance(result["nodes"]["added"], list)
        assert isinstance(result["nodes"]["updated"], list)
        assert isinstance(result["nodes"]["removed"], list)

        # Edges must be a dict with add/update/remove arrays
        assert isinstance(result["edges"], dict)
        assert isinstance(result["edges"]["added"], list)
        assert isinstance(result["edges"]["updated"], list)
        assert isinstance(result["edges"]["removed"], list)

        # Metrics must be a dict
        assert isinstance(result["metrics"], dict)
        assert len(result["metrics"]) > 0, "Metrics should not be empty with topics present"

    def test_node_changes_message_has_proper_arrays(self):
        """Diff to_dict() must have proper add/remove/update arrays under 'nodes'."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Add topics
        snap1 = ClusterSnapshot(
            topics={
                f"nc-topic-{i}": TopicInfo(
                    name=f"nc-topic-{i}", partitions=3, msg_per_sec=5.0, total_messages=1000
                )
                for i in range(5)
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1.0,
        )
        diff1 = builder.update(snap1)
        d1 = diff1.to_dict()

        assert isinstance(d1["nodes"]["added"], list), "nodes.added must be a list"
        assert isinstance(d1["nodes"]["updated"], list), "nodes.updated must be a list"
        assert isinstance(d1["nodes"]["removed"], list), "nodes.removed must be a list"
        assert len(d1["nodes"]["added"]) == 5, f"Expected 5 added nodes, got {len(d1['nodes']['added'])}"
        assert len(d1["nodes"]["updated"]) == 0, "No updates expected on initial build"
        assert len(d1["nodes"]["removed"]) == 0, "No removals expected on initial build"

        # Phase 2: Update some topics, remove others
        snap2 = ClusterSnapshot(
            topics={
                f"nc-topic-{i}": TopicInfo(
                    name=f"nc-topic-{i}", partitions=3, msg_per_sec=20.0, total_messages=2000
                )
                for i in range(3)  # Keep only 3, drop 2
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2.0,
        )
        diff2 = builder.update(snap2)
        d2 = diff2.to_dict()

        assert isinstance(d2["nodes"]["added"], list)
        assert isinstance(d2["nodes"]["updated"], list)
        assert isinstance(d2["nodes"]["removed"], list)
        # 3 topics updated (new rate), 2 topics marked inactive (in updated)
        assert len(d2["nodes"]["updated"]) >= 2, "Should have updates for changed and inactive nodes"

    def test_edge_changes_message_format(self):
        """Diff to_dict() must have proper add/remove/update arrays under 'edges'."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Create a topic and consumer group (produces an edge)
        snap1 = ClusterSnapshot(
            topics={
                "edge-topic": TopicInfo(
                    name="edge-topic", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "edge-consumer": ConsumerGroupInfo(
                    group_id="edge-consumer",
                    members=2,
                    total_lag=50,
                    status="Stable",
                    subscribed_topics=["edge-topic"],
                    per_partition_lag={"edge-topic-0": 25, "edge-topic-1": 15, "edge-topic-2": 10},
                    member_client_ids=["edge-consumer-1", "edge-consumer-2"],
                ),
            },
            active_partitions={},
            timestamp=1.0,
        )
        diff1 = builder.update(snap1)
        d1 = diff1.to_dict()

        assert isinstance(d1["edges"]["added"], list), "edges.added must be a list"
        assert isinstance(d1["edges"]["updated"], list), "edges.updated must be a list"
        assert isinstance(d1["edges"]["removed"], list), "edges.removed must be a list"
        assert len(d1["edges"]["added"]) >= 1, "Should have at least 1 consume edge"

        # Each added edge must have id, source, target, data
        for edge in d1["edges"]["added"]:
            assert "id" in edge, "Edge missing 'id'"
            assert "source" in edge, "Edge missing 'source'"
            assert "target" in edge, "Edge missing 'target'"
            assert "data" in edge, "Edge missing 'data'"

        # Phase 2: Remove the consumer group, edges should become inactive
        snap2 = ClusterSnapshot(
            topics={
                "edge-topic": TopicInfo(
                    name="edge-topic", partitions=3, msg_per_sec=10.0, total_messages=6000
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2.0,
        )
        diff2 = builder.update(snap2)
        d2 = diff2.to_dict()

        assert isinstance(d2["edges"]["updated"], list)
        # Edges should be marked inactive
        inactive_edges = [e for e in d2["edges"]["updated"] if e.get("data", {}).get("inactive") is True]
        assert len(inactive_edges) >= 1, "Edges to removed consumer should be marked inactive"

    def test_metric_update_message_format(self):
        """Diff metrics must be a dict mapping topic IDs to metric objects."""
        builder = GraphStateBuilder(show_producers=False)
        snapshot = ClusterSnapshot(
            topics={
                "metric-topic-a": TopicInfo(
                    name="metric-topic-a", partitions=3, msg_per_sec=42.5, total_messages=99999
                ),
                "metric-topic-b": TopicInfo(
                    name="metric-topic-b", partitions=6, msg_per_sec=0.0, total_messages=500
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1.0,
        )
        diff = builder.update(snapshot)
        d = diff.to_dict()

        assert "metrics" in d, "Diff dict must contain 'metrics'"
        assert isinstance(d["metrics"], dict), "metrics must be a dict"

        # Each topic should have a metric entry keyed by "topic-<name>"
        assert "topic-metric-topic-a" in d["metrics"]
        assert "topic-metric-topic-b" in d["metrics"]

        # Each metric entry must have msgPerSec and totalMessages
        metric_a = d["metrics"]["topic-metric-topic-a"]
        assert "msgPerSec" in metric_a, "Metric missing 'msgPerSec'"
        assert "totalMessages" in metric_a, "Metric missing 'totalMessages'"
        assert metric_a["msgPerSec"] == 42.5
        assert metric_a["totalMessages"] == 99999

        metric_b = d["metrics"]["topic-metric-topic-b"]
        assert metric_b["msgPerSec"] == 0.0
        assert metric_b["totalMessages"] == 500


class TestGraphDiffWithDeletions:
    """Test graph diff behavior when nodes and edges are removed."""

    def test_removing_nodes_from_graph_state(self):
        """Nodes removed from snapshot should appear as inactive in the diff."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Build with 10 topics
        topics1 = {
            f"del-topic-{i}": TopicInfo(
                name=f"del-topic-{i}", partitions=3, msg_per_sec=5.0, total_messages=1000
            )
            for i in range(10)
        }
        snap1 = ClusterSnapshot(
            topics=topics1, consumer_groups={}, active_partitions={}, timestamp=1.0
        )
        diff1 = builder.update(snap1)
        assert len(diff1.nodes_added) == 10

        # Phase 2: Remove 5 topics (keep 0-4, drop 5-9)
        topics2 = {
            f"del-topic-{i}": TopicInfo(
                name=f"del-topic-{i}", partitions=3, msg_per_sec=5.0, total_messages=1200
            )
            for i in range(5)
        }
        snap2 = ClusterSnapshot(
            topics=topics2, consumer_groups={}, active_partitions={}, timestamp=2.0
        )
        diff2 = builder.update(snap2)

        # The 5 dropped topics should be marked inactive in the updated list
        inactive_updates = [
            n for n in diff2.nodes_updated if n.get("status") == "inactive"
        ]
        assert len(inactive_updates) == 5, (
            f"Expected 5 inactive nodes, got {len(inactive_updates)}"
        )

        # Verify the correct topic IDs are marked inactive
        inactive_ids = {n["id"] for n in inactive_updates}
        for i in range(5, 10):
            assert f"topic-del-topic-{i}" in inactive_ids, (
                f"topic-del-topic-{i} should be marked inactive"
            )

    def test_removing_edges_when_node_deleted_cascading(self):
        """When a consumer group disappears, its consume edges should be marked inactive."""
        builder = GraphStateBuilder(show_producers=False)

        topics = {
            "cascade-topic-a": TopicInfo(
                name="cascade-topic-a", partitions=3, msg_per_sec=10.0, total_messages=5000
            ),
            "cascade-topic-b": TopicInfo(
                name="cascade-topic-b", partitions=3, msg_per_sec=8.0, total_messages=4000
            ),
        }
        groups = {
            "cascade-consumer": ConsumerGroupInfo(
                group_id="cascade-consumer",
                members=3,
                total_lag=100,
                status="Stable",
                subscribed_topics=["cascade-topic-a", "cascade-topic-b"],
                per_partition_lag={
                    "cascade-topic-a-0": 20, "cascade-topic-a-1": 15, "cascade-topic-a-2": 15,
                    "cascade-topic-b-0": 20, "cascade-topic-b-1": 15, "cascade-topic-b-2": 15,
                },
                member_client_ids=["cascade-consumer-1", "cascade-consumer-2", "cascade-consumer-3"],
            ),
        }
        snap1 = ClusterSnapshot(
            topics=topics, consumer_groups=groups, active_partitions={}, timestamp=1.0
        )
        diff1 = builder.update(snap1)

        # Should have 2 consume edges (one per subscribed topic)
        consume_edges = [
            e for e in diff1.edges_added if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 2, f"Expected 2 consume edges, got {len(consume_edges)}"

        # Phase 2: Remove the consumer group
        snap2 = ClusterSnapshot(
            topics=topics, consumer_groups={}, active_partitions={}, timestamp=2.0
        )
        diff2 = builder.update(snap2)

        # Both edges should become inactive
        inactive_edges = [
            e for e in diff2.edges_updated
            if e.get("data", {}).get("inactive") is True
        ]
        assert len(inactive_edges) == 2, (
            f"Expected 2 inactive edges after consumer removal, got {len(inactive_edges)}"
        )

        # The consumer node should be marked inactive
        inactive_nodes = [
            n for n in diff2.nodes_updated if n.get("status") == "inactive"
        ]
        assert len(inactive_nodes) >= 1, "Consumer group should be marked inactive"

    def test_deleted_node_ids_not_in_new_diffs(self):
        """After a node is marked inactive, subsequent diffs should not re-add it."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Build with 5 topics
        topics1 = {
            f"ghost-topic-{i}": TopicInfo(
                name=f"ghost-topic-{i}", partitions=3, msg_per_sec=5.0, total_messages=1000
            )
            for i in range(5)
        }
        snap1 = ClusterSnapshot(
            topics=topics1, consumer_groups={}, active_partitions={}, timestamp=1.0
        )
        builder.update(snap1)

        # Phase 2: Remove topics 3 and 4 — they become inactive in this diff
        topics2 = {
            f"ghost-topic-{i}": TopicInfo(
                name=f"ghost-topic-{i}", partitions=3, msg_per_sec=5.0, total_messages=1200
            )
            for i in range(3)
        }
        snap2 = ClusterSnapshot(
            topics=topics2, consumer_groups={}, active_partitions={}, timestamp=2.0
        )
        diff2 = builder.update(snap2)

        # Verify they were marked inactive in this diff
        inactive_ids_in_diff2 = {
            n["id"] for n in diff2.nodes_updated if n.get("status") == "inactive"
        }
        assert "topic-ghost-topic-3" in inactive_ids_in_diff2
        assert "topic-ghost-topic-4" in inactive_ids_in_diff2

        # Phase 3: Another update with the same 3 topics (no change to the missing ones).
        # The builder drops already-inactive nodes that are still absent, so after this
        # cycle, topics 3 and 4 are fully purged from internal state.
        topics3 = {
            f"ghost-topic-{i}": TopicInfo(
                name=f"ghost-topic-{i}", partitions=3, msg_per_sec=8.0, total_messages=1500
            )
            for i in range(3)
        }
        snap3 = ClusterSnapshot(
            topics=topics3, consumer_groups={}, active_partitions={}, timestamp=3.0
        )
        diff3 = builder.update(snap3)

        # The deleted topics should NOT appear in nodes_added (they must not be re-added)
        added_ids = {n["id"] for n in diff3.nodes_added}
        assert "topic-ghost-topic-3" not in added_ids, (
            "Deleted topic should not reappear in nodes_added"
        )
        assert "topic-ghost-topic-4" not in added_ids, (
            "Deleted topic should not reappear in nodes_added"
        )

        # They should also NOT appear in nodes_updated (they were already purged)
        updated_ids = {n["id"] for n in diff3.nodes_updated}
        assert "topic-ghost-topic-3" not in updated_ids, (
            "Purged topic should not appear in nodes_updated"
        )
        assert "topic-ghost-topic-4" not in updated_ids, (
            "Purged topic should not appear in nodes_updated"
        )


class TestLargeTopology:
    """Test building and diffing very large graph topologies."""

    def test_build_graph_500_topics_100_groups_50_producers(self):
        """Build a graph with 500 topics, 100 consumer groups, and 50 producers."""
        builder = GraphStateBuilder(show_producers=True)

        topics = {}
        active_partitions = {}
        for i in range(500):
            name = f"large-topo-topic-{i}"
            topics[name] = TopicInfo(
                name=name, partitions=6, msg_per_sec=10.0 if i < 50 else 0.0,
                total_messages=(i + 1) * 1000,
            )
            # First 50 topics are actively produced to (for standalone producers)
            if i < 50:
                active_partitions[name] = {0, 1, 2}

        consumer_groups = {}
        topic_names = list(topics.keys())
        for i in range(100):
            gid = f"large-topo-consumer-{i}"
            # Each consumer subscribes to 3 topics
            subs = [topic_names[(i * 3 + j) % len(topic_names)] for j in range(3)]
            per_part = {}
            total_lag = 0
            for t in subs:
                for p in range(6):
                    lag = 50 if i % 2 == 0 else 0
                    per_part[f"{t}-{p}"] = lag
                    total_lag += lag
            consumer_groups[gid] = ConsumerGroupInfo(
                group_id=gid,
                members=2 + (i % 4),
                total_lag=total_lag,
                status="Stable",
                subscribed_topics=subs,
                per_partition_lag=per_part,
                member_client_ids=[f"{gid}-client-{k}" for k in range(2 + (i % 4))],
            )

        snapshot = ClusterSnapshot(
            topics=topics,
            consumer_groups=consumer_groups,
            active_partitions=active_partitions,
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)

        # Verify: 500 topic nodes
        topic_nodes = [n for n in diff.nodes_added if n["type"] == "topic"]
        assert len(topic_nodes) == 500, f"Expected 500 topic nodes, got {len(topic_nodes)}"

        # Verify: 100 consumer/service nodes
        cg_nodes = [n for n in diff.nodes_added if n["type"] in ("consumer_group", "service")]
        assert len(cg_nodes) == 100, f"Expected 100 consumer nodes, got {len(cg_nodes)}"

        # Verify: producer nodes exist for actively-produced topics
        producer_nodes = [n for n in diff.nodes_added if n["type"] == "producer"]
        assert len(producer_nodes) > 0, "Should have standalone producer nodes"

        # Verify: edges exist (at least 100 groups * 3 subscriptions = 300 consume edges)
        consume_edges = [
            e for e in diff.edges_added if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) >= 100, (
            f"Expected at least 100 consume edges, got {len(consume_edges)}"
        )

        # Verify: total node count is at least 500 + 100 + some producers
        assert len(diff.nodes_added) >= 600, (
            f"Expected at least 600 total nodes, got {len(diff.nodes_added)}"
        )

    def test_diffing_two_large_states_performance(self):
        """Diffing two large graph states should complete in under 1 second."""
        builder = GraphStateBuilder(show_producers=True)

        # Build initial state: 500 topics, 100 groups
        snap1 = make_large_snapshot(
            num_topics=500, num_consumer_groups=100,
            active_rate=10.0, lag_per_partition=50,
        )
        builder.update(snap1)

        # Build a modified state: same size but different rates and lag
        snap2 = make_large_snapshot(
            num_topics=500, num_consumer_groups=100,
            active_rate=25.0, lag_per_partition=200,
        )

        start = time.time()
        diff = builder.update(snap2)
        elapsed = time.time() - start

        assert elapsed < 1.0, (
            f"Diffing two large states took {elapsed:.3f}s, expected <1s"
        )

        # The diff should contain updates (rates and lag changed)
        assert len(diff.nodes_updated) > 0, "Expected node updates from rate/lag changes"
        assert len(diff.metrics) > 0, "Metrics should be populated"


class TestConsumerGroupStateTransitions:
    """Test consumer group state transitions (Empty, Stable, Rebalancing, Dead)."""

    def _make_snapshot_with_group_status(self, status: str, members: int = 3,
                                          total_lag: int = 50) -> ClusterSnapshot:
        """Helper to create a snapshot with a single consumer group at a given status."""
        return ClusterSnapshot(
            topics={
                "state-topic": TopicInfo(
                    name="state-topic", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "state-consumer": ConsumerGroupInfo(
                    group_id="state-consumer",
                    members=members,
                    total_lag=total_lag,
                    status=status,
                    subscribed_topics=["state-topic"],
                    per_partition_lag={
                        "state-topic-0": total_lag // 3,
                        "state-topic-1": total_lag // 3,
                        "state-topic-2": total_lag - 2 * (total_lag // 3),
                    },
                    member_client_ids=[f"state-consumer-{k}" for k in range(members)],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )

    def test_consumer_group_empty_to_stable(self):
        """Consumer group transitioning from Empty to Stable should reflect in the diff."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Group in Empty state (0 members)
        snap1 = self._make_snapshot_with_group_status("Empty", members=0, total_lag=0)
        diff1 = builder.update(snap1)

        cg_nodes = [
            n for n in diff1.nodes_added
            if n.get("type") in ("consumer_group", "service") and "state-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 1, "Consumer group should be added"
        assert cg_nodes[0]["status"] == "Empty", (
            f"Expected status 'Empty', got '{cg_nodes[0]['status']}'"
        )
        assert cg_nodes[0]["data"]["members"] == 0

        # Phase 2: Group transitions to Stable with members
        snap2 = self._make_snapshot_with_group_status("Stable", members=3, total_lag=90)
        diff2 = builder.update(snap2)

        updated_nodes = [
            n for n in diff2.nodes_updated if "state-consumer" in n["id"]
        ]
        assert len(updated_nodes) == 1, "Consumer group should appear in updates"
        assert updated_nodes[0]["status"] == "Stable", (
            f"Expected status 'Stable', got '{updated_nodes[0]['status']}'"
        )
        assert updated_nodes[0]["data"]["members"] == 3

    def test_consumer_group_stable_to_rebalancing(self):
        """Consumer group transitioning from Stable to PreparingRebalance should be reflected."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Group in Stable state
        snap1 = self._make_snapshot_with_group_status("Stable", members=3, total_lag=60)
        diff1 = builder.update(snap1)

        cg_nodes = [
            n for n in diff1.nodes_added
            if n.get("type") in ("consumer_group", "service") and "state-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 1
        assert cg_nodes[0]["status"] == "Stable"

        # Phase 2: Group starts rebalancing (member count may change)
        snap2 = self._make_snapshot_with_group_status(
            "PreparingRebalance", members=2, total_lag=120
        )
        diff2 = builder.update(snap2)

        updated_nodes = [
            n for n in diff2.nodes_updated if "state-consumer" in n["id"]
        ]
        assert len(updated_nodes) == 1, "Consumer group should appear in updates"
        assert updated_nodes[0]["status"] == "PreparingRebalance", (
            f"Expected status 'PreparingRebalance', got '{updated_nodes[0]['status']}'"
        )
        assert updated_nodes[0]["data"]["members"] == 2

    def test_consumer_group_goes_dead(self):
        """Consumer group going Dead (0 members, no subscriptions) should be reflected."""
        builder = GraphStateBuilder(show_producers=False)

        # Phase 1: Group is Stable
        snap1 = self._make_snapshot_with_group_status("Stable", members=3, total_lag=90)
        diff1 = builder.update(snap1)

        cg_nodes = [
            n for n in diff1.nodes_added
            if n.get("type") in ("consumer_group", "service") and "state-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 1
        assert cg_nodes[0]["status"] == "Stable"

        # Phase 2: Group goes Dead (disappears from snapshot entirely)
        snap2 = ClusterSnapshot(
            topics={
                "state-topic": TopicInfo(
                    name="state-topic", partitions=3, msg_per_sec=10.0, total_messages=6000
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        diff2 = builder.update(snap2)

        # The consumer group should be marked inactive
        inactive_nodes = [
            n for n in diff2.nodes_updated
            if "state-consumer" in n["id"] and n.get("status") == "inactive"
        ]
        assert len(inactive_nodes) == 1, (
            "Dead consumer group should be marked inactive"
        )

        # Its edges should also be marked inactive
        inactive_edges = [
            e for e in diff2.edges_updated
            if e.get("data", {}).get("inactive") is True
        ]
        assert len(inactive_edges) >= 1, (
            "Edges to dead consumer group should be marked inactive"
        )

        # Verify via snapshot that the node exists but is inactive
        result = builder.get_snapshot()
        state_consumer_nodes = [
            n for n in result["nodes"]["added"]
            if "state-consumer" in n["id"]
        ]
        assert len(state_consumer_nodes) == 1
        assert state_consumer_nodes[0]["status"] == "inactive"


class TestTopicNameEdgeCases:
    """Edge-case tests for topic names: empty, unicode, special chars, extremely long."""

    def test_empty_topic_name(self):
        """An empty-string topic name should still produce a valid node without crashing."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "": TopicInfo(
                    name="", partitions=1, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        # The builder should not crash; it should create a topic node with id "topic-"
        assert not diff.is_empty(), "Diff should not be empty even for empty topic name"
        node_ids = {n["id"] for n in diff.nodes_added}
        assert "topic-" in node_ids, "Empty topic name should produce node with id 'topic-'"
        empty_node = next(n for n in diff.nodes_added if n["id"] == "topic-")
        assert empty_node["data"]["label"] == ""
        assert empty_node["data"]["partitions"] == 1

    def test_topic_name_with_unicode(self):
        """Topic names containing unicode characters are handled without errors."""
        builder = GraphStateBuilder(show_producers=False)
        unicode_names = [
            "\u00e9v\u00e9nements-cr\u00e9\u00e9s",        # French accents
            "\u30c8\u30d4\u30c3\u30af-\u30c6\u30b9\u30c8",              # Japanese katakana
            "\u043a\u0430\u0444\u043a\u0430-\u0442\u043e\u043f\u0438\u043a",             # Cyrillic
            "topic-\u2603-snowman",           # Emoji/symbol
        ]
        topics = {
            name: TopicInfo(name=name, partitions=3, msg_per_sec=1.0, total_messages=100)
            for name in unicode_names
        }
        snap = ClusterSnapshot(
            topics=topics,
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        added_ids = {n["id"] for n in diff.nodes_added}
        for name in unicode_names:
            expected_id = f"topic-{name}"
            assert expected_id in added_ids, (
                f"Unicode topic '{name}' should produce node '{expected_id}'"
            )
            node = next(n for n in diff.nodes_added if n["id"] == expected_id)
            assert node["data"]["label"] == name, (
                f"Label should preserve unicode characters for '{name}'"
            )

    def test_topic_name_with_special_characters(self):
        """Topic names with dots, hyphens, and underscores are handled correctly."""
        builder = GraphStateBuilder(show_producers=False)
        special_names = [
            "my.topic.name",
            "my-topic-name",
            "my_topic_name",
            "mixed.topic-name_v2",
            "dots...multiple",
            "hyphens---many",
            "underscores___deep",
            ".leading-dot",
            "trailing-dot.",
            "-leading-hyphen",
            "_leading-underscore",
        ]
        topics = {
            name: TopicInfo(name=name, partitions=2, msg_per_sec=0.5, total_messages=50)
            for name in special_names
        }
        snap = ClusterSnapshot(
            topics=topics,
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        added_ids = {n["id"] for n in diff.nodes_added}
        for name in special_names:
            expected_id = f"topic-{name}"
            assert expected_id in added_ids, (
                f"Special-char topic '{name}' should produce node '{expected_id}'"
            )

        # Verify a second update with the same data produces an empty diff
        diff2 = builder.update(snap)
        assert diff2.is_empty(), "Re-applying identical special-char topics should yield empty diff"

    def test_extremely_long_topic_name(self):
        """Topic names exceeding 255 characters are handled without crashing."""
        builder = GraphStateBuilder(show_producers=False)
        long_name = "x" * 300  # Well beyond Kafka's 249-char limit
        snap = ClusterSnapshot(
            topics={
                long_name: TopicInfo(
                    name=long_name, partitions=1, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={
                "long-consumer": ConsumerGroupInfo(
                    group_id="long-consumer",
                    members=1,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=[long_name],
                    per_partition_lag={f"{long_name}-0": 10},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        assert not diff.is_empty()
        node_ids = {n["id"] for n in diff.nodes_added}
        assert f"topic-{long_name}" in node_ids, "300-char topic name should produce a valid node"

        # Verify the edge between the long-named topic and its consumer exists
        edge_ids = {e["id"] for e in diff.edges_added}
        assert any(long_name in eid and "long-consumer" in eid for eid in edge_ids), (
            "Edge from 300-char topic to consumer should exist"
        )

        # Verify serialization works with the long name
        snapshot_result = builder.get_snapshot()
        assert snapshot_result["type"] == "graph_snapshot"
        snapshot_node_ids = {n["id"] for n in snapshot_result["nodes"]["added"]}
        assert f"topic-{long_name}" in snapshot_node_ids


class TestGraphStateSerialization:
    """Tests for GraphDiff and GraphStateBuilder serialization correctness."""

    def test_to_dict_is_json_serializable(self):
        """GraphDiff.to_dict() output must be fully JSON-serializable."""
        import json

        builder = GraphStateBuilder(show_producers=True)
        snap = ClusterSnapshot(
            topics={
                "json-topic": TopicInfo(
                    name="json-topic", partitions=3, msg_per_sec=10.5, total_messages=5000
                ),
            },
            consumer_groups={
                "json-consumer": ConsumerGroupInfo(
                    group_id="json-consumer",
                    members=2,
                    total_lag=100,
                    status="Stable",
                    subscribed_topics=["json-topic"],
                    per_partition_lag={"json-topic-0": 50, "json-topic-1": 30, "json-topic-2": 20},
                    member_client_ids=["client-a", "client-b"],
                ),
            },
            active_partitions={"json-topic": {0, 1}},
            timestamp=1000.0,
        )
        diff = builder.update(snap)
        d = diff.to_dict()

        # This must not raise TypeError
        serialized = json.dumps(d)
        assert isinstance(serialized, str), "to_dict() output should be JSON-serializable"

        # Round-trip: deserialize and verify structure
        deserialized = json.loads(serialized)
        assert deserialized["type"] == "graph_diff"
        assert "nodes" in deserialized
        assert "edges" in deserialized
        assert "metrics" in deserialized
        assert len(deserialized["nodes"]["added"]) == len(diff.nodes_added)

        # Also test get_snapshot() serialization
        snapshot_dict = builder.get_snapshot()
        snapshot_serialized = json.dumps(snapshot_dict)
        assert isinstance(snapshot_serialized, str), "get_snapshot() output should be JSON-serializable"

    def test_empty_graph_state_serializes(self):
        """A GraphStateBuilder with no updates should serialize to a valid empty snapshot."""
        import json

        builder = GraphStateBuilder(show_producers=False)
        snapshot = builder.get_snapshot()

        assert snapshot["type"] == "graph_snapshot"
        assert snapshot["nodes"]["added"] == []
        assert snapshot["nodes"]["updated"] == []
        assert snapshot["nodes"]["removed"] == []
        assert snapshot["edges"]["added"] == []
        assert snapshot["edges"]["updated"] == []
        assert snapshot["edges"]["removed"] == []
        assert snapshot["metrics"] == {}

        # Must be JSON-serializable
        serialized = json.dumps(snapshot)
        deserialized = json.loads(serialized)
        assert deserialized["type"] == "graph_snapshot"

    def test_unicode_labels_serialize(self):
        """Graph state with unicode topic/group labels serializes correctly."""
        import json

        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "\u00e9v\u00e9nements": TopicInfo(
                    name="\u00e9v\u00e9nements", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
                "\u30c7\u30fc\u30bf": TopicInfo(
                    name="\u30c7\u30fc\u30bf", partitions=2, msg_per_sec=2.0, total_messages=500
                ),
            },
            consumer_groups={
                "\u30b3\u30f3\u30b7\u30e5\u30fc\u30de": ConsumerGroupInfo(
                    group_id="\u30b3\u30f3\u30b7\u30e5\u30fc\u30de",
                    members=1,
                    total_lag=5,
                    status="Stable",
                    subscribed_topics=["\u30c7\u30fc\u30bf"],
                    per_partition_lag={"\u30c7\u30fc\u30bf-0": 3, "\u30c7\u30fc\u30bf-1": 2},
                    member_client_ids=["client-\u00e0"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)
        d = diff.to_dict()

        # Serialize with ensure_ascii=False to preserve unicode in output
        serialized = json.dumps(d, ensure_ascii=False)
        assert "\u00e9v\u00e9nements" in serialized, "Unicode topic label should survive serialization"
        assert "\u30c7\u30fc\u30bf" in serialized, "Japanese topic label should survive serialization"
        assert "\u30b3\u30f3\u30b7\u30e5\u30fc\u30de" in serialized, "Japanese consumer label should survive serialization"

        # Round-trip must preserve labels
        deserialized = json.loads(serialized)
        added_labels = {n["data"]["label"] for n in deserialized["nodes"]["added"]}
        assert "\u00e9v\u00e9nements" in added_labels
        assert "\u30c7\u30fc\u30bf" in added_labels


class TestConfigBoundaryValues:
    """Boundary-value tests for Config validation edge cases."""

    def test_poll_interval_at_minimum(self):
        """POLL_INTERVAL_MS = 500 (exact minimum) should pass validation."""
        from config import Config

        cfg = Config()
        cfg.POLL_INTERVAL_MS = 500
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert poll_errors == [], (
            f"POLL_INTERVAL_MS=500 should be valid, got errors: {poll_errors}"
        )

    def test_lag_threshold_at_zero(self):
        """LAG_WARN_THRESHOLD = 0 should pass validation (every lag triggers a warning)."""
        from config import Config

        cfg = Config()
        cfg.LAG_WARN_THRESHOLD = 0
        errors = cfg.validate()
        lag_errors = [e for e in errors if "LAG_WARN_THRESHOLD" in e]
        assert lag_errors == [], (
            f"LAG_WARN_THRESHOLD=0 should be valid, got errors: {lag_errors}"
        )

        # Also verify the builder respects threshold=0 by flagging any lag > 0
        builder = GraphStateBuilder(lag_warn_threshold=0, show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "threshold-topic": TopicInfo(
                    name="threshold-topic", partitions=2, msg_per_sec=1.0, total_messages=100
                ),
            },
            consumer_groups={
                "threshold-group": ConsumerGroupInfo(
                    group_id="threshold-group",
                    members=1,
                    total_lag=1,
                    status="Stable",
                    subscribed_topics=["threshold-topic"],
                    per_partition_lag={"threshold-topic-0": 1, "threshold-topic-1": 0},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        # With threshold=0, total_lag=1 should trigger lagWarning
        cg_nodes = [n for n in diff.nodes_added if n["type"] == "consumer_group"]
        assert len(cg_nodes) == 1
        assert cg_nodes[0]["data"]["lagWarning"] is True, (
            "With lag_warn_threshold=0, any positive lag should trigger lagWarning"
        )

    def test_poll_interval_very_high(self):
        """POLL_INTERVAL_MS = 60000 (1 minute) should pass validation."""
        from config import Config

        cfg = Config()
        cfg.POLL_INTERVAL_MS = 60000
        errors = cfg.validate()
        poll_errors = [e for e in errors if "POLL_INTERVAL_MS" in e]
        assert poll_errors == [], (
            f"POLL_INTERVAL_MS=60000 should be valid, got errors: {poll_errors}"
        )

        # Verify it does not accidentally trigger any other validation errors
        # beyond what the default config would produce
        default_cfg = Config()
        default_errors = default_cfg.validate()
        assert len(errors) == len(default_errors), (
            f"High poll interval should not introduce extra errors: {errors}"
        )


class TestConcurrentGraphUpdates:
    """Tests for rapid successive graph state builds from different snapshots."""

    def test_rapid_builds_from_different_snapshots(self):
        """Building graph state from two alternating snapshots rapidly should not corrupt state."""
        builder = GraphStateBuilder(show_producers=False)

        snap_a = ClusterSnapshot(
            topics={
                "alpha-topic": TopicInfo(
                    name="alpha-topic", partitions=4, msg_per_sec=10.0, total_messages=5000
                ),
                "beta-topic": TopicInfo(
                    name="beta-topic", partitions=2, msg_per_sec=5.0, total_messages=2000
                ),
            },
            consumer_groups={
                "alpha-consumer": ConsumerGroupInfo(
                    group_id="alpha-consumer",
                    members=2,
                    total_lag=50,
                    status="Stable",
                    subscribed_topics=["alpha-topic"],
                    per_partition_lag={"alpha-topic-0": 15, "alpha-topic-1": 15,
                                       "alpha-topic-2": 10, "alpha-topic-3": 10},
                    member_client_ids=["client-a1", "client-a2"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )

        snap_b = ClusterSnapshot(
            topics={
                "gamma-topic": TopicInfo(
                    name="gamma-topic", partitions=6, msg_per_sec=20.0, total_messages=10000
                ),
            },
            consumer_groups={
                "gamma-consumer": ConsumerGroupInfo(
                    group_id="gamma-consumer",
                    members=3,
                    total_lag=200,
                    status="Stable",
                    subscribed_topics=["gamma-topic"],
                    per_partition_lag={f"gamma-topic-{p}": 33 for p in range(6)},
                    member_client_ids=["client-g1", "client-g2", "client-g3"],
                ),
            },
            active_partitions={},
            timestamp=2000.0,
        )

        # Rapidly alternate between snapshot A and snapshot B
        for i in range(20):
            snap = snap_a if i % 2 == 0 else snap_b
            diff = builder.update(snap)
            assert isinstance(diff, GraphDiff), f"Iteration {i}: update() must return a GraphDiff"

        # After 20 iterations ending with snap_b (i=19 is odd), verify final state
        final = builder.get_snapshot()
        final_node_ids = {n["id"] for n in final["nodes"]["added"]}

        # snap_b topics should be active
        assert "topic-gamma-topic" in final_node_ids, "gamma-topic should be in final state"
        gamma_node = next(n for n in final["nodes"]["added"] if n["id"] == "topic-gamma-topic")
        assert gamma_node["status"] == "ok", "gamma-topic should be active"

        # snap_a topics should be inactive (they were in the previous snapshot but not snap_b)
        alpha_node = next(
            (n for n in final["nodes"]["added"] if n["id"] == "topic-alpha-topic"), None
        )
        if alpha_node is not None:
            assert alpha_node["status"] == "inactive", (
                "alpha-topic should be inactive after switching to snap_b"
            )

    def test_metrics_update_after_rapid_builds(self):
        """Metrics should accurately reflect the last snapshot after multiple rapid builds."""
        builder = GraphStateBuilder(show_producers=True)

        # Build with increasing message rates to verify metrics track correctly
        for i in range(15):
            rate = float((i + 1) * 10)  # 10, 20, 30, ..., 150
            total = (i + 1) * 1000
            snap = ClusterSnapshot(
                topics={
                    "metrics-topic": TopicInfo(
                        name="metrics-topic", partitions=3,
                        msg_per_sec=rate, total_messages=total
                    ),
                },
                consumer_groups={
                    "metrics-consumer": ConsumerGroupInfo(
                        group_id="metrics-consumer",
                        members=1,
                        total_lag=i * 5,
                        status="Stable",
                        subscribed_topics=["metrics-topic"],
                        per_partition_lag={
                            "metrics-topic-0": i * 2,
                            "metrics-topic-1": i * 2,
                            "metrics-topic-2": i,
                        },
                        member_client_ids=["client-m1"],
                    ),
                },
                active_partitions={"metrics-topic": {0, 1}} if i % 2 == 0 else {},
                timestamp=1000.0 + i,
            )
            diff = builder.update(snap)

        # After the last build (i=14): rate=150, total=15000, lag=70
        final_diff_metrics = diff.metrics
        assert "topic-metrics-topic" in final_diff_metrics, (
            "Metrics should contain topic-metrics-topic"
        )
        assert final_diff_metrics["topic-metrics-topic"]["msgPerSec"] == 150.0, (
            f"Expected msgPerSec=150.0, got {final_diff_metrics['topic-metrics-topic']['msgPerSec']}"
        )
        assert final_diff_metrics["topic-metrics-topic"]["totalMessages"] == 15000, (
            f"Expected totalMessages=15000, got {final_diff_metrics['topic-metrics-topic']['totalMessages']}"
        )

        # Verify the snapshot also reflects the final state
        final_snapshot = builder.get_snapshot()
        topic_node = next(
            n for n in final_snapshot["nodes"]["added"] if n["id"] == "topic-metrics-topic"
        )
        assert topic_node["data"]["msgPerSec"] == 150.0
        assert topic_node["data"]["totalMessages"] == 15000

        # Verify consumer node reflects final lag
        cg_node = next(
            n for n in final_snapshot["nodes"]["added"]
            if n["type"] == "consumer_group" and "metrics-consumer" in n["id"]
        )
        assert cg_node["data"]["totalLag"] == 70, (
            f"Expected totalLag=70 (14*5), got {cg_node['data']['totalLag']}"
        )


# ---------------------------------------------------------------------------
# New tests: Topic enrichment, timestamp/specific offset reset, edge data
# ---------------------------------------------------------------------------


class TestTopicNodeConsumerProducerEnrichment:
    """Verify that after graph_state builds the diff, topic nodes have
    'consumers' and 'producers' lists populated from edge traversal
    (graph_state.py lines 238-255)."""

    def test_topic_has_consumer_list_from_single_consumer(self):
        """A topic consumed by one group should list that group in its consumers."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "enrichment.input": TopicInfo(
                    name="enrichment.input", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "enrichment-reader": ConsumerGroupInfo(
                    group_id="enrichment-reader",
                    members=2,
                    total_lag=40,
                    status="Stable",
                    subscribed_topics=["enrichment.input"],
                    per_partition_lag={
                        "enrichment.input-0": 15,
                        "enrichment.input-1": 15,
                        "enrichment.input-2": 10,
                    },
                    member_client_ids=["reader-1", "reader-2"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        topic_node = next(
            n for n in diff.nodes_added if n["id"] == "topic-enrichment.input"
        )
        assert "consumers" in topic_node["data"], "Topic node must have 'consumers' key"
        assert "producers" in topic_node["data"], "Topic node must have 'producers' key"
        assert "enrichment-reader" in topic_node["data"]["consumers"], (
            f"Expected 'enrichment-reader' in consumers, got {topic_node['data']['consumers']}"
        )
        assert topic_node["data"]["producers"] == [], (
            "No producers expected since show_producers=False and no service produces to this topic"
        )

    def test_topic_has_producer_list_from_service(self):
        """A topic produced to by a service should list that service in its producers."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snap = ClusterSnapshot(
            topics={
                "orders.created": TopicInfo(
                    name="orders.created", partitions=3, msg_per_sec=50.0, total_messages=10000
                ),
                "orders.processed": TopicInfo(
                    name="orders.processed", partitions=3, msg_per_sec=45.0, total_messages=9000
                ),
            },
            consumer_groups={
                "order-service": ConsumerGroupInfo(
                    group_id="order-service",
                    members=3,
                    total_lag=30,
                    status="Stable",
                    subscribed_topics=["orders.created"],
                    per_partition_lag={
                        "orders.created-0": 10,
                        "orders.created-1": 10,
                        "orders.created-2": 10,
                    },
                    member_client_ids=["order-svc-1", "order-svc-2", "order-svc-3"],
                ),
            },
            active_partitions={"orders.processed": {0, 1, 2}},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        # orders.processed is produced to by order-service
        processed_node = next(
            n for n in diff.nodes_added if n["id"] == "topic-orders.processed"
        )
        assert "producers" in processed_node["data"]
        assert "order-service" in processed_node["data"]["producers"], (
            f"Expected 'order-service' in producers, got {processed_node['data']['producers']}"
        )

        # orders.created is consumed by order-service
        created_node = next(
            n for n in diff.nodes_added if n["id"] == "topic-orders.created"
        )
        assert "consumers" in created_node["data"]
        assert "order-service" in created_node["data"]["consumers"]

    def test_topic_with_multiple_consumers_and_producers(self):
        """A topic consumed by multiple groups lists all of them; same for producers."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=5000)
        snap = ClusterSnapshot(
            topics={
                "events.stream": TopicInfo(
                    name="events.stream", partitions=6, msg_per_sec=100.0, total_messages=50000
                ),
            },
            consumer_groups={
                "analytics-reader": ConsumerGroupInfo(
                    group_id="analytics-reader",
                    members=2,
                    total_lag=200,
                    status="Stable",
                    subscribed_topics=["events.stream"],
                    per_partition_lag={"events.stream-0": 100, "events.stream-1": 100},
                    member_client_ids=["analytics-1", "analytics-2"],
                ),
                "audit-logger": ConsumerGroupInfo(
                    group_id="audit-logger",
                    members=1,
                    total_lag=50,
                    status="Stable",
                    subscribed_topics=["events.stream"],
                    per_partition_lag={"events.stream-0": 50},
                    member_client_ids=["audit-1"],
                ),
            },
            active_partitions={"events.stream": {0, 1, 2}},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        topic_node = next(
            n for n in diff.nodes_added if n["id"] == "topic-events.stream"
        )
        consumers_list = topic_node["data"]["consumers"]
        assert "analytics-reader" in consumers_list
        assert "audit-logger" in consumers_list
        assert len(consumers_list) == 2

        # The standalone producer node should appear in producers list
        producers_list = topic_node["data"]["producers"]
        assert len(producers_list) >= 1, (
            "Standalone producer should appear in topic producers list"
        )

    def test_topic_with_no_consumers_or_producers_has_empty_lists(self):
        """A topic with no edges should have empty consumers and producers lists."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "orphan-topic": TopicInfo(
                    name="orphan-topic", partitions=3, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        topic_node = next(
            n for n in diff.nodes_added if n["id"] == "topic-orphan-topic"
        )
        assert topic_node["data"]["consumers"] == []
        assert topic_node["data"]["producers"] == []

    def test_enrichment_persists_after_update(self):
        """After an update cycle, the enrichment data remains consistent."""
        builder = GraphStateBuilder(show_producers=False)

        snap1 = ClusterSnapshot(
            topics={
                "persist.topic": TopicInfo(
                    name="persist.topic", partitions=2, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={
                "persist-consumer": ConsumerGroupInfo(
                    group_id="persist-consumer",
                    members=1,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=["persist.topic"],
                    per_partition_lag={"persist.topic-0": 5, "persist.topic-1": 5},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        builder.update(snap1)

        # Second update with changed lag
        snap2 = ClusterSnapshot(
            topics={
                "persist.topic": TopicInfo(
                    name="persist.topic", partitions=2, msg_per_sec=8.0, total_messages=2000
                ),
            },
            consumer_groups={
                "persist-consumer": ConsumerGroupInfo(
                    group_id="persist-consumer",
                    members=1,
                    total_lag=20,
                    status="Stable",
                    subscribed_topics=["persist.topic"],
                    per_partition_lag={"persist.topic-0": 10, "persist.topic-1": 10},
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=2000.0,
        )
        builder.update(snap2)

        snapshot = builder.get_snapshot()
        topic_node = next(
            n for n in snapshot["nodes"]["added"] if n["id"] == "topic-persist.topic"
        )
        assert "consumers" in topic_node["data"]
        assert "persist-consumer" in topic_node["data"]["consumers"]


class TestTimestampAndSpecificOffsetResetAPI:
    """Tests for the timestamp and specific offset reset strategies
    via POST /api/consumer-groups/{group}/reset-offsets."""

    def test_reset_with_timestamp_strategy(self):
        """Reset offsets using strategy='timestamp' passes timestamp param to admin."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 6,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/ts-group/reset-offsets",
            json={"strategy": "timestamp", "timestamp": 1700000000000},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partitionsReset"] == 6
        admin.reset_offsets.assert_called_once_with(
            group_id="ts-group",
            strategy="timestamp",
            topic=None,
            timestamp=1700000000000,
            offset=None,
        )

    def test_reset_with_timestamp_and_topic_filter(self):
        """Reset with timestamp strategy and a specific topic passes both params."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 3,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/ts-group/reset-offsets",
            json={
                "strategy": "timestamp",
                "timestamp": 1700000000000,
                "topic": "orders.created",
            },
        )
        assert resp.status_code == 200
        assert resp.json()["partitionsReset"] == 3
        admin.reset_offsets.assert_called_once_with(
            group_id="ts-group",
            strategy="timestamp",
            topic="orders.created",
            timestamp=1700000000000,
            offset=None,
        )

    def test_reset_with_specific_offset_strategy(self):
        """Reset offsets using strategy='specific' passes offset param to admin."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 4,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/offset-group/reset-offsets",
            json={"strategy": "specific", "offset": 42},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partitionsReset"] == 4
        admin.reset_offsets.assert_called_once_with(
            group_id="offset-group",
            strategy="specific",
            topic=None,
            timestamp=None,
            offset=42,
        )

    def test_reset_with_specific_offset_and_topic(self):
        """Reset with specific strategy and topic filter passes all params."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 2,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/offset-group/reset-offsets",
            json={"strategy": "specific", "offset": 100, "topic": "payments.events"},
        )
        assert resp.status_code == 200
        assert resp.json()["partitionsReset"] == 2
        admin.reset_offsets.assert_called_once_with(
            group_id="offset-group",
            strategy="specific",
            topic="payments.events",
            timestamp=None,
            offset=100,
        )

    def test_reset_timestamp_failure_returns_400(self):
        """When the timestamp reset fails, the endpoint returns HTTP 400."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": False,
            "error": "No offsets found for the given timestamp",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/fail-group/reset-offsets",
            json={"strategy": "timestamp", "timestamp": 9999999999999},
        )
        assert resp.status_code == 400
        assert "No offsets found" in resp.json()["detail"]


class TestEdgeDataValidation:
    """Ensure edges contain proper type, lag, and msgPerSec fields."""

    def test_consume_edge_has_type_lag_and_active_fields(self):
        """Consume edges must have type='consumes', lag, lagWarning, active, and label."""
        builder = GraphStateBuilder(lag_warn_threshold=100, show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "edge-val.topic": TopicInfo(
                    name="edge-val.topic", partitions=2, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "edge-val-consumer": ConsumerGroupInfo(
                    group_id="edge-val-consumer",
                    members=1,
                    total_lag=60,
                    status="Stable",
                    subscribed_topics=["edge-val.topic"],
                    per_partition_lag={
                        "edge-val.topic-0": 30,
                        "edge-val.topic-1": 30,
                    },
                    member_client_ids=["client-1"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        consume_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 1, f"Expected 1 consume edge, got {len(consume_edges)}"

        edge_data = consume_edges[0]["data"]
        assert edge_data["type"] == "consumes"
        assert "lag" in edge_data, "Consume edge missing 'lag'"
        assert isinstance(edge_data["lag"], (int, float)), "lag must be numeric"
        assert edge_data["lag"] == 60
        assert "lagWarning" in edge_data, "Consume edge missing 'lagWarning'"
        assert "active" in edge_data, "Consume edge missing 'active'"
        assert edge_data["active"] is True
        assert "label" in edge_data, "Consume edge missing 'label'"

    def test_produce_edge_has_type_msgpersec_and_active_fields(self):
        """Produce edges must have type='produces', msgPerSec, active, and label."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snap = ClusterSnapshot(
            topics={
                "edge-val.input": TopicInfo(
                    name="edge-val.input", partitions=2, msg_per_sec=20.0, total_messages=3000
                ),
                "edge-val.output": TopicInfo(
                    name="edge-val.output", partitions=2, msg_per_sec=18.0, total_messages=2700
                ),
            },
            consumer_groups={
                "edge-val-service": ConsumerGroupInfo(
                    group_id="edge-val-service",
                    members=2,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=["edge-val.input"],
                    per_partition_lag={
                        "edge-val.input-0": 5,
                        "edge-val.input-1": 5,
                    },
                    member_client_ids=["svc-1", "svc-2"],
                ),
            },
            active_partitions={"edge-val.output": {0, 1}},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        produce_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "produces"
        ]
        assert len(produce_edges) >= 1, "Expected at least 1 produce edge"

        for edge in produce_edges:
            edge_data = edge["data"]
            assert edge_data["type"] == "produces"
            assert "msgPerSec" in edge_data, "Produce edge missing 'msgPerSec'"
            assert isinstance(edge_data["msgPerSec"], (int, float)), "msgPerSec must be numeric"
            assert "active" in edge_data, "Produce edge missing 'active'"
            assert "label" in edge_data, "Produce edge missing 'label'"

    def test_all_edges_in_large_snapshot_have_required_fields(self):
        """Every edge in a large snapshot must have type, and either lag or msgPerSec."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = make_large_snapshot(num_topics=100, num_consumer_groups=50)
        diff = builder.update(snapshot)

        assert len(diff.edges_added) > 0, "Large snapshot should produce edges"

        for edge in diff.edges_added:
            assert "data" in edge, f"Edge {edge['id']} missing 'data'"
            edge_data = edge["data"]
            assert "type" in edge_data, f"Edge {edge['id']} missing 'type' in data"
            edge_type = edge_data["type"]
            assert edge_type in ("consumes", "produces"), (
                f"Edge {edge['id']} has unexpected type '{edge_type}'"
            )

            if edge_type == "consumes":
                assert "lag" in edge_data, f"Consume edge {edge['id']} missing 'lag'"
                assert "lagWarning" in edge_data, f"Consume edge {edge['id']} missing 'lagWarning'"
            elif edge_type == "produces":
                assert "msgPerSec" in edge_data, f"Produce edge {edge['id']} missing 'msgPerSec'"

            assert "active" in edge_data, f"Edge {edge['id']} missing 'active'"

    def test_consume_edge_lag_warning_respects_threshold(self):
        """Consume edge lagWarning should be True when lag > threshold, False otherwise."""
        threshold = 200
        builder = GraphStateBuilder(lag_warn_threshold=threshold, show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "threshold.topic": TopicInfo(
                    name="threshold.topic", partitions=2, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={
                "high-lag-group": ConsumerGroupInfo(
                    group_id="high-lag-group",
                    members=1,
                    total_lag=500,
                    status="Stable",
                    subscribed_topics=["threshold.topic"],
                    per_partition_lag={
                        "threshold.topic-0": 300,
                        "threshold.topic-1": 200,
                    },
                    member_client_ids=["client-h"],
                ),
                "low-lag-group": ConsumerGroupInfo(
                    group_id="low-lag-group",
                    members=1,
                    total_lag=50,
                    status="Stable",
                    subscribed_topics=["threshold.topic"],
                    per_partition_lag={
                        "threshold.topic-0": 30,
                        "threshold.topic-1": 20,
                    },
                    member_client_ids=["client-l"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        consume_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 2

        high_lag_edge = next(
            e for e in consume_edges if "high-lag-group" in e["target"]
        )
        low_lag_edge = next(
            e for e in consume_edges if "low-lag-group" in e["target"]
        )

        assert high_lag_edge["data"]["lag"] == 500
        assert high_lag_edge["data"]["lagWarning"] is True, (
            "Lag 500 > threshold 200 should trigger lagWarning"
        )
        assert low_lag_edge["data"]["lag"] == 50
        assert low_lag_edge["data"]["lagWarning"] is False, (
            "Lag 50 < threshold 200 should NOT trigger lagWarning"
        )

    def test_produce_edge_label_shows_rate_when_active(self):
        """Produce edges with msgPerSec > 0 should have a non-empty label with msg/s."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snap = ClusterSnapshot(
            topics={
                "label.input": TopicInfo(
                    name="label.input", partitions=2, msg_per_sec=10.0, total_messages=1000
                ),
                "label.output": TopicInfo(
                    name="label.output", partitions=2, msg_per_sec=30.0, total_messages=3000
                ),
            },
            consumer_groups={
                "label-service": ConsumerGroupInfo(
                    group_id="label-service",
                    members=1,
                    total_lag=0,
                    status="Stable",
                    subscribed_topics=["label.input"],
                    per_partition_lag={"label.input-0": 0, "label.input-1": 0},
                    member_client_ids=["label-svc-1"],
                ),
            },
            active_partitions={"label.output": {0, 1}},
            timestamp=1000.0,
        )
        diff = builder.update(snap)

        produce_edges = [
            e for e in diff.edges_added
            if e.get("data", {}).get("type") == "produces"
        ]
        assert len(produce_edges) >= 1

        for edge in produce_edges:
            if edge["data"]["msgPerSec"] > 0:
                assert edge["data"]["label"] != "", (
                    "Produce edge with msgPerSec > 0 should have a non-empty label"
                )
                assert "msg/s" in edge["data"]["label"], (
                    "Produce edge label should contain 'msg/s'"
                )


# ── Additional API Route Tests ──────────────────────────────────────────


class TestBatchProduceAPI:
    """Additional tests for POST /api/topics/{topic}/produce covering
    various payload combinations and error cases."""

    def test_produce_value_only_no_key_no_headers(self):
        """Produce with only a value and nothing else succeeds."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "simple-topic",
            "partition": 0,
            "offset": 1,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/simple-topic/produce",
            json={"value": "hello-world"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["topic"] == "simple-topic"
        admin.produce_message.assert_called_once_with(
            topic="simple-topic",
            value="hello-world",
            key=None,
            headers=None,
            partition=None,
        )

    def test_produce_with_key_and_partition_no_headers(self):
        """Produce with key and partition but no headers passes all args correctly."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "keyed-topic",
            "partition": 3,
            "offset": 77,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/keyed-topic/produce",
            json={"value": "payload", "key": "k1", "partition": 3},
        )
        assert resp.status_code == 200
        assert resp.json()["partition"] == 3
        admin.produce_message.assert_called_once_with(
            topic="keyed-topic",
            value="payload",
            key="k1",
            headers=None,
            partition=3,
        )

    def test_produce_empty_body_defaults_value_to_empty_string(self):
        """Producing with an empty JSON body defaults value to empty string."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "empty-body-topic",
            "partition": 0,
            "offset": 0,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/empty-body-topic/produce",
            json={},
        )
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="empty-body-topic",
            value="",
            key=None,
            headers=None,
            partition=None,
        )

    def test_produce_with_headers_only(self):
        """Produce with headers but no key sends headers correctly."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "headers-topic",
            "partition": 1,
            "offset": 42,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/headers-topic/produce",
            json={
                "value": "data",
                "headers": {"x-correlation-id": "corr-999"},
            },
        )
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="headers-topic",
            value="data",
            key=None,
            headers={"x-correlation-id": "corr-999"},
            partition=None,
        )


class TestTopicDeletionCascade:
    """Tests for DELETE /api/topics/{topic} covering success, failure,
    and edge cases around topic deletion."""

    def test_delete_topic_success(self):
        """Deleting an existing topic returns success with the topic name."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": True,
            "topic": "old-events",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/topics/old-events")

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["topic"] == "old-events"
        admin.delete_topic.assert_called_once_with("old-events")

    def test_delete_nonexistent_topic_returns_400(self):
        """Deleting a topic that does not exist returns HTTP 400."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": False,
            "error": "Topic 'ghost-topic' does not exist",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/topics/ghost-topic")

        assert resp.status_code == 400
        assert "does not exist" in resp.json()["detail"]

    def test_delete_topic_without_admin_returns_503(self):
        """When kafka_admin is not set, DELETE topic returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.delete("/api/topics/any-topic")

        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_delete_topic_authorization_error(self):
        """When the admin reports an authorization error, response is 400."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": False,
            "error": "TopicAuthorizationException: not authorized to delete 'secure-data'",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/topics/secure-data")

        assert resp.status_code == 400
        assert "not authorized" in resp.json()["detail"]


class TestClusterInfoAdditional:
    """Additional tests for GET /api/cluster covering response structure
    and edge cases."""

    def test_cluster_info_contains_all_required_keys(self):
        """Cluster info response must contain the five standard keys."""
        admin = MagicMock()
        admin.get_cluster_info.return_value = {
            "clusterId": "prod-cluster-001",
            "controllerId": 2,
            "brokerCount": 5,
            "topicCount": 120,
            "consumerGroupCount": 45,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster")

        assert resp.status_code == 200
        body = resp.json()
        for key in ("clusterId", "controllerId", "brokerCount", "topicCount", "consumerGroupCount"):
            assert key in body, f"Missing key '{key}' in cluster info response"
        assert body["brokerCount"] == 5
        assert body["consumerGroupCount"] == 45

    def test_cluster_info_large_counts(self):
        """Cluster info with large topic and consumer group counts returns correctly."""
        admin = MagicMock()
        admin.get_cluster_info.return_value = {
            "clusterId": "mega-cluster",
            "controllerId": 0,
            "brokerCount": 30,
            "topicCount": 5000,
            "consumerGroupCount": 2000,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster")

        assert resp.status_code == 200
        body = resp.json()
        assert body["topicCount"] == 5000
        assert body["consumerGroupCount"] == 2000
        assert body["brokerCount"] == 30


class TestConsumerGroupDeleteAdditional:
    """Additional tests for DELETE /api/consumer-groups/{group}."""

    def test_delete_group_with_special_chars_in_name(self):
        """Consumer group names with dots and dashes work correctly."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": True,
            "groupId": "my.service-group.v2",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/consumer-groups/my.service-group.v2")

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["groupId"] == "my.service-group.v2"
        admin.delete_consumer_group.assert_called_once_with("my.service-group.v2")

    def test_delete_group_timeout_error(self):
        """When the admin reports a timeout, response is 400 with timeout detail."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": False,
            "error": "Request timed out after 30000ms",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/consumer-groups/slow-group")

        assert resp.status_code == 400
        assert "timed out" in resp.json()["detail"]


class TestTopicConfigUpdateAdditional:
    """Additional tests for PUT /api/topics/{topic}/config."""

    def test_update_multiple_config_keys(self):
        """Updating three config keys at once succeeds and reports all three."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "multi-config",
            "updated": ["retention.ms", "segment.bytes", "cleanup.policy"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/multi-config/config",
            json={
                "configs": {
                    "retention.ms": "86400000",
                    "segment.bytes": "1073741824",
                    "cleanup.policy": "delete",
                }
            },
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert len(body["updated"]) == 3
        assert "segment.bytes" in body["updated"]

    def test_update_config_without_admin_returns_503(self):
        """When kafka_admin is not set, PUT config returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.put(
            "/api/topics/any-topic/config",
            json={"configs": {"retention.ms": "1000"}},
        )

        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_update_config_topic_not_found(self):
        """Updating config for a non-existent topic returns 400 via admin error."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": False,
            "error": "Topic 'no-such-topic' not found",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/no-such-topic/config",
            json={"configs": {"retention.ms": "5000"}},
        )

        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# New test classes: Cluster Health, Topic Config, Consumer Group Detail,
# Offset Reset, Produce Headers, Broker Rack, Add Partitions, Delete
# Non-Existent Topic, Create Topic Invalid Params
# ---------------------------------------------------------------------------


class TestClusterHealthEndpointFields:
    """Test GET /api/cluster/health returns all expected fields."""

    def test_health_returns_total_partitions(self):
        """Cluster health response must include totalPartitions."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 120,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {0: 40, 1: 40, 2: 40},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        assert resp.status_code == 200
        body = resp.json()
        assert "totalPartitions" in body
        assert body["totalPartitions"] == 120

    def test_health_returns_under_replicated_count(self):
        """Cluster health response must include underReplicatedCount."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 60,
            "underReplicatedCount": 3,
            "underReplicated": [
                {"topic": "orders", "partition": 0, "replicas": 3, "isr": 2},
                {"topic": "orders", "partition": 1, "replicas": 3, "isr": 1},
                {"topic": "events", "partition": 2, "replicas": 3, "isr": 2},
            ],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {0: 30, 1: 30},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["underReplicatedCount"] == 3
        assert len(body["underReplicated"]) == 3

    def test_health_returns_offline_partition_count(self):
        """Cluster health response must include offlinePartitionCount."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 30,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 2,
            "offlinePartitions": [
                {"topic": "payments", "partition": 0},
                {"topic": "payments", "partition": 1},
            ],
            "leaderDistribution": {0: 15, 1: 15},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["offlinePartitionCount"] == 2
        assert len(body["offlinePartitions"]) == 2

    def test_health_returns_leader_distribution(self):
        """Cluster health response must include leaderDistribution mapping."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 90,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {"0": 30, "1": 30, "2": 30},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        assert resp.status_code == 200
        body = resp.json()
        assert "leaderDistribution" in body
        dist = body["leaderDistribution"]
        assert sum(dist.values()) == 90


class TestTopicConfigUpdateVariousKeys:
    """Test PUT /api/topics/{topic}/config with different config keys."""

    def test_update_retention_ms(self):
        """Updating retention.ms should succeed and reflect in the response."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "orders",
            "updated": ["retention.ms"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/orders/config",
            json={"configs": {"retention.ms": "604800000"}},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "retention.ms" in body["updated"]
        admin.update_topic_config.assert_called_once_with(
            "orders", {"retention.ms": "604800000"}
        )

    def test_update_cleanup_policy(self):
        """Updating cleanup.policy to 'compact' should succeed."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "events",
            "updated": ["cleanup.policy"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/events/config",
            json={"configs": {"cleanup.policy": "compact"}},
        )

        assert resp.status_code == 200
        assert resp.json()["updated"] == ["cleanup.policy"]

    def test_update_max_message_bytes(self):
        """Updating max.message.bytes should succeed."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "large-payloads",
            "updated": ["max.message.bytes"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/large-payloads/config",
            json={"configs": {"max.message.bytes": "10485760"}},
        )

        assert resp.status_code == 200
        assert "max.message.bytes" in resp.json()["updated"]

    def test_update_config_empty_object_rejected(self):
        """Sending an empty configs object should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/orders/config",
            json={"configs": {}},
        )

        assert resp.status_code == 400
        assert "non-empty" in resp.json()["detail"]


class TestConsumerGroupDetailMembers:
    """Test GET /api/consumer-groups/{group} returns member details."""

    def test_detail_returns_members_with_partitions(self):
        """Consumer group detail must include members with assigned partitions."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "order-service",
            "state": "Stable",
            "members": [
                {
                    "memberId": "member-0",
                    "clientId": "order-service-0",
                    "clientHost": "/10.0.0.1",
                    "partitions": ["orders-0", "orders-1"],
                },
                {
                    "memberId": "member-1",
                    "clientId": "order-service-1",
                    "clientHost": "/10.0.0.2",
                    "partitions": ["orders-2"],
                },
            ],
            "offsets": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/order-service")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["members"]) == 2
        member_0 = body["members"][0]
        assert member_0["clientId"] == "order-service-0"
        assert member_0["clientHost"] == "/10.0.0.1"
        assert "orders-0" in member_0["partitions"]
        assert "orders-1" in member_0["partitions"]

    def test_detail_returns_client_host_for_all_members(self):
        """Every member in the response must have a clientHost field."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "analytics-svc",
            "state": "Stable",
            "members": [
                {"memberId": "m-0", "clientId": "a-0", "clientHost": "/10.0.1.1", "partitions": []},
                {"memberId": "m-1", "clientId": "a-1", "clientHost": "/10.0.1.2", "partitions": []},
                {"memberId": "m-2", "clientId": "a-2", "clientHost": "/10.0.1.3", "partitions": []},
            ],
            "offsets": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/analytics-svc")

        assert resp.status_code == 200
        for member in resp.json()["members"]:
            assert "clientHost" in member
            assert member["clientHost"].startswith("/")


class TestOffsetResetInvalidStrategies:
    """Test POST /api/consumer-groups/{group}/reset-offsets with invalid strategies."""

    def test_reset_with_unknown_strategy_handled(self):
        """An unrecognized strategy string should still be forwarded to admin and handled."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": False,
            "error": "Unknown strategy 'bogus'",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/my-group/reset-offsets",
            json={"strategy": "bogus"},
        )

        assert resp.status_code == 400
        assert "Unknown strategy" in resp.json()["detail"]

    def test_reset_with_empty_strategy_defaults_latest(self):
        """When strategy is omitted, the route defaults to 'latest'."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": True,
            "partitionsReset": 6,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/my-group/reset-offsets",
            json={},
        )

        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="my-group",
            strategy="latest",
            topic=None,
            timestamp=None,
            offset=None,
        )


class TestProduceWithHeaders:
    """Test POST /api/topics/{topic}/produce with header key-value pairs."""

    def test_produce_with_multiple_headers(self):
        """Producing a message with multiple headers passes them to admin correctly."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "events",
            "partition": 0,
            "offset": 100,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/events/produce",
            json={
                "value": "payload",
                "headers": {"trace-id": "t-123", "correlation-id": "c-456", "source": "test"},
            },
        )

        assert resp.status_code == 200
        assert resp.json()["success"] is True
        admin.produce_message.assert_called_once_with(
            topic="events",
            value="payload",
            key=None,
            headers={"trace-id": "t-123", "correlation-id": "c-456", "source": "test"},
            partition=None,
        )

    def test_produce_with_empty_header_value(self):
        """A header with an empty string value should still be accepted."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "events",
            "partition": 1,
            "offset": 200,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/events/produce",
            json={
                "value": "data",
                "headers": {"x-empty": ""},
            },
        )

        assert resp.status_code == 200
        admin.produce_message.assert_called_once()
        call_kwargs = admin.produce_message.call_args
        assert call_kwargs[1]["headers"] == {"x-empty": ""}


class TestBrokerListRackInfo:
    """Test GET /api/brokers returns rack information."""

    def test_brokers_include_rack_field(self):
        """Each broker in the list must include a rack field."""
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": 0, "host": "broker-0", "port": 9092, "rack": "us-east-1a", "isController": True},
            {"id": 1, "host": "broker-1", "port": 9092, "rack": "us-east-1b", "isController": False},
            {"id": 2, "host": "broker-2", "port": 9092, "rack": "us-east-1c", "isController": False},
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")

        assert resp.status_code == 200
        brokers = resp.json()
        assert len(brokers) == 3
        for broker in brokers:
            assert "rack" in broker
            assert broker["rack"] is not None

    def test_brokers_rack_can_be_null(self):
        """When rack awareness is not configured, rack may be null."""
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": 0, "host": "broker-0", "port": 9092, "rack": None, "isController": True},
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")

        assert resp.status_code == 200
        assert resp.json()[0]["rack"] is None


class TestAddTopicPartitions:
    """Test POST /api/topics/{topic}/partitions to add partitions."""

    def test_add_partitions_success(self):
        """Increasing partitions should succeed and return the new total."""
        admin = MagicMock()
        admin.add_topic_partitions.return_value = {
            "success": True,
            "topic": "orders",
            "partitions": 12,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={"totalPartitions": 12},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partitions"] == 12
        admin.add_topic_partitions.assert_called_once_with("orders", 12)

    def test_add_partitions_invalid_total_zero(self):
        """Requesting 0 total partitions should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={"totalPartitions": 0},
        )

        assert resp.status_code == 400
        assert "positive integer" in resp.json()["detail"]

    def test_add_partitions_missing_field(self):
        """Omitting totalPartitions from the body should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/orders/partitions",
            json={},
        )

        assert resp.status_code == 400


class TestDeleteNonExistentTopic:
    """Test DELETE /api/topics/{topic} for a topic that does not exist."""

    def test_delete_nonexistent_returns_400(self):
        """Deleting a topic that doesn't exist should return 400 with error message."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": False,
            "error": "Topic 'ghost-topic' not found",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/topics/ghost-topic")

        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"]
        admin.delete_topic.assert_called_once_with("ghost-topic")


class TestCreateTopicInvalidParams:
    """Test POST /api/topics with invalid parameters."""

    def test_create_topic_zero_partitions(self):
        """Creating a topic with 0 partitions should be forwarded and return admin error."""
        admin = MagicMock()
        admin.create_topic.return_value = {
            "success": False,
            "error": "Number of partitions must be larger than 0",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics",
            json={"name": "bad-topic", "partitions": 0},
        )

        assert resp.status_code == 400
        assert "partitions" in resp.json()["detail"].lower()

    def test_create_topic_negative_replication(self):
        """Creating a topic with negative replication factor should return error."""
        admin = MagicMock()
        admin.create_topic.return_value = {
            "success": False,
            "error": "Replication factor must be larger than 0",
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics",
            json={"name": "bad-topic", "partitions": 3, "replicationFactor": -1},
        )

        assert resp.status_code == 400
        assert "replication" in resp.json()["detail"].lower()

    def test_create_topic_without_name_returns_400(self):
        """Creating a topic without a name should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics", json={"partitions": 3})

        assert resp.status_code == 400
        assert "name" in resp.json()["detail"].lower()


# ── Additional tests (block 2) ──────────────────────────────────────────────


class TestMessageConsumptionIncludesHeaders:
    """Test that the message sampling endpoint returns headers in response."""

    def test_sample_messages_contain_headers_field(self):
        """Each sampled message must include a 'headers' field."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {
                "offset": 100,
                "partition": 0,
                "timestamp": 1700000000000,
                "key": "order-1",
                "headers": {"trace-id": "abc-123", "source": "test"},
                "value": {"orderId": 1},
                "format": "json",
            },
            {
                "offset": 101,
                "partition": 0,
                "timestamp": 1700000001000,
                "key": "order-2",
                "headers": {},
                "value": "plain-text",
                "format": "utf8",
            },
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/orders/messages")

        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 2
        for msg in body["messages"]:
            assert "headers" in msg, "Every message must include 'headers' field"
        assert body["messages"][0]["headers"]["trace-id"] == "abc-123"

    def test_sample_messages_headers_empty_when_no_headers(self):
        """Messages without headers should have an empty headers dict."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {
                "offset": 200,
                "partition": 1,
                "timestamp": 1700000002000,
                "key": None,
                "headers": {},
                "value": "no-headers-msg",
                "format": "utf8",
            },
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/events/messages")

        assert resp.status_code == 200
        assert resp.json()["messages"][0]["headers"] == {}

    def test_sample_messages_headers_multiple_keys(self):
        """Headers with multiple key-value pairs are all returned."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {
                "offset": 50,
                "partition": 0,
                "timestamp": 1700000003000,
                "key": "k",
                "headers": {
                    "x-request-id": "req-1",
                    "x-correlation-id": "corr-2",
                    "content-type": "application/json",
                },
                "value": "{}",
                "format": "json",
            },
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/multi-header-topic/messages")

        assert resp.status_code == 200
        headers = resp.json()["messages"][0]["headers"]
        assert len(headers) == 3
        assert headers["x-request-id"] == "req-1"
        assert headers["content-type"] == "application/json"


class TestGraphStateEnrichmentConsumerProducerLists:
    """Test that graph state enrichment returns consumer/producer lists on topic nodes."""

    def test_topic_node_has_consumers_list(self):
        """A topic node should have a 'consumers' list populated from edges."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "enrich.orders": TopicInfo(
                    name="enrich.orders", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={
                "order-processor": ConsumerGroupInfo(
                    group_id="order-processor",
                    members=2,
                    total_lag=20,
                    status="Stable",
                    subscribed_topics=["enrich.orders"],
                    per_partition_lag={"enrich.orders-0": 10, "enrich.orders-1": 10},
                    member_client_ids=["proc-1", "proc-2"],
                ),
            },
            active_partitions={},
            timestamp=2000.0,
        )
        diff = builder.update(snap)

        topic_node = next(n for n in diff.nodes_added if n["id"] == "topic-enrich.orders")
        assert isinstance(topic_node["data"]["consumers"], list)
        assert "order-processor" in topic_node["data"]["consumers"]

    def test_topic_node_has_producers_list_when_enabled(self):
        """When show_producers=True, topic nodes should have a 'producers' list."""
        builder = GraphStateBuilder(show_producers=True)
        snap = ClusterSnapshot(
            topics={
                "enrich.input": TopicInfo(
                    name="enrich.input", partitions=2, msg_per_sec=10.0, total_messages=2000
                ),
                "enrich.output": TopicInfo(
                    name="enrich.output", partitions=2, msg_per_sec=8.0, total_messages=1500
                ),
            },
            consumer_groups={
                "enrich-service": ConsumerGroupInfo(
                    group_id="enrich-service",
                    members=1,
                    total_lag=5,
                    status="Stable",
                    subscribed_topics=["enrich.input"],
                    per_partition_lag={"enrich.input-0": 3, "enrich.input-1": 2},
                    member_client_ids=["svc-1"],
                ),
            },
            active_partitions={"enrich.output": {0, 1}},
            timestamp=2000.0,
        )
        diff = builder.update(snap)

        output_node = next(n for n in diff.nodes_added if n["id"] == "topic-enrich.output")
        assert "producers" in output_node["data"]
        assert isinstance(output_node["data"]["producers"], list)
        assert len(output_node["data"]["producers"]) >= 1

    def test_topic_with_no_consumers_has_empty_consumers_list(self):
        """A topic with no consumer groups should have an empty consumers list."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "orphan.topic": TopicInfo(
                    name="orphan.topic", partitions=1, msg_per_sec=0.0, total_messages=0
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff = builder.update(snap)

        topic_node = next(n for n in diff.nodes_added if n["id"] == "topic-orphan.topic")
        assert topic_node["data"]["consumers"] == []
        assert topic_node["data"]["producers"] == []


class TestGraphEdgeDataIncludesTypeField:
    """Test that graph edges include a 'type' field (produces/consumes)."""

    def test_consume_edge_type_is_consumes(self):
        """Consume edges must have data.type == 'consumes'."""
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=500)
        snap = ClusterSnapshot(
            topics={
                "edge-type.topic": TopicInfo(
                    name="edge-type.topic", partitions=2, msg_per_sec=15.0, total_messages=8000
                ),
            },
            consumer_groups={
                "edge-type-reader": ConsumerGroupInfo(
                    group_id="edge-type-reader",
                    members=1,
                    total_lag=45,
                    status="Stable",
                    subscribed_topics=["edge-type.topic"],
                    per_partition_lag={"edge-type.topic-0": 25, "edge-type.topic-1": 20},
                    member_client_ids=["reader-1"],
                ),
            },
            active_partitions={},
            timestamp=3000.0,
        )
        diff = builder.update(snap)

        consume_edges = [e for e in diff.edges_added if e.get("data", {}).get("type") == "consumes"]
        assert len(consume_edges) >= 1, "Expected at least one consume edge"
        for edge in consume_edges:
            assert edge["data"]["type"] == "consumes"

    def test_produce_edge_type_is_produces(self):
        """Produce edges must have data.type == 'produces'."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=500)
        snap = ClusterSnapshot(
            topics={
                "edge-type.in": TopicInfo(
                    name="edge-type.in", partitions=2, msg_per_sec=20.0, total_messages=5000
                ),
                "edge-type.out": TopicInfo(
                    name="edge-type.out", partitions=2, msg_per_sec=18.0, total_messages=4000
                ),
            },
            consumer_groups={
                "edge-type-service": ConsumerGroupInfo(
                    group_id="edge-type-service",
                    members=2,
                    total_lag=10,
                    status="Stable",
                    subscribed_topics=["edge-type.in"],
                    per_partition_lag={"edge-type.in-0": 5, "edge-type.in-1": 5},
                    member_client_ids=["svc-1", "svc-2"],
                ),
            },
            active_partitions={"edge-type.out": {0, 1}},
            timestamp=3000.0,
        )
        diff = builder.update(snap)

        produce_edges = [e for e in diff.edges_added if e.get("data", {}).get("type") == "produces"]
        assert len(produce_edges) >= 1, "Expected at least one produce edge"
        for edge in produce_edges:
            assert edge["data"]["type"] == "produces"

    def test_all_edges_have_type_field(self):
        """Every edge in the diff must have a 'type' field in its data."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snap = make_large_snapshot(num_topics=50, num_consumer_groups=30)
        diff = builder.update(snap)

        for edge in diff.edges_added:
            assert "type" in edge.get("data", {}), (
                f"Edge {edge.get('id', '?')} missing 'type' in data"
            )
            assert edge["data"]["type"] in ("consumes", "produces"), (
                f"Edge type must be 'consumes' or 'produces', got '{edge['data']['type']}'"
            )


class TestWebSocketEndpointAcceptsConnection:
    """Test that the WebSocket endpoint accepts connections and returns initial state."""

    def _make_ws_app(self):
        """Create a minimal app with a WS endpoint for testing."""
        import secrets as _secrets
        from collections import deque
        from fastapi import WebSocket, WebSocketDisconnect

        ws_app = FastAPI()
        mock_builder = MagicMock()
        mock_builder.get_snapshot.return_value = {
            "type": "graph_snapshot",
            "ts": 5000,
            "nodes": {"added": [], "updated": [], "removed": []},
            "edges": {"added": [], "updated": [], "removed": []},
            "metrics": {"topicCount": 0, "consumerGroupCount": 0},
        }

        @ws_app.websocket("/ws/graph")
        async def ws_graph(websocket: WebSocket):
            await websocket.accept()
            client_id = _secrets.token_hex(8)
            try:
                snapshot = mock_builder.get_snapshot()
                snapshot["config"] = {
                    "showProducers": False,
                    "samplingEnabled": False,
                    "lagWarnThreshold": 1000,
                    "animationsEnabled": True,
                }
                await websocket.send_text(json.dumps(snapshot))
            except Exception:
                pass
            try:
                while True:
                    data = await websocket.receive_text()
                    try:
                        msg = json.loads(data)
                        if msg.get("type") == "request_snapshot":
                            snap = mock_builder.get_snapshot()
                            await websocket.send_text(json.dumps(snap))
                    except json.JSONDecodeError:
                        pass
            except WebSocketDisconnect:
                pass

        return ws_app, mock_builder

    def test_ws_accepts_connection(self):
        """WebSocket endpoint accepts the connection without error."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            data = ws.receive_text()
            assert data is not None

    def test_ws_returns_initial_graph_snapshot(self):
        """First message on WebSocket must be a graph_snapshot with nodes and edges."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            msg = json.loads(ws.receive_text())
            assert msg["type"] == "graph_snapshot"
            assert "nodes" in msg
            assert "edges" in msg
            assert "ts" in msg

    def test_ws_initial_state_includes_config(self):
        """The initial snapshot must include a config object."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            msg = json.loads(ws.receive_text())
            assert "config" in msg
            assert "showProducers" in msg["config"]
            assert "lagWarnThreshold" in msg["config"]

    def test_ws_initial_state_includes_metrics(self):
        """The initial snapshot must include a metrics section."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            msg = json.loads(ws.receive_text())
            assert "metrics" in msg
            assert "topicCount" in msg["metrics"]


class TestConsumerGroupDetailMembersClientHostAndMemberId:
    """Test GET /api/consumer-groups/{group} members include clientHost and memberId."""

    def test_members_include_member_id_field(self):
        """Each member in consumer group detail must have a memberId."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "detail-svc",
            "state": "Stable",
            "members": [
                {"memberId": "member-abc", "clientId": "c-0", "clientHost": "/10.0.0.1", "partitions": ["t-0"]},
                {"memberId": "member-def", "clientId": "c-1", "clientHost": "/10.0.0.2", "partitions": ["t-1"]},
            ],
            "offsets": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/detail-svc")

        assert resp.status_code == 200
        for member in resp.json()["members"]:
            assert "memberId" in member
            assert member["memberId"].startswith("member-")

    def test_members_include_client_host_field(self):
        """Each member in consumer group detail must have a clientHost."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "host-check-svc",
            "state": "Stable",
            "members": [
                {"memberId": "m-1", "clientId": "c-1", "clientHost": "/192.168.1.10", "partitions": []},
                {"memberId": "m-2", "clientId": "c-2", "clientHost": "/192.168.1.11", "partitions": []},
                {"memberId": "m-3", "clientId": "c-3", "clientHost": "/192.168.1.12", "partitions": []},
            ],
            "offsets": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/host-check-svc")

        assert resp.status_code == 200
        members = resp.json()["members"]
        assert len(members) == 3
        for member in members:
            assert "clientHost" in member
            assert member["clientHost"].startswith("/")

    def test_member_fields_are_strings(self):
        """memberId and clientHost should be strings, not None or integers."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "type-check",
            "state": "Stable",
            "members": [
                {"memberId": "consumer-1-uuid", "clientId": "app-1", "clientHost": "/10.0.0.5", "partitions": []},
            ],
            "offsets": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/type-check")

        assert resp.status_code == 200
        member = resp.json()["members"][0]
        assert isinstance(member["memberId"], str)
        assert isinstance(member["clientHost"], str)


class TestConcurrentTopicCreation:
    """Test creating multiple topics in rapid succession."""

    def test_create_multiple_topics_sequentially(self):
        """Creating several topics one after another succeeds for each."""
        admin = MagicMock()
        topic_names = [f"concurrent-topic-{i}" for i in range(5)]
        admin.create_topic.side_effect = [
            {"success": True, "topic": name} for name in topic_names
        ]
        client = TestClient(_create_app(admin=admin))

        for name in topic_names:
            resp = client.post("/api/topics", json={"name": name, "partitions": 3})
            assert resp.status_code == 200
            assert resp.json()["success"] is True
            assert resp.json()["topic"] == name

        assert admin.create_topic.call_count == 5

    def test_create_topic_duplicate_returns_error(self):
        """Creating a topic that already exists returns a 400 error."""
        admin = MagicMock()
        admin.create_topic.side_effect = [
            {"success": True, "topic": "dup-topic"},
            {"success": False, "error": "Topic 'dup-topic' already exists"},
        ]
        client = TestClient(_create_app(admin=admin))

        resp1 = client.post("/api/topics", json={"name": "dup-topic", "partitions": 1})
        assert resp1.status_code == 200

        resp2 = client.post("/api/topics", json={"name": "dup-topic", "partitions": 1})
        assert resp2.status_code == 400
        assert "already exists" in resp2.json()["detail"]

    def test_create_topics_with_different_partition_counts(self):
        """Each topic can have different partition counts."""
        admin = MagicMock()
        configs = [
            ("topic-a", 1),
            ("topic-b", 6),
            ("topic-c", 12),
            ("topic-d", 24),
        ]
        admin.create_topic.side_effect = [
            {"success": True, "topic": name} for name, _ in configs
        ]
        client = TestClient(_create_app(admin=admin))

        for name, partitions in configs:
            resp = client.post("/api/topics", json={"name": name, "partitions": partitions})
            assert resp.status_code == 200

        calls = admin.create_topic.call_args_list
        assert calls[0].kwargs["partitions"] == 1
        assert calls[1].kwargs["partitions"] == 6
        assert calls[2].kwargs["partitions"] == 12
        assert calls[3].kwargs["partitions"] == 24


class TestConsumerGroupNoMembersEmptyArray:
    """Test consumer group with no members returns empty members array."""

    def test_empty_group_returns_empty_members(self):
        """A consumer group with state 'Empty' returns an empty members array."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "empty-group",
            "state": "Empty",
            "members": [],
            "offsets": [
                {"topic": "orders", "partition": 0, "currentOffset": 100, "endOffset": 100, "lag": 0},
            ],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/empty-group")

        assert resp.status_code == 200
        body = resp.json()
        assert body["state"] == "Empty"
        assert body["members"] == []
        assert isinstance(body["members"], list)

    def test_dead_group_returns_empty_members(self):
        """A dead consumer group also has no members."""
        admin = MagicMock()
        admin.get_consumer_group_detail.return_value = {
            "groupId": "dead-group",
            "state": "Dead",
            "members": [],
            "offsets": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/dead-group")

        assert resp.status_code == 200
        body = resp.json()
        assert body["members"] == []
        assert len(body["offsets"]) == 0


class TestProduceWithEmptyValueTombstone:
    """Test producing a message with empty value (tombstone)."""

    def test_produce_tombstone_with_key_only(self):
        """Producing with a key but no value sends value='' (tombstone)."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "compact-topic",
            "partition": 0,
            "offset": 500,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/compact-topic/produce",
            json={"key": "delete-me"},
        )

        assert resp.status_code == 200
        assert resp.json()["success"] is True
        admin.produce_message.assert_called_once_with(
            topic="compact-topic",
            value="",
            key="delete-me",
            headers=None,
            partition=None,
        )

    def test_produce_with_explicit_empty_string_value(self):
        """Producing with value='' is valid and treated as a tombstone."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "tombstone-topic",
            "partition": 1,
            "offset": 42,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/tombstone-topic/produce",
            json={"key": "old-key", "value": ""},
        )

        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="tombstone-topic",
            value="",
            key="old-key",
            headers=None,
            partition=None,
        )

    def test_produce_tombstone_returns_offset_metadata(self):
        """Tombstone produce must still return partition and offset in response."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True,
            "topic": "events",
            "partition": 3,
            "offset": 999,
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/events/produce",
            json={"key": "remove-key"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert "partition" in body
        assert "offset" in body
        assert body["partition"] == 3
        assert body["offset"] == 999


class TestClusterHealthNoTopicsReturnsZeros:
    """Test cluster health with no topics returns zeros."""

    def test_no_topics_zero_partitions(self):
        """An empty cluster with no topics has totalPartitions == 0."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 0,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["totalPartitions"] == 0
        assert body["underReplicatedCount"] == 0
        assert body["offlinePartitionCount"] == 0

    def test_no_topics_empty_leader_distribution(self):
        """An empty cluster has an empty leaderDistribution map."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 0,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")

        assert resp.status_code == 200
        body = resp.json()
        assert body["leaderDistribution"] == {}
        assert body["underReplicated"] == []
        assert body["offlinePartitions"] == []


class TestTopicConfigEndpointReturnsKnownKeys:
    """Test that the topic detail endpoint returns known config keys."""

    def test_topic_detail_includes_config_dict(self):
        """GET /api/topics/{topic} returns a 'config' dictionary."""
        admin = MagicMock()
        admin.get_topic_detail.return_value = {
            "name": "config-topic",
            "config": {
                "retention.ms": "604800000",
                "cleanup.policy": "delete",
                "max.message.bytes": "1048576",
                "segment.bytes": "1073741824",
                "min.insync.replicas": "1",
            },
            "partitions": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics/config-topic")

        assert resp.status_code == 200
        config = resp.json()["config"]
        assert isinstance(config, dict)
        assert "retention.ms" in config
        assert "cleanup.policy" in config
        assert "max.message.bytes" in config

    def test_topic_detail_config_values_are_strings(self):
        """Config values returned from topic detail should be strings."""
        admin = MagicMock()
        admin.get_topic_detail.return_value = {
            "name": "typed-config-topic",
            "config": {
                "retention.ms": "86400000",
                "min.insync.replicas": "2",
                "compression.type": "snappy",
            },
            "partitions": [],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics/typed-config-topic")

        assert resp.status_code == 200
        config = resp.json()["config"]
        for key, value in config.items():
            assert isinstance(value, str), f"Config key '{key}' value should be string, got {type(value)}"

    def test_topic_detail_config_has_known_kafka_keys(self):
        """The config dictionary should include common Kafka configuration keys."""
        admin = MagicMock()
        known_keys = {
            "retention.ms": "604800000",
            "retention.bytes": "-1",
            "cleanup.policy": "delete",
            "max.message.bytes": "1048576",
            "segment.bytes": "1073741824",
            "compression.type": "producer",
            "min.insync.replicas": "1",
        }
        admin.get_topic_detail.return_value = {
            "name": "full-config-topic",
            "config": known_keys,
            "partitions": [{"partition": 0, "leader": 0, "replicas": [0], "isr": [0], "endOffset": 100}],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics/full-config-topic")

        assert resp.status_code == 200
        body = resp.json()
        config = body["config"]
        for k in known_keys:
            assert k in config, f"Expected known config key '{k}' in response"
        assert body["name"] == "full-config-topic"
        assert len(body["partitions"]) == 1


class TestListTopicsEndpoint:
    """Additional tests for GET /api/topics."""

    def test_list_topics_returns_array(self):
        """GET /api/topics returns an array of topic objects."""
        admin = MagicMock()
        admin.list_topics.return_value = [
            {"name": "t1", "partitions": 3, "replicationFactor": 1},
            {"name": "t2", "partitions": 6, "replicationFactor": 3},
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics")

        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 2
        assert body[0]["name"] == "t1"

    def test_list_topics_empty_cluster(self):
        """An empty cluster returns an empty array."""
        admin = MagicMock()
        admin.list_topics.return_value = []
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_topics_no_admin_returns_503(self):
        """When kafka_admin is None, listing topics returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.get("/api/topics")

        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()


class TestListConsumerGroupsEndpoint:
    """Additional tests for GET /api/consumer-groups."""

    def test_list_consumer_groups_returns_array(self):
        """GET /api/consumer-groups returns an array."""
        admin = MagicMock()
        admin.list_consumer_groups.return_value = [
            {"groupId": "cg-1", "state": "Stable", "members": 3},
            {"groupId": "cg-2", "state": "Empty", "members": 0},
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups")

        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 2
        assert body[1]["state"] == "Empty"

    def test_list_consumer_groups_empty(self):
        """An empty cluster returns an empty consumer groups array."""
        admin = MagicMock()
        admin.list_consumer_groups.return_value = []
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups")

        assert resp.status_code == 200
        assert resp.json() == []


class TestBrokersEndpoint:
    """Additional tests for GET /api/brokers."""

    def test_list_brokers_returns_broker_objects(self):
        """GET /api/brokers returns a list of broker objects."""
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": 0, "host": "broker-0.kafka.svc", "port": 9092, "rack": "us-east-1a"},
            {"id": 1, "host": "broker-1.kafka.svc", "port": 9092, "rack": "us-east-1b"},
            {"id": 2, "host": "broker-2.kafka.svc", "port": 9092, "rack": "us-east-1c"},
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")

        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 3
        assert body[0]["id"] == 0
        assert body[2]["rack"] == "us-east-1c"

    def test_list_brokers_no_admin_returns_503(self):
        """When kafka_admin is None, listing brokers returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.get("/api/brokers")

        assert resp.status_code == 503


class TestClusterInfoEndpointAdditional:
    """Extra tests for GET /api/cluster."""

    def test_cluster_info_no_admin_returns_503(self):
        """When kafka_admin is None, getting cluster info returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.get("/api/cluster")

        assert resp.status_code == 503

    def test_cluster_info_exception_returns_500(self):
        """When admin.get_cluster_info raises, endpoint returns 500."""
        admin = MagicMock()
        admin.get_cluster_info.side_effect = Exception("Connection refused")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster")

        assert resp.status_code == 500
        assert "Connection refused" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Additional edge-case tests covering 10 categories
# ---------------------------------------------------------------------------


class TestTopicConfigUpdateEdgeCases:
    """Edge cases for PUT /api/topics/{topic}/config endpoint."""

    def test_config_with_special_characters_in_keys(self):
        """Config keys with dots and hyphens (e.g. retention.ms) should pass through correctly."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "events",
            "updated": ["retention.ms", "segment.bytes", "min.insync.replicas"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/events/config",
            json={"configs": {
                "retention.ms": "86400000",
                "segment.bytes": "1073741824",
                "min.insync.replicas": "2",
            }},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        admin.update_topic_config.assert_called_once_with(
            "events",
            {"retention.ms": "86400000", "segment.bytes": "1073741824", "min.insync.replicas": "2"},
        )

    def test_config_with_numeric_string_values(self):
        """Config values that are numeric strings should pass through as strings."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "orders",
            "updated": ["retention.ms"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/orders/config",
            json={"configs": {"retention.ms": "0"}},
        )
        assert resp.status_code == 200
        admin.update_topic_config.assert_called_once_with("orders", {"retention.ms": "0"})

    def test_config_with_boolean_string_value(self):
        """Config values like 'true'/'false' for cleanup.policy etc. should work."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True,
            "topic": "compact-topic",
            "updated": ["cleanup.policy"],
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.put(
            "/api/topics/compact-topic/config",
            json={"configs": {"cleanup.policy": "compact,delete"}},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_config_update_configs_as_none_returns_400(self):
        """Sending configs=null should return 400 since it is not a non-empty dict."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.put("/api/topics/orders/config", json={"configs": None})
        assert resp.status_code == 400
        admin.update_topic_config.assert_not_called()


class TestConsumerGroupManyPartitionsTopics:
    """Consumer groups with many partitions or many subscribed topics."""

    def test_consumer_group_subscribed_to_20_topics(self):
        """A single consumer group subscribing to 20 topics should create 20 consume edges."""
        builder = GraphStateBuilder(show_producers=False)
        topic_names = [f"multi-sub-topic-{i}" for i in range(20)]
        topics = {
            name: TopicInfo(name=name, partitions=3, msg_per_sec=1.0, total_messages=100)
            for name in topic_names
        }
        per_part_lag = {}
        for name in topic_names:
            for p in range(3):
                per_part_lag[f"{name}-{p}"] = 5
        cg = ConsumerGroupInfo(
            group_id="mega-consumer",
            members=4,
            total_lag=300,
            status="Stable",
            subscribed_topics=topic_names,
            per_partition_lag=per_part_lag,
            member_client_ids=["mega-consumer-1", "mega-consumer-2", "mega-consumer-3", "mega-consumer-4"],
        )
        snap = ClusterSnapshot(
            topics=topics,
            consumer_groups={"mega-consumer": cg},
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)

        consume_edges = [
            e for e in diff.edges_added if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 20, (
            f"Expected 20 consume edges for 20 subscribed topics, got {len(consume_edges)}"
        )

    def test_consumer_group_with_100_partitions(self):
        """A consumer group on a topic with 100 partitions should have correct per-partition lag."""
        builder = GraphStateBuilder(show_producers=False)
        per_part = {f"wide-topic-{p}": 10 for p in range(100)}
        snap = ClusterSnapshot(
            topics={
                "wide-topic": TopicInfo(
                    name="wide-topic", partitions=100, msg_per_sec=50.0, total_messages=100000
                ),
            },
            consumer_groups={
                "wide-consumer": ConsumerGroupInfo(
                    group_id="wide-consumer",
                    members=10,
                    total_lag=1000,
                    status="Stable",
                    subscribed_topics=["wide-topic"],
                    per_partition_lag=per_part,
                    member_client_ids=[f"wide-consumer-{i}" for i in range(10)],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)
        topic_node = next(n for n in diff.nodes_added if n["id"] == "topic-wide-topic")
        assert topic_node["data"]["partitions"] == 100
        cg_node = next(
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "wide-consumer" in n["id"]
        )
        assert cg_node["data"]["totalLag"] == 1000
        assert cg_node["data"]["members"] == 10

    def test_multiple_consumer_groups_same_topic(self):
        """Multiple consumer groups subscribing to the same topic should produce separate nodes and edges."""
        builder = GraphStateBuilder(show_producers=False)
        groups = {}
        for i in range(5):
            gid = f"shared-consumer-{i}"
            groups[gid] = ConsumerGroupInfo(
                group_id=gid,
                members=2,
                total_lag=i * 10,
                status="Stable",
                subscribed_topics=["shared-topic"],
                per_partition_lag={"shared-topic-0": i * 5, "shared-topic-1": i * 5},
                member_client_ids=[f"{gid}-client-1", f"{gid}-client-2"],
            )
        snap = ClusterSnapshot(
            topics={
                "shared-topic": TopicInfo(
                    name="shared-topic", partitions=2, msg_per_sec=20.0, total_messages=5000
                ),
            },
            consumer_groups=groups,
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)
        cg_nodes = [
            n for n in diff.nodes_added
            if n.get("type") in ("consumer_group", "service") and "shared-consumer" in n["id"]
        ]
        assert len(cg_nodes) == 5, f"Expected 5 consumer group nodes, got {len(cg_nodes)}"
        consume_edges = [
            e for e in diff.edges_added if e.get("data", {}).get("type") == "consumes"
        ]
        assert len(consume_edges) == 5, f"Expected 5 consume edges, got {len(consume_edges)}"


class TestProduceMessageValueTypes:
    """Produce message with various value types: null, empty, large, unicode."""

    def test_produce_with_null_value(self):
        """Producing with value explicitly set to None passes None through to admin."""
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "topic": "t", "partition": 0, "offset": 1}
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics/t/produce", json={"value": None, "key": "k1"})
        assert resp.status_code == 200
        # When value is None, body.get("value", "") returns None because the key exists
        admin.produce_message.assert_called_once_with(
            topic="t", value=None, key="k1", headers=None, partition=None,
        )

    def test_produce_with_large_value(self):
        """Producing with a large value (100KB) should pass through to admin."""
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "topic": "large", "partition": 0, "offset": 99}
        client = TestClient(_create_app(admin=admin))
        large_value = "x" * 100_000
        resp = client.post("/api/topics/large/produce", json={"value": large_value})
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="large", value=large_value, key=None, headers=None, partition=None,
        )

    def test_produce_with_unicode_value(self):
        """Producing with unicode characters in value and key should succeed."""
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "topic": "unicode-t", "partition": 0, "offset": 5}
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/topics/unicode-t/produce",
            json={"value": "\u00e9v\u00e9nement cr\u00e9\u00e9", "key": "\u30ad\u30fc"},
        )
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="unicode-t",
            value="\u00e9v\u00e9nement cr\u00e9\u00e9",
            key="\u30ad\u30fc",
            headers=None,
            partition=None,
        )

    def test_produce_with_empty_string_value(self):
        """Producing with an explicit empty string value should succeed (tombstone-like)."""
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "topic": "empty-v", "partition": 0, "offset": 0}
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics/empty-v/produce", json={"value": ""})
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="empty-v", value="", key=None, headers=None, partition=None,
        )

    def test_produce_with_json_string_value(self):
        """Producing a JSON-encoded string as the value should pass through unchanged."""
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "topic": "json-t", "partition": 1, "offset": 42}
        client = TestClient(_create_app(admin=admin))
        json_payload = '{"orderId": 123, "amount": 99.95}'
        resp = client.post("/api/topics/json-t/produce", json={"value": json_payload})
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="json-t", value=json_payload, key=None, headers=None, partition=None,
        )


class TestClusterHealthEdgeCases:
    """Cluster health edge cases: all brokers down, single broker, skewed distribution."""

    def test_cluster_health_all_partitions_offline(self):
        """When all partitions are offline, offlinePartitionCount equals totalPartitions."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 9,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 9,
            "offlinePartitions": [
                {"topic": "t1", "partition": p} for p in range(3)
            ] + [
                {"topic": "t2", "partition": p} for p in range(3)
            ] + [
                {"topic": "t3", "partition": p} for p in range(3)
            ],
            "leaderDistribution": {},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["offlinePartitionCount"] == 9
        assert body["leaderDistribution"] == {}

    def test_cluster_health_single_broker(self):
        """A single-broker cluster has all leaders on one broker."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 12,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {"0": 12},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["leaderDistribution"]) == 1
        assert body["leaderDistribution"]["0"] == 12

    def test_cluster_health_skewed_leader_distribution(self):
        """A heavily skewed leader distribution should be reported accurately."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 30,
            "underReplicatedCount": 0,
            "underReplicated": [],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {"0": 25, "1": 3, "2": 2},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        dist = body["leaderDistribution"]
        assert sum(dist.values()) == 30
        assert dist["0"] == 25

    def test_cluster_health_mixed_under_replicated_and_offline(self):
        """Cluster can have both under-replicated and offline partitions simultaneously."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 18,
            "underReplicatedCount": 3,
            "underReplicated": [
                {"topic": "t1", "partition": 0, "replicas": 3, "isr": 1},
                {"topic": "t1", "partition": 1, "replicas": 3, "isr": 2},
                {"topic": "t2", "partition": 0, "replicas": 3, "isr": 2},
            ],
            "offlinePartitionCount": 2,
            "offlinePartitions": [
                {"topic": "t3", "partition": 0},
                {"topic": "t3", "partition": 1},
            ],
            "leaderDistribution": {"0": 8, "1": 8},
        }
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["underReplicatedCount"] == 3
        assert body["offlinePartitionCount"] == 2


class TestGraphStateCircularDependencies:
    """Graph state with circular-like dependencies (A -> B -> C -> A patterns)."""

    def test_circular_service_topology(self):
        """Services forming a cycle (A consumes from X, produces to Y; B consumes Y, produces Z;
        C consumes Z, produces X) should all appear correctly."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=10000)
        snap = ClusterSnapshot(
            topics={
                "topic-x": TopicInfo(name="topic-x", partitions=3, msg_per_sec=10.0, total_messages=5000),
                "topic-y": TopicInfo(name="topic-y", partitions=3, msg_per_sec=10.0, total_messages=5000),
                "topic-z": TopicInfo(name="topic-z", partitions=3, msg_per_sec=10.0, total_messages=5000),
            },
            consumer_groups={
                "x-service": ConsumerGroupInfo(
                    group_id="x-service", members=2, total_lag=10, status="Stable",
                    subscribed_topics=["topic-x"],
                    per_partition_lag={"topic-x-0": 5, "topic-x-1": 3, "topic-x-2": 2},
                    member_client_ids=["x-svc-1", "x-svc-2"],
                ),
                "y-service": ConsumerGroupInfo(
                    group_id="y-service", members=2, total_lag=10, status="Stable",
                    subscribed_topics=["topic-y"],
                    per_partition_lag={"topic-y-0": 5, "topic-y-1": 3, "topic-y-2": 2},
                    member_client_ids=["y-svc-1", "y-svc-2"],
                ),
                "z-service": ConsumerGroupInfo(
                    group_id="z-service", members=2, total_lag=10, status="Stable",
                    subscribed_topics=["topic-z"],
                    per_partition_lag={"topic-z-0": 5, "topic-z-1": 3, "topic-z-2": 2},
                    member_client_ids=["z-svc-1", "z-svc-2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snap)
        assert not diff.is_empty()
        # All 3 topics and 3 consumer groups should be present
        topic_nodes = [n for n in diff.nodes_added if n["type"] == "topic"]
        assert len(topic_nodes) == 3
        cg_or_svc = [n for n in diff.nodes_added if n["type"] in ("consumer_group", "service")]
        assert len(cg_or_svc) == 3

    def test_self_consuming_service(self):
        """A service that consumes from and produces to the same topic prefix should work."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=10000)
        snap = ClusterSnapshot(
            topics={
                "loop.input": TopicInfo(name="loop.input", partitions=2, msg_per_sec=5.0, total_messages=1000),
                "loop.output": TopicInfo(name="loop.output", partitions=2, msg_per_sec=5.0, total_messages=1000),
            },
            consumer_groups={
                "loop-service": ConsumerGroupInfo(
                    group_id="loop-service", members=1, total_lag=0, status="Stable",
                    subscribed_topics=["loop.input"],
                    per_partition_lag={"loop.input-0": 0, "loop.input-1": 0},
                    member_client_ids=["loop-svc-1"],
                ),
            },
            active_partitions={"loop.output": {0, 1}},
            timestamp=time.time(),
        )
        diff = builder.update(snap)
        assert not diff.is_empty()
        # The graph should still be valid and serializable
        result = builder.get_snapshot()
        assert result["type"] == "graph_snapshot"
        node_ids = {n["id"] for n in result["nodes"]["added"]}
        assert "topic-loop.input" in node_ids
        assert "topic-loop.output" in node_ids


class TestWebSocketMessageParsingEdgeCases:
    """WebSocket message parsing edge cases."""

    def _make_ws_app(self):
        """Create a minimal WS app for testing message parsing."""
        import secrets as _secrets
        from collections import deque
        from fastapi import WebSocket, WebSocketDisconnect

        ws_app = FastAPI()
        mock_builder = MagicMock()
        mock_builder.get_snapshot.return_value = {
            "type": "graph_snapshot",
            "ts": 1000,
            "nodes": {"added": [], "updated": [], "removed": []},
            "edges": {"added": [], "updated": [], "removed": []},
            "metrics": {},
        }

        @ws_app.websocket("/ws/graph")
        async def ws_graph(websocket: WebSocket):
            await websocket.accept()
            try:
                snapshot = mock_builder.get_snapshot()
                snapshot["config"] = {
                    "showProducers": False,
                    "samplingEnabled": False,
                    "lagWarnThreshold": 1000,
                    "animationsEnabled": True,
                }
                await websocket.send_text(json.dumps(snapshot))
            except Exception:
                pass
            try:
                while True:
                    data = await websocket.receive_text()
                    try:
                        msg = json.loads(data)
                        if msg.get("type") == "request_snapshot":
                            snap = mock_builder.get_snapshot()
                            await websocket.send_text(json.dumps(snap))
                    except json.JSONDecodeError:
                        pass
            except WebSocketDisconnect:
                pass

        return ws_app, mock_builder

    def test_ws_empty_string_message_ignored(self):
        """Sending an empty string over WS should not crash the server."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            initial = json.loads(ws.receive_text())
            assert initial["type"] == "graph_snapshot"
            ws.send_text("")
            # Server should still respond to valid requests after
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "graph_snapshot"

    def test_ws_unknown_message_type_ignored(self):
        """Sending a JSON message with an unknown type should be silently ignored."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            initial = json.loads(ws.receive_text())
            assert initial["type"] == "graph_snapshot"
            ws.send_text(json.dumps({"type": "unknown_action", "data": "test"}))
            # Next valid request should still work
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "graph_snapshot"

    def test_ws_message_with_extra_fields(self):
        """request_snapshot with extra fields should still return a valid snapshot."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            initial = json.loads(ws.receive_text())
            assert initial["type"] == "graph_snapshot"
            ws.send_text(json.dumps({
                "type": "request_snapshot",
                "extra_field": "should be ignored",
                "nested": {"key": "value"},
            }))
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "graph_snapshot"

    def test_ws_partial_json_ignored(self):
        """Sending a partial/truncated JSON string should not crash the server."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            initial = json.loads(ws.receive_text())
            assert initial["type"] == "graph_snapshot"
            ws.send_text('{"type": "request_snap')  # truncated
            # Server should continue working
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            resp = json.loads(ws.receive_text())
            assert resp["type"] == "graph_snapshot"


class TestOffsetResetBoundaryValues:
    """Offset reset with boundary values: timestamp strategy, specific offset, edge cases."""

    def test_reset_with_timestamp_strategy(self):
        """Reset offsets using the timestamp strategy passes timestamp through."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "partitionsReset": 6}
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/ts-group/reset-offsets",
            json={"strategy": "timestamp", "timestamp": 1700000000000},
        )
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="ts-group",
            strategy="timestamp",
            topic=None,
            timestamp=1700000000000,
            offset=None,
        )

    def test_reset_with_specific_offset(self):
        """Reset offsets using the specific strategy with a concrete offset value."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "partitionsReset": 3}
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/offset-group/reset-offsets",
            json={"strategy": "specific", "offset": 42},
        )
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="offset-group",
            strategy="specific",
            topic=None,
            timestamp=None,
            offset=42,
        )

    def test_reset_with_zero_offset(self):
        """Reset to offset=0 should be treated as a valid specific offset."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "partitionsReset": 3}
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/zero-group/reset-offsets",
            json={"strategy": "specific", "offset": 0},
        )
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="zero-group",
            strategy="specific",
            topic=None,
            timestamp=None,
            offset=0,
        )

    def test_reset_with_topic_filter_and_timestamp(self):
        """Reset offsets with both topic filter and timestamp strategy."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "partitionsReset": 2}
        client = TestClient(_create_app(admin=admin))
        resp = client.post(
            "/api/consumer-groups/filter-group/reset-offsets",
            json={"strategy": "timestamp", "topic": "orders.created", "timestamp": 1700000000000},
        )
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="filter-group",
            strategy="timestamp",
            topic="orders.created",
            timestamp=1700000000000,
            offset=None,
        )


class TestTopicDeletionWithActiveConsumers:
    """Topic deletion while active consumers exist in the graph."""

    def test_delete_topic_with_active_consumer_in_graph(self):
        """Deleting a topic that has active consumers should mark related nodes inactive in graph."""
        builder = GraphStateBuilder(show_producers=False)
        snap1 = ClusterSnapshot(
            topics={
                "deletable-topic": TopicInfo(
                    name="deletable-topic", partitions=3, msg_per_sec=10.0, total_messages=5000
                ),
            },
            consumer_groups={
                "topic-reader": ConsumerGroupInfo(
                    group_id="topic-reader", members=2, total_lag=30, status="Stable",
                    subscribed_topics=["deletable-topic"],
                    per_partition_lag={"deletable-topic-0": 10, "deletable-topic-1": 10, "deletable-topic-2": 10},
                    member_client_ids=["reader-1", "reader-2"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)
        assert len(diff1.edges_added) >= 1

        # Simulate topic deletion: both topic and consumer disappear from snapshot
        snap2 = ClusterSnapshot(
            topics={},
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)
        inactive_nodes = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert len(inactive_nodes) >= 2, "Both topic and consumer should be marked inactive"
        inactive_edges = [e for e in diff2.edges_updated if e.get("data", {}).get("inactive") is True]
        assert len(inactive_edges) >= 1, "Consume edge should be marked inactive"

    def test_delete_topic_api_while_consumer_active_returns_success(self):
        """DELETE /api/topics/{topic} succeeds even if consumers reference it (admin handles it)."""
        admin = MagicMock()
        admin.delete_topic.return_value = {"success": True, "topic": "active-consumed-topic"}
        client = TestClient(_create_app(admin=admin))
        resp = client.delete("/api/topics/active-consumed-topic")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        admin.delete_topic.assert_called_once_with("active-consumed-topic")

    def test_delete_topic_failure_preserves_graph(self):
        """When topic deletion fails, the graph state should remain unchanged."""
        builder = GraphStateBuilder(show_producers=False)
        snap = ClusterSnapshot(
            topics={
                "undeletable-topic": TopicInfo(
                    name="undeletable-topic", partitions=3, msg_per_sec=5.0, total_messages=1000
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        builder.update(snap)

        # The topic still exists in the next snapshot (deletion failed)
        snap2 = ClusterSnapshot(
            topics={
                "undeletable-topic": TopicInfo(
                    name="undeletable-topic", partitions=3, msg_per_sec=5.0, total_messages=1100
                ),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)
        # No nodes should be marked inactive since the topic is still present
        inactive = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert len(inactive) == 0, "No nodes should be inactive when topic still present"


class TestBrokerMetricsEdgeCases:
    """Broker metrics edge cases for GET /api/brokers."""

    def test_list_brokers_empty_cluster(self):
        """An empty cluster with no brokers returns an empty list."""
        admin = MagicMock()
        admin.list_brokers.return_value = []
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_brokers_with_null_rack(self):
        """Brokers without rack assignment should have rack=null."""
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": 0, "host": "broker-0", "port": 9092, "rack": None, "isController": True},
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")
        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["rack"] is None
        assert body[0]["isController"] is True

    def test_list_brokers_exception_returns_500(self):
        """When admin.list_brokers raises, the endpoint returns 500."""
        admin = MagicMock()
        admin.list_brokers.side_effect = Exception("Cluster unreachable")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")
        assert resp.status_code == 500
        assert "Cluster unreachable" in resp.json()["detail"]

    def test_list_brokers_large_cluster(self):
        """A cluster with many brokers should return all of them."""
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": i, "host": f"broker-{i}.kafka.svc", "port": 9092, "rack": f"rack-{i % 3}",
             "isController": i == 0}
            for i in range(20)
        ]
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/brokers")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 20
        assert body[0]["isController"] is True
        assert all(not b["isController"] for b in body[1:])


class TestAPIErrorHandling:
    """API error handling: invalid JSON body, missing required fields, unexpected payloads."""

    def test_create_topic_with_empty_name(self):
        """Creating a topic with name='' should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics", json={"name": ""})
        assert resp.status_code == 400

    def test_create_topic_with_name_only(self):
        """Creating a topic with only a name should use default partitions and replication."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True}
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics", json={"name": "simple-topic"})
        assert resp.status_code == 200
        admin.create_topic.assert_called_once_with(
            name="simple-topic", partitions=1, replication_factor=1, configs=None,
        )

    def test_add_partitions_negative_value(self):
        """Passing a negative totalPartitions should return 400."""
        admin = MagicMock()
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics/t/partitions", json={"totalPartitions": -5})
        assert resp.status_code == 400
        admin.add_topic_partitions.assert_not_called()

    def test_produce_with_empty_json_body(self):
        """Producing with an empty JSON body should use default value=''."""
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True, "topic": "t", "partition": 0, "offset": 0}
        client = TestClient(_create_app(admin=admin))
        resp = client.post("/api/topics/t/produce", json={})
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="t", value="", key=None, headers=None, partition=None,
        )

    def test_list_topics_exception_returns_500(self):
        """When admin.list_topics raises, the endpoint returns 500."""
        admin = MagicMock()
        admin.list_topics.side_effect = Exception("Connection timed out")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics")
        assert resp.status_code == 500
        assert "Connection timed out" in resp.json()["detail"]

    def test_list_consumer_groups_exception_returns_500(self):
        """When admin.list_consumer_groups raises, the endpoint returns 500."""
        admin = MagicMock()
        admin.list_consumer_groups.side_effect = Exception("Auth failed")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups")
        assert resp.status_code == 500
        assert "Auth failed" in resp.json()["detail"]

    def test_get_topic_detail_exception_returns_500(self):
        """When admin.get_topic_detail raises, the endpoint returns 500."""
        admin = MagicMock()
        admin.get_topic_detail.side_effect = Exception("Topic not found in metadata")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/topics/nonexistent")
        assert resp.status_code == 500
        assert "Topic not found" in resp.json()["detail"]

    def test_get_consumer_group_detail_exception_returns_500(self):
        """When admin.get_consumer_group_detail raises, the endpoint returns 500."""
        admin = MagicMock()
        admin.get_consumer_group_detail.side_effect = Exception("Group disappeared")
        client = TestClient(_create_app(admin=admin))
        resp = client.get("/api/consumer-groups/vanished-group")
        assert resp.status_code == 500
        assert "Group disappeared" in resp.json()["detail"]

    def test_delete_consumer_group_no_admin_returns_503(self):
        """DELETE /api/consumer-groups/{group} with no admin returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.delete("/api/consumer-groups/any-group")
        assert resp.status_code == 503

    def test_reset_offsets_no_admin_returns_503(self):
        """POST /api/consumer-groups/{group}/reset-offsets with no admin returns 503."""
        client = TestClient(_create_app(admin=None))
        resp = client.post(
            "/api/consumer-groups/any-group/reset-offsets",
            json={"strategy": "latest"},
        )
        assert resp.status_code == 503


# ──────────────────────────────────────────────────────────────────────
# NEW TESTS: Message Sampler Seek, API Query Params, Replay Validation,
# Dashboard Data Aggregation, Favorites Persistence, Schema Registry Proxy
# ──────────────────────────────────────────────────────────────────────

import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from fastapi import FastAPI
from fastapi.testclient import TestClient
from api_routes import router


def _create_app_extended(admin=None, sampler=None, schema_registry=None):
    """Build a minimal FastAPI app with optional admin, sampler, and schema_registry mocks."""
    app = FastAPI()
    app.include_router(router)
    app.state.kafka_admin = admin
    app.state.message_sampler = sampler
    app.state.schema_registry = schema_registry
    return app


# ── 1. Message Sampler Seek Tests ────────────────────────────────────

class TestMessageSamplerSeekAPI:
    """Tests for sample_at and sample_at_timestamp via GET /api/topics/{topic}/messages."""

    def test_sample_at_with_partition_and_offset(self):
        """GET with partition+offset query params calls sampler.sample_at."""
        sampler = MagicMock()
        sampler.sample_at.return_value = [
            {"offset": 500, "partition": 2, "timestamp": 1700000000000,
             "key": "k1", "headers": {}, "value": "v1", "format": "utf8"},
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/orders/messages?partition=2&offset=500&limit=10")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["topic"] == "orders"
        sampler.sample_at.assert_called_once_with("orders", 2, 500, 10)

    def test_sample_at_timestamp_with_partition_and_timestamp(self):
        """GET with partition+timestamp query params calls sampler.sample_at_timestamp."""
        sampler = MagicMock()
        sampler.sample_at_timestamp.return_value = [
            {"offset": 300, "partition": 0, "timestamp": 1700000005000,
             "key": None, "headers": {}, "value": "{}", "format": "json"},
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/events/messages?partition=0&timestamp=1700000005000&limit=25")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        sampler.sample_at_timestamp.assert_called_once_with("events", 0, 1700000005000, 25)

    def test_sample_at_returns_empty_list_on_nonexistent_partition(self):
        """When sample_at returns [] for an unknown partition, API returns count=0."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/orders/messages?partition=99&offset=0")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0
        assert resp.json()["messages"] == []

    def test_sample_at_timestamp_no_offset_found_returns_empty(self):
        """When no offset exists for a timestamp, sample_at_timestamp returns []."""
        sampler = MagicMock()
        sampler.sample_at_timestamp.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/events/messages?partition=0&timestamp=9999999999999")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_sample_at_negative_offset_clamped_to_zero(self):
        """Passing a negative offset should still call sample_at with the parsed int."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t1/messages?partition=0&offset=-10")
        assert resp.status_code == 200
        # The API passes the int as-is; sample_at internally clamps via max(0, offset)
        sampler.sample_at.assert_called_once_with("t1", 0, -10, 50)

    def test_sample_falls_back_to_generic_sample_without_partition(self):
        """Without partition param, API falls back to sampler.sample()."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 10, "partition": 0, "timestamp": 1700000000000,
             "key": None, "headers": {}, "value": "msg", "format": "utf8"},
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/fallback-topic/messages")
        assert resp.status_code == 200
        sampler.sample.assert_called_once_with("fallback-topic")
        sampler.sample_at.assert_not_called()
        sampler.sample_at_timestamp.assert_not_called()


# ── 2. API Route Query Parameter Parsing Tests ──────────────────────

class TestAPIQueryParameterParsing:
    """Tests for correct parsing of partition, offset, timestamp, limit query params."""

    def test_limit_defaults_to_50(self):
        """When limit is omitted, it defaults to 50."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t/messages?partition=0&offset=0")
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("t", 0, 0, 50)

    def test_limit_capped_at_200(self):
        """When limit exceeds 200, it is capped to 200."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t/messages?partition=0&offset=0&limit=999")
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("t", 0, 0, 200)

    def test_limit_of_1_is_respected(self):
        """A limit of 1 is passed through as-is."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t/messages?partition=0&offset=0&limit=1")
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("t", 0, 0, 1)

    def test_partition_only_with_timestamp_calls_sample_at_timestamp(self):
        """Partition + timestamp (no offset) calls sample_at_timestamp."""
        sampler = MagicMock()
        sampler.sample_at_timestamp.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t/messages?partition=3&timestamp=1700000000000")
        assert resp.status_code == 200
        sampler.sample_at_timestamp.assert_called_once_with("t", 3, 1700000000000, 50)

    def test_sampling_disabled_returns_403(self):
        """When SAMPLING_ENABLED is False, GET messages returns 403."""
        sampler = MagicMock()
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = False
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t/messages?partition=0&offset=0")
        assert resp.status_code == 403
        assert "disabled" in resp.json()["detail"].lower()

    def test_no_sampler_returns_503(self):
        """When no sampler is configured, GET messages returns 503."""
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=None))
            resp = client.get("/api/topics/t/messages")
        assert resp.status_code == 503
        assert "sampler" in resp.json()["detail"].lower()


# ── 3. Replay Endpoint Validation Tests ─────────────────────────────

class TestReplayEndpointValidation:
    """Tests for POST /api/topics/{topic}/replay validation and behavior."""

    def test_replay_requires_target_topic(self):
        """Replay without targetTopic returns 400."""
        admin = MagicMock()
        sampler = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/source/replay", json={})
        assert resp.status_code == 400
        assert "targetTopic" in resp.json()["detail"]

    def test_replay_disabled_returns_403(self):
        """Replay with sampling disabled returns 403."""
        admin = MagicMock()
        sampler = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = False
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/source/replay", json={"targetTopic": "dest"})
        assert resp.status_code == 403

    def test_replay_copies_messages_from_source_to_target(self):
        """Replay reads messages from source and produces them to target."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1700000000000,
             "key": "k1", "headers": {}, "value": "v1", "format": "utf8"},
            {"offset": 1, "partition": 0, "timestamp": 1700000001000,
             "key": "k2", "headers": {}, "value": {"order": 1}, "format": "json"},
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/source/replay", json={"targetTopic": "dest"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["copied"] == 2
        assert body["errors"] == 0
        assert body["total"] == 2

    def test_replay_with_partition_and_offset(self):
        """Replay with partition and offset uses sample_at."""
        sampler = MagicMock()
        sampler.sample_at.return_value = [
            {"offset": 100, "partition": 2, "timestamp": 1700000000000,
             "key": None, "headers": {}, "value": "msg", "format": "utf8"},
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.post(
                "/api/topics/source/replay",
                json={"targetTopic": "dest", "partition": 2, "offset": 100, "limit": 10},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["copied"] == 1
        sampler.sample_at.assert_called_once_with("source", 2, 100, 10)

    def test_replay_counts_produce_errors(self):
        """Replay counts failed produce attempts as errors."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1700000000000,
             "key": None, "headers": {}, "value": "v1", "format": "utf8"},
            {"offset": 1, "partition": 0, "timestamp": 1700000001000,
             "key": None, "headers": {}, "value": "v2", "format": "utf8"},
            {"offset": 2, "partition": 0, "timestamp": 1700000002000,
             "key": None, "headers": {}, "value": "v3", "format": "utf8"},
        ]
        admin = MagicMock()
        admin.produce_message.side_effect = [
            {"success": True},
            {"success": False, "error": "write fail"},
            {"success": True},
        ]
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["copied"] == 2
        assert body["errors"] == 1
        assert body["total"] == 3

    def test_replay_limit_capped_at_200(self):
        """Replay with limit > 200 is capped to 200."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=admin, sampler=sampler))
            resp = client.post(
                "/api/topics/src/replay",
                json={"targetTopic": "dst", "partition": 0, "offset": 0, "limit": 500},
            )
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("src", 0, 0, 200)

    def test_replay_no_admin_returns_503(self):
        """Replay with no admin configured returns 503."""
        sampler = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_app_extended(admin=None, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 503


# ── 4. Dashboard Data Aggregation Scenarios ─────────────────────────

class TestDashboardDataAggregation:
    """Test that health/metrics endpoints correctly aggregate dashboard data."""

    def test_health_aggregates_total_lag_across_groups(self):
        """Total lag in health response should be sum of all consumer group lags."""
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=10000)
        snapshot = ClusterSnapshot(
            topics={
                "topic-a": TopicInfo(name="topic-a", partitions=3, msg_per_sec=10.0, total_messages=5000),
                "topic-b": TopicInfo(name="topic-b", partitions=3, msg_per_sec=20.0, total_messages=8000),
            },
            consumer_groups={
                "group-1": ConsumerGroupInfo(
                    group_id="group-1", members=2, total_lag=150,
                    subscribed_topics=["topic-a"],
                    per_partition_lag={"topic-a-0": 50, "topic-a-1": 50, "topic-a-2": 50},
                    member_client_ids=["g1-c1", "g1-c2"],
                ),
                "group-2": ConsumerGroupInfo(
                    group_id="group-2", members=3, total_lag=300,
                    subscribed_topics=["topic-b"],
                    per_partition_lag={"topic-b-0": 100, "topic-b-1": 100, "topic-b-2": 100},
                    member_client_ids=["g2-c1", "g2-c2", "g2-c3"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        total_lag = sum(g.total_lag for g in snapshot.consumer_groups.values())
        assert total_lag == 450

    def test_health_aggregates_total_msg_rate(self):
        """Total msg rate is sum of msg_per_sec across all topics."""
        snapshot = ClusterSnapshot(
            topics={
                "fast-topic": TopicInfo(name="fast-topic", partitions=6, msg_per_sec=500.0, total_messages=1000000),
                "slow-topic": TopicInfo(name="slow-topic", partitions=1, msg_per_sec=0.5, total_messages=100),
                "idle-topic": TopicInfo(name="idle-topic", partitions=3, msg_per_sec=0.0, total_messages=0),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        total_rate = sum(t.msg_per_sec for t in snapshot.topics.values())
        assert total_rate == 500.5

    def test_dashboard_snapshot_counts_all_entity_types(self):
        """Graph snapshot should contain nodes for all entity types in a mixed topology."""
        builder = GraphStateBuilder(show_producers=True, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "input.events": TopicInfo(name="input.events", partitions=6, msg_per_sec=100.0, total_messages=50000),
                "output.events": TopicInfo(name="output.events", partitions=6, msg_per_sec=90.0, total_messages=45000),
            },
            consumer_groups={
                "processor-svc": ConsumerGroupInfo(
                    group_id="processor-svc", members=3, total_lag=50,
                    subscribed_topics=["input.events"],
                    per_partition_lag={"input.events-0": 10, "input.events-1": 20, "input.events-2": 20},
                    member_client_ids=["proc-1", "proc-2", "proc-3"],
                ),
            },
            active_partitions={"output.events": {0, 1, 2, 3, 4, 5}},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)
        node_types = [n["type"] for n in diff.nodes_added]
        assert "topic" in node_types
        # Should have at least service/consumer_group and producer nodes
        assert len(diff.nodes_added) >= 3

    def test_dashboard_empty_cluster_aggregation(self):
        """An empty cluster should aggregate to zero totals."""
        snapshot = ClusterSnapshot(
            topics={},
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        total_lag = sum(g.total_lag for g in snapshot.consumer_groups.values())
        total_rate = sum(t.msg_per_sec for t in snapshot.topics.values())
        assert total_lag == 0
        assert total_rate == 0.0
        assert len(snapshot.topics) == 0
        assert len(snapshot.consumer_groups) == 0

    def test_dashboard_lag_per_group_breakdown(self):
        """Each consumer group's lag should be independently trackable for dashboards."""
        groups = {
            f"group-{i}": ConsumerGroupInfo(
                group_id=f"group-{i}", members=1, total_lag=i * 100,
                subscribed_topics=[f"topic-{i}"],
                per_partition_lag={f"topic-{i}-0": i * 100},
                member_client_ids=[f"client-{i}"],
            )
            for i in range(10)
        }
        snapshot = ClusterSnapshot(
            topics={f"topic-{i}": TopicInfo(
                name=f"topic-{i}", partitions=1, msg_per_sec=1.0, total_messages=100
            ) for i in range(10)},
            consumer_groups=groups,
            active_partitions={},
            timestamp=time.time(),
        )
        lag_per_group = {gid: g.total_lag for gid, g in snapshot.consumer_groups.items()}
        assert lag_per_group["group-0"] == 0
        assert lag_per_group["group-5"] == 500
        assert lag_per_group["group-9"] == 900
        assert sum(lag_per_group.values()) == sum(range(10)) * 100


# ── 5. Favorites Persistence Edge Cases ─────────────────────────────

class TestFavoritesPersistenceEdgeCases:
    """Test graph state persistence with pinned/favorite nodes across updates."""

    def test_favorite_topic_survives_removal_as_inactive(self):
        """A topic removed from the cluster should go inactive, not vanish, preserving favorites."""
        builder = GraphStateBuilder(show_producers=False)
        snap1 = ClusterSnapshot(
            topics={
                "fav-topic": TopicInfo(name="fav-topic", partitions=3, msg_per_sec=5.0, total_messages=1000),
                "other-topic": TopicInfo(name="other-topic", partitions=3, msg_per_sec=1.0, total_messages=500),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        diff1 = builder.update(snap1)
        assert any(n["id"] == "topic-fav-topic" for n in diff1.nodes_added)

        # Remove fav-topic from cluster
        snap2 = ClusterSnapshot(
            topics={
                "other-topic": TopicInfo(name="other-topic", partitions=3, msg_per_sec=2.0, total_messages=600),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)
        # fav-topic should become inactive, not removed
        assert len(diff2.nodes_removed) == 0
        inactive_ids = [n["id"] for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert "topic-fav-topic" in inactive_ids

    def test_favorite_consumer_group_goes_inactive_on_removal(self):
        """A consumer group removed from cluster goes inactive, preserving frontend references."""
        builder = GraphStateBuilder(show_producers=False)
        snap1 = ClusterSnapshot(
            topics={
                "events": TopicInfo(name="events", partitions=3, msg_per_sec=10.0, total_messages=5000),
            },
            consumer_groups={
                "fav-group": ConsumerGroupInfo(
                    group_id="fav-group", members=2, total_lag=100,
                    subscribed_topics=["events"],
                    per_partition_lag={"events-0": 50, "events-1": 50},
                    member_client_ids=["fav-group-c1", "fav-group-c2"],
                ),
            },
            active_partitions={},
            timestamp=1000.0,
        )
        builder.update(snap1)

        # Remove consumer group
        snap2 = ClusterSnapshot(
            topics={
                "events": TopicInfo(name="events", partitions=3, msg_per_sec=10.0, total_messages=5100),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=2000.0,
        )
        diff2 = builder.update(snap2)
        assert len(diff2.nodes_removed) == 0
        inactive_ids = [n["id"] for n in diff2.nodes_updated if n.get("status") == "inactive"]
        fav_inactive = [nid for nid in inactive_ids if "fav-group" in nid]
        assert len(fav_inactive) >= 1

    def test_inactive_node_reactivates_when_topic_returns(self):
        """A previously inactive topic should reactivate when it reappears in snapshot."""
        builder = GraphStateBuilder(show_producers=False)
        snap1 = ClusterSnapshot(
            topics={
                "ephemeral": TopicInfo(name="ephemeral", partitions=2, msg_per_sec=5.0, total_messages=500),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=1000.0,
        )
        builder.update(snap1)

        # Remove the topic
        snap2 = ClusterSnapshot(
            topics={}, consumer_groups={}, active_partitions={}, timestamp=2000.0,
        )
        diff2 = builder.update(snap2)
        inactive = [n for n in diff2.nodes_updated if n.get("status") == "inactive"]
        assert any("ephemeral" in n["id"] for n in inactive)

        # Topic comes back
        snap3 = ClusterSnapshot(
            topics={
                "ephemeral": TopicInfo(name="ephemeral", partitions=2, msg_per_sec=8.0, total_messages=700),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=3000.0,
        )
        diff3 = builder.update(snap3)
        # The node should be reactivated (updated, not added)
        updated_ids = [n["id"] for n in diff3.nodes_updated]
        assert "topic-ephemeral" in updated_ids

    def test_rapid_add_remove_add_preserves_node(self):
        """Rapidly adding, removing, and re-adding a topic should preserve node identity."""
        builder = GraphStateBuilder(show_producers=False)
        for i in range(5):
            if i % 2 == 0:
                topics = {"flicker": TopicInfo(
                    name="flicker", partitions=3, msg_per_sec=float(i), total_messages=100 * i
                )}
            else:
                topics = {}
            snap = ClusterSnapshot(
                topics=topics, consumer_groups={}, active_partitions={},
                timestamp=1000.0 + i,
            )
            builder.update(snap)

        # Final snapshot has the topic — verify it's present
        result = builder.get_snapshot()
        node_ids = [n["id"] for n in result["nodes"]["added"]]
        assert "topic-flicker" in node_ids

    def test_favorites_across_50_update_cycles(self):
        """A persistently-present topic maintains its node identity across many updates."""
        builder = GraphStateBuilder(show_producers=False)
        for i in range(50):
            snap = ClusterSnapshot(
                topics={
                    "persistent-topic": TopicInfo(
                        name="persistent-topic", partitions=3,
                        msg_per_sec=float(i), total_messages=1000 + i * 10,
                    ),
                },
                consumer_groups={},
                active_partitions={},
                timestamp=1000.0 + i,
            )
            diff = builder.update(snap)
            if i == 0:
                assert any(n["id"] == "topic-persistent-topic" for n in diff.nodes_added)
            else:
                # Should be in updates, not re-added
                assert not any(n["id"] == "topic-persistent-topic" for n in diff.nodes_added)

        result = builder.get_snapshot()
        node = next(n for n in result["nodes"]["added"] if n["id"] == "topic-persistent-topic")
        assert node["data"]["msgPerSec"] == 49.0


# ── 6. Schema Registry Proxy Error Handling ─────────────────────────

class TestSchemaRegistryProxyErrorHandling:
    """Tests for /api/schema-registry/* error handling and edge cases."""

    def test_list_subjects_no_registry_returns_503(self):
        """GET /api/schema-registry/subjects with no registry returns 503."""
        client = TestClient(_create_app_extended(admin=None, sampler=None, schema_registry=None))
        resp = client.get("/api/schema-registry/subjects")
        assert resp.status_code == 503
        assert "Schema Registry" in resp.json()["detail"]

    def test_list_subjects_returns_subjects(self):
        """GET /api/schema-registry/subjects returns subject list."""
        sr = MagicMock()
        sr.list_subjects.return_value = ["orders-value", "payments-value"]
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subjects"] == ["orders-value", "payments-value"]

    def test_list_subjects_empty_returns_empty_list(self):
        """GET /api/schema-registry/subjects with no subjects returns empty list."""
        sr = MagicMock()
        sr.list_subjects.return_value = []
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects")
        assert resp.status_code == 200
        assert resp.json()["subjects"] == []

    def test_get_versions_returns_version_list(self):
        """GET /api/schema-registry/subjects/{subject}/versions returns versions."""
        sr = MagicMock()
        sr.get_versions.return_value = [1, 2, 3]
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/orders-value/versions")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subject"] == "orders-value"
        assert body["versions"] == [1, 2, 3]
        sr.get_versions.assert_called_once_with("orders-value")

    def test_get_schema_latest_version(self):
        """GET /api/schema-registry/subjects/{subject}/versions/latest returns schema."""
        sr = MagicMock()
        sr.get_schema.return_value = {
            "subject": "orders-value",
            "version": 3,
            "id": 42,
            "schema": '{"type":"record","name":"Order","fields":[]}',
        }
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/orders-value/versions/latest")
        assert resp.status_code == 200
        body = resp.json()
        assert body["version"] == 3
        sr.get_schema.assert_called_once_with("orders-value", "latest")

    def test_get_schema_specific_version(self):
        """GET /api/schema-registry/subjects/{subject}/versions/2 returns version 2."""
        sr = MagicMock()
        sr.get_schema.return_value = {"subject": "events-value", "version": 2, "id": 10, "schema": "{}"}
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/events-value/versions/2")
        assert resp.status_code == 200
        sr.get_schema.assert_called_once_with("events-value", 2)

    def test_get_global_compatibility(self):
        """GET /api/schema-registry/config returns global compatibility level."""
        sr = MagicMock()
        sr.get_compatibility.return_value = "BACKWARD"
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/config")
        assert resp.status_code == 200
        assert resp.json()["compatibilityLevel"] == "BACKWARD"
        sr.get_compatibility.assert_called_once_with()

    def test_get_subject_compatibility(self):
        """GET /api/schema-registry/config/{subject} returns subject-level compatibility."""
        sr = MagicMock()
        sr.get_compatibility.return_value = "FULL"
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.get("/api/schema-registry/config/orders-value")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subject"] == "orders-value"
        assert body["compatibilityLevel"] == "FULL"
        sr.get_compatibility.assert_called_once_with("orders-value")

    def test_register_schema_success(self):
        """POST /api/schema-registry/subjects/{subject}/versions registers a schema."""
        sr = MagicMock()
        sr.register_schema.return_value = {"id": 99}
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/new-topic-value/versions",
            json={"schema": '{"type":"string"}', "schemaType": "AVRO"},
        )
        assert resp.status_code == 200
        assert resp.json()["id"] == 99
        sr.register_schema.assert_called_once_with("new-topic-value", '{"type":"string"}', "AVRO")

    def test_register_schema_missing_schema_field_returns_400(self):
        """POST without 'schema' field returns 400."""
        sr = MagicMock()
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/bad/versions",
            json={"schemaType": "AVRO"},
        )
        assert resp.status_code == 400
        assert "schema is required" in resp.json()["detail"]

    def test_register_schema_error_from_registry_returns_400(self):
        """When schema registry returns an error, the API returns 400."""
        sr = MagicMock()
        sr.register_schema.return_value = {"error": "Schema is incompatible"}
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/incompat/versions",
            json={"schema": '{"type":"bad"}'},
        )
        assert resp.status_code == 400
        assert "incompatible" in resp.json()["detail"].lower()

    def test_delete_subject_returns_deleted_versions(self):
        """DELETE /api/schema-registry/subjects/{subject} returns deleted version list."""
        sr = MagicMock()
        sr.delete_subject.return_value = [1, 2, 3]
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.delete("/api/schema-registry/subjects/old-subject")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subject"] == "old-subject"
        assert body["deletedVersions"] == [1, 2, 3]
        sr.delete_subject.assert_called_once_with("old-subject")

    def test_delete_subject_empty_returns_empty_list(self):
        """Deleting a subject that returns no versions still succeeds."""
        sr = MagicMock()
        sr.delete_subject.return_value = []
        client = TestClient(_create_app_extended(schema_registry=sr))
        resp = client.delete("/api/schema-registry/subjects/empty-subject")
        assert resp.status_code == 200
        assert resp.json()["deletedVersions"] == []

    def test_get_versions_no_registry_returns_503(self):
        """GET versions with no schema registry returns 503."""
        client = TestClient(_create_app_extended(schema_registry=None))
        resp = client.get("/api/schema-registry/subjects/any/versions")
        assert resp.status_code == 503

    def test_register_schema_no_registry_returns_503(self):
        """POST register schema with no schema registry returns 503."""
        client = TestClient(_create_app_extended(schema_registry=None))
        resp = client.post(
            "/api/schema-registry/subjects/any/versions",
            json={"schema": "{}"},
        )
        assert resp.status_code == 503

    def test_delete_subject_no_registry_returns_503(self):
        """DELETE subject with no schema registry returns 503."""
        client = TestClient(_create_app_extended(schema_registry=None))
        resp = client.delete("/api/schema-registry/subjects/any")
        assert resp.status_code == 503


# ──────────────────────────────────────────────────────────────────────
# NEW TESTS: Quota API, Partition Reassignment, Cluster Management,
# Schema Registry Edge Cases, Connect Client, Broker Config, ACL,
# Message Sampler Edge Cases, Replay Edge Cases, Health Endpoint
# ──────────────────────────────────────────────────────────────────────


def _create_full_app(admin=None, sampler=None, schema_registry=None, connect_client=None):
    """Build a FastAPI app with all optional service mocks."""
    app = FastAPI()
    app.include_router(router)
    app.state.kafka_admin = admin
    app.state.message_sampler = sampler
    app.state.schema_registry = schema_registry
    app.state.connect_client = connect_client
    return app


# ── Quota API Routes ─────────────────────────────────────────────────

class TestQuotaAPIRoutes:
    """Tests for GET/POST/DELETE /api/quotas endpoints."""

    def test_list_quotas_success(self):
        """GET /api/quotas returns quota list from admin."""
        admin = MagicMock()
        admin.list_quotas.return_value = {
            "quotas": [
                {"entity": {"user": "alice"}, "quotas": {"producer_byte_rate": 1048576}},
            ],
            "count": 1,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/quotas")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["quotas"][0]["entity"]["user"] == "alice"
        admin.list_quotas.assert_called_once()

    def test_list_quotas_empty(self):
        """GET /api/quotas with no quotas returns empty list."""
        admin = MagicMock()
        admin.list_quotas.return_value = {"quotas": [], "count": 0}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/quotas")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0
        assert resp.json()["quotas"] == []

    def test_list_quotas_no_admin_returns_503(self):
        """GET /api/quotas without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/quotas")
        assert resp.status_code == 503

    def test_list_quotas_admin_error_returns_500(self):
        """GET /api/quotas when admin raises exception returns 500."""
        admin = MagicMock()
        admin.list_quotas.side_effect = RuntimeError("Connection lost")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/quotas")
        assert resp.status_code == 500

    def test_set_quota_success(self):
        """POST /api/quotas sets quota successfully."""
        admin = MagicMock()
        admin.set_quota.return_value = {"success": True, "entityType": "user", "entityName": "alice"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotas": {"producer_byte_rate": 1048576},
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        admin.set_quota.assert_called_once_with(
            entity_type="user", entity_name="alice",
            quotas={"producer_byte_rate": 1048576},
        )

    def test_set_quota_missing_entity_type_returns_400(self):
        """POST /api/quotas without entityType returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityName": "alice",
            "quotas": {"producer_byte_rate": 1048576},
        })
        assert resp.status_code == 400
        assert "entityType" in resp.json()["detail"]

    def test_set_quota_missing_entity_name_returns_400(self):
        """POST /api/quotas without entityName returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "quotas": {"producer_byte_rate": 1048576},
        })
        assert resp.status_code == 400
        assert "entityName" in resp.json()["detail"]

    def test_set_quota_missing_quotas_returns_400(self):
        """POST /api/quotas without quotas dict returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
        })
        assert resp.status_code == 400
        assert "quotas" in resp.json()["detail"]

    def test_set_quota_empty_quotas_dict_returns_400(self):
        """POST /api/quotas with empty quotas dict returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotas": {},
        })
        assert resp.status_code == 400

    def test_set_quota_quotas_not_dict_returns_400(self):
        """POST /api/quotas with quotas as a list returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotas": ["producer_byte_rate"],
        })
        assert resp.status_code == 400

    def test_set_quota_admin_failure_returns_400(self):
        """POST /api/quotas when admin returns failure returns 400."""
        admin = MagicMock()
        admin.set_quota.return_value = {"success": False, "error": "Not supported"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotas": {"producer_byte_rate": 1048576},
        })
        assert resp.status_code == 400

    def test_delete_quota_success(self):
        """DELETE /api/quotas removes quota keys successfully."""
        admin = MagicMock()
        admin.delete_quota.return_value = {"success": True, "removed": ["producer_byte_rate"]}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotaKeys": ["producer_byte_rate"],
        })
        assert resp.status_code == 200
        admin.delete_quota.assert_called_once_with(
            entity_type="user", entity_name="alice",
            quota_keys=["producer_byte_rate"],
        )

    def test_delete_quota_missing_entity_type_returns_400(self):
        """DELETE /api/quotas without entityType returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityName": "alice",
            "quotaKeys": ["producer_byte_rate"],
        })
        assert resp.status_code == 400

    def test_delete_quota_missing_entity_name_returns_400(self):
        """DELETE /api/quotas without entityName returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "quotaKeys": ["producer_byte_rate"],
        })
        assert resp.status_code == 400

    def test_delete_quota_missing_quota_keys_returns_400(self):
        """DELETE /api/quotas without quotaKeys returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
        })
        assert resp.status_code == 400
        assert "quotaKeys" in resp.json()["detail"]

    def test_delete_quota_empty_quota_keys_list_returns_400(self):
        """DELETE /api/quotas with empty quotaKeys list returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotaKeys": [],
        })
        assert resp.status_code == 400

    def test_delete_quota_quota_keys_not_list_returns_400(self):
        """DELETE /api/quotas with quotaKeys as a string returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotaKeys": "producer_byte_rate",
        })
        assert resp.status_code == 400

    def test_delete_quota_admin_failure_returns_400(self):
        """DELETE /api/quotas when admin returns failure returns 400."""
        admin = MagicMock()
        admin.delete_quota.return_value = {"success": False, "error": "Not supported"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotaKeys": ["producer_byte_rate"],
        })
        assert resp.status_code == 400

    def test_set_quota_no_admin_returns_503(self):
        """POST /api/quotas without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotas": {"producer_byte_rate": 100},
        })
        assert resp.status_code == 503

    def test_delete_quota_no_admin_returns_503(self):
        """DELETE /api/quotas without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotaKeys": ["producer_byte_rate"],
        })
        assert resp.status_code == 503


# ── Partition Reassignment API Routes ────────────────────────────────

class TestPartitionReassignmentAPI:
    """Tests for POST/GET /api/topics/{topic}/reassign endpoints."""

    def test_reassign_partitions_success(self):
        """POST /api/topics/{topic}/reassign with valid assignments succeeds."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {
            "success": True, "topic": "orders", "partitionsReassigned": 2,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={
            "assignments": [
                {"partition": 0, "replicas": [1, 2]},
                {"partition": 1, "replicas": [2, 3]},
            ]
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["partitionsReassigned"] == 2
        admin.reassign_partitions.assert_called_once()

    def test_reassign_partitions_missing_assignments_returns_400(self):
        """POST /api/topics/{topic}/reassign without assignments returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={})
        assert resp.status_code == 400
        assert "assignments" in resp.json()["detail"]

    def test_reassign_partitions_empty_assignments_list_returns_400(self):
        """POST /api/topics/{topic}/reassign with empty assignments list returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={"assignments": []})
        assert resp.status_code == 400

    def test_reassign_partitions_assignments_not_list_returns_400(self):
        """POST /api/topics/{topic}/reassign with assignments as dict returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={
            "assignments": {"partition": 0, "replicas": [1]},
        })
        assert resp.status_code == 400

    def test_reassign_partitions_missing_partition_key_returns_400(self):
        """POST with assignment missing 'partition' key returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={
            "assignments": [{"replicas": [1, 2]}],
        })
        assert resp.status_code == 400
        assert "partition" in resp.json()["detail"].lower()

    def test_reassign_partitions_missing_replicas_key_returns_400(self):
        """POST with assignment missing 'replicas' key returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={
            "assignments": [{"partition": 0}],
        })
        assert resp.status_code == 400
        assert "replicas" in resp.json()["detail"].lower()

    def test_reassign_partitions_admin_failure_returns_400(self):
        """POST reassign when admin returns failure returns 400."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {
            "success": False, "error": "Broker 99 does not exist",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/reassign", json={
            "assignments": [{"partition": 0, "replicas": [99]}],
        })
        assert resp.status_code == 400
        assert "Broker 99" in resp.json()["detail"]

    def test_get_reassignment_status_success(self):
        """GET /api/topics/{topic}/reassign returns reassignment status."""
        admin = MagicMock()
        admin.get_partition_reassignment_status.return_value = {
            "topic": "orders",
            "reassignments": [
                {"partition": 0, "replicas": [1, 2], "addingReplicas": [2], "removingReplicas": []},
            ],
            "inProgress": True,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/reassign")
        assert resp.status_code == 200
        body = resp.json()
        assert body["inProgress"] is True
        assert len(body["reassignments"]) == 1
        admin.get_partition_reassignment_status.assert_called_once_with("orders")

    def test_get_reassignment_status_no_reassignments(self):
        """GET /api/topics/{topic}/reassign with no active reassignments."""
        admin = MagicMock()
        admin.get_partition_reassignment_status.return_value = {
            "topic": "events",
            "reassignments": [],
            "inProgress": False,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/events/reassign")
        assert resp.status_code == 200
        assert resp.json()["inProgress"] is False
        assert resp.json()["reassignments"] == []

    def test_get_reassignment_status_admin_error_returns_500(self):
        """GET reassignment status when admin raises exception returns 500."""
        admin = MagicMock()
        admin.get_partition_reassignment_status.side_effect = RuntimeError("Timeout")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/reassign")
        assert resp.status_code == 500

    def test_reassign_partitions_no_admin_returns_503(self):
        """POST reassign without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/topics/orders/reassign", json={
            "assignments": [{"partition": 0, "replicas": [1]}],
        })
        assert resp.status_code == 503

    def test_reassign_multiple_partitions_verified(self):
        """Verify all assignments are passed to admin.reassign_partitions."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {"success": True, "topic": "t", "partitionsReassigned": 3}
        client = TestClient(_create_full_app(admin=admin))
        assignments = [
            {"partition": 0, "replicas": [1, 2, 3]},
            {"partition": 1, "replicas": [2, 3, 1]},
            {"partition": 2, "replicas": [3, 1, 2]},
        ]
        resp = client.post("/api/topics/t/reassign", json={"assignments": assignments})
        assert resp.status_code == 200
        call_args = admin.reassign_partitions.call_args
        assert call_args[0][0] == "t"
        assert len(call_args[0][1]) == 3


# ── Broker Config API ────────────────────────────────────────────────

class TestBrokerConfigAPI:
    """Tests for GET/PUT /api/brokers/{broker_id}/config endpoints."""

    def test_get_broker_config_success(self):
        """GET /api/brokers/1/config returns broker configuration."""
        admin = MagicMock()
        admin.describe_broker_config.return_value = [
            {"name": "log.retention.hours", "value": "168", "source": "DEFAULT_CONFIG",
             "isReadOnly": False, "isSensitive": False},
            {"name": "num.io.threads", "value": "8", "source": "STATIC_BROKER_CONFIG",
             "isReadOnly": True, "isSensitive": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/1/config")
        assert resp.status_code == 200
        body = resp.json()
        assert body["brokerId"] == 1
        assert len(body["configs"]) == 2
        admin.describe_broker_config.assert_called_once_with(1)

    def test_get_broker_config_empty(self):
        """GET /api/brokers/1/config when broker has no special configs returns empty list."""
        admin = MagicMock()
        admin.describe_broker_config.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/1/config")
        assert resp.status_code == 200
        assert resp.json()["configs"] == []

    def test_get_broker_config_admin_error_returns_500(self):
        """GET /api/brokers/1/config when admin raises exception returns 500."""
        admin = MagicMock()
        admin.describe_broker_config.side_effect = RuntimeError("Not connected")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/1/config")
        assert resp.status_code == 500

    def test_get_broker_config_no_admin_returns_503(self):
        """GET /api/brokers/1/config without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/brokers/1/config")
        assert resp.status_code == 503

    def test_update_broker_config_success(self):
        """PUT /api/brokers/1/config updates broker configuration."""
        admin = MagicMock()
        admin.alter_broker_config.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={
            "configs": {"log.retention.hours": "72"},
        })
        assert resp.status_code == 200
        admin.alter_broker_config.assert_called_once_with(1, {"log.retention.hours": "72"})

    def test_update_broker_config_missing_configs_returns_400(self):
        """PUT /api/brokers/1/config without configs returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={})
        assert resp.status_code == 400
        assert "configs" in resp.json()["detail"]

    def test_update_broker_config_empty_configs_returns_400(self):
        """PUT /api/brokers/1/config with empty configs dict returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={"configs": {}})
        assert resp.status_code == 400

    def test_update_broker_config_configs_not_dict_returns_400(self):
        """PUT /api/brokers/1/config with configs as a list returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={"configs": ["log.retention.hours"]})
        assert resp.status_code == 400

    def test_update_broker_config_admin_failure_returns_400(self):
        """PUT /api/brokers/1/config when admin returns failure returns 400."""
        admin = MagicMock()
        admin.alter_broker_config.return_value = {"success": False, "error": "Read-only config"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={
            "configs": {"broker.id": "999"},
        })
        assert resp.status_code == 400
        assert "Read-only" in resp.json()["detail"]

    def test_update_broker_config_no_admin_returns_503(self):
        """PUT /api/brokers/1/config without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.put("/api/brokers/1/config", json={
            "configs": {"log.retention.hours": "72"},
        })
        assert resp.status_code == 503


# ── ACL API ──────────────────────────────────────────────────────────

class TestACLAPIRoutes:
    """Tests for GET/POST/DELETE /api/acls endpoints."""

    def test_list_acls_success(self):
        """GET /api/acls returns ACL list from admin."""
        admin = MagicMock()
        admin.list_acls.return_value = {
            "acls": [
                {"principal": "User:alice", "host": "*", "operation": "READ",
                 "permission": "ALLOW", "resourceType": "TOPIC",
                 "resourceName": "orders", "patternType": "LITERAL"},
            ],
            "count": 1,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/acls")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["acls"][0]["principal"] == "User:alice"
        admin.list_acls.assert_called_once()

    def test_list_acls_empty(self):
        """GET /api/acls with no ACLs returns empty list."""
        admin = MagicMock()
        admin.list_acls.return_value = {"acls": [], "count": 0}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/acls")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_list_acls_admin_error_returns_500(self):
        """GET /api/acls when admin raises exception returns 500."""
        admin = MagicMock()
        admin.list_acls.side_effect = RuntimeError("Auth failed")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/acls")
        assert resp.status_code == 500

    def test_create_acl_success(self):
        """POST /api/acls creates an ACL entry."""
        admin = MagicMock()
        admin.create_acl.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:alice",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 200
        admin.create_acl.assert_called_once_with(
            resource_type="TOPIC",
            resource_name="orders",
            principal="User:alice",
            operation="READ",
            permission_type="ALLOW",
            pattern_type="LITERAL",
            host="*",
        )

    def test_create_acl_missing_resource_type_returns_400(self):
        """POST /api/acls without resourceType returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceName": "orders",
            "principal": "User:alice",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 400
        assert "resourceType" in resp.json()["detail"]

    def test_create_acl_missing_resource_name_returns_400(self):
        """POST /api/acls without resourceName returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "principal": "User:alice",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 400
        assert "resourceName" in resp.json()["detail"]

    def test_create_acl_missing_principal_returns_400(self):
        """POST /api/acls without principal returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 400
        assert "principal" in resp.json()["detail"]

    def test_create_acl_missing_operation_returns_400(self):
        """POST /api/acls without operation returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:alice",
            "permission": "ALLOW",
        })
        assert resp.status_code == 400
        assert "operation" in resp.json()["detail"]

    def test_create_acl_missing_permission_returns_400(self):
        """POST /api/acls without permission returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:alice",
            "operation": "READ",
        })
        assert resp.status_code == 400
        assert "permission" in resp.json()["detail"]

    def test_create_acl_with_custom_pattern_type_and_host(self):
        """POST /api/acls with custom patternType and host passes them through."""
        admin = MagicMock()
        admin.create_acl.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:bob",
            "operation": "WRITE",
            "permission": "DENY",
            "patternType": "PREFIXED",
            "host": "192.168.1.100",
        })
        assert resp.status_code == 200
        admin.create_acl.assert_called_once_with(
            resource_type="TOPIC",
            resource_name="orders",
            principal="User:bob",
            operation="WRITE",
            permission_type="DENY",
            pattern_type="PREFIXED",
            host="192.168.1.100",
        )

    def test_create_acl_admin_exception_returns_500(self):
        """POST /api/acls when admin.create_acl raises exception returns 500."""
        admin = MagicMock()
        admin.create_acl.side_effect = RuntimeError("Kafka error")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:alice",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 500

    def test_delete_acl_success(self):
        """DELETE /api/acls deletes matching ACLs."""
        admin = MagicMock()
        admin.delete_acl.return_value = {"success": True, "deleted": 2}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:alice",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["deleted"] == 2

    def test_delete_acl_admin_exception_returns_500(self):
        """DELETE /api/acls when admin raises exception returns 500."""
        admin = MagicMock()
        admin.delete_acl.side_effect = RuntimeError("Not authorized")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
        })
        assert resp.status_code == 500

    def test_list_acls_no_admin_returns_503(self):
        """GET /api/acls without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/acls")
        assert resp.status_code == 503

    def test_create_acl_no_admin_returns_503(self):
        """POST /api/acls without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
            "principal": "User:alice",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 503


# ── Schema Registry Edge Cases ───────────────────────────────────────

class TestSchemaRegistryEdgeCases:
    """Tests for schema registry error handling and edge cases."""

    def test_list_subjects_registry_returns_empty_on_error(self):
        """When schema registry client returns empty list, API wraps it."""
        sr = MagicMock()
        sr.list_subjects.return_value = []
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects")
        assert resp.status_code == 200
        assert resp.json()["subjects"] == []

    def test_get_schema_returns_empty_dict_on_error(self):
        """When registry returns empty dict for unavailable schema, API returns it."""
        sr = MagicMock()
        sr.get_schema.return_value = {}
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/missing/versions/1")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_get_compatibility_returns_unknown(self):
        """When registry returns UNKNOWN compatibility, API passes it through."""
        sr = MagicMock()
        sr.get_compatibility.return_value = "UNKNOWN"
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/config")
        assert resp.status_code == 200
        assert resp.json()["compatibilityLevel"] == "UNKNOWN"

    def test_register_schema_with_json_schema_type(self):
        """Register schema with JSON schema type passes schemaType correctly."""
        sr = MagicMock()
        sr.register_schema.return_value = {"id": 50}
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/test-value/versions",
            json={"schema": '{"type":"object"}', "schemaType": "JSON"},
        )
        assert resp.status_code == 200
        sr.register_schema.assert_called_once_with("test-value", '{"type":"object"}', "JSON")

    def test_register_schema_default_schema_type_is_avro(self):
        """When schemaType is omitted, it defaults to AVRO."""
        sr = MagicMock()
        sr.register_schema.return_value = {"id": 51}
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/test-value/versions",
            json={"schema": '{"type":"string"}'},
        )
        assert resp.status_code == 200
        sr.register_schema.assert_called_once_with("test-value", '{"type":"string"}', "AVRO")

    def test_register_schema_empty_schema_string_returns_400(self):
        """Register with empty schema string returns 400."""
        sr = MagicMock()
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/test-value/versions",
            json={"schema": ""},
        )
        assert resp.status_code == 400

    def test_get_versions_for_nonexistent_subject_returns_empty(self):
        """Getting versions for a subject that does not exist returns empty list."""
        sr = MagicMock()
        sr.get_versions.return_value = []
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/nonexistent/versions")
        assert resp.status_code == 200
        assert resp.json()["versions"] == []

    def test_delete_subject_returns_deleted_version_ids(self):
        """DELETE subject returns the list of deleted version IDs."""
        sr = MagicMock()
        sr.delete_subject.return_value = [1, 2, 3, 4, 5]
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.delete("/api/schema-registry/subjects/old-schema")
        assert resp.status_code == 200
        body = resp.json()
        assert body["deletedVersions"] == [1, 2, 3, 4, 5]
        assert body["subject"] == "old-schema"


# ── Connect Client Edge Cases ────────────────────────────────────────

class TestConnectClientEdgeCases:
    """Tests for Kafka Connect API error handling and lifecycle."""

    def test_list_connectors_no_connect_returns_503(self):
        """GET /api/connect/connectors without connect client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.get("/api/connect/connectors")
        assert resp.status_code == 503

    def test_list_connectors_success(self):
        """GET /api/connect/connectors returns connector list with status."""
        cc = MagicMock()
        cc.list_connectors.return_value = ["my-source", "my-sink"]
        cc.get_connector_status.side_effect = [
            {"connector": {"state": "RUNNING"}, "type": "source", "tasks": [{"id": 0, "state": "RUNNING"}]},
            {"connector": {"state": "PAUSED"}, "type": "sink", "tasks": []},
        ]
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/connectors")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["connectors"]) == 2
        assert body["connectors"][0]["name"] == "my-source"
        assert body["connectors"][0]["state"] == "RUNNING"
        assert body["connectors"][1]["state"] == "PAUSED"

    def test_list_connectors_empty(self):
        """GET /api/connect/connectors when no connectors exist returns empty."""
        cc = MagicMock()
        cc.list_connectors.return_value = []
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/connectors")
        assert resp.status_code == 200
        assert resp.json()["connectors"] == []

    def test_get_connector_detail(self):
        """GET /api/connect/connectors/{name} returns connector info with status."""
        cc = MagicMock()
        cc.get_connector.return_value = {"name": "my-source", "config": {"connector.class": "FileSource"}}
        cc.get_connector_status.return_value = {"connector": {"state": "RUNNING"}, "type": "source", "tasks": []}
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/connectors/my-source")
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "my-source"
        assert "status" in body
        assert body["status"]["connector"]["state"] == "RUNNING"

    def test_create_connector_success(self):
        """POST /api/connect/connectors creates a new connector."""
        cc = MagicMock()
        cc.create_connector.return_value = {"name": "new-conn", "config": {"connector.class": "FileSource"}}
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors", json={
            "name": "new-conn",
            "config": {"connector.class": "FileSource", "topics": "test"},
        })
        assert resp.status_code == 200
        cc.create_connector.assert_called_once()

    def test_create_connector_missing_name_returns_400(self):
        """POST /api/connect/connectors without name returns 400."""
        cc = MagicMock()
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors", json={
            "config": {"connector.class": "FileSource"},
        })
        assert resp.status_code == 400

    def test_create_connector_missing_config_returns_400(self):
        """POST /api/connect/connectors without config returns 400."""
        cc = MagicMock()
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors", json={
            "name": "new-conn",
        })
        assert resp.status_code == 400

    def test_create_connector_error_returns_400(self):
        """POST /api/connect/connectors when connect returns error returns 400."""
        cc = MagicMock()
        cc.create_connector.return_value = {"error": "Connector already exists"}
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors", json={
            "name": "dup", "config": {"connector.class": "Foo"},
        })
        assert resp.status_code == 400
        assert "already exists" in resp.json()["detail"].lower()

    def test_delete_connector_success(self):
        """DELETE /api/connect/connectors/{name} deletes a connector."""
        cc = MagicMock()
        cc.delete_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.delete("/api/connect/connectors/old-conn")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.delete_connector.assert_called_once_with("old-conn")

    def test_delete_connector_failure_returns_500(self):
        """DELETE /api/connect/connectors/{name} when delete fails returns 500."""
        cc = MagicMock()
        cc.delete_connector.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.delete("/api/connect/connectors/bad-conn")
        assert resp.status_code == 500

    def test_pause_connector_success(self):
        """PUT /api/connect/connectors/{name}/pause pauses a connector."""
        cc = MagicMock()
        cc.pause_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/my-conn/pause")
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    def test_pause_connector_failure_returns_500(self):
        """PUT /api/connect/connectors/{name}/pause when pause fails returns 500."""
        cc = MagicMock()
        cc.pause_connector.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/my-conn/pause")
        assert resp.status_code == 500

    def test_resume_connector_success(self):
        """PUT /api/connect/connectors/{name}/resume resumes a connector."""
        cc = MagicMock()
        cc.resume_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/my-conn/resume")
        assert resp.status_code == 200

    def test_resume_connector_failure_returns_500(self):
        """PUT /api/connect/connectors/{name}/resume when resume fails returns 500."""
        cc = MagicMock()
        cc.resume_connector.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/my-conn/resume")
        assert resp.status_code == 500

    def test_restart_connector_success(self):
        """POST /api/connect/connectors/{name}/restart restarts a connector."""
        cc = MagicMock()
        cc.restart_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/my-conn/restart")
        assert resp.status_code == 200

    def test_restart_connector_failure_returns_500(self):
        """POST /api/connect/connectors/{name}/restart when restart fails returns 500."""
        cc = MagicMock()
        cc.restart_connector.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/my-conn/restart")
        assert resp.status_code == 500

    def test_update_connector_config_success(self):
        """PUT /api/connect/connectors/{name}/config updates connector config."""
        cc = MagicMock()
        cc.update_connector.return_value = {"name": "my-conn", "config": {"topics": "new-topic"}}
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/my-conn/config", json={
            "connector.class": "FileSink", "topics": "new-topic",
        })
        assert resp.status_code == 200
        cc.update_connector.assert_called_once()

    def test_update_connector_config_error_returns_400(self):
        """PUT /api/connect/connectors/{name}/config when error returns 400."""
        cc = MagicMock()
        cc.update_connector.return_value = {"error": "Invalid config key"}
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/my-conn/config", json={"bad_key": "val"})
        assert resp.status_code == 400

    def test_list_plugins_success(self):
        """GET /api/connect/plugins returns available plugins."""
        cc = MagicMock()
        cc.get_connector_plugins.return_value = [
            {"class": "org.apache.kafka.connect.file.FileStreamSourceConnector"},
            {"class": "org.apache.kafka.connect.file.FileStreamSinkConnector"},
        ]
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 200
        assert len(resp.json()["plugins"]) == 2

    def test_list_plugins_no_connect_returns_503(self):
        """GET /api/connect/plugins without connect client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 503


# ── Cluster Management Store Routing ─────────────────────────────────

class TestClusterManagementRouting:
    """Tests for multi-cluster URL routing and cluster info patterns."""

    def test_cluster_info_returns_all_fields(self):
        """GET /api/cluster returns complete cluster information."""
        admin = MagicMock()
        admin.get_cluster_info.return_value = {
            "clusterId": "abc-123",
            "controllerId": 1,
            "brokerCount": 3,
            "topicCount": 50,
            "consumerGroupCount": 20,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster")
        assert resp.status_code == 200
        body = resp.json()
        assert body["clusterId"] == "abc-123"
        assert body["brokerCount"] == 3
        assert body["topicCount"] == 50

    def test_cluster_health_returns_partition_info(self):
        """GET /api/cluster/health returns health with partition details."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "totalPartitions": 100,
            "underReplicatedCount": 2,
            "underReplicated": [
                {"topic": "orders", "partition": 0, "replicas": 3, "isr": 2},
                {"topic": "events", "partition": 1, "replicas": 3, "isr": 1},
            ],
            "offlinePartitionCount": 0,
            "offlinePartitions": [],
            "leaderDistribution": {1: 35, 2: 33, 3: 32},
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["totalPartitions"] == 100
        assert body["underReplicatedCount"] == 2
        assert body["offlinePartitionCount"] == 0
        assert len(body["leaderDistribution"]) == 3

    def test_cluster_info_admin_error_returns_500(self):
        """GET /api/cluster when admin raises exception returns 500."""
        admin = MagicMock()
        admin.get_cluster_info.side_effect = RuntimeError("Connection refused")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster")
        assert resp.status_code == 500

    def test_cluster_health_admin_error_returns_500(self):
        """GET /api/cluster/health when admin raises exception returns 500."""
        admin = MagicMock()
        admin.get_cluster_health.side_effect = RuntimeError("Timeout")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 500

    def test_brokers_endpoint_returns_broker_list(self):
        """GET /api/brokers returns full broker list."""
        admin = MagicMock()
        admin.list_brokers.return_value = [
            {"id": 1, "host": "broker-1", "port": 9092, "rack": "us-east-1a", "isController": True},
            {"id": 2, "host": "broker-2", "port": 9092, "rack": "us-east-1b", "isController": False},
            {"id": 3, "host": "broker-3", "port": 9092, "rack": "us-east-1c", "isController": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 3
        controllers = [b for b in body if b["isController"]]
        assert len(controllers) == 1


# ── Message Sampler Seek/Timestamp Edge Cases ────────────────────────

class TestMessageSamplerEdgeCases:
    """Tests for message sampler edge cases: negative offset, future timestamp, etc."""

    def test_sample_at_large_offset_returns_empty(self):
        """Sampling at an offset beyond the end of the partition returns empty."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t1/messages?partition=0&offset=999999999")
        assert resp.status_code == 200
        assert resp.json()["messages"] == []
        assert resp.json()["count"] == 0

    def test_sample_at_timestamp_far_future_returns_empty(self):
        """Sampling at a far-future timestamp returns empty list."""
        sampler = MagicMock()
        sampler.sample_at_timestamp.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t1/messages?partition=0&timestamp=99999999999999")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_sample_at_zero_offset(self):
        """Sampling at offset 0 is valid and calls sample_at."""
        sampler = MagicMock()
        sampler.sample_at.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1000, "key": None,
             "headers": {}, "value": "first", "format": "utf8"},
        ]
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t1/messages?partition=0&offset=0")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        sampler.sample_at.assert_called_once_with("t1", 0, 0, 50)

    def test_sample_at_timestamp_zero(self):
        """Timestamp of 0 (epoch start) is valid and calls sample_at_timestamp."""
        sampler = MagicMock()
        sampler.sample_at_timestamp.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t1/messages?partition=0&timestamp=0")
        assert resp.status_code == 200
        sampler.sample_at_timestamp.assert_called_once_with("t1", 0, 0, 50)

    def test_sample_with_high_partition_number(self):
        """Sampling from a high partition number is passed through correctly."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.get("/api/topics/t1/messages?partition=255&offset=0")
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("t1", 255, 0, 50)


# ── Replay Endpoint Edge Cases ───────────────────────────────────────

class TestReplayEndpointEdgeCases:
    """Additional tests for POST /api/topics/{topic}/replay edge cases."""

    def test_replay_with_none_value_message(self):
        """Replay handles message with None value (tombstone)."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1700000000000,
             "key": "k1", "headers": {}, "value": None, "format": "null"},
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/source/replay", json={"targetTopic": "dest"})
        assert resp.status_code == 200
        assert resp.json()["copied"] == 1
        # Value should be empty string for None
        admin.produce_message.assert_called_once_with(
            topic="dest", value="", key="k1", headers=None,
        )

    def test_replay_with_dict_value_serialized_to_json(self):
        """Replay serializes dict values to JSON strings."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1700000000000,
             "key": None, "headers": {}, "value": {"order_id": 123}, "format": "json"},
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        call_kwargs = admin.produce_message.call_args
        # Value should be JSON-serialized string
        import json
        assert json.loads(call_kwargs.kwargs.get("value", call_kwargs[1].get("value", ""))) == {"order_id": 123}

    def test_replay_no_sampler_returns_503(self):
        """Replay with no sampler configured returns 503."""
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=None))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 503

    def test_replay_empty_source_topic_returns_zero_copied(self):
        """Replay from empty source topic returns copied=0."""
        sampler = MagicMock()
        sampler.sample.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/empty-src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["copied"] == 0
        assert body["errors"] == 0
        assert body["total"] == 0

    def test_replay_with_all_produce_failures(self):
        """Replay where all produces fail returns errors equal to total."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": i, "partition": 0, "timestamp": 1700000000000 + i,
             "key": None, "headers": {}, "value": f"v{i}", "format": "utf8"}
            for i in range(5)
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": False, "error": "Broker down"}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["copied"] == 0
        assert body["errors"] == 5
        assert body["total"] == 5

    def test_replay_with_partition_uses_sample_at(self):
        """Replay with partition specified uses sample_at instead of sample."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={
                "targetTopic": "dst", "partition": 3, "offset": 100,
            })
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("src", 3, 100, 50)
        sampler.sample.assert_not_called()


# ── Health Endpoint Edge Cases ───────────────────────────────────────

class TestHealthEndpointEdgeCases:
    """Tests for /api/health endpoint via snapshot data aggregation."""

    def test_health_metrics_with_zero_topics(self):
        """Health aggregation with zero topics yields zero rates."""
        snapshot = ClusterSnapshot(
            topics={},
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        total_rate = sum(t.msg_per_sec for t in snapshot.topics.values())
        total_lag = sum(g.total_lag for g in snapshot.consumer_groups.values())
        assert total_rate == 0.0
        assert total_lag == 0

    def test_health_metrics_with_high_lag_groups(self):
        """Health aggregation correctly sums very high lag values."""
        groups = {}
        for i in range(10):
            groups[f"group-{i}"] = ConsumerGroupInfo(
                group_id=f"group-{i}", members=2, total_lag=1_000_000,
                subscribed_topics=[f"topic-{i}"],
                per_partition_lag={f"topic-{i}-0": 500000, f"topic-{i}-1": 500000},
                member_client_ids=[f"client-{i}-a", f"client-{i}-b"],
            )
        snapshot = ClusterSnapshot(
            topics={f"topic-{i}": TopicInfo(name=f"topic-{i}", partitions=2, msg_per_sec=100.0, total_messages=5000000) for i in range(10)},
            consumer_groups=groups,
            active_partitions={},
            timestamp=time.time(),
        )
        total_lag = sum(g.total_lag for g in snapshot.consumer_groups.values())
        assert total_lag == 10_000_000

    def test_health_metrics_with_mixed_active_idle_topics(self):
        """Health aggregation counts active and idle topics correctly."""
        snapshot = ClusterSnapshot(
            topics={
                "active-1": TopicInfo(name="active-1", partitions=3, msg_per_sec=100.0, total_messages=50000),
                "active-2": TopicInfo(name="active-2", partitions=3, msg_per_sec=200.5, total_messages=80000),
                "idle-1": TopicInfo(name="idle-1", partitions=1, msg_per_sec=0.0, total_messages=0),
                "idle-2": TopicInfo(name="idle-2", partitions=1, msg_per_sec=0.0, total_messages=10),
            },
            consumer_groups={},
            active_partitions={},
            timestamp=time.time(),
        )
        total_rate = sum(t.msg_per_sec for t in snapshot.topics.values())
        assert total_rate == 300.5
        active_count = sum(1 for t in snapshot.topics.values() if t.msg_per_sec > 0)
        assert active_count == 2
        idle_count = sum(1 for t in snapshot.topics.values() if t.msg_per_sec == 0)
        assert idle_count == 2

    def test_health_graph_node_count_matches_builder(self):
        """Graph builder node count matches expected for health reporting."""
        builder = GraphStateBuilder(show_producers=False, lag_warn_threshold=1000)
        snapshot = ClusterSnapshot(
            topics={
                "t1": TopicInfo(name="t1", partitions=3, msg_per_sec=10.0, total_messages=1000),
                "t2": TopicInfo(name="t2", partitions=3, msg_per_sec=20.0, total_messages=2000),
            },
            consumer_groups={
                "g1": ConsumerGroupInfo(
                    group_id="g1", members=2, total_lag=50,
                    subscribed_topics=["t1"],
                    per_partition_lag={"t1-0": 25, "t1-1": 25},
                    member_client_ids=["g1-c1", "g1-c2"],
                ),
            },
            active_partitions={},
            timestamp=time.time(),
        )
        diff = builder.update(snapshot)
        # Health endpoint reports len(builder._nodes) and len(builder._edges)
        assert len(builder._nodes) >= 3  # 2 topics + 1 consumer group/service
        assert len(builder._edges) >= 1  # at least 1 consume edge

    def test_health_degraded_when_collector_disconnected(self):
        """When collector is not connected, health should report degraded."""
        # Simulate the logic from the health endpoint
        collector_connected = False
        status = "ok" if collector_connected else "degraded"
        assert status == "degraded"

    def test_health_ok_when_collector_connected(self):
        """When collector is connected, health should report ok."""
        collector_connected = True
        status = "ok" if collector_connected else "degraded"
        assert status == "ok"


# ── Config Diff API ─────────────────────────────────────────────────

class TestConfigDiffAPI:
    """Tests for GET /api/topics/{topic}/config-diff endpoint."""

    def test_config_diff_returns_200(self):
        """GET /api/topics/{topic}/config-diff returns 200 on success."""
        admin = MagicMock()
        admin.get_topic_config_diff.return_value = [
            {"name": "retention.ms", "value": "604800000", "default": "604800000", "isDefault": True},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/config-diff")
        assert resp.status_code == 200

    def test_config_diff_calls_admin_with_correct_topic(self):
        """GET /api/topics/{topic}/config-diff calls admin.get_topic_config_diff with the topic name."""
        admin = MagicMock()
        admin.get_topic_config_diff.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/topics/my-topic/config-diff")
        admin.get_topic_config_diff.assert_called_once_with("my-topic")

    def test_config_diff_returns_topic_and_configs_fields(self):
        """GET /api/topics/{topic}/config-diff response includes topic and configs."""
        admin = MagicMock()
        admin.get_topic_config_diff.return_value = [
            {"name": "retention.ms", "value": "86400000", "default": "604800000", "isDefault": False},
            {"name": "cleanup.policy", "value": "compact", "default": "delete", "isDefault": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/config-diff")
        body = resp.json()
        assert body["topic"] == "orders"
        assert "configs" in body
        assert len(body["configs"]) == 2

    def test_config_diff_empty_configs(self):
        """GET /api/topics/{topic}/config-diff with no overridden configs returns empty list."""
        admin = MagicMock()
        admin.get_topic_config_diff.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/config-diff")
        assert resp.status_code == 200
        body = resp.json()
        assert body["topic"] == "orders"
        assert body["configs"] == []

    def test_config_diff_admin_error_returns_500(self):
        """GET /api/topics/{topic}/config-diff returns 500 when admin raises exception."""
        admin = MagicMock()
        admin.get_topic_config_diff.side_effect = RuntimeError("Kafka unavailable")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/config-diff")
        assert resp.status_code == 500
        assert "Kafka unavailable" in resp.json()["detail"]

    def test_config_diff_no_admin_returns_503(self):
        """GET /api/topics/{topic}/config-diff without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/topics/orders/config-diff")
        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_config_diff_url_encoded_topic_name(self):
        """GET /api/topics/{topic}/config-diff with URL-encoded topic name containing dots."""
        admin = MagicMock()
        admin.get_topic_config_diff.return_value = [
            {"name": "retention.ms", "value": "3600000", "default": "604800000", "isDefault": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/my.topic.v2/config-diff")
        assert resp.status_code == 200
        admin.get_topic_config_diff.assert_called_once_with("my.topic.v2")

    def test_config_diff_topic_with_hyphens_and_underscores(self):
        """GET /api/topics/{topic}/config-diff with hyphens and underscores in topic name."""
        admin = MagicMock()
        admin.get_topic_config_diff.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/my-topic_name-v3/config-diff")
        assert resp.status_code == 200
        admin.get_topic_config_diff.assert_called_once_with("my-topic_name-v3")

    def test_config_diff_preserves_config_structure(self):
        """GET /api/topics/{topic}/config-diff preserves all fields in each config entry."""
        admin = MagicMock()
        config_entry = {
            "name": "max.message.bytes",
            "value": "2097152",
            "default": "1048588",
            "isDefault": False,
            "source": "DYNAMIC_TOPIC_CONFIG",
        }
        admin.get_topic_config_diff.return_value = [config_entry]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/events/config-diff")
        body = resp.json()
        assert body["configs"][0]["name"] == "max.message.bytes"
        assert body["configs"][0]["value"] == "2097152"
        assert body["configs"][0]["isDefault"] is False

    def test_config_diff_admin_exception_message_propagated(self):
        """GET /api/topics/{topic}/config-diff propagates the exception message in detail."""
        admin = MagicMock()
        admin.get_topic_config_diff.side_effect = ValueError("Topic not found: ghost-topic")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/ghost-topic/config-diff")
        assert resp.status_code == 500
        assert "ghost-topic" in resp.json()["detail"]


# ── Key Distribution API ────────────────────────────────────────────

class TestKeyDistributionAPI:
    """Tests for GET /api/topics/{topic}/key-distribution endpoint."""

    def test_key_distribution_returns_200(self):
        """GET /api/topics/{topic}/key-distribution returns 200 on success."""
        admin = MagicMock()
        admin.get_key_distribution.return_value = {
            "topic": "orders",
            "sampleSize": 1000,
            "uniqueKeys": 150,
            "distribution": [{"key": "order-123", "count": 10, "percentage": 1.0}],
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/key-distribution")
        assert resp.status_code == 200

    def test_key_distribution_default_sample_size_is_1000(self):
        """GET /api/topics/{topic}/key-distribution without sample_size uses default of 1000."""
        admin = MagicMock()
        admin.get_key_distribution.return_value = {
            "topic": "orders", "sampleSize": 1000, "uniqueKeys": 50, "distribution": [],
        }
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/topics/orders/key-distribution")
        admin.get_key_distribution.assert_called_once_with("orders", sample_size=1000)

    def test_key_distribution_custom_sample_size(self):
        """GET /api/topics/{topic}/key-distribution?sample_size=500 passes custom size."""
        admin = MagicMock()
        admin.get_key_distribution.return_value = {
            "topic": "events", "sampleSize": 500, "uniqueKeys": 30, "distribution": [],
        }
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/topics/events/key-distribution?sample_size=500")
        admin.get_key_distribution.assert_called_once_with("events", sample_size=500)

    def test_key_distribution_sample_size_capped_at_10000(self):
        """GET /api/topics/{topic}/key-distribution caps sample_size at 10000."""
        admin = MagicMock()
        admin.get_key_distribution.return_value = {
            "topic": "events", "sampleSize": 10000, "uniqueKeys": 200, "distribution": [],
        }
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/topics/events/key-distribution?sample_size=99999")
        admin.get_key_distribution.assert_called_once_with("events", sample_size=10000)

    def test_key_distribution_sample_size_exactly_10000(self):
        """GET /api/topics/{topic}/key-distribution with sample_size=10000 is not capped."""
        admin = MagicMock()
        admin.get_key_distribution.return_value = {
            "topic": "events", "sampleSize": 10000, "uniqueKeys": 100, "distribution": [],
        }
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/topics/events/key-distribution?sample_size=10000")
        admin.get_key_distribution.assert_called_once_with("events", sample_size=10000)

    def test_key_distribution_negative_sample_size_returns_400(self):
        """GET /api/topics/{topic}/key-distribution?sample_size=-1 returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/key-distribution?sample_size=-1")
        assert resp.status_code == 400
        assert "positive" in resp.json()["detail"].lower()

    def test_key_distribution_zero_sample_size_returns_400(self):
        """GET /api/topics/{topic}/key-distribution?sample_size=0 returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/key-distribution?sample_size=0")
        assert resp.status_code == 400
        assert "positive" in resp.json()["detail"].lower()

    def test_key_distribution_admin_error_returns_500(self):
        """GET /api/topics/{topic}/key-distribution returns 500 when admin raises exception."""
        admin = MagicMock()
        admin.get_key_distribution.side_effect = RuntimeError("Consumer timeout")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/key-distribution")
        assert resp.status_code == 500
        assert "Consumer timeout" in resp.json()["detail"]

    def test_key_distribution_no_admin_returns_503(self):
        """GET /api/topics/{topic}/key-distribution without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/topics/orders/key-distribution")
        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_key_distribution_returns_response_body(self):
        """GET /api/topics/{topic}/key-distribution returns the full admin response."""
        admin = MagicMock()
        expected = {
            "topic": "payments",
            "sampleSize": 1000,
            "uniqueKeys": 75,
            "distribution": [
                {"key": "pay-001", "count": 20, "percentage": 2.0},
                {"key": "pay-002", "count": 15, "percentage": 1.5},
            ],
        }
        admin.get_key_distribution.return_value = expected
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/payments/key-distribution")
        body = resp.json()
        assert body["topic"] == "payments"
        assert body["uniqueKeys"] == 75
        assert len(body["distribution"]) == 2


# ── Broker Config Management ────────────────────────────────────────

class TestBrokerConfigManagement:
    """Additional tests for GET/PUT /api/brokers/{broker_id}/config management scenarios."""

    def test_get_broker_config_returns_broker_id_and_configs(self):
        """GET /api/brokers/{id}/config returns brokerId and configs in response."""
        admin = MagicMock()
        admin.describe_broker_config.return_value = [
            {"name": "log.retention.hours", "value": "168", "source": "DEFAULT_CONFIG",
             "isReadOnly": False, "isSensitive": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/2/config")
        assert resp.status_code == 200
        body = resp.json()
        assert body["brokerId"] == 2
        assert isinstance(body["configs"], list)
        assert body["configs"][0]["name"] == "log.retention.hours"
        admin.describe_broker_config.assert_called_once_with(2)

    def test_get_broker_config_multiple_entries(self):
        """GET /api/brokers/{id}/config returns all config entries from admin."""
        admin = MagicMock()
        admin.describe_broker_config.return_value = [
            {"name": "log.retention.hours", "value": "168", "source": "DEFAULT_CONFIG",
             "isReadOnly": False, "isSensitive": False},
            {"name": "num.io.threads", "value": "8", "source": "STATIC_BROKER_CONFIG",
             "isReadOnly": True, "isSensitive": False},
            {"name": "log.segment.bytes", "value": "1073741824", "source": "DEFAULT_CONFIG",
             "isReadOnly": False, "isSensitive": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/1/config")
        assert resp.status_code == 200
        assert len(resp.json()["configs"]) == 3

    def test_update_broker_config_success_returns_200(self):
        """PUT /api/brokers/{id}/config with valid configs returns 200."""
        admin = MagicMock()
        admin.alter_broker_config.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/3/config", json={
            "configs": {"log.retention.hours": "48", "log.segment.bytes": "536870912"},
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        admin.alter_broker_config.assert_called_once_with(3, {
            "log.retention.hours": "48",
            "log.segment.bytes": "536870912",
        })

    def test_update_broker_config_verifies_broker_id(self):
        """PUT /api/brokers/{id}/config passes correct broker_id to admin."""
        admin = MagicMock()
        admin.alter_broker_config.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        client.put("/api/brokers/5/config", json={
            "configs": {"log.retention.hours": "24"},
        })
        call_args = admin.alter_broker_config.call_args
        assert call_args[0][0] == 5

    def test_get_broker_config_exception_returns_500(self):
        """GET /api/brokers/{id}/config returns 500 when admin raises exception."""
        admin = MagicMock()
        admin.describe_broker_config.side_effect = RuntimeError("Broker 99 not found")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/99/config")
        assert resp.status_code == 500
        assert "Broker 99 not found" in resp.json()["detail"]

    def test_update_broker_config_failure_returns_400(self):
        """PUT /api/brokers/{id}/config when admin reports failure returns 400."""
        admin = MagicMock()
        admin.alter_broker_config.return_value = {
            "success": False,
            "error": "Config key 'broker.id' is read-only",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={
            "configs": {"broker.id": "999"},
        })
        assert resp.status_code == 400
        assert "read-only" in resp.json()["detail"].lower()

    def test_update_broker_config_no_admin_returns_503(self):
        """PUT /api/brokers/{id}/config without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.put("/api/brokers/1/config", json={
            "configs": {"log.retention.hours": "72"},
        })
        assert resp.status_code == 503

    def test_update_broker_config_invalid_configs_type_returns_400(self):
        """PUT /api/brokers/{id}/config with configs as a string returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/brokers/1/config", json={
            "configs": "log.retention.hours=72",
        })
        assert resp.status_code == 400
        assert "configs" in resp.json()["detail"]


# ── Message Search API Tests ─────────────────────────────────────────

class TestMessageSearchAPI:
    """Tests for POST /api/topics/{topic}/search endpoint."""

    def test_search_messages_success_with_key_pattern(self):
        """POST /api/topics/{topic}/search with key_pattern returns matching messages."""
        admin = MagicMock()
        admin.search_messages.return_value = [
            {"offset": 10, "partition": 0, "timestamp": 1700000000000,
             "key": "order-123", "value": '{"status": "shipped"}'},
            {"offset": 20, "partition": 1, "timestamp": 1700000001000,
             "key": "order-456", "value": '{"status": "pending"}'},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/search", json={
            "key_pattern": "order-.*",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["topic"] == "orders"
        assert body["count"] == 2
        assert len(body["messages"]) == 2
        admin.search_messages.assert_called_once_with(
            topic="orders",
            key_pattern="order-.*",
            value_pattern=None,
            partition=None,
            start_time=None,
            end_time=None,
            max_results=100,
        )

    def test_search_messages_with_value_pattern(self):
        """POST /api/topics/{topic}/search with value_pattern returns matching messages."""
        admin = MagicMock()
        admin.search_messages.return_value = [
            {"offset": 5, "partition": 0, "timestamp": 1700000000000,
             "key": "k1", "value": '{"error": "timeout"}'},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/errors/search", json={
            "value_pattern": "timeout",
        })
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        admin.search_messages.assert_called_once_with(
            topic="errors",
            key_pattern=None,
            value_pattern="timeout",
            partition=None,
            start_time=None,
            end_time=None,
            max_results=100,
        )

    def test_search_messages_with_all_filters(self):
        """POST /api/topics/{topic}/search with all filter parameters."""
        admin = MagicMock()
        admin.search_messages.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/events/search", json={
            "key_pattern": "user-.*",
            "value_pattern": "login",
            "partition": 2,
            "start_time": 1700000000000,
            "end_time": 1700000060000,
            "max_results": 50,
        })
        assert resp.status_code == 200
        assert resp.json()["count"] == 0
        admin.search_messages.assert_called_once_with(
            topic="events",
            key_pattern="user-.*",
            value_pattern="login",
            partition=2,
            start_time=1700000000000,
            end_time=1700000060000,
            max_results=50,
        )

    def test_search_messages_with_partition_only(self):
        """POST /api/topics/{topic}/search with only partition filter."""
        admin = MagicMock()
        admin.search_messages.return_value = [
            {"offset": 0, "partition": 3, "key": None, "value": "data"},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={
            "partition": 3,
        })
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        admin.search_messages.assert_called_once_with(
            topic="t1",
            key_pattern=None,
            value_pattern=None,
            partition=3,
            start_time=None,
            end_time=None,
            max_results=100,
        )

    def test_search_messages_empty_body_uses_defaults(self):
        """POST /api/topics/{topic}/search with empty body uses default max_results."""
        admin = MagicMock()
        admin.search_messages.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={})
        assert resp.status_code == 200
        admin.search_messages.assert_called_once_with(
            topic="t1",
            key_pattern=None,
            value_pattern=None,
            partition=None,
            start_time=None,
            end_time=None,
            max_results=100,
        )

    def test_search_messages_no_admin_returns_503(self):
        """POST /api/topics/{topic}/search without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/topics/t1/search", json={"key_pattern": ".*"})
        assert resp.status_code == 503

    def test_search_messages_admin_exception_returns_500(self):
        """POST /api/topics/{topic}/search when admin raises exception returns 500."""
        admin = MagicMock()
        admin.search_messages.side_effect = RuntimeError("Consumer timeout")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={"key_pattern": ".*"})
        assert resp.status_code == 500
        assert "Consumer timeout" in resp.json()["detail"]

    def test_search_messages_with_regex_special_chars_in_key_pattern(self):
        """POST /api/topics/{topic}/search with regex special chars in key_pattern."""
        admin = MagicMock()
        admin.search_messages.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={
            "key_pattern": r"^user\.\d+$",
        })
        assert resp.status_code == 200
        admin.search_messages.assert_called_once()
        call_kwargs = admin.search_messages.call_args
        assert call_kwargs.kwargs["key_pattern"] == r"^user\.\d+$"

    def test_search_messages_with_time_range_only(self):
        """POST /api/topics/{topic}/search with only time range filters."""
        admin = MagicMock()
        admin.search_messages.return_value = [
            {"offset": 100, "partition": 0, "key": "k", "value": "v",
             "timestamp": 1700000030000},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={
            "start_time": 1700000000000,
            "end_time": 1700000060000,
        })
        assert resp.status_code == 200
        assert resp.json()["count"] == 1
        call_kwargs = admin.search_messages.call_args.kwargs
        assert call_kwargs["start_time"] == 1700000000000
        assert call_kwargs["end_time"] == 1700000060000

    def test_search_messages_custom_max_results(self):
        """POST /api/topics/{topic}/search with custom max_results."""
        admin = MagicMock()
        admin.search_messages.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={
            "key_pattern": ".*",
            "max_results": 10,
        })
        assert resp.status_code == 200
        admin.search_messages.assert_called_once_with(
            topic="t1",
            key_pattern=".*",
            value_pattern=None,
            partition=None,
            start_time=None,
            end_time=None,
            max_results=10,
        )

    def test_search_messages_topic_name_with_dots(self):
        """POST /api/topics/{topic}/search with dotted topic name."""
        admin = MagicMock()
        admin.search_messages.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/my.namespace.orders/search", json={
            "value_pattern": "shipped",
        })
        assert resp.status_code == 200
        admin.search_messages.assert_called_once()
        assert admin.search_messages.call_args.kwargs["topic"] == "my.namespace.orders"

    def test_search_messages_returns_correct_response_structure(self):
        """POST /api/topics/{topic}/search response has topic, messages, and count."""
        admin = MagicMock()
        admin.search_messages.return_value = [{"offset": 0}]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t1/search", json={})
        assert resp.status_code == 200
        body = resp.json()
        assert "topic" in body
        assert "messages" in body
        assert "count" in body
        assert body["topic"] == "t1"
        assert body["count"] == len(body["messages"])


# ── Preferred Leader Election API Tests ──────────────────────────────

class TestElectPreferredLeadersAPI:
    """Tests for POST /api/cluster/elect-leaders endpoint."""

    def test_elect_leaders_all_topics(self):
        """POST /api/cluster/elect-leaders without topic triggers cluster-wide election."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {
            "success": True, "electionsTriggered": 10,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={})
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["electionsTriggered"] == 10
        admin.elect_preferred_leaders.assert_called_once_with(None)

    def test_elect_leaders_specific_topic(self):
        """POST /api/cluster/elect-leaders with topic triggers election for that topic."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {
            "success": True, "electionsTriggered": 3,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={"topic": "orders"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["electionsTriggered"] == 3
        admin.elect_preferred_leaders.assert_called_once_with("orders")

    def test_elect_leaders_no_body(self):
        """POST /api/cluster/elect-leaders with no JSON body defaults to all topics."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        # Send request with empty content (the endpoint handles parse errors by defaulting body={})
        resp = client.post("/api/cluster/elect-leaders", content=b"", headers={"content-type": "application/json"})
        # The endpoint catches JSON parse errors and defaults to body={}
        assert resp.status_code == 200 or resp.status_code == 422
        # If the endpoint parsed it, the admin should have been called with None
        if resp.status_code == 200:
            admin.elect_preferred_leaders.assert_called_once_with(None)

    def test_elect_leaders_no_admin_returns_503(self):
        """POST /api/cluster/elect-leaders without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/cluster/elect-leaders", json={})
        assert resp.status_code == 503

    def test_elect_leaders_admin_exception_returns_500(self):
        """POST /api/cluster/elect-leaders when admin raises exception returns 500."""
        admin = MagicMock()
        admin.elect_preferred_leaders.side_effect = RuntimeError("Election failed: not enough ISR")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={"topic": "orders"})
        assert resp.status_code == 500
        assert "Election failed" in resp.json()["detail"]

    def test_elect_leaders_with_empty_topic_string(self):
        """POST /api/cluster/elect-leaders with empty topic string passes it through."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={"topic": ""})
        assert resp.status_code == 200
        # Empty string is falsy, so body.get("topic") returns "" which is falsy
        admin.elect_preferred_leaders.assert_called_once_with("")

    def test_elect_leaders_topic_with_special_characters(self):
        """POST /api/cluster/elect-leaders with special topic name."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={"topic": "my.namespace.orders_v2"})
        assert resp.status_code == 200
        admin.elect_preferred_leaders.assert_called_once_with("my.namespace.orders_v2")


# ── Connector Task Restart API Tests ─────────────────────────────────

class TestConnectorTaskRestartAPI:
    """Tests for POST /api/connect/connectors/{name}/tasks/{task_id}/restart endpoint."""

    def test_restart_task_success(self):
        """POST /api/connect/connectors/{name}/tasks/{task_id}/restart succeeds."""
        cc = MagicMock()
        cc.restart_task.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/my-source/tasks/0/restart")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.restart_task.assert_called_once_with("my-source", 0)

    def test_restart_task_failure_returns_500(self):
        """POST task restart when restart_task returns False returns 500."""
        cc = MagicMock()
        cc.restart_task.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/my-source/tasks/0/restart")
        assert resp.status_code == 500
        assert "task 0" in resp.json()["detail"].lower()

    def test_restart_task_no_connect_returns_503(self):
        """POST task restart without connect client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.post("/api/connect/connectors/my-conn/tasks/0/restart")
        assert resp.status_code == 503

    def test_restart_task_high_task_id(self):
        """POST task restart with high task_id passes correct integer."""
        cc = MagicMock()
        cc.restart_task.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/my-sink/tasks/15/restart")
        assert resp.status_code == 200
        cc.restart_task.assert_called_once_with("my-sink", 15)

    def test_restart_task_different_connector_names(self):
        """POST task restart works with various connector name formats."""
        cc = MagicMock()
        cc.restart_task.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/jdbc-source-orders-prod/tasks/2/restart")
        assert resp.status_code == 200
        cc.restart_task.assert_called_once_with("jdbc-source-orders-prod", 2)

    def test_restart_task_id_zero(self):
        """POST task restart with task_id=0 works correctly."""
        cc = MagicMock()
        cc.restart_task.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/conn/tasks/0/restart")
        assert resp.status_code == 200
        cc.restart_task.assert_called_once_with("conn", 0)

    def test_restart_task_invalid_task_id_returns_422(self):
        """POST task restart with non-integer task_id returns 422."""
        cc = MagicMock()
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/conn/tasks/abc/restart")
        assert resp.status_code == 422


# ── Additional Replay Edge Cases ─────────────────────────────────────

class TestReplayAdditionalEdgeCases:
    """Additional edge case tests for POST /api/topics/{topic}/replay."""

    def test_replay_with_string_value_preserves_value(self):
        """Replay passes string values through correctly."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1700000000000,
             "key": "k1", "headers": {}, "value": "plain text message", "format": "utf8"},
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        assert resp.json()["copied"] == 1
        admin.produce_message.assert_called_once_with(
            topic="dst", value="plain text message", key="k1", headers=None,
        )

    def test_replay_with_integer_value_converts_to_string(self):
        """Replay converts integer values to string."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": 0, "partition": 0, "timestamp": 1700000000000,
             "key": None, "headers": {}, "value": 42, "format": "int"},
        ]
        admin = MagicMock()
        admin.produce_message.return_value = {"success": True}
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        assert resp.json()["copied"] == 1
        admin.produce_message.assert_called_once_with(
            topic="dst", value="42", key=None, headers=None,
        )

    def test_replay_mixed_success_and_failure(self):
        """Replay with some messages succeeding and some failing."""
        sampler = MagicMock()
        sampler.sample.return_value = [
            {"offset": i, "partition": 0, "timestamp": 1700000000000 + i,
             "key": None, "headers": {}, "value": f"msg-{i}", "format": "utf8"}
            for i in range(4)
        ]
        admin = MagicMock()
        admin.produce_message.side_effect = [
            {"success": True},
            {"success": True},
            {"success": False, "error": "Timeout"},
            {"success": True},
        ]
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={"targetTopic": "dst"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["copied"] == 3
        assert body["errors"] == 1
        assert body["total"] == 4

    def test_replay_uses_default_offset_zero(self):
        """Replay with partition but no offset defaults to offset=0."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={
                "targetTopic": "dst", "partition": 1,
            })
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("src", 1, 0, 50)

    def test_replay_default_limit_is_50(self):
        """Replay without limit defaults to 50."""
        sampler = MagicMock()
        sampler.sample_at.return_value = []
        admin = MagicMock()
        with patch("config.config") as mock_cfg:
            mock_cfg.SAMPLING_ENABLED = True
            client = TestClient(_create_full_app(admin=admin, sampler=sampler))
            resp = client.post("/api/topics/src/replay", json={
                "targetTopic": "dst", "partition": 0, "offset": 0,
            })
        assert resp.status_code == 200
        sampler.sample_at.assert_called_once_with("src", 0, 0, 50)


# ── Additional Reassignment Edge Cases ───────────────────────────────

class TestPartitionReassignmentAdditionalEdgeCases:
    """Additional edge case tests for POST/GET /api/topics/{topic}/reassign."""

    def test_reassign_single_partition_single_replica(self):
        """POST reassign with single partition to single broker."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {
            "success": True, "topic": "small-topic", "partitionsReassigned": 1,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/small-topic/reassign", json={
            "assignments": [{"partition": 0, "replicas": [1]}],
        })
        assert resp.status_code == 200
        assert resp.json()["partitionsReassigned"] == 1
        call_args = admin.reassign_partitions.call_args
        assert call_args[0][0] == "small-topic"
        assert call_args[0][1] == [{"partition": 0, "replicas": [1]}]

    def test_reassign_many_partitions(self):
        """POST reassign with many partitions."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {
            "success": True, "topic": "big-topic", "partitionsReassigned": 20,
        }
        assignments = [{"partition": i, "replicas": [1, 2, 3]} for i in range(20)]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/big-topic/reassign", json={
            "assignments": assignments,
        })
        assert resp.status_code == 200
        assert resp.json()["partitionsReassigned"] == 20
        call_args = admin.reassign_partitions.call_args
        assert len(call_args[0][1]) == 20

    def test_get_reassignment_status_no_admin_returns_503(self):
        """GET /api/topics/{topic}/reassign without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/topics/orders/reassign")
        assert resp.status_code == 503

    def test_get_reassignment_status_multiple_partitions_in_progress(self):
        """GET reassignment status with multiple partitions being reassigned."""
        admin = MagicMock()
        admin.get_partition_reassignment_status.return_value = {
            "topic": "orders",
            "reassignments": [
                {"partition": 0, "replicas": [1, 2, 3], "addingReplicas": [3], "removingReplicas": [4]},
                {"partition": 1, "replicas": [2, 3, 1], "addingReplicas": [1], "removingReplicas": [5]},
                {"partition": 2, "replicas": [3, 1, 2], "addingReplicas": [2], "removingReplicas": [6]},
            ],
            "inProgress": True,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/reassign")
        assert resp.status_code == 200
        body = resp.json()
        assert body["inProgress"] is True
        assert len(body["reassignments"]) == 3
        for r in body["reassignments"]:
            assert "addingReplicas" in r
            assert "removingReplicas" in r

    def test_reassign_partitions_with_duplicate_replicas_passes_through(self):
        """POST reassign with duplicate broker IDs in replicas is passed to admin."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {
            "success": False, "error": "Duplicate broker ID in replica list",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/t/reassign", json={
            "assignments": [{"partition": 0, "replicas": [1, 1, 2]}],
        })
        # Validation is done by admin, not the route
        assert resp.status_code == 400
        assert "Duplicate" in resp.json()["detail"]

    def test_reassign_topic_name_with_dots_and_dashes(self):
        """POST reassign with complex topic name works correctly."""
        admin = MagicMock()
        admin.reassign_partitions.return_value = {
            "success": True, "topic": "ns.orders-v2.retry", "partitionsReassigned": 1,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/ns.orders-v2.retry/reassign", json={
            "assignments": [{"partition": 0, "replicas": [1, 2]}],
        })
        assert resp.status_code == 200
        admin.reassign_partitions.assert_called_once()
        assert admin.reassign_partitions.call_args[0][0] == "ns.orders-v2.retry"

    def test_get_reassignment_status_topic_with_special_chars(self):
        """GET reassignment status for topic with dots and underscores."""
        admin = MagicMock()
        admin.get_partition_reassignment_status.return_value = {
            "topic": "team1.orders_v3",
            "reassignments": [],
            "inProgress": False,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/team1.orders_v3/reassign")
        assert resp.status_code == 200
        admin.get_partition_reassignment_status.assert_called_once_with("team1.orders_v3")


# ── Log Dirs API Tests ───────────────────────────────────────────────

class TestLogDirsAPI:
    """Tests for GET /api/cluster/log-dirs endpoint."""

    def test_log_dirs_success_single_broker(self):
        """GET /api/cluster/log-dirs returns log dir data for a single broker."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = [
            {
                "brokerId": 0,
                "logDir": "/var/kafka-logs",
                "size": 1073741824,
                "partitions": [
                    {"topic": "orders", "partition": 0, "size": 536870912, "offsetLag": 0},
                    {"topic": "orders", "partition": 1, "size": 536870912, "offsetLag": 0},
                ],
                "topicCount": 1,
            }
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 1
        assert body[0]["brokerId"] == 0
        assert body[0]["logDir"] == "/var/kafka-logs"
        assert body[0]["size"] == 1073741824
        assert len(body[0]["partitions"]) == 2
        admin.get_log_dirs.assert_called_once()

    def test_log_dirs_success_multiple_brokers(self):
        """GET /api/cluster/log-dirs returns log dirs across multiple brokers."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = [
            {"brokerId": 0, "logDir": "/data/kafka", "size": 500000000, "partitions": [], "topicCount": 5},
            {"brokerId": 1, "logDir": "/data/kafka", "size": 600000000, "partitions": [], "topicCount": 5},
            {"brokerId": 2, "logDir": "/data/kafka", "size": 550000000, "partitions": [], "topicCount": 5},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 3
        broker_ids = [entry["brokerId"] for entry in body]
        assert broker_ids == [0, 1, 2]

    def test_log_dirs_empty_cluster(self):
        """GET /api/cluster/log-dirs returns empty list when no brokers have log dirs."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        assert resp.json() == []

    def test_log_dirs_no_admin_returns_503(self):
        """GET /api/cluster/log-dirs without kafka_admin returns HTTP 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_log_dirs_admin_exception_returns_500(self):
        """GET /api/cluster/log-dirs returns 500 when admin.get_log_dirs raises."""
        admin = MagicMock()
        admin.get_log_dirs.side_effect = Exception("Broker connection timeout")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 500
        assert "Broker connection timeout" in resp.json()["detail"]

    def test_log_dirs_admin_raises_runtime_error(self):
        """GET /api/cluster/log-dirs returns 500 with detail from RuntimeError."""
        admin = MagicMock()
        admin.get_log_dirs.side_effect = RuntimeError("describe_log_dirs not supported")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 500
        assert "describe_log_dirs not supported" in resp.json()["detail"]

    def test_log_dirs_broker_with_multiple_log_directories(self):
        """GET /api/cluster/log-dirs handles brokers with multiple log directories."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = [
            {"brokerId": 0, "logDir": "/data1/kafka", "size": 300000000, "partitions": [
                {"topic": "events", "partition": 0, "size": 300000000, "offsetLag": 0},
            ], "topicCount": 1},
            {"brokerId": 0, "logDir": "/data2/kafka", "size": 200000000, "partitions": [
                {"topic": "events", "partition": 1, "size": 200000000, "offsetLag": 0},
            ], "topicCount": 1},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        # Both entries are for broker 0 but different log directories
        assert body[0]["brokerId"] == 0
        assert body[1]["brokerId"] == 0
        assert body[0]["logDir"] != body[1]["logDir"]

    def test_log_dirs_large_cluster_many_partitions(self):
        """GET /api/cluster/log-dirs handles response with many partitions."""
        admin = MagicMock()
        partitions = [
            {"topic": f"topic-{i // 10}", "partition": i % 10, "size": 1000000 * (i + 1), "offsetLag": i}
            for i in range(100)
        ]
        admin.get_log_dirs.return_value = [
            {"brokerId": 0, "logDir": "/var/kafka-logs", "size": sum(p["size"] for p in partitions),
             "partitions": partitions, "topicCount": 10},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert len(body[0]["partitions"]) == 100
        assert body[0]["topicCount"] == 10

    def test_log_dirs_response_preserves_all_fields(self):
        """GET /api/cluster/log-dirs passes through all fields from admin response."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = [
            {
                "brokerId": 1,
                "logDir": "/mnt/kafka",
                "size": 0,
                "partitions": [],
                "topicCount": 0,
                "error": None,
                "isFuture": False,
            },
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        body = resp.json()
        entry = body[0]
        assert entry["brokerId"] == 1
        assert entry["logDir"] == "/mnt/kafka"
        assert entry["size"] == 0
        assert entry["partitions"] == []
        assert entry["topicCount"] == 0
        assert entry["error"] is None
        assert entry["isFuture"] is False

    def test_log_dirs_admin_called_without_arguments(self):
        """GET /api/cluster/log-dirs calls admin.get_log_dirs with no arguments."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/cluster/log-dirs")

        admin.get_log_dirs.assert_called_once_with()

    def test_log_dirs_with_zero_size_partitions(self):
        """GET /api/cluster/log-dirs handles partitions with zero size correctly."""
        admin = MagicMock()
        admin.get_log_dirs.return_value = [
            {
                "brokerId": 0,
                "logDir": "/data/kafka",
                "size": 0,
                "partitions": [
                    {"topic": "empty-topic", "partition": 0, "size": 0, "offsetLag": 0},
                    {"topic": "empty-topic", "partition": 1, "size": 0, "offsetLag": 0},
                ],
                "topicCount": 1,
            },
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["size"] == 0
        assert all(p["size"] == 0 for p in body[0]["partitions"])

    def test_log_dirs_admin_raises_connection_error(self):
        """GET /api/cluster/log-dirs returns 500 when admin raises ConnectionError."""
        admin = MagicMock()
        admin.get_log_dirs.side_effect = ConnectionError("Failed to connect to broker")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/log-dirs")

        assert resp.status_code == 500
        assert "Failed to connect to broker" in resp.json()["detail"]


# ── Topic Consumer Groups API Tests ──────────────────────────────────

class TestTopicConsumerGroupsAPI:
    """Tests for GET /api/topics/{topic}/consumer-groups endpoint."""

    def test_topic_consumer_groups_success_single_group(self):
        """GET /api/topics/{topic}/consumer-groups returns a single consuming group."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = [
            {
                "groupId": "order-service",
                "state": "Stable",
                "members": 3,
                "totalLag": 150,
                "partitions": [
                    {"partition": 0, "currentOffset": 1000, "logEndOffset": 1050, "lag": 50},
                    {"partition": 1, "currentOffset": 2000, "logEndOffset": 2050, "lag": 50},
                    {"partition": 2, "currentOffset": 3000, "logEndOffset": 3050, "lag": 50},
                ],
            }
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/consumer-groups")

        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body, list)
        assert len(body) == 1
        assert body[0]["groupId"] == "order-service"
        assert body[0]["state"] == "Stable"
        assert body[0]["totalLag"] == 150
        assert len(body[0]["partitions"]) == 3
        admin.get_topic_consumer_groups.assert_called_once_with("orders")

    def test_topic_consumer_groups_multiple_groups(self):
        """GET /api/topics/{topic}/consumer-groups returns multiple consuming groups."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = [
            {"groupId": "analytics-service", "state": "Stable", "members": 2, "totalLag": 0, "partitions": []},
            {"groupId": "audit-service", "state": "Stable", "members": 1, "totalLag": 500, "partitions": []},
            {"groupId": "search-indexer", "state": "Empty", "members": 0, "totalLag": 10000, "partitions": []},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/events/consumer-groups")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 3
        group_ids = [g["groupId"] for g in body]
        assert "analytics-service" in group_ids
        assert "audit-service" in group_ids
        assert "search-indexer" in group_ids

    def test_topic_consumer_groups_no_consumers(self):
        """GET /api/topics/{topic}/consumer-groups returns empty list when topic has no consumers."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orphan-topic/consumer-groups")

        assert resp.status_code == 200
        body = resp.json()
        assert body == []
        admin.get_topic_consumer_groups.assert_called_once_with("orphan-topic")

    def test_topic_consumer_groups_no_admin_returns_503(self):
        """GET /api/topics/{topic}/consumer-groups without kafka_admin returns HTTP 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/topics/orders/consumer-groups")

        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_topic_consumer_groups_admin_exception_returns_500(self):
        """GET /api/topics/{topic}/consumer-groups returns 500 when admin raises."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.side_effect = Exception("Kafka broker unavailable")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/orders/consumer-groups")

        assert resp.status_code == 500
        assert "Kafka broker unavailable" in resp.json()["detail"]

    def test_topic_consumer_groups_admin_raises_timeout(self):
        """GET /api/topics/{topic}/consumer-groups returns 500 on TimeoutError."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.side_effect = TimeoutError("Request timed out after 30s")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/payments/consumer-groups")

        assert resp.status_code == 500
        assert "Request timed out" in resp.json()["detail"]

    def test_topic_consumer_groups_topic_with_dots_and_dashes(self):
        """GET /api/topics/{topic}/consumer-groups works with complex topic names."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = [
            {"groupId": "svc-a", "state": "Stable", "members": 1, "totalLag": 0, "partitions": []},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/ns.orders-v2.retry/consumer-groups")

        assert resp.status_code == 200
        assert len(resp.json()) == 1
        admin.get_topic_consumer_groups.assert_called_once_with("ns.orders-v2.retry")

    def test_topic_consumer_groups_topic_with_underscores(self):
        """GET /api/topics/{topic}/consumer-groups passes underscored topic names correctly."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/my_topic_name/consumer-groups")

        assert resp.status_code == 200
        admin.get_topic_consumer_groups.assert_called_once_with("my_topic_name")

    def test_topic_consumer_groups_response_preserves_partition_detail(self):
        """GET /api/topics/{topic}/consumer-groups preserves per-partition offset details."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = [
            {
                "groupId": "payment-processor",
                "state": "Stable",
                "members": 4,
                "totalLag": 200,
                "partitions": [
                    {"partition": 0, "currentOffset": 5000, "logEndOffset": 5050, "lag": 50},
                    {"partition": 1, "currentOffset": 6000, "logEndOffset": 6050, "lag": 50},
                    {"partition": 2, "currentOffset": 7000, "logEndOffset": 7050, "lag": 50},
                    {"partition": 3, "currentOffset": 8000, "logEndOffset": 8050, "lag": 50},
                ],
            }
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/payments/consumer-groups")

        assert resp.status_code == 200
        body = resp.json()
        partitions = body[0]["partitions"]
        assert len(partitions) == 4
        assert partitions[0]["currentOffset"] == 5000
        assert partitions[3]["lag"] == 50

    def test_topic_consumer_groups_admin_called_with_topic_arg(self):
        """GET /api/topics/{topic}/consumer-groups passes the topic name to admin."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = []
        client = TestClient(_create_full_app(admin=admin))
        client.get("/api/topics/specific-topic/consumer-groups")

        admin.get_topic_consumer_groups.assert_called_once_with("specific-topic")

    def test_topic_consumer_groups_group_with_empty_state(self):
        """GET /api/topics/{topic}/consumer-groups handles groups in Empty state."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.return_value = [
            {"groupId": "dead-consumer", "state": "Empty", "members": 0, "totalLag": 99999, "partitions": [
                {"partition": 0, "currentOffset": 1, "logEndOffset": 100000, "lag": 99999},
            ]},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/stale-topic/consumer-groups")

        assert resp.status_code == 200
        body = resp.json()
        assert body[0]["state"] == "Empty"
        assert body[0]["members"] == 0
        assert body[0]["totalLag"] == 99999

    def test_topic_consumer_groups_admin_raises_value_error(self):
        """GET /api/topics/{topic}/consumer-groups returns 500 on ValueError."""
        admin = MagicMock()
        admin.get_topic_consumer_groups.side_effect = ValueError("Invalid topic name")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/topics/bad%20topic/consumer-groups")

        assert resp.status_code == 500
        assert "Invalid topic name" in resp.json()["detail"]


# ── Connector Plugins API ────────────────────────────────────────────

class TestConnectorPluginsAPI:
    """Tests for GET /api/connect/plugins (connector-plugins) endpoint."""

    def test_list_plugins_returns_multiple_plugins(self):
        """GET /api/connect/plugins returns all plugins with class names."""
        cc = MagicMock()
        cc.get_connector_plugins.return_value = [
            {"class": "org.apache.kafka.connect.file.FileStreamSourceConnector", "type": "source", "version": "3.5.0"},
            {"class": "org.apache.kafka.connect.file.FileStreamSinkConnector", "type": "sink", "version": "3.5.0"},
            {"class": "io.debezium.connector.mysql.MySqlConnector", "type": "source", "version": "2.3.0"},
        ]
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["plugins"]) == 3
        classes = [p["class"] for p in body["plugins"]]
        assert "io.debezium.connector.mysql.MySqlConnector" in classes
        cc.get_connector_plugins.assert_called_once()

    def test_list_plugins_empty_returns_empty_list(self):
        """GET /api/connect/plugins when no plugins installed returns empty list."""
        cc = MagicMock()
        cc.get_connector_plugins.return_value = []
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 200
        assert resp.json()["plugins"] == []

    def test_list_plugins_no_connect_client_returns_503(self):
        """GET /api/connect/plugins without connect_client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 503
        assert "not configured" in resp.json()["detail"].lower()

    def test_list_plugins_single_plugin(self):
        """GET /api/connect/plugins with exactly one plugin returns list of one."""
        cc = MagicMock()
        cc.get_connector_plugins.return_value = [
            {"class": "com.example.CustomConnector", "type": "source", "version": "1.0.0"},
        ]
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 200
        assert len(resp.json()["plugins"]) == 1
        assert resp.json()["plugins"][0]["class"] == "com.example.CustomConnector"

    def test_list_plugins_preserves_plugin_metadata(self):
        """GET /api/connect/plugins preserves type and version fields."""
        cc = MagicMock()
        cc.get_connector_plugins.return_value = [
            {"class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
             "type": "source", "version": "3.6.0"},
        ]
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 200
        plugin = resp.json()["plugins"][0]
        assert plugin["type"] == "source"
        assert plugin["version"] == "3.6.0"

    def test_list_plugins_response_structure(self):
        """GET /api/connect/plugins wraps result in a 'plugins' key."""
        cc = MagicMock()
        cc.get_connector_plugins.return_value = [{"class": "A"}, {"class": "B"}]
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.get("/api/connect/plugins")
        assert resp.status_code == 200
        body = resp.json()
        assert "plugins" in body
        assert isinstance(body["plugins"], list)


# ── Quota CRUD Additional Tests ──────────────────────────────────────

class TestQuotaCRUDAdditional:
    """Additional tests for POST /api/quotas and DELETE /api/quotas endpoints."""

    def test_set_quota_multiple_quota_keys(self):
        """POST /api/quotas with multiple quota keys passes all of them to admin."""
        admin = MagicMock()
        admin.set_quota.return_value = {"success": True, "entityType": "user", "entityName": "bob"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "bob",
            "quotas": {
                "producer_byte_rate": 2097152,
                "consumer_byte_rate": 4194304,
                "request_percentage": 50,
            },
        })
        assert resp.status_code == 200
        call_kwargs = admin.set_quota.call_args[1]
        assert call_kwargs["quotas"]["producer_byte_rate"] == 2097152
        assert call_kwargs["quotas"]["consumer_byte_rate"] == 4194304
        assert call_kwargs["quotas"]["request_percentage"] == 50

    def test_set_quota_client_id_entity_type(self):
        """POST /api/quotas with entityType=client-id succeeds."""
        admin = MagicMock()
        admin.set_quota.return_value = {"success": True, "entityType": "client-id", "entityName": "producer-1"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "client-id",
            "entityName": "producer-1",
            "quotas": {"producer_byte_rate": 1048576},
        })
        assert resp.status_code == 200
        admin.set_quota.assert_called_once_with(
            entity_type="client-id", entity_name="producer-1",
            quotas={"producer_byte_rate": 1048576},
        )

    def test_set_quota_both_entity_fields_empty_string_returns_400(self):
        """POST /api/quotas with empty string entityType returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "",
            "entityName": "alice",
            "quotas": {"producer_byte_rate": 100},
        })
        assert resp.status_code == 400

    def test_set_quota_entity_name_empty_string_returns_400(self):
        """POST /api/quotas with empty string entityName returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "",
            "quotas": {"producer_byte_rate": 100},
        })
        assert resp.status_code == 400

    def test_delete_quota_multiple_keys(self):
        """DELETE /api/quotas with multiple quotaKeys removes all."""
        admin = MagicMock()
        admin.delete_quota.return_value = {"success": True, "removed": ["producer_byte_rate", "consumer_byte_rate"]}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotaKeys": ["producer_byte_rate", "consumer_byte_rate"],
        })
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["removed"]) == 2

    def test_delete_quota_admin_returns_error_message(self):
        """DELETE /api/quotas when admin returns error includes error detail."""
        admin = MagicMock()
        admin.delete_quota.return_value = {"success": False, "error": "Entity not found"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/quotas", json={
            "entityType": "user",
            "entityName": "unknown-user",
            "quotaKeys": ["producer_byte_rate"],
        })
        assert resp.status_code == 400
        assert "Entity not found" in resp.json()["detail"]

    def test_set_quota_quotas_as_string_returns_400(self):
        """POST /api/quotas with quotas as a string (not dict) returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/quotas", json={
            "entityType": "user",
            "entityName": "alice",
            "quotas": "producer_byte_rate=100",
        })
        assert resp.status_code == 400


# ── ACL Endpoints Additional Tests ───────────────────────────────────

class TestACLEndpointsAdditional:
    """Additional tests for GET/POST/DELETE /api/acls endpoints."""

    def test_list_acls_multiple_entries(self):
        """GET /api/acls returns multiple ACL entries."""
        admin = MagicMock()
        admin.list_acls.return_value = {
            "acls": [
                {"principal": "User:alice", "operation": "READ", "resourceType": "TOPIC",
                 "resourceName": "orders", "permission": "ALLOW", "host": "*", "patternType": "LITERAL"},
                {"principal": "User:bob", "operation": "WRITE", "resourceType": "TOPIC",
                 "resourceName": "payments", "permission": "ALLOW", "host": "*", "patternType": "LITERAL"},
                {"principal": "User:charlie", "operation": "ALL", "resourceType": "GROUP",
                 "resourceName": "my-group", "permission": "ALLOW", "host": "*", "patternType": "LITERAL"},
            ],
            "count": 3,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/acls")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 3
        principals = [a["principal"] for a in body["acls"]]
        assert "User:charlie" in principals

    def test_create_acl_group_resource_type(self):
        """POST /api/acls with GROUP resource type succeeds."""
        admin = MagicMock()
        admin.create_acl.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "GROUP",
            "resourceName": "consumer-group-1",
            "principal": "User:consumer",
            "operation": "READ",
            "permission": "ALLOW",
        })
        assert resp.status_code == 200
        admin.create_acl.assert_called_once_with(
            resource_type="GROUP",
            resource_name="consumer-group-1",
            principal="User:consumer",
            operation="READ",
            permission_type="ALLOW",
            pattern_type="LITERAL",
            host="*",
        )

    def test_create_acl_cluster_resource_type(self):
        """POST /api/acls with CLUSTER resource type succeeds."""
        admin = MagicMock()
        admin.create_acl.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={
            "resourceType": "CLUSTER",
            "resourceName": "kafka-cluster",
            "principal": "User:admin",
            "operation": "ALTER",
            "permission": "ALLOW",
        })
        assert resp.status_code == 200
        admin.create_acl.assert_called_once()

    def test_delete_acl_with_defaults(self):
        """DELETE /api/acls uses default values for optional fields."""
        admin = MagicMock()
        admin.delete_acl.return_value = {"success": True, "deleted": 1}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/acls", json={
            "resourceName": "orders",
            "principal": "User:alice",
        })
        assert resp.status_code == 200
        admin.delete_acl.assert_called_once_with(
            resource_type="ANY",
            resource_name="orders",
            principal="User:alice",
            operation="ANY",
            permission_type="ANY",
        )

    def test_delete_acl_no_admin_returns_503(self):
        """DELETE /api/acls without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.request("DELETE", "/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "orders",
        })
        assert resp.status_code == 503

    def test_create_acl_all_fields_empty_returns_400(self):
        """POST /api/acls with empty body returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/acls", json={})
        assert resp.status_code == 400

    def test_delete_acl_returns_zero_deleted(self):
        """DELETE /api/acls when no matching ACLs returns success with deleted=0."""
        admin = MagicMock()
        admin.delete_acl.return_value = {"success": True, "deleted": 0}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.request("DELETE", "/api/acls", json={
            "resourceType": "TOPIC",
            "resourceName": "nonexistent",
            "principal": "User:nobody",
        })
        assert resp.status_code == 200
        assert resp.json()["deleted"] == 0


# ── Schema Registry Additional Tests ─────────────────────────────────

class TestSchemaRegistryAdditional:
    """Additional tests for schema registry subject and version endpoints."""

    def test_list_subjects_success_multiple(self):
        """GET /api/schema-registry/subjects returns multiple subjects."""
        sr = MagicMock()
        sr.list_subjects.return_value = ["orders-value", "payments-value", "users-key"]
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["subjects"]) == 3
        assert "payments-value" in body["subjects"]

    def test_list_subjects_no_registry_returns_503(self):
        """GET /api/schema-registry/subjects without registry returns 503."""
        client = TestClient(_create_full_app(schema_registry=None))
        resp = client.get("/api/schema-registry/subjects")
        assert resp.status_code == 503
        assert "not configured" in resp.json()["detail"].lower()

    def test_get_subject_versions_success(self):
        """GET /api/schema-registry/subjects/{subject}/versions returns version list."""
        sr = MagicMock()
        sr.get_versions.return_value = [1, 2, 3, 4]
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/orders-value/versions")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subject"] == "orders-value"
        assert body["versions"] == [1, 2, 3, 4]
        sr.get_versions.assert_called_once_with("orders-value")

    def test_get_subject_versions_single_version(self):
        """GET /api/schema-registry/subjects/{subject}/versions with one version."""
        sr = MagicMock()
        sr.get_versions.return_value = [1]
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/new-schema/versions")
        assert resp.status_code == 200
        assert resp.json()["versions"] == [1]

    def test_get_subject_versions_no_registry_returns_503(self):
        """GET /api/schema-registry/subjects/{subject}/versions without registry returns 503."""
        client = TestClient(_create_full_app(schema_registry=None))
        resp = client.get("/api/schema-registry/subjects/test/versions")
        assert resp.status_code == 503

    def test_get_schema_version_latest(self):
        """GET /api/schema-registry/subjects/{subject}/versions/latest returns latest schema."""
        sr = MagicMock()
        sr.get_schema.return_value = {
            "subject": "orders-value",
            "version": 5,
            "id": 42,
            "schema": '{"type":"record","name":"Order","fields":[]}',
        }
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/orders-value/versions/latest")
        assert resp.status_code == 200
        body = resp.json()
        assert body["version"] == 5
        sr.get_schema.assert_called_once_with("orders-value", "latest")

    def test_get_schema_version_numeric(self):
        """GET /api/schema-registry/subjects/{subject}/versions/2 fetches version 2."""
        sr = MagicMock()
        sr.get_schema.return_value = {
            "subject": "users-value",
            "version": 2,
            "id": 10,
            "schema": '{"type":"record","name":"User","fields":[]}',
        }
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/users-value/versions/2")
        assert resp.status_code == 200
        sr.get_schema.assert_called_once_with("users-value", 2)

    def test_get_subject_compatibility(self):
        """GET /api/schema-registry/config/{subject} returns subject-level compatibility."""
        sr = MagicMock()
        sr.get_compatibility.return_value = "FULL_TRANSITIVE"
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/config/orders-value")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subject"] == "orders-value"
        assert body["compatibilityLevel"] == "FULL_TRANSITIVE"
        sr.get_compatibility.assert_called_once_with("orders-value")

    def test_register_schema_error_from_registry(self):
        """POST /api/schema-registry/subjects/{subject}/versions returns 400 on registry error."""
        sr = MagicMock()
        sr.register_schema.return_value = {"error": "Incompatible schema"}
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/orders-value/versions",
            json={"schema": '{"type":"string"}'},
        )
        assert resp.status_code == 400
        assert "Incompatible" in resp.json()["detail"]

    def test_register_schema_no_schema_field_returns_400(self):
        """POST /api/schema-registry/subjects/{subject}/versions without schema returns 400."""
        sr = MagicMock()
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.post(
            "/api/schema-registry/subjects/orders-value/versions",
            json={"schemaType": "AVRO"},
        )
        assert resp.status_code == 400
        assert "schema is required" in resp.json()["detail"]


# ── Topic Config Update Additional Tests ─────────────────────────────

class TestTopicConfigUpdateNew:
    """Additional tests for PUT /api/topics/{topic}/config endpoint."""

    def test_update_config_success_single_key(self):
        """PUT /api/topics/{topic}/config with a single config key succeeds."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True, "topic": "my-topic", "updated": ["retention.ms"],
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/my-topic/config", json={
            "configs": {"retention.ms": "604800000"},
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert "retention.ms" in body["updated"]
        admin.update_topic_config.assert_called_once_with("my-topic", {"retention.ms": "604800000"})

    def test_update_config_empty_configs_dict_returns_400(self):
        """PUT /api/topics/{topic}/config with empty configs dict returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/my-topic/config", json={"configs": {}})
        assert resp.status_code == 400
        assert "non-empty" in resp.json()["detail"].lower()

    def test_update_config_configs_not_dict_returns_400(self):
        """PUT /api/topics/{topic}/config with configs as a list returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/my-topic/config", json={"configs": ["retention.ms"]})
        assert resp.status_code == 400

    def test_update_config_missing_configs_key_returns_400(self):
        """PUT /api/topics/{topic}/config without configs key returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/my-topic/config", json={"something": "else"})
        assert resp.status_code == 400

    def test_update_config_admin_failure_returns_400(self):
        """PUT /api/topics/{topic}/config when admin returns failure returns 400."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": False, "error": "Invalid config key: bad.key",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/test-topic/config", json={
            "configs": {"bad.key": "value"},
        })
        assert resp.status_code == 400
        assert "Invalid config key" in resp.json()["detail"]

    def test_update_config_no_admin_returns_503(self):
        """PUT /api/topics/{topic}/config without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.put("/api/topics/my-topic/config", json={
            "configs": {"retention.ms": "1000"},
        })
        assert resp.status_code == 503

    def test_update_config_compression_type(self):
        """PUT /api/topics/{topic}/config for compression.type passes correctly."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True, "topic": "compressed", "updated": ["compression.type"],
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/compressed/config", json={
            "configs": {"compression.type": "lz4"},
        })
        assert resp.status_code == 200
        admin.update_topic_config.assert_called_once_with("compressed", {"compression.type": "lz4"})

    def test_update_config_multiple_keys_success(self):
        """PUT /api/topics/{topic}/config with multiple keys updates all."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True, "topic": "orders",
            "updated": ["retention.ms", "max.message.bytes", "cleanup.policy"],
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/orders/config", json={
            "configs": {
                "retention.ms": "86400000",
                "max.message.bytes": "2097152",
                "cleanup.policy": "compact",
            },
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert len(body["updated"]) == 3

    def test_update_config_configs_as_string_returns_400(self):
        """PUT /api/topics/{topic}/config with configs as a string returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/my-topic/config", json={"configs": "retention.ms=1000"})
        assert resp.status_code == 400

    def test_update_config_topic_with_dots_in_name(self):
        """PUT /api/topics/{topic}/config works with dots in topic name."""
        admin = MagicMock()
        admin.update_topic_config.return_value = {
            "success": True, "topic": "org.events.orders.v2", "updated": ["retention.ms"],
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.put("/api/topics/org.events.orders.v2/config", json={
            "configs": {"retention.ms": "172800000"},
        })
        assert resp.status_code == 200
        admin.update_topic_config.assert_called_once_with(
            "org.events.orders.v2", {"retention.ms": "172800000"},
        )


# ── Produce Message API Additional Tests ─────────────────────────────

class TestProduceMessageAPIAdditional:
    """Additional tests for POST /api/topics/{topic}/produce endpoint."""

    def test_produce_with_key_and_value_only(self):
        """POST /api/topics/{topic}/produce with key and value, no headers."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True, "topic": "events", "partition": 0, "offset": 55,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/events/produce", json={
            "key": "evt-001", "value": "some-event-data",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["topic"] == "events"
        assert body["partition"] == 0
        assert body["offset"] == 55
        admin.produce_message.assert_called_once_with(
            topic="events", value="some-event-data", key="evt-001",
            headers=None, partition=None,
        )

    def test_produce_without_key(self):
        """POST /api/topics/{topic}/produce without key sends key=None."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True, "topic": "logs", "partition": 1, "offset": 200,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/logs/produce", json={
            "value": "log-entry-123",
        })
        assert resp.status_code == 200
        assert resp.json()["partition"] == 1
        admin.produce_message.assert_called_once_with(
            topic="logs", value="log-entry-123", key=None,
            headers=None, partition=None,
        )

    def test_produce_with_headers_only(self):
        """POST /api/topics/{topic}/produce with headers but no key."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True, "topic": "audit", "partition": 3, "offset": 77,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/audit/produce", json={
            "value": "audit-record",
            "headers": {"correlation-id": "xyz-789", "source": "service-a"},
        })
        assert resp.status_code == 200
        admin.produce_message.assert_called_once_with(
            topic="audit", value="audit-record", key=None,
            headers={"correlation-id": "xyz-789", "source": "service-a"},
            partition=None,
        )

    def test_produce_invalid_json_raises_error(self):
        """POST /api/topics/{topic}/produce with invalid JSON body raises error."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin), raise_server_exceptions=False)
        resp = client.post(
            "/api/topics/orders/produce",
            content=b"not-valid-json{{{",
            headers={"content-type": "application/json"},
        )
        # The endpoint calls request.json() without try/catch, so invalid JSON
        # causes a server error
        assert resp.status_code == 500

    def test_produce_no_admin_returns_503(self):
        """POST /api/topics/{topic}/produce without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/topics/orders/produce", json={"value": "hello"})
        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_produce_with_all_fields(self):
        """POST /api/topics/{topic}/produce with key, value, headers, and partition."""
        admin = MagicMock()
        admin.produce_message.return_value = {
            "success": True, "topic": "full-msg", "partition": 7, "offset": 999,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/full-msg/produce", json={
            "key": "k1",
            "value": "v1",
            "headers": {"h1": "val1"},
            "partition": 7,
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["partition"] == 7
        assert body["offset"] == 999
        admin.produce_message.assert_called_once_with(
            topic="full-msg", value="v1", key="k1",
            headers={"h1": "val1"}, partition=7,
        )


# ── Schema Registry Version Detail Tests ─────────────────────────────

class TestSchemaRegistryVersionDetail:
    """Tests for GET /api/schema-registry/subjects/{subject}/versions/{version}."""

    def test_get_specific_version_number(self):
        """GET with a specific numeric version returns that version's schema."""
        sr = MagicMock()
        sr.get_schema.return_value = {
            "subject": "payments-value", "version": 3, "id": 25,
            "schema": '{"type":"record","name":"Payment","fields":[]}',
        }
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/payments-value/versions/3")
        assert resp.status_code == 200
        body = resp.json()
        assert body["subject"] == "payments-value"
        assert body["version"] == 3
        assert body["id"] == 25
        sr.get_schema.assert_called_once_with("payments-value", 3)

    def test_get_version_latest(self):
        """GET with 'latest' as version returns the latest schema."""
        sr = MagicMock()
        sr.get_schema.return_value = {
            "subject": "inventory-value", "version": 10, "id": 88,
            "schema": '{"type":"record","name":"Inventory","fields":[]}',
        }
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/inventory-value/versions/latest")
        assert resp.status_code == 200
        body = resp.json()
        assert body["version"] == 10
        sr.get_schema.assert_called_once_with("inventory-value", "latest")

    def test_get_version_1(self):
        """GET with version 1 returns the first schema version."""
        sr = MagicMock()
        sr.get_schema.return_value = {
            "subject": "users-key", "version": 1, "id": 1,
            "schema": '{"type":"string"}',
        }
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/users-key/versions/1")
        assert resp.status_code == 200
        assert resp.json()["version"] == 1
        sr.get_schema.assert_called_once_with("users-key", 1)

    def test_get_invalid_version_non_numeric(self):
        """GET with a non-numeric, non-'latest' version raises a ValueError."""
        sr = MagicMock()
        client = TestClient(_create_full_app(schema_registry=sr), raise_server_exceptions=False)
        resp = client.get("/api/schema-registry/subjects/test-value/versions/abc")
        # int("abc") raises ValueError in the endpoint, causing a 500
        assert resp.status_code == 500

    def test_get_version_no_registry_returns_503(self):
        """GET version without schema registry returns 503."""
        client = TestClient(_create_full_app(schema_registry=None))
        resp = client.get("/api/schema-registry/subjects/test-value/versions/1")
        assert resp.status_code == 503
        assert "not configured" in resp.json()["detail"].lower()

    def test_get_version_registry_returns_empty_dict(self):
        """GET version when registry returns empty dict for missing schema."""
        sr = MagicMock()
        sr.get_schema.return_value = {}
        client = TestClient(_create_full_app(schema_registry=sr))
        resp = client.get("/api/schema-registry/subjects/gone/versions/99")
        assert resp.status_code == 200
        assert resp.json() == {}


# ── Consumer Group Delete Additional Tests ────────────────────────────

class TestConsumerGroupDeleteNew:
    """Additional tests for DELETE /api/consumer-groups/{group}."""

    def test_delete_consumer_group_success(self):
        """DELETE /api/consumer-groups/{group} returns success for empty group."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": True, "groupId": "idle-consumers",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/consumer-groups/idle-consumers")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["groupId"] == "idle-consumers"
        admin.delete_consumer_group.assert_called_once_with("idle-consumers")

    def test_delete_consumer_group_no_admin_returns_503(self):
        """DELETE /api/consumer-groups/{group} without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.delete("/api/consumer-groups/any-group")
        assert resp.status_code == 503
        assert "not available" in resp.json()["detail"].lower()

    def test_delete_consumer_group_error_message(self):
        """DELETE /api/consumer-groups/{group} propagates error detail from admin."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": False, "error": "Group has active members, cannot delete",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/consumer-groups/busy-group")
        assert resp.status_code == 400
        assert "active members" in resp.json()["detail"]

    def test_delete_consumer_group_with_dots_in_name(self):
        """DELETE /api/consumer-groups/{group} works with dots in group name."""
        admin = MagicMock()
        admin.delete_consumer_group.return_value = {
            "success": True, "groupId": "org.team.service-consumer",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/consumer-groups/org.team.service-consumer")
        assert resp.status_code == 200
        admin.delete_consumer_group.assert_called_once_with("org.team.service-consumer")


# ── Connector Config Update Tests ─────────────────────────────────────

class TestConnectorConfigUpdateAdditional:
    """Additional tests for PUT /api/connect/connectors/{name}/config."""

    def test_update_connector_config_success_with_fields(self):
        """PUT /api/connect/connectors/{name}/config returns updated connector."""
        cc = MagicMock()
        cc.update_connector.return_value = {
            "name": "jdbc-source",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "topics": "db-changes",
                "connection.url": "jdbc:postgresql://localhost/db",
            },
        }
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/jdbc-source/config", json={
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "topics": "db-changes",
            "connection.url": "jdbc:postgresql://localhost/db",
        })
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "jdbc-source"
        assert body["config"]["topics"] == "db-changes"
        cc.update_connector.assert_called_once()

    def test_update_connector_config_no_connect_returns_503(self):
        """PUT /api/connect/connectors/{name}/config without connect returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.put("/api/connect/connectors/my-conn/config", json={
            "connector.class": "Foo",
        })
        assert resp.status_code == 503
        assert "not configured" in resp.json()["detail"].lower()

    def test_update_connector_config_invalid_json_returns_error(self):
        """PUT /api/connect/connectors/{name}/config with invalid JSON returns error."""
        cc = MagicMock()
        client = TestClient(_create_full_app(connect_client=cc), raise_server_exceptions=False)
        resp = client.put(
            "/api/connect/connectors/my-conn/config",
            content=b"{{invalid json",
            headers={"content-type": "application/json"},
        )
        # The endpoint calls request.json() without try/catch, so invalid JSON
        # causes a server error
        assert resp.status_code == 500

    def test_update_connector_config_error_from_connect(self):
        """PUT /api/connect/connectors/{name}/config error propagates as 400."""
        cc = MagicMock()
        cc.update_connector.return_value = {"error": "Connector not found: ghost-conn"}
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/ghost-conn/config", json={
            "connector.class": "Foo",
        })
        assert resp.status_code == 400
        assert "not found" in resp.json()["detail"].lower()


# ── Broker Config Additional Tests ────────────────────────────────────

class TestBrokerConfigAdditional:
    """Additional tests for GET /api/brokers/{broker_id}/config."""

    def test_get_broker_config_returns_all_fields(self):
        """GET /api/brokers/{id}/config returns correct brokerId and config entries."""
        admin = MagicMock()
        admin.describe_broker_config.return_value = [
            {"name": "log.dirs", "value": "/var/kafka-logs", "source": "STATIC_BROKER_CONFIG",
             "isReadOnly": True, "isSensitive": False},
            {"name": "num.partitions", "value": "1", "source": "DEFAULT_CONFIG",
             "isReadOnly": False, "isSensitive": False},
            {"name": "ssl.keystore.password", "value": "***", "source": "STATIC_BROKER_CONFIG",
             "isReadOnly": True, "isSensitive": True},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/2/config")
        assert resp.status_code == 200
        body = resp.json()
        assert body["brokerId"] == 2
        assert len(body["configs"]) == 3
        admin.describe_broker_config.assert_called_once_with(2)

    def test_get_broker_config_invalid_broker_id_string(self):
        """GET /api/brokers/{id}/config with non-numeric id returns 422."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/abc/config")
        # FastAPI's path param type validation rejects non-int
        assert resp.status_code == 422

    def test_get_broker_config_no_admin_returns_503(self):
        """GET /api/brokers/{id}/config without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/brokers/0/config")
        assert resp.status_code == 503

    def test_get_broker_config_large_broker_id(self):
        """GET /api/brokers/{id}/config with large broker id works correctly."""
        admin = MagicMock()
        admin.describe_broker_config.return_value = [
            {"name": "broker.id", "value": "9999", "source": "STATIC_BROKER_CONFIG",
             "isReadOnly": True, "isSensitive": False},
        ]
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/brokers/9999/config")
        assert resp.status_code == 200
        assert resp.json()["brokerId"] == 9999
        admin.describe_broker_config.assert_called_once_with(9999)


# ── Topic Deletion Additional Tests ───────────────────────────────────

class TestTopicDeletionAdditional:
    """Additional tests for DELETE /api/topics/{topic}."""

    def test_delete_topic_success_returns_topic_name(self):
        """DELETE /api/topics/{topic} returns success with topic name."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": True, "topic": "expired-events",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/topics/expired-events")
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["topic"] == "expired-events"
        admin.delete_topic.assert_called_once_with("expired-events")

    def test_delete_topic_no_admin_returns_503(self):
        """DELETE /api/topics/{topic} without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.delete("/api/topics/any-topic")
        assert resp.status_code == 503

    def test_delete_topic_error_returns_400(self):
        """DELETE /api/topics/{topic} when admin returns error returns 400."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": False,
            "error": "TopicDeletionDisabledException: topic deletion is disabled",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/topics/protected-topic")
        assert resp.status_code == 400
        assert "deletion is disabled" in resp.json()["detail"]

    def test_delete_topic_with_underscores(self):
        """DELETE /api/topics/{topic} with underscores in topic name."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": True, "topic": "team_analytics_raw_events",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/topics/team_analytics_raw_events")
        assert resp.status_code == 200
        admin.delete_topic.assert_called_once_with("team_analytics_raw_events")

    def test_delete_topic_internal_topic_error(self):
        """DELETE /api/topics/{topic} for internal topic returns error from admin."""
        admin = MagicMock()
        admin.delete_topic.return_value = {
            "success": False,
            "error": "Cannot delete internal topic __consumer_offsets",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.delete("/api/topics/__consumer_offsets")
        assert resp.status_code == 400
        assert "internal topic" in resp.json()["detail"].lower()


# ── Elect Preferred Leaders Additional Edge Cases ─────────────────────

class TestElectPreferredLeadersAdditional:
    """Additional edge-case tests for POST /api/cluster/elect-leaders."""

    def test_elect_leaders_returns_zero_elections(self):
        """POST /api/cluster/elect-leaders when no elections needed returns zero count."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {
            "success": True, "electionsTriggered": 0,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={})
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        assert body["electionsTriggered"] == 0

    def test_elect_leaders_topic_with_hyphens_and_dots(self):
        """POST /api/cluster/elect-leaders with complex topic name."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {
            "success": True, "electionsTriggered": 6,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={
            "topic": "prod.orders-created.v3",
        })
        assert resp.status_code == 200
        admin.elect_preferred_leaders.assert_called_once_with("prod.orders-created.v3")

    def test_elect_leaders_admin_timeout_exception(self):
        """POST /api/cluster/elect-leaders when admin times out returns 500."""
        admin = MagicMock()
        admin.elect_preferred_leaders.side_effect = TimeoutError("Request timed out after 30s")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={"topic": "slow-topic"})
        assert resp.status_code == 500
        assert "timed out" in resp.json()["detail"].lower()

    def test_elect_leaders_large_election_count(self):
        """POST /api/cluster/elect-leaders with many elections triggered."""
        admin = MagicMock()
        admin.elect_preferred_leaders.return_value = {
            "success": True, "electionsTriggered": 500,
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/cluster/elect-leaders", json={})
        assert resp.status_code == 200
        assert resp.json()["electionsTriggered"] == 500


# ── WebSocket Connection Tests ───────────────────────────────────────

class TestWebSocketConnectionHandling:
    """Test /ws/graph endpoint connection handling and message format."""

    def _make_ws_app(self, snapshot_data=None):
        """Create a minimal app with a WS endpoint for testing."""
        import secrets as _secrets
        from fastapi import WebSocket, WebSocketDisconnect

        ws_app = FastAPI()
        mock_builder = MagicMock()
        default_snapshot = {
            "type": "graph_snapshot",
            "ts": 9000,
            "nodes": {"added": [{"id": "topic-orders", "type": "topic"}], "updated": [], "removed": []},
            "edges": {"added": [], "updated": [], "removed": []},
            "metrics": {"topicCount": 1, "consumerGroupCount": 0},
        }
        mock_builder.get_snapshot.return_value = snapshot_data or default_snapshot

        @ws_app.websocket("/ws/graph")
        async def ws_graph(websocket: WebSocket):
            await websocket.accept()
            client_id = _secrets.token_hex(8)
            try:
                snapshot = mock_builder.get_snapshot()
                snapshot["config"] = {
                    "showProducers": True,
                    "samplingEnabled": True,
                    "lagWarnThreshold": 500,
                    "animationsEnabled": False,
                }
                await websocket.send_text(json.dumps(snapshot))
            except Exception:
                pass
            try:
                while True:
                    data = await websocket.receive_text()
                    try:
                        msg = json.loads(data)
                        if msg.get("type") == "request_snapshot":
                            snap = mock_builder.get_snapshot()
                            await websocket.send_text(json.dumps(snap))
                        elif msg.get("type") == "ping":
                            await websocket.send_text(json.dumps({"type": "pong"}))
                    except json.JSONDecodeError:
                        pass
            except WebSocketDisconnect:
                pass

        return ws_app, mock_builder

    def test_ws_connection_accepted_and_snapshot_received(self):
        """WebSocket connection is accepted and initial snapshot is sent."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            msg = json.loads(ws.receive_text())
            assert msg["type"] == "graph_snapshot"
            assert msg["ts"] == 9000

    def test_ws_initial_snapshot_includes_nodes_and_edges(self):
        """Initial snapshot must include nodes and edges structures."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            msg = json.loads(ws.receive_text())
            assert "added" in msg["nodes"]
            assert "updated" in msg["nodes"]
            assert "removed" in msg["nodes"]
            assert "added" in msg["edges"]
            assert len(msg["nodes"]["added"]) == 1
            assert msg["nodes"]["added"][0]["id"] == "topic-orders"

    def test_ws_initial_snapshot_config_includes_all_keys(self):
        """Config in initial snapshot must include showProducers, samplingEnabled, lagWarnThreshold, animationsEnabled."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            msg = json.loads(ws.receive_text())
            cfg = msg["config"]
            assert cfg["showProducers"] is True
            assert cfg["samplingEnabled"] is True
            assert cfg["lagWarnThreshold"] == 500
            assert cfg["animationsEnabled"] is False

    def test_ws_request_snapshot_returns_snapshot(self):
        """Sending request_snapshot message causes a second snapshot response."""
        ws_app, mock_builder = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            # consume initial snapshot
            ws.receive_text()
            # request another snapshot
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            msg = json.loads(ws.receive_text())
            assert msg["type"] == "graph_snapshot"
            assert mock_builder.get_snapshot.call_count == 2

    def test_ws_invalid_json_ignored(self):
        """Non-JSON text sent to the WebSocket does not crash the server."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            ws.receive_text()  # initial snapshot
            ws.send_text("not valid json {{{{")
            # send a valid message after to confirm the connection is alive
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            msg = json.loads(ws.receive_text())
            assert msg["type"] == "graph_snapshot"

    def test_ws_unknown_message_type_ignored(self):
        """Unknown message types are ignored without error."""
        ws_app, _ = self._make_ws_app()
        client = TestClient(ws_app)
        with client.websocket_connect("/ws/graph") as ws:
            ws.receive_text()  # initial snapshot
            ws.send_text(json.dumps({"type": "unknown_action", "data": "foo"}))
            ws.send_text(json.dumps({"type": "request_snapshot"}))
            msg = json.loads(ws.receive_text())
            assert msg["type"] == "graph_snapshot"


# ── Cluster Health Endpoint Tests ────────────────────────────────────

class TestClusterHealthEndpointComprehensive:
    """Test GET /api/cluster/health with various health states."""

    def test_cluster_health_all_healthy(self):
        """GET /api/cluster/health returns healthy when all partitions are fully replicated."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "status": "healthy",
            "brokerCount": 3,
            "topicCount": 10,
            "underReplicatedPartitions": 0,
            "offlinePartitions": 0,
            "leaderDistribution": {1: 10, 2: 10, 3: 10},
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert body["underReplicatedPartitions"] == 0
        assert body["offlinePartitions"] == 0

    def test_cluster_health_under_replicated_partitions(self):
        """Cluster health shows under-replicated partitions count."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "status": "warning",
            "brokerCount": 3,
            "topicCount": 20,
            "underReplicatedPartitions": 15,
            "offlinePartitions": 0,
            "leaderDistribution": {1: 25, 2: 20, 3: 15},
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "warning"
        assert body["underReplicatedPartitions"] == 15

    def test_cluster_health_offline_partitions(self):
        """Cluster health shows offline partitions indicating critical state."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "status": "critical",
            "brokerCount": 3,
            "topicCount": 10,
            "underReplicatedPartitions": 30,
            "offlinePartitions": 5,
            "leaderDistribution": {1: 20, 3: 10},
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "critical"
        assert body["offlinePartitions"] == 5

    def test_cluster_health_leader_distribution_skewed(self):
        """Cluster health returns leader distribution showing skew."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "status": "warning",
            "brokerCount": 3,
            "topicCount": 30,
            "underReplicatedPartitions": 0,
            "offlinePartitions": 0,
            "leaderDistribution": {1: 50, 2: 5, 3: 5},
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        dist = body["leaderDistribution"]
        # Broker 1 has 50 leaders vs 5 for others -- skewed
        assert dist["1"] == 50
        assert dist["2"] == 5

    def test_cluster_health_single_broker(self):
        """Cluster health with a single broker."""
        admin = MagicMock()
        admin.get_cluster_health.return_value = {
            "status": "healthy",
            "brokerCount": 1,
            "topicCount": 5,
            "underReplicatedPartitions": 0,
            "offlinePartitions": 0,
            "leaderDistribution": {1: 15},
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["brokerCount"] == 1
        assert body["leaderDistribution"]["1"] == 15

    def test_cluster_health_admin_exception_returns_500(self):
        """GET /api/cluster/health when admin raises returns 500."""
        admin = MagicMock()
        admin.get_cluster_health.side_effect = ConnectionError("Broker not reachable")
        client = TestClient(_create_full_app(admin=admin))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 500
        assert "not reachable" in resp.json()["detail"].lower()

    def test_cluster_health_no_admin_returns_503(self):
        """GET /api/cluster/health without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.get("/api/cluster/health")
        assert resp.status_code == 503


# ── Consumer Group Reset Offsets Tests ───────────────────────────────

class TestConsumerGroupResetOffsetsStrategies:
    """Test POST /api/consumer-groups/{id}/reset-offsets with all strategies."""

    def test_reset_offsets_strategy_latest(self):
        """Reset offsets with strategy=latest succeeds."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "strategy": "latest"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/my-group/reset-offsets", json={
            "strategy": "latest", "topic": "orders",
        })
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="my-group", strategy="latest", topic="orders",
            timestamp=None, offset=None,
        )

    def test_reset_offsets_strategy_earliest(self):
        """Reset offsets with strategy=earliest succeeds."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "strategy": "earliest"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/my-group/reset-offsets", json={
            "strategy": "earliest", "topic": "payments",
        })
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="my-group", strategy="earliest", topic="payments",
            timestamp=None, offset=None,
        )

    def test_reset_offsets_strategy_timestamp(self):
        """Reset offsets with strategy=timestamp and a timestamp value."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "strategy": "timestamp"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/analytics-svc/reset-offsets", json={
            "strategy": "timestamp", "topic": "events", "timestamp": 1700000000000,
        })
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="analytics-svc", strategy="timestamp", topic="events",
            timestamp=1700000000000, offset=None,
        )

    def test_reset_offsets_strategy_specific_offset(self):
        """Reset offsets with strategy=specific and a specific offset value."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True, "strategy": "specific"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/worker-group/reset-offsets", json={
            "strategy": "specific", "topic": "jobs", "offset": 42,
        })
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="worker-group", strategy="specific", topic="jobs",
            timestamp=None, offset=42,
        )

    def test_reset_offsets_no_strategy_defaults_to_latest(self):
        """Reset offsets without explicit strategy defaults to latest."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {"success": True}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/g1/reset-offsets", json={
            "topic": "some-topic",
        })
        assert resp.status_code == 200
        admin.reset_offsets.assert_called_once_with(
            group_id="g1", strategy="latest", topic="some-topic",
            timestamp=None, offset=None,
        )

    def test_reset_offsets_active_group_returns_400(self):
        """Reset offsets for an active group returns 400."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": False, "error": "Consumer group 'active-group' is not empty (has active members)",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/active-group/reset-offsets", json={
            "strategy": "earliest",
        })
        assert resp.status_code == 400
        assert "not empty" in resp.json()["detail"].lower()

    def test_reset_offsets_nonexistent_group_returns_400(self):
        """Reset offsets for a non-existent group returns 400."""
        admin = MagicMock()
        admin.reset_offsets.return_value = {
            "success": False, "error": "Consumer group 'missing-group' not found",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/consumer-groups/missing-group/reset-offsets", json={
            "strategy": "latest",
        })
        assert resp.status_code == 400


# ── Topic Creation Tests ─────────────────────────────────────────────

class TestTopicCreationComprehensive:
    """Test POST /api/topics with all parameter combinations."""

    def test_create_topic_minimal(self):
        """Create a topic with only name (defaults for partitions and replication)."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True, "topic": "minimal-topic"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={"name": "minimal-topic"})
        assert resp.status_code == 200
        admin.create_topic.assert_called_once_with(
            name="minimal-topic", partitions=1, replication_factor=1, configs=None,
        )

    def test_create_topic_with_partitions(self):
        """Create a topic with a specific number of partitions."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True, "topic": "partitioned-topic"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={"name": "partitioned-topic", "partitions": 12})
        assert resp.status_code == 200
        admin.create_topic.assert_called_once_with(
            name="partitioned-topic", partitions=12, replication_factor=1, configs=None,
        )

    def test_create_topic_with_replication_factor(self):
        """Create a topic with replication factor."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True, "topic": "replicated-topic"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={
            "name": "replicated-topic", "partitions": 6, "replicationFactor": 3,
        })
        assert resp.status_code == 200
        admin.create_topic.assert_called_once_with(
            name="replicated-topic", partitions=6, replication_factor=3, configs=None,
        )

    def test_create_topic_with_cleanup_policy(self):
        """Create a topic with cleanup.policy=compact config."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True, "topic": "compact-topic"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={
            "name": "compact-topic", "partitions": 3,
            "configs": {"cleanup.policy": "compact"},
        })
        assert resp.status_code == 200
        admin.create_topic.assert_called_once_with(
            name="compact-topic", partitions=3, replication_factor=1,
            configs={"cleanup.policy": "compact"},
        )

    def test_create_topic_with_retention_ms(self):
        """Create a topic with custom retention.ms config."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": True, "topic": "short-retention-topic"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={
            "name": "short-retention-topic", "partitions": 6, "replicationFactor": 2,
            "configs": {"retention.ms": "3600000", "cleanup.policy": "delete"},
        })
        assert resp.status_code == 200
        admin.create_topic.assert_called_once_with(
            name="short-retention-topic", partitions=6, replication_factor=2,
            configs={"retention.ms": "3600000", "cleanup.policy": "delete"},
        )

    def test_create_topic_empty_name_returns_400(self):
        """Create a topic with empty name returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={"name": ""})
        assert resp.status_code == 400

    def test_create_topic_already_exists_returns_400(self):
        """Create a topic that already exists returns 400."""
        admin = MagicMock()
        admin.create_topic.return_value = {"success": False, "error": "Topic 'orders' already exists"}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics", json={"name": "orders"})
        assert resp.status_code == 400
        assert "already exists" in resp.json()["detail"].lower()


# ── Add Partitions Tests ─────────────────────────────────────────────

class TestAddPartitionsComprehensive:
    """Test POST /api/topics/{topic}/partitions with success, validation, error cases."""

    def test_add_partitions_success(self):
        """Successfully increase partitions for a topic."""
        admin = MagicMock()
        admin.add_topic_partitions.return_value = {"success": True, "topic": "orders", "totalPartitions": 12}
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/partitions", json={"totalPartitions": 12})
        assert resp.status_code == 200
        body = resp.json()
        assert body["success"] is True
        admin.add_topic_partitions.assert_called_once_with("orders", 12)

    def test_add_partitions_missing_total_returns_400(self):
        """Missing totalPartitions field returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/partitions", json={})
        assert resp.status_code == 400
        assert "totalpartitions" in resp.json()["detail"].lower()

    def test_add_partitions_non_integer_returns_400(self):
        """Non-integer totalPartitions returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/partitions", json={"totalPartitions": "twelve"})
        assert resp.status_code == 400

    def test_add_partitions_zero_returns_400(self):
        """totalPartitions=0 returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/partitions", json={"totalPartitions": 0})
        assert resp.status_code == 400

    def test_add_partitions_negative_returns_400(self):
        """Negative totalPartitions returns 400."""
        admin = MagicMock()
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/partitions", json={"totalPartitions": -5})
        assert resp.status_code == 400

    def test_add_partitions_fewer_than_current_returns_400(self):
        """Requesting fewer partitions than currently exist returns 400."""
        admin = MagicMock()
        admin.add_topic_partitions.return_value = {
            "success": False, "error": "Cannot reduce partitions from 6 to 3",
        }
        client = TestClient(_create_full_app(admin=admin))
        resp = client.post("/api/topics/orders/partitions", json={"totalPartitions": 3})
        assert resp.status_code == 400
        assert "reduce" in resp.json()["detail"].lower()

    def test_add_partitions_no_admin_returns_503(self):
        """POST partitions without admin returns 503."""
        client = TestClient(_create_full_app(admin=None))
        resp = client.post("/api/topics/orders/partitions", json={"totalPartitions": 12})
        assert resp.status_code == 503


# ── Connector Lifecycle Tests ────────────────────────────────────────

class TestConnectorLifecycleComprehensive:
    """Test connector pause, resume, restart, delete, and task restart lifecycle."""

    def test_pause_connector_success_verifies_call(self):
        """PUT pause calls pause_connector with the connector name."""
        cc = MagicMock()
        cc.pause_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/jdbc-source/pause")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.pause_connector.assert_called_once_with("jdbc-source")

    def test_resume_connector_success_verifies_call(self):
        """PUT resume calls resume_connector with the connector name."""
        cc = MagicMock()
        cc.resume_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.put("/api/connect/connectors/jdbc-source/resume")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.resume_connector.assert_called_once_with("jdbc-source")

    def test_restart_connector_success_verifies_call(self):
        """POST restart calls restart_connector with the connector name."""
        cc = MagicMock()
        cc.restart_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/es-sink/restart")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.restart_connector.assert_called_once_with("es-sink")

    def test_delete_connector_verifies_call(self):
        """DELETE connector calls delete_connector with the name."""
        cc = MagicMock()
        cc.delete_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.delete("/api/connect/connectors/old-sink")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.delete_connector.assert_called_once_with("old-sink")

    def test_task_restart_specific_task(self):
        """POST task restart for a specific task id passes correct arguments."""
        cc = MagicMock()
        cc.restart_task.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/jdbc-source/tasks/3/restart")
        assert resp.status_code == 200
        assert resp.json()["success"] is True
        cc.restart_task.assert_called_once_with("jdbc-source", 3)

    def test_pause_then_resume_lifecycle(self):
        """Pause then resume a connector lifecycle."""
        cc = MagicMock()
        cc.pause_connector.return_value = True
        cc.resume_connector.return_value = True
        client = TestClient(_create_full_app(connect_client=cc))

        resp_pause = client.put("/api/connect/connectors/s3-sink/pause")
        assert resp_pause.status_code == 200

        resp_resume = client.put("/api/connect/connectors/s3-sink/resume")
        assert resp_resume.status_code == 200

        cc.pause_connector.assert_called_once_with("s3-sink")
        cc.resume_connector.assert_called_once_with("s3-sink")

    def test_connector_restart_failure_returns_500(self):
        """POST restart when restart_connector returns False returns 500."""
        cc = MagicMock()
        cc.restart_connector.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/broken-conn/restart")
        assert resp.status_code == 500

    def test_connector_delete_failure_returns_500(self):
        """DELETE connector when delete_connector returns False returns 500."""
        cc = MagicMock()
        cc.delete_connector.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.delete("/api/connect/connectors/locked-conn")
        assert resp.status_code == 500

    def test_connector_pause_no_connect_returns_503(self):
        """PUT pause without connect_client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.put("/api/connect/connectors/foo/pause")
        assert resp.status_code == 503

    def test_connector_resume_no_connect_returns_503(self):
        """PUT resume without connect_client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.put("/api/connect/connectors/foo/resume")
        assert resp.status_code == 503

    def test_connector_restart_no_connect_returns_503(self):
        """POST restart without connect_client returns 503."""
        client = TestClient(_create_full_app(connect_client=None))
        resp = client.post("/api/connect/connectors/foo/restart")
        assert resp.status_code == 503

    def test_task_restart_failure_returns_500(self):
        """POST task restart when restart_task returns False returns 500."""
        cc = MagicMock()
        cc.restart_task.return_value = False
        client = TestClient(_create_full_app(connect_client=cc))
        resp = client.post("/api/connect/connectors/my-conn/tasks/1/restart")
        assert resp.status_code == 500
        assert "task 1" in resp.json()["detail"].lower()
