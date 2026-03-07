"""GraphStateBuilder — builds full pipeline graph with service nodes."""

import logging
import re
import time
from dataclasses import dataclass, field

from kafka_collector import ClusterSnapshot

logger = logging.getLogger(__name__)


@dataclass
class NodeData:
    id: str
    type: str  # "topic" | "service" | "producer" | "consumer_group"
    data: dict = field(default_factory=dict)
    status: str = "ok"


@dataclass
class EdgeData:
    id: str
    source: str
    target: str
    data: dict = field(default_factory=dict)


@dataclass
class GraphDiff:
    nodes_added: list[dict] = field(default_factory=list)
    nodes_updated: list[dict] = field(default_factory=list)
    nodes_removed: list[str] = field(default_factory=list)
    edges_added: list[dict] = field(default_factory=list)
    edges_updated: list[dict] = field(default_factory=list)
    edges_removed: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    timestamp: float = 0.0

    def is_empty(self) -> bool:
        return not any([
            self.nodes_added, self.nodes_updated, self.nodes_removed,
            self.edges_added, self.edges_updated, self.edges_removed,
        ])

    def to_dict(self) -> dict:
        return {
            "type": "graph_diff",
            "ts": int(self.timestamp * 1000),
            "nodes": {
                "added": self.nodes_added,
                "updated": self.nodes_updated,
                "removed": self.nodes_removed,
            },
            "edges": {
                "added": self.edges_added,
                "updated": self.edges_updated,
                "removed": self.edges_removed,
            },
            "metrics": self.metrics,
        }


# Regex to strip instance suffixes from client.ids
# "payment-service-prod-01" → "payment-service"
_SUFFIX_RE = re.compile(r"[-_](prod|consumer|worker|instance)[-_]\d+$")
_TRAILING_ID_RE = re.compile(r"-[a-z0-9]{5,}$")


def _extract_service_name(client_id: str) -> str:
    """Extract base service name from a client.id."""
    name = _SUFFIX_RE.sub("", client_id)
    name = _TRAILING_ID_RE.sub("", name)
    return name


class GraphStateBuilder:
    """Builds a pipeline graph: Producer/Service → Topic → Service/Consumer."""

    def __init__(self, lag_warn_threshold: int = 1000, show_producers: bool = True):
        self.lag_warn_threshold = lag_warn_threshold
        self.show_producers = show_producers
        self._nodes: dict[str, NodeData] = {}
        self._edges: dict[str, EdgeData] = {}

    def get_snapshot(self) -> dict:
        nodes = [
            {"id": n.id, "type": n.type, "data": n.data, "status": n.status}
            for n in self._nodes.values()
        ]
        edges = [
            {"id": e.id, "source": e.source, "target": e.target, "data": e.data}
            for e in self._edges.values()
        ]
        return {
            "type": "graph_snapshot",
            "ts": int(time.time() * 1000),
            "nodes": {"added": nodes, "updated": [], "removed": []},
            "edges": {"added": edges, "updated": [], "removed": []},
            "metrics": self._build_metrics(),
        }

    def update(self, snapshot: ClusterSnapshot) -> GraphDiff:
        diff = GraphDiff(timestamp=snapshot.timestamp or time.time())
        new_nodes: dict[str, NodeData] = {}
        new_edges: dict[str, EdgeData] = {}

        # --- 1. Build topic nodes ---
        for topic_name, info in snapshot.topics.items():
            nid = f"topic-{topic_name}"
            new_nodes[nid] = NodeData(
                id=nid, type="topic",
                data={
                    "label": topic_name,
                    "partitions": info.partitions,
                    "msgPerSec": info.msg_per_sec,
                    "totalMessages": info.total_messages,
                },
                status=info.status,
            )

        # --- 2. Build service nodes from consumer groups ---
        # A "service" is a consumer group that also produces to other topics.
        # Detection: match the consumer group name (e.g. "payment-service")
        # against active-producer topics (e.g. "payments.processed").
        # The base name "payment" is extracted from the group name, then
        # matched against topic prefixes.

        service_produces: dict[str, set[str]] = {}  # group_id -> topics it produces to

        for gid, group in snapshot.consumer_groups.items():
            # Extract base name: "payment-service" → "payment"
            # "notification-service" → "notification"
            svc_base = gid.replace("-service", "").replace("-svc", "").replace("_service", "")

            for topic_name in snapshot.active_partitions:
                topic_prefix = topic_name.split(".")[0]
                # Match: "payment" ↔ "payments" (handle singular/plural)
                if (topic_prefix.rstrip("s").startswith(svc_base.rstrip("s")) or
                    svc_base.rstrip("s").startswith(topic_prefix.rstrip("s"))):
                    # Don't mark as producer for topics it already consumes
                    if topic_name not in group.subscribed_topics:
                        service_produces.setdefault(gid, set()).add(topic_name)

        # Identify standalone producers (topics with activity but no matching service)
        topics_with_known_producer: set[str] = set()
        for produces_set in service_produces.values():
            topics_with_known_producer |= produces_set

        for gid, group in snapshot.consumer_groups.items():
            lag_warning = group.total_lag > self.lag_warn_threshold
            produces_topics = list(service_produces.get(gid, set()))
            is_service = len(produces_topics) > 0

            nid = f"svc-{gid}" if is_service else f"cg-{gid}"
            node_type = "service" if is_service else "consumer_group"

            new_nodes[nid] = NodeData(
                id=nid, type=node_type,
                data={
                    "label": gid,
                    "members": group.members,
                    "totalLag": group.total_lag,
                    "lagWarning": lag_warning,
                    "perPartitionLag": group.per_partition_lag,
                    "consumes": group.subscribed_topics,
                    "produces": produces_topics,
                    "clientIds": group.member_client_ids,
                },
                status=group.status,
            )

            # Edges: topic → service/consumer (consumes)
            for topic_name in group.subscribed_topics:
                topic_nid = f"topic-{topic_name}"
                if topic_nid in new_nodes:
                    eid = f"edge-{topic_nid}-{nid}"
                    lag_for_topic = sum(
                        v for k, v in group.per_partition_lag.items()
                        if k.startswith(topic_name + "-")
                    )
                    new_edges[eid] = EdgeData(
                        id=eid, source=topic_nid, target=nid,
                        data={
                            "type": "consumes",
                            "active": True,
                            "lag": lag_for_topic,
                            "lagWarning": lag_for_topic > self.lag_warn_threshold,
                            "label": f"lag {lag_for_topic}" if lag_for_topic > 0 else "",
                        },
                    )

            # Edges: service → topic (produces)
            for topic_name in produces_topics:
                topic_nid = f"topic-{topic_name}"
                if topic_nid in new_nodes:
                    eid = f"edge-{nid}-{topic_nid}"
                    rate = snapshot.topics[topic_name].msg_per_sec if topic_name in snapshot.topics else 0
                    new_edges[eid] = EdgeData(
                        id=eid, source=nid, target=topic_nid,
                        data={
                            "type": "produces",
                            "active": rate > 0,
                            "msgPerSec": rate,
                            "label": f"{rate} msg/s" if rate > 0 else "",
                        },
                    )

        # --- 3. Standalone producer nodes for topics with no known service ---
        if self.show_producers:
            for topic_name in snapshot.active_partitions:
                if topic_name in topics_with_known_producer:
                    continue
                topic_nid = f"topic-{topic_name}"
                if topic_nid not in new_nodes:
                    continue
                pid = f"producer-{topic_name}"
                rate = snapshot.topics[topic_name].msg_per_sec if topic_name in snapshot.topics else 0
                new_nodes[pid] = NodeData(
                    id=pid, type="producer",
                    data={
                        "label": f"{topic_name} producer",
                        "inferred": True,
                        "msgPerSec": rate,
                    },
                )
                eid = f"edge-{pid}-{topic_nid}"
                new_edges[eid] = EdgeData(
                    id=eid, source=pid, target=topic_nid,
                    data={
                        "type": "produces",
                        "active": rate > 0,
                        "msgPerSec": rate,
                        "label": f"{rate} msg/s" if rate > 0 else "",
                    },
                )

        # --- Compute diff ---
        # Mark nodes that disappeared as inactive instead of removing them
        for nid, node in new_nodes.items():
            if nid not in self._nodes:
                diff.nodes_added.append(
                    {"id": node.id, "type": node.type, "data": node.data, "status": node.status}
                )
            elif self._nodes[nid].data != node.data or self._nodes[nid].status != node.status:
                diff.nodes_updated.append(
                    {"id": node.id, "data": node.data, "status": node.status}
                )

        for nid, old_node in self._nodes.items():
            if nid not in new_nodes:
                if old_node.status != "inactive":
                    # Mark as inactive instead of removing
                    diff.nodes_updated.append(
                        {"id": old_node.id, "data": old_node.data, "status": "inactive"}
                    )
                    # Keep the node in state as inactive
                    new_nodes[nid] = NodeData(
                        id=old_node.id, type=old_node.type,
                        data=old_node.data, status="inactive",
                    )
            else:
                # If it was inactive and now reappears, it's already in new_nodes with active status
                if self._nodes[nid].status == "inactive" and new_nodes[nid].status != "inactive":
                    # Make sure it shows up as updated
                    if not any(u["id"] == nid for u in diff.nodes_updated):
                        diff.nodes_updated.append(
                            {"id": new_nodes[nid].id, "data": new_nodes[nid].data, "status": new_nodes[nid].status}
                        )

        for eid, edge in new_edges.items():
            if eid not in self._edges:
                diff.edges_added.append(
                    {"id": edge.id, "source": edge.source, "target": edge.target, "data": edge.data}
                )
            elif self._edges[eid].data != edge.data:
                diff.edges_updated.append({"id": edge.id, "data": edge.data})

        for eid, old_edge in self._edges.items():
            if eid not in new_edges:
                if not old_edge.data.get("inactive"):
                    # Mark edge as inactive instead of removing
                    inactive_data = {**old_edge.data, "inactive": True, "active": False}
                    diff.edges_updated.append({"id": old_edge.id, "data": inactive_data})
                    new_edges[eid] = EdgeData(
                        id=old_edge.id, source=old_edge.source,
                        target=old_edge.target, data=inactive_data,
                    )
            else:
                # If edge was inactive and reappears, ensure it's marked active
                if self._edges[eid].data.get("inactive") and not new_edges[eid].data.get("inactive"):
                    if not any(u["id"] == eid for u in diff.edges_updated):
                        diff.edges_updated.append({"id": new_edges[eid].id, "data": new_edges[eid].data})

        diff.metrics = {
            f"topic-{name}": {"msgPerSec": info.msg_per_sec, "totalMessages": info.total_messages}
            for name, info in snapshot.topics.items()
        }

        self._nodes = new_nodes
        self._edges = new_edges
        return diff

    def _build_metrics(self) -> dict:
        return {
            nid: {"msgPerSec": n.data.get("msgPerSec", 0), "totalMessages": n.data.get("totalMessages", 0)}
            for nid, n in self._nodes.items() if n.type == "topic"
        }
