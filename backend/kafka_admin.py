"""KafkaAdmin — admin operations for topics, consumer groups, brokers, and message production."""

import logging
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import (
    KafkaError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
    GroupAuthorizationFailedError,
)

logger = logging.getLogger(__name__)


class KafkaAdmin:
    """Wraps kafka-python-ng for admin operations."""

    def __init__(
        self,
        bootstrap_servers: str,
        sasl_enabled: bool = False,
        sasl_username: str = "",
        sasl_password: str = "",
        ssl_enabled: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.sasl_enabled = sasl_enabled
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ssl_enabled = ssl_enabled
        self._admin: KafkaAdminClient | None = None

    def _build_kafka_config(self) -> dict:
        cfg = {"bootstrap_servers": self.bootstrap_servers}
        if self.sasl_enabled:
            cfg["security_protocol"] = "SASL_SSL" if self.ssl_enabled else "SASL_PLAINTEXT"
            cfg["sasl_mechanism"] = "SCRAM-SHA-512"
            cfg["sasl_plain_username"] = self.sasl_username
            cfg["sasl_plain_password"] = self.sasl_password
        elif self.ssl_enabled:
            cfg["security_protocol"] = "SSL"
        return cfg

    def connect(self):
        cfg = self._build_kafka_config()
        self._admin = KafkaAdminClient(**cfg)

    def close(self):
        if self._admin:
            try:
                self._admin.close()
            except Exception:
                pass

    def _get_admin(self) -> KafkaAdminClient:
        if not self._admin:
            raise RuntimeError("KafkaAdmin not connected")
        return self._admin

    # ── Topics ──────────────────────────────────────────────

    def list_topics(self) -> list[dict]:
        cfg = self._build_kafka_config()
        consumer = KafkaConsumer(**cfg)
        try:
            topic_names = consumer.topics()
            result = []
            for name in sorted(topic_names):
                parts = consumer.partitions_for_topic(name) or set()
                total_messages = 0
                try:
                    partitions = [TopicPartition(name, p) for p in parts]
                    if partitions:
                        end_offsets = consumer.end_offsets(partitions)
                        total_messages = sum(end_offsets.values())
                except Exception:
                    pass
                result.append({
                    "name": name,
                    "partitions": len(parts),
                    "replicationFactor": self._get_replication_factor(name),
                    "totalMessages": total_messages,
                })
            return result
        finally:
            consumer.close()

    def _get_replication_factor(self, topic: str) -> int:
        try:
            admin = self._get_admin()
            desc = admin.describe_topics([topic])
            if desc and desc[0].get("partitions"):
                return len(desc[0]["partitions"][0].get("replicas", []))
        except Exception:
            pass
        return -1

    def get_topic_detail(self, topic: str) -> dict:
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        # Config entries
        from kafka.admin import ConfigResource, ConfigResourceType
        resource = ConfigResource(ConfigResourceType.TOPIC, topic)
        configs_result = admin.describe_configs([resource])
        config_entries = {}
        if configs_result:
            for entry in configs_result[0].resources:
                # entry is (error_code, error_message, resource_type, resource_name, config_entries)
                if len(entry) >= 5:
                    for ce in entry[4]:
                        config_entries[ce[0]] = ce[1]

        # Partition info
        desc = admin.describe_topics([topic])
        partitions = []
        if desc and desc[0].get("partitions"):
            consumer = KafkaConsumer(**cfg)
            try:
                for p in desc[0]["partitions"]:
                    pid = p["partition"]
                    tp = TopicPartition(topic, pid)
                    try:
                        end_offset = consumer.end_offsets([tp]).get(tp, 0)
                    except Exception:
                        end_offset = 0
                    partitions.append({
                        "partition": pid,
                        "leader": p.get("leader"),
                        "replicas": p.get("replicas", []),
                        "isr": p.get("isr", []),
                        "endOffset": end_offset,
                    })
            finally:
                consumer.close()

        return {
            "name": topic,
            "config": config_entries,
            "partitions": partitions,
        }

    def create_topic(self, name: str, partitions: int = 1,
                     replication_factor: int = 1, configs: dict | None = None) -> dict:
        admin = self._get_admin()
        topic = NewTopic(
            name=name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            topic_configs=configs or {},
        )
        try:
            admin.create_topics([topic])
            return {"success": True, "topic": name}
        except TopicAlreadyExistsError:
            return {"success": False, "error": f"Topic '{name}' already exists"}

    def delete_topic(self, topic: str) -> dict:
        admin = self._get_admin()
        try:
            admin.delete_topics([topic])
            return {"success": True, "topic": topic}
        except UnknownTopicOrPartitionError:
            return {"success": False, "error": f"Topic '{topic}' not found"}

    # ── Consumer Groups ─────────────────────────────────────

    def list_consumer_groups(self) -> list[dict]:
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        try:
            groups = admin.list_consumer_groups()
        except GroupAuthorizationFailedError:
            return []

        result = []
        for group_id, protocol_type in groups:
            info = {"groupId": group_id, "status": "Unknown", "members": 0, "totalLag": 0, "topics": []}
            try:
                described = admin.describe_consumer_groups([group_id])
                if described:
                    desc = described[0]
                    if hasattr(desc, "state"):
                        info["status"] = desc.state
                    if hasattr(desc, "members"):
                        info["members"] = len(desc.members)

                # Calculate lag
                offsets = admin.list_consumer_group_offsets(group_id)
                consumer = KafkaConsumer(**cfg)
                try:
                    total_lag = 0
                    topics = set()
                    for tp, offset_meta in offsets.items():
                        topics.add(tp.topic)
                        try:
                            end_offsets = consumer.end_offsets([tp])
                            end = end_offsets.get(tp, 0)
                            lag = max(0, end - offset_meta.offset)
                            total_lag += lag
                        except Exception:
                            pass
                    info["totalLag"] = total_lag
                    info["topics"] = sorted(topics)
                finally:
                    consumer.close()
            except Exception as e:
                logger.debug(f"Failed to describe group {group_id}: {e}")

            result.append(info)
        return result

    def get_consumer_group_detail(self, group_id: str) -> dict:
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        described = admin.describe_consumer_groups([group_id])
        members = []
        if described and hasattr(described[0], "members"):
            for m in described[0].members:
                member = {
                    "memberId": getattr(m, "member_id", ""),
                    "clientId": getattr(m, "client_id", ""),
                    "clientHost": getattr(m, "client_host", ""),
                    "partitions": [],
                }
                if hasattr(m, "member_assignment") and m.member_assignment:
                    try:
                        from kafka.protocol.group import MemberAssignment
                        assignment = MemberAssignment.decode(m.member_assignment)
                        if hasattr(assignment, "assignment"):
                            for topic, parts in assignment.assignment:
                                for p in parts:
                                    member["partitions"].append(f"{topic}-{p}")
                    except Exception:
                        pass
                members.append(member)

        # Per-partition offsets and lag
        offsets_data = []
        try:
            offsets = admin.list_consumer_group_offsets(group_id)
            consumer = KafkaConsumer(**cfg)
            try:
                for tp, offset_meta in offsets.items():
                    try:
                        end_offsets = consumer.end_offsets([tp])
                        end = end_offsets.get(tp, 0)
                    except Exception:
                        end = 0
                    lag = max(0, end - offset_meta.offset)
                    offsets_data.append({
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "currentOffset": offset_meta.offset,
                        "endOffset": end,
                        "lag": lag,
                    })
            finally:
                consumer.close()
        except Exception as e:
            logger.debug(f"Failed to get offsets for group {group_id}: {e}")

        state = "Unknown"
        if described and hasattr(described[0], "state"):
            state = described[0].state

        return {
            "groupId": group_id,
            "state": state,
            "members": members,
            "offsets": offsets_data,
        }

    def reset_offsets(self, group_id: str, strategy: str = "latest", topic: str | None = None) -> dict:
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        # Get current offsets to know which topic-partitions to reset
        offsets = admin.list_consumer_group_offsets(group_id)
        tps_to_reset = []
        for tp in offsets:
            if topic and tp.topic != topic:
                continue
            tps_to_reset.append(tp)

        if not tps_to_reset:
            return {"success": False, "error": "No partitions found to reset"}

        consumer = KafkaConsumer(**cfg)
        try:
            if strategy == "earliest":
                new_offsets = consumer.beginning_offsets(tps_to_reset)
            else:
                new_offsets = consumer.end_offsets(tps_to_reset)
        finally:
            consumer.close()

        from kafka.structs import OffsetAndMetadata
        offset_map = {tp: OffsetAndMetadata(offset, "") for tp, offset in new_offsets.items()}

        try:
            admin.alter_consumer_group_offsets(group_id, offset_map)
            return {"success": True, "partitionsReset": len(offset_map)}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Brokers ─────────────────────────────────────────────

    def list_brokers(self) -> list[dict]:
        admin = self._get_admin()
        cluster_meta = admin.describe_cluster()
        brokers = []
        controller_id = cluster_meta.get("controller_id")
        for broker in cluster_meta.get("brokers", []):
            brokers.append({
                "id": broker["node_id"],
                "host": broker["host"],
                "port": broker["port"],
                "rack": broker.get("rack"),
                "isController": broker["node_id"] == controller_id,
            })
        return brokers

    def get_cluster_info(self) -> dict:
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        cluster_meta = admin.describe_cluster()
        consumer = KafkaConsumer(**cfg)
        try:
            topic_count = len(consumer.topics())
        finally:
            consumer.close()

        try:
            groups = admin.list_consumer_groups()
            group_count = len(groups)
        except Exception:
            group_count = 0

        return {
            "clusterId": cluster_meta.get("cluster_id", ""),
            "controllerId": cluster_meta.get("controller_id"),
            "brokerCount": len(cluster_meta.get("brokers", [])),
            "topicCount": topic_count,
            "consumerGroupCount": group_count,
        }

    # ── Produce ─────────────────────────────────────────────

    def produce_message(self, topic: str, value: str, key: str | None = None,
                        headers: dict | None = None, partition: int | None = None) -> dict:
        cfg = self._build_kafka_config()
        producer = KafkaProducer(
            **cfg,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: v.encode("utf-8"),
        )
        try:
            kafka_headers = [(k, v.encode("utf-8") if v else None) for k, v in (headers or {}).items()]
            kwargs: dict = {"topic": topic, "value": value}
            if key is not None:
                kwargs["key"] = key
            if partition is not None:
                kwargs["partition"] = partition
            if kafka_headers:
                kwargs["headers"] = kafka_headers

            future = producer.send(**kwargs)
            record_meta = future.get(timeout=10)
            return {
                "success": True,
                "topic": record_meta.topic,
                "partition": record_meta.partition,
                "offset": record_meta.offset,
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            producer.close()
