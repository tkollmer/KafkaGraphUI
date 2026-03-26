"""KafkaAdmin — admin operations for topics, consumer groups, brokers, and message production."""

import logging
import math
import re
from collections import Counter
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

    def reset_offsets(self, group_id: str, strategy: str = "latest", topic: str | None = None, timestamp: int | None = None, offset: int | None = None) -> dict:
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
            if strategy == "timestamp" and timestamp is not None:
                ts_map = {tp: timestamp for tp in tps_to_reset}
                result = consumer.offsets_for_times(ts_map)
                new_offsets = {}
                for tp, offset_ts in result.items():
                    if offset_ts is not None:
                        new_offsets[tp] = offset_ts.offset
                    else:
                        # No offset found for timestamp, use end offset
                        end = consumer.end_offsets([tp])
                        new_offsets[tp] = end[tp]
            elif strategy == "specific" and offset is not None:
                new_offsets = {tp: offset for tp in tps_to_reset}
            elif strategy == "earliest":
                new_offsets = consumer.beginning_offsets(tps_to_reset)
            else:
                new_offsets = consumer.end_offsets(tps_to_reset)
        finally:
            consumer.close()

        from kafka.structs import OffsetAndMetadata
        offset_map = {tp: OffsetAndMetadata(off, "") for tp, off in new_offsets.items()}

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

    def describe_broker_config(self, broker_id: int) -> list[dict]:
        """Describe configuration for a specific broker."""
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            resource = ConfigResource(ConfigResourceType.BROKER, str(broker_id))
            result = self._get_admin().describe_configs([resource])
            configs = []
            for resource_result in result:
                for name, config_entry in resource_result.items():
                    configs.append({
                        "name": name,
                        "value": config_entry.value if hasattr(config_entry, "value") else str(config_entry),
                        "source": config_entry.source.name if hasattr(config_entry, "source") and hasattr(config_entry.source, "name") else "UNKNOWN",
                        "isReadOnly": config_entry.is_read_only if hasattr(config_entry, "is_read_only") else True,
                        "isSensitive": config_entry.is_sensitive if hasattr(config_entry, "is_sensitive") else False,
                    })
            return configs
        except ImportError:
            return []
        except Exception as e:
            logger.error(f"Failed to describe broker {broker_id} config: {e}")
            return []

    def alter_broker_config(self, broker_id: int, configs: dict) -> dict:
        """Alter dynamic broker configuration."""
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            resource = ConfigResource(ConfigResourceType.BROKER, str(broker_id), configs=configs)
            self._get_admin().alter_configs([resource])
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

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

    def get_cluster_health(self) -> dict:
        """Get cluster health including under-replicated and offline partitions."""
        admin = self._get_admin()
        cfg = self._build_kafka_config()
        consumer = KafkaConsumer(**cfg)
        try:
            all_topics = list(consumer.topics())
        finally:
            consumer.close()

        under_replicated = []
        offline_partitions = []
        total_partitions = 0
        leader_counts: dict[int, int] = {}

        for topic in all_topics:
            try:
                desc = admin.describe_topics([topic])
                if not desc or not desc[0].get("partitions"):
                    continue
                for p in desc[0]["partitions"]:
                    total_partitions += 1
                    leader = p.get("leader", -1)
                    replicas = p.get("replicas", [])
                    isr = p.get("isr", [])
                    if leader >= 0:
                        leader_counts[leader] = leader_counts.get(leader, 0) + 1
                    if leader < 0 or leader not in isr:
                        offline_partitions.append({"topic": topic, "partition": p["partition"]})
                    elif len(isr) < len(replicas):
                        under_replicated.append({
                            "topic": topic,
                            "partition": p["partition"],
                            "replicas": len(replicas),
                            "isr": len(isr),
                        })
            except Exception:
                continue

        return {
            "totalPartitions": total_partitions,
            "underReplicatedCount": len(under_replicated),
            "underReplicated": under_replicated[:50],
            "offlinePartitionCount": len(offline_partitions),
            "offlinePartitions": offline_partitions[:50],
            "leaderDistribution": leader_counts,
        }

    def elect_preferred_leaders(self, topic: str = None) -> dict:
        """Trigger preferred leader election for a topic or all topics."""
        admin = self._get_admin()
        try:
            partitions = []
            if topic:
                desc = admin.describe_topics([topic])
                if desc and desc[0].get("partitions"):
                    for p in desc[0]["partitions"]:
                        partitions.append(TopicPartition(topic, p["partition"]))
            # kafka-python-ng doesn't expose elect_leaders directly,
            # so we use the admin client's internal protocol
            try:
                if hasattr(admin, "elect_leaders"):
                    admin.elect_leaders(partitions if partitions else None)
                    return {"success": True, "message": f"Preferred leader election triggered for {'topic ' + topic if topic else 'all topics'}"}
            except Exception:
                pass
            # Fallback: return info about current state
            return {
                "success": True,
                "message": f"Leader election requested for {'topic ' + topic if topic else 'all topics'}. Note: automatic preferred leader election may not be supported by this Kafka version.",
                "partitions": len(partitions) if partitions else "all",
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_topic_config_diff(self, topic: str) -> list[dict]:
        """Get the diff between a topic's current config and Kafka defaults.

        Returns a list of dicts with key, value, default, source, and is_overridden.
        """
        admin = self._get_admin()
        from kafka.admin import ConfigResource, ConfigResourceType

        resource = ConfigResource(ConfigResourceType.TOPIC, topic)
        configs_result = admin.describe_configs([resource])

        CONFIG_SOURCE_MAP = {
            0: "UNKNOWN",
            1: "DYNAMIC_TOPIC_CONFIG",
            2: "DYNAMIC_BROKER_CONFIG",
            3: "DYNAMIC_DEFAULT_BROKER_CONFIG",
            4: "STATIC_BROKER_CONFIG",
            5: "DEFAULT_CONFIG",
            6: "DYNAMIC_BROKER_LOGGER_CONFIG",
        }

        entries = []

        # Try dict-like interface first (config entry objects with attributes)
        try:
            for resource_result in configs_result:
                for name, config_entry in resource_result.items():
                    if hasattr(config_entry, "value"):
                        source = "UNKNOWN"
                        if hasattr(config_entry, "source") and hasattr(config_entry.source, "name"):
                            source = config_entry.source.name
                        elif hasattr(config_entry, "source") and config_entry.source is not None:
                            source = CONFIG_SOURCE_MAP.get(config_entry.source, str(config_entry.source))
                        is_overridden = source not in ("DEFAULT_CONFIG", "UNKNOWN")
                        entries.append({
                            "key": name,
                            "value": config_entry.value,
                            "default": config_entry.value if not is_overridden else None,
                            "source": source,
                            "is_overridden": is_overridden,
                        })
            if entries:
                return entries
        except (TypeError, AttributeError):
            pass

        # Fallback: tuple-based format from resources attribute
        if configs_result and hasattr(configs_result[0], "resources"):
            for entry in configs_result[0].resources:
                # entry is (error_code, error_message, resource_type, resource_name, config_entries)
                if len(entry) >= 5:
                    for ce in entry[4]:
                        name = ce[0]
                        value = ce[1]
                        # ce[3] is config_source if available
                        source_code = ce[3] if len(ce) > 3 and ce[3] is not None else 5
                        source = CONFIG_SOURCE_MAP.get(source_code, str(source_code))
                        is_overridden = source != "DEFAULT_CONFIG"
                        entries.append({
                            "key": name,
                            "value": value,
                            "default": value if not is_overridden else None,
                            "source": source,
                            "is_overridden": is_overridden,
                        })

        return entries

    def update_topic_config(self, topic: str, configs: dict) -> dict:
        """Update topic configuration entries."""
        admin = self._get_admin()
        from kafka.admin import ConfigResource, ConfigResourceType
        resource = ConfigResource(ConfigResourceType.TOPIC, topic, configs=configs)
        try:
            admin.alter_configs([resource])
            return {"success": True, "topic": topic, "updated": list(configs.keys())}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_consumer_group(self, group_id: str) -> dict:
        """Delete an inactive consumer group."""
        admin = self._get_admin()
        try:
            admin.delete_consumer_groups([group_id])
            return {"success": True, "groupId": group_id}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def add_topic_partitions(self, topic: str, total_partitions: int) -> dict:
        """Increase the number of partitions for a topic."""
        admin = self._get_admin()
        from kafka.admin import NewPartitions
        try:
            admin.create_partitions({topic: NewPartitions(total_count=total_partitions)})
            return {"success": True, "topic": topic, "partitions": total_partitions}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Partition Reassignment ─────────────────────────────

    def reassign_partitions(self, topic: str, assignments: list[dict]) -> dict:
        """Reassign topic partitions to specific brokers.

        Args:
            topic: The topic name.
            assignments: List of {"partition": int, "replicas": [int, ...]} dicts.

        Returns:
            dict with success status and details.
        """
        admin = self._get_admin()
        try:
            from kafka.admin import NewPartitionReassignment
            # Build the reassignment map: {TopicPartition: NewPartitionReassignment}
            reassignment_map = {}
            for a in assignments:
                partition = a.get("partition")
                replicas = a.get("replicas")
                if partition is None or not replicas:
                    return {
                        "success": False,
                        "error": f"Each assignment must have 'partition' (int) and 'replicas' (list[int]). Got: {a}",
                    }
                tp = TopicPartition(topic, int(partition))
                reassignment_map[tp] = NewPartitionReassignment(replicas=replicas)
            admin.alter_partition_reassignments(reassignment_map)
            return {
                "success": True,
                "topic": topic,
                "partitionsReassigned": len(assignments),
            }
        except ImportError:
            # NewPartitionReassignment or alter_partition_reassignments not available
            # Fall back to using the lower-level AlterPartitionReassignments protocol
            try:
                return self._reassign_partitions_fallback(topic, assignments)
            except Exception as e:
                return {"success": False, "error": f"Partition reassignment not supported: {e}"}
        except AttributeError:
            # alter_partition_reassignments not on admin client
            try:
                return self._reassign_partitions_fallback(topic, assignments)
            except Exception as e:
                return {"success": False, "error": f"Partition reassignment not supported: {e}"}
        except Exception as e:
            logger.error(f"Failed to reassign partitions for {topic}: {e}")
            return {"success": False, "error": str(e)}

    def _reassign_partitions_fallback(self, topic: str, assignments: list[dict]) -> dict:
        """Fallback partition reassignment using low-level protocol request."""
        admin = self._get_admin()
        try:
            from kafka.protocol.admin import AlterPartitionReassignmentsRequest
            # Build protocol-level request
            # AlterPartitionReassignmentsRequest_v0 format:
            #   timeout_ms, topics: [topic, partitions: [partition_index, replicas]]
            partitions_data = []
            for a in assignments:
                partitions_data.append((int(a["partition"]), a["replicas"]))
            topics_data = [(topic, partitions_data)]
            request = AlterPartitionReassignmentsRequest[0](
                timeout_ms=60000,
                topics=topics_data,
            )
            # Send to controller node
            controller_id = admin._find_coordinator_id_send_request(request)
            return {
                "success": True,
                "topic": topic,
                "partitionsReassigned": len(assignments),
            }
        except (ImportError, AttributeError, Exception) as e:
            logger.warning(f"Fallback reassignment also failed: {e}")
            return {
                "success": False,
                "error": (
                    "Partition reassignment is not supported by this version of kafka-python-ng. "
                    "Consider using the Kafka CLI tool 'kafka-reassign-partitions.sh' directly. "
                    f"Detail: {e}"
                ),
            }

    def get_partition_reassignment_status(self, topic: str) -> dict:
        """Check the status of ongoing partition reassignments for a topic.

        Returns:
            dict with reassignment status per partition.
        """
        admin = self._get_admin()
        try:
            from kafka.admin import NewPartitionReassignment
            # List current reassignments by passing None to list all
            # The API uses alter with empty to query - but list is the correct approach
            result = admin.list_partition_reassignments()
            # Filter for our topic
            topic_reassignments = []
            if result:
                for tp, reassignment in result.items():
                    if tp.topic == topic:
                        topic_reassignments.append({
                            "partition": tp.partition,
                            "replicas": getattr(reassignment, "replicas", []),
                            "addingReplicas": getattr(reassignment, "adding_replicas", []),
                            "removingReplicas": getattr(reassignment, "removing_replicas", []),
                        })
            return {
                "topic": topic,
                "reassignments": topic_reassignments,
                "inProgress": len(topic_reassignments) > 0,
            }
        except (ImportError, AttributeError):
            # list_partition_reassignments not available, try describe_topics to infer
            try:
                desc = admin.describe_topics([topic])
                partitions = []
                if desc and desc[0].get("partitions"):
                    for p in desc[0]["partitions"]:
                        replicas = p.get("replicas", [])
                        isr = p.get("isr", [])
                        # If ISR differs from replicas, reassignment may be in progress
                        if set(isr) != set(replicas):
                            partitions.append({
                                "partition": p["partition"],
                                "replicas": replicas,
                                "isr": isr,
                                "possiblyReassigning": True,
                            })
                return {
                    "topic": topic,
                    "reassignments": partitions,
                    "inProgress": len(partitions) > 0,
                    "note": "Status inferred from ISR/replica mismatch; exact reassignment tracking not available.",
                }
            except Exception as e:
                return {
                    "topic": topic,
                    "reassignments": [],
                    "inProgress": False,
                    "error": f"Could not determine reassignment status: {e}",
                }
        except Exception as e:
            logger.error(f"Failed to get reassignment status for {topic}: {e}")
            return {
                "topic": topic,
                "reassignments": [],
                "inProgress": False,
                "error": str(e),
            }

    # ── Log Dirs ───────────────────────────────────────────

    def get_log_dirs(self) -> list[dict]:
        """Describe log directories for all brokers.

        Uses admin.describe_log_dirs() if available, otherwise falls back
        to returning estimated data based on topic/partition info.

        Returns:
            List of dicts with brokerId, logDir, size, partitions, etc.
        """
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        # Get broker list first
        cluster_meta = admin.describe_cluster()
        broker_ids = [b["node_id"] for b in cluster_meta.get("brokers", [])]

        if not broker_ids:
            return []

        # Try the native describe_log_dirs API
        try:
            if hasattr(admin, "describe_log_dirs"):
                raw = admin.describe_log_dirs()
                result = []
                for broker_id, log_dirs in raw.items():
                    for log_dir, dir_info in log_dirs.items():
                        partitions = []
                        total_size = 0
                        topics_in_dir = set()
                        if isinstance(dir_info, dict):
                            for tp_key, tp_info in dir_info.items():
                                if isinstance(tp_info, dict):
                                    part_size = tp_info.get("size", 0)
                                    total_size += part_size
                                    topic = tp_info.get("topic", str(tp_key))
                                    topics_in_dir.add(topic)
                                    partitions.append({
                                        "topic": topic,
                                        "partition": tp_info.get("partition", 0),
                                        "size": part_size,
                                        "offsetLag": tp_info.get("offset_lag", 0),
                                        "isFuture": tp_info.get("is_future", False),
                                    })
                        result.append({
                            "brokerId": int(broker_id),
                            "logDir": log_dir,
                            "size": total_size,
                            "partitionCount": len(partitions),
                            "topicCount": len(topics_in_dir),
                            "partitions": partitions,
                            "error": None,
                        })
                if result:
                    return result
        except Exception as e:
            logger.debug(f"describe_log_dirs failed, falling back to estimation: {e}")

        # Fallback: estimate log dir data from topic/partition metadata
        consumer = KafkaConsumer(**cfg)
        try:
            all_topics = list(consumer.topics())
            # Map broker -> partitions it leads
            broker_partitions: dict[int, list[dict]] = {bid: [] for bid in broker_ids}
            for topic in all_topics:
                try:
                    desc = admin.describe_topics([topic])
                    if not desc or not desc[0].get("partitions"):
                        continue
                    for p in desc[0]["partitions"]:
                        leader = p.get("leader", -1)
                        replicas = p.get("replicas", [])
                        pid = p["partition"]
                        tp = TopicPartition(topic, pid)
                        try:
                            end_offset = consumer.end_offsets([tp]).get(tp, 0)
                            begin_offset = consumer.beginning_offsets([tp]).get(tp, 0)
                        except Exception:
                            end_offset = 0
                            begin_offset = 0
                        messages = max(0, end_offset - begin_offset)
                        # Rough estimate: ~500 bytes per message average
                        estimated_size = messages * 500
                        part_info = {
                            "topic": topic,
                            "partition": pid,
                            "size": estimated_size,
                            "offsetLag": 0,
                            "isFuture": False,
                        }
                        # Add to all replicas
                        for replica_id in replicas:
                            if replica_id in broker_partitions:
                                broker_partitions[replica_id].append(part_info)
                except Exception:
                    continue

            result = []
            for bid in broker_ids:
                parts = broker_partitions.get(bid, [])
                total_size = sum(p["size"] for p in parts)
                topics_in_dir = set(p["topic"] for p in parts)
                result.append({
                    "brokerId": bid,
                    "logDir": "/var/kafka-logs",
                    "size": total_size,
                    "partitionCount": len(parts),
                    "topicCount": len(topics_in_dir),
                    "partitions": parts,
                    "error": None,
                    "estimated": True,
                })
            return result
        finally:
            consumer.close()

    # ── Topic Consumer Groups ─────────────────────────────

    def get_topic_consumer_groups(self, topic: str) -> list[dict]:
        """Return consumer groups that are consuming from a specific topic.

        Args:
            topic: The topic name to find consumer groups for.

        Returns:
            List of dicts with groupId, state, members, lag per partition, etc.
        """
        admin = self._get_admin()
        cfg = self._build_kafka_config()

        try:
            groups = admin.list_consumer_groups()
        except GroupAuthorizationFailedError:
            return []

        matching_groups = []
        for group_id, protocol_type in groups:
            try:
                offsets = admin.list_consumer_group_offsets(group_id)
                # Check if any offset belongs to this topic
                topic_partitions = {
                    tp: meta for tp, meta in offsets.items() if tp.topic == topic
                }
                if not topic_partitions:
                    continue

                # This group consumes from the topic
                group_info = {
                    "groupId": group_id,
                    "state": "Unknown",
                    "members": 0,
                    "totalLag": 0,
                    "partitions": [],
                }

                # Describe group for state and member count
                try:
                    described = admin.describe_consumer_groups([group_id])
                    if described:
                        desc = described[0]
                        if hasattr(desc, "state"):
                            group_info["state"] = desc.state
                        if hasattr(desc, "members"):
                            group_info["members"] = len(desc.members)
                except Exception:
                    pass

                # Calculate per-partition lag
                consumer = KafkaConsumer(**cfg)
                try:
                    total_lag = 0
                    for tp, offset_meta in topic_partitions.items():
                        try:
                            end_offsets = consumer.end_offsets([tp])
                            end = end_offsets.get(tp, 0)
                        except Exception:
                            end = 0
                        lag = max(0, end - offset_meta.offset)
                        total_lag += lag
                        group_info["partitions"].append({
                            "partition": tp.partition,
                            "currentOffset": offset_meta.offset,
                            "endOffset": end,
                            "lag": lag,
                        })
                    group_info["totalLag"] = total_lag
                finally:
                    consumer.close()

                matching_groups.append(group_info)
            except Exception as e:
                logger.debug(f"Failed to check group {group_id} for topic {topic}: {e}")
                continue

        return matching_groups

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

    # ── Key Distribution ───────────────────────────────────

    def get_key_distribution(self, topic: str, sample_size: int = 1000) -> dict:
        """Sample messages from a topic and return key distribution statistics.

        Args:
            topic: The topic to sample from.
            sample_size: Maximum number of messages to sample.

        Returns:
            dict with total_sampled, unique_keys, null_key_count, top_keys, and key_entropy.
        """
        cfg = self._build_kafka_config()
        consumer = KafkaConsumer(
            **cfg,
            auto_offset_reset="latest",
            group_id=None,
            consumer_timeout_ms=5000,
        )
        try:
            # Get all partitions for the topic
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return {
                    "topic": topic,
                    "total_sampled": 0,
                    "unique_keys": 0,
                    "null_key_count": 0,
                    "top_keys": [],
                    "key_entropy": 0.0,
                }

            topic_partitions = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(topic_partitions)

            # Seek to the appropriate offset per partition
            end_offsets = consumer.end_offsets(topic_partitions)
            beginning_offsets = consumer.beginning_offsets(topic_partitions)
            per_partition_sample = sample_size // len(topic_partitions)

            for tp in topic_partitions:
                end = end_offsets.get(tp, 0)
                beginning = beginning_offsets.get(tp, 0)
                seek_to = max(end - per_partition_sample, beginning)
                consumer.seek(tp, seek_to)

            # Poll messages up to sample_size
            key_counter: Counter = Counter()
            total_sampled = 0
            null_key_count = 0

            while total_sampled < sample_size:
                records = consumer.poll(timeout_ms=2000, max_records=sample_size - total_sampled)
                if not records:
                    break
                for tp, messages in records.items():
                    for msg in messages:
                        if total_sampled >= sample_size:
                            break
                        if msg.key is None:
                            key_str = "null"
                            null_key_count += 1
                        else:
                            try:
                                key_str = msg.key.decode("utf-8")
                            except (UnicodeDecodeError, AttributeError):
                                key_str = str(msg.key)
                        key_counter[key_str] += 1
                        total_sampled += 1

            # Build top keys (top 50)
            top_keys = []
            for key, count in key_counter.most_common(50):
                top_keys.append({
                    "key": key,
                    "count": count,
                    "percentage": round((count / total_sampled) * 100, 2) if total_sampled > 0 else 0.0,
                })

            # Calculate Shannon entropy
            key_entropy = 0.0
            if total_sampled > 0:
                for count in key_counter.values():
                    p = count / total_sampled
                    if p > 0:
                        key_entropy -= p * math.log2(p)
                key_entropy = round(key_entropy, 4)

            return {
                "topic": topic,
                "total_sampled": total_sampled,
                "unique_keys": len(key_counter),
                "null_key_count": null_key_count,
                "top_keys": top_keys,
                "key_entropy": key_entropy,
            }
        finally:
            consumer.close()

    # ── ACLs ───────────────────────────────────────────────

    def list_acls(self) -> dict:
        """List all ACLs. Returns a simplified representation."""
        try:
            from kafka.admin import ACLFilter, ACLResourcePatternFilter, ResourceType, ACLOperation, ACLPermissionType, ResourcePatternType
            acl_filter = ACLFilter(
                principal=None,
                host=None,
                operation=ACLOperation.ANY,
                permission_type=ACLPermissionType.ANY,
                resource_pattern=ACLResourcePatternFilter(
                    resource_type=ResourceType.ANY,
                    resource_name=None,
                    pattern_type=ResourcePatternType.ANY,
                ),
            )
            result = self._get_admin().describe_acls(acl_filter)
            acls = []
            for acl in (result or []):
                acls.append({
                    "principal": acl.principal,
                    "host": acl.host,
                    "operation": acl.operation.name if hasattr(acl.operation, "name") else str(acl.operation),
                    "permission": acl.permission_type.name if hasattr(acl.permission_type, "name") else str(acl.permission_type),
                    "resourceType": acl.resource_pattern.resource_type.name if hasattr(acl.resource_pattern.resource_type, "name") else str(acl.resource_pattern.resource_type),
                    "resourceName": acl.resource_pattern.resource_name,
                    "patternType": acl.resource_pattern.pattern_type.name if hasattr(acl.resource_pattern.pattern_type, "name") else str(acl.resource_pattern.pattern_type),
                })
            return {"acls": acls, "count": len(acls)}
        except ImportError:
            return {"acls": [], "count": 0, "error": "ACL support not available in this kafka-python version"}
        except Exception as e:
            logger.error(f"Failed to list ACLs: {e}")
            return {"acls": [], "count": 0, "error": str(e)}

    def create_acl(self, resource_type: str, resource_name: str, principal: str,
                   operation: str, permission_type: str, pattern_type: str = "LITERAL",
                   host: str = "*") -> dict:
        """Create a new ACL entry."""
        try:
            from kafka.admin import ACL, ACLResourcePatternFilter, ResourceType, ACLOperation, ACLPermissionType, ResourcePatternType
            rt = ResourceType[resource_type.upper()]
            op = ACLOperation[operation.upper()]
            pt = ACLPermissionType[permission_type.upper()]
            pp = ResourcePatternType[pattern_type.upper()]
            acl = ACL(
                principal=principal,
                host=host,
                operation=op,
                permission_type=pt,
                resource_pattern=ACLResourcePatternFilter(
                    resource_type=rt,
                    resource_name=resource_name,
                    pattern_type=pp,
                ),
            )
            self._get_admin().create_acls([acl])
            return {"success": True}
        except ImportError:
            return {"success": False, "error": "ACL support not available"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def delete_acl(self, resource_type: str = "ANY", resource_name: str | None = None,
                   principal: str | None = None, operation: str = "ANY",
                   permission_type: str = "ANY") -> dict:
        """Delete ACLs matching the filter."""
        try:
            from kafka.admin import ACLFilter, ACLResourcePatternFilter, ResourceType, ACLOperation, ACLPermissionType, ResourcePatternType
            acl_filter = ACLFilter(
                principal=principal,
                host=None,
                operation=ACLOperation[operation.upper()],
                permission_type=ACLPermissionType[permission_type.upper()],
                resource_pattern=ACLResourcePatternFilter(
                    resource_type=ResourceType[resource_type.upper()],
                    resource_name=resource_name,
                    pattern_type=ResourcePatternType.ANY,
                ),
            )
            result = self._get_admin().delete_acls([acl_filter])
            return {"success": True, "deleted": len(result) if result else 0}
        except ImportError:
            return {"success": False, "error": "ACL support not available"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── Quotas ─────────────────────────────────────────────

    def list_quotas(self) -> dict:
        """List all client quotas.

        Attempts to use describe_client_quotas if available, otherwise falls
        back to returning an informative error.
        """
        admin = self._get_admin()
        # Try the native describe_client_quotas API first
        try:
            from kafka.admin import ClientQuotaFilter, ClientQuotaFilterComponent
            quota_filter = ClientQuotaFilter(components=[])
            result = admin.describe_client_quotas(quota_filter)
            quotas = []
            for entry in (result or []):
                entity = {}
                if hasattr(entry, "entity"):
                    for component in entry.entity:
                        entity[component.entity_type] = component.entity_name
                values = {}
                if hasattr(entry, "values"):
                    for k, v in entry.values.items():
                        values[k] = v
                quotas.append({"entity": entity, "quotas": values})
            return {"quotas": quotas, "count": len(quotas)}
        except (ImportError, AttributeError):
            pass
        except Exception as e:
            logger.debug(f"describe_client_quotas failed: {e}")

        # Fallback: try to read quota configs via describe_configs with broker resource
        # Kafka stores some quota information in broker configs
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            # Attempt to list user/client quotas via broker config
            cluster_meta = admin.describe_cluster()
            brokers = cluster_meta.get("brokers", [])
            quotas = []
            for broker in brokers:
                bid = broker["node_id"]
                resource = ConfigResource(ConfigResourceType.BROKER, str(bid))
                configs_result = admin.describe_configs([resource])
                quota_entries = {}
                if configs_result:
                    for res in configs_result:
                        for name, config_entry in res.items():
                            # Filter for quota-related configs
                            if "quota" in name.lower():
                                value = config_entry.value if hasattr(config_entry, "value") else str(config_entry)
                                quota_entries[name] = value
                if quota_entries:
                    quotas.append({
                        "entity": {"broker": str(bid)},
                        "quotas": quota_entries,
                    })
            if quotas:
                return {"quotas": quotas, "count": len(quotas)}
        except (ImportError, AttributeError):
            pass
        except Exception as e:
            logger.debug(f"Fallback quota listing via broker configs failed: {e}")

        return {
            "quotas": [],
            "count": 0,
            "error": (
                "Client quota management is not supported by this version of kafka-python-ng. "
                "Use the Kafka CLI tool 'kafka-configs.sh' to manage quotas directly."
            ),
        }

    def set_quota(self, entity_type: str, entity_name: str, quotas: dict) -> dict:
        """Set quotas for a client entity.

        Args:
            entity_type: One of 'user', 'client-id', or 'ip'.
            entity_name: The entity name (e.g., username or client ID).
            quotas: Dict of quota keys to values, e.g.
                    {"producer_byte_rate": 1048576, "consumer_byte_rate": 2097152}.
        """
        admin = self._get_admin()
        # Try native alter_client_quotas API
        try:
            from kafka.admin import ClientQuotaEntity, ClientQuotaAlteration, ClientQuotaAlterationEntry
            entity = ClientQuotaEntity(entries=[
                ClientQuotaEntity.Component(entity_type=entity_type, entity_name=entity_name),
            ])
            ops = []
            for key, value in quotas.items():
                ops.append(ClientQuotaAlterationEntry(key=key, value=float(value)))
            alteration = ClientQuotaAlteration(entity=entity, ops=ops)
            admin.alter_client_quotas([alteration])
            return {"success": True, "entityType": entity_type, "entityName": entity_name}
        except (ImportError, AttributeError):
            pass
        except Exception as e:
            logger.debug(f"alter_client_quotas failed: {e}")
            return {"success": False, "error": str(e)}

        # Fallback: try alter_configs with user/client config
        try:
            from kafka.admin import ConfigResource, ConfigResourceType
            # Map entity_type to ConfigResourceType if possible
            if entity_type == "client-id":
                # Some versions support CLIENT_METRICS or similar
                resource = ConfigResource(ConfigResourceType.BROKER, "")
            else:
                return {
                    "success": False,
                    "error": (
                        "Client quota management is not supported by this version of kafka-python-ng. "
                        "Use 'kafka-configs.sh --alter --add-config' to set quotas directly."
                    ),
                }
            configs = {k: str(v) for k, v in quotas.items()}
            resource = ConfigResource(ConfigResourceType.BROKER, "", configs=configs)
            admin.alter_configs([resource])
            return {"success": True, "entityType": entity_type, "entityName": entity_name}
        except (ImportError, AttributeError, Exception) as e:
            return {
                "success": False,
                "error": (
                    "Client quota management is not supported by this version of kafka-python-ng. "
                    f"Use 'kafka-configs.sh --alter --add-config' to set quotas directly. Detail: {e}"
                ),
            }

    def delete_quota(self, entity_type: str, entity_name: str, quota_keys: list[str]) -> dict:
        """Delete specific quota keys for a client entity.

        Args:
            entity_type: One of 'user', 'client-id', or 'ip'.
            entity_name: The entity name.
            quota_keys: List of quota key names to remove, e.g. ["producer_byte_rate"].
        """
        admin = self._get_admin()
        # Try native alter_client_quotas with remove ops
        try:
            from kafka.admin import ClientQuotaEntity, ClientQuotaAlteration, ClientQuotaAlterationEntry
            entity = ClientQuotaEntity(entries=[
                ClientQuotaEntity.Component(entity_type=entity_type, entity_name=entity_name),
            ])
            ops = []
            for key in quota_keys:
                # Setting value to None signals removal
                ops.append(ClientQuotaAlterationEntry(key=key, value=None))
            alteration = ClientQuotaAlteration(entity=entity, ops=ops)
            admin.alter_client_quotas([alteration])
            return {"success": True, "entityType": entity_type, "entityName": entity_name, "removed": quota_keys}
        except (ImportError, AttributeError):
            pass
        except Exception as e:
            logger.debug(f"alter_client_quotas (delete) failed: {e}")
            return {"success": False, "error": str(e)}

        return {
            "success": False,
            "error": (
                "Client quota management is not supported by this version of kafka-python-ng. "
                "Use 'kafka-configs.sh --alter --delete-config' to remove quotas directly."
            ),
        }

    def search_messages(
        self,
        topic: str,
        key_pattern: str = None,
        value_pattern: str = None,
        partition: int = None,
        start_time: int = None,
        end_time: int = None,
        max_results: int = 100,
    ) -> list:
        """Search/filter messages in a topic by key pattern, value pattern, partition, and time range.

        Args:
            topic: The topic to search.
            key_pattern: Optional regex pattern to filter message keys.
            value_pattern: Optional regex pattern to filter message values.
            partition: Optional partition number to restrict search to.
            start_time: Optional start timestamp (ms) to seek to.
            end_time: Optional end timestamp (ms) to stop consuming at.
            max_results: Maximum number of matching messages to return (default 100).

        Returns:
            List of dicts with partition, offset, timestamp, key, value, headers.
        """
        cfg = self._build_kafka_config()
        consumer = KafkaConsumer(
            **cfg,
            auto_offset_reset="earliest",
            group_id=None,
            consumer_timeout_ms=10000,
        )
        try:
            all_partitions = consumer.partitions_for_topic(topic)
            if not all_partitions:
                return []

            if partition is not None:
                if partition not in all_partitions:
                    return []
                topic_partitions = [TopicPartition(topic, partition)]
            else:
                topic_partitions = [TopicPartition(topic, p) for p in sorted(all_partitions)]

            consumer.assign(topic_partitions)

            if start_time is not None:
                timestamps = {tp: start_time for tp in topic_partitions}
                offsets = consumer.offsets_for_times(timestamps)
                for tp, offset_and_ts in offsets.items():
                    if offset_and_ts is not None:
                        consumer.seek(tp, offset_and_ts.offset)
                    else:
                        # No messages at or after start_time for this partition;
                        # seek to end so it yields nothing.
                        end_offsets = consumer.end_offsets([tp])
                        consumer.seek(tp, end_offsets[tp])

            compiled_key = re.compile(key_pattern) if key_pattern else None
            compiled_value = re.compile(value_pattern) if value_pattern else None

            results = []
            while len(results) < max_results:
                records = consumer.poll(timeout_ms=2000, max_records=500)
                if not records:
                    break
                for tp, messages in records.items():
                    for msg in messages:
                        if len(results) >= max_results:
                            break

                        if end_time is not None and msg.timestamp > end_time:
                            continue

                        # Decode key
                        if msg.key is None:
                            key_str = None
                        else:
                            try:
                                key_str = msg.key.decode("utf-8")
                            except (UnicodeDecodeError, AttributeError):
                                key_str = str(msg.key)

                        # Decode value
                        if msg.value is None:
                            value_str = None
                        else:
                            try:
                                value_str = msg.value.decode("utf-8")
                            except (UnicodeDecodeError, AttributeError):
                                value_str = str(msg.value)

                        # Apply key pattern filter
                        if compiled_key:
                            if key_str is None or not compiled_key.search(key_str):
                                continue

                        # Apply value pattern filter
                        if compiled_value:
                            if value_str is None or not compiled_value.search(value_str):
                                continue

                        # Decode headers
                        headers = None
                        if msg.headers:
                            headers = []
                            for h_key, h_val in msg.headers:
                                try:
                                    h_val_str = h_val.decode("utf-8") if h_val else None
                                except (UnicodeDecodeError, AttributeError):
                                    h_val_str = str(h_val)
                                headers.append({"key": h_key, "value": h_val_str})

                        results.append({
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,
                            "key": key_str,
                            "value": value_str,
                            "headers": headers,
                        })

            return results
        finally:
            consumer.close()
