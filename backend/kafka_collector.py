"""KafkaCollector — polls Kafka AdminClient for metadata, offsets, consumer groups."""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import (
    KafkaError,
    KafkaConnectionError,
    TopicAuthorizationFailedError,
    GroupAuthorizationFailedError,
)

logger = logging.getLogger(__name__)


@dataclass
class TopicInfo:
    name: str
    partitions: int
    end_offsets: dict[int, int] = field(default_factory=dict)
    prev_offsets: dict[int, int] = field(default_factory=dict)
    msg_per_sec: float = 0.0
    total_messages: int = 0
    status: str = "ok"


@dataclass
class ConsumerGroupInfo:
    group_id: str
    members: int = 0
    total_lag: int = 0
    per_partition_lag: dict[str, int] = field(default_factory=dict)
    subscribed_topics: list[str] = field(default_factory=list)
    member_client_ids: list[str] = field(default_factory=list)
    status: str = "ok"


@dataclass
class ClusterSnapshot:
    topics: dict[str, TopicInfo] = field(default_factory=dict)
    consumer_groups: dict[str, ConsumerGroupInfo] = field(default_factory=dict)
    active_partitions: dict[str, set[int]] = field(default_factory=dict)
    timestamp: float = 0.0


class KafkaCollector:
    """Polls Kafka cluster metadata at configurable interval."""

    def __init__(self, bootstrap_servers: str, poll_interval_ms: int = 2000,
                 sasl_enabled: bool = False, sasl_username: str = "",
                 sasl_password: str = "", ssl_enabled: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.poll_interval_s = poll_interval_ms / 1000.0
        self.sasl_enabled = sasl_enabled
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ssl_enabled = ssl_enabled
        self._admin: KafkaAdminClient | None = None
        self._consumer: KafkaConsumer | None = None
        self._snapshot = ClusterSnapshot()
        self._connected = False
        self._running = False
        self._last_poll_time = 0.0

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def snapshot(self) -> ClusterSnapshot:
        return self._snapshot

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

    async def connect(self, max_retries: int = 30, retry_delay: float = 2.0):
        """Connect to Kafka with retry logic."""
        for attempt in range(max_retries):
            try:
                kafka_cfg = self._build_kafka_config()
                self._admin = KafkaAdminClient(**kafka_cfg)
                self._consumer = KafkaConsumer(**kafka_cfg)
                self._connected = True
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except KafkaConnectionError as e:
                logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries}: {e}")
                await asyncio.sleep(retry_delay)
            except Exception as e:
                logger.error(f"Unexpected error connecting to Kafka: {e}")
                await asyncio.sleep(retry_delay)
        raise ConnectionError(f"Failed to connect to Kafka after {max_retries} attempts")

    async def poll(self) -> ClusterSnapshot:
        """Single poll cycle — fetch topics, offsets, consumer groups."""
        if not self._admin:
            raise RuntimeError("Not connected to Kafka")

        now = time.time()
        elapsed = now - self._last_poll_time if self._last_poll_time > 0 else self.poll_interval_s
        self._last_poll_time = now

        snapshot = ClusterSnapshot(timestamp=now)

        # Fetch topics
        try:
            topic_names = self._consumer.topics() if self._consumer else set()
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return self._snapshot

        for topic_name in topic_names:
            if topic_name.startswith("_"):
                continue

            try:
                part_ids = self._consumer.partitions_for_topic(topic_name) or set()
            except Exception:
                part_ids = set()

            topic_info = TopicInfo(name=topic_name, partitions=len(part_ids))

            # Get end offsets
            try:
                partitions = [TopicPartition(topic_name, p) for p in part_ids]
                if partitions and self._consumer:
                    end_offsets = self._consumer.end_offsets(partitions)
                    for tp, offset in end_offsets.items():
                        topic_info.end_offsets[tp.partition] = offset
                        topic_info.total_messages += offset
            except TopicAuthorizationFailedError:
                topic_info.status = "access_denied"
            except Exception as e:
                logger.debug(f"Failed to get offsets for {topic_name}: {e}")

            # Calculate msg/s from offset deltas
            prev_topic = self._snapshot.topics.get(topic_name)
            if prev_topic and elapsed > 0:
                total_delta = 0
                active_parts = set()
                for part, offset in topic_info.end_offsets.items():
                    prev_offset = prev_topic.end_offsets.get(part, offset)
                    delta = offset - prev_offset
                    if delta > 0:
                        total_delta += delta
                        active_parts.add(part)
                topic_info.msg_per_sec = round(total_delta / elapsed, 1)
                topic_info.prev_offsets = prev_topic.end_offsets.copy()
                if active_parts:
                    snapshot.active_partitions[topic_name] = active_parts

            snapshot.topics[topic_name] = topic_info

        # Fetch consumer groups
        try:
            group_listing = self._admin.list_consumer_groups()
            group_ids = [g[0] for g in group_listing]
        except GroupAuthorizationFailedError:
            group_ids = []
        except Exception as e:
            logger.error(f"Failed to list consumer groups: {e}")
            group_ids = []

        for gid in group_ids:
            group_info = ConsumerGroupInfo(group_id=gid)
            try:
                described = self._admin.describe_consumer_groups([gid])
                if described:
                    group_desc = described[0]
                    if hasattr(group_desc, 'members'):
                        group_info.members = len(group_desc.members)
                        # Extract client_ids from members
                        for member in group_desc.members:
                            if hasattr(member, 'client_id') and member.client_id:
                                group_info.member_client_ids.append(member.client_id)
                            # Extract subscribed topics from metadata
                            if hasattr(member, 'member_metadata') and member.member_metadata:
                                try:
                                    from kafka.protocol.group import MemberMetadata
                                    metadata = MemberMetadata.decode(member.member_metadata)
                                    if hasattr(metadata, 'subscription'):
                                        group_info.subscribed_topics = list(
                                            set(group_info.subscribed_topics) | set(metadata.subscription)
                                        )
                                except Exception:
                                    pass

                # Get offsets for lag calculation
                try:
                    offsets = self._admin.list_consumer_group_offsets(gid)
                    total_lag = 0
                    for tp, offset_meta in offsets.items():
                        if tp.topic in snapshot.topics:
                            end_offset = snapshot.topics[tp.topic].end_offsets.get(tp.partition, 0)
                            lag = max(0, end_offset - offset_meta.offset)
                            total_lag += lag
                            group_info.per_partition_lag[f"{tp.topic}-{tp.partition}"] = lag
                            if tp.topic not in group_info.subscribed_topics:
                                group_info.subscribed_topics.append(tp.topic)
                    group_info.total_lag = total_lag
                except Exception as e:
                    logger.debug(f"Failed to get offsets for group {gid}: {e}")

            except GroupAuthorizationFailedError:
                group_info.status = "access_denied"
            except Exception as e:
                logger.debug(f"Failed to describe group {gid}: {e}")

            snapshot.consumer_groups[gid] = group_info

        self._snapshot = snapshot
        return snapshot

    async def start_polling(self, callback=None):
        self._running = True
        while self._running:
            try:
                snapshot = await self.poll()
                if callback:
                    await callback(snapshot)
            except Exception as e:
                logger.error(f"Poll error: {e}")
            await asyncio.sleep(self.poll_interval_s)

    def stop(self):
        self._running = False
        for client in (self._admin, self._consumer):
            if client:
                try:
                    client.close()
                except Exception:
                    pass
        self._connected = False
