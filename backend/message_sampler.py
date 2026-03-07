"""MessageSampler — reads last N messages from a topic for inspection."""

import json
import logging
from kafka import KafkaConsumer, TopicPartition

logger = logging.getLogger(__name__)


class MessageSampler:
    """Samples recent messages from a Kafka topic."""

    def __init__(self, bootstrap_servers: str, max_messages: int = 20,
                 sasl_enabled: bool = False, sasl_username: str = "",
                 sasl_password: str = "", ssl_enabled: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.max_messages = max_messages
        self.sasl_enabled = sasl_enabled
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.ssl_enabled = ssl_enabled

    def _build_config(self) -> dict:
        cfg = {
            "bootstrap_servers": self.bootstrap_servers,
            "consumer_timeout_ms": 3000,
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
        }
        if self.sasl_enabled:
            cfg["security_protocol"] = "SASL_SSL" if self.ssl_enabled else "SASL_PLAINTEXT"
            cfg["sasl_mechanism"] = "SCRAM-SHA-512"
            cfg["sasl_plain_username"] = self.sasl_username
            cfg["sasl_plain_password"] = self.sasl_password
        elif self.ssl_enabled:
            cfg["security_protocol"] = "SSL"
        return cfg

    def sample(self, topic: str) -> list[dict]:
        """Fetch last N messages from a topic across all partitions."""
        messages = []
        consumer = None
        try:
            cfg = self._build_config()
            consumer = KafkaConsumer(**cfg)

            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                return []

            tps = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(tps)

            # Seek to near end of each partition
            end_offsets = consumer.end_offsets(tps)
            for tp in tps:
                end = end_offsets.get(tp, 0)
                start = max(0, end - self.max_messages)
                consumer.seek(tp, start)

            # Read messages
            raw = consumer.poll(timeout_ms=3000, max_records=self.max_messages * len(tps))
            for tp, records in raw.items():
                for record in records:
                    messages.append(self._format_record(record))

            # Sort by timestamp, take last N
            messages.sort(key=lambda m: m.get("timestamp", 0), reverse=True)
            return messages[:self.max_messages]

        except Exception as e:
            logger.error(f"Failed to sample topic {topic}: {e}")
            return []
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

    def _format_record(self, record) -> dict:
        """Format a ConsumerRecord into a JSON-serializable dict."""
        value = self._decode_value(record.value)
        key = record.key.decode("utf-8", errors="replace") if record.key else None

        headers = {}
        if record.headers:
            for k, v in record.headers:
                headers[k] = v.decode("utf-8", errors="replace") if v else None

        return {
            "offset": record.offset,
            "partition": record.partition,
            "timestamp": record.timestamp,
            "key": key,
            "headers": headers,
            "value": value,
            "format": self._detect_format(record.value),
        }

    def _decode_value(self, raw: bytes | None) -> str | dict | None:
        if raw is None:
            return None
        # Try JSON
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        # Try UTF-8
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            pass
        # Hex fallback
        return raw.hex()

    def _detect_format(self, raw: bytes | None) -> str:
        if raw is None:
            return "null"
        try:
            json.loads(raw)
            return "json"
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        try:
            raw.decode("utf-8")
            return "utf8"
        except UnicodeDecodeError:
            return "hex"
