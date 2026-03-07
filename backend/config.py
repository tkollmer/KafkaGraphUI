"""Configuration — all settings via environment variables."""

import os
import re


class Config:
    """Validated configuration from environment variables."""

    def __init__(self):
        self.KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.POLL_INTERVAL_MS = int(os.getenv("POLL_INTERVAL_MS", "2000"))
        self.MAX_WS_QUEUE = int(os.getenv("MAX_WS_QUEUE", "50"))
        self.SHOW_PRODUCERS = os.getenv("SHOW_PRODUCERS", "false").lower() == "true"
        self.SAMPLING_ENABLED = os.getenv("SAMPLING_ENABLED", "false").lower() == "true"
        self.MAX_SAMPLE_MESSAGES = int(os.getenv("MAX_SAMPLE_MESSAGES", "20"))
        self.LAG_WARN_THRESHOLD = int(os.getenv("LAG_WARN_THRESHOLD", "1000"))
        self.ANIMATIONS_ENABLED = os.getenv("ANIMATIONS_ENABLED", "true").lower() == "true"
        self.PRODUCER_GROUP_REGEX = os.getenv("PRODUCER_GROUP_REGEX", r"-[a-z0-9]{5,}$")
        self.UI_AUTH_ENABLED = os.getenv("UI_AUTH_ENABLED", "false").lower() == "true"
        self.UI_USERNAME = os.getenv("UI_USERNAME", "")
        self.UI_PASSWORD = os.getenv("UI_PASSWORD", "")
        self.KAFKA_SASL_ENABLED = os.getenv("KAFKA_SASL_ENABLED", "false").lower() == "true"
        self.KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
        self.KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
        self.KAFKA_SSL_ENABLED = os.getenv("KAFKA_SSL_ENABLED", "false").lower() == "true"
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
        self.PORT = int(os.getenv("PORT", "8080"))

    def validate(self) -> list[str]:
        """Return list of validation errors. Empty = valid."""
        errors = []

        if not self.KAFKA_BOOTSTRAP_SERVERS:
            errors.append("KAFKA_BOOTSTRAP_SERVERS must not be empty")

        if self.POLL_INTERVAL_MS < 500:
            errors.append("POLL_INTERVAL_MS must be >= 500")

        if self.MAX_WS_QUEUE < 1:
            errors.append("MAX_WS_QUEUE must be >= 1")

        if self.MAX_SAMPLE_MESSAGES < 1:
            errors.append("MAX_SAMPLE_MESSAGES must be >= 1")

        if self.LAG_WARN_THRESHOLD < 0:
            errors.append("LAG_WARN_THRESHOLD must be >= 0")

        try:
            re.compile(self.PRODUCER_GROUP_REGEX)
        except re.error as e:
            errors.append(f"PRODUCER_GROUP_REGEX is invalid: {e}")

        if self.UI_AUTH_ENABLED:
            if not self.UI_USERNAME:
                errors.append("UI_USERNAME required when UI_AUTH_ENABLED=true")
            if not self.UI_PASSWORD:
                errors.append("UI_PASSWORD required when UI_AUTH_ENABLED=true")

        if self.KAFKA_SASL_ENABLED:
            if not self.KAFKA_SASL_USERNAME:
                errors.append("KAFKA_SASL_USERNAME required when KAFKA_SASL_ENABLED=true")
            if not self.KAFKA_SASL_PASSWORD:
                errors.append("KAFKA_SASL_PASSWORD required when KAFKA_SASL_ENABLED=true")

        if self.LOG_LEVEL not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            errors.append(f"LOG_LEVEL must be DEBUG|INFO|WARNING|ERROR, got '{self.LOG_LEVEL}'")

        return errors


config = Config()
