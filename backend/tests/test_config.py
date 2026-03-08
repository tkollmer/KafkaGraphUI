"""Tests for configuration validation."""

import os
import pytest
from config import Config


class TestConfig:
    def test_defaults(self):
        cfg = Config()
        assert cfg.KAFKA_BOOTSTRAP_SERVERS == "localhost:9092"
        assert cfg.POLL_INTERVAL_MS == 2000
        assert cfg.MAX_WS_QUEUE == 50
        assert cfg.SHOW_PRODUCERS is False
        assert cfg.SAMPLING_ENABLED is False
        assert cfg.MAX_SAMPLE_MESSAGES == 20
        assert cfg.LAG_WARN_THRESHOLD == 1000
        assert cfg.ANIMATIONS_ENABLED is True
        assert cfg.LOG_LEVEL == "INFO"
        assert cfg.PORT == 8080

    def test_valid_config(self):
        cfg = Config()
        errors = cfg.validate()
        assert errors == []

    def test_poll_interval_too_low(self):
        cfg = Config()
        cfg.POLL_INTERVAL_MS = 100
        errors = cfg.validate()
        assert any("POLL_INTERVAL_MS" in e for e in errors)

    def test_invalid_regex(self):
        cfg = Config()
        cfg.PRODUCER_GROUP_REGEX = "[invalid"
        errors = cfg.validate()
        assert any("PRODUCER_GROUP_REGEX" in e for e in errors)

    def test_sasl_requires_credentials(self):
        cfg = Config()
        cfg.KAFKA_SASL_ENABLED = True
        cfg.KAFKA_SASL_USERNAME = ""
        cfg.KAFKA_SASL_PASSWORD = ""
        errors = cfg.validate()
        assert any("KAFKA_SASL_USERNAME" in e for e in errors)
        assert any("KAFKA_SASL_PASSWORD" in e for e in errors)

    def test_ui_auth_requires_credentials(self):
        cfg = Config()
        cfg.UI_AUTH_ENABLED = True
        cfg.UI_USERNAME = ""
        cfg.UI_PASSWORD = ""
        errors = cfg.validate()
        assert any("UI_USERNAME" in e for e in errors)
        assert any("UI_PASSWORD" in e for e in errors)

    def test_invalid_log_level(self):
        cfg = Config()
        cfg.LOG_LEVEL = "TRACE"
        errors = cfg.validate()
        assert any("LOG_LEVEL" in e for e in errors)

    def test_negative_lag_threshold(self):
        cfg = Config()
        cfg.LAG_WARN_THRESHOLD = -1
        errors = cfg.validate()
        assert any("LAG_WARN_THRESHOLD" in e for e in errors)

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
        monkeypatch.setenv("POLL_INTERVAL_MS", "5000")
        monkeypatch.setenv("SAMPLING_ENABLED", "true")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")
        cfg = Config()
        assert cfg.KAFKA_BOOTSTRAP_SERVERS == "broker1:9092,broker2:9092"
        assert cfg.POLL_INTERVAL_MS == 5000
        assert cfg.SAMPLING_ENABLED is True
        assert cfg.LOG_LEVEL == "DEBUG"
